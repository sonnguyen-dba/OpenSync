#include "WorkerThread.h"
#include <unordered_map>
#include <chrono>
#include <sstream>
#include <thread>
#include <atomic>
#include "Queues.h"
#include "../../metrics/MetricsExporter.h"
#include "BufferGCManager.h"
#include "../../logger/Logger.h"

void workerThread(KafkaProcessor& processor, int batchFlushIntervalMs, std::atomic<bool>& shouldShutdown) {
    constexpr int shrinkIdleThresholdMs = 1000; // 🧠 Shrink lag nếu idle > 1s
    std::unordered_map<std::string, TableBatch> batches;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> lastMessageTimes;

    auto lastShrinkCheck = std::chrono::steady_clock::now();

    while (!shouldShutdown) {
        std::tuple<std::string, int, int64_t, int64_t, rd_kafka_message_t*> item;
        bool hasData = kafkaMessageQueue.try_pop(item);
        auto now = std::chrono::steady_clock::now();

        if (!hasData) {
            auto idleDuration = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastShrinkCheck).count();
            if (idleDuration >= shrinkIdleThresholdMs) {
                processor.shrinkLagBuffers(30);
                lastShrinkCheck = now;
            }

            // Check timeout flush
            for (auto& [tableKey, batch] : batches) {
                if (!batch.sqls.empty()) {
                    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastMessageTimes[tableKey]).count();
                    if (elapsedMs >= batchFlushIntervalMs) {
                        Logger::debug("⏱️ Flushing timeout batch: " + tableKey + ", size=" + std::to_string(batch.sqls.size()));
                        dbWriteQueue.push({tableKey, std::move(batch)});
                        batches[tableKey] = TableBatch{};
                        lastMessageTimes[tableKey] = now;
                    }
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        // Parse message
        auto& [message, partition, offset, timestamp, rawMsg] = item;
        auto batchMap = processor.processMessageByTable(message, partition, offset, timestamp);
        now = std::chrono::steady_clock::now(); // Cập nhật lại thời gian

        for (auto& [tableKey, sqls] : batchMap) {
            auto& batch = batches[tableKey];
            for (auto& sql : sqls) {
                batch.sqls.push_back(std::move(sql));
                batch.messages.push_back(rawMsg);
            }
            lastMessageTimes[tableKey] = now;

            if (batch.sqls.size() >= batchSize) {
                Logger::debug("📦 Flushing full batch: " + tableKey + ", size=" + std::to_string(batch.sqls.size()));
                dbWriteQueue.push({tableKey, std::move(batch)});
                batches[tableKey] = TableBatch{};
                lastMessageTimes[tableKey] = now;
            }
        }
    }

    // Shutdown: flush remaining
    Logger::info("⚙️ Worker shutting down. Flushing remaining batches...");
    for (auto& [tableKey, batch] : batches) {
        if (!batch.sqls.empty()) {
            Logger::info("🧹 Final batch flush: " + tableKey + ", size=" + std::to_string(batch.sqls.size()));
            dbWriteQueue.push({tableKey, std::move(batch)});
        }
    }

    // Flush Kafka messages left
    std::tuple<std::string, int, int64_t, int64_t, rd_kafka_message_t*> itemLeft;
    while (kafkaMessageQueue.try_pop(itemLeft)) {
        auto& [message, partition, offset, timestamp, rawMsg] = itemLeft;
        //Logger::info("Processing message, size: " + std::to_string(message.size() / 1024) + " KB");
        auto batchMap = processor.processMessageByTable(message, partition, offset, timestamp);
        //auto now = std::chrono::steady_clock::now();
        for (auto& [tableKey, sqls] : batchMap) {
            auto& batch = batches[tableKey];
            for (auto& sql : sqls) {
                batch.sqls.push_back(std::move(sql));
                batch.messages.push_back(rawMsg);
            }
            if (batch.sqls.size() >= batchSize) {
                Logger::info("🧹 Flushing Kafka left batch: " + tableKey + ", size=" + std::to_string(batch.sqls.size()));
                dbWriteQueue.push({tableKey, std::move(batch)});
                batches[tableKey] = TableBatch{};
            }
        }
    }

    // Flush final
    for (auto& [tableKey, batch] : batches) {
        if (!batch.sqls.empty()) {
            Logger::info("🧹 Flushing Kafka left final batch: " + tableKey + ", size=" + std::to_string(batch.sqls.size()));
            dbWriteQueue.push({tableKey, std::move(batch)});
        }
    }

    Logger::info("✅ Worker thread stopped cleanly.");
}
