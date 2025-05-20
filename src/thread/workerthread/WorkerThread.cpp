#include "WorkerThread.h"
#include "../../kafka/KafkaProcessor.h"
#include "../../common/Queues.h"
#include "../../common/TableBatch.h"
#include "../../logger/Logger.h"
#include "../../common/TimeCur.h"
#include <chrono>
#include <thread>

void workerThread(KafkaProcessor& processor, int batchFlushIntervalMs, std::atomic<bool>& shouldShutdown) {
    OpenSync::Logger::info("🧵 Worker thread started.");

    std::unordered_map<std::string, TableBatch> tableBuffers;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> lastFlushTime;

    while (!shouldShutdown) {
        std::tuple<std::string, int, int64_t, int64_t, rd_kafka_message_t*> item;
        bool hasMessage = kafkaMessageQueue.try_pop(item, std::chrono::milliseconds(100));

        auto now = std::chrono::steady_clock::now();

        if (hasMessage) {
            auto& [message, partition, offset, timestamp, rawMsg] = item;

            // Parse Kafka message

	    OpenSync::Logger::debug("📩 Kafka message received at: " + std::to_string(getCurrentTimeMs()));
            auto batchMap = processor.processMessageByTable(message, partition, offset, timestamp);
	    OpenSync::Logger::debug("✅ SQLBuilder called at: " + std::to_string(getCurrentTimeMs()));


            for (auto& [tableKey, sqls] : batchMap) {
                auto& batch = tableBuffers[tableKey];
                batch.sqls.insert(batch.sqls.end(), sqls.begin(), sqls.end());
                batch.messages.push_back(rawMsg);
                lastFlushTime[tableKey] = now;

                // Flush nếu batch đủ lớn
                if (batch.sqls.size() >= batchSize) {
                    dbWriteQueue.push({tableKey, std::move(batch)});
                    tableBuffers.erase(tableKey);
                    lastFlushTime.erase(tableKey);
                }
            }
        }

        // Kiểm tra timeout flush buffer
        for (auto it = lastFlushTime.begin(); it != lastFlushTime.end();) {
            const auto& tableKey = it->first;
            auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second).count();

            if (elapsedMs >= batchFlushIntervalMs) {
                if (!tableBuffers[tableKey].sqls.empty()) {
                    OpenSync::Logger::debug("⏳ Timeout flush for table: " + tableKey + ", batch size: " + std::to_string(tableBuffers[tableKey].sqls.size()));
                    dbWriteQueue.push({tableKey, std::move(tableBuffers[tableKey])});
                }
                tableBuffers.erase(tableKey);
                it = lastFlushTime.erase(it);
            } else {
                ++it;
            }
        }
    }

    // 🔥 Khi shutdown, flush toàn bộ còn lại
    OpenSync::Logger::info("🛑 Flushing remaining buffers before shutdown...");
    for (auto& [tableKey, batch] : tableBuffers) {
        if (!batch.sqls.empty()) {
            dbWriteQueue.push({tableKey, std::move(batch)});
        }
    }
    tableBuffers.clear();
    lastFlushTime.clear();

    OpenSync::Logger::info("🎯 Worker thread exited cleanly.");
}

