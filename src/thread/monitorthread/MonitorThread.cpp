// MonitorThread.cpp
#include "MonitorThread.h"
#include "../../utils/MemoryUtils.h"
#include "../../logger/Logger.h"
#include "../../metrics/MetricsExporter.h"
#include "../ThreadSafeQueue.h"
#include "../../common/TableBatch.h"
#include "../../WriteDataToDB/WriteDataToDB.h"
#include <thread>
#include <chrono>
#include <malloc.h>

extern ThreadSafeQueue<std::tuple<std::string, int, int64_t, int64_t, rd_kafka_message_t*>> kafkaMessageQueue;
extern ThreadSafeQueue<std::tuple<std::string, TableBatch>> dbWriteQueue;

void startMemoryMonitorThread(std::atomic<bool>& stopFlag) {
    std::thread([&stopFlag]() {
        Logger::info("🧠 Starting Memory Monitor Thread...");
        while (!stopFlag.load()) {
            auto [vmMB, rssMB] = MemoryUtils::getMemoryUsageMB();
            Logger::info("[Memory] VM: " + std::to_string(vmMB) + " MB, RSS: " + std::to_string(rssMB) + " MB");
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
        Logger::info("🧠 Memory Monitor Thread stopped.");
    }).detach();
}

void startMetricsMonitorThread(std::atomic<bool>& stopFlag) {
    std::thread([&stopFlag]() {
        Logger::info("📈 Starting Metrics Monitor Thread...");
        while (!stopFlag.load()) {
            size_t kafkaSize = kafkaMessageQueue.size();
            size_t dbQueueSize = dbWriteQueue.size();

            MetricsExporter::getInstance().setGauge("kafka_queue_size", kafkaSize, {});
            MetricsExporter::getInstance().setGauge("db_queue_size", dbQueueSize, {});

            auto [vm, rss] = MemoryUtils::getMemoryUsageMB();
            MetricsExporter::getInstance().setGauge("vm_memory_mb", vm, {});
            MetricsExporter::getInstance().setGauge("rss_memory_mb", rss, {});

            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
        Logger::info("📈 Metrics Monitor Thread stopped.");
    }).detach();
}

void startConnectorMetricsThread(std::atomic<bool>& stopFlag, WriteDataToDB& writeData) {
    std::thread([&stopFlag, &writeData]() {
        Logger::info("🔌 Starting Connector Metrics Thread...");
        while (!stopFlag.load()) {
            writeData.reportMemoryUsagePerDBType();
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
        Logger::info("🔌 Connector Metrics Thread stopped.");
    }).detach();
}

void startTableBufferMetricsThread(std::atomic<bool>& stopFlag, WriteDataToDB& writeData) {
    std::thread([&stopFlag, &writeData]() {
        Logger::info("📄 Starting Table Buffer Metrics Thread...");
        while (!stopFlag.load()) {
            writeData.reportTableSQLBufferMetrics();
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
        Logger::info("📄 Table Buffer Metrics Thread stopped.");
    }).detach();
}

void startTableBufferCleanupThread(std::atomic<bool>& stopFlag, WriteDataToDB& writeData) {
    std::thread([&stopFlag, &writeData]() {
        Logger::info("🧹 Starting Table Buffer Cleanup Thread...");
        while (!stopFlag.load()) {
            auto bufferCopy = writeData.drainTableSQLBuffers();
            for (const auto& [table, batch] : bufferCopy) {
                if (!batch.empty()) {
                    Logger::info("🧹 Drained " + std::to_string(batch.size()) + " rows from buffer of table " + table);
                }
            }
            malloc_trim(0);  // Force OS memory release
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
        Logger::info("🧹 Table Buffer Cleanup Thread stopped.");
    }).detach();
}

