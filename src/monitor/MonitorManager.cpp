#include "MonitorManager.h"
#include "../utils/MemoryUtils.h"
#include "../logger/Logger.h"
#include "../metrics/MetricsExporter.h"
#include "../thread/ThreadSafeQueue.h"
#include "../common/TableBatch.h"
#include <thread>
#include <chrono>
#include <malloc.h>

extern ThreadSafeQueue<std::tuple<std::string, int, int64_t, int64_t, rd_kafka_message_t*>> kafkaMessageQueue;
extern ThreadSafeQueue<std::tuple<std::string, TableBatch>> dbWriteQueue;

void MonitorManager::startMonitors(
    KafkaConsumer& consumer,
    WriteDataToDB& writeData,
    ConfigLoader& config
) {
    Logger::info("🧠 Starting Unified MonitorManager...");

    auto& stopFlag = consumer.getStopFlag();

    // Đọc các interval từ config
    int memoryInterval = config.getInt("monitor.memory_monitor_interval_sec", 5);
    int metricsInterval = config.getInt("monitor.metrics_monitor_interval_sec", 5);
    int connectorInterval = config.getInt("monitor.connector_monitor_interval_sec", 10);
    int bufferInterval = config.getInt("monitor.table_buffer_monitor_interval_sec", 5);
    int cleanupInterval = config.getInt("monitor.table_buffer_cleanup_interval_sec", 2);

    std::thread([&stopFlag, &writeData, memoryInterval, metricsInterval, connectorInterval, bufferInterval, cleanupInterval]() {
        Logger::info("🔎 Unified Monitor thread started.");

        using clock = std::chrono::steady_clock;
        auto lastMemoryCheck = clock::now();
        auto lastMetricsCheck = clock::now();
        auto lastConnectorCheck = clock::now();
        auto lastBufferCheck = clock::now();
        auto lastCleanupCheck = clock::now();

        while (!stopFlag.load()) {
            auto now = clock::now();

            if (std::chrono::duration_cast<std::chrono::seconds>(now - lastMemoryCheck).count() >= memoryInterval) {
                auto [vmMB, rssMB] = MemoryUtils::getMemoryUsageMB();
                Logger::info("[Memory] VM: " + std::to_string(vmMB) + " MB, RSS: " + std::to_string(rssMB) + " MB");
                lastMemoryCheck = now;
            }

            if (std::chrono::duration_cast<std::chrono::seconds>(now - lastMetricsCheck).count() >= metricsInterval) {
                size_t kafkaSize = kafkaMessageQueue.size();
                size_t dbQueueSize = dbWriteQueue.size();

                MetricsExporter::getInstance().setGauge("kafka_queue_size", kafkaSize, {});
                MetricsExporter::getInstance().setGauge("db_queue_size", dbQueueSize, {});

                auto [vm, rss] = MemoryUtils::getMemoryUsageMB();
                MetricsExporter::getInstance().setGauge("vm_memory_mb", vm, {});
                MetricsExporter::getInstance().setGauge("rss_memory_mb", rss, {});

                lastMetricsCheck = now;
            }

            if (std::chrono::duration_cast<std::chrono::seconds>(now - lastConnectorCheck).count() >= connectorInterval) {
                writeData.reportMemoryUsagePerDBType();
                lastConnectorCheck = now;
            }

            if (std::chrono::duration_cast<std::chrono::seconds>(now - lastBufferCheck).count() >= bufferInterval) {
                writeData.reportTableSQLBufferMetrics();
                lastBufferCheck = now;
            }

            if (std::chrono::duration_cast<std::chrono::seconds>(now - lastCleanupCheck).count() >= cleanupInterval) {
                auto bufferCopy = writeData.drainTableSQLBuffers();
                for (const auto& [table, batch] : bufferCopy) {
                    if (!batch.empty()) {
                        Logger::info("🧹 Drained " + std::to_string(batch.size()) + " rows from buffer of table " + table);
                    }
                }
                malloc_trim(0);
                lastCleanupCheck = now;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(200)); // sleep nhẹ nhàng
        }

        Logger::info("🛑 Unified Monitor thread stopped.");
    }).detach();
}
