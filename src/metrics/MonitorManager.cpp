#include "MonitorManager.h"
#include "../utils/MemoryUtils.h"
#include "../logger/Logger.h"
#include "../metrics/MetricsExporter.h"
#include "../thread/ThreadSafeQueue.h"
#include "../common/TableBatch.h"
#include "../kafka/KafkaProcessor.h"
#include <thread>
#include <chrono>
#include <fstream>
#include <unistd.h>
#include <malloc.h>

extern ThreadSafeQueue<std::tuple<std::string, int, int64_t, int64_t, rd_kafka_message_t*>> kafkaMessageQueue;
extern ThreadSafeQueue<std::tuple<std::string, TableBatch>> dbWriteQueue;
extern KafkaProcessor* globalKafkaProcessor;

static std::pair<int, int> getRSSandVMMemory() {
    std::ifstream statm("/proc/self/statm");
    size_t totalPages = 0, residentPages = 0;
    statm >> totalPages >> residentPages;

    long pageSize = sysconf(_SC_PAGESIZE);
    int rssMb = static_cast<int>((residentPages * static_cast<size_t>(pageSize)) / (1024 * 1024));
    int vmMb  = static_cast<int>((totalPages * static_cast<size_t>(pageSize)) / (1024 * 1024));
    return {rssMb, vmMb};
}

void MonitorManager::startMonitors(KafkaConsumer& consumer, WriteDataToDB& writeData, ConfigLoader& config) {
    OpenSync::Logger::info("🧠 Starting Unified MonitorManager...");

    auto& stopFlag = consumer.getStopFlag();

    int memoryInterval = config.getInt("monitor.memory_monitor_interval_sec", 5);
    int metricsInterval = config.getInt("monitor.metrics_monitor_interval_sec", 5);
    int connectorInterval = config.getInt("monitor.connector_monitor_interval_sec", 10);
    int bufferInterval = config.getInt("monitor.table_buffer_monitor_interval_sec", 5);
    int cleanupInterval = config.getInt("monitor.table_buffer_cleanup_interval_sec", 10);
    int systemMetricsInterval = config.getInt("monitor.system_metrics_monitor_interval_sec", 1);

    using clock = std::chrono::steady_clock;

    std::thread([&stopFlag, &writeData, memoryInterval, metricsInterval, connectorInterval, bufferInterval, cleanupInterval, systemMetricsInterval]() {
        OpenSync::Logger::info("🔎 Unified Monitor thread started.");

        auto lastMemoryCheck = clock::now();
        auto lastMetricsCheck = clock::now();
        auto lastConnectorCheck = clock::now();
        auto lastBufferCheck = clock::now();
        auto lastCleanupCheck = clock::now();
        auto lastSystemMetricsCheck = clock::now();
        static size_t lastDrainedTableCount = 0;
        static int lastLoggedRssMB = 0;
        static int lastLoggedVmMB = 0;

        while (!stopFlag.load()) {
            auto now = clock::now();

            if (std::chrono::duration_cast<std::chrono::seconds>(now - lastMemoryCheck).count() >= memoryInterval) {
                auto [vmMB, rssMB] = MemoryUtils::getMemoryUsageMB();
                const int MEMORY_LOG_THRESHOLD_MB = 3000; // Chỉ log nếu vượt ngưỡng nhất định
                if (rssMB > MEMORY_LOG_THRESHOLD_MB || vmMB > MEMORY_LOG_THRESHOLD_MB) {
                    OpenSync::Logger::info("[Memory] VM: " + std::to_string(vmMB) + " MB, RSS: " + std::to_string(rssMB) + " MB");
                }
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
                if (!bufferCopy.empty() || lastDrainedTableCount != 0) {
                    OpenSync::Logger::info("🧹 Drained tableSQLBuffer, tables: " + std::to_string(bufferCopy.size()));
                    lastDrainedTableCount = bufferCopy.size();
                }
                malloc_trim(0);
                lastCleanupCheck = now;
            }

            if (std::chrono::duration_cast<std::chrono::seconds>(now - lastSystemMetricsCheck).count() >= systemMetricsInterval) {
                auto processor = globalKafkaProcessor;
                if (processor) {
                    size_t dedupMem = processor->estimateDedupCacheMemory();
                    MetricsExporter::getInstance().setGauge("memory_usage_bytes", dedupMem, {{"component", "dedup_cache"}});
                }
                size_t tableMem = writeData.estimateMemoryUsage();
                size_t activeTables = writeData.getActiveTableCount();
                MetricsExporter::getInstance().setGauge("memory_usage_bytes", tableMem, {{"component", "table_sql_buffer"}});
                MetricsExporter::getInstance().setGauge("active_tables_count", static_cast<double>(activeTables), {{"component", "write_data_to_db"}});

                extern size_t getOracleSchemaCacheMemoryUsage();
                size_t schemaMem = getOracleSchemaCacheMemoryUsage();
                MetricsExporter::getInstance().setGauge("memory_usage_bytes", schemaMem, {{"component", "oracle_schema_cache"}});

                if (processor) {
                    auto lagMap = processor->getTotalLagByPartition();
                    for (const auto& [tp, lag] : lagMap) {
                        MetricsExporter::getInstance().setGauge("kafka_partition_lag_sum", lag, { {"topic", tp.first}, {"partition", std::to_string(tp.second)} });
                    }
                }

                auto [rssMb, vmMb] = getRSSandVMMemory();
                MetricsExporter::getInstance().setGauge("rss_memory_mb", rssMb, {});
                MetricsExporter::getInstance().setGauge("vm_memory_mb", vmMb, {});

                const int RSS_MEMORY_THRESHOLD_MB = 4000;
                const int VM_MEMORY_THRESHOLD_MB = 8000;
                bool shouldLogMemoryWarning =
                    (rssMb > RSS_MEMORY_THRESHOLD_MB && std::abs(rssMb - lastLoggedRssMB) >= 50) ||
                    (vmMb > VM_MEMORY_THRESHOLD_MB && std::abs(vmMb - lastLoggedVmMB) >= 50);

                if (shouldLogMemoryWarning) {
                    OpenSync::Logger::warn("⚠️ High memory detected, shrinking lag buffers... (VM: " + std::to_string(vmMb) + " MB, RSS: " + std::to_string(rssMb) + " MB)");
                    if (processor) processor->shrinkLagBuffers();
                    lastLoggedRssMB = rssMb;
                    lastLoggedVmMB = vmMb;
                }

                lastSystemMetricsCheck = now;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }

        OpenSync::Logger::info("🛑 Unified Monitor thread stopped.");
    }).detach();
}

