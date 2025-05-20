#include "SystemMetricsUtils.h"
#include "../metrics/MetricsExporter.h"
#include "../kafka/KafkaProcessor.h"
#include "../writer/WriteDataToDB.h"
#include "../logger/Logger.h"
#include <thread>
#include <chrono>
#include <fstream>
#include <unistd.h>

// Global pointer definitions
KafkaProcessor* globalKafkaProcessor = nullptr;
WriteDataToDB* globalWriteDataToDB = nullptr;

std::atomic<bool> SystemMetricsUtils::stopFlag = false;

void SystemMetricsUtils::startBackgroundMetricsThread() {
    std::thread([] {
        backgroundMetricsThread();
    }).detach();
}

void SystemMetricsUtils::stopBackgroundMetricsThread() {
    stopFlag = true;
}

void SystemMetricsUtils::backgroundMetricsThread() {
    while (!stopFlag) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // Memory usage by internal modules
        if (globalKafkaProcessor) {
            size_t dedupMem = globalKafkaProcessor->estimateDedupCacheMemory();
            MetricsExporter::getInstance().setGauge("memory_usage_bytes", dedupMem, {{"component", "dedup_cache"}});
        }

        if (globalWriteDataToDB) {
            size_t tableMem = globalWriteDataToDB->estimateMemoryUsage();
            size_t activeTables = globalWriteDataToDB->getActiveTableCount();

            MetricsExporter::getInstance().setGauge("memory_usage_bytes", tableMem, {{"component", "table_sql_buffer"}});
            MetricsExporter::getInstance().setGauge("active_tables_count", static_cast<double>(activeTables), {{"component", "write_data_to_db"}});
        }

        // Oracle schema cache
        extern size_t getOracleSchemaCacheMemoryUsage();
        size_t schemaMem = getOracleSchemaCacheMemoryUsage();
        MetricsExporter::getInstance().setGauge("memory_usage_bytes", schemaMem, {{"component", "oracle_schema_cache"}});

        // DB Connector Pool
        if (globalWriteDataToDB) {
            globalWriteDataToDB->reportMemoryUsagePerDBType();
        }

        // Kafka lag per partition
        if (globalKafkaProcessor) {
            auto lagMap = globalKafkaProcessor->getTotalLagByPartition();
            for (const auto& [tp, lag] : lagMap) {
                MetricsExporter::getInstance().setGauge("kafka_partition_lag_sum", lag, {
                    {"topic", tp.first}, {"partition", std::to_string(tp.second)}
                });
            }
        }

        // RSS and VM usage (in MB)
        auto [rssMb, vmMb] = getRSSandVMMemory();

	MetricsExporter::getInstance().setGauge("rss_memory_mb", rssMb, {});
	MetricsExporter::getInstance().setGauge("vm_memory_mb", vmMb, {});

	// RSS memory check (auto-clean if high)
	const int RSS_MEMORY_THRESHOLD_MB = 4000;
	const int VM_MEMORY_THRESHOLD_MB = 8000;

	if (rssMb > RSS_MEMORY_THRESHOLD_MB || vmMb > VM_MEMORY_THRESHOLD_MB) {
		OpenSync::Logger::info("⚠️ High memory usage detected (rss=" + std::to_string(rssMb) +
                 " MB, vm=" + std::to_string(vmMb) + " MB), shrinking lag buffers...");
	    if (globalKafkaProcessor) {
	        globalKafkaProcessor->shrinkLagBuffers();
	    }
	}
    }
}

std::pair<int, int> SystemMetricsUtils::getRSSandVMMemory() {
    std::ifstream statm("/proc/self/statm");
    size_t totalPages = 0, residentPages = 0;
    statm >> totalPages >> residentPages;

    long pageSize = sysconf(_SC_PAGESIZE); // thường là 4096

    int rssMb = static_cast<int>((residentPages * static_cast<size_t>(pageSize)) / (1024 * 1024));
    int vmMb  = static_cast<int>((totalPages * static_cast<size_t>(pageSize)) / (1024 * 1024));

    // Optional debug log (có thể bật nếu muốn kiểm tra)
    // Logger::debug("statm: totalPages=" + std::to_string(totalPages) + ", residentPages=" +
    //               std::to_string(residentPages) + ", pageSize=" + std::to_string(pageSize) +
    //               ", rssMb=" + std::to_string(rssMb) + ", vmMb=" + std::to_string(vmMb));

    return {rssMb, vmMb};
}

