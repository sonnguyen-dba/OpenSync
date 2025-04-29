#include "DBWriterThread.h"
#include "../../utils/MutexRegistry.h"
#include "../../metrics/MetricsExporter.h"
#include "../../logger/Logger.h"
#include "../../WriteDataToDB/WriteDataToDB.h"
#include "../../kafka/KafkaConsumer.h"
#include "../../common/Queues.h"
#include <chrono>
#include <sstream>
#include <atomic>

static constexpr int maxIdleSeconds = 10;

void dbWriterThread(WriteDataToDB& writeData, KafkaConsumer& consumer, const std::string& dbType, std::atomic<bool>& shouldShutdown) {
    while (!shouldShutdown) {
        std::tuple<std::string, TableBatch> item;

        {
            std::stringstream ss;
            ss << "Waiting for data in dbWriteQueue...";
            Logger::debug(ss.str());
        }
        bool hasData = dbWriteQueue.try_pop(item, std::chrono::seconds(maxIdleSeconds));
        if (!hasData) {
            std::stringstream ss;
            ss << "No new data in queue after " << maxIdleSeconds << " seconds, flushing remaining batches...";
            Logger::debug(ss.str());
            while (dbWriteQueue.try_pop(item)) {
                auto& [tableKey, batch] = item;
                {
                    std::stringstream ss;
                    ss << "Flushing batch for table: " << tableKey;
                    Logger::debug(ss.str());
                }

                auto tableMtx = getTableMutex(tableKey);
                std::lock_guard<std::mutex> lock(*tableMtx);

                std::atomic<int> totalRowsWritten{0};
                std::atomic<int> totalBatches{0};
                auto startTime = std::chrono::steady_clock::now();

                MetricsExporter::getInstance().incrementGauge("active_tables", tableKey);
                auto start = std::chrono::high_resolution_clock::now();

                bool success = writeData.writeBatchToDB(dbType, batch.sqls, tableKey);

                auto end = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double, std::milli> elapsed = end - start;

                MetricsExporter::getInstance().setMetric("db_write_time_ms", elapsed.count(), { {"table", tableKey} });
                MetricsExporter::getInstance().setMetric("db_batch_size", batch.sqls.size(), { {"table", tableKey} });

                std::string status = success ? "success" : "failure";
                MetricsExporter::getInstance().incrementCounter("db_batch_status", { {"table", tableKey}, {"status", status} });

                if (success) {
                    for (auto* msg : batch.messages) {
                        consumer.commitOffset(msg);
                        rd_kafka_message_destroy(msg);
                    }

                    MetricsExporter::getInstance().incrementCounter("table_throughput_rows_total", {{"table", tableKey}}, batch.sqls.size());

                    totalRowsWritten += batch.sqls.size();
                    totalBatches++;

                    auto now = std::chrono::steady_clock::now();
                    auto elapsedSec = std::chrono::duration_cast<std::chrono::seconds>(now - startTime).count();

                    if (elapsedSec > 0) {
                        double throughput = static_cast<double>(totalRowsWritten) / elapsedSec;
                        double avgBatch = static_cast<double>(totalRowsWritten) / totalBatches;

                        MetricsExporter::getInstance().setMetric("total_rows_written", totalRowsWritten);
                        MetricsExporter::getInstance().setMetric("rows_per_sec", throughput);
                        MetricsExporter::getInstance().setMetric("avg_rows_per_batch", avgBatch);
                    }
                } else {
                    MetricsExporter::getInstance().incrementCounter("table_rows_rollback", {{"table", tableKey}}, batch.sqls.size());
                    for (auto* msg : batch.messages) {
                        rd_kafka_message_destroy(msg);
                    }
                }

                batch.messages.clear();
                MetricsExporter::getInstance().decrementGauge("active_tables", tableKey);

                std::stringstream ss;
                ss << "[Thread " << std::this_thread::get_id() << "] "
                   << (success ? "✅ Successfully" : "❌ Failed to")
                   << " wrote " << batch.sqls.size() << " queries to " << dbType
                   << " (table: " << tableKey << ") in " << elapsed.count() << " ms.";
                success ? Logger::info(ss.str()) : Logger::error(ss.str());
            }
            continue;
        }

        auto& [tableKey, batch] = item;
        {
            std::stringstream ss;
            ss << "Processing batch for table: " << tableKey;
            Logger::debug(ss.str());
        }

        auto tableMtx = getTableMutex(tableKey);
        std::lock_guard<std::mutex> lock(*tableMtx);

        std::atomic<int> totalRowsWritten{0};
        std::atomic<int> totalBatches{0};
        auto startTime = std::chrono::steady_clock::now();

        MetricsExporter::getInstance().incrementGauge("active_tables", tableKey);
        auto start = std::chrono::high_resolution_clock::now();

        bool success = writeData.writeBatchToDB(dbType, batch.sqls, tableKey);

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> elapsed = end - start;

        MetricsExporter::getInstance().setMetric("db_write_time_ms", elapsed.count(), { {"table", tableKey} });
        MetricsExporter::getInstance().setMetric("db_batch_size", batch.sqls.size(), { {"table", tableKey} });

        std::string status = success ? "success" : "failure";
        MetricsExporter::getInstance().incrementCounter("db_batch_status", { {"table", tableKey}, {"status", status} });

        if (success) {
            for (auto* msg : batch.messages) {
                consumer.commitOffset(msg);
                rd_kafka_message_destroy(msg);
            }

            MetricsExporter::getInstance().incrementCounter("table_throughput_rows_total", {{"table", tableKey}}, batch.sqls.size());

            totalRowsWritten += batch.sqls.size();
            totalBatches++;

            auto now = std::chrono::steady_clock::now();
            auto elapsedSec = std::chrono::duration_cast<std::chrono::seconds>(now - startTime).count();

            if (elapsedSec > 0) {
                double throughput = static_cast<double>(totalRowsWritten) / elapsedSec;
                double avgBatch = static_cast<double>(totalRowsWritten) / totalBatches;

                MetricsExporter::getInstance().setMetric("total_rows_written", totalRowsWritten);
                MetricsExporter::getInstance().setMetric("rows_per_sec", throughput);
                MetricsExporter::getInstance().setMetric("avg_rows_per_batch", avgBatch);
            }
        } else {
            MetricsExporter::getInstance().incrementCounter("table_rows_rollback", {{"table", tableKey}}, batch.sqls.size());
            for (auto* msg : batch.messages) {
                rd_kafka_message_destroy(msg);
            }
        }

        batch.messages.clear();
        MetricsExporter::getInstance().decrementGauge("active_tables", tableKey);

        std::stringstream ss;
        ss << "[Thread " << std::this_thread::get_id() << "] "
           << (success ? "✅ Successfully" : "❌ Failed to")
           << " wrote " << batch.sqls.size() << " queries to " << dbType
           << " (table: " << tableKey << ") in " << elapsed.count() << " ms.";
        success ? Logger::info(ss.str()) : Logger::error(ss.str());
    }

    // Flush tất cả batch khi shutdown
    {
        std::stringstream ss;
        ss << "Shutting down DB writer, flushing all remaining batches...";
        Logger::info(ss.str());
    }
    std::tuple<std::string, TableBatch> item;
    while (dbWriteQueue.try_pop(item)) {
        auto& [tableKey, batch] = item;
        {
            std::stringstream ss;
            ss << "Flushing batch for table: " << tableKey;
            Logger::debug(ss.str());
        }

        auto tableMtx = getTableMutex(tableKey);
        std::lock_guard<std::mutex> lock(*tableMtx);

        std::atomic<int> totalRowsWritten{0};
        std::atomic<int> totalBatches{0};
        auto startTime = std::chrono::steady_clock::now();

        MetricsExporter::getInstance().incrementGauge("active_tables", tableKey);
        auto start = std::chrono::high_resolution_clock::now();

        bool success = writeData.writeBatchToDB(dbType, batch.sqls, tableKey);

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> elapsed = end - start;

        MetricsExporter::getInstance().setMetric("db_write_time_ms", elapsed.count(), { {"table", tableKey} });
        MetricsExporter::getInstance().setMetric("db_batch_size", batch.sqls.size(), { {"table", tableKey} });

        std::string status = success ? "success" : "failure";
        MetricsExporter::getInstance().incrementCounter("db_batch_status", { {"table", tableKey}, {"status", status} });

        if (success) {
            for (auto* msg : batch.messages) {
                consumer.commitOffset(msg);
                rd_kafka_message_destroy(msg);
            }

            MetricsExporter::getInstance().incrementCounter("table_throughput_rows_total", {{"table", tableKey}}, batch.sqls.size());

            totalRowsWritten += batch.sqls.size();
            totalBatches++;

            auto now = std::chrono::steady_clock::now();
            auto elapsedSec = std::chrono::duration_cast<std::chrono::seconds>(now - startTime).count();

            if (elapsedSec > 0) {
                double throughput = static_cast<double>(totalRowsWritten) / elapsedSec;
                double avgBatch = static_cast<double>(totalRowsWritten) / totalBatches;

                MetricsExporter::getInstance().setMetric("total_rows_written", totalRowsWritten);
                MetricsExporter::getInstance().setMetric("rows_per_sec", throughput);
                MetricsExporter::getInstance().setMetric("avg_rows_per_batch", avgBatch);
            }
        } else {
            MetricsExporter::getInstance().incrementCounter("table_rows_rollback", {{"table", tableKey}}, batch.sqls.size());
            for (auto* msg : batch.messages) {
                rd_kafka_message_destroy(msg);
            }
        }

        batch.messages.clear();
        MetricsExporter::getInstance().decrementGauge("active_tables", tableKey);

        std::stringstream ss;
        ss << "[Thread " << std::this_thread::get_id() << "] "
           << (success ? "✅ Successfully" : "❌ Failed to")
           << " wrote " << batch.sqls.size() << " queries to " << dbType
           << " (table: " << tableKey << ") in " << elapsed.count() << " ms.";
        success ? Logger::info(ss.str()) : Logger::error(ss.str());
    }
}
