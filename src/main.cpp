#include <thread>
#include <vector>
#include <atomic>
#include <csignal>
#include <chrono>
#include "app/AppInitializer.h"
#include "writer/CheckpointManager.h"
//#include "metrics/SystemMetricsUtils.h"
#include "thread/workerthread/WorkerThread.h"
#include "thread/dbwriterthread/DBWriterThread.h"
#include "thread/KafkaConsumerThread.h"
#include "schema/OracleSchemaCache.h"
#include "logger/Logger.h"

size_t batchSize = 0;
std::atomic<bool> shouldShutdown{false};  // ✅ Global shutdown flag

void signalHandler(int sig) {
    (void)sig;
    OpenSync::Logger::warn("⚠️ Received shutdown signal.");
    shouldShutdown = true;
}

int main() {
    std::set_terminate([] {
        OpenSync::Logger::fatal("💥 std::terminate called! Uncaught exception?");
        std::abort();
    });

    OpenSync::Logger::info("🚀 Starting Data Sync System...");

    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    auto app = AppInitializer::initialize("config/config.json");
    if (!app) return 1;

    auto& config    = *app->config;
    auto& processor = *app->processor;
    auto& consumer  = *app->consumer;
    auto& writeData = *app->writeData;
    auto& metrics   = *app->metrics;

    batchSize = config.getInt("batch_size", 100);
    int batchFlushIntervalMs = config.getInt("batch_flush_interval_ms", 1000);
    int numWorkers = config.getInt("num_workers", 4);
    int numDBWriters = config.getInt("num_db_writers", 1);

    OpenSync::Logger::info("batch_size = " + std::to_string(batchSize));
    OpenSync::Logger::info("batch_flush_interval_ms = " + std::to_string(batchFlushIntervalMs));
    OpenSync::Logger::info("num_workers = " + std::to_string(numWorkers));
    OpenSync::Logger::info("num_db_writers = " + std::to_string(numDBWriters));

    // Start metrics server
    std::thread metricsThread([&metrics]() { metrics.start(); });

    // Start system metrics
    //std::thread systemMetricsThread(SystemMetricsUtils::backgroundMetricsThread);

    // Start CheckpointManager
    auto& checkpointMgr = CheckpointManager::getInstance("checkpoint/checkpoints.txt");
    checkpointMgr.startAutoFlush(300);

    // Kafka Consumer thread
    std::thread kafkaThread(kafkaConsumerThread, std::ref(consumer), std::ref(shouldShutdown));

    // Worker threads
    std::vector<std::thread> workerThreads;
    for (int i = 0; i < numWorkers; ++i) {
        workerThreads.emplace_back(workerThread, std::ref(processor), batchFlushIntervalMs, std::ref(shouldShutdown));
    }

    // DB Writer threads
    std::vector<std::thread> dbWriterThreads;
    std::string dbType = config.getConfig("db_type", "oracle");
    for (int i = 0; i < numDBWriters; ++i) {
    	dbWriterThreads.emplace_back(dbWriterThread, std::ref(writeData), std::ref(consumer), dbType, std::ref(shouldShutdown));
    }


    OpenSync::Logger::info("✅ All threads started. Total: " + std::to_string(1 + numWorkers + numDBWriters + 2));

    // Wait for shutdown signal
    while (!shouldShutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    OpenSync::Logger::info("🛑 Stopping Data Sync System...");

    // Shutdown sequence
    if (kafkaThread.joinable()) kafkaThread.join();
    for (auto& t : workerThreads) if (t.joinable()) t.join();
    for (auto& t : dbWriterThreads) if (t.joinable()) t.join();
    if (metricsThread.joinable()) metricsThread.join();
    //if (systemMetricsThread.joinable()) systemMetricsThread.join();

    checkpointMgr.flushToDisk();
    checkpointMgr.stopAutoFlush();
    OracleSchemaCache::getInstance().stopAutoRefreshThread();

    OpenSync::Logger::info("🎯 Data Sync System stopped cleanly.");
    return 0;
}

