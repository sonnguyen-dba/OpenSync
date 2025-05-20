#include "WorkerThread.h"
#include <unordered_map> 
#include <chrono>
#include "Queues.h"
#include "MetricsExporter.h"
#include "BufferGCManager.h"
#include "../../logger/Logger.h"

void workerThread(KafkaProcessor& processor, int batchFlushIntervalMs) {
    static thread_local std::unordered_map<std::string, std::vector<std::string>> tableBuffer;
    static thread_local std::unordered_map<std::string, std::vector<rd_kafka_message_t*>> msgBuffer;
    static thread_local std::unordered_map<std::string, std::chrono::steady_clock::time_point> lastFlushTime;
    static thread_local std::unique_ptr<BufferGCManager> gcManager = nullptr;
    static thread_local bool gcManagerStarted = false;

    if (!gcManagerStarted) {
        int shrinkInterval = 10;
        std::string shrinkIntervalStr = processor.getConfig().getConfig("shrink_interval_sec");
        if (!shrinkIntervalStr.empty()) {
            shrinkInterval = std::stoi(shrinkIntervalStr);
        }
	gcManager = std::make_unique<BufferGCManager>(tableBuffer, msgBuffer, lastFlushTime, shrinkInterval);

        gcManager->start();
        gcManagerStarted = true;
	Logger::info("🧹 .BufferGCManager started for thread" + std::to_string(reinterpret_cast<std::uintptr_t>(std::addressof(tableBuffer))));

    }

    while (true) {
        auto [message, partition, offset, timestamp, rawMsg] = kafkaMessageQueue.pop();
        auto start = std::chrono::high_resolution_clock::now();

        auto batchMap = processor.processMessageByTable(message, partition, offset, timestamp);
        auto now = std::chrono::steady_clock::now();

        for (auto& [tableKey, sqls] : batchMap) {
            if (sqls.empty()) continue;

            auto& buf = tableBuffer[tableKey];
            auto& msgs = msgBuffer[tableKey];
            buf.insert(buf.end(), sqls.begin(), sqls.end());
            msgs.push_back(rawMsg);

            if (lastFlushTime.find(tableKey) == lastFlushTime.end()) {
                lastFlushTime[tableKey] = now;
            }

            MetricsExporter::getInstance().incrementCounter("kafka_messages_consumed", { {"table", tableKey} });

            if (buf.size() >= static_cast<size_t>(batchSize) ||
                //std::chrono::duration_cast(now - lastFlushTime[tableKey]).count() >= batchFlushIntervalMs) {
		std::chrono::duration_cast<std::chrono::milliseconds>(now - lastFlushTime[tableKey]).count() >= batchFlushIntervalMs) {


                TableBatch batch;
                batch.sqls = std::move(buf);
                batch.messages = std::move(msgs);
                dbWriteQueue.push({tableKey, std::move(batch)});
                buf.clear();
                msgs.clear();
                lastFlushTime[tableKey] = now;
            }

            MetricsExporter::getInstance().setMetric("sql_queue_size", tableBuffer[tableKey].size(), { {"table", tableKey} });
        }

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration processingTime = end - start;
        MetricsExporter::getInstance().setMetric("kafka_processing_time_ms", processingTime.count());
    }
}

void startWorker(KafkaProcessor& processor, int batchFlushIntervalMs) {
    std::thread(workerThread, std::ref(processor), batchFlushIntervalMs).detach();
}
