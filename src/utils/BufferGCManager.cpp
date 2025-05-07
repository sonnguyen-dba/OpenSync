#include "BufferGCManager.h"
#include "../metrics/MetricsExporter.h"
#include "../logger/Logger.h"

BufferGCManager::BufferGCManager(
    std::unordered_map<std::string, std::vector<std::string>>& tableBuffer,
    std::unordered_map<std::string, std::vector<rd_kafka_message_t*>>& msgBuffer,
    std::unordered_map<std::string, std::chrono::steady_clock::time_point>& lastFlushTime,
    int shrinkIntervalSec,
    int inactiveThresholdSec
) : tableBufferRef(tableBuffer),
    msgBufferRef(msgBuffer),
    lastFlushRef(lastFlushTime),
    shrinkIntervalSec(shrinkIntervalSec),
    inactiveThresholdSec(inactiveThresholdSec)
{}

void BufferGCManager::start() {
    stopFlag = false;
    gcThread = std::thread([this]() {
        while (!stopFlag) {
            std::this_thread::sleep_for(std::chrono::seconds(shrinkIntervalSec));
            shrinkBuffersIfNeeded();
        }
    });
}

void BufferGCManager::stop() {
    stopFlag = true;
    if (gcThread.joinable()) {
        gcThread.join();
    }
}

void BufferGCManager::shrinkBuffersIfNeeded() {
    std::lock_guard<std::mutex> lock(gcMutex);
    auto now = std::chrono::steady_clock::now();
    int shrinkCount = 0;

    for (const auto& [tableKey, lastFlush] : lastFlushRef) {
        if (std::chrono::duration_cast<std::chrono::seconds>(now - lastFlush).count() > inactiveThresholdSec) {
            if (!tableBufferRef[tableKey].empty()) continue;

            tableBufferRef[tableKey].shrink_to_fit();
            msgBufferRef[tableKey].shrink_to_fit();

	          OpenSync::Logger::info("🧹 Shrunk buffer for inactive table: " + tableKey);
            MetricsExporter::getInstance().incrementCounter("buffer_shrink_total", {{"table", tableKey}});
            shrinkCount++;
        }
    }

    if (shrinkCount > 0) {
	       OpenSync::Logger::debug("🔄 Shrink cycle complete. Total tables shrunk: " + std::to_string(shrinkCount));
    }
}

void BufferGCManager::start(std::unordered_map<std::string, std::pair<std::vector<std::string>, std::chrono::steady_clock::time_point>>& tableSQLBuffer,
                            std::mutex& bufferMutex, int ttlSeconds) {
    std::thread([&]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            auto now = std::chrono::steady_clock::now();
            std::lock_guard<std::mutex> lock(bufferMutex);
            for (auto it = tableSQLBuffer.begin(); it != tableSQLBuffer.end();) {
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.second).count();
                if (elapsed >= ttlSeconds) {
                    std::string table = it->first;
                    //size_t oldCap = it->second.first.capacity();
		                OpenSync::Logger::info("♻️ GC table buffer: " + table + ", elapsed=" + std::to_string(elapsed) + "s, size=" + std::to_string(it->second.first.size()));
                    std::vector<std::string>().swap(it->second.first);
                    it = tableSQLBuffer.erase(it);

                    MetricsExporter::getInstance().incrementCounter("buffer_gc_shrink_total", {{"table", table}});
                } else {
                    ++it;
                }
            }
        }
    }).detach();
}
