#pragma once
#include <unordered_map>
#include <vector>
#include <string>
#include <chrono>
#include <mutex>
#include <thread>
#include <atomic>
#include <librdkafka/rdkafka.h>

class BufferGCManager {
public:
    BufferGCManager(
        std::unordered_map<std::string, std::vector<std::string>>& tableBuffer,
        std::unordered_map<std::string, std::vector<rd_kafka_message_t*>>& msgBuffer,
        std::unordered_map<std::string, std::chrono::steady_clock::time_point>& lastFlushTime,
        int shrinkIntervalSec = 5,
        int inactiveThresholdSec = 60
    );

    void start();
    void stop();
    static void start(std::unordered_map<std::string, std::pair<std::vector<std::string>, std::chrono::steady_clock::time_point>>& tableSQLBuffer,
                      std::mutex& bufferMutex, int ttlSeconds = 60);

private:
    void shrinkBuffersIfNeeded();

    std::unordered_map<std::string, std::vector<std::string>>& tableBufferRef;
    std::unordered_map<std::string, std::vector<rd_kafka_message_t*>>& msgBufferRef;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point>& lastFlushRef;

    int shrinkIntervalSec;
    int inactiveThresholdSec;

    std::atomic<bool> stopFlag{false};
    std::thread gcThread;
    std::mutex gcMutex;
};
