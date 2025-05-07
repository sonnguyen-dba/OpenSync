#pragma once

#include <cstddef>
#include <map>
#include <string>
#include <utility>
#include <unordered_map>
#include <atomic>

struct pair_hash;

class KafkaProcessor;
class WriteDataToDB;

extern KafkaProcessor* globalKafkaProcessor;
extern WriteDataToDB* globalWriteDataToDB;

class SystemMetricsUtils {
public:
    static void backgroundMetricsThread();

    // Metrics gathering helpers
    static size_t getMemoryUsageOfDedupCache();
    static size_t getMemoryUsageOfSchemaCache();
    static size_t getMemoryUsageOfTableBuffer();
    static size_t getActiveTableCount();

    static void startBackgroundMetricsThread();
    static void stopBackgroundMetricsThread();

    static std::unordered_map<std::pair<std::string, int>, double, struct pair_hash> getPartitionLagMap();
private:
  
    static std::pair<int, int> getRSSandVMMemory();
    static std::atomic<bool> stopFlag;
};
