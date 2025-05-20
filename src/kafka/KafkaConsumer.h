#ifndef KAFKA_CONSUMER_H
#define KAFKA_CONSUMER_H

#include <string>
#include <vector>
#include <librdkafka/rdkafka.h>
#include "../metrics/MetricsServer.h"
#include "../thread/ThreadSafeQueue.h"
#include "KafkaProcessor.h"
#include <unordered_set>
#include <filesystem>

namespace fs = std::filesystem;

class KafkaConsumer {
public:
    //KafkaConsumer(const std::string& brokers, const std::string& topic, const std::string& groupId);
    /*KafkaConsumer(const std::string& brokers, const std::string& topic,
                  const std::string& groupId, const std::string& reset, MetricsServer& metrics);*/

    KafkaConsumer(KafkaProcessor& processor, const std::string& brokers, const std::string& topic,
                  const std::string& groupId, const std::string& offsetReset, const std::string& enableAutoCommit, MetricsServer& metrics, const std::string& filterConfigPath);
    ~KafkaConsumer();

    // 🆕 Sửa đổi để trả về partition, offset, timestamp
    bool consumeMessage(std::string& message, int& partition, int64_t& offset, int64_t& timestamp, rd_kafka_message_t** rawMsg);

    //bool consumeMessage(std::string& message);
    void loadTableFilter(const std::string& configPath);  // 🔹 Thêm khai báo hàm loadTableFilter
    void startAutoReload(const std::string& configPath);
    void reloadTableFilter(const std::string& configPath);
    bool isTableFiltered(const std::string& owner, const std::string& table);  // 🔹 Thêm khai báo hàm isTableFiltered
    void printPartitionOffset(int partition, int64_t offset, int64_t timestamp, const std::string& table); // Hiển thị partition & offset
    void printFilteredTables(); 
    void commitOffset(rd_kafka_message_t* message);

    //bool isMessageProcessed(const std::string& message);
    //void markMessageProcessed(const std::string& message);
    //std::string computeMessageHash(const std::string& message);

    static void rebalanceCallback(rd_kafka_t* rk,
                              rd_kafka_resp_err_t err,
                              rd_kafka_topic_partition_list_t* partitions,
                              void* opaque);
    // 🆕 Hàm expose stopFlag
    std::atomic<bool>& getStopFlag() {
        return shouldShutdown;
    }

private:
    KafkaProcessor& processor;  // 🆕 Thêm tham chiếu đến KafkaProcessor
    std::string brokers;
    std::string topic;
    std::string groupId;

    std::atomic<bool> stopReloading;
    std::thread reloadThread;
    std::string filterConfigPath;
    //std::filesystem::file_time_type lastModifiedTime;
    fs::file_time_type lastModifiedTime = fs::last_write_time(fs::path(filterConfigPath));


    rd_kafka_t* consumer;
    rd_kafka_conf_t* conf;
    rd_kafka_topic_partition_list_t* topics;

    rd_kafka_t* getRawKafkaHandle() const { return consumer; }

    MetricsServer& metrics;  // 🆕 Thêm reference tới MetricsServer
    std::string enableAutoCommit = "false";
    std::unordered_set<std::string> tableFilter;  // 🔹 Lưu danh sách bảng cần lọc
    void reloadFilterConfigLoop(); //thread chay nen
    
    void initKafka(const std::string& offsetReset);
    std::atomic<bool> shouldShutdown = false;
};

#endif

