#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <atomic>
#include <mutex>
#include <optional>
#include <thread>
#include <filesystem>
#include <rapidjson/document.h>
#include "../common/FilterEntry.h"
#include "../config/ConfigLoader.h"
#include "../schema/OracleColumnInfo.h"
#include "../sqlbuilder/SQLBuilderBase.h"

struct pair_hash {
    template <class T1, class T2>
    std::size_t operator()(const std::pair<T1, T2>& p) const {
        return std::hash<T1>()(p.first) ^ (std::hash<T2>()(p.second) << 1);
    }
};

class KafkaConsumer;

class KafkaProcessor {
public:
    //KafkaProcessor();
    KafkaProcessor(ConfigLoader& config);
    ~KafkaProcessor();

    //Gọi từ main để khởi động cleanup thread
    void startDedupCleanup();
    void stopDedupCleanup();  //(nếu muốn)

    void startGlobalDedupCleanup();
    void stopGlobalDedupCleanup();

    void addFilter(const FilterEntry& filter);
    void setMapping(const std::unordered_map<std::string, std::string>& mappingConfig);
    void loadFilterConfig(const std::string& configPath);
    bool isCurrentlyReloading();
    //void startAutoReload(const std::string& configPath);
    void startAutoReload(const std::string& configPath, KafkaConsumer* consumer = nullptr);
    void printPartitionOffset(int partition, int64_t offset, int64_t timestamp, const std::string& table);

    std::unordered_map<std::string, std::vector<std::string>> processMessageByTable(
        const std::string& jsonMessage, int partition, int64_t offset, int64_t timestamp);

    std::string buildInsertSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data);
    std::string buildUpdateSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data, const std::string& primaryKey);
    std::string buildDeleteSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data, const std::string& primaryKey);

    void setKafkaTopic(const std::string& topic);

    //Truy xuất Oracle PK index hint nếu có
    std::string getPKIndexHint(const std::string& schema, const std::string& table);
    ConfigLoader& getConfig() { return config; }

    size_t estimateDedupCacheMemory();
    std::unordered_map<std::pair<std::string, int>, double, pair_hash> getTotalLagByPartition() const;

    void clearLagBuffers();
    void shrinkLagBuffers(int maxAgeSeconds = 60);
    void shrinkLagBuffersIfOversized();

    bool enableISODebugLog = false;

    void registerSQLBuilder(const std::string& dbType, std::unique_ptr<SQLBuilderBase> builder);

private:
    std::optional<FilterEntry> matchFilter(const std::string& owner, const std::string& table);
    void updateProcessingRate();

    std::vector<FilterEntry> filters;
    std::unordered_map<std::string, std::string> mapping;
    std::unordered_map<std::string, std::string> warnedTables;

    std::mutex filterMutex;
    std::mutex dedupMutex;
    std::mutex filterReloadMutex;

    ConfigLoader& config;
    std::atomic<bool> isReloading{false};
    std::atomic<bool> stopReloading{false};

    std::atomic<int64_t> messagesProcessed;
    std::atomic<int64_t> lastUpdateTime;
    std::filesystem::file_time_type lastModifiedTime;

    std::thread reloadThread;
    std::thread metricUpdateThread;


    std::unordered_map<std::string, std::chrono::steady_clock::time_point> recentPrimaryKeyCache;
    std::unordered_map<std::string, std::unordered_map<std::string, std::chrono::steady_clock::time_point>> recentPrimaryKeyCachePerTable;
    std::chrono::seconds dedupTTL = std::chrono::seconds(10);

    mutable std::mutex lagMutex; // Protects lag tracking
    std::unordered_map<std::string, std::vector<double>> kafkaLagTableMs; // tableKey -> list of lag
    std::unordered_map<std::pair<std::string, int>, std::vector<double>, pair_hash> kafkaLagByPartition; // (topic, partition) -> lag

    std::string kafkaTopic; // Đặt khi khởi tạo nếu cần

    std::thread lagUpdateThread;
    void updateKafkaLagMetrics();

    std::unordered_map<std::string, std::chrono::steady_clock::time_point> recentGlobalPKCache;
    std::mutex globalCacheMutex;

    // 👇 Cleanup thread
    std::thread cleanupThread;
    std::atomic<bool> stopCleanup{false};

    std::thread globalDedupCleanupThread;
    std::atomic<bool> stopGlobalCleanup{false};

    void shrinkLagBuffersIfNeeded(
    std::unordered_map<std::string, std::vector<double>>& tableLags,
    std::unordered_map<std::pair<std::string, int>, std::vector<double>, pair_hash>& partitionLags,
    size_t maxSamples = 1000);

    // Thêm vào private:
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> tableLagLastUpdate;
    std::unordered_map<std::pair<std::string, int>, std::chrono::steady_clock::time_point, pair_hash> partitionLagLastUpdate;

    void shrinkLagBuffers(const std::string& tableKey, const std::pair<std::string, int>& tp);

    std::unordered_map<std::string, std::unique_ptr<SQLBuilderBase>> sqlBuilders;

};
