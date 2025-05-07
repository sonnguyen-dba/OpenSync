#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <atomic>
#include <thread>
#include "PostgreSQLColumnInfo.h"
#include "../../config/ConfigLoader.h"

class PostgreSQLSchemaCache {
public:
    static PostgreSQLSchemaCache& getInstance();

    void loadTableSchema(const std::string& fullTableName, const ConfigLoader& config);
    void loadSchemaIfNeeded(const std::string& fullTableName, const ConfigLoader& config) {
        std::lock_guard<std::mutex> lock(cacheMutex);
        if (schemaCache.find(fullTableName) != schemaCache.end()) {
            lastAccessTime[fullTableName] = std::chrono::steady_clock::now();
            return;
        }
        loadTableSchema(fullTableName, config);
    }

    const std::unordered_map<std::string, PostgreSQLColumnInfo>& getColumnInfo(const std::string& fullTable) const {
        auto it = schemaCache.find(fullTable);
        if (it != schemaCache.end()) return it->second;
        static const std::unordered_map<std::string, PostgreSQLColumnInfo> empty;
        return empty;
    }

    void preloadAllSchemas(const ConfigLoader& config);
    void startAutoRefreshThread(const ConfigLoader& config, int intervalSeconds);
    void stopAutoRefreshThread();
    std::optional<PostgreSQLColumnInfo> getColumnInfo(const std::string& fullTable, const std::string& column) const;

    size_t estimateMemoryUsage() const;

private:
    PostgreSQLSchemaCache() = default;
    ~PostgreSQLSchemaCache() = default;
    PostgreSQLSchemaCache(const PostgreSQLSchemaCache&) = delete;
    PostgreSQLSchemaCache& operator=(const PostgreSQLSchemaCache&) = delete;

    void refreshAllSchemas(const ConfigLoader& config);
    void mergeSchema(const std::string& fullTableName,
                     const std::unordered_map<std::string, PostgreSQLColumnInfo>& newSchema);
    void logSchemaDriftToFile(const std::string& fullTable,
                              const std::string& col,
                              const std::string& action,
                              const PostgreSQLColumnInfo* oldCol,
                              const PostgreSQLColumnInfo* newCol);

    mutable std::mutex cacheMutex;
    std::unordered_map<std::string, std::unordered_map<std::string, PostgreSQLColumnInfo>> schemaCache;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> lastAccessTime;

    std::thread refreshThread;
    std::atomic<bool> stopRefresh{false};
};

// Global metric hook
size_t getPostgreSQLSchemaCacheMemoryUsage();
