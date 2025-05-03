#pragma once
#include <string>
#include <unordered_map>
#include <mutex>
#include "../../db/postgresql/PostgreSQLConnector.h"
#include "../../config/ConfigLoader.h"
#include "PostgreSQLColumnInfo.h"

struct PostgreSQLSchemaCacheEntry {
    std::vector<PostgreSQLColumnInfo> columns;
    std::chrono::steady_clock::time_point lastAccess;
};

class PostgreSQLSchemaCache {
public:
    static PostgreSQLSchemaCache& getInstance();

    void loadSchemaIfNeeded(const std::string& fullTableName, const ConfigLoader& config);
    std::unordered_map<std::string, PostgreSQLColumnInfo> getColumnInfo(const std::string& fullTableName);
    void clear();
    void preloadAllSchemas(const ConfigLoader& config);

    void startAutoRefreshThread(const ConfigLoader& config, int refreshIntervalSec);
    void shrinkIfInactive(int inactiveSeconds);


private:
    PostgreSQLSchemaCache() = default;
    PostgreSQLSchemaCache(const PostgreSQLSchemaCache&) = delete;
    PostgreSQLSchemaCache& operator=(const PostgreSQLSchemaCache&) = delete;

    std::unordered_map<std::string, std::unordered_map<std::string, PostgreSQLColumnInfo>> cache;
    std::mutex cacheMutex;

//    std::unordered_map<std::string, std::vector<PostgreSQLColumnInfo>> schemaCache;
    std::unordered_map<std::string, PostgreSQLSchemaCacheEntry> schemaCache;

    std::mutex mutex_;
    std::mutex mutex;
};
