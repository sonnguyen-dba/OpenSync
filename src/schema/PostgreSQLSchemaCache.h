#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <vector>
#include "../reader/ConfigLoader.h"
#include "PostgreSQLColumnInfo.h"

class PostgreSQLConnector;

struct PostgreSQLSchemaCacheEntry {
    std::unordered_map<std::string, PostgreSQLColumnInfo> columns;
    std::chrono::steady_clock::time_point lastAccess;
};

class PostgreSQLSchemaCache {
public:
    static PostgreSQLSchemaCache& getInstance();

    const std::unordered_map<std::string, PostgreSQLColumnInfo>&
    getSchema(const std::string& fullTableName);

    void loadSchemaIfNeeded(const std::string& fullTableName, const ConfigLoader& config);
    void loadSchemaIfNeeded(const std::string& fullTableName, PostgreSQLConnector& connector);

    void preloadAllSchemas(const ConfigLoader& config);
    void autoRefreshSchemas(const ConfigLoader& config, int intervalSeconds);
    void shrinkInactiveSchemas(int maxAgeSeconds);

    bool hasColumn(const std::string& fullTableName, const std::string& column);
    PostgreSQLColumnInfo getColumnInfo(const std::string& fullTableName, const std::string& column);
    std::unordered_map<std::string, PostgreSQLColumnInfo> getColumnInfo(const std::string& fullTableName);
    std::vector<std::string> getPrimaryKeys(const std::string& fullTableName);


private:
    PostgreSQLSchemaCache() = default;
    void loadSchemaInternal(const std::string& fullTableName, PostgreSQLConnector& connector);

    std::unordered_map<std::string, PostgreSQLSchemaCacheEntry> cache;
    std::mutex cacheMutex;
};

