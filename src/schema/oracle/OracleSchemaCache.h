#pragma once

#include "OracleColumnInfo.h"
#include "../../config/ConfigLoader.h"
#include "../../db/oracle/OracleConnector.h"
#include <map>
#include <mutex>
#include <atomic>
#include <thread>
#include <string>

class DBConnector;

class OracleSchemaCache {
public:
    static OracleSchemaCache& getInstance();

    void loadSchemaIfNeeded(const std::string& fullTableName, const ConfigLoader& config);
    void loadSchemaIfNeeded(const std::string& fullTableName, DBConnector& connector);  // overload mới
    void removeSchema(const std::string& fullTable); // Added declaration

    const std::map<std::string, OracleColumnInfo>& getColumnTypes(const std::string& fullTableName) const;
    std::map<std::string, OracleColumnInfo> getColumnInfo(const std::string& fullTableName) const;
    void mergeSchema(const std::string& fullTableName,
                 const std::map<std::string, OracleColumnInfo>& newSchema);
    void startAutoRefreshThread(const ConfigLoader& config, int ttlSeconds);
    void refreshAllSchemas(const ConfigLoader& config);

    void stopAutoRefreshThread();

    void logSchemaDriftToFile(const std::string& fullTableName,
                          const std::string& colName,
                          const std::string& action,
                          const OracleColumnInfo* oldCol,
                          const OracleColumnInfo* newCol);

    void preloadAllSchemas(const ConfigLoader& config);
    static std::unique_ptr<OracleConnector> createTempOracleConnector(const ConfigLoader& config);
    void shrinkIfInactive(int ttlSeconds);
    size_t estimateMemoryUsage() const;
    size_t getOracleSchemaCacheMemoryUsage();

private:
    OracleSchemaCache() = default;
    void loadTableSchema(const std::string& fullTableName, const ConfigLoader& config);

    std::map<std::string, std::map<std::string, OracleColumnInfo>> schemaCache;
    mutable std::mutex cacheMutex;
    std::thread refreshThread;
    std::atomic<bool> stopRefresh{false};
    std::mutex driftLogMutex;

    std::unordered_map<std::string, std::chrono::steady_clock::time_point> lastAccessTime;
};
