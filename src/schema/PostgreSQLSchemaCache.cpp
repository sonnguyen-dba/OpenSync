#include "PostgreSQLSchemaCache.h"
#include "PostgreSQLColumnInfo.h"
#include "../../db/postgresql/PostgreSQLConnector.h"
#include "../../config/ConfigLoader.h"
#include "../../logger/Logger.h"
#include "../../utils/SQLUtils.h"
#include "../../metrics/MetricsExporter.h"
#include "../../config/FilterConfigLoader.h"
#include <unordered_set>
#include <thread>
#include <fstream>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

PostgreSQLSchemaCache& PostgreSQLSchemaCache::getInstance() {
    static PostgreSQLSchemaCache instance;
    return instance;
}

void PostgreSQLSchemaCache::loadTableSchema(const std::string& fullTableName, const ConfigLoader& config) {
    OpenSync::Logger::info("🔄 Loading PostgreSQL schema for table: " + fullTableName);

    try {
        auto connector = std::make_unique<PostgreSQLConnector>(
            config.getDBConfig("postgresql", "host"),
            std::stoi(config.getDBConfig("postgresql", "port")),
            config.getDBConfig("postgresql", "user"),
            config.getDBConfig("postgresql", "password"),
            config.getDBConfig("postgresql", "dbname")
        );

        if (!connector->connect()) {
            OpenSync::Logger::error("❌ Failed to connect to PostgreSQL for schema: " + fullTableName);
            return;
        }

        auto colInfo = connector->getFullColumnInfo(fullTableName);
        OpenSync::Logger::info("👀 Fetched " + std::to_string(colInfo.size()) + " columns for " + fullTableName);
        mergeSchema(fullTableName, colInfo);
        connector->disconnect();
    } catch (const std::exception& ex) {
        OpenSync::Logger::error("❌ Exception loading schema for " + fullTableName + ": " + ex.what());
    }
}

void PostgreSQLSchemaCache::mergeSchema(const std::string& fullTable,
    const std::unordered_map<std::string, PostgreSQLColumnInfo>& newSchema) {
    std::lock_guard<std::mutex> lock(cacheMutex);
    std::string lowerTable = SQLUtils::toLower(fullTable);
    auto& existing = schemaCache[lowerTable];
    std::unordered_set<std::string> seenCols;
    int driftCount = 0;

    for (const auto& [col, newCol] : newSchema) {
        std::string lowerCol = SQLUtils::toLower(col);
        seenCols.insert(lowerCol);
        auto it = existing.find(lowerCol);
        if (it == existing.end()) {
            OpenSync::Logger::info("➕ [Schema] Added column: " + col + " to " + fullTable);
            logSchemaDriftToFile(fullTable, col, "ADDED", nullptr, &newCol);
            existing[lowerCol] = newCol;
            driftCount++;
        } else if (it->second != newCol) {
            OpenSync::Logger::warn("⚠️ [Schema Drift] Modified column: " + col);
            logSchemaDriftToFile(fullTable, col, "MODIFIED", &it->second, &newCol);
            it->second = newCol;
            driftCount++;
        }
    }

    for (auto it = existing.begin(); it != existing.end();) {
        if (seenCols.find(it->first) == seenCols.end()) {
            OpenSync::Logger::info("➖ [Schema] Removed column: " + it->first);
            logSchemaDriftToFile(fullTable, it->first, "REMOVED", &it->second, nullptr);
            it = existing.erase(it);
            driftCount++;
        } else {
            ++it;
        }
    }

    if (driftCount > 0) {
        OpenSync::Logger::warn("🚨 Schema drift detected in " + fullTable);
        MetricsExporter::getInstance().incrementCounter("postgresql_schema_drift_total", {{"table", fullTable}}, driftCount);
    }
}

void PostgreSQLSchemaCache::logSchemaDriftToFile(const std::string& fullTable,
                                                 const std::string& col,
                                                 const std::string& action,
                                                 const PostgreSQLColumnInfo* oldCol,
                                                 const PostgreSQLColumnInfo* newCol) {
    std::ofstream f("logs/pg_drift_audit.log", std::ios::app);
    if (!f.is_open()) return;

    rapidjson::Document d;
    d.SetObject();
    auto& alloc = d.GetAllocator();
    d.AddMember("timestamp", rapidjson::Value().SetString(OpenSync::Logger::getCurrentTimestamp().c_str(), alloc), alloc);
    d.AddMember("table", rapidjson::Value().SetString(fullTable.c_str(), alloc), alloc);
    d.AddMember("column", rapidjson::Value().SetString(col.c_str(), alloc), alloc);
    d.AddMember("action", rapidjson::Value().SetString(action.c_str(), alloc), alloc);
    if (oldCol) d.AddMember("old_type", rapidjson::Value().SetString(oldCol->getFullTypeString().c_str(), alloc), alloc);
    if (newCol) d.AddMember("new_type", rapidjson::Value().SetString(newCol->getFullTypeString().c_str(), alloc), alloc);

    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    d.Accept(writer);
    f << buf.GetString() << std::endl;
}

void PostgreSQLSchemaCache::preloadAllSchemas(const ConfigLoader& config) {
    OpenSync::Logger::info("🚀 Preloading PostgreSQL schemas...");
    auto filters = FilterConfigLoader::getInstance().getAllFilters();
    for (const auto& f : filters) {
        std::string fullTable = f.owner + "." + f.table;
        loadTableSchema(fullTable, config);
    }
}

void PostgreSQLSchemaCache::startAutoRefreshThread(const ConfigLoader& config, int intervalSeconds) {
    static std::atomic<bool> started{false};
    if (started.exchange(true)) {
        OpenSync::Logger::info("🟡 PostgreSQL schema auto-refresh already running.");
        return;
    }

    stopRefresh = false;
    refreshThread = std::thread([this, &config, intervalSeconds]() {
        while (!stopRefresh) {
            for (int i = 0; i < intervalSeconds && !stopRefresh; ++i)
                std::this_thread::sleep_for(std::chrono::seconds(1));
            if (stopRefresh) break;

            OpenSync::Logger::info("🔄 Auto-refreshing PostgreSQL schema cache...");
            refreshAllSchemas(config);
        }
        OpenSync::Logger::info("✅ PostgreSQL schema auto-refresh exited.");
    });
}

void PostgreSQLSchemaCache::refreshAllSchemas(const ConfigLoader& config) {
    std::lock_guard<std::mutex> lock(cacheMutex);
    for (const auto& [table, _] : schemaCache) {
        loadTableSchema(table, config);
    }
}

void PostgreSQLSchemaCache::stopAutoRefreshThread() {
    stopRefresh = true;
    if (refreshThread.joinable()) {
        OpenSync::Logger::info("🛑 Stopping PostgreSQL schema auto-refresh thread...");
        refreshThread.join();
    }
}

size_t PostgreSQLSchemaCache::estimateMemoryUsage() const {
    std::lock_guard<std::mutex> lock(cacheMutex);
    size_t total = 0;
    for (const auto& [table, cols] : schemaCache) {
        total += table.capacity();
        for (const auto& [col, info] : cols) {
            total += col.capacity();
            total += info.dataType.capacity();
        }
    }
    return total;
}

std::optional<PostgreSQLColumnInfo> PostgreSQLSchemaCache::getColumnInfo(
    const std::string& schemaTable, const std::string& column) const {

    std::string lowerSchemaTable = SQLUtils::toLower(schemaTable);
    std::string lowerCol = SQLUtils::toLower(column);

    std::lock_guard<std::mutex> lock(cacheMutex);
    auto it = schemaCache.find(lowerSchemaTable);
    if (it != schemaCache.end()) {
        const auto& columns = it->second;
        auto colIt = columns.find(lowerCol);
        if (colIt != columns.end()) {
            return colIt->second;
        }
    }
    return std::nullopt;
}

size_t getPostgreSQLSchemaCacheMemoryUsage() {
    return PostgreSQLSchemaCache::getInstance().estimateMemoryUsage();
}
