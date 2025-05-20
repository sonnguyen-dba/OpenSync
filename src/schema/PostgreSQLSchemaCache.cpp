#include "PostgreSQLSchemaCache.h"
#include "../reader/FilterConfigLoader.h"
#include "../db/postgresql/PostgreSQLConnector.h"
#include "../utils/SQLUtils.h"
#include "../logger/Logger.h"
#include <thread>
#include <algorithm>

PostgreSQLSchemaCache& PostgreSQLSchemaCache::getInstance() {
    static PostgreSQLSchemaCache instance;
    return instance;
}

void PostgreSQLSchemaCache::loadSchemaIfNeeded(const std::string& fullTableName, const ConfigLoader& config) {
    PostgreSQLConnector connector(
        config.getDBConfig("postgresql", "host"),
        config.getInt("postgresql.port", 5432),
        config.getDBConfig("postgresql", "user"),
        config.getDBConfig("postgresql", "password"),
        config.getDBConfig("postgresql", "dbname"),
        config.getDBConfig("postgresql", "sslmode"),
        config.getDBConfig("postgresql", "sslrootcert"),
        config.getDBConfig("postgresql", "sslcert"),
        config.getDBConfig("postgresql", "sslkey")
    );

    if (!connector.connect()) {
        OpenSync::Logger::warn("⚠️ Failed to connect to PostgreSQL for schema load: " + fullTableName);
        return;
    }

    loadSchemaInternal(fullTableName, connector);
}

void PostgreSQLSchemaCache::loadSchemaIfNeeded(const std::string& fullTableName, PostgreSQLConnector& connector) {
    std::string lowerName = SQLUtils::toLower(fullTableName);
    std::lock_guard<std::mutex> lock(cacheMutex);
    if (cache.find(lowerName) != cache.end()) {
        cache[lowerName].lastAccess = std::chrono::steady_clock::now();
        return;
    }

    loadSchemaInternal(lowerName, connector);
}

void PostgreSQLSchemaCache::loadSchemaInternal(const std::string& fullTableName, PostgreSQLConnector& connector) {
    auto columns = connector.getFullColumnInfo(fullTableName);
    if (columns.empty()) {
        OpenSync::Logger::warn("⚠️ PostgreSQLSchemaCache: Failed to load schema for: " + fullTableName);
        return;
    }

    std::string lowerName = SQLUtils::toLower(fullTableName);
    std::lock_guard<std::mutex> lock(cacheMutex);
    PostgreSQLSchemaCacheEntry entry;
    entry.columns = std::move(columns);
    entry.lastAccess = std::chrono::steady_clock::now();
    cache[lowerName] = std::move(entry);
    OpenSync::Logger::info("✅ PostgreSQLSchemaCache: Loaded and cached schema for: " + lowerName);
}

const std::unordered_map<std::string, PostgreSQLColumnInfo>&
PostgreSQLSchemaCache::getSchema(const std::string& fullTableName) {
    std::string lowerName = SQLUtils::toLower(fullTableName);
    std::lock_guard<std::mutex> lock(cacheMutex);
    return cache[lowerName].columns;
}

bool PostgreSQLSchemaCache::hasColumn(const std::string& fullTableName, const std::string& column) {
    std::string lowerName = SQLUtils::toLower(fullTableName);
    std::lock_guard<std::mutex> lock(cacheMutex);
    auto it = cache.find(lowerName);
    if (it == cache.end()) return false;
    return it->second.columns.find(column) != it->second.columns.end();
}

PostgreSQLColumnInfo PostgreSQLSchemaCache::getColumnInfo(const std::string& fullTableName, const std::string& column) {
    std::string lowerName = SQLUtils::toLower(fullTableName);
    std::lock_guard<std::mutex> lock(cacheMutex);
    auto it = cache.find(lowerName);
    if (it != cache.end() && it->second.columns.find(column) != it->second.columns.end()) {
        return it->second.columns.at(column);
    }
    return {}; // return default if not found
}

std::unordered_map<std::string, PostgreSQLColumnInfo>
PostgreSQLSchemaCache::getColumnInfo(const std::string& fullTableName) {
    std::string lowerName = SQLUtils::toLower(fullTableName);
    std::lock_guard<std::mutex> lock(cacheMutex);
    auto it = cache.find(lowerName);
    if (it != cache.end()) {
        return it->second.columns;
    } else {
        OpenSync::Logger::warn("⚠️ getColumnInfo: No schema cached for: " + fullTableName);
        return {};
    }
}

std::vector<std::string>
PostgreSQLSchemaCache::getPrimaryKeys(const std::string& fullTableName) {
    std::string lowerName = SQLUtils::toLower(fullTableName);
    std::vector<std::string> result;
    std::lock_guard<std::mutex> lock(cacheMutex);
    auto it = cache.find(lowerName);
    if (it == cache.end()) {
        OpenSync::Logger::warn("⚠️ getPrimaryKeys: No schema cached for: " + fullTableName);
        return {};
    }

    for (const auto& [col, info] : it->second.columns) {
        if (info.isPrimaryKey) {
            result.push_back(col);
        }
    }

    return result;
}

void PostgreSQLSchemaCache::preloadAllSchemas(const ConfigLoader& config) {
    auto filters = FilterConfigLoader::getInstance().getAllFilters();
    for (const auto& f : filters) {
        std::string fullTable = f.owner + "." + f.table;
        OpenSync::Logger::info("🔄 Loading PostgreSQL schema for table: " + fullTable);
        loadSchemaIfNeeded(fullTable, config);
    }
}

void PostgreSQLSchemaCache::autoRefreshSchemas(const ConfigLoader& config, int intervalSeconds) {
    std::thread([this, &config, intervalSeconds]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(intervalSeconds));
            preloadAllSchemas(config);
        }
    }).detach();
}

void PostgreSQLSchemaCache::shrinkInactiveSchemas(int maxAgeSeconds) {
    std::lock_guard<std::mutex> lock(cacheMutex);
    auto now = std::chrono::steady_clock::now();
    for (auto it = cache.begin(); it != cache.end();) {
        auto age = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.lastAccess).count();
        if (age > maxAgeSeconds) {
            OpenSync::Logger::info("🧹 Removing stale schema from cache: " + it->first);
            it = cache.erase(it);
        } else {
            ++it;
        }
    }
}

