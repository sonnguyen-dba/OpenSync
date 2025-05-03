#include "PostgreSQLSchemaCache.h"
#include "../config/FilterConfigLoader.h"
#include "../db/postgresql/PostgreSQLConnector.h"      // ✅ để tạo connector
#include "../logger/Logger.h"
#include <thread>
#include <sstream>

PostgreSQLSchemaCache& PostgreSQLSchemaCache::getInstance() {
    static PostgreSQLSchemaCache instance;
    return instance;
}

void PostgreSQLSchemaCache::loadSchemaIfNeeded(const std::string& fullTableName, const ConfigLoader& config) {
    std::lock_guard<std::mutex> lock(cacheMutex);
    if (cache.find(fullTableName) != cache.end()) return;

    Logger::info("📥 Loading PostgreSQL schema for table: " + fullTableName);

    PostgreSQLConnector connector(
        config.getDBConfig("postgresql", "host"),
        std::stoi(config.getDBConfig("postgresql", "port")),
        config.getDBConfig("postgresql", "user"),
        config.getDBConfig("postgresql", "password"),
        config.getDBConfig("postgresql", "database")
    );

    if (!connector.connect()) {
        Logger::error("❌ Failed to connect for schema load: " + fullTableName);
        return;
    }

    std::string schema = "public";
    std::string table = fullTableName;
    size_t dot = fullTableName.find('.');
    if (dot != std::string::npos) {
        schema = fullTableName.substr(0, dot);
        table = fullTableName.substr(dot + 1);
    }

    std::ostringstream query;
    query << "SELECT column_name, data_type, character_maximum_length, "
          << "numeric_precision, numeric_scale, is_nullable "
          << "FROM information_schema.columns "
          << "WHERE table_schema = '" << schema << "' AND table_name = '" << table << "'";

    PGresult* res = PQexec(connector.conn, query.str().c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        Logger::warn("⚠️ Failed to load schema for " + fullTableName + ": " + PQerrorMessage(connector.conn));
        PQclear(res);
        return;
    }

    int rows = PQntuples(res);
    std::unordered_map<std::string, PostgreSQLColumnInfo> colMap;

    for (int i = 0; i < rows; ++i) {
        PostgreSQLColumnInfo info;
        info.columnName = PQgetvalue(res, i, 0);
        info.dataType = PQgetvalue(res, i, 1);

        const char* maxLen = PQgetvalue(res, i, 2);
        const char* prec = PQgetvalue(res, i, 3);
        const char* scale = PQgetvalue(res, i, 4);
        const char* nullable = PQgetvalue(res, i, 5);

        info.charMaxLength = maxLen ? atoi(maxLen) : 0;
        info.numericPrecision = prec ? atoi(prec) : 0;
        info.numericScale = scale ? atoi(scale) : 0;
        info.nullable = (nullable && std::string(nullable) == "YES");

        colMap[info.columnName] = info;
    }

    PQclear(res);
    cache[fullTableName] = std::move(colMap);

    Logger::info("✅ Loaded schema for " + fullTableName + " with " + std::to_string(rows) + " columns");
}

std::unordered_map<std::string, PostgreSQLColumnInfo> PostgreSQLSchemaCache::getColumnInfo(const std::string& fullTableName) {
    std::lock_guard<std::mutex> lock(cacheMutex);
    if (cache.find(fullTableName) != cache.end()) {
        return cache[fullTableName];
    }
    return {};
}

void PostgreSQLSchemaCache::clear() {
    std::lock_guard<std::mutex> lock(cacheMutex);
    cache.clear();
}

void PostgreSQLSchemaCache::preloadAllSchemas(const ConfigLoader& config) {
    std::string host = config.getDBConfig("postgresql", "host");
    int port = std::stoi(config.getDBConfig("postgresql", "port"));
    std::string user = config.getDBConfig("postgresql", "user");
    std::string password = config.getDBConfig("postgresql", "password");
    std::string database = config.getDBConfig("postgresql", "dbname");

    PostgreSQLConnector connector(host, port, user, password, database);
    if (!connector.connect()) {
        Logger::error("❌ Failed to connect to PostgreSQL for schema preload");
        return;
    }

    for (const auto& entry : FilterConfigLoader::getInstance().getAllFilters()) {
        std::string fullTable = entry.owner + "." + entry.table;
        Logger::info("🔄 Preloading PostgreSQL schema for: " + fullTable);
        auto info = connector.getFullColumnInfo(fullTable);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            //schemaCache[fullTable] = std::move(info);
	    schemaCache[fullTable] = {
		 .columns = std::move(info),
		 .lastAccess = std::chrono::steady_clock::now()
	    };

        }
    }
}

void PostgreSQLSchemaCache::startAutoRefreshThread(const ConfigLoader& config, int refreshIntervalSec) {
    std::thread([this, &config, refreshIntervalSec] {
        while (true) {
            Logger::info("[PostgreSQLSchemaCache] 🔄 Auto-refreshing schema...");
            std::lock_guard<std::mutex> lock(mutex);
            for (auto& [table, cacheEntry] : schemaCache) {
                std::string refreshedTable = table;
                try {
                    PostgreSQLConnector conn(
                        config.getDBConfig("postgresql", "host"),
                        std::stoi(config.getDBConfig("postgresql", "port")),
                        config.getDBConfig("postgresql", "user"),
                        config.getDBConfig("postgresql", "password"),
                        config.getDBConfig("postgresql", "dbname")
                    );
                    if (conn.connect()) {
                        auto schema = conn.getFullColumnInfo(table);
                        //cacheEntry.columns = std::move(schema);
                        //cacheEntry.lastAccess = std::chrono::steady_clock::now();
			schemaCache[table] = PostgreSQLSchemaCacheEntry{
				.columns = std::move(schema),
				.lastAccess = std::chrono::steady_clock::now()
			};

                        Logger::debug("[PostgreSQLSchemaCache] ✅ Refreshed schema for " + table);
                    }
                } catch (const std::exception& e) {
                    Logger::error("[PostgreSQLSchemaCache] ❌ Error refreshing schema for " + table + ": " + e.what());
                }
            }
            std::this_thread::sleep_for(std::chrono::seconds(refreshIntervalSec));
        }
    }).detach();
}

void PostgreSQLSchemaCache::shrinkIfInactive(int inactiveSeconds) {
    std::lock_guard<std::mutex> lock(mutex);
    auto now = std::chrono::steady_clock::now();
    int removed = 0;
    for (auto it = schemaCache.begin(); it != schemaCache.end(); ) {
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.lastAccess).count();
        if (elapsed > inactiveSeconds) {
            Logger::info("[PostgreSQLSchemaCache] 🧹 Removing inactive schema: " + it->first);
            it = schemaCache.erase(it);
            removed++;
        } else {
            ++it;
        }
    }
    if (removed > 0) {
        Logger::info("[PostgreSQLSchemaCache] ✅ Removed " + std::to_string(removed) + " inactive schema entries.");
    }
}


