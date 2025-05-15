#include "OracleSchemaCache.h"
#include "../db/oracle/OracleConnector.h"
#include "../dbconnector/DBConnectorFactory.h"
#include "../logger/Logger.h"
#include "../config/ConfigLoader.h"
#include "../metrics/MetricsExporter.h"
#include "../dbconnector/DBConnector.h"
#include "FilterConfigLoader.h"
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <atomic>
#include <thread>
#include <sstream>
#include <fstream>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>


void OracleSchemaCache::loadTableSchema(const std::string& fullTableName, const ConfigLoader& config) {
    size_t dotPos = fullTableName.find('.');
    if (dotPos == std::string::npos) {
	       OpenSync::Logger::error("❌ Invalid table name format (expect OWNER.TABLE): " + fullTableName);
         return;
    }

    std::string owner = fullTableName.substr(0, dotPos);
    std::string table = fullTableName.substr(dotPos + 1);
    OpenSync::Logger::info("🔄 Loading schema for table: " + fullTableName);

    try {
        std::unique_ptr<OracleConnector> connector = std::make_unique<OracleConnector>(
            config.getDBConfig("oracle", "host"),
            std::stoi(config.getDBConfig("oracle", "port")),
            config.getDBConfig("oracle", "user"),
            config.getDBConfig("oracle", "password"),
            config.getDBConfig("oracle", "service")
        );

        if (!connector->connect()) {
	           OpenSync::Logger::error("❌ Failed to connect to Oracle while fetching schema for: " + fullTableName);
             return;
        }

        //OpenSync::Logger::info("✅ Connected to Oracle successfully (for schema fetch)");
        auto columnInfo = connector->getFullColumnInfo(fullTableName);
	      OpenSync::Logger::info("👀 Fetched " + std::to_string(columnInfo.size()) + " columns from Oracle for " + fullTableName);

        {
      	    OpenSync::Logger::info("🔧 Calling mergeSchema() for table: " + fullTableName);
            //schemaCache[fullTableName] = columnInfo; //moi thay the bang merge
      	    mergeSchema(fullTableName, columnInfo);
      	    OpenSync::Logger::info("✅ Schema inserted into cache for: " + fullTableName);
        }

        std::stringstream ss;
        ss << "📦 Cached Oracle schema for " << fullTableName << ", cols: " << columnInfo.size();
	      OpenSync::Logger::info(ss.str());

        connector->disconnect();
        //OpenSync::Logger::info("🔌 Disconnected from Oracle.");
    } catch (const std::exception& ex) {
	      OpenSync::Logger::error("❌ Exception while loading Oracle schema for " + fullTableName + ": " + ex.what());
    }
}

void OracleSchemaCache::loadSchemaIfNeeded(const std::string& fullTableName, DBConnector& connector) {
    auto* oracle = dynamic_cast<OracleConnector*>(&connector);
    if (!oracle) {
	       OpenSync::Logger::error("❌ Invalid DBConnector type (not Oracle) for: " + fullTableName);
         return;
    }
    if (!oracle->isConnected()) {
	       OpenSync::Logger::error("❌ OracleConnector is not connected while preloading: " + fullTableName);
         return;
    }

    OpenSync::Logger::info("🔄 Loading schema for table: " + fullTableName);

    auto columnInfo = oracle->getFullColumnInfo(fullTableName);
    OpenSync::Logger::info("👀 Fetched " + std::to_string(columnInfo.size()) + " columns from Oracle for " + fullTableName);
    mergeSchema(fullTableName, columnInfo);
    OpenSync::Logger::info("✅ Schema inserted into cache for: " + fullTableName);
    lastAccessTime[fullTableName] = std::chrono::steady_clock::now();
}

void OracleSchemaCache::removeSchema(const std::string& fullTable) {
        std::lock_guard<std::mutex> lock(cacheMutex);
        auto it = schemaCache.find(fullTable);
        if (it != schemaCache.end()) {
            schemaCache.erase(it);
            OpenSync::Logger::info("Removed schema for " + fullTable + " from cache.");
        } else {
            OpenSync::Logger::warn("Schema for " + fullTable + " not found in cache.");
        }
}

const std::map<std::string, OracleColumnInfo>& OracleSchemaCache::getColumnTypes(const std::string& fullTableName) const {
    auto it = schemaCache.find(fullTableName);
    if (it != schemaCache.end()) return it->second;
    static std::map<std::string, OracleColumnInfo> empty;
    return empty;
}

std::map<std::string, OracleColumnInfo> OracleSchemaCache::getColumnInfo(const std::string& fullTableName) const {
    auto it = schemaCache.find(fullTableName);
    if (it != schemaCache.end()) return it->second;
    return {};
}

OracleSchemaCache& OracleSchemaCache::getInstance() {
    static OracleSchemaCache instance;
    return instance;
}

void OracleSchemaCache::mergeSchema(
    const std::string& fullTableName,
    const std::map<std::string, OracleColumnInfo>& newSchema) {

    //OpenSync::Logger::debug("🔍 Attempting to acquire cacheMutex for: " + fullTableName);
    std::stringstream ss;
            ss << "⏳ Acquiring lock for schema cache (thread: " << std::this_thread::get_id() << ")";
    OpenSync::Logger::info(ss.str());
    std::lock_guard<std::mutex> lock(cacheMutex);

    auto& existingSchema = schemaCache[fullTableName];

   // OpenSync::Logger::debug("🔍 ExistingSchema.empty() = " + std::string(existingSchema.empty() ? "true" : "false"));
   // OpenSync::Logger::debug("🔍 newSchema.size() = " + std::to_string(newSchema.size()));

    int driftCount = 0;

    if (existingSchema.empty()) {
          // ⚠️ Đây là lần đầu tiên nạp schema → lưu thẳng
          existingSchema = newSchema;
	        //for (const auto& [colName, colInfo] : newSchema) {
            //std::stringstream ss;
            //ss << "   ↪️ " << colName << " : " << colInfo.getFullTypeString()
            //   << (colInfo.nullable ? " NULLABLE" : " NOT NULL");
            //OpenSync::Logger::info(ss.str());
         //}
	       OpenSync::Logger::info("🆕 [Schema] Inserted new schema for " + fullTableName + ", cols: " + std::to_string(newSchema.size()));
         return;
    }

    // Tiếp tục xử lý merge như cũ nếu schema đã có
    std::unordered_set<std::string> seenCols;

    for (const auto& [colName, newCol] : newSchema) {
        seenCols.insert(colName);

        auto it = existingSchema.find(colName);
        if (it == existingSchema.end()) {
	    OpenSync::Logger::info("➕ [Schema] New column added: " + colName + " in " + fullTableName);

	    logSchemaDriftToFile(fullTableName, colName, "ADDED", nullptr, &newCol);

            existingSchema[colName] = newCol;
	    driftCount++;

        } else if (it->second != newCol) {
	    OpenSync::Logger::warn("⚠️ [Schema Drift] Column changed: " + colName + " in " + fullTableName);
	    OpenSync::Logger::warn("     Old: " + it->second.getFullTypeString());
	    OpenSync::Logger::warn("     New: " + newCol.getFullTypeString());

	    logSchemaDriftToFile(fullTableName, colName, "MODIFIED", &it->second, &newCol);
            it->second = newCol;
	    driftCount++;
        }
    }

    for (auto it = existingSchema.begin(); it != existingSchema.end(); ) {
        if (seenCols.find(it->first) == seenCols.end()) {
	    OpenSync::Logger::info("➖ [Schema Drift] Column removed: " + it->first + " in " + fullTableName);
	    logSchemaDriftToFile(fullTableName, it->first, "REMOVED", &it->second, nullptr);
            it = existingSchema.erase(it);
	    driftCount++;
        } else {
            ++it;
        }
    }

    if (driftCount > 0) {
	OpenSync::Logger::warn("🚨 Schema drift detected in table: " + fullTableName + " (changes: " + std::to_string(driftCount) + ")");
        MetricsExporter::getInstance().incrementCounter("oracle_schema_drift_total", {{"table", fullTableName}}, driftCount);
    }

    OpenSync::Logger::info("✅ [Schema] Merged schema for " + fullTableName + ", total cols: " + std::to_string(existingSchema.size()));
}

void OracleSchemaCache::startAutoRefreshThread(const ConfigLoader& config, int ttlSeconds) {
    static std::atomic<bool> threadStarted = false;

    if (threadStarted.exchange(true)) {
        OpenSync::Logger::info("🟡 Schema auto-refresh thread already started. Skipping...");
        return;
    }

    stopRefresh = false;  // ✅ reset flag nếu restart

    OpenSync::Logger::info("⏱️ Starting schema auto-refresh thread (interval: " + std::to_string(ttlSeconds) + "s)");

    refreshThread = std::thread([this, &config, ttlSeconds]() {
        try {
            while (!stopRefresh) {
                for (int i = 0; i < ttlSeconds && !stopRefresh; ++i) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
                if (stopRefresh) break;

                OpenSync::Logger::info("🔄 Auto-refreshing Oracle schema cache...");
                refreshAllSchemas(config);
            }
            OpenSync::Logger::info("✅ Oracle schema auto-refresh thread exited.");
        } catch (const std::exception& ex) {
            OpenSync::Logger::error("❌ Exception in schema auto-refresh thread: " + std::string(ex.what()));
        } catch (...) {
            OpenSync::Logger::fatal("💥 Unknown exception in schema auto-refresh thread");
        }
    });
    // ❌ Không detach — để stopAutoRefreshThread() join được
}


void OracleSchemaCache::refreshAllSchemas(const ConfigLoader& config) {
    std::lock_guard<std::mutex> lock(cacheMutex);
    for (const auto& [fullTableName, _] : schemaCache) {
	OpenSync::Logger::info("🔁 Refreshing schema for: " + fullTableName);
        loadTableSchema(fullTableName, config);
    }
}

void OracleSchemaCache::stopAutoRefreshThread() {
    stopRefresh = true;
    if (refreshThread.joinable()) {
        OpenSync::Logger::info("🛑 Waiting for schema auto-refresh thread to stop...");
        refreshThread.join();
    }
}

void OracleSchemaCache::logSchemaDriftToFile(const std::string& fullTableName,
                                             const std::string& colName,
                                             const std::string& action,
                                             const OracleColumnInfo* oldCol,
                                             const OracleColumnInfo* newCol) {
    std::ofstream file("logs/drift_audit.log", std::ios::app);
    if (!file.is_open()) return;

    rapidjson::Document d;
    d.SetObject();
    rapidjson::Document::AllocatorType& allocator = d.GetAllocator();

    //d.AddMember("timestamp", rapidjson::Value().SetString(OpenSync::Logger::getInstance().getTimestamp().c_str(), allocator), allocator);
    d.AddMember("timestamp", rapidjson::Value().SetString(OpenSync::Logger::getCurrentTimestamp().c_str(), allocator), allocator);
    d.AddMember("table", rapidjson::Value().SetString(fullTableName.c_str(), allocator), allocator);
    d.AddMember("column", rapidjson::Value().SetString(colName.c_str(), allocator), allocator);
    d.AddMember("action", rapidjson::Value().SetString(action.c_str(), allocator), allocator);

    if (oldCol) {
        d.AddMember("old_type", rapidjson::Value().SetString(oldCol->getFullTypeString().c_str(), allocator), allocator);
    }
    if (newCol) {
        d.AddMember("new_type", rapidjson::Value().SetString(newCol->getFullTypeString().c_str(), allocator), allocator);
    }

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    d.Accept(writer);

    file << buffer.GetString() << std::endl;
}

std::unique_ptr<OracleConnector> OracleSchemaCache::createTempOracleConnector(const ConfigLoader& config) {
    return std::make_unique<OracleConnector>(
        config.getDBConfig("oracle", "host"),
        std::stoi(config.getDBConfig("oracle", "port")),
        config.getDBConfig("oracle", "user"),
        config.getDBConfig("oracle", "password"),
        config.getDBConfig("oracle", "service")
    );
}

void OracleSchemaCache::preloadAllSchemas(const ConfigLoader& config) {
    OpenSync::Logger::info("🚀 Starting preload of all Oracle table schemas...");

    try {
        auto connector = OracleSchemaCache::createTempOracleConnector(config);
        if (!connector || !connector->connect()) {
	           OpenSync::Logger::fatal("❌ Failed to connect to Oracle for schema preload");
             return;
        }

         //OpenSync::Logger::info("✅ Connected to Oracle successfully for schema preload");
         const auto& filters = FilterConfigLoader::getInstance().getAllFilters();
	       OpenSync::Logger::info("📦 Total filters to preload: " + std::to_string(filters.size()));

        for (const auto& f : filters) {
            std::string fullTableName = f.owner + "." + f.table;
	          OpenSync::Logger::info("🔄 Preloading schema for table: " + fullTableName);
            try {
                OracleSchemaCache::getInstance().loadSchemaIfNeeded(fullTableName, *connector);
            } catch (const std::exception& ex) {
		            OpenSync::Logger::error("❌ Exception while loading schema for " + fullTableName + ": " + ex.what());
            }
        }

        connector->disconnect();
        //OpenSync::Logger::info("🔌 Disconnected Oracle after schema preload");

    } catch (const std::exception& ex) {
	     OpenSync::Logger::fatal("❌ Exception during preloadAllSchemas(): " + std::string(ex.what()));
    } catch (...) {
	     OpenSync::Logger::fatal("❌ Unknown exception during preloadAllSchemas()");
    }
}

void OracleSchemaCache::loadSchemaIfNeeded(const std::string& fullTableName, const ConfigLoader& config) {
    std::lock_guard<std::mutex> lock(cacheMutex);
    if (schemaCache.find(fullTableName) != schemaCache.end()) {
	OpenSync::Logger::info("✅ Oracle schema already cached for table: " + fullTableName);
	lastAccessTime[fullTableName] = std::chrono::steady_clock::now();
        return;
    }
    loadTableSchema(fullTableName, config);
}

void OracleSchemaCache::shrinkIfInactive(int ttlSeconds) {
	(void)ttlSeconds;
    /*std::lock_guard<std::mutex> lock(cacheMutex);
    auto now = std::chrono::steady_clock::now();

    int removed = 0;

    for (auto it = schemaCache.begin(); it != schemaCache.end(); ) {
        const std::string& fullTableName = it->first;

        auto accessIt = lastAccessTime.find(fullTableName);
        if (accessIt == lastAccessTime.end()) {
            ++it;
            continue;
        }

        auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - accessIt->second).count();

        if (duration > ttlSeconds) {
	    OpenSync::Logger::info("🧹 Removing stale schema from cache: " + fullTableName +
                     " (last accessed " + std::to_string(duration) + "s ago)");
            accessIt = lastAccessTime.erase(accessIt);
            it = schemaCache.erase(it);
            removed++;
        } else {
            ++it;
        }
    }

    if (removed > 0) {
	     OpenSync::Logger::info("✅ Removed " + std::to_string(removed) + " stale schemas from cache.");
    }*/
}

size_t OracleSchemaCache::estimateMemoryUsage() const {
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

size_t getOracleSchemaCacheMemoryUsage() {
    return OracleSchemaCache::getInstance().estimateMemoryUsage();
}
