#include "OracleSchemaCache.h"
#include "../../db/oracle/OracleConnector.h"
#include "../../dbconnector/DBConnectorFactory.h"
#include "../../logger/Logger.h"
#include "../../config/ConfigLoader.h"
#include "../../metrics/MetricsExporter.h"
#include "../../dbconnector/DBConnector.h"
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
	Logger::error("❌ Invalid table name format (expect OWNER.TABLE): " + fullTableName);
        return;
    }

    std::string owner = fullTableName.substr(0, dotPos);
    std::string table = fullTableName.substr(dotPos + 1);
    Logger::info("🔄 Loading schema for table: " + fullTableName);

    try {
        std::unique_ptr<OracleConnector> connector = std::make_unique<OracleConnector>(
            config.getDBConfig("oracle", "host"),
            std::stoi(config.getDBConfig("oracle", "port")),
            config.getDBConfig("oracle", "user"),
            config.getDBConfig("oracle", "password"),
            config.getDBConfig("oracle", "service")
        );

        if (!connector->connect()) {
	          Logger::error("❌ Failed to connect to Oracle while fetching schema for: " + fullTableName);
            return;
        }

        //Logger::info("✅ Connected to Oracle successfully (for schema fetch)");
        auto columnInfo = connector->getFullColumnInfo(fullTableName);
	      Logger::info("👀 Fetched " + std::to_string(columnInfo.size()) + " columns from Oracle for " + fullTableName);

        {
	          Logger::debug("🔧 Calling mergeSchema() for table: " + fullTableName);
	          mergeSchema(fullTableName, columnInfo);
	          Logger::info("✅ Schema inserted into cache for: " + fullTableName);
        }

        std::stringstream ss;
        ss << "📦 Cached Oracle schema for " << fullTableName << ", cols: " << columnInfo.size();
	      Logger::info(ss.str());

        connector->disconnect();
        //Logger::info("🔌 Disconnected from Oracle.");
    } catch (const std::exception& ex) {
	      Logger::error("❌ Exception while loading Oracle schema for " + fullTableName + ": " + ex.what());
    }
}

void OracleSchemaCache::loadSchemaIfNeeded(const std::string& fullTableName, DBConnector& connector) {
    auto* oracle = dynamic_cast<OracleConnector*>(&connector);
    if (!oracle) {
	      Logger::error("❌ Invalid DBConnector type (not Oracle) for: " + fullTableName);
        return;
    }
    if (!oracle->isConnected()) {
	      Logger::error("❌ OracleConnector is not connected while preloading: " + fullTableName);
        return;
    }

    Logger::info("🔄 Loading schema for table: " + fullTableName);

    auto columnInfo = oracle->getFullColumnInfo(fullTableName);
    Logger::info("👀 Fetched " + std::to_string(columnInfo.size()) + " columns from Oracle for " + fullTableName);
    mergeSchema(fullTableName, columnInfo);
    Logger::debug("✅ Schema inserted into cache for: " + fullTableName);
    lastAccessTime[fullTableName] = std::chrono::steady_clock::now();
}

void OracleSchemaCache::removeSchema(const std::string& fullTable) {
        std::lock_guard<std::mutex> lock(cacheMutex);
        auto it = schemaCache.find(fullTable);
        if (it != schemaCache.end()) {
            schemaCache.erase(it);
            Logger::info("Removed schema for " + fullTable + " from cache.");
        } else {
            Logger::warn("Schema for " + fullTable + " not found in cache.");
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

    //Logger::debug("🔍 Attempting to acquire cacheMutex for: " + fullTableName);
    std::stringstream ss;
            ss << "⏳ Acquiring lock for schema cache (thread: " << std::this_thread::get_id() << ")";
    Logger::info(ss.str());
    std::lock_guard<std::mutex> lock(cacheMutex);

    auto& existingSchema = schemaCache[fullTableName];

   // Logger::debug("🔍 ExistingSchema.empty() = " + std::string(existingSchema.empty() ? "true" : "false"));
   // Logger::debug("🔍 newSchema.size() = " + std::to_string(newSchema.size()));

    int driftCount = 0;

    if (existingSchema.empty()) {
        //Đây là lần đầu tiên nạp schema → lưu thẳng
        existingSchema = newSchema;
	      //for (const auto& [colName, colInfo] : newSchema) {
            //std::stringstream ss;
            //ss << "   ↪️ " << colName << " : " << colInfo.getFullTypeString()
            //   << (colInfo.nullable ? " NULLABLE" : " NOT NULL");
            //Logger::info(ss.str());
        //}
	      Logger::info("🆕 [Schema] Inserted new schema for " + fullTableName + ", cols: " + std::to_string(newSchema.size()));
        return;
    }

    // Tiếp tục xử lý merge như cũ nếu schema đã có
    std::unordered_set<std::string> seenCols;

    for (const auto& [colName, newCol] : newSchema) {
        seenCols.insert(colName);

        auto it = existingSchema.find(colName);
        if (it == existingSchema.end()) {
	          Logger::info("➕ [Schema] New column added: " + colName + " in " + fullTableName);
	          logSchemaDriftToFile(fullTableName, colName, "ADDED", nullptr, &newCol);
            existingSchema[colName] = newCol;
	          driftCount++;
        } else if (it->second != newCol) {
	          Logger::warn("⚠️ [Schema Drift] Column changed: " + colName + " in " + fullTableName);
	          Logger::warn("     Old: " + it->second.getFullTypeString());
	          Logger::warn("     New: " + newCol.getFullTypeString());

	          logSchemaDriftToFile(fullTableName, colName, "MODIFIED", &it->second, &newCol);
            it->second = newCol;
	          driftCount++;
        }
    }

    for (auto it = existingSchema.begin(); it != existingSchema.end(); ) {
        if (seenCols.find(it->first) == seenCols.end()) {
	    Logger::info("➖ [Schema Drift] Column removed: " + it->first + " in " + fullTableName);
	    logSchemaDriftToFile(fullTableName, it->first, "REMOVED", &it->second, nullptr);
            it = existingSchema.erase(it);
	    driftCount++;
        } else {
            ++it;
        }
    }

    if (driftCount > 0) {
	Logger::warn("🚨 Schema drift detected in table: " + fullTableName + " (changes: " + std::to_string(driftCount) + ")");
        MetricsExporter::getInstance().incrementCounter("oracle_schema_drift_total", {{"table", fullTableName}}, driftCount);
    }

    Logger::info("✅ [Schema] Merged schema for " + fullTableName + ", total cols: " + std::to_string(existingSchema.size()));
}

void OracleSchemaCache::startAutoRefreshThread(const ConfigLoader& config, int ttlSeconds) {
    static std::atomic<bool> threadStarted = false;

    if (threadStarted.exchange(true)) {
        Logger::info("🟡 Schema auto-refresh thread already started. Skipping...");
        return;
    }

    stopRefresh = false;  //reset flag nếu restart

    Logger::info("⏱️ Starting schema auto-refresh thread (interval: " + std::to_string(ttlSeconds) + "s)");

    refreshThread = std::thread([this, &config, ttlSeconds]() {
        try {
            while (!stopRefresh) {
                for (int i = 0; i < ttlSeconds && !stopRefresh; ++i) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
                if (stopRefresh) break;

                Logger::info("🔄 Auto-refreshing Oracle schema cache...");
                refreshAllSchemas(config);
            }
            Logger::info("✅ Oracle schema auto-refresh thread exited.");
        } catch (const std::exception& ex) {
            Logger::error("❌ Exception in schema auto-refresh thread: " + std::string(ex.what()));
        } catch (...) {
            Logger::fatal("💥 Unknown exception in schema auto-refresh thread");
        }
    });
    // ❌ Không detach — để stopAutoRefreshThread() join được
}

void OracleSchemaCache::refreshAllSchemas(const ConfigLoader& config) {
    std::lock_guard<std::mutex> lock(cacheMutex);
    for (const auto& [fullTableName, _] : schemaCache) {
	      Logger::info("🔁 Refreshing schema for: " + fullTableName);
        loadTableSchema(fullTableName, config);
    }
}

void OracleSchemaCache::stopAutoRefreshThread() {
    stopRefresh = true;
    if (refreshThread.joinable()) {
        Logger::info("🛑 Waiting for schema auto-refresh thread to stop...");
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

    //d.AddMember("timestamp", rapidjson::Value().SetString(Logger::getInstance().getTimestamp().c_str(), allocator), allocator);
    d.AddMember("timestamp", rapidjson::Value().SetString(Logger::getCurrentTimestamp().c_str(), allocator), allocator);
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
    Logger::info("🚀 Starting preload of all Oracle table schemas...");

    try {
        auto connector = OracleSchemaCache::createTempOracleConnector(config);
        if (!connector || !connector->connect()) {
	          Logger::fatal("❌ Failed to connect to Oracle for schema preload");
            return;
        }

        //Logger::debug("✅ Connected to Oracle successfully for schema preload");

        const auto& filters = FilterConfigLoader::getInstance().getAllFilters();
	      Logger::info("📦 Total filters to preload: " + std::to_string(filters.size()));

        for (const auto& f : filters) {
            std::string fullTableName = f.owner + "." + f.table;
	          Logger::info("🔄 Preloading schema for table: " + fullTableName);
            try {
                OracleSchemaCache::getInstance().loadSchemaIfNeeded(fullTableName, *connector);
            } catch (const std::exception& ex) {
		            Logger::error("❌ Exception while loading schema for " + fullTableName + ": " + ex.what());
            }
        }

        connector->disconnect();
        //Logger::debug("🔌 Disconnected Oracle after schema preload");

    } catch (const std::exception& ex) {
	     Logger::fatal("❌ Exception during preloadAllSchemas(): " + std::string(ex.what()));
    } catch (...) {
	     Logger::fatal("❌ Unknown exception during preloadAllSchemas()");
    }
}

void OracleSchemaCache::loadSchemaIfNeeded(const std::string& fullTableName, const ConfigLoader& config) {
    std::lock_guard<std::mutex> lock(cacheMutex);
    if (schemaCache.find(fullTableName) != schemaCache.end()) {
	      Logger::info("✅ Oracle schema already cached for table: " + fullTableName);
	      lastAccessTime[fullTableName] = std::chrono::steady_clock::now();
        return;
    }
    loadTableSchema(fullTableName, config);
}

void OracleSchemaCache::shrinkIfInactive(int ttlSeconds) {
	(void)ttlSeconds;
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
