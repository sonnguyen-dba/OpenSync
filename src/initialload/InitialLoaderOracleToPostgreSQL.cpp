#include "InitialLoaderOracleToPostgreSQL.h"
#include "SchemaMapper.h"
#include "../reader/FilterConfigLoader.h"
#include "../schema/OracleSchemaCache.h"
#include "../schema/PostgreSQLSchemaCache.h"
#include "../sqlbuilder/PostgreSQLSQLBuilder.h"
#include "../logger/Logger.h"
#include "../utils/SQLUtils.h"
#include "../common/TimeUtils.h"
#include "../common/date.h"
#include <rapidjson/document.h>
#include <thread>

InitialLoaderOracleToPostgreSQL::InitialLoaderOracleToPostgreSQL(const ConfigLoader& config)
    : config(config) {
    oracle = std::make_shared<OracleConnector>(
        config.getDBConfig("oracle", "host"),
        config.getInt("oracle.port", 1521),
        config.getDBConfig("oracle", "user"),
        config.getDBConfig("oracle", "password"),
        config.getDBConfig("oracle", "service")
    );

    postgres = std::make_shared<PostgreSQLConnector>(
        config.getDBConfig("postgresql", "host"),
        config.getInt("postgresql.port", 5432),
        config.getDBConfig("postgresql", "user"),
        config.getDBConfig("postgresql", "password"),
        config.getDBConfig("postgresql", "dbname")
    );

    if (!oracle->connect() || !postgres->connect()) {
        OpenSync::Logger::error("❌ Failed to connect to Oracle or PostgreSQL.");
    }
}

void InitialLoaderOracleToPostgreSQL::runAllTablesIfEnabled() {
    if (!config.getBool("initial-load", false)) return;

    int batchSize = config.getInt("initial-load-batch-size", 1000);
    const auto& filters = FilterConfigLoader::getInstance().getAllFilters();

    createAllPostgreSQLTables();

    for (const auto& f : filters) {
        std::string fullTable = f.owner + "." + f.table;
        PostgreSQLSchemaCache::getInstance().loadSchemaIfNeeded(fullTable, config);
    }

    for (const auto& f : filters) {
        std::string schema = f.owner;
        std::string table = f.table;
        OpenSync::Logger::info("🚀 Initial loading: " + schema + "." + table);
        runInitialLoadForTable(schema, table, batchSize);
    }
}

void InitialLoaderOracleToPostgreSQL::createAllPostgreSQLTables() {
    const auto& filters = FilterConfigLoader::getInstance().getAllFilters();
    for (const auto& f : filters) {
        std::string schema = f.owner;
        std::string table = f.table;
        std::string fullTable = schema + "." + table;

        auto schemaInfo = OracleSchemaCache::getInstance().getColumnInfo(fullTable);
        if (schemaInfo.empty()) {
            OpenSync::Logger::warn("⚠️ No Oracle schema info for: " + fullTable);
            continue;
        }

        std::unordered_map<std::string, OracleColumnInfo> schemaInfoUnordered(
            schemaInfo.begin(), schemaInfo.end()
        );

        if (!postgres->tableExists(schema, table)) {
            std::string createSQL = SchemaMapper::mapOracleToPostgreSQL(schema, table, schemaInfoUnordered);
            OpenSync::Logger::info("🛠️ Executing PostgreSQL CREATE TABLE: " + createSQL);
            if (!postgres->executeStatementSQL(createSQL)) {
                OpenSync::Logger::error("❌ Failed to create table in PostgreSQL: " + fullTable);
            }
        } else {
            OpenSync::Logger::debug("ℹ️ Skipped CREATE TABLE — table already exists: " + fullTable);
        }
    }
}
/*//////////
bool InitialLoaderOracleToPostgreSQL::runInitialLoadForTable(
    const std::string& schema,
    const std::string& table,
    int batchSize
) {
    std::string fullTable = schema + "." + table;

    auto schemaInfo = OracleSchemaCache::getInstance().getColumnInfo(fullTable);
    if (schemaInfo.empty()) {
        OpenSync::Logger::warn("⚠️ No schema info for table: " + fullTable);
        return false;
    }

    PostgreSQLSQLBuilder builder(config, false);
    int offset = 0;

    while (true) {
        std::vector<std::map<std::string, std::string>> rows =
            oracle->queryTableWithOffset(schema, table, offset, batchSize);

        if (rows.empty()) break;

        for (const auto& row : rows) {
            std::ostringstream oss;
            oss << "[InitialLoad][OracleRow offset=" << offset << "] ";
            for (const auto& [col, val] : row) {
                oss << col << "=" << val << " ";
            }
            OpenSync::Logger::debug(oss.str());
        }

        std::vector<std::string> inserts;
        for (const auto& row : rows) {
            rapidjson::Document doc;
            doc.SetObject();
            auto& allocator = doc.GetAllocator();

            for (const auto& [key, value] : row) {
                std::string lowerKey = SQLUtils::toLower(key);
                const auto& it = schemaInfo.find(key);
                std::string colType = (it != schemaInfo.end()) ? SQLUtils::toLower(it->second.dataType) : "";

                rapidjson::Value jsonVal;
                try {
                    if (value.empty() || value == "NULL") {
                        jsonVal.SetNull();
                    } else if (colType.find("number") != std::string::npos ||
                               colType == "float" || colType == "decimal") {
                        jsonVal.SetDouble(std::stod(value));
		    } else if (colType.find("timestamp") != std::string::npos ||
           			colType == "date") {
			if (value != "NULL" && value != "0") {
        		    int64_t micros = TimeUtils::parseISOTimestampToMicros(value);
		            if (micros != -1) {  // ✅ Cho phép micros < 0
		               jsonVal.SetInt64(micros);
		            } else {
		               OpenSync::Logger::debug("⛔ Failed to parse ISO timestamp: " + value + " at " + fullTable + "." + lowerKey);
		               jsonVal.SetNull();
		           }
	               } else {
		           jsonVal.SetNull();
			   //jsonVal.SetString(value.c_str(), allocator);
		       }
		  
		    } else {
                        jsonVal.SetString(value.c_str(), allocator);
                    }
                } catch (const std::exception& e) {
                    OpenSync::Logger::debug("❗ Parse error on column [" + key + "] = [" + value + "], type = " + colType + ": " + e.what());
                    jsonVal.SetNull();
                }

                doc.AddMember(
                    rapidjson::Value().SetString(key.c_str(), allocator),
                    jsonVal,
                    allocator
                );
            }

            try {
                std::string insertSQL = builder.buildInsertSQL(schema, table, doc);
                OpenSync::Logger::debug("[InitialLoad] Built INSERT: " + insertSQL);
                inserts.push_back(std::move(insertSQL));
            } catch (const std::exception& e) {
                OpenSync::Logger::error("❌ Failed to build INSERT SQL for row in table " + fullTable + ": " + e.what());
            }
        }

        if (!postgres->executeBatchQuery(inserts)) {
            OpenSync::Logger::warn("⚠️ Failed to insert batch at offset " + std::to_string(offset));
        } else {
            OpenSync::Logger::info("✅ Inserted batch of " + std::to_string(rows.size()) + " rows at offset " + std::to_string(offset));
        }

        offset += batchSize;
    }

    OpenSync::Logger::info("✅ Finished initial load for table: " + fullTable);
    return true;
}
*/

bool InitialLoaderOracleToPostgreSQL::runInitialLoadForTable(
    const std::string& schema,
    const std::string& table,
    int batchSize
) {
    std::string fullTable = schema + "." + table;

    auto schemaInfo = OracleSchemaCache::getInstance().getColumnInfo(fullTable);
    if (schemaInfo.empty()) {
        OpenSync::Logger::warn("⚠️ No schema info for table: " + fullTable);
        return false;
    }

    // 🆕 timestamp_unit: 0 = ns, 1 = μs, 2 = ms
    int timestampUnit = config.getInt("timestamp_unit", 1);

    PostgreSQLSQLBuilder builder(config, false);
    int offset = 0;

    while (true) {
        std::vector<std::map<std::string, std::string>> rows =
            oracle->queryTableWithOffset(schema, table, offset, batchSize);

        if (rows.empty()) break;

        // 🪵 Debug log: từng dòng
        for (const auto& row : rows) {
            std::ostringstream oss;
            oss << "[InitialLoad][OracleRow offset=" << offset << "] ";
            for (const auto& [col, val] : row) {
                oss << col << "=" << val << " ";
            }
            OpenSync::Logger::debug(oss.str());
        }

        std::vector<std::string> inserts;
        for (const auto& row : rows) {
            rapidjson::Document doc;
            doc.SetObject();
            auto& allocator = doc.GetAllocator();

            for (const auto& [key, value] : row) {
                std::string lowerKey = SQLUtils::toLower(key);
                const auto& it = schemaInfo.find(key);
                std::string colType = (it != schemaInfo.end()) ? SQLUtils::toLower(it->second.dataType) : "";

                rapidjson::Value jsonVal;
                try {
                    if (value.empty() || value == "NULL") {
                        jsonVal.SetNull();
                    }
                    else if (colType.find("number") != std::string::npos ||
                             colType == "float" || colType == "decimal") {
                        jsonVal.SetDouble(std::stod(value));
                    }
                    else if (colType.find("timestamp") != std::string::npos ||
                             colType == "date") {
                        if (value != "0") {
                            int64_t micros = TimeUtils::parseISOTimestampToMicros(value);
                            if (micros != -1) {
                                int64_t result = micros;
                                if (timestampUnit == 0) result = micros * 1000;      // ns
                                else if (timestampUnit == 2) result = micros / 1000; // ms

                                jsonVal.SetInt64(result);
                            } else {
                                OpenSync::Logger::debug("⛔ Failed to parse ISO timestamp: " + value + " at " + fullTable + "." + lowerKey);
                                jsonVal.SetNull();
                            }
                        } else {
                            jsonVal.SetNull();
                        }
                    }
                    else {
                        jsonVal.SetString(value.c_str(), static_cast<rapidjson::SizeType>(value.length()), allocator);
                    }
                } catch (const std::exception& e) {
                    OpenSync::Logger::debug("❗ Parse error on column [" + key + "] = [" + value + "], type = " + colType + ": " + e.what());
                    jsonVal.SetNull();
                }

                doc.AddMember(
                    rapidjson::Value().SetString(key.c_str(), allocator),
                    jsonVal,
                    allocator
                );
            }

            try {
                std::string insertSQL = builder.buildInsertSQL(schema, table, doc);
                OpenSync::Logger::debug("[InitialLoad] Built INSERT: " + insertSQL);
                inserts.push_back(std::move(insertSQL));
            } catch (const std::exception& e) {
                OpenSync::Logger::error("❌ Failed to build INSERT SQL for row in table " + fullTable + ": " + e.what());
            }
        }

        if (!postgres->executeBatchQuery(inserts)) {
            OpenSync::Logger::warn("⚠️ Failed to insert batch at offset " + std::to_string(offset));
        } else {
            OpenSync::Logger::info("✅ Inserted batch of " + std::to_string(rows.size()) + " rows at offset " + std::to_string(offset));
        }

        offset += batchSize;
    }

    OpenSync::Logger::info("✅ Finished initial load for table: " + fullTable);
    return true;
}