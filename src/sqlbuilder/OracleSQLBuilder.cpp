#include "OracleSQLBuilder.h"
#include "../logger/Logger.h"
#include "../utils/SQLUtils.h"
#include "../common/TimeUtils.h"
#include "../schema/OracleSchemaCache.h"
#include <sstream>

OracleSQLBuilder::OracleSQLBuilder(ConfigLoader& config, bool enableLog)
    : config(config), enableISODebugLog(enableLog), timestamp_unit(config.getTimestampUnit()) {
	//OpenSync::Logger::info("OracleSQLBuilder initialized with timestamp_unit=" +
        //      std::to_string(timestamp_unit) +
        //         " (" + (timestamp_unit == 0 ? "nanosecond" : timestamp_unit == 1 ? "microsecond" : "millisecond") + ")");

	OpenSync::Logger::info("OracleSQLBuilder initialized with timestamp_unit=" + std::to_string(timestamp_unit));
    }
/*
std::string OracleSQLBuilder::buildInsertSQL(const std::string& schema, const std::string& table,
                                            const rapidjson::Value& data) {
    std::ostringstream sql;
    std::ostringstream columns;
    std::ostringstream values;

    std::string fullTable = schema + "." + table;
    const auto& colTypes = OracleSchemaCache::getInstance().getColumnTypes(fullTable);

    if (colTypes.empty()) {
        OpenSync::Logger::warn("🔎 Oracle schema not found for table: " + fullTable + ", fallback to basic quoting");
    }

    bool first = true;
    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it) {
        const std::string colName = it->name.GetString();
        const auto& jsonVal = it->value;

        if (!first) {
            columns << ", ";
            values << ", ";
        }
        columns << colName;

        // Xử lý cột F (TIMESTAMP) từ JSON
        if (colName == "F" && !jsonVal.IsNull()) {
            int64_t timestamp_us = 0;
            std::string log_value;

            if (jsonVal.IsInt64()) {
                timestamp_us = jsonVal.GetInt64();
                log_value = std::to_string(timestamp_us);
            } else if (jsonVal.IsDouble()) {
                timestamp_us = static_cast<int64_t>(jsonVal.GetDouble());
                log_value = std::to_string(jsonVal.GetDouble());
            } else if (jsonVal.IsString()) {
                std::string str = jsonVal.GetString();
                log_value = str;
                try {
                    timestamp_us = std::stoll(str);
                } catch (...) {
                    OpenSync::Logger::warn("Invalid timestamp string for column F in table " + fullTable + ": " + str);
                    values << "NULL";
                    first = false;
                    continue;
                }
            } else {
                OpenSync::Logger::warn("Invalid type for column F in table: " + fullTable);
                values << "NULL";
                first = false;
                continue;
            }

            OpenSync::Logger::debug("Column F raw value for " + fullTable + ": " + log_value);

            // Kiểm tra phạm vi timestamp (1900-01-01 to 2100-01-01)
            if (timestamp_us < -2208988800000000 || timestamp_us > 4102444800000000) {
                OpenSync::Logger::warn("Timestamp out of range for column F in table " + fullTable + ": " + std::to_string(timestamp_us));
                values << "NULL";
                first = false;
                continue;
            }

            std::string timestamp_str = TimeUtils::convertMicrosecondsToTimestamp(timestamp_us);
            OpenSync::Logger::debug("Column F converted timestamp for " + fullTable + ": " + timestamp_str);
            values << "TO_TIMESTAMP('" << timestamp_str << "', 'YYYY-MM-DD HH24:MI:SS.FF6')";
        } else {
            std::string sqlValue = SQLUtils::safeConvert("oracle", fullTable, colName, jsonVal, false);
            values << sqlValue;
        }
        first = false;

        if (enableISODebugLog) {
            std::string iso = SQLUtils::convertToISO8601(jsonVal);
            if (!iso.empty()) {
                OpenSync::Logger::debug("🕓 ISO Timestamp | " + fullTable + "." + colName + " = " + iso);
            }
        }
    }

    sql << "INSERT INTO " << fullTable << " (" << columns.str() << ") VALUES (" << values.str() << ")";
    OpenSync::Logger::debug("SQL: " + sql.str());
    return sql.str();
}*/

std::string OracleSQLBuilder::buildInsertSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data) {
    std::ostringstream sql;
    std::ostringstream columns;
    std::ostringstream values;

    std::string fullTable = schema + "." + table;
    const auto& colTypes = OracleSchemaCache::getInstance().getColumnTypes(fullTable);

    if (colTypes.empty()) {
        OpenSync::Logger::warn("🔎 Oracle schema not found for table: " + fullTable + ", fallback to basic quoting");
    }

    bool first = true;
    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it) {
        const std::string colName = it->name.GetString();
        const auto& jsonVal = it->value;

        std::string sqlValue = SQLUtils::safeConvert("oracle", fullTable, colName, jsonVal, false, timestamp_unit);

        if (!first) {
            columns << ", ";
            values << ", ";
        }
        columns << colName;
        values << sqlValue;
        first = false;

        if (enableISODebugLog) {
            std::string iso = SQLUtils::convertToISO8601(jsonVal);
            if (!iso.empty()) {
                OpenSync::Logger::debug("🕓 ISO Timestamp | " + fullTable + "." + colName + " = " + iso);
            }
        }
    }

    sql << "INSERT INTO " << fullTable << " (" << columns.str() << ") VALUES (" << values.str() << ")";
    OpenSync::Logger::debug("SQL: " + sql.str());
    return sql.str();
}

std::string OracleSQLBuilder::buildUpdateSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data, const std::string& primaryKey) {
    std::ostringstream sql;
    std::ostringstream setClause;

    std::string fullTable = schema + "." + table;
    std::string pkValue;
    bool first = true;

    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it) {
        const std::string colName = it->name.GetString();
        const auto& jsonVal = it->value;

        std::string sqlValue = SQLUtils::safeConvert("oracle", fullTable, colName, jsonVal, false, timestamp_unit);

        if (colName == primaryKey) {
            pkValue = sqlValue;
            continue;
        }

        if (!first) setClause << ", ";
        setClause << "\"" << colName << "\" = " << sqlValue;
        first = false;
    }

    sql << "UPDATE " << fullTable << " SET " << setClause.str()
        << " WHERE \"" << primaryKey << "\" = " << pkValue;

    return sql.str();
}

std::string OracleSQLBuilder::buildDeleteSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data, const std::string& primaryKey) {
    std::string fullTable = schema + "." + table;

    if (!data.HasMember(primaryKey.c_str())) {
        OpenSync::Logger::warn("⚠️ Missing primary key [" + primaryKey + "] in data for DELETE");
        return "";
    }

    const auto& pkVal = data[primaryKey.c_str()];
    std::string pkValue = SQLUtils::safeConvert("oracle", fullTable, primaryKey, pkVal, false, timestamp_unit);

    return "DELETE FROM " + fullTable + " WHERE \"" + primaryKey + "\" = " + pkValue;
}

