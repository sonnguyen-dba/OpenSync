#include "OracleSQLBuilder.h"
#include "../logger/Logger.h"
#include "../utils/SQLUtils.h"
#include "../time/TimeUtils.h"
#include "../schema/OracleSchemaCache.h"
#include <sstream>

OracleSQLBuilder::OracleSQLBuilder(ConfigLoader& config, bool enableLog)
    : config(config), enableISODebugLog(enableLog), timestamp_unit(config.getTimestampUnit()) {
	      //Logger::info("OracleSQLBuilder initialized with timestamp_unit=" +
        //      std::to_string(timestamp_unit) +
        //         " (" + (timestamp_unit == 0 ? "nanosecond" : timestamp_unit == 1 ? "microsecond" : "millisecond") + ")");
	       Logger::info("OracleSQLBuilder initialized with timestamp_unit=" + std::to_string(timestamp_unit));
    }

std::string OracleSQLBuilder::buildInsertSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data) {
    std::ostringstream sql;
    std::ostringstream columns;
    std::ostringstream values;

    std::string fullTable = schema + "." + table;
    const auto& colTypes = OracleSchemaCache::getInstance().getColumnTypes(fullTable);

    if (colTypes.empty()) {
        Logger::warn("🔎 Oracle schema not found for table: " + fullTable + ", fallback to basic quoting");
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
                Logger::debug("🕓 ISO Timestamp | " + fullTable + "." + colName + " = " + iso);
            }
        }
    }

    sql << "INSERT INTO " << fullTable << " (" << columns.str() << ") VALUES (" << values.str() << ")";
    Logger::debug("SQL: " + sql.str());
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
        Logger::warn("⚠️ Missing primary key [" + primaryKey + "] in data for DELETE");
        return "";
    }

    const auto& pkVal = data[primaryKey.c_str()];
    std::string pkValue = SQLUtils::safeConvert("oracle", fullTable, primaryKey, pkVal, false, timestamp_unit);

    return "DELETE FROM " + fullTable + " WHERE \"" + primaryKey + "\" = " + pkValue;
}
