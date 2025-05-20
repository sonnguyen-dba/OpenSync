#pragma once

#include <string>
#include <vector>
#include <rapidjson/document.h>
#include "../schema/OracleColumnInfo.h"
#include "../schema/PostgreSQLColumnInfo.h"

class SQLUtils {
public:
    static std::string convertToSQLValue(const rapidjson::Value& val);
    static std::string convertToSQLValue(const rapidjson::Value& val, const std::string& colName);

    static bool isPostgreSQLTimestampOutOfRange(const std::string& timestampStr);

    static std::string convertToSQLValueWithType(
        const rapidjson::Value& val,
        const std::string& dbType,
        const OracleColumnInfo& colInfo,
        const std::string& tableName,
        const std::string& colName,
        bool useISO8601ForDebug,
        int timestamp_unit
    );

    static std::string safeConvert(
        const std::string& dbType,
        const std::string& tableName,
        const std::string& colName,
        const rapidjson::Value& val,
        bool useISO8601ForDebug,
        int timestamp_unit
    );

    static std::string safeConvert(
        const std::string& dbType,
        const std::string& tableName,
        const std::string& colName,
        const rapidjson::Value& val
    );

    static std::string safeConvert(
    	const std::string& dbType,
    	const std::string& tableName,
    	const std::string& colName,
    	const rapidjson::Value& val,
    	bool useISO8601ForDebug
    );

    static std::string convertMicrosecondsToTimestamp(double microsec);
    static std::string convertMicrosecondsToDate(double microsec);

    static std::string escapeString(const std::string& input);
    static std::string quoteString(const std::string& input);

    static int64_t extractMicroseconds(const rapidjson::Value& val, int timestamp_unit);
    static std::string extractTableFromInsert(const std::string& sql);
    static std::string convertToISO8601(const rapidjson::Value& val);

    // PostgreSQL
    static std::string safeConvertPostgreSQL(
        const rapidjson::Value& val,
        const PostgreSQLColumnInfo& colInfo,
        const std::string& tableName,
        const std::string& colName,
        bool useISO8601ForDebug,
        int timestamp_unit
    );

    static std::string toLower(const std::string& input);
    static std::string toUpper(const std::string& input);

    static std::string buildPostgreSQLUpsertSQL(const std::string& fullTable, const rapidjson::Value& jsonObj);
    static std::string join(const std::vector<std::string>& vec, const std::string& delimiter);


};