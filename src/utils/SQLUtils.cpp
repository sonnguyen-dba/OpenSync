#include "SQLUtils.h"
#include "../logger/Logger.h"
#include "../common/TimeUtils.h"
#include "../schema/OracleSchemaCache.h"
#include "../schema/PostgreSQLSchemaCache.h"
#include <sstream>
#include <iomanip>
#include <string>
#include <algorithm>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

using rapidjson::Value;
using namespace std;

std::string SQLUtils::convertToSQLValue(const Value& val) {
    if (val.IsNull()) return "NULL";
    if (val.IsBool()) return val.GetBool() ? "1" : "0";
    if (val.IsInt64()) return std::to_string(val.GetInt64());
    if (val.IsUint64()) return std::to_string(val.GetUint64());
    if (val.IsDouble()) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(6) << val.GetDouble();
        return oss.str();
    }
    if (val.IsString()) return quoteString(val.GetString());

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    val.Accept(writer);
    return quoteString(buffer.GetString());
}

std::string SQLUtils::convertToSQLValue(const Value& val, const std::string& colName) {
    try {
        return convertToSQLValue(val);
    } catch (const std::exception& ex) {
        OpenSync::Logger::warn("Failed to convert value for column " + colName + ": " + ex.what());
        return "NULL";
    }
}

bool SQLUtils::isPostgreSQLTimestampOutOfRange(const std::string& timestampStr) {
    if (!timestampStr.empty() && timestampStr[0] == '-') {
        try {
            int year = std::stoi(timestampStr.substr(1, 4));
            return year > 4713;
        } catch (...) {
            return true;
        }
    }
    return false;
}

std::string SQLUtils::convertToSQLValueWithType(
    const Value& val,
    const std::string& dbType,
    const OracleColumnInfo& colInfo,
    const std::string& tableName,
    const std::string& colName,
    bool useISO8601ForDebug,
    int timestamp_unit)
{
    (void)dbType;
    (void)useISO8601ForDebug;
    if (val.IsNull()) return "NULL";

    const std::string& dataType = colInfo.dataType;
    try {
        if (dataType == "DATE") {
            int64_t microsec = extractMicroseconds(val, timestamp_unit);
            std::string formatted = TimeUtils::convertMicrosecondsToDate(microsec);
            return "TO_DATE('" + formatted + "', 'YYYY-MM-DD')";
        }
        if (dataType.find("TIMESTAMP") != std::string::npos) {
            int64_t microsec = extractMicroseconds(val, timestamp_unit);
            std::string formatted = TimeUtils::convertMicrosecondsToTimestamp(microsec);
            return "TO_TIMESTAMP('" + formatted + "', 'YYYY-MM-DD HH24:MI:SS.FF6')";
        }
        if (dataType.find("CHAR") != std::string::npos || dataType.find("CLOB") != std::string::npos || dataType.find("TEXT") != std::string::npos) {
            return val.IsString() ? quoteString(val.GetString()) : quoteString("?");
        }
        if (dataType.find("NUMBER") != std::string::npos || dataType == "FLOAT" || dataType == "DECIMAL") {
            if (val.IsNumber()) return std::to_string(val.GetDouble());
            if (val.IsString()) return val.GetString();
            return "NULL";
        }
        if (val.IsString()) return quoteString(val.GetString());

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        val.Accept(writer);
        return quoteString(buffer.GetString());

    } catch (const std::exception& ex) {
        OpenSync::Logger::warn("Failed to convert value for " + tableName + "." + colName +
                     " with type=" + dataType + ": " + ex.what());
        return "NULL";
    }
}

std::string SQLUtils::safeConvert(
    const std::string& dbType,
    const std::string& tableName,
    const std::string& colName,
    const Value& val,
    bool useISO8601ForDebug,
    int timestamp_unit)
{
    if (dbType == "oracle") {
        const auto& colInfoMap = OracleSchemaCache::getInstance().getColumnInfo(tableName);
        auto it = colInfoMap.find(colName);
        if (it != colInfoMap.end()) {
            return convertToSQLValueWithType(val, dbType, it->second, tableName, colName, useISO8601ForDebug, timestamp_unit);
        } else {
            OpenSync::Logger::warn("❗️[Ora] Column not found: " + tableName + "." + colName);
            return "NULL";
        }
    } else if (dbType == "postgresql") {

	std::string lowerTable = tableName;
	std::string lowerCol = colName;
	std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(), ::tolower);
    	std::transform(lowerCol.begin(), lowerCol.end(), lowerCol.begin(), ::tolower);

	const auto& colInfoMap = PostgreSQLSchemaCache::getInstance().getColumnInfo(lowerTable);
	auto it = colInfoMap.find(lowerCol);
	if (it != colInfoMap.end()) {
            return safeConvertPostgreSQL(val, it->second, lowerTable, lowerCol, useISO8601ForDebug, timestamp_unit);
 	} else {
	    OpenSync::Logger::warn("❗️[PG] Column not found: " + lowerTable + "." + lowerCol);
    	    return "NULL";
	}

    } else {
        OpenSync::Logger::error("❌ Unsupported dbType in SQLUtils::safeConvert: " + dbType);
        return "NULL";
    }
}

std::string SQLUtils::safeConvert(
    const std::string& dbType,
    const std::string& tableName,
    const std::string& colName,
    const rapidjson::Value& val)
{
    return safeConvert(dbType, tableName, colName, val, false, 1);
}

std::string SQLUtils::safeConvert(
    const std::string& dbType,
    const std::string& tableName,
    const std::string& colName,
    const Value& val,
    bool useISO8601ForDebug)
{
    int timestamp_unit = 1; // mặc định microseconds
    return SQLUtils::safeConvert(dbType, tableName, colName, val, useISO8601ForDebug, timestamp_unit);
}

int64_t SQLUtils::extractMicroseconds(const Value& val, int timestamp_unit) {
    int64_t value = 0;
    if (val.IsInt64()) {
        value = val.GetInt64();
    }
    else if (val.IsUint64()) {
        value = static_cast<int64_t>(val.GetUint64());
    }
    else if (val.IsDouble()) {
        value = static_cast<int64_t>(val.GetDouble());
    }
    else if (val.IsString()) {
        std::string s = val.GetString();
        if (s == "NULL" || s == "null" || s.empty()) {
            return 0; // Trả về 0 hoặc sentinel value tùy xử lý phía trên
        }
        try {
            value = std::stoll(s);
        } catch (...) {
            OpenSync::Logger::warn("⛔ Invalid timestamp string (not numeric): " + s);
            return 0;
        }
    }
    else {
        OpenSync::Logger::warn("⛔ Invalid type for timestamp");
        return 0;
    }

    switch (timestamp_unit) {
        case 0: return value / 1000;       // milliseconds
        case 2: return value * 1000;       // nanoseconds
        default: return value;             // microseconds
    }
}

/*int64_t SQLUtils::extractMicroseconds(const Value& val, int timestamp_unit) {
    int64_t value = 0;
    if (val.IsInt64()) value = val.GetInt64();
    else if (val.IsUint64()) value = static_cast<int64_t>(val.GetUint64());
    else if (val.IsDouble()) value = static_cast<int64_t>(val.GetDouble());
    else if (val.IsString()) {
    	std::string s = val.GetString();
    	if (s == "NULL" || s == "null" || s.empty()) {
            return 0; // Trả về 0 hoặc bạn có thể dùng sentinel khác nếu muốn
    	}
    	try {
            value = std::stoll(s);
    	} catch (...) {
            OpenSync::Logger::warn("⛔ Invalid timestamp string (not numeric): " + s);
            return 0;
        }
    }

    else if (val.IsString()) {
        try { value = std::stoll(val.GetString()); }
        catch (...) {
            OpenSync::Logger::warn("Invalid timestamp string: " + std::string(val.GetString()));
            return 0;
        }
    }

    else {
        OpenSync::Logger::warn("Invalid type for timestamp");
        return 0;
    }

    switch (timestamp_unit) {
        case 0: return value / 1000;
        case 2: return value * 1000;
        default: return value;
    }
}*/

std::string SQLUtils::convertMicrosecondsToTimestamp(double microsec) {
    return TimeUtils::convertMicrosecondsToTimestamp(static_cast<int64_t>(microsec));
}

std::string SQLUtils::convertMicrosecondsToDate(double microsec) {
    return TimeUtils::convertMicrosecondsToDate(static_cast<int64_t>(microsec));
}

std::string SQLUtils::escapeString(const std::string& input) {
    std::string result;
    result.reserve(input.size() + 10);
    for (char c : input) {
        if (c == '\'') result += "''";
        else result += c;
    }
    return result;
}

std::string SQLUtils::quoteString(const std::string& input) {
    return "'" + escapeString(input) + "'";
}

std::string SQLUtils::extractTableFromInsert(const std::string& sql) {
    auto pos = sql.find("INTO ");
    if (pos == std::string::npos) return "";
    auto end = sql.find(" ", pos + 5);
    if (end == std::string::npos) return "";
    return sql.substr(pos + 5, end - pos - 5);
}

std::string SQLUtils::convertToISO8601(const rapidjson::Value& val) {
    int64_t microsec = extractMicroseconds(val, 1);
    return TimeUtils::epochToIso8601(microsec);
}

// PostgreSQL logic
std::string SQLUtils::safeConvertPostgreSQL(
    const Value& val,
    const PostgreSQLColumnInfo& colInfo,
    const std::string& tableName,
    const std::string& colName,
    bool useISO8601ForDebug,
    int timestamp_unit)
{
    (void)useISO8601ForDebug;

    // ✅ Fix 1: detect null đúng
    if (val.IsNull() || (val.IsString() && (
            val.GetString() == std::string("NULL") ||
            val.GetString() == std::string("null") ||
            val.GetString() == std::string("")))) {
        return "NULL";
    }

    const std::string& dataType = colInfo.dataType;

    try {
        if (dataType == "timestamp" || dataType == "timestamp without time zone") {
            int64_t microsec = extractMicroseconds(val, timestamp_unit);

            // ✅ Fix 2: skip timestamp = 0
            if (microsec == 0) {
                OpenSync::Logger::debug("⛔ Skipping timestamp=0 (NULL?) at " + tableName + "." + colName);
                return "NULL";
            }

            constexpr int64_t MIN_US = -3786825600000000;
            constexpr int64_t MAX_US = 4102444800000000;
            if (microsec < MIN_US || microsec > MAX_US) {
                std::string formatted = TimeUtils::convertMicrosecondsToTimestamp(microsec);
                OpenSync::Logger::debug("⛔ Out-of-range timestamp: " + formatted + " at " + tableName + "." + colName);
                return "NULL";
            }

            std::string formatted = TimeUtils::convertMicrosecondsToTimestamp(microsec);
            return "'" + formatted + "'";
        }

        if (dataType == "date") {
            int64_t microsec = extractMicroseconds(val, timestamp_unit);
            if (microsec == 0) return "NULL";
            std::string formatted = TimeUtils::convertMicrosecondsToDate(microsec);
            return "'" + formatted + "'";
        }

        if (dataType.find("char") != std::string::npos || dataType == "text") {
            return val.IsString() ? quoteString(val.GetString()) : quoteString("?");
        }

        if (dataType.find("int") != std::string::npos || dataType.find("numeric") != std::string::npos ||
            dataType.find("float") != std::string::npos || dataType.find("double") != std::string::npos) {
            if (val.IsNumber()) return std::to_string(val.GetDouble());
            if (val.IsString()) return val.GetString();
            return "NULL";
        }

        if (val.IsString()) return quoteString(val.GetString());

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        val.Accept(writer);
        return quoteString(buffer.GetString());

    } catch (const std::exception& ex) {
        OpenSync::Logger::warn("[PG] Failed to convert value for " + tableName + "." + colName +
                               " with type=" + dataType + ": " + ex.what());
        return "NULL";
    }
}

std::string SQLUtils::toLower(const std::string& input) {
    std::string result = input;
    std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) {
        return std::tolower(c);
    });
    return result;
}

std::string SQLUtils::toUpper(const std::string& input) {
    std::string result = input;
    std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) {
        return std::toupper(c);
    });
    return result;
}

std::string SQLUtils::buildPostgreSQLUpsertSQL(
    const std::string& fullTable,
    const rapidjson::Value& jsonObj)
{
    const auto& pkCols = PostgreSQLSchemaCache::getInstance().getPrimaryKeys(fullTable);
    if (pkCols.empty()) {
	OpenSync::Logger::warn("⚠️ Cannot build UPSERT SQL: missing primary key for table " + fullTable);
        return "";
    }

    std::vector<std::string> cols, values, updates;
    for (auto it = jsonObj.MemberBegin(); it != jsonObj.MemberEnd(); ++it) {
        std::string col = SQLUtils::toLower(it->name.GetString());
        const auto& val = it->value;

        cols.push_back(col);
        values.push_back(SQLUtils::safeConvert("postgresql", fullTable, col, val, false));

        // Only add to update clause if not in PK
        if (std::find(pkCols.begin(), pkCols.end(), col) == pkCols.end()) {
            updates.push_back(col + " = EXCLUDED." + col);
        }
    }

    std::ostringstream sql;
    sql << "INSERT INTO " << fullTable << " ("
        << SQLUtils::join(cols, ", ") << ") VALUES ("
        << SQLUtils::join(values, ", ") << ")"
        << " ON CONFLICT (" << SQLUtils::join(pkCols, ", ") << ") DO UPDATE SET "
        << SQLUtils::join(updates, ", ");

    return sql.str();
}

std::string SQLUtils::join(const std::vector<std::string>& vec, const std::string& delimiter) {
    std::ostringstream oss;
    for (size_t i = 0; i < vec.size(); ++i) {
        if (i != 0) oss << delimiter;
        oss << vec[i];
    }
    return oss.str();
}