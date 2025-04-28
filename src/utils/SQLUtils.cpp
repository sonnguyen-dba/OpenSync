#include "SQLUtils.h"
#include "../logger/Logger.h"
#include "../time/TimeUtils.h"
#include "OracleSchemaCache.h"

#include <algorithm>
#include <stdexcept>
#include <cctype>
#include <sstream>
#include <iomanip>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

using rapidjson::Value;
using namespace std;

// Hàm convert đơn giản với 1 tham số (dùng khi không cần biết kiểu cột)
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

    if (val.IsString()) {
        return quoteString(val.GetString());
    }

    // fallback: stringify object/array
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    val.Accept(writer);
    return quoteString(buffer.GetString());
}

// Overload: dùng khi muốn log thêm tên cột nếu lỗi
std::string SQLUtils::convertToSQLValue(const Value& val, const std::string& colName) {
    try {
        return SQLUtils::convertToSQLValue(val);
    } catch (const std::exception& ex) {
        Logger::warn("Failed to convert value for column " + colName + ": " + ex.what());
        return "NULL";
    }
}

// Hàm chuyển đổi có xét đến kiểu cột (DATE, TIMESTAMP, NUMBER...)
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
            int64_t microsec = SQLUtils::extractMicroseconds(val, timestamp_unit);
            std::string formatted = TimeUtils::convertMicrosecondsToDate(microsec);
            return "TO_DATE('" + formatted + "', 'YYYY-MM-DD')";
        }

        if (dataType.find("TIMESTAMP") != std::string::npos) {
            int64_t microsec = SQLUtils::extractMicroseconds(val, timestamp_unit);
            std::string formatted = TimeUtils::convertMicrosecondsToTimestamp(microsec);
            return "TO_TIMESTAMP('" + formatted + "', 'YYYY-MM-DD HH24:MI:SS.FF6')";
        }

        if (dataType.find("CHAR") != std::string::npos || dataType.find("CLOB") != std::string::npos || dataType.find("TEXT") != std::string::npos) {
            if (val.IsString()) {
                return quoteString(val.GetString());
            } else {
                rapidjson::StringBuffer buffer;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                val.Accept(writer);
                return quoteString(buffer.GetString());
            }
        }

        if (dataType.find("NUMBER") != std::string::npos || dataType == "FLOAT" || dataType == "DECIMAL") {
            if (val.IsNumber()) return std::to_string(val.GetDouble());
            if (val.IsString()) return val.GetString(); // giả sử là số đúng format
            return "NULL";
        }

        // fallback
        if (val.IsString()) {
            return quoteString(val.GetString());
        } else {
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            val.Accept(writer);
            return quoteString(buffer.GetString());
        }

    } catch (const std::exception& ex) {
        Logger::warn("Failed to convert value for " + tableName + "." + colName +
                     " with type=" + dataType + ": " + ex.what());
        return "NULL";
    }
}

// Dùng schema để xác định kiểu cột trước khi gọi convert
std::string SQLUtils::safeConvert(
    const std::string& dbType,
    const std::string& tableName,
    const std::string& colName,
    const Value& val,
    bool useISO8601ForDebug,
    int timestamp_unit)
{
    const auto& colInfoMap = OracleSchemaCache::getInstance().getColumnInfo(tableName);
    auto it = colInfoMap.find(colName);
    if (it != colInfoMap.end()) {
        const OracleColumnInfo& colInfo = it->second;
        return convertToSQLValueWithType(val, dbType, colInfo, tableName, colName, useISO8601ForDebug, timestamp_unit);
    } else {
        Logger::warn("❗️Column not found: " + tableName + "." + colName);
        return "NULL";
    }
}

// Utilities
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
    return "'" + SQLUtils::escapeString(input) + "'";
}

std::string SQLUtils::extractTableFromInsert(const std::string& sql) {
    auto pos = sql.find("INTO ");
    if (pos == std::string::npos) return "";
    auto end = sql.find(" ", pos + 5);
    if (end == std::string::npos) return "";
    return sql.substr(pos + 5, end - pos - 5);
}

/*int64_t SQLUtils::extractMicroseconds(const Value& val, int timestamp_unit) {
    if (val.IsInt64()) return val.GetInt64();
    if (val.IsUint64()) return static_cast<int64_t>(val.GetUint64());
    if (val.IsDouble()) return static_cast<int64_t>(val.GetDouble());
    if (val.IsString()) {
        try {
            return std::stoll(val.GetString());
        } catch (...) {
            return 0;
        }
    }
    return 0;
}*/

int64_t SQLUtils::extractMicroseconds(const Value& val, int timestamp_unit) {
    int64_t value = 0;
    std::string unit_str;

    if (val.IsInt64()) {
        value = val.GetInt64();
    } else if (val.IsUint64()) {
        value = static_cast<int64_t>(val.GetUint64());
    } else if (val.IsDouble()) {
        value = static_cast<int64_t>(val.GetDouble());
    } else if (val.IsString()) {
        try {
            value = std::stoll(val.GetString());
        } catch (...) {
            Logger::warn("Invalid timestamp string: " + std::string(val.GetString()));
            return 0;
        }
    } else {
        Logger::warn("Invalid type for timestamp value");
        return 0;
    }

    switch (timestamp_unit) {
        case 0: // Nanosecond
            unit_str = "nanoseconds";
            value /= 1000; // Nanosecond to microsecond
            Logger::debug("Extracted timestamp from nanoseconds to microseconds: value=" +
                         std::to_string(val.GetInt64()) + ", result=" + std::to_string(value));
            break;
        case 1: // Microsecond
            unit_str = "microseconds";
            Logger::debug("Interpreted timestamp as microseconds: value=" + std::to_string(value));
            break;
        case 2: // Millisecond
            unit_str = "milliseconds";
            value *= 1000; // Millisecond to microsecond
            Logger::debug("Extracted timestamp from milliseconds to microseconds: value=" +
                         std::to_string(val.GetInt64()) + ", result=" + std::to_string(value));
            break;
        default:
            Logger::error("Invalid timestamp_unit: " + std::to_string(timestamp_unit) + ", treating as microseconds");
            unit_str = "microseconds";
            break;
    }

    return value;
}

std::string SQLUtils::safeConvert(
    const std::string& dbType,
    const std::string& tableName,
    const std::string& colName,
    const rapidjson::Value& val)
{
    return SQLUtils::safeConvert(dbType, tableName, colName, val, "");
}

std::string SQLUtils::convertToISO8601(const rapidjson::Value& val) {
    int64_t microsec = extractMicroseconds(val);
    return TimeUtils::epochToIso8601(microsec);
}
