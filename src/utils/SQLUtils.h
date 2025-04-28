#pragma once

#include <string>
#include <cstdint>
#include <rapidjson/document.h>
#include "OracleColumnInfo.h"

class SQLUtils {
public:
    // Chuyển value bất kỳ sang SQL literal (quote nếu cần)
    static std::string convertToSQLValue(const rapidjson::Value& val);

    // Overload: truyền thêm tên cột để log nếu lỗi
    static std::string convertToSQLValue(const rapidjson::Value& val, const std::string& colName);

    // Chuyển đổi có xét đến kiểu dữ liệu thực tế trong schema
    static std::string convertToSQLValueWithType(
        const rapidjson::Value& val,
        const std::string& dbType,
        const OracleColumnInfo& colInfo,
        const std::string& tableName,
        const std::string& colName,
	bool useISO8601ForDebug = false,
	int timestamp_unit = 1);

    // Wrapper: dùng OracleSchemaCache để tự lấy kiểu cột rồi convert
    static std::string safeConvert(
	const std::string& dbType,
	const std::string& tableName,
    	const std::string& colName,
    	const rapidjson::Value& val,
    	bool useISO8601ForDebug = false,
	int timestamp_unit = 1);

    static std::string safeConvert(
    	const std::string& dbType,
    	const std::string& tableName,
    	const std::string& colName,
    	const rapidjson::Value& val);

    // Chuyển microseconds → định dạng timestamp SQL
    static std::string convertMicrosecondsToTimestamp(double microsec);
    static std::string convertMicrosecondsToDate(double microsec);

    // Escape chuỗi để gán vào SQL safely ('' thay cho ')
    static std::string escapeString(const std::string& input);
    static std::string quoteString(const std::string& input);

    // Trích tên bảng từ câu INSERT
    static std::string extractTableFromInsert(const std::string& sql);

    // Trích số microsecond từ value bất kỳ
    static int64_t extractMicroseconds(const rapidjson::Value& val, int timestamp_unit = 1);

    static std::string convertToISO8601(const rapidjson::Value& val);

};

