#include "DataTypeHandler.h"
#include <sstream>
#include <rapidjson/stringbuffer.h>  // Thêm thư viện này
#include <rapidjson/writer.h>        // Thêm thư viện này

#include "DataTypeHandler.h"
#include <sstream>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

std::string DataTypeHandler::trimWhitespace(const std::string& str) {
    if (str.empty()) return str;

    // Tìm vị trí ký tự đầu tiên không phải khoảng trắng
    size_t first = str.find_first_not_of(" \t");
    // Tìm vị trí ký tự cuối cùng không phải khoảng trắng
    size_t last = str.find_last_not_of(" \t");

    if (first == std::string::npos) return ""; // Trường hợp chuỗi toàn khoảng trắng
    return str.substr(first, last - first + 1);
}

std::string DataTypeHandler::formatValue(const rapidjson::Value& value) {
    if (value.IsString()) {
        std::string strValue = trimWhitespace(value.GetString()); // Trim cả trước & sau

        // Escape dấu "
        size_t pos = 0;
        while ((pos = strValue.find("\"", pos)) != std::string::npos) {
            strValue.insert(pos, "\\");
            pos += 2;
        }

        return "\"" + strValue + "\"";  // Bao lại bằng nháy kép
    } else if (value.IsBool()) {
        return value.GetBool() ? "true" : "false";
	//return value.GetBool() ? "1" : "0";
    } else if (value.IsInt()) {
        return std::to_string(value.GetInt());
    } else if (value.IsInt64()) {
        return std::to_string(value.GetInt64());
    } else if (value.IsDouble()) {
        std::ostringstream oss;
        oss.precision(10);
        oss << value.GetDouble();
        return oss.str();
    } else if (value.IsNull()) {
        return "null";
    } else if (value.IsArray() || value.IsObject()) {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        value.Accept(writer);
        std::string jsonStr = trimWhitespace(buffer.GetString()); // Trim cả trước & sau

        return "\"" + jsonStr + "\"";  // Bao lại bằng nháy kép
    } else {
        return "\"Unsupported\"";  // Dùng nháy kép thay vì nháy đơn
    }
}

/*
std::string DataTypeHandler::formatValue(const rapidjson::Value& value) {
    if (value.IsString()) {
        std::string strValue = value.GetString();
        size_t pos = 0;
        while ((pos = strValue.find("\"", pos)) != std::string::npos) {
            strValue.insert(pos, "\\");
            pos += 2;
        }
        return "\"" + strValue + "\"";  // Đổi dấu nháy đơn thành nháy kép
    } else if (value.IsBool()) {
        return value.GetBool() ? "true" : "false";
    } else if (value.IsInt()) {
        return std::to_string(value.GetInt());
    } else if (value.IsInt64()) {
        return std::to_string(value.GetInt64());
    } else if (value.IsDouble()) {
        std::ostringstream oss;
        oss.precision(10);
        oss << value.GetDouble();
        return oss.str();
    } else if (value.IsNull()) {
        return "null";
    } else if (value.IsArray() || value.IsObject()) {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        value.Accept(writer);
        return "\"" + std::string(buffer.GetString()) + "\"";  // Bao lại bằng nháy kép
    } else {
        return "\"Unsupported\"";  // Dùng nháy kép thay vì nháy đơn
    }
}
*/
