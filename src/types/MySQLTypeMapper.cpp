#include "MySQLTypeMapper.h"
#include "DataType.h"
#include <unordered_map>

const std::string& MySQLTypeMapper::map(int dataType) {
    static const std::unordered_map<int, std::string> mapping = {
        {DataType::BOOLEAN, "TINYINT(1)"},
        {DataType::TINYINT, "TINYINT"},
        {DataType::SMALLINT, "SMALLINT"},
        {DataType::INTEGER, "INT"},
        {DataType::BIGINT, "BIGINT"},
        {DataType::FLOAT, "FLOAT"},
        {DataType::DOUBLE, "DOUBLE"},
        {DataType::DECIMAL, "DECIMAL"},
        {DataType::NUMERIC, "DECIMAL"},
        {DataType::DATE, "DATETIME"},
        {DataType::TIMESTAMP, "TIMESTAMP"},
        {DataType::TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP"},
        {DataType::VARCHAR, "TEXT"},
        {DataType::BINARY, "BLOB"},
        {DataType::BLOB, "BLOB"},
        {DataType::CLOB, "TEXT"},
        {DataType::NCLOB, "TEXT"},
        {DataType::SQLXML, "TEXT"},
        {DataType::JSON, "JSON"}
    };

    static const std::string unknown = "TEXT";
    auto it = mapping.find(dataType);
    return (it != mapping.end()) ? it->second : unknown;
}
