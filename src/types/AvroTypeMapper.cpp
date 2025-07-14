#include "AvroTypeMapper.h"
#include "DataType.h"
#include <unordered_map>

const std::string& AvroTypeMapper::map(int dataType) {
    static const std::unordered_map<int, std::string> mapping = {
        {DataType::BOOLEAN, "boolean"},
        {DataType::TINYINT, "int"},
        {DataType::SMALLINT, "int"},
        {DataType::INTEGER, "int"},
        {DataType::BIGINT, "long"},
        {DataType::FLOAT, "float"},
        {DataType::DOUBLE, "double"},
        {DataType::DECIMAL, "bytes"},
        {DataType::NUMERIC, "bytes"},
        {DataType::DATE, "long"},              // logicalType = date
        {DataType::TIMESTAMP, "long"},         // logicalType = timestamp-millis
        {DataType::TIMESTAMP_WITH_TIMEZONE, "long"},
        {DataType::VARCHAR, "string"},
        {DataType::BINARY, "bytes"},
        {DataType::BLOB, "bytes"},
        {DataType::CLOB, "string"},
        {DataType::NCLOB, "string"},
        {DataType::SQLXML, "string"},
        {DataType::JSON, "string"}
    };

    static const std::string unknown = "string";
    auto it = mapping.find(dataType);
    return (it != mapping.end()) ? it->second : unknown;
}
