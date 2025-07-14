#include "PostgreSQLTypeMapper.h"
#include "DataType.h"
#include <unordered_map>

const std::string& PostgreSQLTypeMapper::map(int dataType) {
    static const std::unordered_map<int, std::string> mapping = {
        {DataType::BOOLEAN, "boolean"},
        {DataType::TINYINT, "smallint"},
        {DataType::SMALLINT, "smallint"},
        {DataType::INTEGER, "integer"},
        {DataType::BIGINT, "bigint"},
        {DataType::FLOAT, "real"},
        {DataType::DOUBLE, "double precision"},
        {DataType::DECIMAL, "numeric"},
        {DataType::NUMERIC, "numeric"},
        {DataType::DATE, "timestamp"},
        {DataType::TIMESTAMP, "timestamp"},
        {DataType::TIMESTAMP_WITH_TIMEZONE, "timestamp with time zone"},
        {DataType::VARCHAR, "text"},
        {DataType::BINARY, "bytea"},
        {DataType::BLOB, "lo"},
        {DataType::CLOB, "text"},
        {DataType::NCLOB, "text"},
        {DataType::SQLXML, "text"},
        {DataType::JSON, "text"}
    };

    static const std::string unknown = "text";
    auto it = mapping.find(dataType);
    return (it != mapping.end()) ? it->second : unknown;
}
