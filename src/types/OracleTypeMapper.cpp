#include "OracleTypeMapper.h"
#include "DataType.h"
#include <unordered_map>

const std::string& OracleTypeMapper::map(int dataType) {
    static const std::unordered_map<int, std::string> mapping = {
        {DataType::BOOLEAN, "CHAR(1)"},
        {DataType::TINYINT, "NUMBER(3)"},
        {DataType::SMALLINT, "NUMBER(5)"},
        {DataType::INTEGER, "NUMBER(10)"},
        {DataType::BIGINT, "NUMBER(19)"},
        {DataType::FLOAT, "BINARY_FLOAT"},
        {DataType::DOUBLE, "BINARY_DOUBLE"},
        {DataType::DECIMAL, "NUMBER"},
        {DataType::NUMERIC, "NUMBER"},
        {DataType::DATE, "DATE"},
        {DataType::TIMESTAMP, "TIMESTAMP"},
        {DataType::TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP(9) WITH TIME ZONE"},
        {DataType::VARCHAR, "VARCHAR2(4000)"},
        {DataType::BINARY, "RAW(2000)"},
        {DataType::BLOB, "BLOB"},
        {DataType::CLOB, "CLOB"},
        {DataType::NCLOB, "NCLOB"},
        {DataType::SQLXML, "XMLTYPE"},
        {DataType::JSON, "CLOB"}
    };

    static const std::string unknown = "CLOB";  // fallback cho Oracle
    auto it = mapping.find(dataType);
    return (it != mapping.end()) ? it->second : unknown;
}
