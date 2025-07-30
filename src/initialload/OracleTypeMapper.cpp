#include "OracleTypeMapper.h"
#include <unordered_map>
#include <algorithm>

std::string OracleTypeMapper::mapToPostgreSQL(const OracleColumnInfo& colInfo) {
    std::string dataType = colInfo.dataType;
    std::transform(dataType.begin(), dataType.end(), dataType.begin(), ::tolower);

    static const std::unordered_map<std::string, std::string> simpleTypeMap = {
        {"nclob", "text"},
        {"blob", "bytea"},
        {"binary_float", "real"},
        {"binary_double", "double precision"},
        {"date", "timestamp"},
        {"timestamp", "timestamp"},
        {"timestamp with time zone", "timestamp with time zone"},
        {"timestamp with local time zone", "timestamp"}
    };

    auto it = simpleTypeMap.find(dataType);
    if (it != simpleTypeMap.end()) {
        return it->second;
    }
    // ⏱️ TIMESTAMP variants
    if (dataType.find("timestamp") != std::string::npos) {
        if (dataType.find("with time zone") != std::string::npos)
            return "timestamp with time zone";
        return "timestamp";
    }
    
    if (dataType == "varchar2" || dataType == "nvarchar2" || dataType == "char") {
        return "varchar(" + std::to_string(colInfo.dataLength) + ")";
    }

    if (dataType == "number") {
        int p = colInfo.precision;
        int s = colInfo.scale;

        if (p == 1 && s == 0)
            return "boolean";
        if ((p == 3 || p == 5) && s == 0)
            return "smallint";
        if (p == 10 && s == 0)
            return "int";
        if (p == 19 && s == 0)
            return "bigint";
        if (s > 0)
            return "decimal(" + std::to_string(p) + "," + std::to_string(s) + ")";
        if (p > 0)
            return "decimal(" + std::to_string(p) + ")";
        return "decimal";
    }

    return "text"; // fallback
}
