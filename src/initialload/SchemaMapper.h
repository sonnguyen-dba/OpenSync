#pragma once

#include <string>
#include <unordered_map>
#include "../schema/OracleColumnInfo.h"

class SchemaMapper {
public:
    static std::string mapOracleToPostgreSQL(
        const std::string& schema,
        const std::string& table,
        const std::unordered_map<std::string, OracleColumnInfo>& columns);
};

