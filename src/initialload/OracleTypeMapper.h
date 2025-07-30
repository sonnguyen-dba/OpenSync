#pragma once

#include <string>
#include "../schema/OracleColumnInfo.h"

class OracleTypeMapper {
public:
    static std::string mapToPostgreSQL(const OracleColumnInfo& colInfo);
};
