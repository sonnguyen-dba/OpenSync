#pragma once
#include <string>

class MySQLTypeMapper {
public:
    static const std::string& map(int dataType);
};
