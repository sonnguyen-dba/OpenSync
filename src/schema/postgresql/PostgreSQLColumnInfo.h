#pragma once
#include <string>

struct PostgreSQLColumnInfo {
    std::string columnName;
    std::string dataType;
    int charMaxLength = 0;
    int numericPrecision = 0;
    int numericScale = 0;
    bool nullable = true;
};

