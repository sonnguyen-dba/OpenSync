#ifndef POSTGRESQL_COLUMN_INFO_H
#define POSTGRESQL_COLUMN_INFO_H

#include <string>

struct PostgreSQLColumnInfo {
    std::string columnName;
    std::string dataType;
    int charMaxLength = 0;
    int numericPrecision = 0;
    int numericScale = 0;
    bool nullable = true;

    bool operator==(const PostgreSQLColumnInfo& other) const {
        return dataType == other.dataType &&
               charMaxLength == other.charMaxLength &&
               numericPrecision == other.numericPrecision &&
               numericScale == other.numericScale &&
               nullable == other.nullable;
    }

    bool operator!=(const PostgreSQLColumnInfo& other) const {
        return !(*this == other);
    }

    std::string getFullTypeString() const {
        std::string s = dataType;
        if (numericPrecision > 0 || numericScale > 0) {
            s += "(" + std::to_string(numericPrecision);
            if (numericScale > 0) s += "," + std::to_string(numericScale);
            s += ")";
        } else if (charMaxLength > 0) {
            s += "(" + std::to_string(charMaxLength) + ")";
        }
        return s + (nullable ? " NULL" : " NOT NULL");
    }
};

#endif

