#pragma once
#include <string>

struct OracleColumnInfo {
    std::string dataType;
    int dataLength = 0;
    int precision = -1;
    int scale = -1;
    bool nullable = true;

    bool isNotNull() const {
        return !nullable;
    }

    std::string getFullTypeString() const {
        if (dataType == "NUMBER" && precision >= 0) {
            return dataType + "(" + std::to_string(precision) +
                   (scale >= 0 ? "," + std::to_string(scale) : "") + ")";
        }
        if ((dataType == "VARCHAR2" || dataType == "CHAR" || dataType == "NVARCHAR2") && dataLength >= 0) {
            return dataType + "(" + std::to_string(dataLength) + ")";
        }
        return dataType;
    }

    bool operator==(const OracleColumnInfo& other) const {
        return dataType == other.dataType &&
               dataLength == other.dataLength &&
               precision == other.precision &&
               scale == other.scale &&
               nullable == other.nullable;
    }

    bool operator!=(const OracleColumnInfo& other) const {
        return !(*this == other);
    }
};
