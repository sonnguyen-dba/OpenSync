#include "SchemaMapper.h"
#include "../reader/FilterConfigLoader.h"
#include "../utils/SQLUtils.h"
#include <algorithm>

std::string SchemaMapper::mapOracleToPostgreSQL(
    const std::string& schema,
    const std::string& table,
    const std::unordered_map<std::string, OracleColumnInfo>& columns
) {
    // Convert schema & table to lowercase để tránh lỗi schema không tồn tại
    std::string schemaLower = SQLUtils::toLower(schema);
    std::string tableLower = SQLUtils::toLower(table);

    std::string sql = "CREATE TABLE IF NOT EXISTS " + schemaLower + "." + tableLower + " (\n";
    bool first = true;

    for (const auto& [colName, colInfo] : columns) {
        if (!first) sql += ",\n";
        first = false;

	std::string colLower = SQLUtils::toLower(colName);
        sql += "  " + colLower + " ";
        //sql += "  \"" + colName + "\" ";

        std::string dataType = colInfo.dataType;
        std::transform(dataType.begin(), dataType.end(), dataType.begin(), ::tolower);

        if (dataType == "varchar2" || dataType == "nvarchar2" || dataType == "char") {
            sql += "varchar(" + std::to_string(colInfo.dataLength) + ")";
        } else if (dataType == "number") {
            if (colInfo.scale > 0) {
                sql += "numeric(" + std::to_string(colInfo.precision) + "," + std::to_string(colInfo.scale) + ")";
            } else if (colInfo.precision > 0) {
                sql += "numeric(" + std::to_string(colInfo.precision) + ")";
            } else {
                sql += "numeric";
            }
        } else if (dataType == "date") {
            sql += "timestamp";
        } else if (dataType.find("timestamp") != std::string::npos) {
            sql += "timestamp";
        } else {
            sql += "text";
        }

        if (!colInfo.nullable) {
            sql += " not null";
        }
    }

    // ✅ Append primary key definition
    const std::string fullTable = schema + "." + table;
    const auto& pkMap = FilterConfigLoader::getInstance().getPrimaryKeyColumns();
    auto it = pkMap.find(fullTable);
    if (it != pkMap.end()) {
    	std::string pkLower = it->second;
    	std::transform(pkLower.begin(), pkLower.end(), pkLower.begin(), ::tolower);
    	sql += ",\n  primary key (" + pkLower + ")";
    }

    sql += "\n);";
    return sql;
}

