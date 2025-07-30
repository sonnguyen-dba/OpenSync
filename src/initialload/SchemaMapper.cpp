#include "SchemaMapper.h"
#include "../reader/FilterConfigLoader.h"
#include "../utils/SQLUtils.h"
#include <algorithm>

std::string SchemaMapper::mapOracleToPostgreSQL(
    const std::string& schema,
    const std::string& table,
    const std::unordered_map<std::string, OracleColumnInfo>& columns
) {
    // Convert schema & table to lowercase to avoid schema not found errors
    std::string schemaLower = SQLUtils::toLower(schema);
    std::string tableLower = SQLUtils::toLower(table);

    std::string sql = "CREATE TABLE IF NOT EXISTS " + schemaLower + "." + tableLower + " (\n";
    bool first = true;

    for (const auto& [colName, colInfo] : columns) {
        if (!first) sql += ",\n";
        first = false;

        std::string colLower = SQLUtils::toLower(colName);
        sql += "  " + colLower + " ";

        std::string dataType = colInfo.dataType;
        std::transform(dataType.begin(), dataType.end(), dataType.begin(), ::tolower);

        if (dataType == "varchar2" || dataType == "nvarchar2" || dataType == "char") {
            sql += "varchar(" + std::to_string(colInfo.dataLength) + ")";
        } else if (dataType == "number") {
            if (colInfo.scale > 0) {
                sql += "decimal(" + std::to_string(colInfo.precision) + "," + std::to_string(colInfo.scale) + ")";
            } else if (colInfo.precision == 1) {
                sql += "boolean";
            } else if (colInfo.precision <= 5) {
                sql += "smallint";
            } else if (colInfo.precision <= 10) {
                sql += "int";
            } else if (colInfo.precision <= 19) {
                sql += "bigint";
            } else {
                sql += "decimal";
            }
        } else if (dataType == "date") {
            sql += "timestamp";
        } else if (dataType.find("timestamp") != std::string::npos) {
            sql += "timestamp";
        } else if (dataType == "binary_float") {
            sql += "real";
        } else if (dataType == "binary_double") {
            sql += "double precision";
        } else if (dataType == "nclob") {
            sql += "text";
        } else if (dataType == "blob") {
            sql += "bytea";
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
