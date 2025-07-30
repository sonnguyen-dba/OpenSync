#include "SchemaMapper.h"
#include "../reader/FilterConfigLoader.h"
#include "../utils/SQLUtils.h"
#include <algorithm>

std::string SchemaMapper::mapOracleToPostgreSQL(
    const std::string& schema,
    const std::string& table,
    const std::unordered_map<std::string, OracleColumnInfo>& columns
) {
    std::string schemaLower = SQLUtils::toLower(schema);
    std::string tableLower = SQLUtils::toLower(table);

    std::string sql = "CREATE TABLE IF NOT EXISTS " + schemaLower + "." + tableLower + " (\n";
    bool first = true;

    for (const auto& [colName, colInfo] : columns) {
        if (!first) sql += ",\n";
        first = false;

        std::string colLower = SQLUtils::toLower(colName);
        sql += "  " + colLower + " ";

        std::string dataType = SQLUtils::toLower(colInfo.dataType);

        if (dataType == "varchar2" || dataType == "nvarchar2" || dataType == "char") {
            sql += "varchar(" + std::to_string(colInfo.dataLength) + ")";
        } else if (dataType == "nclob") {
            sql += "text";
        } else if (dataType == "blob") {
            sql += "bytea";
        } else if (dataType == "binary_float") {
            sql += "real";
        } else if (dataType == "binary_double") {
            sql += "double precision";
        } else if (dataType == "number") {
            if (colInfo.precision == 1 && colInfo.scale == 0) {
                sql += "boolean";
            } else if ((colInfo.precision == 3 || colInfo.precision == 5) && colInfo.scale == 0) {
                sql += "smallint";
            } else if (colInfo.precision == 10 && colInfo.scale == 0) {
                sql += "int";
            } else if (colInfo.precision == 19 && colInfo.scale == 0) {
                sql += "bigint";
            } else if (colInfo.scale > 0) {
                sql += "decimal(" + std::to_string(colInfo.precision) + "," + std::to_string(colInfo.scale) + ")";
            } else if (colInfo.precision > 0) {
                sql += "decimal(" + std::to_string(colInfo.precision) + ")";
            } else {
                sql += "decimal";
            }
        } else if (dataType == "date") {
            sql += "timestamp";
        } else if (dataType.find("timestamp") != std::string::npos) {
            sql += "timestamp";
        } else {
            sql += "text"; // fallback type
        }

        if (!colInfo.nullable) {
            sql += " not null";
        }
    }

    const std::string fullTable = schema + "." + table;
    const auto& pkMap = FilterConfigLoader::getInstance().getPrimaryKeyColumns();
    auto it = pkMap.find(fullTable);
    if (it != pkMap.end()) {
        std::string pkLower = SQLUtils::toLower(it->second);
        sql += ",\n  primary key (" + pkLower + ")";
    }

    sql += "\n);";
    return sql;
}
