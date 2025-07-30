#include "SchemaMapper.h"
#include "OracleTypeMapper.h"  // ✅ thêm dòng này
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

        // ✅ dùng mapper mới
        sql += OracleTypeMapper::mapToPostgreSQL(colInfo);

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
