#include "PostgreSQLSQLBuilder.h"
#include "../utils/SQLUtils.h"
#include "../schema/PostgreSQLSchemaCache.h"
#include "../reader/FilterConfigLoader.h"
#include "../logger/Logger.h"
#include <sstream>
#include <algorithm>

PostgreSQLSQLBuilder::PostgreSQLSQLBuilder(const ConfigLoader& config, bool enableISODebugLog)
    : config(config), enableISODebugLog(enableISODebugLog) {}

std::string PostgreSQLSQLBuilder::buildInsertSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data) {
    auto start = std::chrono::steady_clock::now();

    std::ostringstream sql, columns, values;
    std::string lowerSchema = SQLUtils::toLower(schema);
    std::string lowerTable = SQLUtils::toLower(table);
    std::string fullTable = lowerSchema + "." + lowerTable;
    
    auto t_schemaStart = std::chrono::steady_clock::now();
    const auto& columnInfoMap = PostgreSQLSchemaCache::getInstance().getColumnInfo(fullTable);
    (void) columnInfoMap;
    auto t_schemaEnd = std::chrono::steady_clock::now();

    auto t_convertStart = std::chrono::steady_clock::now();
    const auto pkMap = FilterConfigLoader::getInstance().getPrimaryKeyColumns();
    std::string pk = pkMap.count(fullTable) ? SQLUtils::toLower(pkMap.at(fullTable)) : "";

    bool first = true;
    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it) {
        std::string col = SQLUtils::toLower(it->name.GetString());
        const auto& val = it->value;

        if (!first) {
            columns << ", ";
            values << ", ";
        }
        first = false;

        columns << col;
        values << SQLUtils::safeConvert("postgresql", fullTable, col, val, false);
    }

    sql << "insert into " << fullTable << " (" << columns.str() << ") values (" << values.str() << ")";
    if (!pk.empty()) {
        sql << " ON CONFLICT (" << pk << ") DO NOTHING";
    }

    auto t_convertEnd = std::chrono::steady_clock::now();

    auto end = std::chrono::steady_clock::now();

    OpenSync::Logger::debug("✅ SQLBuilder buildInsertSQL timing for " + fullTable +
        " | schema: " + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(t_schemaEnd - t_schemaStart).count()) + " ms" +
        " | convert: " + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(t_convertEnd - t_convertStart).count()) + " ms" +
        " | total: " + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()) + " ms");

    OpenSync::Logger::debug("sql:" + sql.str());
    return sql.str();
}

std::string PostgreSQLSQLBuilder::buildUpsertSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data) {
    std::string fullTable = SQLUtils::toLower(schema) + "." + SQLUtils::toLower(table);
    return SQLUtils::buildPostgreSQLUpsertSQL(fullTable, data);
}

std::string PostgreSQLSQLBuilder::buildUpdateSQL(const std::string& schema, const std::string& table,
                                                 const rapidjson::Value& data, const std::string& primaryKey) {
    std::ostringstream sql, setClause;
    std::string lowerSchema = SQLUtils::toLower(schema);
    std::string lowerTable = SQLUtils::toLower(table);
    std::string lowerPK = SQLUtils::toLower(primaryKey);
    std::string fullTable = lowerSchema + "." + lowerTable;

    const auto& columnInfoMap = PostgreSQLSchemaCache::getInstance().getColumnInfo(fullTable);
    (void) columnInfoMap;
    std::string pkValue;
    bool first = true;

    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it) {
        std::string col = SQLUtils::toLower(it->name.GetString());
        const auto& val = it->value;

        if (col == lowerPK) {
            pkValue = SQLUtils::safeConvert("postgresql", fullTable, col, val, false);
            continue;
        }

        if (!first) setClause << ", ";
        first = false;

        setClause << col << " = " << SQLUtils::safeConvert("postgresql", fullTable, col, val, false);
    }

    sql << "update " << fullTable << " set " << setClause.str()
        << " where " << lowerPK << " = " << pkValue;
    return sql.str();
}

std::string PostgreSQLSQLBuilder::buildDeleteSQL(const std::string& schema, const std::string& table,
                                                 const rapidjson::Value& before, const std::string& primaryKey) {
    std::ostringstream sql;
    std::string lowerSchema = SQLUtils::toLower(schema);
    std::string lowerTable = SQLUtils::toLower(table);
    std::string lowerPK = SQLUtils::toLower(primaryKey);
    std::string fullTable = lowerSchema + "." + lowerTable;

    if (!before.HasMember(primaryKey.c_str())) {
        OpenSync::Logger::warn("PostgreSQLSQLBuilder: missing PK '" + primaryKey + "' in delete row for table " + fullTable);
        return "";
    }

    const auto& pkVal = before[primaryKey.c_str()];
    std::string pkValue = SQLUtils::safeConvert("postgresql", fullTable, lowerPK, pkVal, false);

    sql << "delete from " << fullTable << " where " << lowerPK << " = " << pkValue;
    return sql.str();
}
