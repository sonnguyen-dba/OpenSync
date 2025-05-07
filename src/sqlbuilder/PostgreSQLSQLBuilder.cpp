#include "PostgreSQLSQLBuilder.h"
#include "../utils/SQLUtils.h"
#include "../schema/postgresql/PostgreSQLSchemaCache.h"
#include "../config/FilterConfigLoader.h"
#include "../logger/Logger.h"
#include <sstream>

PostgreSQLSQLBuilder::PostgreSQLSQLBuilder(const ConfigLoader& config, bool enableISODebugLog)
    : config(config), enableISODebugLog(enableISODebugLog) {}

std::string PostgreSQLSQLBuilder::buildInsertSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data) {
    std::ostringstream sql, columns, values;
    std::string fullTable = schema + "." + table;

    const auto& columnInfoMap = PostgreSQLSchemaCache::getInstance().getColumnInfo(fullTable);
    const auto pkMap = FilterConfigLoader::getInstance().getPrimaryKeyColumns();

    std::string pk = pkMap.count(fullTable) ? pkMap.at(fullTable) : "";

    bool first = true;
    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it) {
        const std::string col = it->name.GetString();
        const auto& val = it->value;

        if (!first) {
            columns << ", ";
            values << ", ";
        }
        first = false;

        columns << "\"" << col << "\"";
        //values << SQLUtils::safeConvert("postgresql", fullTable, col, val);
	      values << SQLUtils::safeConvert("postgresql", fullTable, col, val, false); // ✅ rõ ràng
    }

    sql << "INSERT INTO " << fullTable << " (" << columns.str() << ") VALUES (" << values.str() << ")";
    if (!pk.empty()) {
        sql << " ON CONFLICT (\"" << pk << "\") DO NOTHING";
    }

    return sql.str();
}

std::string PostgreSQLSQLBuilder::buildUpdateSQL(const std::string& schema, const std::string& table,
                                                 const rapidjson::Value& data, const std::string& primaryKey) {
    std::ostringstream sql, setClause;
    std::string fullTable = schema + "." + table;
    std::string pkValue;

    const auto& columnInfoMap = PostgreSQLSchemaCache::getInstance().getColumnInfo(fullTable);

    bool first = true;
    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it) {
        const std::string col = it->name.GetString();
        const auto& val = it->value;

        if (col == primaryKey) {
            //pkValue = SQLUtils::safeConvert("postgresql", fullTable, col, val);
	          pkValue = SQLUtils::safeConvert("postgresql", fullTable, col, val, false); // ✅ rõ ràng
            continue;
        }

        if (!first) setClause << ", ";
        first = false;

        //setClause << "\"" << col << "\" = " << SQLUtils::safeConvert("postgresql", fullTable, col, val);
	      setClause << "\"" << col << "\" = " << SQLUtils::safeConvert("postgresql", fullTable, col, val, false); // ✅ rõ ràng
    }

    sql << "UPDATE " << fullTable << " SET " << setClause.str()
        << " WHERE \"" << primaryKey << "\" = " << pkValue;

    return sql.str();
}

std::string PostgreSQLSQLBuilder::buildDeleteSQL(const std::string& schema, const std::string& table,
                                                 const rapidjson::Value& before, const std::string& primaryKey) {
    std::ostringstream sql;
    std::string fullTable = schema + "." + table;

    if (!before.HasMember(primaryKey.c_str())) {
        Logger::warn("PostgreSQLSQLBuilder: missing PK '" + primaryKey + "' in delete row for table " + fullTable);
        return "";
    }

    const auto& pkVal = before[primaryKey.c_str()];
    //std::string pkValue = SQLUtils::safeConvert("postgresql", fullTable, primaryKey, pkVal);
    std::string pkValue = SQLUtils::safeConvert("postgresql", fullTable, primaryKey, pkVal, false);

    sql << "DELETE FROM " << fullTable << " WHERE \"" << primaryKey << "\" = " << pkValue;
    return sql.str();
}
