#pragma once

#include "SQLBuilderBase.h"
#include "../reader/ConfigLoader.h"

class PostgreSQLSQLBuilder : public SQLBuilderBase {
public:
    //explicit PostgreSQLSQLBuilder(const ConfigLoader& config);
    PostgreSQLSQLBuilder(const ConfigLoader& config, bool enableISODebugLog);
    std::string buildInsertSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data) override;
    std::string buildUpsertSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data);

    std::string buildUpdateSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data, const std::string& primaryKey) override;
    std::string buildDeleteSQL(const std::string& schema, const std::string& table, const rapidjson::Value& before, const std::string& primaryKey) override;

private:
    const ConfigLoader& config;
    bool enableISODebugLog = false;
};
