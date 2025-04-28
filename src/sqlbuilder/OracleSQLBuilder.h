#pragma once
#include "SQLBuilderBase.h"
#include "../config/ConfigLoader.h"

class OracleSQLBuilder : public SQLBuilderBase {
public:
    OracleSQLBuilder(ConfigLoader& config, bool enableISODebugLog);
    std::string buildInsertSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data) override;
    std::string buildUpdateSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data, const std::string& primaryKey) override;
    std::string buildDeleteSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data, const std::string& primaryKey) override;

private:
    ConfigLoader& config;
    bool enableISODebugLog = false;
    int timestamp_unit;
};
