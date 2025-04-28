#pragma once
#include <string>
#include <rapidjson/document.h>

class SQLBuilderBase {
public:
    virtual ~SQLBuilderBase() = default;

    virtual std::string buildInsertSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data) = 0;
    virtual std::string buildUpdateSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data, const std::string& primaryKey) = 0;
    virtual std::string buildDeleteSQL(const std::string& schema, const std::string& table, const rapidjson::Value& data, const std::string& primaryKey) = 0;
};

