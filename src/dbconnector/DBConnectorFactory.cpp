#include "DBConnectorFactory.h"
#include "../logger/Logger.h"

std::unique_ptr<DBConnector> DBConnectorFactory::createConnector(
    const std::string& dbType,
    const std::unordered_map<std::string, std::string>& config
) {
    if (dbType == "oracle") {
        return std::make_unique<OracleConnector>(
            config.at("host"),
            std::stoi(config.at("port")),
            config.at("user"),
            config.at("password"),
            config.at("service")
        );
    }
    else if (dbType == "postgresql") {
        return std::make_unique<PostgreSQLConnector>(
            config.at("host"),
            std::stoi(config.at("port")),
            config.at("user"),
            config.at("password"),
            config.at("dbname")
        );
    }
    /*else if (dbType == "mysql") {
        return std::make_unique<MySQLConnector>(
            config.at("host"),
            std::stoi(config.at("port")),
            config.at("user"),
            config.at("password"),
            config.at("database")
        );
    }
    else if (dbType == "mssql") {
        std::string connStr = "DRIVER={SQL Server};SERVER=" + config.at("host") +
                              "," + config.at("port") +
                              ";DATABASE=" + config.at("database") +
                              ";UID=" + config.at("user") +
                              ";PWD=" + config.at("password");

        return std::make_unique<MSSQLConnector>(connStr);
    }*/

    Logger::error("❌ Unsupported DB type in factory: " + dbType);
    return nullptr;
}
