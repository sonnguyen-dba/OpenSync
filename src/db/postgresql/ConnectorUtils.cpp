// src/utils/ConnectorUtils.cpp
#include "ConnectorUtils.h"

std::unique_ptr<PostgreSQLConnector> createPostgreSQLConnector(const ConfigLoader& config) {
    return std::make_unique<PostgreSQLConnector>(
        config.getDBConfig("postgresql", "host"),
        std::stoi(config.getDBConfig("postgresql", "port")),
        config.getDBConfig("postgresql", "user"),
        config.getDBConfig("postgresql", "password"),
        config.getDBConfig("postgresql", "dbname")
    );
}

