#ifndef DB_CONNECTOR_FACTORY_H
#define DB_CONNECTOR_FACTORY_H

#include "DBConnector.h"
#include "../db/oracle/OracleConnector.h"
//#include "PostgreSQLConnector.h"
//#include "MySQLConnector.h"
//#include "MSSQLConnector.h"
#include <memory>
#include <string>
#include <unordered_map>

class DBConnectorFactory {
public:
    static std::unique_ptr<DBConnector> createConnector(
        const std::string& dbType,
        const std::unordered_map<std::string, std::string>& config
    );
};

#endif

