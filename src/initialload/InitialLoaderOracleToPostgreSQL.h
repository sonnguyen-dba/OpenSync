#pragma once

#include "../reader/ConfigLoader.h"
#include "../db/DBConnector.h"
#include "../db/oracle/OracleConnector.h"
#include "../db/postgresql/PostgreSQLConnector.h"
#include <memory>
#include <string>
#include <vector>
#include <map>

class InitialLoaderOracleToPostgreSQL {
public:
    explicit InitialLoaderOracleToPostgreSQL(const ConfigLoader& config);

    // Gọi nếu cấu hình "initial-load" bật
    void runAllTablesIfEnabled();

    // Chạy load dữ liệu cho 1 bảng cụ thể
    bool runInitialLoadForTable(
        const std::string& schema,
        const std::string& table,
        int batchSize
    );

private:
    const ConfigLoader& config;
    std::shared_ptr<OracleConnector> oracle;
    std::shared_ptr<PostgreSQLConnector> postgres;
    void createAllPostgreSQLTables();
};

