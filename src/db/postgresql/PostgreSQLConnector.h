#pragma once

#include "DBConnector.h"
#include <libpq-fe.h>
#include <string>
#include <map>
#include <vector>
#include <unordered_map>
#include "../../schema/PostgreSQLColumnInfo.h"

class PostgreSQLConnector : public DBConnector {
public:
    PostgreSQLConnector(const std::string& host,
                        int port,
                        const std::string& user,
                        const std::string& password,
                        const std::string& dbname);

    ~PostgreSQLConnector();

    bool connect() override;
    void disconnect() override;
    bool isConnected() override;

    bool executeQuery(const std::string& sql) override;
    bool executeBatchQuery(const std::vector<std::string>& sqlBatch) override;

    std::unique_ptr<DBConnector> clone() const override;

    std::map<std::string, std::string> getColumnTypes(const std::string& fullTableName);
    std::map<std::string, std::string> getPrimaryKeys(const std::string& fullTableName);

    //std::vector<PostgreSQLColumnInfo> getFullColumnInfo(const std::string& fullTableName);
    std::unordered_map<std::string, PostgreSQLColumnInfo> getFullColumnInfo(const std::string& fullTableName);

    PGconn* conn = nullptr;  // public for schema cache access (or make friend if needed)

private:
    std::string host;
    int port;
    std::string user;
    std::string password;
    std::string dbname;
};

