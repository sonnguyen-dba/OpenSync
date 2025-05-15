#ifndef ORACLE_CONNECTOR_H
#define ORACLE_CONNECTOR_H

#include "../../dbconnector/DBConnector.h"
#include "OracleColumnInfo.h"
#include <occi.h>
#include "map"
#include "mutex"
#include "memory"
#include "string"

class OracleConnector : public DBConnector {
public:
    //wallet
    /*OracleConnector(const std::string& tnsAlias);*/

    OracleConnector(const std::string& host, int port,
                    const std::string& user, const std::string& password,
                    const std::string& service);
    ~OracleConnector();

    std::unique_ptr<DBConnector> clone() const override;

    bool connect() override;
    bool reconnect();
    void disconnect() override;
    bool isConnected() override;
    bool executeQuery(const std::string& sql) override;
    bool executeBatchQuery(const std::vector<std::string>& sqlBatch) override;

    oracle::occi::Connection* getConnection() const;

    std::map<std::string, std::string> getColumnTypes(const std::string& fullTableName);
    std::map<std::string, OracleColumnInfo> getFullColumnInfo(const std::string& fullTableName);
    void logStatementMemoryUsage();

private:
    //wallet
    /*std::string tnsAlias;*/
    std::string host;
    int port;
    std::string user;
    std::string password;
    std::string service;

    oracle::occi::Environment* env;
    oracle::occi::Connection* conn;

    std::mutex connMutex;
    bool connected = false;
};

#endif
