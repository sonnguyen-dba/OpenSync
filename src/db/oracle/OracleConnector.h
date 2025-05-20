#ifndef ORACLE_CONNECTOR_H
#define ORACLE_CONNECTOR_H

#include "../../db/DBConnector.h"
#include "OracleColumnInfo.h"
#include <occi.h>
#include "map"
#include "mutex"
#include "memory"
#include "string"

class OracleConnector : public DBConnector {
public:
    OracleConnector(const std::string& host, int port, 
                    const std::string& user, const std::string& password,
                    const std::string& service);

    //wallet
    /*
    OracleConnector(const std::string& tnsAlias);
    */

    ~OracleConnector();

    bool connect() override;
    bool reconnect();
    void disconnect() override;
    bool isConnected() override;
    bool executeQuery(const std::string& sql) override;
    bool executeBatchQuery(const std::vector<std::string>& sqlBatch) override;

    std::unique_ptr<DBConnector> clone() const override;
    //oracle::occi::Connection* getConnection() const { return conn; }
    oracle::occi::Connection* getConnection() const;

    std::map<std::string, std::string> getColumnTypes(const std::string& fullTableName);
    std::map<std::string, OracleColumnInfo> getFullColumnInfo(const std::string& fullTableName);
    void logStatementMemoryUsage();

    std::vector<std::map<std::string, std::string>> queryTableWithOffset(
    const std::string& schema, const std::string& table, int offset, int limit);


private:
    std::string host;
    int port;
    std::string user;
    std::string password;
    std::string service;
    
    //wallet
    //std::string tnsAlias;

    oracle::occi::Environment* env;
    oracle::occi::Connection* conn;

    std::mutex connMutex;
    bool connected = false;
};

#endif