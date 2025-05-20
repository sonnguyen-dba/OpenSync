#pragma once

#include "../DBConnector.h"
#include <libpq-fe.h>
#include <string>
#include <map>
#include <vector>
#include <mutex>
#include <unordered_map>
#include "../../schema/PostgreSQLColumnInfo.h"

// Cấu trúc để hỗ trợ batch insert động
struct BatchInsert {
    std::string fullTableName; // Ví dụ: public.my_table
    std::vector<std::string> columns; // Danh sách cột
    std::vector<std::vector<std::string>> values; // Giá trị cho mỗi hàng
};

class PostgreSQLConnector : public DBConnector {
public:
    /*PostgreSQLConnector(const std::string& host,
                        int port,
                        const std::string& user,
                        const std::string& password,
                        const std::string& dbname);*/

    PostgreSQLConnector(const std::string& host,
                        int port,
                        const std::string& user,
                        const std::string& password,
                        const std::string& dbname,
                        const std::string& sslmode = "prefer",
                        const std::string& sslrootcert = "",
                        const std::string& sslcert = "",
                        const std::string& sslkey = "");

    ~PostgreSQLConnector();

    bool connect() override;
    void disconnect() override;
    bool isConnected() override;

    bool executeQuery(const std::string& sql) override;
    bool executeBatchQuery(const std::vector<std::string>& sqlBatch) override;
    bool executeBatchQuery(const std::vector<BatchInsert>& batchInserts); // Phương thức mới

    std::unique_ptr<DBConnector> clone() const override;

    std::map<std::string, std::string> getColumnTypes(const std::string& fullTableName);
    std::map<std::string, std::string> getPrimaryKeys(const std::string& fullTableName);

    //std::vector<PostgreSQLColumnInfo> getFullColumnInfo(const std::string& fullTableName);
    std::unordered_map<std::string, PostgreSQLColumnInfo> getFullColumnInfo(const std::string& fullTableName);

    //PGconn* conn = nullptr;  // public for schema cache access (or make friend if needed)

    bool executeStatementSQL(const std::string& sql);
    // Thêm vào public:
    bool tableExists(const std::string& schema, const std::string& table);


private:
    std::string host;
    int port;
    std::string user;
    std::string password;
    std::string dbname;
    std::string sslmode;
    std::string sslrootcert;
    std::string sslcert;
    std::string sslkey;
    PGconn* conn = nullptr;
    std::mutex connMutex;
};

