#ifndef DB_CONNECTOR_H
#define DB_CONNECTOR_H

#include "string"
#include "vector"
#include "memory"

class DBConnector {
public:
    virtual ~DBConnector() = default;
    
    // Kết nối tới database
    virtual bool connect() = 0;
    
    // Ngắt kết nối khỏi database
    virtual void disconnect() = 0;

    // Kiểm tra trạng thái kết nối
    virtual bool isConnected() = 0;

    // Thực thi câu lệnh SQL
    virtual bool executeQuery(const std::string& sql) = 0;
    
    // Thực thi batch query (nhiều SQL cùng lúc)
    virtual bool executeBatchQuery(const std::vector<std::string>& sqlBatch) = 0;
    
    // 
    virtual std::unique_ptr<DBConnector> clone() const = 0;
};

#endif

