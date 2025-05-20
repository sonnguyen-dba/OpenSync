#ifndef WRITEDATATODB_H
#define WRITEDATATODB_H

#include "../db/DBConnector.h"
#include "map"
#include "unordered_map"
#include "mutex"
#include "thread"
#include "memory"
#include "vector"
#include "string"
#include "functional"
#include "queue"

class WriteDataToDB {
public:
    WriteDataToDB();
    ~WriteDataToDB();

    void addDatabaseConnectorFactory(const std::string& dbType, std::function<std::unique_ptr<DBConnector>()> factory);
    DBConnector* getConnectorForThread(const std::string& dbType);
    std::unique_ptr<DBConnector> cloneConnector(const std::string& dbType);

    bool writeToDB(const std::string& dbType, const std::vector<std::string>& sqlQueries);
    bool writeBatchToDB(const std::string& dbType, const std::vector<std::string>& sqlBatch);
    bool writeBatchToDB(const std::string& dbType, const std::vector<std::string>& sqlBatch, const std::string& tableKey);

    std::mutex& getTableMutex(const std::string& tableKey);

    // 🔢 Table SQL Buffer APIs
    void addToTableSQLBuffer(const std::string& tableKey, const std::string& sql);
    std::unordered_map<std::string, std::vector<std::string>> drainTableSQLBuffers();
    void reportTableSQLBufferMetrics();

    // 📊 Prometheus: memory usage per thread/db
    void reportMemoryUsagePerDBType();

    size_t estimateMemoryUsage() const;
    size_t getActiveTableCount() const;

private:
    std::map<std::string, std::function<std::unique_ptr<DBConnector>()>> connectorFactories;
    std::map<std::string, std::map<std::thread::id, std::unique_ptr<DBConnector>>> dbConnectorPools;
    std::mutex connectorPoolMutex;

    std::unordered_map<std::string, std::mutex> tableMutexMap;
    std::mutex mutexMapLock;

    //SQL buffer per table
    //std::unordered_map<std::string, std::vector<std::string>> tableSQLBuffer;
    std::mutex bufferMutex;

    //std::mutex tableBufferMutex;
    //std::unordered_map<std::string, std::vector<std::string>> tableSQLBuffer;
    //std::unordered_map<std::string, std::vector<std::string>> tableSQLBuffer;
    mutable std::mutex tableBufferMutex;
    std::unordered_map<std::string, std::vector<std::string>> tableSQLBuffer;
};

#endif // WRITEDATATODB_H

