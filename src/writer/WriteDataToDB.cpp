#include "WriteDataToDB.h"
#include "../logger/Logger.h"
#include "MetricsExporter.h"
#include "../db/oracle/OracleConnector.h"
#include <malloc.h>

WriteDataToDB::WriteDataToDB() {}

WriteDataToDB::~WriteDataToDB() {
    dbConnectorPools.clear();
    tableSQLBuffer.clear();
}

void WriteDataToDB::addDatabaseConnectorFactory(const std::string& dbType, std::function<std::unique_ptr<DBConnector>()> factory) {
    connectorFactories[dbType] = std::move(factory);
}

DBConnector* WriteDataToDB::getConnectorForThread(const std::string& dbType) {
    std::thread::id threadId = std::this_thread::get_id();
    std::lock_guard<std::mutex> lock(connectorPoolMutex);

    auto& threadMap = dbConnectorPools[dbType];
    if (threadMap.find(threadId) == threadMap.end()) {
        if (connectorFactories.find(dbType) == connectorFactories.end()) {
	          OpenSync::Logger::error("No factory registered for DB type: " + dbType);
            return nullptr;
        }
        threadMap[threadId] = connectorFactories[dbType]();
    }
    return threadMap[threadId].get();
}

size_t WriteDataToDB::estimateMemoryUsage() const {
    std::lock_guard<std::mutex> lock(tableBufferMutex);
    size_t total = 0;
    for (const auto& [table, vec] : tableSQLBuffer) {
        for (const auto& sql : vec) {
            total += sql.capacity();
        }
    }
    return total;
}

size_t WriteDataToDB::getActiveTableCount() const {
    std::lock_guard<std::mutex> lock(tableBufferMutex);
    return tableSQLBuffer.size();
}

bool WriteDataToDB::writeToDB(const std::string& dbType, const std::vector<std::string>& sqlQueries) {
    DBConnector* dbConnector = getConnectorForThread(dbType);
    if (!dbConnector) return false;

    if (!dbConnector->isConnected() && !dbConnector->connect()) {
	       OpenSync::Logger::error("Failed to connect to " + dbType);
         return false;
    }

    bool success = true;
    for (const auto& sql : sqlQueries) {
        if (!dbConnector->executeQuery(sql)) {
            success = false;
	          OpenSync::Logger::error("❌ SQL execution failed: " + sql);
        }
    }

    return success;
}

bool WriteDataToDB::writeBatchToDB(const std::string& dbType,
                                   const std::vector<std::string>& sqlBatch,
                                   const std::string& tableKey) {
    DBConnector* dbConnector = getConnectorForThread(dbType);
    if (!dbConnector) return false;

    if (!dbConnector->isConnected() && !dbConnector->connect()) {
	      OpenSync::Logger::error("Failed to connect to " + dbType);
        return false;
    }

    bool result = dbConnector->executeBatchQuery(sqlBatch);

    // 📌 Logging session memory sau khi ghi batch
    if (dbType == "oracle") {
        if (auto* oracleConn = dynamic_cast<OracleConnector*>(dbConnector)) {
            oracleConn->logStatementMemoryUsage();
        }
    }

    // ✅ Clean + shrink tableSQLBuffer sau khi flush xong
    {
        std::lock_guard<std::mutex> lock(bufferMutex);
        auto it = tableSQLBuffer.find(tableKey);
        if (it != tableSQLBuffer.end()) {
            auto& buf = it->second;
            size_t oldCap = buf.capacity();
            size_t oldSize = buf.size();
            std::vector<std::string>().swap(buf);  // shrink to fit
            tableSQLBuffer.erase(it); // hoặc giữ lại nếu cần reuse
	          OpenSync::Logger::info("🧽 Flushed and cleaned buffer for table: " + tableKey +
                         ", oldSize=" + std::to_string(oldSize) +
                         ", released capacity=" + std::to_string(oldCap));
            MetricsExporter::getInstance().setGauge("table_sql_buffer_size", 0, {{"table", tableKey}});
        }
    }

    return result;
}

std::unique_ptr<DBConnector> WriteDataToDB::cloneConnector(const std::string& dbType) {
    if (connectorFactories.find(dbType) == connectorFactories.end()) {
	      OpenSync::Logger::error("Cannot clone: No factory registered for DB type " + dbType);
        return nullptr;
    }
    return connectorFactories[dbType]();
}

std::mutex& WriteDataToDB::getTableMutex(const std::string& tableKey) {
    std::lock_guard<std::mutex> lock(mutexMapLock);
    if (tableMutexMap.find(tableKey) == tableMutexMap.end()) {
	     OpenSync::Logger::info("🧵 Creating mutex for table: " + tableKey);
    }
    return tableMutexMap[tableKey];
}

void WriteDataToDB::reportTableSQLBufferMetrics() {
    std::lock_guard<std::mutex> lock(tableBufferMutex);

    for (const auto& [table, buffer] : tableSQLBuffer) {
        size_t totalBytes = 0;
        for (const auto& sql : buffer) {
            totalBytes += sql.capacity();
        }

        MetricsExporter::getInstance().setGauge("table_sql_buffer_count", buffer.size(), {{"table", table}});
        MetricsExporter::getInstance().setGauge("table_sql_buffer_size_bytes", totalBytes, {{"table", table}});
    }
}

void WriteDataToDB::addToTableSQLBuffer(const std::string& tableKey, const std::string& sql) {
    std::lock_guard<std::mutex> lock(tableBufferMutex);
    OpenSync::Logger::info("addToTableSQLBuffer called for " + tableKey + ", SQL size: " + std::to_string(sql.size()) + " bytes");
    tableSQLBuffer[tableKey].push_back(sql);
}

/*std::unordered_map<std::string, std::vector<std::string>> WriteDataToDB::drainTableSQLBuffers() {
    std::lock_guard<std::mutex> lock(tableBufferMutex);
    std::unordered_map<std::string, std::vector<std::string>> drained;
    drained.swap(tableSQLBuffer); // Chuyển nội dung sang drained, tableSQLBuffer rỗng
    for (auto& [table, sqls] : drained) {
        OpenSync::Logger::info("Drained tableSQLBuffer for " + table + ", size: " + std::to_string(sqls.size()) + " SQLs");
        sqls.shrink_to_fit(); // Thu gọn vector<std::string>
    }
    // Thu gọn tableSQLBuffer bằng cách swap với map mới
    std::unordered_map<std::string, std::vector<std::string>> emptyMap;
    tableSQLBuffer.swap(emptyMap);
    OpenSync::Logger::info("Drained tableSQLBuffer, tables: " + std::to_string(drained.size()));
    return drained;
}*/

std::unordered_map<std::string, std::vector<std::string>> WriteDataToDB::drainTableSQLBuffers() {
    std::lock_guard<std::mutex> lock(tableBufferMutex);

    // Chuyển ownership sang drained
    std::unordered_map<std::string, std::vector<std::string>> drained;
    drained.swap(tableSQLBuffer); // tableSQLBuffer sẽ tự động rỗng sau khi swap

    size_t totalSQLs = 0;
    for (auto& [table, sqls] : drained) {
        totalSQLs += sqls.size();
        sqls.shrink_to_fit(); // thu gọn bộ nhớ từng vector
        OpenSync::Logger::debug("Drained tableSQLBuffer for " + table + ", size: " + std::to_string(sqls.size()) + " SQLs");
    }

    if (!drained.empty()) {
        OpenSync::Logger::debug" 🧹 Drained tableSQLBuffer, tables: " + std::to_string(drained.size()));
    }

    return drained;
}

void WriteDataToDB::reportMemoryUsagePerDBType() {
    std::lock_guard<std::mutex> lock(connectorPoolMutex);
    for (const auto& [dbType, threadMap] : dbConnectorPools) {
        size_t count = threadMap.size();
        MetricsExporter::getInstance().setGauge("db_connector_pool_size", count, {
            {"db_type", dbType}
        });
    }
}
