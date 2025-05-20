#include "WriteDataToDB.h"
#include "../logger/Logger.h"
#include "MetricsExporter.h"
#include "../db/oracle/OracleConnector.h"
#include "../sqlbuilder/PostgreSQLSQLBuilder.h"
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

    // Nếu thất bại với PostgreSQL do duplicate key thì thử fallback sang UPSERT
    if (!result && dbType == "postgresql") {
        OpenSync::Logger::warn("⚠️ Detected PostgreSQL batch failure. Attempting UPSERT fallback...");
        for (const auto& sql : sqlBatch) {
            // Giả sử SQL là dạng INSERT, thì có thể gọi lại buildUpsertSQL (nếu bạn giữ payload để rebuild)
            OpenSync::Logger::debug("🔄 Fallback UPSERT for: " + sql);
            // 👉 Ở đây bạn cần cơ chế giữ lại `rapidjson::Value data` gốc để rebuild UPSERT từ builder
            // Giả sử bạn có: std::string schema, table, và data (value)
             //std::string upsertSQL = PostgreSQLSQLBuilder::buildUpsertSQL(schema, table, data);
             //dbConnector->executeQuery(upsertSQL);
        }
        // Tùy thuộc vào xử lý từng dòng, bạn có thể chọn return true nếu ít nhất 1 dòng thành công
    }
    if (dbType == "oracle") {
        if (auto* oracleConn = dynamic_cast<OracleConnector*>(dbConnector)) {
            oracleConn->logStatementMemoryUsage();
        }
    }

    {
        std::lock_guard<std::mutex> lock(bufferMutex);
        auto it = tableSQLBuffer.find(tableKey);
        if (it != tableSQLBuffer.end()) {
            auto& buf = it->second;
            size_t oldCap = buf.capacity();
            size_t oldSize = buf.size();
            std::vector<std::string>().swap(buf);
            tableSQLBuffer.erase(it);
            OpenSync::Logger::info("🧽 Flushed and cleaned buffer for table: " + tableKey +
                ", oldSize=" + std::to_string(oldSize) +
                ", released capacity=" + std::to_string(oldCap));
            MetricsExporter::getInstance().setGauge("table_sql_buffer_size", 0, {{"table", tableKey}});
        }
    }

    return result;
}

/*
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

    if (!result && dbType == "postgresql") {
        OpenSync::Logger::warn("⚠️ PostgreSQL batch insert failed. Attempting UPSERT fallback...");

        std::vector<std::string> fallbackUpserts;
        for (const auto& insertSQL : sqlBatch) {
            OpenSync::Logger::warn("⚠️ Skipping fallback UPSERT because original JSON data not available: " + insertSQL);
            // TODO: Replace with actual upsert if original JSON is preserved
        }

        if (!fallbackUpserts.empty()) {
            result = dbConnector->executeBatchQuery(fallbackUpserts);
        }
    }

    if (dbType == "oracle") {
        if (auto* oracleConn = dynamic_cast<OracleConnector*>(dbConnector)) {
            oracleConn->logStatementMemoryUsage();
        }
    }

    {
        std::lock_guard<std::mutex> lock(bufferMutex);
        auto it = tableSQLBuffer.find(tableKey);
        if (it != tableSQLBuffer.end()) {
            auto& buf = it->second;
            size_t oldCap = buf.capacity();
            size_t oldSize = buf.size();
            std::vector<std::string>().swap(buf);
            tableSQLBuffer.erase(it);
            OpenSync::Logger::info("🧽 Flushed and cleaned buffer for table: " + tableKey +
                ", oldSize=" + std::to_string(oldSize) +
                ", released capacity=" + std::to_string(oldCap));
            MetricsExporter::getInstance().setGauge("table_sql_buffer_size", 0, {{"table", tableKey}});
        }
    }

    return result;
}
*/
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

std::unordered_map<std::string, std::vector<std::string>> WriteDataToDB::drainTableSQLBuffers() {
    std::lock_guard<std::mutex> lock(tableBufferMutex);

    std::unordered_map<std::string, std::vector<std::string>> drained;
    drained.swap(tableSQLBuffer);

    size_t totalSQLs = 0;
    for (auto& [table, sqls] : drained) {
        totalSQLs += sqls.size();
        sqls.shrink_to_fit();
        OpenSync::Logger::debug("Drained tableSQLBuffer for " + table + ", size: " + std::to_string(sqls.size()) + " SQLs");
    }

    if (!drained.empty()) {
        OpenSync::Logger::debug("🧹🧹Drained tableSQLBuffer, tables: " + std::to_string(drained.size()));
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

