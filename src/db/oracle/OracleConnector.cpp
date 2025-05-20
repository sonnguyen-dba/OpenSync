#include "OracleConnector.h"
#include "../../logger/Logger.h"
#include "../../common/TimeUtils.h"
#include "../../common/date.h"
#include "SQLUtils.h"
#include "MetricsExporter.h"
#include "OracleSchemaCache.h"
#include "../DBException.h"
#include <iostream>
#include <algorithm>
#include <sstream>
#include <regex>
#include <occi.h> 

using namespace oracle::occi;
// Assume OracleConnector class and necessary includes
using oracle::occi::Statement;
using oracle::occi::ResultSet;
using oracle::occi::MetaData;

OracleConnector::OracleConnector(const std::string& host, int port,
                                 const std::string& user, const std::string& password,
                                 const std::string& service)
    : host(host), port(port), user(user), password(password), service(service), env(nullptr), conn(nullptr) {}

/*
 * This functions for wallet
 *
 
 OracleConnector::OracleConnector(const std::string& tnsAlias)
    : tnsAlias(tnsAlias), env(nullptr), conn(nullptr) {}


std::unique_ptr<DBConnector> OracleConnector::clone() const {
    return std::make_unique<OracleConnector>(tnsAlias);
}*/

std::unique_ptr<DBConnector> OracleConnector::clone() const {
    return std::make_unique<OracleConnector>(host, port, user, password, service);
}

OracleConnector::~OracleConnector() {
    disconnect();
}

bool OracleConnector::connect() {
    try {
        env = Environment::createEnvironment(Environment::DEFAULT);
        conn = env->createConnection(user, password, "//" + host + ":" + std::to_string(port) + "/" + service);
	OpenSync::Logger::info("✅ Connected to Oracle successfully!");
        return true;
    } catch (SQLException& e) {
	OpenSync::Logger::error("❌ Oracle connection failed: " + std::string(e.getMessage()));
        return false;
    }
}
/*
bool OracleConnector::connect() {
    try {
        env = Environment::createEnvironment(Environment::DEFAULT);
        conn = env->createConnection("", "", tnsAlias); // Không cần user/password
        OpenSync::Logger::info("✅ Connected to Oracle successfully using wallet!");
        return true;
    } catch (SQLException& e) {
        OpenSync::Logger::error("❌ Oracle connection failed: " + std::string(e.getMessage()));
        return false;
    }
}*/

void OracleConnector::disconnect() {
    if (conn) {
        env->terminateConnection(conn);
        Environment::terminateEnvironment(env);
        conn = nullptr;
        env = nullptr;
	OpenSync::Logger::info("🔌 Disconnected from Oracle.");
    }
}

bool OracleConnector::isConnected() {
    return conn != nullptr;
}

bool OracleConnector::reconnect() {
    OpenSync::Logger::warn("🔄 Connection lost. Attempting to reconnect...");

    disconnect();  // 🛑 Đóng kết nối cũ trước khi tạo kết nối mới

    if (connect()) {
	OpenSync::Logger::info("✅ Reconnected to Oracle successfully!");
        return true;
    }

    OpenSync::Logger::error("❌ Reconnection failed.");
    return false;
}

bool OracleConnector::executeQuery(const std::string& sql) {
    if (!isConnected()) return false;

    try {
        Statement* stmt = conn->createStatement(sql);
        stmt->executeUpdate();
        conn->commit();
	conn->terminateStatement(stmt);
        return true;
    } catch (SQLException& e) {
	OpenSync::Logger::error("❌ Oracle query failed: " + std::string(e.getMessage()));
        return false;
    }
}

bool OracleConnector::executeBatchQuery(const std::vector<std::string>& sqlBatch) {
    if (!isConnected()) return false;

    Statement* stmt = nullptr;

    try {
        stmt = conn->createStatement();
        int successCount = 0;
        int skippedCount = 0;

        for (const auto& sql : sqlBatch) {
            try {
                stmt->executeUpdate(sql);
                successCount++;
            } catch (SQLException& e) {
                std::string errMsg = e.getMessage();
                int errCode = e.getErrorCode();
                DBExecResult result = DBExceptionHelper::classifyOracleError(errCode, errMsg);

                std::string tableKey = SQLUtils::extractTableFromInsert(sql);

                if (result == DBExecResult::DUPLICATE_PK) {
                    OpenSync::Logger::warn("⚠️ ORA-00001: Duplicate PK. Skipping row.");
                    MetricsExporter::getInstance().incrementCounter("oracle_duplicate_pk_skipped", {
                        {"table", tableKey}
                    });
                    skippedCount++;
                    continue;
                } else if (result == DBExecResult::INVALID_DATA) {
                    OpenSync::Logger::warn("⚠️ ORA-01839 or similar: Invalid date/data. Skipping row.");
                    MetricsExporter::getInstance().incrementCounter("oracle_invalid_data_skipped", {
                        {"table", tableKey},
                        {"error", DBExceptionHelper::toString(result)}
                    });
                    skippedCount++;
                    continue;
                } else {
                    OpenSync::Logger::error("❌ SQL execution failed: " + errMsg);
                    MetricsExporter::getInstance().incrementCounter("oracle_batch_failed", {
                        {"error", DBExceptionHelper::toString(result)}
                    });
                    conn->terminateStatement(stmt);
                    conn->rollback();  // ⚠️ Lỗi nghiêm trọng → rollback toàn batch
                    return false;
                }
            }
        }

        conn->commit();  // ✅ Commit các lệnh thành công
        conn->terminateStatement(stmt);

        if (successCount > 0) {
            //OpenSync::Logger::debug("✅ Batch executed with " + std::to_string(successCount) + " successes, " + std::to_string(skippedCount) + " skipped.");
            return true;
        } else {
            OpenSync::Logger::warn("⚠️ All rows skipped. Nothing committed.");
            return true;  // ✅ Không lỗi, nhưng toàn bộ bị skip
        }

    } catch (SQLException& e) {
        OpenSync::Logger::error("❌ Batch execution failed: " + std::string(e.getMessage()));
        conn->rollback();
        if (stmt) conn->terminateStatement(stmt);
        return false;
    }
}

oracle::occi::Connection* OracleConnector::getConnection() const {
    return conn;
}

std::map<std::string, OracleColumnInfo> OracleConnector::getFullColumnInfo(const std::string& fullTableName) {
    std::map<std::string, OracleColumnInfo> result;

    std::string owner, table;
    auto pos = fullTableName.find('.');
    if (pos != std::string::npos) {
        owner = fullTableName.substr(0, pos);
        table = fullTableName.substr(pos + 1);
    } else {
	OpenSync::Logger::error("❌ Invalid table name format (expect OWNER.TABLE): " + fullTableName);
        return result;
    }

    std::string query = R"(
        SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = :1 AND TABLE_NAME = :2
    )";

    oracle::occi::Statement* stmt = conn->createStatement(query);
    stmt->setString(1, owner);
    stmt->setString(2, table);

    oracle::occi::ResultSet* rs = stmt->executeQuery();
    while (rs->next()) {
        std::string colName = rs->getString(1);
	std::transform(colName.begin(), colName.end(), colName.begin(), ::toupper);

        OracleColumnInfo info;
        info.dataType = rs->getString(2);
        info.dataLength = rs->isNull(3) ? -1 : rs->getInt(3);
        info.precision = rs->isNull(4) ? -1 : rs->getInt(4);
        info.scale = rs->isNull(5) ? -1 : rs->getInt(5);
        info.nullable = (rs->getString(6) == "Y");

        result[colName] = info;

        // 🔍 Log kiểu dữ liệu đầy đủ
        //OpenSync::Logger::debug("   ↪️ " + colName + " : " + info.getFullTypeString());
    }

    stmt->closeResultSet(rs);
    conn->terminateStatement(stmt);

    return result;
}

void OracleConnector::logStatementMemoryUsage() {
    std::lock_guard<std::mutex> lock(connMutex);  // 🧠 Đảm bảo bạn có `connMutex`
    if (!connected || !conn) return;

    try {
        std::unique_ptr<Statement> stmt(conn->createStatement(
            "SELECT name, value FROM v$sesstat "
            "JOIN v$statname USING (statistic#) "
            "WHERE sid = (SELECT sid FROM v$mystat WHERE rownum = 1) "
            "AND name IN ('session pga memory', 'session uga memory')"
        ));
        ResultSet* rs = stmt->executeQuery();
        while (rs->next()) {
            std::string name = rs->getString(1);
            int64_t value = static_cast<int64_t>(rs->getInt(2));
	    OpenSync::Logger::debug("[OracleMem] " + name + ": " + std::to_string(value) + " bytes");
        }
        stmt->closeResultSet(rs);
        conn->terminateStatement(stmt.release());
    } catch (SQLException& e) {
	    OpenSync::Logger::error("Oracle mem log error: " + std::string(e.getMessage()));
    }
}

std::vector<std::map<std::string, std::string>> OracleConnector::queryTableWithOffset(
    const std::string& schema, const std::string& table, int offset, int limit) {

    std::vector<std::map<std::string, std::string>> result;
    if (!conn) return result;

    try {
        // Truy vấn metadata để biết cột nào là DATE/TIMESTAMP
        std::string metaQuery =
            "SELECT column_name, data_type FROM all_tab_columns "
            "WHERE owner = UPPER('" + schema + "') AND table_name = UPPER('" + table + "')";

        Statement* metaStmt = conn->createStatement(metaQuery);
        ResultSet* metaRs = metaStmt->executeQuery();

        std::map<std::string, std::string> columnTypes;  // colName → data_type
        while (metaRs->next()) {
            std::string col = metaRs->getString(1);
            std::string type = metaRs->getString(2);
            columnTypes[col] = type;
        }
        metaStmt->closeResultSet(metaRs);
        conn->terminateStatement(metaStmt);

        // ✅ Tạo SELECT ... với TO_CHAR cho DATE/TIMESTAMP
        std::string selectClause = "SELECT ";
        int colIdx = 0;
        for (const auto& [col, type] : columnTypes) {
            if (colIdx++ > 0) selectClause += ", ";
            if (type == "DATE")
                selectClause += "TO_CHAR(" + col + ", 'YYYY-MM-DD HH24:MI:SS') AS " + col;
            else if (type.find("TIMESTAMP") != std::string::npos)
                selectClause += "TO_CHAR(" + col + ", 'YYYY-MM-DD HH24:MI:SS.FF6') AS " + col;
            else
                selectClause += col;
        }

        std::string query = selectClause + " FROM " + schema + "." + table +
                            " OFFSET " + std::to_string(offset) +
                            " ROWS FETCH NEXT " + std::to_string(limit) + " ROWS ONLY";

        Statement* stmt = conn->createStatement(query);
        ResultSet* rs = stmt->executeQuery();
        std::vector<MetaData> meta = rs->getColumnListMetaData();
        int colCount = meta.size();

        while (rs->next()) {
            std::map<std::string, std::string> row;
            for (int i = 1; i <= colCount; ++i) {
                std::string colName = meta[i - 1].getString(MetaData::ATTR_NAME);
                std::string val = rs->isNull(i) ? "NULL" : rs->getString(i);
                row[colName] = val;
            }
            result.push_back(std::move(row));
        }

        stmt->closeResultSet(rs);
        conn->terminateStatement(stmt);
    } catch (SQLException& e) {
	 OpenSync::Logger::error("❌ queryTableWithOffset failed: " + std::string(e.getMessage()));
    }

    return result;
}