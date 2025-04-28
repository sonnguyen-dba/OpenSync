#include "OracleConnector.h"
#include "../../logger/Logger.h"
#include "SQLUtils.h"
#include "MetricsExporter.h"
#include "OracleSchemaCache.h"
#include "DBException.h"
#include <iostream>
#include <algorithm>


using namespace oracle::occi;

OracleConnector::OracleConnector(const std::string& host, int port,
                                 const std::string& user, const std::string& password,
                                 const std::string& service)
    : host(host), port(port), user(user), password(password), service(service), env(nullptr), conn(nullptr) {}

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
	Logger::info("✅ Connected to Oracle successfully!");
        return true;
    } catch (SQLException& e) {
	Logger::error("❌ Oracle connection failed: " + std::string(e.getMessage()));
        return false;
    }
}

void OracleConnector::disconnect() {
    if (conn) {
        env->terminateConnection(conn);
        Environment::terminateEnvironment(env);
        conn = nullptr;
        env = nullptr;
	Logger::info("🔌 Disconnected from Oracle.");
    }
}

bool OracleConnector::isConnected() {
    return conn != nullptr;
}

bool OracleConnector::reconnect() {
    Logger::warn("🔄 Connection lost. Attempting to reconnect...");

    disconnect();  // 🛑 Đóng kết nối cũ trước khi tạo kết nối mới

    if (connect()) {
	Logger::info("✅ Reconnected to Oracle successfully!");
        return true;
    }

    Logger::error("❌ Reconnection failed.");
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
	Logger::error("❌ Oracle query failed: " + std::string(e.getMessage()));
        return false;
    }
}

/*bool OracleConnector::executeBatchQuery(const std::vector<std::string>& sqlBatch) {
    if (!isConnected()) return false;

    Statement* stmt = nullptr;

    try {
        // 🛠️ Tạo statement (batch chạy nhanh hơn)
        stmt = conn->createStatement();
	bool success = true;
        for (const auto& sql : sqlBatch) {
	    try {
        	stmt->executeUpdate(sql);  // ✅ Thực thi từng câu SQL
   	     } catch (SQLException& e) {
        	std::string errMsg = e.getMessage();
        	if (errMsg.find("ORA-00001") != std::string::npos) {
            	   LOG_WARNING("⚠️ ORA-00001: Duplicate PK detected.");
                   LOG_WARNING("SQL: " + sql);
        	} else {
                   LOG_ERROR("❌ SQL execution failed: " + errMsg);
                   LOG_ERROR("Query: " + sql);
        	}

        	success = false;
    	     }
        }

	if (!success) {
    	   conn->rollback();
           conn->terminateStatement(stmt);
           LOG_WARNING("⚠️ Batch failed. Rolled back.");
           return false;
	}

        conn->commit();  // ✅ Chỉ commit khi chạy xong tất cả lệnh
        conn->terminateStatement(stmt);  // 🛠️ Giải phóng statement
        LOG_INFO("✅ Batch executed successfully with " + std::to_string(sqlBatch.size()) + " queries.");
        return true;

    } catch (SQLException& e) {
        LOG_ERROR("❌ Batch execution failed: " + std::string(e.getMessage()));
        conn->rollback();  // 🛑 Rollback toàn bộ nếu có lỗi

        if (stmt) conn->terminateStatement(stmt);  // 🛠️ Đảm bảo giải phóng statement nếu có lỗi
        return false;
    }
}*/

/*bool OracleConnector::executeStatementSQL(const std::string& sql) {
    if (!conn || !isConnected()) return false;

    try {
        Statement* stmt = conn->createStatement(sql);
        stmt->executeUpdate();
        conn->commit();
        conn->terminateStatement(stmt);
        return true;
    } catch (const SQLException& ex) {
        if (ex.getErrorCode() == 1) {  // ORA-00001
            LOG_WARNING("⚠️ ORA-00001 (Duplicate PK) — Ignored.");
            return true; // ✅ Bỏ qua lỗi PK
        }
        LOG_ERROR("❌ Oracle executeStatementSQL failed: " + std::string(ex.getMessage()));
        return false;
    }
}*/

/*bool OracleConnector::executeBatchQuery(const std::vector<std::string>& sqlBatch) {
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
                if (errMsg.find("ORA-00001") != std::string::npos) {
		    Logger::warn("⚠️ ORA-00001: Duplicate PK detected. Skipping: ");
                    //LOG_WARNING("SQL: " + sql);
                    // continue mà không rollback
		    // 🔹 Extract table name from SQL (tạm thời đơn giản)
                    std::string tableKey = SQLUtils::extractTableFromInsert(sql);
                    MetricsExporter::getInstance().incrementCounter("oracle_duplicate_pk_skipped", {{"table", tableKey}});

                    skippedCount++;
                    continue; // ✅ Bỏ qua câu lỗi, không đánh fail toàn batch

                } else {
		    Logger::error("❌ SQL execution failed: " + errMsg);
                    //LOG_ERROR("Query: " + sql);
                    conn->terminateStatement(stmt);
                    conn->rollback();  // Lỗi nghiêm trọng -> rollback toàn bộ
                    return false;
                }
            }
        }

        conn->commit();  // ✅ Commit tất cả câu thành công
        conn->terminateStatement(stmt);

        if (successCount > 0) {
            //Logger::info("✅ Batch executed successfully with " + std::to_string(successCount) + " queries.");
            return true;
        } else {
	    Logger::warn("⚠️ All queries skipped due to duplicates.");
            return true;  // ✅ Trả về true để không retry lại batch này
        }

    } catch (SQLException& e) {
	Logger::error("❌ Batch execution failed: " + std::string(e.getMessage()));
        conn->rollback();
        if (stmt) conn->terminateStatement(stmt);
        return false;
    }
}*/

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
                    Logger::warn("⚠️ ORA-00001: Duplicate PK. Skipping row.");
                    MetricsExporter::getInstance().incrementCounter("oracle_duplicate_pk_skipped", {
                        {"table", tableKey}
                    });
                    skippedCount++;
                    continue;
                } else if (result == DBExecResult::INVALID_DATA) {
                    Logger::warn("⚠️ ORA-01839 or similar: Invalid date/data. Skipping row.");
                    MetricsExporter::getInstance().incrementCounter("oracle_invalid_data_skipped", {
                        {"table", tableKey},
                        {"error", DBExceptionHelper::toString(result)}
                    });
                    skippedCount++;
                    continue;
                } else {
                    Logger::error("❌ SQL execution failed: " + errMsg);
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
            //Logger::info("✅ Batch executed with " + std::to_string(successCount) + " successes, " + std::to_string(skippedCount) + " skipped.");
            return true;
        } else {
            Logger::warn("⚠️ All rows skipped. Nothing committed.");
            return true;  // ✅ Không lỗi, nhưng toàn bộ bị skip
        }

    } catch (SQLException& e) {
        Logger::error("❌ Batch execution failed: " + std::string(e.getMessage()));
        conn->rollback();
        if (stmt) conn->terminateStatement(stmt);
        return false;
    }
}


/*bool OracleConnector::executeBatchQuery(const std::vector<std::string>& sqlBatch) {
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
                int errorCode = e.getErrorCode();
                std::string errMsg = e.getMessage();
                DBExecResult result = DBExceptionHelper::classifyOracleError(errorCode, errMsg);
                std::string tableKey = SQLUtils::extractTableFromInsert(sql);

                MetricsExporter::getInstance().incrementCounter("db_error_total", {
                    {"error_code", std::to_string(errorCode)},
                    {"category", DBExceptionHelper::toString(result)},
                    {"table", tableKey}
                });

                if (result == DBExecResult::DUPLICATE_PK) {
                    Logger::warn("⚠️ ORA-00001: Duplicate PK. Skipping.");
                    MetricsExporter::getInstance().incrementCounter("oracle_duplicate_pk_skipped", {{"table", tableKey}});
                    skippedCount++;
                    continue; // Skip this query, do not fail batch
                } else {
                    Logger::error("❌ SQL execution failed [" + std::to_string(errorCode) + "]: " + errMsg);
                    conn->terminateStatement(stmt);
                    conn->rollback();
                    return false;
                }
            }
        }

        conn->commit();
        conn->terminateStatement(stmt);

        if (successCount > 0) {
            return true;
        } else {
            Logger::warn("⚠️ All queries skipped due to duplicates.");
            return true;
        }

    } catch (SQLException& e) {
        Logger::error("❌ Batch execution failed: " + std::string(e.getMessage()));
        if (conn) conn->rollback();
        if (stmt) conn->terminateStatement(stmt);
        return false;
    }
}*/

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
	Logger::error("❌ Invalid table name format (expect OWNER.TABLE): " + fullTableName);
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
        //Logger::info("   ↪️ " + colName + " : " + info.getFullTypeString());
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
	    Logger::debug("[OracleMem] " + name + ": " + std::to_string(value) + " bytes");
        }
        stmt->closeResultSet(rs);
        conn->terminateStatement(stmt.release());
    } catch (SQLException& e) {
	    Logger::error("Oracle mem log error: " + std::string(e.getMessage()));
    }
}

/*bool OracleConnector::executeStatementSQL(const std::string& sql) {
    if (!conn || !isConnected()) return false;

    try {
        Statement* stmt = conn->createStatement(sql);
        stmt->executeUpdate();
        conn->commit();
        conn->terminateStatement(stmt);
        return true;

    } catch (const SQLException& e) {
        int errCode = e.getErrorCode();
        std::string msg = e.getMessage();
        DBExecResult type = DBExceptionHelper::classifyOracleError(errCode, msg);

        Logger::error("❌ Oracle executeStatementSQL failed [ORA-" + std::to_string(errCode) + "]: " +
                      DBExceptionHelper::toString(type) + " — " + msg);

        MetricsExporter::getInstance().incrementCounter("db_error_total", {
            {"db", "oracle"},
            {"error_code", "ORA-" + std::to_string(errCode)}
        });

        // Xử lý đặc biệt nếu là lỗi Duplicate PK
        if (type == DBExecResult::DUPLICATE_PK) {
            Logger::warn("⚠️ ORA-00001 (Duplicate PK) — Ignored.");
            return true;
        }

        return false;
    }
}*/

/*bool OracleConnector::executeStatementSQL(const std::string& sql) {
    if (!isConnected()) return false;

    Statement* stmt = nullptr;

    try {
        stmt = conn->createStatement(sql);
        stmt->executeUpdate();
        conn->commit();
        conn->terminateStatement(stmt);
        return true;

    } catch (const SQLException& ex) {
        std::string errMsg = ex.getMessage();
        int errorCode = ex.getErrorCode();

        // Phân loại lỗi thông qua helper
        std::string category = DBExceptionHelper::classifyOracleError(errorCode);
        std::string errorName = DBExceptionHelper::getOracleErrorName(errorCode);

        Logger::error("❌ executeStatementSQL failed: [" + errorName + "] " + errMsg);

        // Đếm lỗi theo mã lỗi và bảng (nếu extract được)
        std::string tableKey = SQLUtils::extractTableFromInsert(sql);
        MetricsExporter::getInstance().incrementCounter("db_error_total", {
            {"db", "oracle"},
            {"error_code", std::to_string(errorCode)},
            {"category", category},
            {"table", tableKey}
        });

        // ❗Fallback ví dụ (nếu cần sau này)
        //
        //if (errorCode == 1 // ORA-00001  /) {
        //    Logger::warn("🔁 Retrying with MERGE due to Duplicate PK");
            // TODO: sinh MERGE SQL fallback
        //}
        

        if (stmt) conn->terminateStatement(stmt);
        conn->rollback();
        return false;
    }
}*/


