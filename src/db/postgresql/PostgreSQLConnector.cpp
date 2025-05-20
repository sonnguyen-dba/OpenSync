#include "PostgreSQLConnector.h"
#include "../../schema/PostgreSQLColumnInfo.h"
#include "../../logger/Logger.h"
#include "../../utils/SQLUtils.h"
#include <libpq-fe.h>
#include <sstream>
#include <algorithm>
#include <set>

/*PostgreSQLConnector::PostgreSQLConnector(const std::string& host,
                                         int port,
                                         const std::string& user,
                                         const std::string& password,
                                         const std::string& dbname)
    : host(host), port(port), user(user), password(password), dbname(dbname) {}*/

PostgreSQLConnector::PostgreSQLConnector(const std::string& host,
                                         int port,
                                         const std::string& user,
                                         const std::string& password,
                                         const std::string& dbname,
                                         const std::string& sslmode,
                                         const std::string& sslrootcert,
                                         const std::string& sslcert,
                                         const std::string& sslkey)
    : host(host), port(port), user(user), password(password), dbname(dbname),
      sslmode(sslmode), sslrootcert(sslrootcert), sslcert(sslcert), sslkey(sslkey), conn(nullptr) {}

/*PostgreSQLConnector::~PostgreSQLConnector() {
    if (conn) {
        PQfinish(conn);
        conn = nullptr;
    }
}

bool PostgreSQLConnector::connect() {
    std::ostringstream connStr;
    connStr << "host=" << host << " port=" << port
            << " user=" << user << " password=" << password
            << " dbname=" << dbname;

    conn = PQconnectdb(connStr.str().c_str());

    if (PQstatus(conn) != CONNECTION_OK) {
        OpenSync::Logger::error("❌ PostgreSQL connection failed: " + std::string(PQerrorMessage(conn)));
        return false;
    }

    OpenSync::Logger::info("✅ Connected to PostgreSQL successfully");
    return true;
}*/

PostgreSQLConnector::~PostgreSQLConnector() {
    disconnect();
}

bool PostgreSQLConnector::connect() {
    std::ostringstream connStr;
    connStr << "host=" << host << " port=" << port
            << " user=" << user << " password=" << password
            << " dbname=" << dbname << " sslmode=" << sslmode;

    if (!sslrootcert.empty()) connStr << " sslrootcert=" << sslrootcert;
    if (!sslcert.empty()) connStr << " sslcert=" << sslcert;
    if (!sslkey.empty()) connStr << " sslkey=" << sslkey;

    conn = PQconnectdb(connStr.str().c_str());

    if (PQstatus(conn) != CONNECTION_OK) {
        OpenSync::Logger::error("❌ PostgreSQL connection failed: " + std::string(PQerrorMessage(conn)));
        PQfinish(conn);
        conn = nullptr;
        return false;
    }

    if (PQsslInUse(conn)) {
        OpenSync::Logger::info("✅ Connected to PostgreSQL with SSL (library: " +
                               std::string(PQsslAttribute(conn, "library") ? PQsslAttribute(conn, "library") : "unknown") + ")");
    } else if (sslmode == "require" || sslmode == "verify-ca" || sslmode == "verify-full") {
        OpenSync::Logger::error("❌ SSL required but not used! Connection rejected.");
        PQfinish(conn);
        conn = nullptr;
        return false;
    } else {
        OpenSync::Logger::warn("⚠️ Connected to PostgreSQL without SSL.");
    }

    OpenSync::Logger::info("✅ Connected to PostgreSQL successfully");
    return true;
}

bool PostgreSQLConnector::isConnected() {
    return conn && PQstatus(conn) == CONNECTION_OK;
}

bool PostgreSQLConnector::executeQuery(const std::string& sql) {
    if (!isConnected()) return false;

    PGresult* res = PQexec(conn, sql.c_str());
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        OpenSync::Logger::error("❌ PostgreSQL execution failed: " + std::string(PQerrorMessage(conn)) + " | SQL: " + sql);
        PQclear(res);
        return false;
    }

    PQclear(res);
    return true;
}

std::unique_ptr<DBConnector> PostgreSQLConnector::clone() const {
    auto copy = std::make_unique<PostgreSQLConnector>(host, port, user, password, dbname);
    copy->connect(); // establish new connection
    return copy;
}

std::map<std::string, std::string> PostgreSQLConnector::getColumnTypes(const std::string& fullTableName) {
    std::map<std::string, std::string> colTypes;
    if (!isConnected()) return colTypes;

    std::string schema = "public";
    std::string table = fullTableName;
    auto dot = fullTableName.find('.');
    if (dot != std::string::npos) {
        schema = fullTableName.substr(0, dot);
        table = fullTableName.substr(dot + 1);
    }

    std::ostringstream query;
    query << "SELECT column_name, data_type FROM information_schema.columns "
          << "WHERE table_schema = '" << schema << "' AND table_name = '" << table << "'";

    PGresult* res = PQexec(conn, query.str().c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        OpenSync::Logger::warn("⚠️ Failed to get column types for table " + fullTableName + ": " + PQerrorMessage(conn));
        PQclear(res);
        return colTypes;
    }

    int rows = PQntuples(res);
    for (int i = 0; i < rows; ++i) {
        std::string colName = PQgetvalue(res, i, 0);
        std::string dataType = PQgetvalue(res, i, 1);
        colTypes[colName] = dataType;
    }

    PQclear(res);
    return colTypes;
}

std::map<std::string, std::string> PostgreSQLConnector::getPrimaryKeys(const std::string& fullTableName) {
    std::map<std::string, std::string> pkMap;
    if (!isConnected()) return pkMap;

    std::string schema = "public";
    std::string table = fullTableName;
    auto dot = fullTableName.find('.');
    if (dot != std::string::npos) {
        schema = fullTableName.substr(0, dot);
        table = fullTableName.substr(dot + 1);
    }

    std::ostringstream query;
    query << "SELECT kcu.column_name FROM information_schema.table_constraints tc "
          << "JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name "
          << "AND tc.table_schema = kcu.table_schema "
          << "WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = '"
          << schema << "' AND tc.table_name = '" << table << "'";

    PGresult* res = PQexec(conn, query.str().c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        OpenSync::Logger::warn("⚠️ Failed to get primary keys for table " + fullTableName + ": " + PQerrorMessage(conn));
        PQclear(res);
        return pkMap;
    }

    int rows = PQntuples(res);
    for (int i = 0; i < rows; ++i) {
        std::string pkCol = PQgetvalue(res, i, 0);
        pkMap[pkCol] = "PRIMARY";
    }

    PQclear(res);
    return pkMap;
}


void PostgreSQLConnector::disconnect() {
    if (conn) {
        PQfinish(conn);
        conn = nullptr;
    }
}

/*bool PostgreSQLConnector::executeQuery(const std::string& sql) {
    return executeStatementSQL(sql);
}*/
/*
bool PostgreSQLConnector::executeBatchQuery(const std::vector<std::string>& sqlBatch) {
    if (!isConnected() && !connect()) {
        OpenSync::Logger::error("PostgreSQLConnector not connected.");
        return false;
    }

    for (size_t i = 0; i < sqlBatch.size(); ++i) {
        const std::string& sql = sqlBatch[i];

        OpenSync::Logger::debug("🔢 Executing SQL [" + std::to_string(i + 1) + "/" + std::to_string(sqlBatch.size()) + "]: " + sql);

        PGresult* res = PQexec(conn, sql.c_str());

        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            std::string errMsg = PQresultErrorMessage(res);
            PQclear(res);

            OpenSync::Logger::error("❌ PostgreSQL INSERT failed. SQL: " + sql);
            OpenSync::Logger::error("🔎 PostgreSQL error message: " + errMsg);

            // Check for duplicate key → return false to trigger fallback
            if (errMsg.find("duplicate key") != std::string::npos) {
                OpenSync::Logger::warn("⚠️ Detected duplicate key violation. Fallback to UPSERT may be needed.");
                return false;
            }

            return false;
        }

        PQclear(res);
        OpenSync::Logger::debug("✅ SQL executed successfully.");
    }

    return true;
}
*/

// Phương thức gốc để tương thích với DBConnector
bool PostgreSQLConnector::executeBatchQuery(const std::vector<std::string>& sqlBatch) {
    std::lock_guard<std::mutex> lock(connMutex);
    if (!isConnected() && !connect()) {
        OpenSync::Logger::error("❌ PostgreSQLConnector not connected.");
        return false;
    }

    if (!executeQuery("BEGIN")) {
        OpenSync::Logger::error("❌ Failed to start transaction.");
        return false;
    }

    int successCount = 0;
    int skippedCount = 0;

    for (size_t i = 0; i < sqlBatch.size(); ++i) {
        const std::string& sql = sqlBatch[i];
        OpenSync::Logger::debug("🔢 Executing SQL [" + std::to_string(i + 1) + "/" + 
                               std::to_string(sqlBatch.size()) + "]: " + sql);

        PGresult* res = PQexec(conn, sql.c_str());
        ExecStatusType status = PQresultStatus(res);

        if (status != PGRES_COMMAND_OK) {
            std::string errMsg = PQresultErrorMessage(res);
            PQclear(res);

            if (errMsg.find("duplicate key") != std::string::npos) {
                OpenSync::Logger::warn("⚠️ Duplicate key violation detected. Skipping: " + sql);
                skippedCount++;
                continue;
            } else if (errMsg.find("23502") != std::string::npos) {
                OpenSync::Logger::warn("⚠️ Not null violation detected. Skipping: " + sql);
                skippedCount++;
                continue;
            }

            //OpenSync::Logger::error("❌ PostgreSQL INSERT failed. SQL: " + sql);
            OpenSync::Logger::error("🔎 PostgreSQL error message: " + errMsg);
            executeQuery("ROLLBACK");
            return false;
        }

        PQclear(res);
        successCount++;
        OpenSync::Logger::debug("✅ SQL executed successfully.");
    }

    if (!executeQuery("COMMIT")) {
        OpenSync::Logger::error("❌ Failed to commit transaction.");
        executeQuery("ROLLBACK");
        return false;
    }

    OpenSync::Logger::info("✅ Batch executed: " + std::to_string(successCount) + 
                          " succeeded, " + std::to_string(skippedCount) + " skipped.");
    return true;
}

// Hàm tiện ích để tạo câu INSERT động
static std::string buildInsertSQL(const std::string& fullTableName, 
                                 const std::vector<std::string>& columns) {
    std::ostringstream sql;
    sql << "INSERT INTO " << fullTableName << " (";

    for (size_t i = 0; i < columns.size(); ++i) {
        sql << columns[i];
        if (i < columns.size() - 1) sql << ", ";
    }

    sql << ") VALUES (";

    for (size_t i = 0; i < columns.size(); ++i) {
        sql << "$" << (i + 1);
        if (i < columns.size() - 1) sql << ", ";
    }

    sql << ")";
    return sql.str();
}

bool PostgreSQLConnector::executeBatchQuery(const std::vector<BatchInsert>& batchInserts) {
    std::lock_guard<std::mutex> lock(connMutex);
    if (!isConnected() && !connect()) {
        OpenSync::Logger::error("❌ PostgreSQLConnector not connected.");
        return false;
    }

    if (!executeQuery("BEGIN")) {
        OpenSync::Logger::error("❌ Failed to start transaction.");
        return false;
    }

    int successCount = 0;
    int skippedCount = 0;

    for (size_t batchIdx = 0; batchIdx < batchInserts.size(); ++batchIdx) {
        const BatchInsert& batch = batchInserts[batchIdx];
        const std::string& fullTableName = batch.fullTableName;
        const std::vector<std::string>& columns = batch.columns;
        const std::vector<std::vector<std::string>>& values = batch.values;

        std::string stmtName = "insert_stmt_" + std::to_string(batchIdx);
        std::string sql = buildInsertSQL(fullTableName, columns);
        OpenSync::Logger::debug("🔢 Preparing statement: " + sql);

        PGresult* res = PQprepare(conn, stmtName.c_str(), sql.c_str(), columns.size(), nullptr);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            OpenSync::Logger::error("❌ Failed to prepare statement: " + 
                                   std::string(PQerrorMessage(conn)));
            PQclear(res);
            executeQuery("ROLLBACK");
            return false;
        }
        PQclear(res);

        for (size_t rowIdx = 0; rowIdx < values.size(); ++rowIdx) {
            const std::vector<std::string>& row = values[rowIdx];
            if (row.size() != columns.size()) {
                OpenSync::Logger::error("❌ Mismatch between columns and values in row " + 
                                       std::to_string(rowIdx));
                executeQuery("ROLLBACK");
                return false;
            }

            std::vector<const char*> paramValues(row.size());
            for (size_t i = 0; i < row.size(); ++i) {
                paramValues[i] = row[i].c_str();
            }

            OpenSync::Logger::debug("🔢 Executing row [" + std::to_string(rowIdx + 1) + "/" + 
                                   std::to_string(values.size()) + "] for table " + fullTableName);

            res = PQexecPrepared(conn, stmtName.c_str(), columns.size(), 
                                paramValues.data(), nullptr, nullptr, 0);
            ExecStatusType status = PQresultStatus(res);

            if (status != PGRES_COMMAND_OK) {
                std::string errMsg = PQresultErrorMessage(res);
                PQclear(res);

                if (errMsg.find("duplicate key") != std::string::npos) {
                    OpenSync::Logger::warn("⚠️ Duplicate key violation detected. Skipping row.");
                    skippedCount++;
                    continue;
                } else if (errMsg.find("23502") != std::string::npos) {
                    OpenSync::Logger::warn("⚠️ Not null violation detected. Skipping row.");
                    skippedCount++;
                    continue;
                } else if (errMsg.find("22001") != std::string::npos) {
                    OpenSync::Logger::warn("⚠️ String too long detected. Skipping row.");
                    skippedCount++;
                    continue;
                }

                //OpenSync::Logger::error("❌ PostgreSQL INSERT failed: " + errMsg);
                executeQuery("ROLLBACK");
                return false;
            }

            PQclear(res);
            successCount++;
            OpenSync::Logger::debug("✅ Row executed successfully.");
        }
    }

    if (!executeQuery("COMMIT")) {
        OpenSync::Logger::error("❌ Failed to commit transaction.");
        executeQuery("ROLLBACK");
        return false;
    }

    OpenSync::Logger::info("✅ Batch executed: " + std::to_string(successCount) + 
                          " succeeded, " + std::to_string(skippedCount) + " skipped.");
    return true;
}

/*bool PostgreSQLConnector::executeBatchQuery(const std::vector<std::string>& sqlBatch) {
    if (!isConnected() && !connect()) {
        OpenSync::Logger::error("PostgreSQLConnector not connected.");
        return false;
    }

    for (const auto& sql : sqlBatch) {
        PGresult* res = PQexec(conn, sql.c_str());

        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            std::string errMsg = PQresultErrorMessage(res);
            PQclear(res);

            // 🔥 Logging chi tiết lỗi
            OpenSync::Logger::error("❌ PostgreSQL INSERT failed. SQL: " + sql);
            OpenSync::Logger::error("🔎 PostgreSQL error message: " + errMsg);

            // Check for duplicate key → return false to trigger fallback
            if (errMsg.find("duplicate key") != std::string::npos) {
                OpenSync::Logger::warn("⚠️ Detected duplicate key violation. Fallback to UPSERT may be needed.");
                return false;
            }

            return false;
        }

        PQclear(res);
    }

    return true;
}*/

/*bool PostgreSQLConnector::executeBatchQuery(const std::vector<std::string>& sqlBatch) {
    if (!isConnected()) return false;
    for (const auto& sql : sqlBatch) {
        if (!executeQuery(sql)) {
            return false;
        }
    }
    return true;
}*/


 /*bool PostgreSQLConnector::executeStatementSQL(const std::string& sql) {
    if (!isConnected() && !connect()) {
        OpenSync::Logger::error("PostgreSQLConnector not connected.");
        return false;
    }

    PGresult* res = PQexec(conn, sql.c_str());
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::string errMsg = PQresultErrorMessage(res);
        PQclear(res);

        OpenSync::Logger::error("❌ PostgreSQL SQL execution failed: " + sql);
        OpenSync::Logger::error("🔎 Error message: " + errMsg);

        return false;
    }

    PQclear(res);
    return true;
}*/

/*std::vector<PostgreSQLColumnInfo> PostgreSQLConnector::getFullColumnInfo(const std::string& fullTableName) {
    std::vector<PostgreSQLColumnInfo> result;

    if (!isConnected() && !connect()) {
        OpenSync::Logger::error("PostgreSQL: Failed to connect for schema query");
        return result;
    }

    // 🧠 Tách schema.table → schema + table
    size_t dotPos = fullTableName.find('.');
    if (dotPos == std::string::npos) {
        OpenSync::Logger::error("Invalid fullTableName format (expected schema.table): " + fullTableName);
        return result;
    }

    std::string schema = fullTableName.substr(0, dotPos);
    std::string table = fullTableName.substr(dotPos + 1);

    std::transform(schema.begin(), schema.end(), schema.begin(), ::tolower);
    std::transform(table.begin(), table.end(), table.begin(), ::tolower);

    std::string sql =
        "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable "
        "FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = '" + table + "'";

    PGresult* res = PQexec(conn, sql.c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        OpenSync::Logger::error("Failed to fetch schema for table: " + fullTableName);
        PQclear(res);
        return result;
    }

    int nRows = PQntuples(res);
    for (int i = 0; i < nRows; ++i) {
        PostgreSQLColumnInfo col;
        col.columnName = PQgetvalue(res, i, 0);
        col.dataType = PQgetvalue(res, i, 1);

	col.charMaxLength     = PQgetisnull(res, i, 2) ? -1 : std::atoi(PQgetvalue(res, i, 2));
        col.numericPrecision  = PQgetisnull(res, i, 3) ? -1 : std::atoi(PQgetvalue(res, i, 3));
        col.numericScale      = PQgetisnull(res, i, 4) ? -1 : std::atoi(PQgetvalue(res, i, 4));
        col.nullable          = std::string(PQgetvalue(res, i, 5)) == "YES";

        result.push_back(std::move(col));
    }

    PQclear(res);
    return result;
}*/

/*std::unordered_map<std::string, PostgreSQLColumnInfo> PostgreSQLConnector::getFullColumnInfo(const std::string& fullTableName) {
    std::unordered_map<std::string, PostgreSQLColumnInfo> colMap;

    if (!conn) return colMap;

    std::string schema = "public";
    std::string table = fullTableName;
    size_t dot = fullTableName.find('.');
    if (dot != std::string::npos) {
        schema = fullTableName.substr(0, dot);
        table = fullTableName.substr(dot + 1);
    }

    std::ostringstream query;
    query << "SELECT column_name, data_type, character_maximum_length, "
          << "numeric_precision, numeric_scale, is_nullable "
          << "FROM information_schema.columns "
          << "WHERE table_schema = '" << schema << "' AND table_name = '" << table << "'";

    PGresult* res = PQexec(conn, query.str().c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        OpenSync::Logger::warn("⚠️ Failed to load schema: " + std::string(PQerrorMessage(conn)));
        PQclear(res);
        return colMap;
    }

    int rows = PQntuples(res);
    for (int i = 0; i < rows; ++i) {
        PostgreSQLColumnInfo info;
        info.columnName = PQgetvalue(res, i, 0);
        info.dataType = PQgetvalue(res, i, 1);
        info.charMaxLength = std::atoi(PQgetvalue(res, i, 2));
        info.numericPrecision = std::atoi(PQgetvalue(res, i, 3));
        info.numericScale = std::atoi(PQgetvalue(res, i, 4));
        info.nullable = (std::string(PQgetvalue(res, i, 5)) == "YES");
        colMap[info.columnName] = info;
    }

    PQclear(res);
    return colMap;
}*/

std::unordered_map<std::string, PostgreSQLColumnInfo>
PostgreSQLConnector::getFullColumnInfo(const std::string& fullTableName) {
    std::unordered_map<std::string, PostgreSQLColumnInfo> colMap;

    if (!conn && !connect()) {
        OpenSync::Logger::error("❌ PostgreSQLConnector: Not connected when loading schema for " + fullTableName);
        return colMap;
    }

    std::string schema = "public";
    std::string table = fullTableName;
    size_t dot = fullTableName.find('.');
    if (dot != std::string::npos) {
        schema = fullTableName.substr(0, dot);
        table = fullTableName.substr(dot + 1);
    }

    std::string lowerSchema = SQLUtils::toLower(schema);
    std::string lowerTable  = SQLUtils::toLower(table);

    // 🧠 Step 1: Fetch primary key columns
    std::set<std::string> pkCols;
    {
        std::ostringstream pkQuery;
        pkQuery << "SELECT kcu.column_name FROM information_schema.table_constraints tc "
                << "JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name "
                << "AND tc.table_schema = kcu.table_schema "
                << "WHERE tc.constraint_type = 'PRIMARY KEY' AND lower(tc.table_schema) = '"
                << lowerSchema << "' AND lower(tc.table_name) = '" << lowerTable << "'";

        PGresult* pkRes = PQexec(conn, pkQuery.str().c_str());
        if (PQresultStatus(pkRes) == PGRES_TUPLES_OK) {
            int pkRows = PQntuples(pkRes);
            for (int i = 0; i < pkRows; ++i) {
                std::string pkCol = SQLUtils::toLower(PQgetvalue(pkRes, i, 0));
                pkCols.insert(pkCol);
            }
        } else {
            OpenSync::Logger::warn("⚠️ Failed to fetch primary keys for " + fullTableName + ": " + PQerrorMessage(conn));
        }
        PQclear(pkRes);
    }

    // 🧠 Step 2: Fetch column info
    std::ostringstream query;
    query << "SELECT column_name, data_type, character_maximum_length, "
          << "numeric_precision, numeric_scale, is_nullable "
          << "FROM information_schema.columns "
          << "WHERE lower(table_schema) = '" << lowerSchema
          << "' AND lower(table_name) = '" << lowerTable << "'";

    PGresult* res = PQexec(conn, query.str().c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        OpenSync::Logger::warn("⚠️ Failed to fetch schema for " + fullTableName + ": " + PQerrorMessage(conn));
        PQclear(res);
        return colMap;
    }

    int rows = PQntuples(res);
    for (int i = 0; i < rows; ++i) {
        PostgreSQLColumnInfo col;
        col.columnName       = SQLUtils::toLower(PQgetvalue(res, i, 0));
        col.dataType         = SQLUtils::toLower(PQgetvalue(res, i, 1));
        col.charMaxLength    = PQgetisnull(res, i, 2) ? -1 : std::atoi(PQgetvalue(res, i, 2));
        col.numericPrecision = PQgetisnull(res, i, 3) ? -1 : std::atoi(PQgetvalue(res, i, 3));
        col.numericScale     = PQgetisnull(res, i, 4) ? -1 : std::atoi(PQgetvalue(res, i, 4));
        col.nullable         = std::string(PQgetvalue(res, i, 5)) == "YES";
        col.isPrimaryKey     = pkCols.count(col.columnName) > 0;

        colMap[col.columnName] = std::move(col);
    }

    PQclear(res);

    OpenSync::Logger::info("✅ Loaded schema for " + lowerSchema + "." + lowerTable +
                           " with " + std::to_string(colMap.size()) + " columns");

    return colMap;
}

/*
std::unordered_map<std::string, PostgreSQLColumnInfo>
PostgreSQLConnector::getFullColumnInfo(const std::string& fullTableName) {
    std::unordered_map<std::string, PostgreSQLColumnInfo> colMap;

    if (!conn && !connect()) {
        OpenSync::Logger::error("❌ PostgreSQLConnector: Not connected when loading schema for " + fullTableName);
        return colMap;
    }

    std::string schema = "public";
    std::string table = fullTableName;
    size_t dot = fullTableName.find('.');
    if (dot != std::string::npos) {
        schema = fullTableName.substr(0, dot);
        table = fullTableName.substr(dot + 1);
    }

    std::string lowerSchema = SQLUtils::toLower(schema);
    std::string lowerTable  = SQLUtils::toLower(table);

    std::ostringstream query;
    query << "SELECT column_name, data_type, character_maximum_length, "
          << "numeric_precision, numeric_scale, is_nullable "
          << "FROM information_schema.columns "
          << "WHERE lower(table_schema) = " << "'" << lowerSchema << "'"
          << " AND lower(table_name) = " << "'" << lowerTable << "'";

    PGresult* res = PQexec(conn, query.str().c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        OpenSync::Logger::warn("⚠️ Failed to fetch schema for " + fullTableName + ": " + PQerrorMessage(conn));
        PQclear(res);
        return colMap;
    }

    int rows = PQntuples(res);
    for (int i = 0; i < rows; ++i) {
        PostgreSQLColumnInfo col;
        col.columnName       = SQLUtils::toLower(PQgetvalue(res, i, 0));
        col.dataType         = SQLUtils::toLower(PQgetvalue(res, i, 1));
        col.charMaxLength    = PQgetisnull(res, i, 2) ? -1 : std::atoi(PQgetvalue(res, i, 2));
        col.numericPrecision = PQgetisnull(res, i, 3) ? -1 : std::atoi(PQgetvalue(res, i, 3));
        col.numericScale     = PQgetisnull(res, i, 4) ? -1 : std::atoi(PQgetvalue(res, i, 4));
        col.nullable         = std::string(PQgetvalue(res, i, 5)) == "YES";
	col.isPrimaryKey     = pkCols.count(col.columnName) > 0;

        colMap[col.columnName] = std::move(col);
    }

    PQclear(res);

    OpenSync::Logger::info("✅ Loaded schema for " + lowerSchema + "." + lowerTable +
                           " with " + std::to_string(colMap.size()) + " columns");

    return colMap;
}
*/
bool PostgreSQLConnector::executeStatementSQL(const std::string& sql) {
    if (!isConnected() && !connect()) {
        OpenSync::Logger::error("PostgreSQLConnector not connected.");
        return false;
    }

    PGresult* res = PQexec(conn, sql.c_str());
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::string errMsg = PQresultErrorMessage(res);
        PQclear(res);

        OpenSync::Logger::error("❌ PostgreSQL SQL execution failed: " + sql);
        OpenSync::Logger::error("🔎 Error message: " + errMsg);

        return false;
    }

    PQclear(res);
    return true;
}

bool PostgreSQLConnector::tableExists(const std::string& schema, const std::string& table) {
    if (!isConnected() && !connect()) {
        return false;
    }

    const char* paramValues[2] = { schema.c_str(), table.c_str() };
    const int paramLengths[2] = { static_cast<int>(schema.length()), static_cast<int>(table.length()) };
    const int paramFormats[2] = { 0, 0 };  // text format

    const std::string query = R"(
        select exists (
            select 1
            from information_schema.tables
            where table_schema = $1 and table_name = $2
        )
    )";

    PGresult* res = PQexecParams(
        conn,
        query.c_str(),
        2,
        nullptr,
        paramValues,
        paramLengths,
        paramFormats,
        0  // 0 = result in text format
    );

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        OpenSync::Logger::warn("⚠️ Failed to check if table exists: " + std::string(PQerrorMessage(conn)));
        PQclear(res);
        return false;
    }

    bool exists = std::string(PQgetvalue(res, 0, 0)) == "t";
    PQclear(res);
    return exists;
}

