#include "PostgreSQLConnector.h"
#include "../../schema/PostgreSQLColumnInfo.h"
#include "../../logger/Logger.h"
#include <sstream>

PostgreSQLConnector::PostgreSQLConnector(const std::string& host,
                                         int port,
                                         const std::string& user,
                                         const std::string& password,
                                         const std::string& dbname)
    : host(host), port(port), user(user), password(password), dbname(dbname) {}

PostgreSQLConnector::~PostgreSQLConnector() {
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
        Logger::error("❌ PostgreSQL connection failed: " + std::string(PQerrorMessage(conn)));
        return false;
    }

    Logger::info("✅ Connected to PostgreSQL successfully");
    return true;
}

bool PostgreSQLConnector::isConnected() {
    return conn && PQstatus(conn) == CONNECTION_OK;
}

bool PostgreSQLConnector::executeQuery(const std::string& sql) {
    if (!isConnected()) return false;

    PGresult* res = PQexec(conn, sql.c_str());
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        Logger::error("❌ PostgreSQL execution failed: " + std::string(PQerrorMessage(conn)) + " | SQL: " + sql);
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
        Logger::warn("⚠️ Failed to get column types for table " + fullTableName + ": " + PQerrorMessage(conn));
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
        Logger::warn("⚠️ Failed to get primary keys for table " + fullTableName + ": " + PQerrorMessage(conn));
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


bool PostgreSQLConnector::executeBatchQuery(const std::vector<std::string>& sqlBatch) {
    if (!isConnected() && !connect()) {
        Logger::error("PostgreSQLConnector not connected.");
        return false;
    }

    for (const auto& sql : sqlBatch) {
        PGresult* res = PQexec(conn, sql.c_str());

        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            std::string errMsg = PQresultErrorMessage(res);
            PQclear(res);

            // 🔥 Logging chi tiết lỗi
            Logger::error("❌ PostgreSQL INSERT failed. SQL: " + sql);
            Logger::error("🔎 PostgreSQL error message: " + errMsg);

            // Check for duplicate key → return false to trigger fallback
            if (errMsg.find("duplicate key") != std::string::npos) {
                Logger::warn("⚠️ Detected duplicate key violation. Fallback to UPSERT may be needed.");
                return false;
            }

            return false;
        }

        PQclear(res);
    }

    return true;
}

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
        Logger::error("PostgreSQLConnector not connected.");
        return false;
    }

    PGresult* res = PQexec(conn, sql.c_str());
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::string errMsg = PQresultErrorMessage(res);
        PQclear(res);

        Logger::error("❌ PostgreSQL SQL execution failed: " + sql);
        Logger::error("🔎 Error message: " + errMsg);

        return false;
    }

    PQclear(res);
    return true;
}*/

std::vector<PostgreSQLColumnInfo> PostgreSQLConnector::getFullColumnInfo(const std::string& fullTableName) {
    std::vector<PostgreSQLColumnInfo> result;

    if (!isConnected() && !connect()) {
        Logger::error("PostgreSQL: Failed to connect for schema query");
        return result;
    }

    // 🧠 Tách schema.table → schema + table
    size_t dotPos = fullTableName.find('.');
    if (dotPos == std::string::npos) {
        Logger::error("Invalid fullTableName format (expected schema.table): " + fullTableName);
        return result;
    }

    std::string schema = fullTableName.substr(0, dotPos);
    std::string table = fullTableName.substr(dotPos + 1);

    std::string sql =
        "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable "
        "FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = '" + table + "'";

    PGresult* res = PQexec(conn, sql.c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        Logger::error("Failed to fetch schema for table: " + fullTableName);
        PQclear(res);
        return result;
    }

    int nRows = PQntuples(res);
    for (int i = 0; i < nRows; ++i) {
        PostgreSQLColumnInfo col;
        col.columnName = PQgetvalue(res, i, 0);
        col.dataType = PQgetvalue(res, i, 1);
        col.charMaxLength = PQgetvalue(res, i, 2) ? std::atoi(PQgetvalue(res, i, 2)) : -1;
        col.numericPrecision = PQgetvalue(res, i, 3) ? std::atoi(PQgetvalue(res, i, 3)) : -1;
        col.numericScale = PQgetvalue(res, i, 4) ? std::atoi(PQgetvalue(res, i, 4)) : -1;
        col.nullable = std::string(PQgetvalue(res, i, 5)) == "YES";
        result.push_back(std::move(col));
    }

    PQclear(res);
    return result;
}

/*std::vector<PostgreSQLColumnInfo> PostgreSQLConnector::getFullColumnInfo(const std::string& fullTableName) {
    std::vector<PostgreSQLColumnInfo> result;

    if (!isConnected() && !connect()) {
        Logger::error("PostgreSQL: Failed to connect for schema query");
        return result;
    }

    std::string sql =
        "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable "
        "FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = '" + table + "'";

    PGresult* res = PQexec(conn, sql.c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        Logger::error("Failed to fetch schema for table: " + fullTableName);
        PQclear(res);
        return result;
    }

    int nRows = PQntuples(res);
    for (int i = 0; i < nRows; ++i) {
        PostgreSQLColumnInfo col;
        col.columnName = PQgetvalue(res, i, 0);
        col.dataType = PQgetvalue(res, i, 1);
        col.charMaxLength = PQgetvalue(res, i, 2) ? std::atoi(PQgetvalue(res, i, 2)) : -1;
        col.numericPrecision = PQgetvalue(res, i, 3) ? std::atoi(PQgetvalue(res, i, 3)) : -1;
        col.numericScale = PQgetvalue(res, i, 4) ? std::atoi(PQgetvalue(res, i, 4)) : -1;
        col.nullable = std::string(PQgetvalue(res, i, 5)) == "YES";
        result.push_back(std::move(col));
    }

    PQclear(res);
    return result;
}*/

