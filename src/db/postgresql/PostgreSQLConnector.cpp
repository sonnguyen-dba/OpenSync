#include "PostgreSQLConnector.h"
#include "../../schema/PostgreSQLColumnInfo.h"
#include "../../logger/Logger.h"
#include "../../utils/SQLUtils.h"
#include <sstream>
#include <algorithm>

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
        OpenSync::Logger::error("❌ PostgreSQL connection failed: " + std::string(PQerrorMessage(conn)));
        return false;
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

        colMap[col.columnName] = std::move(col);
    }

    PQclear(res);

    OpenSync::Logger::info("✅ Loaded schema for " + lowerSchema + "." + lowerTable +
                           " with " + std::to_string(colMap.size()) + " columns");

    return colMap;
}



