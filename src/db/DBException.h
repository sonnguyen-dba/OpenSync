#pragma once
#include <string>

enum class DBExecResult {
    SUCCESS,
    DUPLICATE_PK,
    INVALID_DATA,
    SCHEMA_MISMATCH,
    CONNECTION_LOST,
    TIMEOUT,
    UNKNOWN_ERROR
};

class DBExceptionHelper {
public:
    static DBExecResult classifyOracleError(int errorCode, const std::string& message);
    static std::string toString(DBExecResult result);
};

