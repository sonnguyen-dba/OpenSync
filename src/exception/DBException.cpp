#include "DBException.h"
#include <algorithm>

DBExecResult DBExceptionHelper::classifyOracleError(int errorCode, const std::string& message) {
    switch (errorCode) {
        case 1:     return DBExecResult::DUPLICATE_PK;        // ORA-00001
        case 904:   return DBExecResult::SCHEMA_MISMATCH;     // ORA-00904
        case 932:   return DBExecResult::INVALID_DATA;        // ORA-00932
        case 1843:  return DBExecResult::INVALID_DATA;        // ORA-01843
        case 3113:  // ORA-03113
        case 3114:  return DBExecResult::CONNECTION_LOST;
	case 1839:  return DBExecResult::INVALID_DATA; // ORA-01839
        case 12170: return DBExecResult::TIMEOUT;
        default:    break;
    }

    std::string lowerMsg = message;
    std::transform(lowerMsg.begin(), lowerMsg.end(), lowerMsg.begin(), ::tolower);

    if (lowerMsg.find("duplicate") != std::string::npos)
        return DBExecResult::DUPLICATE_PK;

    return DBExecResult::UNKNOWN_ERROR;
}

std::string DBExceptionHelper::toString(DBExecResult result) {
    switch (result) {
        case DBExecResult::SUCCESS: return "SUCCESS";
        case DBExecResult::DUPLICATE_PK: return "DUPLICATE_PK";
        case DBExecResult::INVALID_DATA: return "INVALID_DATA";
        case DBExecResult::SCHEMA_MISMATCH: return "SCHEMA_MISMATCH";
        case DBExecResult::CONNECTION_LOST: return "CONNECTION_LOST";
        case DBExecResult::TIMEOUT: return "TIMEOUT";
        case DBExecResult::UNKNOWN_ERROR: return "UNKNOWN_ERROR";
    }
    return "UNKNOWN";
}

