#include "PostgreSQLErrorClassifier.h"

std::string PostgreSQLErrorClassifier::classify(const std::string& errorMessage) {
    if (errorMessage.find("duplicate key value violates unique constraint") != std::string::npos) {
        return "PG-DUPLICATE-KEY";
    }
    if (errorMessage.find("value too long for type character varying") != std::string::npos) {
        return "PG-LONG-VARCHAR";
    }
    if (errorMessage.find("invalid input syntax for") != std::string::npos) {
        return "PG-INVALID-SYNTAX";
    }
    if (errorMessage.find("null value in column") != std::string::npos) {
        return "PG-NULL-VIOLATION";
    }

    return "PG-UNKNOWN";
}

