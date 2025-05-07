#pragma once
#include <string>

class PostgreSQLErrorClassifier {
public:
    // Trả về error_code như "PG-DUPLICATE-KEY", "PG-LONG-VARCHAR", "PG-UNKNOWN"
    static std::string classify(const std::string& errorMessage);
};

