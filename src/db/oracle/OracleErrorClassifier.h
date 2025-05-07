#pragma once
#include <string>

class OracleErrorClassifier {
public:
    // Trả về mã lỗi Prometheus-friendly: ORA-12899, ORA-00001, ORA-UNKNOWN...
    static std::string classify(const std::string& errorMessage);
};

