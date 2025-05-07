#include "OracleErrorClassifier.h"
#include <vector>

std::string OracleErrorClassifier::classify(const std::string& errorMessage) {
    static const std::vector<std::string> knownErrors = {
        "ORA-00001", // unique constraint
        "ORA-00932", // inconsistent data type
        "ORA-01722", // invalid number
        "ORA-01843", // not a valid month
        "ORA-06502", // PL/SQL value error
        "ORA-12899", // value too large
        "ORA-28000", // account locked
        "ORA-12170", // connect timeout
        "ORA-12514"  // service not known
    };

    for (const auto& code : knownErrors) {
        if (errorMessage.find(code) != std::string::npos) {
            return code;
        }
    }

    return "ORA-UNKNOWN";
}

