#ifndef DATATYPEHANDLER_H
#define DATATYPEHANDLER_H

#include <string>
#include "rapidjson/document.h"

class DataTypeHandler {
public:
    static std::string trimWhitespace(const std::string& str);
    static std::string formatValue(const rapidjson::Value& value);
};

#endif // DATATYPEHANDLER_H
