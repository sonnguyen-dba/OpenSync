#pragma once
#include <string>

class AvroTypeMapper {
public:
    static const std::string& map(int dataType);
};
