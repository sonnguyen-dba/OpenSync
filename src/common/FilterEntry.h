// FilterEntry.h
#pragma once

#include <string>

struct FilterEntry {
    std::string owner;
    std::string table;
    std::string primaryKey;
    std::string pkIndex;
};

