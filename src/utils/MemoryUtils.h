#pragma once

#include <string>
#include <utility>

class MemoryUtils {
public:
    static std::pair<size_t, size_t> getMemoryUsageMB(); // Returns (vmSizeMB, rssMB)
};

