#pragma once
#include <unordered_map>
#include <mutex>
#include <string>

class PerTableMutexManager {
public:
    std::mutex& getMutex(const std::string& tableKey) {
        std::lock_guard<std::mutex> lock(mapMutex);
        return tableMutexes[tableKey];
    }

private:
    std::unordered_map<std::string, std::mutex> tableMutexes;
    std::mutex mapMutex;
};

