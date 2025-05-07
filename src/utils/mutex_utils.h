#include <unordered_map>
#include <mutex>
#include <shared_mutex>

static std::unordered_map<std::string, std::shared_ptr<std::mutex>> tableMutexMap;
static std::shared_mutex tableMutexMapGlobalLock;

std::shared_ptr<std::mutex> getTableMutex(const std::string& tableKey) {
    {
        std::shared_lock<std::shared_mutex> readLock(tableMutexMapGlobalLock);
        auto it = tableMutexMap.find(tableKey);
        if (it != tableMutexMap.end()) return it->second;
    }

    std::unique_lock<std::shared_mutex> writeLock(tableMutexMapGlobalLock);
    auto& mtx = tableMutexMap[tableKey];
    if (!mtx) {
        mtx = std::make_shared<std::mutex>();
        LOG_INFO("🧵 Creating mutex for table: " + tableKey);
    }
    return mtx;
}

