#ifndef FILTER_CONFIG_LOADER_H
#define FILTER_CONFIG_LOADER_H

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include "KafkaProcessor.h"
#include "../common/FilterEntry.h"

/*struct FilterEntry {
    std::string owner;
    std::string table;
    std::string primaryKey;
    std::string pkIndex;
};*/

class FilterConfigLoader {
public:
    static FilterConfigLoader& getInstance();

    bool loadConfig(const std::string& filePath);
    std::string getPKIndex(const std::string& fullTableName) const;
    std::vector<FilterEntry> getAllFilters() const;
    std::unordered_map<std::string, std::string> getPrimaryKeyColumns() const;

private:
    mutable std::mutex mutex;
    std::vector<FilterEntry> filters;
    std::unordered_map<std::string, std::string> pkIndexMap;
    //std::vector<FilterEntry> getAllFilters() const;

    // Disallow external construction/copy
    FilterConfigLoader() = default;
    ~FilterConfigLoader() = default;
    FilterConfigLoader(const FilterConfigLoader&) = delete;
    FilterConfigLoader& operator=(const FilterConfigLoader&) = delete;
};

#endif // FILTER_CONFIG_LOADER_H
