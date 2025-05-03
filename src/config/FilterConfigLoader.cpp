#include "FilterConfigLoader.h"
#include "../logger/Logger.h"
#include <fstream>
#include <iostream>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <mutex>

FilterConfigLoader& FilterConfigLoader::getInstance() {
    static FilterConfigLoader instance;
    return instance;
}

bool FilterConfigLoader::loadConfig(const std::string& filePath) {
    std::ifstream ifs(filePath);
    if (!ifs.is_open()) {
	      Logger::error("Unable to open filter config file: " + filePath);
        return false;
    }

    rapidjson::IStreamWrapper isw(ifs);
    rapidjson::Document doc;
    doc.ParseStream(isw);

    if (doc.HasParseError()) {
	      Logger::error("JSON parse error in filter config file: " + filePath);
        return false;
    }

    if (!doc.HasMember("tables") || !doc["tables"].IsArray()) {
	      Logger::error("Invalid or missing 'tables' array in filter config.");
        return false;
    }

    const auto& tables = doc["tables"];
    for (rapidjson::SizeType i = 0; i < tables.Size(); i++) {
        const auto& entry = tables[i];
        if (!entry.HasMember("owner") || !entry.HasMember("table")) continue;

        FilterEntry filter;
        filter.owner = entry["owner"].GetString();
        filter.table = entry["table"].GetString();

        if (entry.HasMember("primaryKey"))
            filter.primaryKey = entry["primaryKey"].GetString();
        else if (entry.HasMember("primary_key"))
            filter.primaryKey = entry["primary_key"].GetString();

        if (entry.HasMember("pk_index"))
            filter.pkIndex = entry["pk_index"].GetString();

        std::string fullTable = filter.owner + "." + filter.table;

        {
            std::lock_guard<std::mutex> lock(mutex);
            filters.push_back(filter);
            if (!filter.pkIndex.empty()) {
                pkIndexMap[fullTable] = filter.pkIndex;
		            Logger::info("✔️  Table added to filter: - " + fullTable + " ↪️  With PK Index:" + filter.pkIndex);
            } else {
		            Logger::info("✔️  Table added to filter: - " + fullTable + " (No PK Index hint)");
            }
        }
    }

    return true;
}

std::string FilterConfigLoader::getPKIndex(const std::string& fullTableName) const {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = pkIndexMap.find(fullTableName);
    if (it != pkIndexMap.end()) {
        Logger::debug("getPKIndex: Found index " + it->second + " for table: " + fullTableName);
        return it->second;
    }
    Logger::debug("getPKIndex: No index found for table: " + fullTableName);
    return "";
}

std::vector<FilterEntry> FilterConfigLoader::getAllFilters() const {
    std::lock_guard<std::mutex> lock(mutex);
    return filters;
}

std::unordered_map<std::string, std::string> FilterConfigLoader::getPrimaryKeyColumns() const {
    std::unordered_map<std::string, std::string> result;
    std::lock_guard<std::mutex> lock(mutex);
    for (const auto& filter : filters) {
        std::string fullTable = filter.owner + "." + filter.table;
        result[fullTable] = filter.primaryKey;
    }
    return result;
}
