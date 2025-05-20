#include "FilterConfigLoader.h"
#include "ConfigLoader.h"
#include "../logger/Logger.h"
#include <algorithm>
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
    return loadConfig(filePath, "oracle");
}

bool FilterConfigLoader::loadConfig(const std::string& filePath, const std::string& dbType) {
    std::ifstream ifs(filePath);
    (void) dbType;
    if (!ifs.is_open()) {
	OpenSync::Logger::error("Unable to open filter config file: " + filePath);
        return false;
    }

    rapidjson::IStreamWrapper isw(ifs);
    rapidjson::Document doc;
    doc.ParseStream(isw);

    if (doc.HasParseError()) {
	OpenSync::Logger::error("JSON parse error in filter config file: " + filePath);
        return false;
    }

    if (!doc.HasMember("tables") || !doc["tables"].IsArray()) {
	OpenSync::Logger::error("Invalid or missing 'tables' array in filter config.");
        return false;
    }

    //bool toLower = (dbType == "postgresql");

    const auto& tables = doc["tables"];
    for (rapidjson::SizeType i = 0; i < tables.Size(); i++) {
        const auto& entry = tables[i];
        if (!entry.HasMember("owner") || !entry.HasMember("table")) continue;

        FilterEntry filter;
        filter.owner = entry["owner"].GetString();
        filter.table = entry["table"].GetString();

	/*if (toLower) {
            std::transform(filter.owner.begin(), filter.owner.end(), filter.owner.begin(), ::tolower);
            std::transform(filter.table.begin(), filter.table.end(), filter.table.begin(), ::tolower);
        }*/

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
		OpenSync::Logger::info("✔️  Table added to filter: - " + fullTable + "  ↪️  PK: " + filter.primaryKey + " ↪️  With PK Index:" + filter.pkIndex);
            } else {
		OpenSync::Logger::info("✔️  Table added to filter: - " + fullTable + " (No PK Index hint)");
            }
        }
    }

    return true;
}

std::string FilterConfigLoader::getPKIndex(const std::string& fullTableName) const {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = pkIndexMap.find(fullTableName);
    if (it != pkIndexMap.end()) {
        OpenSync::Logger::debug("getPKIndex: Found index " + it->second + " for table: " + fullTableName);
        return it->second;
    }
    OpenSync::Logger::debug("getPKIndex: No index found for table: " + fullTableName);
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
