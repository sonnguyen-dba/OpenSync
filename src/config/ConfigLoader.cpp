#include "ConfigLoader.h"
#include "../logger/Logger.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <rapidjson/istreamwrapper.h>

ConfigLoader::ConfigLoader(const std::string& configPath) : configFilePath(configPath) {}

bool ConfigLoader::loadConfig() {
    std::ifstream ifs(configFilePath);
    if (!ifs.is_open()) {
        OpenSync::Logger::error("❌ Unable to open config file: " + configFilePath);
        return false;
    }

    rapidjson::IStreamWrapper isw(ifs);
    configJson.ParseStream(isw);

    if (configJson.HasParseError()) {
        OpenSync::Logger::error("❌ JSON parse error in config file.");
        return false;
    }

    OpenSync::Logger::info("📥 Config loaded successfully from: " + configFilePath);

    std::ofstream configDump("log/config_dump.log", std::ios::app);
    for (auto it = configJson.MemberBegin(); it != configJson.MemberEnd(); ++it) {
        std::string key = it->name.GetString();
        std::string value;

        const auto& v = it->value;
        if (v.IsString()) value = v.GetString();
        else if (v.IsInt()) value = std::to_string(v.GetInt());
        else if (v.IsBool()) value = v.GetBool() ? "true" : "false";
        else value = "[non-string]";

        if (OpenSync::Logger::isDebugEnabled()) {
            OpenSync::Logger::debug("🔧 Config [" + key + "] = " + value);
        }

        if (configDump.is_open()) {
            configDump << "[CONFIG] " << key << " = " << value << std::endl;
        }
    }
    configDump.close();
    return true;
}

std::string ConfigLoader::getKafkaConfig(const std::string& key) {
    if (configJson.HasMember("kafka") && configJson["kafka"].HasMember(key.c_str())) {
        return configJson["kafka"][key.c_str()].GetString();
    }
    return "";
}

std::string ConfigLoader::getDBConfig(const std::string& dbType, const std::string& key) const {
    if (!configJson.IsObject()) {
        std::cerr << "❌ Error: Config JSON is not properly loaded!" << std::endl;
        return "";
    }

    if (configJson.HasMember(dbType.c_str()) && configJson[dbType.c_str()].HasMember(key.c_str())) {
        return configJson[dbType.c_str()][key.c_str()].GetString();
    }

    if (configJson.HasMember("databases") && configJson["databases"].HasMember(dbType.c_str())) {
        if (configJson["databases"][dbType.c_str()].HasMember(key.c_str())) {
            return configJson["databases"][dbType.c_str()][key.c_str()].GetString();
        }
    }

    std::cerr << "❌ Error: Key '" << key << "' not found for database '" << dbType << "' in config!" << std::endl;
    return "";
}


// 🆕 Thêm hàm lấy cấu hình chung từ file JSON
std::string ConfigLoader::getConfig(const std::string& key) const {
    if (configJson.HasMember(key.c_str()) && configJson[key.c_str()].IsString()) {
        return configJson[key.c_str()].GetString();
    }
    return "";  // Trả về chuỗi rỗng nếu không tìm thấy
}

bool ConfigLoader::isLogISO8601Enabled() const {
    return configJson.HasMember("log_iso8601") && configJson["log_iso8601"].GetBool();

}

bool ConfigLoader::getBool(const std::string& key, bool defaultValue) const {
    if (configJson.HasMember(key.c_str())) {
        const auto& val = configJson[key.c_str()];
        if (val.IsBool()) return val.GetBool();
        if (val.IsString()) {
            std::string str = val.GetString();
            std::transform(str.begin(), str.end(), str.begin(), ::tolower);
            return (str == "true" || str == "1");
        }
        if (val.IsInt()) return val.GetInt() != 0;
    }
    return defaultValue;
}

int ConfigLoader::getInt(const std::string& key, int defaultValue) const {
    if (configJson.HasMember(key.c_str())) {
        const auto& val = configJson[key.c_str()];
        if (val.IsInt()) return val.GetInt();
        if (val.IsString()) {
            try {
                return std::stoi(val.GetString());
            } catch (...) {
                return defaultValue;
            }
        }
    }
    return defaultValue;
}

void ConfigLoader::dumpConfig(const std::string& outputFile, bool logToConsole) const {
    std::ofstream ofs(outputFile);
    if (!ofs.is_open()) {
        OpenSync::Logger::warn("⚠️ Failed to open config dump file: " + outputFile);
        return;
    }

    for (auto it = configJson.MemberBegin(); it != configJson.MemberEnd(); ++it) {
        const std::string key = it->name.GetString();
        const auto& val = it->value;

        std::string valStr;
        if (val.IsString()) {
            valStr = val.GetString();
        } else if (val.IsInt()) {
            valStr = std::to_string(val.GetInt());
        } else if (val.IsBool()) {
            valStr = val.GetBool() ? "true" : "false";
        } else {
            valStr = "[non-string]";
        }

        std::string line = "[CONFIG] " + key + " = " + valStr;

        ofs << line << std::endl;
        if (logToConsole) {
            std::cout << line << std::endl;
        }
    }

    OpenSync::Logger::info("✅ Configuration dumped to: " + outputFile);
}

bool ConfigLoader::shouldLogConfigToConsole() const {
    return getBool("log_config_to_console", false);  // mặc định là false nếu không khai báo
}

std::string ConfigLoader::getConfig(const std::string& key, const std::string& defaultValue) const {
    if (configJson.HasMember(key.c_str()) && configJson[key.c_str()].IsString()) {
        return configJson[key.c_str()].GetString();
    }
    return defaultValue;
}

int ConfigLoader::getTimestampUnit() const {
    //OpenSync::Logger::debug("Returning timestamp_unit: " + std::to_string(timestamp_unit));
    //return timestamp_unit;
    return getInt("timestamp_unit", 1);
}
