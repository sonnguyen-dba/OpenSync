#ifndef CONFIG_LOADER_H
#define CONFIG_LOADER_H

#include <string>
#include <unordered_map>
#include <map>
#include <rapidjson/document.h>

class ConfigLoader {
public:
    explicit ConfigLoader(const std::string& configPath);
    bool loadConfig();
    std::string getKafkaConfig(const std::string& key);

    std::string getDBConfig(const std::string& dbType, const std::string& key) const;
    const std::map<std::string, std::string>& getConfigMap() const { return configMap; }

    // 🆕 Thêm hàm để lấy cấu hình chung từ config.json
    std::string getConfig(const std::string& key) const;

    bool isLogISO8601Enabled() const;

    bool getBool(const std::string& key, bool defaultValue = false) const;
    int getInt(const std::string& key, int defaultValue = 0) const;

    void dumpConfig(const std::string& filepath = "config_dump.log", bool toConsole = false) const;
    bool shouldLogConfigToConsole() const;

    std::string getConfig(const std::string& key, const std::string& defaultValue) const;
    int getTimestampUnit() const; // Thêm getter cho timestamp_unit
private:
    std::string configFilePath;
    rapidjson::Document configJson;
    std::map<std::string, std::string> configMap;
    int timestamp_unit; // 0: nanosecond, 1: microsecond, 2: millisecond
};

#endif
