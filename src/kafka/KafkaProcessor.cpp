#include "KafkaProcessor.h"
#include "KafkaConsumer.h"
#include "../metrics/MetricsExporter.h"
#include "FilterConfigLoader.h"
#include "../schema/OracleSchemaCache.h"
#include "../logger/Logger.h"
#include "../utils/SQLUtils.h"
#include "../time/TimeUtils.h"
#include "FileWatcher.h"
#include <sstream>
#include <iostream>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <algorithm>
#include <iomanip>
#include <unordered_set>
#include <unordered_map>
#include <chrono>
#include <filesystem>
#include <numeric>
#include <sys/inotify.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/inotify.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>


namespace fs = std::filesystem;

KafkaProcessor::KafkaProcessor(ConfigLoader& config) : config(config), stopReloading(false), messagesProcessed(0), lastUpdateTime(std::time(nullptr)) {
    std::string isoLogFlag = config.getConfig("enable_iso_log");
    enableISODebugLog = (isoLogFlag == "true" || isoLogFlag == "1");

    lastModifiedTime = fs::file_time_type::clock::now();
    metricUpdateThread = std::thread(&KafkaProcessor::updateProcessingRate, this);
    lagUpdateThread = std::thread(&KafkaProcessor::updateKafkaLagMetrics, this);
    startGlobalDedupCleanup();
}

KafkaProcessor::~KafkaProcessor() {
    stopDedupCleanup();
    stopReloading = true;
    stopGlobalDedupCleanup();
    if (reloadThread.joinable()) reloadThread.join();
    if (metricUpdateThread.joinable()) metricUpdateThread.join();
    if (lagUpdateThread.joinable()) lagUpdateThread.join();
}

void KafkaProcessor::addFilter(const FilterEntry& filter) {
    std::lock_guard<std::mutex> lock(filterMutex);
    filters.push_back(filter);
}

void KafkaProcessor::setMapping(const std::unordered_map<std::string, std::string>& mappingConfig) {
    mapping = mappingConfig;
}

std::string normalizeString(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::toupper);
    result.erase(0, result.find_first_not_of(" \t\n\r\f\v"));
    result.erase(result.find_last_not_of(" \t\n\r\f\v") + 1);
    return result;
}

std::optional<FilterEntry> KafkaProcessor::matchFilter(const std::string& owner, const std::string& table) {
    std::lock_guard<std::mutex> lock(filterMutex);
    for (const auto& filter : filters) {
        if (filter.owner == owner && filter.table == table) {
            return filter;
        }
    }
    return std::nullopt;
}

std::unordered_map<std::string, std::vector<std::string>> KafkaProcessor::processMessageByTable(
    const std::string& jsonMessage, int partition, int64_t offset, int64_t timestamp) {

    (void)partition;
    (void)offset;
    (void)timestamp;

    std::unordered_map<std::string, std::vector<std::string>> batchMap;
    rapidjson::Document doc;
    if (doc.Parse(jsonMessage.c_str()).HasParseError()) {
	Logger::error("KafkaProcessor: JSON parse error");
        return batchMap;
    }
    if (!doc.HasMember("payload") || !doc["payload"].IsArray()) return batchMap;
    const auto& payload = doc["payload"];

    auto now = std::chrono::steady_clock::now();
    std::unordered_map<std::string, std::unordered_set<std::string>> batchDedupCache;

    for (auto& record : payload.GetArray()) {
        if (!record.HasMember("schema")) continue;
        const auto& schema = record["schema"];
        if (!schema.HasMember("owner") || !schema.HasMember("table")) continue;

        std::string owner = schema["owner"].GetString();
        std::string table = schema["table"].GetString();

        auto filter = matchFilter(owner, table);
        if (!filter.has_value()) continue;

        std::string mappedOwner = mapping.count(owner) ? mapping[owner] : owner;
        std::string mappedTable = mapping.count(table) ? mapping[table] : table;
        std::string tableKey = mappedOwner + "." + mappedTable;

        //Kafka lag tracking
        /*auto nowSystem = std::chrono::system_clock::now();
        auto messageTime = std::chrono::system_clock::time_point{std::chrono::milliseconds(timestamp)};
        auto lagMs = std::chrono::duration_cast<std::chrono::milliseconds>(nowSystem - messageTime).count();
        MetricsExporter::getInstance().setMetric("kafka_lag_ms", lagMs, {{"table", tableKey}});
        {
            std::lock_guard<std::mutex> lock(lagMutex);
	          const size_t MAX_LAG_SAMPLES = 1000;
	          if (kafkaLagTableMs[tableKey].size() >= MAX_LAG_SAMPLES) {
	            kafkaLagTableMs[tableKey].erase(kafkaLagTableMs[tableKey].begin());
	          }
            kafkaLagTableMs[tableKey].push_back(lagMs);

	          if (kafkaLagByPartition[{kafkaTopic, partition}].size() >= MAX_LAG_SAMPLES) {
		          kafkaLagByPartition[{kafkaTopic, partition}].erase(kafkaLagByPartition[{kafkaTopic, partition}].begin());
	          }
            kafkaLagByPartition[{kafkaTopic, partition}].push_back(lagMs);
        }*/
	// Kafka lag tracking
	/*auto nowSystem = std::chrono::system_clock::now();
	auto messageTime = std::chrono::system_clock::time_point{std::chrono::milliseconds(timestamp)};
	auto lagMs = std::chrono::duration_cast<std::chrono::milliseconds>(nowSystem - messageTime).count();
	MetricsExporter::getInstance().setMetric("kafka_lag_ms", lagMs, {{"table", tableKey}});

	{
	    std::lock_guard<std::mutex> lock(lagMutex);

	    kafkaLagTableMs[tableKey].push_back(lagMs);
	    kafkaLagByPartition[{kafkaTopic, partition}].push_back(lagMs);

	    // 🧹 Shrink nếu quá giới hạn
	    shrinkLagBuffers(tableKey, {kafkaTopic, partition});

	    // 🕒 Ghi nhận thời gian cập nhật cuối
	    auto nowSteady = std::chrono::steady_clock::now();
	    tableLagLastUpdate[tableKey] = nowSteady;
	    partitionLagLastUpdate[{kafkaTopic, partition}] = nowSteady;
	}*/

	auto nowSystem = std::chrono::system_clock::now();
	auto messageTime = std::chrono::system_clock::time_point{std::chrono::milliseconds(timestamp)};
	auto lagMs = std::chrono::duration_cast<std::chrono::milliseconds>(nowSystem - messageTime).count();

	MetricsExporter::getInstance().setMetric("kafka_lag_ms", lagMs, {{"table", tableKey}});

	{
	    std::lock_guard<std::mutex> lock(lagMutex);
	    const size_t MAX_LAG_SAMPLES = 1000;

	    auto& tableLagVec = kafkaLagTableMs[tableKey];
	    tableLagVec.push_back(lagMs);
	    if (tableLagVec.size() > MAX_LAG_SAMPLES + 100) {
	        tableLagVec.erase(tableLagVec.begin(), tableLagVec.end() - MAX_LAG_SAMPLES);
	        std::vector<double>().swap(tableLagVec); // shrink
	    }

	    auto& partitionLagVec = kafkaLagByPartition[{kafkaTopic, partition}];
	    partitionLagVec.push_back(lagMs);
	    if (partitionLagVec.size() > MAX_LAG_SAMPLES + 100) {
	        partitionLagVec.erase(partitionLagVec.begin(), partitionLagVec.end() - MAX_LAG_SAMPLES);
	        std::vector<double>().swap(partitionLagVec);
	    }

	    // ⏱️ Track last update time
	    tableLagLastUpdate[tableKey] = std::chrono::steady_clock::now();
	    partitionLagLastUpdate[{kafkaTopic, partition}] = std::chrono::steady_clock::now();
	}

        if (!record.HasMember("op") || !record["op"].IsString()) continue;
        std::string op = record["op"].GetString();

        std::string sql;
        std::string opType;

        if (op == "c" && record.HasMember("after")) {
            const auto& data = record["after"];
            if (data.HasMember(filter->primaryKey.c_str())) {
                //Chuẩn hóa dedupKey từ primary key value
                std::string pkValue = SQLUtils::convertToSQLValue(data[filter->primaryKey.c_str()], filter->primaryKey);
                std::string dedupKey = tableKey + ":" + pkValue;

                //Check duplicate trong batch hiện tại
                if (batchDedupCache[tableKey].count(dedupKey)) {
                    continue;
                }
                batchDedupCache[tableKey].insert(dedupKey);

                //Check duplicate toàn cục (Kafka replay)
                /*{
                    std::lock_guard<std::mutex> gLock(globalCacheMutex);
                    auto it = recentGlobalPKCache.find(dedupKey);
                    if (it != recentGlobalPKCache.end()) {
                        auto age = std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count();
                        if (age < 60) {
			                      Logger::warn("🔁 Skipped replayed message with dedupKey: " + dedupKey);
                            continue;
                        }
                    }
                    recentGlobalPKCache[dedupKey] = now;
                }*/

                //Check dedup theo cache 10s cục bộ (per-table)
                {
                    std::lock_guard<std::mutex> lock(dedupMutex);
                    auto& tableCache = recentPrimaryKeyCachePerTable[tableKey];
                    for (auto it = tableCache.begin(); it != tableCache.end();) {
                        if (std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count() >= 10) {
                            it = tableCache.erase(it);
                        } else {
                            ++it;
                        }
                    }
                    if (tableCache.find(dedupKey) != tableCache.end()) {
                        continue;
                    }
                    tableCache[dedupKey] = now;
                }
            }

            //sql = buildInsertSQL(mappedOwner, mappedTable, data);
	          sql = sqlBuilders["oracle"]->buildInsertSQL(mappedOwner, mappedTable, data);
            opType = "insert";

        } else if (op == "u" && record.HasMember("after")) {
            //sql = buildUpdateSQL(mappedOwner, mappedTable, record["after"], filter->primaryKey);
	          sql = sqlBuilders["oracle"]->buildUpdateSQL(mappedOwner, mappedTable, record["after"], filter->primaryKey);
            opType = "update";

        } else if (op == "d" && record.HasMember("before")) {
            //sql = buildDeleteSQL(mappedOwner, mappedTable, record["before"], filter->primaryKey);
	          sql = sqlBuilders["oracle"]->buildDeleteSQL(mappedOwner, mappedTable, record["before"], filter->primaryKey);
            opType = "delete";
        }

        if (!sql.empty()) {
            batchMap[tableKey].push_back(sql);
            MetricsExporter::getInstance().incrementCounter("kafka_messages_processed");
            MetricsExporter::getInstance().incrementCounter("kafka_ops_total", {
                {"table", tableKey},
                {"op", opType}
            });
            messagesProcessed.fetch_add(1, std::memory_order_relaxed);
        }
    }

    doc.GetAllocator().Clear();
    doc.SetObject(); // reset lại Document

    return batchMap;
}

void KafkaProcessor::updateProcessingRate() {
    while (!stopReloading) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        int64_t now = std::time(nullptr);
        int64_t elapsed = now - lastUpdateTime.exchange(now);
        if (elapsed > 0) {
            double rate = static_cast<double>(messagesProcessed.exchange(0)) / elapsed;
            MetricsExporter::getInstance().setMetric("kafka_messages_processed_per_sec", rate);
        }
    }
}

void KafkaProcessor::loadFilterConfig(const std::string& configPath) {
    Logger::info("KafkaProcessor loading filter config: " + configPath);
    std::ifstream ifs(configPath);
    if (!ifs.is_open()) return;
    rapidjson::IStreamWrapper isw(ifs);
    rapidjson::Document doc;
    doc.ParseStream(isw);
    if (doc.HasParseError()) return;

    std::lock_guard<std::mutex> lock(filterMutex);
    std::vector<FilterEntry> newFilters;
    if (doc.HasMember("tables") && doc["tables"].IsArray()) {
        for (const auto& entry : doc["tables"].GetArray()) {
            if (entry.HasMember("owner") && entry.HasMember("table") && entry.HasMember("primaryKey")) {
                //newFilters.push_back({ entry["owner"].GetString(), entry["table"].GetString(), entry["primaryKey"].GetString() });
		        FilterEntry fe;
		        fe.owner = entry["owner"].GetString();
		        fe.table = entry["table"].GetString();
		        fe.primaryKey = entry["primaryKey"].GetString();
		        if (entry.HasMember("pk_index") && entry["pk_index"].IsString()) {
    		        fe.pkIndex = entry["pk_index"].GetString();
		        }
		        newFilters.push_back(fe);
            }
        }
    }
    if (!newFilters.empty()) {
        filters.swap(newFilters);
        warnedTables.clear();
    }
    lastModifiedTime = fs::last_write_time(configPath);
}

bool KafkaProcessor::isCurrentlyReloading() {
    return isReloading.load();
}

void KafkaProcessor::startAutoReload(const std::string& configPath, KafkaConsumer* consumer) {
    stopReloading = false;

    reloadThread = std::thread([this, configPath, consumer]() {
        FileWatcher::watchFile(configPath, [this, configPath, consumer]() {
            std::lock_guard<std::mutex> lock(filterReloadMutex);

            Logger::info("🔁 KafkaProcessor detected change in filter config: " + configPath);
            isReloading = true;

            if (FilterConfigLoader::getInstance().loadConfig(configPath)) {
                const auto& newFilters = FilterConfigLoader::getInstance().getAllFilters();
                std::unordered_set<std::string> existingTables;
                for (const auto& f : filters) {
                    existingTables.insert(f.owner + "." + f.table);
                }

                for (const auto& f : newFilters) {
                    std::string fullTable = f.owner + "." + f.table;
                    if (existingTables.find(fullTable) == existingTables.end()) {
                        Logger::info("🆕 New table detected: " + fullTable);
                        OracleSchemaCache::getInstance().loadSchemaIfNeeded(fullTable, config);
                    }
                    addFilter(f);
                }

                filters = newFilters;
                Logger::info("✅ Processor reloaded filter config.");

                // 👉 Nếu có KafkaConsumer truyền vào, reload luôn
                if (consumer) {
                    consumer->reloadTableFilter(configPath);
                }
            } else {
                Logger::error("❌ Failed to reload filter config.");
            }

            isReloading = false;
        }, stopReloading);
    });
}

size_t KafkaProcessor::estimateDedupCacheMemory() {
    size_t total = 0;
    for (const auto& [table, cache] : recentPrimaryKeyCachePerTable) {
        total += sizeof(table) + cache.size() * (64 + sizeof(std::chrono::steady_clock::time_point));
    }
    total += recentGlobalPKCache.size() * (64 + sizeof(std::chrono::steady_clock::time_point));
    return total;
}

std::unordered_map<std::pair<std::string, int>, double, pair_hash> KafkaProcessor::getTotalLagByPartition() const {
    std::lock_guard<std::mutex> lock(lagMutex);
    std::unordered_map<std::pair<std::string, int>, double, pair_hash> result;

    for (const auto& [key, vec] : kafkaLagByPartition) {
        if (!vec.empty()) {
            double sum = 0.0;
            for (double lag : vec) sum += lag;
            result[key] = sum / vec.size();
        }
    }

    return result;
}

void KafkaProcessor::shrinkLagBuffersIfOversized() {
    std::lock_guard<std::mutex> lock(lagMutex);
    constexpr size_t MAX_LAG_SAMPLES = 1000;

    for (auto& [key, vec] : kafkaLagByPartition) {
        if (vec.size() > MAX_LAG_SAMPLES) {
            vec.erase(vec.begin(), vec.end() - MAX_LAG_SAMPLES);
            std::vector<double>().swap(vec); // shrink capacity
        }
    }

    for (auto& [table, vec] : kafkaLagTableMs) {
        if (vec.size() > MAX_LAG_SAMPLES) {
            vec.erase(vec.begin(), vec.end() - MAX_LAG_SAMPLES);
            std::vector<double>().swap(vec);
        }
    }
}

void KafkaProcessor::updateKafkaLagMetrics() {
    while (!stopReloading) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        std::unordered_map<std::string, std::vector<double>> lagCopyTable;
        std::unordered_map<std::pair<std::string, int>, std::vector<double>, pair_hash> lagCopyPartition;

        {
            std::lock_guard<std::mutex> lock(lagMutex);
            lagCopyTable.swap(kafkaLagTableMs);
            lagCopyPartition.swap(kafkaLagByPartition);
        }

	    // Shrink buffer để tránh giữ quá nhiều sample
	    shrinkLagBuffersIfNeeded(lagCopyTable, lagCopyPartition);

        for (const auto& [tableKey, lags] : lagCopyTable) {
            if (lags.empty()) continue;
            std::vector<double> sorted = lags;
            std::sort(sorted.begin(), sorted.end());

            double avg = std::accumulate(sorted.begin(), sorted.end(), 0.0) / sorted.size();
            double max = sorted.back();
            double p99 = sorted[static_cast<size_t>(std::min(sorted.size() - 1, size_t(sorted.size() * 0.99)))];

            MetricsExporter::getInstance().setMetric("kafka_lag_avg_ms", avg, {{"table", tableKey}});
            MetricsExporter::getInstance().setMetric("kafka_lag_max_ms", max, {{"table", tableKey}});
            MetricsExporter::getInstance().setMetric("kafka_lag_p99_ms", p99, {{"table", tableKey}});
        }

        for (const auto& [tp, lags] : lagCopyPartition) {
            if (lags.empty()) continue;
            std::string topic = tp.first;
            int partition = tp.second;

            double avg = std::accumulate(lags.begin(), lags.end(), 0.0) / lags.size();
            double max = *std::max_element(lags.begin(), lags.end());

            MetricsExporter::getInstance().setMetric("kafka_partition_lag_avg_ms", avg, {
                {"topic", topic},
                {"partition", std::to_string(partition)}
            });
            MetricsExporter::getInstance().setMetric("kafka_partition_lag_max_ms", max, {
                {"topic", topic},
                {"partition", std::to_string(partition)}
            });
        }

	lagCopyTable.clear();
	lagCopyPartition.clear();
    }
}

void KafkaProcessor::setKafkaTopic(const std::string& topic) {
    kafkaTopic = topic;
}

void KafkaProcessor::startDedupCleanup() {
    cleanupThread = std::thread([this]() {
        while (!stopCleanup.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(2));
            auto now = std::chrono::steady_clock::now();
            std::lock_guard<std::mutex> lock(dedupMutex);

            for (auto& [table, cache] : recentPrimaryKeyCachePerTable) {
                for (auto it = cache.begin(); it != cache.end();) {
                    if (std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count() >= 10) {
                        it = cache.erase(it);
                    } else {
                        ++it;
                    }
                }
                size_t after = cache.size();
		            //Logger::info("Global dedup cache size: " + std::to_string(recentGlobalPKCache.size()));
                MetricsExporter::getInstance().setMetric("dedup_cache_size", after, {{"table", table}});
            }
        }
    });
}

void KafkaProcessor::stopDedupCleanup() {
    stopCleanup.store(true);
    if (cleanupThread.joinable()) {
        cleanupThread.join();
    }
}

std::string KafkaProcessor::getPKIndexHint(const std::string& schema, const std::string& table) {
    return FilterConfigLoader::getInstance().getPKIndex(schema + "." + table);
}

void KafkaProcessor::startGlobalDedupCleanup() {
    stopGlobalCleanup = false;
    globalDedupCleanupThread = std::thread([this]() {
        while (!stopGlobalCleanup.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(2));
            auto now = std::chrono::steady_clock::now();
            size_t before = 0;
            size_t after = 0;

            {
                std::lock_guard<std::mutex> lock(globalCacheMutex);
                //before = recentGlobalPKCache.size();

                for (auto it = recentGlobalPKCache.begin(); it != recentGlobalPKCache.end();) {
                    if (std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count() >= 100) {
                        it = recentGlobalPKCache.erase(it);
                    } else {
                        ++it;
                    }
                }
                after = recentGlobalPKCache.size();
            }

            Logger::debug("🧹 Global dedup cache cleanup: before=" + std::to_string(before) +
                     ", after=" + std::to_string(after));

            MetricsExporter::getInstance().setMetric("global_dedup_cache_size", after);
        }
    });
}

void KafkaProcessor::stopGlobalDedupCleanup() {
    stopGlobalCleanup = true;
    if (globalDedupCleanupThread.joinable()) {
        globalDedupCleanupThread.join();
    }
}

void KafkaProcessor::shrinkLagBuffersIfNeeded(
    std::unordered_map<std::string, std::vector<double>>& tableLags,
    std::unordered_map<std::pair<std::string, int>, std::vector<double>, pair_hash>& partitionLags,
    size_t maxSamples) {

    for (auto& [table, lags] : tableLags) {
        if (lags.size() > maxSamples) {
            lags.erase(lags.begin(), lags.begin() + (lags.size() - maxSamples));
        }
    }

    for (auto& [tp, lags] : partitionLags) {
        if (lags.size() > maxSamples) {
            lags.erase(lags.begin(), lags.begin() + (lags.size() - maxSamples));
        }
    }
}

void KafkaProcessor::clearLagBuffers() {
    std::lock_guard<std::mutex> lock(lagMutex);
    kafkaLagTableMs.clear();
    kafkaLagByPartition.clear();
    Logger::warn("⚠️ Cleared all Kafka lag buffers to reduce memory.");
    MetricsExporter::getInstance().incrementCounter("kafka_lag_buffer_clear_total");
}

void KafkaProcessor::shrinkLagBuffers(int maxAgeSeconds) {
    std::lock_guard<std::mutex> lock(lagMutex);

    auto now = std::chrono::steady_clock::now();
    int removed = 0;

    for (auto it = kafkaLagByPartition.begin(); it != kafkaLagByPartition.end(); ++it) {
        auto lastUpdateIt = partitionLagLastUpdate.find(it->first);
        if (lastUpdateIt != partitionLagLastUpdate.end()) {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - lastUpdateIt->second).count();
            if (elapsed > maxAgeSeconds) {
                Logger::info("🧹 Shrinking lag buffer for topic=" + it->first.first +
                             ", partition=" + std::to_string(it->first.second) +
                             ", last updated " + std::to_string(elapsed) + "s ago");
                it->second.clear();
                removed++;
            }
        }
    }

    if (removed > 0) {
        Logger::info("✅ Shrunk " + std::to_string(removed) + " lag buffers older than "
                     + std::to_string(maxAgeSeconds) + "s");
    }
}

void KafkaProcessor::registerSQLBuilder(const std::string& dbType, std::unique_ptr<SQLBuilderBase> builder) {
    sqlBuilders[dbType] = std::move(builder);
}
