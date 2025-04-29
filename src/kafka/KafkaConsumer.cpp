#include "KafkaConsumer.h"
#include "FileWatcher.h"
#include "../logger/Logger.h"
#include "../metrics/MetricsServer.h"
#include "../metrics/MetricsExporter.h"
#include "../writer/CheckpointManager.h"
#include "../utils/KafkaMessageWrapper.h"

#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <cstring>
#include <sstream>
#include <utility>

KafkaConsumer::KafkaConsumer(KafkaProcessor& processor, const std::string& brokers, const std::string& topic, const std::string& groupId, const std::string& offsetReset, MetricsServer& metrics, const std::string& filterConfigPath, const std::string& enableAutoCommit)  // 🆕 Thêm queue)
    : processor(processor), brokers(brokers), topic(topic), groupId(groupId), stopReloading(true), filterConfigPath(filterConfigPath), consumer(nullptr), conf(nullptr), topics(nullptr), metrics(metrics), enableAutoCommit(enableAutoCommit) {

    //Load list tables
    loadTableFilter(filterConfigPath);
    Logger::info("Initializing KafkaConsumer with topic: " + topic);

    initKafka(offsetReset);
}


void KafkaConsumer::initKafka(const std::string& offsetReset) {
    char errstr[512];

    //Tạo Kafka configuration
    conf = rd_kafka_conf_new();
    if (!conf) {
	      Logger::error("Failed to create Kafka configuration.");
        return;
    }

    //Thiết lập các config cơ bản
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
	     Logger::error("Failed to set bootstrap.servers: " + std::string(errstr));
    }
    if (rd_kafka_conf_set(conf, "group.id", groupId.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
	     Logger::error("Failed to set group.id: " + std::string(errstr));
    }

    std::string validOffsetReset = (offsetReset == "earliest" || offsetReset == "latest" || offsetReset == "none")
                                   ? offsetReset
                                   : "latest";

    if (rd_kafka_conf_set(conf, "auto.offset.reset", validOffsetReset.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
	     Logger::info("⚠️ Failed to set auto.offset.reset to " + validOffsetReset + ", using default.");
    } else {
	     Logger::info("✔️ Kafka auto.offset.reset set to: " + validOffsetReset);
    }

    if (rd_kafka_conf_set(conf, "enable.auto.commit", enableAutoCommit.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
	     Logger::warn("⚠️ Failed to set enable.auto.commit to " + enableAutoCommit + ", using default.");
    } else {
	     Logger::info("✔️ Kafka enable.auto.commit set to: " + enableAutoCommit);
    }

    // 🚀 Cấu hình Kafka để xử lý message lớn hơn
    if (rd_kafka_conf_set(conf, "receive.message.max.bytes", "200000000", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
	     Logger::error("Failed to set receive.message.max.bytes: " + std::string(errstr));
    }
    if (rd_kafka_conf_set(conf, "fetch.message.max.bytes", "200000000", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
	     Logger::error("Failed to set fetch.message.max.bytes: " + std::string(errstr));
    }
    if (rd_kafka_conf_set(conf, "fetch.wait.max.ms", "1000", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
	     Logger::error("Failed to set fetch.wait.max.ms: " + std::string(errstr));
    }

    // Tạo Kafka consumer
    consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!consumer) {
	      Logger::error("Failed to create Kafka Consumer: " + std::string(errstr));
        return;
    }

    Logger::info("Kafka Consumer successfully created.");

    // Đăng ký topic
    topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, topic.c_str(), RD_KAFKA_PARTITION_UA);

    if (rd_kafka_poll_set_consumer(consumer) != RD_KAFKA_RESP_ERR_NO_ERROR) {
	     Logger::error("Failed to set consumer poll.");
    }
    if (rd_kafka_subscribe(consumer, topics) != RD_KAFKA_RESP_ERR_NO_ERROR) {
	     Logger::error("Failed to subscribe to topic: " + topic);
    } else {
	     Logger::info("Successfully subscribed to topic: " + topic);
    }
}

KafkaConsumer::~KafkaConsumer() {
	  Logger::info("Closing KafkaConsumer...");
    if (consumer) {
        rd_kafka_consumer_close(consumer);
        rd_kafka_destroy(consumer);
	      Logger::info("KafkaConsumer closed successfully.");
    }
    if (topics) {
        rd_kafka_topic_partition_list_destroy(topics);
    }
}

bool KafkaConsumer::consumeMessage(std::string& message, int& partition, int64_t& offset, int64_t& timestamp, rd_kafka_message_t** rawMsg) {
    static int nullPollCount = 0;
    const int maxNullPollBeforeLog = 10;

    if (!consumer) {
	      Logger::error("Consumer is not initialized.");
        return false;
    }

    rd_kafka_message_t* msg = rd_kafka_consumer_poll(consumer, 1000);
    if (!msg) {
        nullPollCount++;
        if (nullPollCount >= maxNullPollBeforeLog) {
	          Logger::debug("⚠️ Kafka poll returned NULL.");
            nullPollCount = 0;
        }
        return false;
    }

    nullPollCount = 0;

    if (msg->err) {
	      Logger::error("Kafka consume error: " + std::string(rd_kafka_err2str(msg->err)));
        rd_kafka_message_destroy(msg);
        return false;
    }

    if (!msg->payload) {
	      Logger::error("Received Kafka message with NULL payload!");
        rd_kafka_message_destroy(msg);
        return false;
    }

    message.assign(static_cast<char*>(msg->payload), msg->len);

    rapidjson::Document doc;
    doc.Parse(message.c_str());
    if (doc.HasParseError()) {
	      Logger::error("❌ JSON parse error in Kafka message.");
        rd_kafka_message_destroy(msg);
        return false;
    }

    if (!doc.HasMember("payload") || !doc["payload"].IsArray()) {
        rd_kafka_message_destroy(msg);
        return false;
    }

    const rapidjson::Value& payloadArray = doc["payload"];
    bool validRecordFound = false;

    for (rapidjson::SizeType i = 0; i < payloadArray.Size(); i++) {
        const rapidjson::Value& record = payloadArray[i];

        // Skip nếu thiếu "op"
        if (!record.HasMember("op") || !record["op"].IsString())
            continue;

        std::string op = record["op"].GetString();
        if (op != "c" && op != "u" && op != "d") {
            continue; // Skip begin, commit, snapshot
        }

        // Skip nếu không có schema
        if (!record.HasMember("schema") || !record["schema"].IsObject())
            continue;

        const auto& schema = record["schema"];
        if (!schema.HasMember("owner") || !schema.HasMember("table"))
            continue;

        std::string owner = schema["owner"].GetString();
        std::string table = schema["table"].GetString();
        if (!isTableFiltered(owner, table))
            continue;

        validRecordFound = true;
        break;  // chỉ cần 1 bản ghi hợp lệ
    }

    if (!validRecordFound) {
        rd_kafka_message_destroy(msg);
        return false;
    }

    partition = msg->partition;
    offset = msg->offset;

    rd_kafka_timestamp_type_t timestampType;
    timestamp = rd_kafka_message_timestamp(msg, &timestampType);
    if (timestamp <= 0) timestamp = 0;

    try {
        MetricsExporter::getInstance().incrementCounter("kafka_messages_processed");
    } catch (const std::exception& e) {
	     Logger::error("Error updating metrics: " + std::string(e.what()));
    }

    if (rawMsg) *rawMsg = msg;
    // Đừng gọi rd_kafka_message_destroy() ở đây nữa — sẽ destroy sau khi ghi thành công
    return true;
}

//Kiểm tra bảng có trong danh sách filter từ KafkaProcessor
bool KafkaConsumer::isTableFiltered(const std::string& owner, const std::string& table) {
    if (processor.isCurrentlyReloading()) {
        //Logger::debug("⏳ KafkaProcessor is reloading. Skipping filter check...");
        return false;
    }
    return (tableFilter.find(owner + "." + table) != tableFilter.end());
}

void KafkaConsumer::loadTableFilter(const std::string& configPath) {
    std::ifstream ifs(configPath);
    if (!ifs.is_open()) {
	      Logger::error("Unable to open filter config file: " + configPath);
        return;
    } else {
	      Logger::info("KafkaConsumer is loading filter config from: " + configPath);
    }

    rapidjson::IStreamWrapper isw(ifs);
    rapidjson::Document doc;
    doc.ParseStream(isw);

    if (doc.HasParseError()) {
	      Logger::error("JSON parse error in filter config file: " + configPath);
        return;
    }

    std::unordered_set<std::string> newFilter;  //Danh sách filter mới
    bool isFirstLoad = tableFilter.empty();  //Kiểm tra lần đầu load

    if (doc.HasMember("tables") && doc["tables"].IsArray()) {
        const auto& tables = doc["tables"];

        if (isFirstLoad) {
	         Logger::info("✅ [Filtered Tables] Initial Whitelist tables:");
        }

        std::vector<std::string> addedTables;
        std::vector<std::string> removedTables;

        //Duyệt danh sách mới từ JSON
        for (rapidjson::SizeType i = 0; i < tables.Size(); i++) {
            const auto& entry = tables[i];
            if (entry.HasMember("owner") && entry["owner"].IsString() &&
                entry.HasMember("table") && entry["table"].IsString()) {

                std::string owner = entry["owner"].GetString();
                std::string table = entry["table"].GetString();
                std::string key = owner + "." + table;

                newFilter.insert(key);

                if (isFirstLoad) {
		                Logger::info("✔️ Table added to filter: - " + key);
                } else if (tableFilter.find(key) == tableFilter.end()) {
                    addedTables.push_back(key);
                }
            }
        }

        //Kiểm tra các bảng đã bị xóa khỏi filter cũ
        for (const auto& oldTable : tableFilter) {
            if (newFilter.find(oldTable) == newFilter.end()) {
                removedTables.push_back(oldTable);
            }
        }

        //If have new table
        if (!isFirstLoad && !addedTables.empty()) {
	          Logger::info("✔️ Filter config updated. New tables loaded:");
            for (const auto& tbl : addedTables) {
		            Logger::info("➕ New Table: " + tbl);
            }
        }

        //Nếu có bảng bị xóa, log cảnh báo
        if (!isFirstLoad && !removedTables.empty()) {
	          Logger::warn("❌ Tables removed from filter:");
            for (const auto& tbl : removedTables) {
		            Logger::warn("➖ Removed Table: " + tbl);
            }
        }

    } else {
	     Logger::error("Filter config missing 'tables' array.");
        return;
    }

    //Cập nhật danh sách `tableFilter` đúng cách
    tableFilter.swap(newFilter);  //Thay thế toàn bộ danh sách cũ bằng danh sách mới

    lastModifiedTime = std::filesystem::last_write_time(configPath);
}

void KafkaConsumer::startAutoReload(const std::string& configPath) {
    stopReloading = false;
    reloadThread = std::thread([this, configPath]() {
        FileWatcher::watchFile(configPath, [this]() {
            Logger::info("🔁 KafkaConsumer detected change in filter config: " + filterConfigPath);
            loadTableFilter(filterConfigPath);
        }, stopReloading);
    });
}

void KafkaConsumer::reloadTableFilter(const std::string& configPath) {
    Logger::info("🔁 KafkaConsumer reloading table filter...");
    loadTableFilter(configPath);
}

void KafkaConsumer::printFilteredTables() {
    Logger::info("Filtered Tables: ");
    for (const auto& table : tableFilter) {
	 Logger::info(" - " + table);
    }
}

//commitoffset
void KafkaConsumer::commitOffset(rd_kafka_message_t* message) {
    if (!message || !consumer) return;
    rd_kafka_resp_err_t err = rd_kafka_commit_message(consumer, message, 0);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
	Logger::error("❌ Failed to commit offset: " + std::string(rd_kafka_err2str(err)));
    } else {
        Logger::debug("✅ Kafka offset committed: topic=" + std::string(rd_kafka_topic_name(message->rkt)) + ", partition=" + std::to_string(message->partition) + ", offset=" + std::to_string(message->offset));
    }
    //rd_kafka_message_destroy(message);
}

void KafkaConsumer::rebalanceCallback(rd_kafka_t* rk,
                                      rd_kafka_resp_err_t err,
                                      rd_kafka_topic_partition_list_t* partitions,
                                      void* opaque) {
    KafkaConsumer* consumer = static_cast<KafkaConsumer*>(opaque);
    (void)consumer;
    auto& checkpointMgr = CheckpointManager::getInstance();

    if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
        std::cout << "[KafkaConsumer] ⚖️  Rebalancing: assigning partitions..." << std::endl;
        rd_kafka_assign(rk, partitions);

        for (int i = 0; i < partitions->cnt; ++i) {
            rd_kafka_topic_partition_t* p = partitions->elems + i;
            std::string topic = p->topic;
            int partition = p->partition;
            int64_t offset = checkpointMgr.getLastCheckpoint(topic, partition);

            if (offset >= 0) {

		rd_kafka_topic_t* rkt = rd_kafka_topic_new(rk, topic.c_str(), nullptr);
		if (!rkt) {
    		   Logger::error("[KafkaConsumer] ❌ Failed to create topic handle for " + topic);
    		   continue;
		}

		if (rd_kafka_seek(rkt, partition, offset, 1000) != 0) {
    		   std::cerr << "[KafkaConsumer] ❌ Seek failed for " << topic << ":" << partition
                    << " to offset " << offset << "\n";
		} else {
    			std::cout << "[KafkaConsumer] 🔁 Seeked to offset " << offset << " for "
              		<< topic << ":" << partition << "\n";
		}

		rd_kafka_topic_destroy(rkt);

            } else {
                std::cout << "[KafkaConsumer] ℹ️ No checkpoint found for " << topic << ":" << partition << "\n";
            }
        }
    } else if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
        std::cout << "[KafkaConsumer] ⚖️  Rebalancing: revoking partitions..." << std::endl;
        rd_kafka_assign(rk, nullptr);
    } else {
        std::cerr << "[KafkaConsumer] ⚠️ Rebalance error: " << rd_kafka_err2str(err) << std::endl;
        rd_kafka_assign(rk, nullptr);
    }
}
