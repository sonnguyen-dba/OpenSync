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

KafkaConsumer::KafkaConsumer(KafkaProcessor& processor, const std::string& brokers, const std::string& topic, const std::string& groupId, const std::string& offsetReset, const std::string& enableAutoCommit, MetricsServer& metrics, const std::string& filterConfigPath)  // 🆕 Thêm queue)
    : processor(processor), brokers(brokers), topic(topic), groupId(groupId), stopReloading(true), filterConfigPath(filterConfigPath), consumer(nullptr), conf(nullptr), topics(nullptr), metrics(metrics), enableAutoCommit(enableAutoCommit) {

    //Load danh sách bảng ngay khi khởi động
    loadTableFilter(filterConfigPath);
    //Chạy thread kiểm tra thay đổi file
    //reloadThread = std::thread(&KafkaConsumer::reloadFilterConfigLoop, this);

    OpenSync::Logger::info("Initializing KafkaConsumer with topic: " + topic);
    //Tách khởi tạo Kafka ra một hàm riêng
    initKafka(offsetReset);
}


void KafkaConsumer::initKafka(const std::string& offsetReset) {
    char errstr[512];

    //Tạo Kafka configuration
    conf = rd_kafka_conf_new();
    if (!conf) {
	OpenSync::Logger::error("Failed to create Kafka configuration.");
        return;
    }

    //Thiết lập các config cơ bản
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
	OpenSync::Logger::error("Failed to set bootstrap.servers: " + std::string(errstr));
    }
    if (rd_kafka_conf_set(conf, "group.id", groupId.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
	OpenSync::Logger::error("Failed to set group.id: " + std::string(errstr));
    }

    std::string validOffsetReset = (offsetReset == "earliest" || offsetReset == "latest" || offsetReset == "none")
                                   ? offsetReset
                                   : "latest";

    if (rd_kafka_conf_set(conf, "auto.offset.reset", validOffsetReset.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
	OpenSync::Logger::info("⚠️ Failed to set auto.offset.reset to " + validOffsetReset + ", using default.");
    } else {
	OpenSync::Logger::info("✔️ Kafka auto.offset.reset set to: " + validOffsetReset);
    }

    if (rd_kafka_conf_set(conf, "enable.auto.commit", enableAutoCommit.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
	OpenSync::Logger::warn("⚠️ Failed to set enable.auto.commit to " + enableAutoCommit + ", using default.");
    } else {
	OpenSync::Logger::info("✔️ Kafka enable.auto.commit set to: " + enableAutoCommit);
    }

    // 🚀 Cấu hình Kafka để xử lý message lớn hơn
    if (rd_kafka_conf_set(conf, "receive.message.max.bytes", "200000000", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
	OpenSync::Logger::error("Failed to set receive.message.max.bytes: " + std::string(errstr));
    }
    if (rd_kafka_conf_set(conf, "fetch.message.max.bytes", "200000000", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
	OpenSync::Logger::error("Failed to set fetch.message.max.bytes: " + std::string(errstr));
    }
    if (rd_kafka_conf_set(conf, "fetch.wait.max.ms", "1000", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
	OpenSync::Logger::error("Failed to set fetch.wait.max.ms: " + std::string(errstr));
    }

    // Đăng ký rebalance callback và opaque trước khi tạo consumer
    //rd_kafka_conf_set_rebalance_cb(conf, rebalanceCallback);
    //rd_kafka_conf_set_opaque(conf, this);
    //OpenSync::Logger::info("✔️ Registered rebalance callback for Kafka consumer.");

    // Tạo Kafka consumer
    consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!consumer) {
	OpenSync::Logger::error("Failed to create Kafka Consumer: " + std::string(errstr));
        return;
    }

    OpenSync::Logger::info("Kafka Consumer successfully created.");

    // Đăng ký topic
    topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, topic.c_str(), RD_KAFKA_PARTITION_UA);

    if (rd_kafka_poll_set_consumer(consumer) != RD_KAFKA_RESP_ERR_NO_ERROR) {
	OpenSync::Logger::error("Failed to set consumer poll.");
    }
    if (rd_kafka_subscribe(consumer, topics) != RD_KAFKA_RESP_ERR_NO_ERROR) {
	OpenSync::Logger::error("Failed to subscribe to topic: " + topic);
    } else {
	    OpenSync::Logger::info("Successfully subscribed to topic: " + topic);
    }

    // Đăng ký rebalance callback
    rd_kafka_conf_set_rebalance_cb(conf, rebalanceCallback);
    rd_kafka_conf_set_opaque(conf, this);
}

KafkaConsumer::~KafkaConsumer() {
	OpenSync::Logger::info("Closing KafkaConsumer...");
    if (consumer) {
        rd_kafka_consumer_close(consumer);
        rd_kafka_destroy(consumer);
	OpenSync::Logger::info("KafkaConsumer closed successfully.");
    }
    if (topics) {
        rd_kafka_topic_partition_list_destroy(topics);
    }
}

bool KafkaConsumer::consumeMessage(std::string& message, int& partition, int64_t& offset, int64_t& timestamp, rd_kafka_message_t** rawMsg) {
    static int nullPollCount = 0;
    const int maxNullPollBeforeLog = 30;

    if (!consumer) {
	OpenSync::Logger::error("Consumer is not initialized.");
        return false;
    }

    rd_kafka_message_t* msg = rd_kafka_consumer_poll(consumer, 1000);
    if (!msg) {
        nullPollCount++;
        if (nullPollCount >= maxNullPollBeforeLog) {
	    OpenSync::Logger::debug("⚠️ Kafka poll returned NULL.");
            nullPollCount = 0;
        }
        return false;
    }

    nullPollCount = 0;

    if (msg->err) {
	OpenSync::Logger::error("Kafka consume error: " + std::string(rd_kafka_err2str(msg->err)));
        rd_kafka_message_destroy(msg);
        return false;
    }

    if (!msg->payload) {
	OpenSync::Logger::error("Received Kafka message with NULL payload!");
        rd_kafka_message_destroy(msg);
        return false;
    }

    message.assign(static_cast<char*>(msg->payload), msg->len);

    rapidjson::Document doc;
    doc.Parse(message.c_str());
    if (doc.HasParseError()) {
	OpenSync::Logger::error("❌ JSON parse error in Kafka message.");
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
	OpenSync::Logger::error("Error updating metrics: " + std::string(e.what()));
    }

    if (rawMsg) *rawMsg = msg;
    // Đừng gọi rd_kafka_message_destroy() ở đây nữa — sẽ destroy sau khi ghi thành công
    return true;
}

//Kiểm tra bảng có trong danh sách filter từ KafkaProcessor
bool KafkaConsumer::isTableFiltered(const std::string& owner, const std::string& table) {
    if (processor.isCurrentlyReloading()) {
        //OpenSync::Logger::debug("⏳ KafkaProcessor is reloading. Skipping filter check...");
        return false;
    }
    return (tableFilter.find(owner + "." + table) != tableFilter.end());
}

void KafkaConsumer::loadTableFilter(const std::string& configPath) {
    std::ifstream ifs(configPath);
    if (!ifs.is_open()) {
	OpenSync::Logger::error("Unable to open filter config file: " + configPath);
        return;
    } else {
	OpenSync::Logger::info("KafkaConsumer is loading filter config from: " + configPath);
    }

    rapidjson::IStreamWrapper isw(ifs);
    rapidjson::Document doc;
    doc.ParseStream(isw);

    if (doc.HasParseError()) {
	OpenSync::Logger::error("JSON parse error in filter config file: " + configPath);
        return;
    }

    std::unordered_set<std::string> newFilter;  //Danh sách filter mới
    bool isFirstLoad = tableFilter.empty();  //Kiểm tra lần đầu load

    if (doc.HasMember("tables") && doc["tables"].IsArray()) {
        const auto& tables = doc["tables"];

        if (isFirstLoad) {
	    OpenSync::Logger::info("✅ [Filtered Tables] Initial Whitelist tables:");
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
		    OpenSync::Logger::info("✔️ Table added to filter: - " + key);
                } else if (tableFilter.find(key) == tableFilter.end()) {
                    addedTables.push_back(key);
                }
            }
        }

        // ✅ Kiểm tra các bảng đã bị xóa khỏi filter cũ
        for (const auto& oldTable : tableFilter) {
            if (newFilter.find(oldTable) == newFilter.end()) {
                removedTables.push_back(oldTable);
            }
        }

        //Nếu có bảng mới, log cập nhật
        if (!isFirstLoad && !addedTables.empty()) {
	    OpenSync::Logger::info("✔️ Filter config updated. New tables loaded:");
            for (const auto& tbl : addedTables) {
		 OpenSync::Logger::info("➕ New Table: " + tbl);
            }
        }

        //Nếu có bảng bị xóa, log cảnh báo
        if (!isFirstLoad && !removedTables.empty()) {
	    OpenSync::Logger::warn("❌ Tables removed from filter:");
            for (const auto& tbl : removedTables) {
		OpenSync::Logger::warn("➖ Removed Table: " + tbl);
            }
        }

    } else {
	OpenSync::Logger::error("Filter config missing 'tables' array.");
        return;
    }

    //Cập nhật danh sách `tableFilter` đúng cách
    tableFilter.swap(newFilter);  //Thay thế toàn bộ danh sách cũ bằng danh sách mới

    lastModifiedTime = std::filesystem::last_write_time(configPath);
}

/*void KafkaConsumer::reloadFilterConfigLoop() {
    while (!stopReloading) {
        std::this_thread::sleep_for(std::chrono::seconds(120));  // Kiểm tra mỗi 10 giây

        auto currentModifiedTime = std::filesystem::last_write_time(filterConfigPath);
        if (currentModifiedTime != lastModifiedTime) {
	    OpenSync::Logger::info("🔄 Detected change in filter_config.json, reloading...");
            loadTableFilter(filterConfigPath);
        }
    }
}*/

void KafkaConsumer::startAutoReload(const std::string& configPath) {
    stopReloading = false;
    reloadThread = std::thread([this, configPath]() {
        FileWatcher::watchFile(configPath, [this]() {
            OpenSync::Logger::info("🔁 KafkaConsumer detected change in filter config: " + filterConfigPath);
            loadTableFilter(filterConfigPath);
        }, stopReloading);
    });
}

void KafkaConsumer::reloadTableFilter(const std::string& configPath) {
    OpenSync::Logger::info("🔁 KafkaConsumer reloading table filter...");
    loadTableFilter(configPath);
}


void KafkaConsumer::printFilteredTables() {
    OpenSync::Logger::info("Filtered Tables: ");
    for (const auto& table : tableFilter) {
	 OpenSync::Logger::info(" - " + table);
    }
}

//commitoffset 
/*void KafkaConsumer::commitOffset(rd_kafka_message_t* message) {
    if (!message || !consumer) return;
    rd_kafka_resp_err_t err = rd_kafka_commit_message(consumer, message, 0);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
	OpenSync::Logger::error("❌ Failed to commit offset: " + std::string(rd_kafka_err2str(err)));
    } else {
        OpenSync::Logger::debug("✅ Kafka offset committed: topic=" + std::string(rd_kafka_topic_name(message->rkt)) + ", partition=" + std::to_string(message->partition) + ", offset=" + std::to_string(message->offset));
    }
    //rd_kafka_message_destroy(message);
}*/

void KafkaConsumer::commitOffset(rd_kafka_message_t* message) {
    if (!message || !consumer) {
        OpenSync::Logger::error("❌ Invalid message or consumer not initialized for offset commit.");
        return;
    }

    std::string topic = rd_kafka_topic_name(message->rkt);
    int partition = message->partition;
    int64_t offset = message->offset;

    rd_kafka_resp_err_t err = rd_kafka_commit_message(consumer, message, 0);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        OpenSync::Logger::error("❌ Failed to commit offset: " + std::string(rd_kafka_err2str(err)));
    } else {
        //OpenSync::Logger::info("✅ Kafka offset committed: topic=" + topic +
        //                       ", partition=" + std::to_string(partition) +
        //                       ", offset=" + std::to_string(offset));
        // Ghi checkpoint vào file ngay sau khi commit offset
        CheckpointManager::getInstance(filterConfigPath).updateCheckpoint(topic, partition, offset);
    }
}

void KafkaConsumer::rebalanceCallback(rd_kafka_t* rk,
                                     rd_kafka_resp_err_t err,
                                     rd_kafka_topic_partition_list_t* partitions,
                                     void* opaque) {
    KafkaConsumer* consumer = static_cast<KafkaConsumer*>(opaque);
    auto& checkpointMgr = CheckpointManager::getInstance(consumer->filterConfigPath);

    if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
        OpenSync::Logger::info("⚖️ Rebalancing: assigning partitions...");
        rd_kafka_assign(rk, partitions);

        for (int i = 0; i < partitions->cnt; ++i) {
            rd_kafka_topic_partition_t* p = partitions->elems + i;
            std::string topic = p->topic;
            int partition = p->partition;
            int64_t offset = checkpointMgr.getLastCheckpoint(topic, partition);

            if (offset >= 0) {
                // Kiểm tra offset hợp lệ
                rd_kafka_topic_partition_list_t* offset_check = rd_kafka_topic_partition_list_new(1);
                rd_kafka_topic_partition_list_add(offset_check, topic.c_str(), partition);
                rd_kafka_topic_partition_t* part = offset_check->elems;
                part->offset = offset;

                rd_kafka_resp_err_t seek_err = rd_kafka_offsets_for_times(rk, offset_check, 1000);
                if (seek_err != RD_KAFKA_RESP_ERR_NO_ERROR || part->offset < 0) {
                    OpenSync::Logger::warn("⚠️ Invalid offset for " + topic + ":" +
                                          std::to_string(partition) + ": " + rd_kafka_err2str(seek_err));
                    MetricsExporter::getInstance().incrementCounter("kafka_offset_reset");
                    offset = RD_KAFKA_OFFSET_END;
                } else {
                    offset = part->offset;
                }
                rd_kafka_topic_partition_list_destroy(offset_check);

                rd_kafka_topic_t* rkt = rd_kafka_topic_new(rk, topic.c_str(), nullptr);
                if (!rkt) {
                    OpenSync::Logger::error("❌ Failed to create topic handle for " + topic);
                    continue;
                }

                if (rd_kafka_seek(rkt, partition, offset, 1000) != 0) {
                    OpenSync::Logger::error("❌ Seek failed for " + topic + ":" +
                                           std::to_string(partition) + " to offset " + std::to_string(offset));
                } else {
                    OpenSync::Logger::info("🔁 Seeked to offset " + std::to_string(offset) +
                                          " for " + topic + ":" + std::to_string(partition));
                }

                rd_kafka_topic_destroy(rkt);
            } else {
                OpenSync::Logger::info("ℹ️ No checkpoint found for " + topic + ":" +
                                      std::to_string(partition) + ", using latest offset.");
            }
        }
    } else if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
        OpenSync::Logger::info("⚖️ Rebalancing: revoking partitions...");
        rd_kafka_assign(rk, nullptr);
    } else {
        OpenSync::Logger::error("⚠️ Rebalance error: " + std::string(rd_kafka_err2str(err)));
        rd_kafka_assign(rk, nullptr);
    }
}

/*void KafkaConsumer::rebalanceCallback(rd_kafka_t* rk,
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
    		   OpenSync::Logger::error("[KafkaConsumer] ❌ Failed to create topic handle for " + topic);
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
}*/


/*void KafkaConsumer::commitOffset(rd_kafka_message_t* message) {
    if (!message || !consumer) {
        OpenSync::Logger::error("❌ Invalid message or consumer not initialized for offset commit.");
        return;
    }

    std::string topic = rd_kafka_topic_name(message->rkt);
    int partition = message->partition;
    int64_t offset = message->offset;

    rd_kafka_resp_err_t err = rd_kafka_commit_message(consumer, message, 0);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        OpenSync::Logger::error("❌ Failed to commit offset: " + std::string(rd_kafka_err2str(err)));
    } else {
        OpenSync::Logger::info("✅ Kafka offset committed: topic=" + topic +
                              ", partition=" + std::to_string(partition) +
                              ", offset=" + std::to_string(offset));
        CheckpointManager::getInstance(filterConfigPath).updateCheckpoint(topic, partition, offset);
    }
}*/

/*void KafkaConsumer::rebalanceCallback(rd_kafka_t* rk,
                                     rd_kafka_resp_err_t err,
                                     rd_kafka_topic_partition_list_t* partitions,
                                     void* opaque) {
    KafkaConsumer* consumer = static_cast<KafkaConsumer*>(opaque);
    auto& checkpointMgr = CheckpointManager::getInstance(consumer->filterConfigPath);

    if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
        OpenSync::Logger::info("⚖️ Rebalancing: assigning partitions...");
        rd_kafka_assign(rk, partitions);

        for (int i = 0; i < partitions->cnt; ++i) {
            rd_kafka_topic_partition_t* p = partitions->elems + i;
            std::string topic = p->topic;
            int partition = p->partition;
            int64_t checkpointOffset = checkpointMgr.getLastCheckpoint(topic, partition);

            int64_t offset = RD_KAFKA_OFFSET_END;
            if (checkpointOffset >= 0) {
                OpenSync::Logger::info("ℹ️ Checkpoint found for " + topic + ":" +
                                      std::to_string(partition) + ", offset=" + std::to_string(checkpointOffset));
                rd_kafka_topic_partition_list_t* offsetCheck = rd_kafka_topic_partition_list_new(1);
                rd_kafka_topic_partition_list_add(offsetCheck, topic.c_str(), partition);
                rd_kafka_topic_partition_t* part = offsetCheck->elems;
                part->offset = checkpointOffset;

                rd_kafka_resp_err_t seekErr = rd_kafka_offsets_for_times(rk, offsetCheck, 2000);
                if (seekErr == RD_KAFKA_RESP_ERR_NO_ERROR && part->offset >= 0) {
                    offset = part->offset;
                    OpenSync::Logger::info("✅ Valid offset found: " + topic + ":" +
                                          std::to_string(partition) + ", offset=" + std::to_string(offset));
                } else {
                    OpenSync::Logger::warn("⚠️ Invalid checkpoint offset " + std::to_string(checkpointOffset) +
                                          " for " + topic + ":" + std::to_string(partition) +
                                          ". Error: " + std::string(rd_kafka_err2str(seekErr)));
                    MetricsExporter::getInstance().incrementCounter("kafka_offset_reset");

                    // Thử offset sớm nhất
                    rd_kafka_topic_partition_list_t* earliestCheck = rd_kafka_topic_partition_list_new(1);
                    rd_kafka_topic_partition_list_add(earliestCheck, topic.c_str(), partition);
                    part = earliestCheck->elems;
                    part->offset = RD_KAFKA_OFFSET_BEGINNING;

                    seekErr = rd_kafka_offsets_for_times(rk, earliestCheck, 2000);
                    if (seekErr == RD_KAFKA_RESP_ERR_NO_ERROR && part->offset >= 0) {
                        offset = part->offset;
                        OpenSync::Logger::info("🔄 Using earliest available offset: " + std::to_string(offset) +
                                              " for " + topic + ":" + std::to_string(partition));
                        MetricsExporter::getInstance().incrementCounter("kafka_offset_reset_to_beginning");
                    } else {
                        OpenSync::Logger::warn("⚠️ No valid offset found, falling back to END for " +
                                              topic + ":" + std::to_string(partition));
                        offset = RD_KAFKA_OFFSET_END;
                        MetricsExporter::getInstance().incrementCounter("kafka_offset_reset_to_end");
                    }
                    rd_kafka_topic_partition_list_destroy(earliestCheck);
                }
                rd_kafka_topic_partition_list_destroy(offsetCheck);
            } else {
                OpenSync::Logger::info("ℹ️ No checkpoint found for " + topic + ":" +
                                      std::to_string(partition) + ", trying earliest offset.");
                // Thử offset sớm nhất thay vì END
                rd_kafka_topic_partition_list_t* earliestCheck = rd_kafka_topic_partition_list_new(1);
                rd_kafka_topic_partition_list_add(earliestCheck, topic.c_str(), partition);
                rd_kafka_topic_partition_t* part = earliestCheck->elems;
                part->offset = RD_KAFKA_OFFSET_BEGINNING;

                rd_kafka_resp_err_t seekErr = rd_kafka_offsets_for_times(rk, earliestCheck, 2000);
                if (seekErr == RD_KAFKA_RESP_ERR_NO_ERROR && part->offset >= 0) {
                    offset = part->offset;
                    OpenSync::Logger::info("🔄 Using earliest available offset: " + std::to_string(offset) +
                                          " for " + topic + ":" + std::to_string(partition));
                    MetricsExporter::getInstance().incrementCounter("kafka_offset_reset_to_beginning");
                } else {
                    OpenSync::Logger::warn("⚠️ No valid earliest offset found, falling back to END for " +
                                          topic + ":" + std::to_string(partition));
                    offset = RD_KAFKA_OFFSET_END;
                    MetricsExporter::getInstance().incrementCounter("kafka_offset_reset_to_end");
                }
                rd_kafka_topic_partition_list_destroy(earliestCheck);
            }

            rd_kafka_topic_t* rkt = rd_kafka_topic_new(rk, topic.c_str(), nullptr);
            if (!rkt) {
                OpenSync::Logger::error("❌ Failed to create topic handle for " + topic);
                continue;
            }

            if (rd_kafka_seek(rkt, partition, offset, 2000) != 0) {
                OpenSync::Logger::error("❌ Seek failed for " + topic + ":" +
                                       std::to_string(partition) + " to offset " + std::to_string(offset));
            } else {
                OpenSync::Logger::info("🔁 Seeked to offset " + std::to_string(offset) +
                                      " for " + topic + ":" + std::to_string(partition));
            }

            rd_kafka_topic_destroy(rkt);
        }
    } else if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
        OpenSync::Logger::info("⚖️ Rebalancing: revoking partitions...");
        rd_kafka_assign(rk, nullptr);
    } else {
        OpenSync::Logger::error("⚠️ Rebalance error: " + std::string(rd_kafka_err2str(err)));
        rd_kafka_assign(rk, nullptr);
    }
}*/
/*
void KafkaConsumer::rebalanceCallback(rd_kafka_t* rk,
                                     rd_kafka_resp_err_t err,
                                     rd_kafka_topic_partition_list_t* partitions,
                                     void* opaque) {
    KafkaConsumer* consumer = static_cast<KafkaConsumer*>(opaque);
    auto& checkpointMgr = CheckpointManager::getInstance(consumer->filterConfigPath);

    if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
        OpenSync::Logger::info("⚖️ Rebalancing: assigning partitions...");
        rd_kafka_assign(rk, partitions);

        for (int i = 0; i < partitions->cnt; ++i) {
            rd_kafka_topic_partition_t* p = partitions->elems + i;
            std::string topic = p->topic;
            int partition = p->partition;
            int64_t checkpointOffset = checkpointMgr.getLastCheckpoint(topic, partition);

            int64_t offset = RD_KAFKA_OFFSET_END;
            if (checkpointOffset >= 0) {
                OpenSync::Logger::info("ℹ️ Checkpoint found for " + topic + ":" +
                                      std::to_string(partition) + ", offset=" + std::to_string(checkpointOffset));
                rd_kafka_topic_partition_list_t* offsetCheck = rd_kafka_topic_partition_list_new(1);
                rd_kafka_topic_partition_list_add(offsetCheck, topic.c_str(), partition);
                rd_kafka_topic_partition_t* part = offsetCheck->elems;
                part->offset = checkpointOffset;

                rd_kafka_resp_err_t seekErr = rd_kafka_offsets_for_times(rk, offsetCheck, 1000);
                if (seekErr == RD_KAFKA_RESP_ERR_NO_ERROR && part->offset >= 0) {
                    offset = part->offset;
                    OpenSync::Logger::info("✅ Valid offset found: " + topic + ":" +
                                          std::to_string(partition) + ", offset=" + std::to_string(offset));
                } else {
                    OpenSync::Logger::warn("⚠️ Invalid checkpoint offset " + std::to_string(checkpointOffset) +
                                          " for " + topic + ":" + std::to_string(partition) +
                                          ". Querying earliest available offset.");
                    MetricsExporter::getInstance().incrementCounter("kafka_offset_reset");

                    // Thử offset sớm nhất
                    rd_kafka_topic_partition_list_t* earliestCheck = rd_kafka_topic_partition_list_new(1);
                    rd_kafka_topic_partition_list_add(earliestCheck, topic.c_str(), partition);
                    part = earliestCheck->elems;
                    part->offset = RD_KAFKA_OFFSET_BEGINNING;

                    seekErr = rd_kafka_offsets_for_times(rk, earliestCheck, 1000);
                    if (seekErr == RD_KAFKA_RESP_ERR_NO_ERROR && part->offset >= 0) {
                        offset = part->offset;
                        OpenSync::Logger::info("🔄 Using earliest available offset: " + std::to_string(offset) +
                                              " for " + topic + ":" + std::to_string(partition));
                        MetricsExporter::getInstance().incrementCounter("kafka_offset_reset_to_beginning");
                    } else {
                        OpenSync::Logger::warn("⚠️ No valid offset found, falling back to END for " +
                                              topic + ":" + std::to_string(partition));
                        offset = RD_KAFKA_OFFSET_END;
                        MetricsExporter::getInstance().incrementCounter("kafka_offset_reset_to_end");
                    }
                    rd_kafka_topic_partition_list_destroy(earliestCheck);
                }
                rd_kafka_topic_partition_list_destroy(offsetCheck);
            } else {
                OpenSync::Logger::info("ℹ️ No checkpoint found for " + topic + ":" +
                                      std::to_string(partition) + ", using latest offset.");
            }

            rd_kafka_topic_t* rkt = rd_kafka_topic_new(rk, topic.c_str(), nullptr);
            if (!rkt) {
                OpenSync::Logger::error("❌ Failed to create topic handle for " + topic);
                continue;
            }

            if (rd_kafka_seek(rkt, partition, offset, 1000) != 0) {
                OpenSync::Logger::error("❌ Seek failed for " + topic + ":" +
                                       std::to_string(partition) + " to offset " + std::to_string(offset));
            } else {
                OpenSync::Logger::info("🔁 Seeked to offset " + std::to_string(offset) +
                                      " for " + topic + ":" + std::to_string(partition));
            }

            rd_kafka_topic_destroy(rkt);
        }
    } else if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
        OpenSync::Logger::info("⚖️ Rebalancing: revoking partitions...");
        rd_kafka_assign(rk, nullptr);
    } else {
        OpenSync::Logger::error("⚠️ Rebalance error: " + std::string(rd_kafka_err2str(err)));
        rd_kafka_assign(rk, nullptr);
    }
}*/
/*
std::string KafkaConsumer::computeMessageHash(const std::string& message) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(message.c_str()), message.size(), hash);
    std::ostringstream oss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }
    return oss.str();
}

bool KafkaConsumer::isMessageProcessed(const std::string& message) {
    std::lock_guard<std::mutex> lock(processedMutex);
    std::string hash = computeMessageHash(message);
    if (processedMessagesCache.count(hash) > 0) {
        return true;
    }
    std::ifstream inFile(processedMessagesFilePath);
    if (inFile.is_open()) {
        std::string line;
        while (std::getline(inFile, line)) {
            if (line == hash) {
                inFile.close();
                return true;
            }
        }
        inFile.close();
    }
    return false;
}

void KafkaConsumer::markMessageProcessed(const std::string& message) {
    std::lock_guard<std::mutex> lock(processedMutex);
    std::string hash = computeMessageHash(message);
    processedMessagesCache.insert(hash);
    std::ofstream outFile(processedMessagesFilePath, std::ios::app);
    if (outFile.is_open()) {
        outFile << hash << "\n";
        outFile.close();
    } else {
        OpenSync::Logger::error("❌ Failed to write to processed messages file: " + processedMessagesFilePath);
    }
}*/
