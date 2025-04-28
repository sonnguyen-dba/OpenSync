#pragma once

#include <tuple>
#include <string>
#include <librdkafka/rdkafka.h>
#include "TableBatch.h"
#include "../thread/ThreadSafeQueue.h"

// Queue chứa message thô từ Kafka
extern ThreadSafeQueue<std::tuple<
    std::string,      // message (json string)
    int,              // partition
    int64_t,          // offset
    int64_t,          // timestamp
    rd_kafka_message_t*>> kafkaMessageQueue;

// Queue chứa batch SQL theo table (TableBatch)
extern ThreadSafeQueue<std::tuple<
    std::string,      // tableKey (owner.table)
    TableBatch>> dbWriteQueue;
