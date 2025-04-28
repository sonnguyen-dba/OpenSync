#ifndef TABLE_BATCH_H
#define TABLE_BATCH_H

#include <string>
#include <vector>
#include <librdkafka/rdkafka.h>

struct TableBatch {
    //std::string tableKey;
    std::vector<std::string> sqls;
    std::vector<rd_kafka_message_t*> messages;
};

#endif // TABLE_BATCH_H

