#include "Queues.h"

// Định nghĩa cụ thể queues
ThreadSafeQueue<std::tuple<
    std::string, int, int64_t, int64_t, rd_kafka_message_t*>> kafkaMessageQueue(5000);

ThreadSafeQueue<std::tuple<
    std::string, TableBatch>> dbWriteQueue(2500);

