#include "KafkaConsumerThread.h"
#include "../common/Queues.h"
#include "../logger/Logger.h"

void kafkaConsumerThread(KafkaConsumer& consumer, std::atomic<bool>& shouldShutdown) {
    static int64_t message_count = 0;
    static auto start_time = std::chrono::steady_clock::now();

    while (!shouldShutdown) {
        std::string message;
        int partition;
        int64_t offset;
        int64_t timestamp;
        rd_kafka_message_t* rawMsg = nullptr;

        if (consumer.consumeMessage(message, partition, offset, timestamp, &rawMsg)) {
            message_count++;
            kafkaMessageQueue.push({message, partition, offset, timestamp, rawMsg});
            std::stringstream ss;
            ss << "Pushed message to kafkaMessageQueue (offset: " << offset << ")";
            Logger::debug(ss.str());

            // Log messages/s mỗi 10 giây
            auto current_time = std::chrono::steady_clock::now();
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_time).count();
            if (elapsed_ms >= 10'000) {
                double messages_per_sec = static_cast<double>(message_count) / (elapsed_ms / 1000.0);
                Logger::info("Consumer messages/s: " + std::to_string(messages_per_sec));
                message_count = 0;
                start_time = current_time;
            }
        } // Loại bỏ sleep 1000ms để tăng tần suất poll()
    }

    Logger::info("Shutting down Kafka consumer thread...");
}

void startKafkaConsumer(KafkaConsumer& consumer, std::atomic<bool>& shouldShutdown) {
    std::thread(kafkaConsumerThread, std::ref(consumer), std::ref(shouldShutdown)).detach();
}
