#ifndef KAFKA_CONSUMER_THREAD_H
#define KAFKA_CONSUMER_THREAD_H

#include "../kafka/KafkaConsumer.h"

void kafkaConsumerThread(KafkaConsumer& consumer, std::atomic<bool>& shutdown);
void startKafkaConsumer(KafkaConsumer& consumer, std::atomic<bool>& shutdown);

#endif
