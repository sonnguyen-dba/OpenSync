#ifndef WORKER_THREAD_H
#define WORKER_THREAD_H

#include "../../kafka/KafkaProcessor.h"

extern size_t  batchSize;
//void workerThread(KafkaProcessor& processor, int batchFlushIntervalMs);
void workerThread(KafkaProcessor& processor, int batchFlushIntervalMs, std::atomic<bool>& shutdown);

void startWorker(KafkaProcessor& processor, int batchFlushIntervalMs);

#endif
