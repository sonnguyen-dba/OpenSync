#pragma once

#include "string"
#include "KafkaConsumer.h"
#include "../writer/WriteDataToDB.h"
#include "TableBatch.h"
#include "ThreadSafeQueue.h"

extern ThreadSafeQueue<std::tuple<std::string, TableBatch>> dbWriteQueue;

//void dbWriterThread(WriteDataToDB& writeData, KafkaConsumer& consumer, const std::string& dbType);
void dbWriterThread(WriteDataToDB& writeData, KafkaConsumer& consumer, const std::string& dbType, std::atomic<bool>& shutdown);