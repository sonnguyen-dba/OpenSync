#pragma once

#include "../kafka/KafkaConsumer.h"
#include "../WriteDataToDB/WriteDataToDB.h"
#include "../config/ConfigLoader.h"

class MonitorManager {
public:
    static void startMonitors(KafkaConsumer& consumer, WriteDataToDB& writeData, ConfigLoader& config);
};

