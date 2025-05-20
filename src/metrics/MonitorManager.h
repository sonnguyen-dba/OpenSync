#pragma once

#include "../kafka/KafkaConsumer.h"
#include "../writer/WriteDataToDB.h"
#include "../reader/ConfigLoader.h"

class MonitorManager {
public:
    static void startMonitors(KafkaConsumer& consumer, WriteDataToDB& writeData, ConfigLoader& config);
};

