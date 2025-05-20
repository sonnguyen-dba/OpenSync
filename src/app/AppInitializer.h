#pragma once

#include <memory>
#include "../reader/ConfigLoader.h"
#include "../kafka/KafkaProcessor.h"
#include "../kafka/KafkaConsumer.h"
#include "../writer/WriteDataToDB.h"
#include "../metrics/MetricsServer.h"

struct AppComponents {
    std::unique_ptr<ConfigLoader> config;
    std::unique_ptr<KafkaProcessor> processor;
    std::unique_ptr<KafkaConsumer> consumer;
    std::unique_ptr<WriteDataToDB> writeData;
    std::unique_ptr<MetricsServer> metrics;
};

class AppInitializer {
public:
    static std::unique_ptr<AppComponents> initialize(const std::string& configPath);
};

