#include "MetricsServer.h"
#include "MetricsExporter.h"
#include "../logger/Logger.h"
#include <iostream>
#include <sstream>

MetricsServer::MetricsServer(int port) : port(port) {}

MetricsServer::~MetricsServer() {
    stop();
}

void MetricsServer::start() {
    server.Get("/metrics", [&](const httplib::Request& req, httplib::Response& res) {
	(void)req;  // 🆕 Bỏ cảnh báo unused parameter
        std::string metricsData = MetricsExporter::getInstance().exportMetrics();
        res.set_content(metricsData, "text/plain");
    });

    serverThread = std::thread([this]() {
        try {
	          Logger::info("Starting Prometheus metrics, listening on: 0.0.0.0:" + std::to_string(port));
            server.listen("0.0.0.0", port);
        } catch (const std::exception& e) {
	          Logger::info("MetricsServer crashed: " + std::string(e.what()));
        }
    });

}

void MetricsServer::stop() {
    server.stop();
    if (serverThread.joinable()) {
        serverThread.join();
    }
}

// 🆕 this call from KafkaConsumer when receive message
void MetricsServer::incrementProcessedMessages() {
    processedMessages++;
    Logger::info("Metrics updated: kafka_messages_processed = " + std::to_string(processedMessages.load()));
}
