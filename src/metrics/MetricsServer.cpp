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
	    //LOG_INFO("Starting Prometheus metrics, listening on: 0.0.0.0:" + std::to_string(port));
	    OpenSync::Logger::info("Starting Prometheus metrics, listening on: 0.0.0.0:" + std::to_string(port));
            server.listen("0.0.0.0", port);
        } catch (const std::exception& e) {
            //LOG_ERROR("MetricsServer crashed: " + std::string(e.what()));
	    OpenSync::Logger::info("MetricsServer crashed: " + std::string(e.what()));
        }
    });

}

void MetricsServer::stop() {
    server.stop();
    if (serverThread.joinable()) {
        serverThread.join();
    }
}

// 🆕 Hàm này sẽ được gọi từ KafkaConsumer khi nhận được message
void MetricsServer::incrementProcessedMessages() {
    processedMessages++;
    //LOG_INFO("Metrics updated: kafka_messages_processed = " + std::to_string(processedMessages.load()));
    OpenSync::Logger::info("Metrics updated: kafka_messages_processed = " + std::to_string(processedMessages.load()));
}
