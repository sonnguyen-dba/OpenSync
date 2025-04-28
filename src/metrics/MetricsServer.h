#ifndef METRICS_SERVER_H
#define METRICS_SERVER_H

#include "../external/httplib.h"
#include <thread>

class MetricsServer {
public:
    MetricsServer(int port = 8087);
    ~MetricsServer();

    void start();
    void stop();

    void incrementProcessedMessages();  // 🆕 Thêm hàm tăng số lượng messages

private:
    int port;
    std::thread serverThread;
    httplib::Server server;

    std::atomic<int> processedMessages;  // 🆕 Biến đếm message (thread-safe)`
};

#endif

