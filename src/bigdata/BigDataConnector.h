#ifndef BIGDATA_CONNECTOR_H
#define BIGDATA_CONNECTOR_H

#include <string>
#include <vector>

class BigDataConnector {
public:
    virtual ~BigDataConnector() = default;

    // Kết nối đến hệ thống Big Data
    virtual bool connect() = 0;

    // Ngắt kết nối
    virtual void disconnect() = 0;

    // Kiểm tra kết nối
    virtual bool isConnected() = 0;

    // Gửi dữ liệu (JSON format)
    virtual bool writeData(const std::vector<std::string>& jsonData) = 0;
};

#endif

