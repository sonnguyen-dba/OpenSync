#ifndef ICEBERG_CONNECTOR_H
#define ICEBERG_CONNECTOR_H

#include "BigDataConnector.h"
#include <string>

class IcebergConnector : public BigDataConnector {
public:
    explicit IcebergConnector(const std::string& warehousePath);
    ~IcebergConnector();

    bool connect() override;
    void disconnect() override;
    bool isConnected() override;
    bool writeData(const std::vector<std::string>& jsonData) override;

private:
    std::string warehousePath;
    bool connected;
};

#endif

