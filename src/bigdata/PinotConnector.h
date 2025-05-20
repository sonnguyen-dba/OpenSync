#ifndef PINOT_CONNECTOR_H
#define PINOT_CONNECTOR_H

#include "BigDataConnector.h"
#include <string>

class PinotConnector : public BigDataConnector {
public:
    explicit PinotConnector(const std::string& brokerUrl);
    ~PinotConnector();

    bool connect() override;
    void disconnect() override;
    bool isConnected() override;
    bool writeData(const std::vector<std::string>& jsonData) override;

private:
    std::string brokerUrl;
    bool connected;
};

#endif

