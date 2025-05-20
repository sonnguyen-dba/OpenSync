#ifndef KUDU_CONNECTOR_H
#define KUDU_CONNECTOR_H

#include "BigDataConnector.h"
#include <string>
#include <memory>
#include <kudu/client/client.h>

class KuduConnector : public BigDataConnector {
public:
    explicit KuduConnector(const std::string& masterServer);
    ~KuduConnector();

    bool connect() override;
    void disconnect() override;
    bool isConnected() override;
    bool writeData(const std::vector<std::string>& jsonData) override;

private:
    std::string masterServer;
    bool connected;
    std::shared_ptr<kudu::client::KuduClient> client;
};

#endif

