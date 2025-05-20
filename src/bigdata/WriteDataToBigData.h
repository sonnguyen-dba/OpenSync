#ifndef WRITE_DATA_TO_BIGDATA_H
#define WRITE_DATA_TO_BIGDATA_H

//#include "BigDataConnector.h"
//#include "PinotConnector.h"
//#include "IcebergConnector.h"
//#include "KuduConnector.h"
#include <memory>
#include <unordered_map>

class WriteDataToBigData {
public:
    WriteDataToBigData();
    ~WriteDataToBigData();

    // Thêm Big Data Connector (Pinot, Iceberg, Kudu)
    void addBigDataConnector(const std::string& bigDataType, std::unique_ptr<BigDataConnector> connector);

    // Gửi dữ liệu đến hệ thống Big Data
    bool writeToBigData(const std::string& bigDataType, const std::vector<std::string>& jsonData);

private:
    std::unordered_map<std::string, std::unique_ptr<BigDataConnector>> bigDataConnectors;
};

#endif

