#pragma once

#include <atomic>

class WriteDataToDB;  // Forward declaration, không cần include nặng

void startMemoryMonitorThread(std::atomic<bool>& stopFlag);
void startMetricsMonitorThread(std::atomic<bool>& stopFlag);
void startConnectorMetricsThread(std::atomic<bool>& stopFlag, WriteDataToDB& writeData);
void startTableBufferMetricsThread(std::atomic<bool>& stopFlag, WriteDataToDB& writeData);
void startTableBufferCleanupThread(std::atomic<bool>& stopFlag, WriteDataToDB& writeData);

