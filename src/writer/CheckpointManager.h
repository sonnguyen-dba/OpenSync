#pragma once

#include <string>
#include <map>              // ✅ Quan trọng: thêm dòng này
#include <unordered_map>
#include <mutex>
#include <fstream>
#include <filesystem>
#include <thread>
#include <atomic>


class CheckpointManager {
public:
    static CheckpointManager& getInstance(const std::string& path = "checkpoint/checkpoints_test.txt");

    CheckpointManager(const std::string& path);  // vẫn cần nếu bạn gọi trực tiếp

    void saveCheckpoint(const std::string& topic, int partition, int64_t offset);
    int64_t loadCheckpoint(const std::string& topic, int partition);

    void loadCheckpointFromFile();
    void updateCheckpoint(const std::string& topic, int partition, int64_t offset);
    int64_t getLastCheckpoint(const std::string& topic, int partition);

    void flushToDisk(); // make public nếu dùng ngoài
    void flushToDiskImmediate(const std::string& topic, int partition, int64_t offset);

    void startAutoFlush(int intervalSeconds = 120); // ✅ Hàm khởi chạy auto-flush
    void stopAutoFlush();                         // ✅ Hàm dừng thread

    /*static CheckpointManager& getInstance(const std::string& path);
    void updateCheckpoint(const std::string& topic, int partition, int64_t offset);
    int64_t getLastCheckpoint(const std::string& topic, int partition);
    void flushToDisk();
    void flushToDiskImmediate(const std::string& topic, int partition, int64_t offset);
    void startAutoFlush(int intervalSeconds);
    void stopAutoFlush();*/

private:
    CheckpointManager(); // cho singleton mặc định
    //CheckpointManager(const std::string& path);
    //void loadCheckpointFromFile();
    std::string getFilePath(const std::string& topic, int partition);
    std::string checkpointFilePath;

    std::map<std::string, int64_t> checkpointMap;       // ✅ Đã khai báo
    std::mutex mutex;
    std::unordered_map<std::string, int64_t> offsetCache;

    const std::string checkpointDir = "checkpoint";

    std::thread flushThread;
    std::atomic<bool> stopFlag{false}; // ✅ điều kiện dừng thread
/*
    CheckpointManager(const std::string& path);
    void loadCheckpointFromFile();

    std::string checkpointFilePath;
    std::map<std::string, int64_t> checkpointMap;
    std::mutex mutex;
    std::thread flushThread;
    std::atomic<bool> stopFlag{false};*/
    int pendingFlushes;
};
