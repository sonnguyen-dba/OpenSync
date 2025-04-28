#include "CheckpointManager.h"
#include "../logger/Logger.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <filesystem>

namespace fs = std::filesystem;

// Singleton instance getter
CheckpointManager& CheckpointManager::getInstance(const std::string& path) {
    static CheckpointManager instance(path);
    return instance;
}

// Constructor có path (private trong header)
CheckpointManager::CheckpointManager(const std::string& path)
    : checkpointFilePath(path) {
    fs::create_directories(fs::path(path).parent_path());  // Tạo thư mục nếu chưa có
    loadCheckpointFromFile();
}

// Optional: constructor mặc định (không dùng)
CheckpointManager::CheckpointManager() {}

void CheckpointManager::loadCheckpointFromFile() {
    std::lock_guard<std::mutex> lock(mutex);

    std::ifstream inFile(checkpointFilePath);
    if (!inFile.is_open()) {
	Logger::error("❌ [CheckpointManager] No checkpoint file found at " + checkpointFilePath + ". Starting fresh.");
        return;
    }

    std::string line;
    while (std::getline(inFile, line)) {
        std::istringstream iss(line);
        std::string topicPartition;
        int64_t offset;
        if (iss >> topicPartition >> offset) {
            checkpointMap[topicPartition] = offset;
        }
    }
    Logger::info("✅ Checkpoint loaded " + std::to_string(checkpointMap.size()) + " offsets from checkpoint file.");
}

void CheckpointManager::updateCheckpoint(const std::string& topic, int partition, int64_t offset) {
    std::lock_guard<std::mutex> lock(mutex);
    std::string key = topic + ":" + std::to_string(partition);
    checkpointMap[key] = offset;
}

int64_t CheckpointManager::getLastCheckpoint(const std::string& topic, int partition) {
    std::lock_guard<std::mutex> lock(mutex);
    std::string key = topic + ":" + std::to_string(partition);
    auto it = checkpointMap.find(key);
    return (it != checkpointMap.end()) ? it->second : -1;
}

void CheckpointManager::flushToDisk() {
    std::lock_guard<std::mutex> lock(mutex);

    std::ofstream outFile(checkpointFilePath, std::ios::trunc);
    if (!outFile.is_open()) {
	Logger::error("❌ Checkpoint failed to open file for writing: " + checkpointFilePath );
        return;
    }

    for (const auto& [key, offset] : checkpointMap) {
        outFile << key << " " << offset << "\n";
    }

    Logger::info("✅ Checkpoint flushed checkpoint to disk. Total entries: " + std::to_string(checkpointMap.size()));
}

void CheckpointManager::startAutoFlush(int intervalSeconds) {
    stopFlag = false;

    flushThread = std::thread([this, intervalSeconds]() {
        while (!stopFlag) {
            std::this_thread::sleep_for(std::chrono::seconds(intervalSeconds));
            this->flushToDisk();
        }
    });

    Logger::info("⏳ Checkpoint auto flush thread started (interval: " + std::to_string(intervalSeconds) + "s)");
}

void CheckpointManager::stopAutoFlush() {
    stopFlag = true;

    if (flushThread.joinable()) {
        flushThread.join();
	Logger::info("✅ Checkpoint auto flush thread stopped.");
    }
}

