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
/*CheckpointManager::CheckpointManager(const std::string& path)
    : checkpointFilePath(path) {
    fs::create_directories(fs::path(path).parent_path());  // Tạo thư mục nếu chưa có
    loadCheckpointFromFile();
}*/

CheckpointManager::CheckpointManager(const std::string& path)
    : checkpointFilePath(path), pendingFlushes(0) {
    fs::create_directories(fs::path(path).parent_path());
    loadCheckpointFromFile();
}

// Optional: constructor mặc định (không dùng)
CheckpointManager::CheckpointManager() {}

/*void CheckpointManager::loadCheckpointFromFile() {
    std::lock_guard<std::mutex> lock(mutex);

    std::ifstream inFile(checkpointFilePath);
    if (!inFile.is_open()) {
	OpenSync::Logger::error("❌ [CheckpointManager] No checkpoint file found at " + checkpointFilePath + ". Starting fresh.");
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
    OpenSync::Logger::info("✅ Checkpoint loaded " + std::to_string(checkpointMap.size()) + " offsets from checkpoint file.");
}*/

void CheckpointManager::loadCheckpointFromFile() {
    std::lock_guard<std::mutex> lock(mutex);
    std::ifstream inFile(checkpointFilePath);
    if (!inFile.is_open()) {
        OpenSync::Logger::error("❌ [CheckpointManager] No checkpoint file found at " + checkpointFilePath +
                               ". Starting fresh.");
        return;
    }

    std::string line;
    size_t lineNum = 0;
    while (std::getline(inFile, line)) {
        ++lineNum;
        std::istringstream iss(line);
        std::string topicPartition;
        int64_t offset;
        if (!(iss >> topicPartition >> offset)) {
            OpenSync::Logger::warn("⚠️ Invalid checkpoint line " + std::to_string(lineNum) +
                                  " in " + checkpointFilePath + ": " + line);
            continue;
        }

        size_t colonPos = topicPartition.find(':');
        if (colonPos == std::string::npos || colonPos == 0 || colonPos == topicPartition.size() - 1) {
            OpenSync::Logger::warn("⚠️ Invalid topic:partition format in line " + std::to_string(lineNum) +
                                  ": " + topicPartition);
            continue;
        }

        std::string topic = topicPartition.substr(0, colonPos);
        std::string partitionStr = topicPartition.substr(colonPos + 1);
        int partition;
        try {
            partition = std::stoi(partitionStr);
        } catch (const std::exception& e) {
            OpenSync::Logger::warn("⚠️ Invalid partition in line " + std::to_string(lineNum) +
                                  ": " + partitionStr);
            continue;
        }

        checkpointMap[topicPartition] = offset;
        OpenSync::Logger::info("✅ Loaded checkpoint: topic=" + topic +
                              ", partition=" + std::to_string(partition) +
                              ", offset=" + std::to_string(offset));
    }
    inFile.close();
    OpenSync::Logger::info("✅ Checkpoint loaded " + std::to_string(checkpointMap.size()) +
                          " offsets from checkpoint file.");
    //MetricsExporter::getInstance().setGauge("checkpoint_entries", static_cast<double>(checkpointMap.size()), {});
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
/*
void CheckpointManager::flushToDisk() {
    std::lock_guard<std::mutex> lock(mutex);

    std::ofstream outFile(checkpointFilePath, std::ios::trunc);
    if (!outFile.is_open()) {
	OpenSync::Logger::error("❌ Checkpoint failed to open file for writing: " + checkpointFilePath );
        return;
    }

    for (const auto& [key, offset] : checkpointMap) {
        outFile << key << " " << offset << "\n";
    }

    OpenSync::Logger::info("✅ Checkpoint flushed checkpoint to disk. Total entries: " + std::to_string(checkpointMap.size()));
}*/


/*void CheckpointManager::flushToDisk() {
    std::lock_guard<std::mutex> lock(mutex);
    std::string tempFile = checkpointFilePath + ".tmp";
    std::ofstream outFile(tempFile, std::ios::trunc);
    if (!outFile.is_open()) {
        OpenSync::Logger::error("❌ Checkpoint failed to open temp file for writing: " + tempFile);
        return;
    }

    for (const auto& [key, offset] : checkpointMap) {
        outFile << key << " " << offset << "\n";
    }

    outFile.flush();
    outFile.close();
    try {
        fs::rename(tempFile, checkpointFilePath);
        OpenSync::Logger::info("✅ Checkpoint flushed to disk. Total entries: " +
                              std::to_string(checkpointMap.size()));
        //MetricsExporter::getInstance().incrementCounter("checkpoint_flush_count");
        //MetricsExporter::getInstance().setGauge("checkpoint_entries", static_cast<double>(checkpointMap.size()), {});
    } catch (const fs::filesystem_error& e) {
        OpenSync::Logger::error("❌ Failed to rename temp file to " + checkpointFilePath + ": " + e.what());
    }
    pendingFlushes = 0;
}*/

void CheckpointManager::flushToDisk() {
    std::lock_guard<std::mutex> lock(mutex);
    std::string tempFile = checkpointFilePath + ".tmp";
    std::ofstream outFile(tempFile, std::ios::trunc);
    if (!outFile.is_open()) {
        OpenSync::Logger::error("❌ Checkpoint failed to open temp file for writing: " + tempFile);
        return;
    }

    for (const auto& [key, offset] : checkpointMap) {
        outFile << key << " " << offset << "\n";
        size_t colonPos = key.find(':');
        std::string topic = key.substr(0, colonPos);
        std::string partition = key.substr(colonPos + 1);
        OpenSync::Logger::debug("✅ Flushed checkpoint: topic=" + topic +
                              ", partition=" + partition +
                              ", offset=" + std::to_string(offset));
    }

    outFile.flush();
    outFile.close();
    try {
        fs::rename(tempFile, checkpointFilePath);
        OpenSync::Logger::debug("✅ Checkpoint flushed to disk. Total entries: " +
                              std::to_string(checkpointMap.size()));
        //MetricsExporter::getInstance().incrementCounter("checkpoint_flush_count");
        //MetricsExporter::getInstance().setGauge("checkpoint_entries", static_cast<double>(checkpointMap.size()), {});
    } catch (const fs::filesystem_error& e) {
        OpenSync::Logger::error("❌ Failed to rename temp file to " + checkpointFilePath + ": " + e.what());
    }
    pendingFlushes = 0;
}

void CheckpointManager::startAutoFlush(int intervalSeconds) {
    stopFlag = false;

    flushThread = std::thread([this, intervalSeconds]() {
        while (!stopFlag) {
            std::this_thread::sleep_for(std::chrono::seconds(intervalSeconds));
            this->flushToDisk();
        }
    });

    OpenSync::Logger::info("⏳ Checkpoint auto flush thread started (interval: " + std::to_string(intervalSeconds) + "s)");
}

void CheckpointManager::stopAutoFlush() {
    stopFlag = true;

    if (flushThread.joinable()) {
        flushThread.join();
	OpenSync::Logger::info("✅ Checkpoint auto flush thread stopped.");
    }
}

void CheckpointManager::flushToDiskImmediate(const std::string& topic, int partition, int64_t offset) {
    std::lock_guard<std::mutex> lock(mutex);
    std::string key = topic + ":" + std::to_string(partition);
    if (checkpointMap[key] == offset) {
        OpenSync::Logger::debug("ℹ️ Checkpoint unchanged for topic=" + topic +
                               ", partition=" + std::to_string(partition) +
                               ", offset=" + std::to_string(offset) + ". Skipping flush.");
        return;
    }
    checkpointMap[key] = offset;
    pendingFlushes++;
    if (pendingFlushes >= 100) {
        flushToDisk();
    }
}
