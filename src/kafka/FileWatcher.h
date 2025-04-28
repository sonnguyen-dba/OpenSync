#pragma once
#include <string>
#include <functional>
#include <atomic>

class FileWatcher {
public:
    static void watchFile(
        const std::string& path,
        const std::function<void()>& onChangeCallback,
        const std::atomic<bool>& stopFlag);
};

