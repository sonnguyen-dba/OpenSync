#pragma once

#include <vector>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <functional>
#include <atomic>
#include <unordered_map>

class ThreadPoolDBWriter {
public:
    using Task = std::function<void()>;

    explicit ThreadPoolDBWriter(size_t numThreads);
    ~ThreadPoolDBWriter();

    void enqueueTask(const std::string& table, Task task);

private:
    void workerThread();

    std::vector<std::thread> workers;
    std::queue<std::pair<std::string, Task>> tasks;
    std::mutex queueMutex;
    std::condition_variable cv;
    std::atomic<bool> stop;
    std::unordered_map<std::string, std::mutex> tableMutexes;
};

