#include "ThreadPoolDBWriter.h"
#include <chrono>
#include <malloc.h>

ThreadPoolDBWriter::ThreadPoolDBWriter(size_t numThreads) : stop(false) {
    for (size_t i = 0; i < numThreads; ++i) {
        workers.emplace_back(&ThreadPoolDBWriter::workerThread, this);
    }
}

ThreadPoolDBWriter::~ThreadPoolDBWriter() {
    stop = true;
    cv.notify_all();
    for (auto& t : workers) {
        if (t.joinable()) t.join();
    }
}

void ThreadPoolDBWriter::enqueueTask(const std::string& table, Task task) {
    {
        std::lock_guard<std::mutex> lock(queueMutex);
        tasks.emplace(table, std::move(task));
    }
    cv.notify_one();
}

void ThreadPoolDBWriter::workerThread() {
    auto idleSince = std::chrono::steady_clock::now();

    while (!stop) {
        std::pair<std::string, Task> taskItem;
        bool hasTask = false;

        {
            std::unique_lock<std::mutex> lock(queueMutex);
            if (!cv.wait_for(lock, std::chrono::seconds(10), [this] { return stop || !tasks.empty(); })) {
                // Timeout sau 10s: idle check
                auto now = std::chrono::steady_clock::now();
                if (std::chrono::duration_cast<std::chrono::seconds>(now - idleSince).count() > 60) {
                    // Nếu idle > 60s, dọn nhẹ bộ nhớ
                    malloc_trim(0);
                    idleSince = now;
                }
                continue;
            }

            if (stop && tasks.empty()) return;

            if (!tasks.empty()) {
                taskItem = std::move(tasks.front());
                tasks.pop();
                hasTask = true;
            }
        }

        if (hasTask) {
            const std::string& table = taskItem.first;
            Task& task = taskItem.second;

            std::mutex& tableLock = tableMutexes[table];
            {
                std::lock_guard<std::mutex> lock(tableLock);
                try {
                    task();
                } catch (const std::exception& ex) {
                    // LOG_ERROR("DBWriter task exception: " + std::string(ex.what()));
                } catch (...) {
                    // LOG_ERROR("DBWriter task unknown exception");
                }
            }

            idleSince = std::chrono::steady_clock::now(); // reset idle timer
        }
    }
}