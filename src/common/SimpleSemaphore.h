// SimpleSemaphore.h
#pragma once
#include <mutex>
#include <condition_variable>

class SimpleSemaphore {
public:
    explicit SimpleSemaphore(int max) : count(max) {}

    void acquire() {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [&]() { return count > 0; });
        --count;
    }

    void release() {
        std::unique_lock<std::mutex> lock(mtx);
        ++count;
        cv.notify_one();
    }

private:
    int count;
    std::mutex mtx;
    std::condition_variable cv;
};

