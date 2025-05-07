#pragma once
#include <queue>
#include <mutex>
#include <condition_variable>

template<typename T>
class ThreadSafeQueue {
public:
    ThreadSafeQueue(size_t maxCapacity = 1000) : maxCapacity(maxCapacity) {}

    void push(const T& value) {
        std::unique_lock<std::mutex> lock(mtx);
        condVarFull.wait(lock, [this] { return queue.size() < maxCapacity; });
        queue.push(value);
        condVarEmpty.notify_one();
    }

    void push(T&& value) {
        std::unique_lock<std::mutex> lock(mtx);
        condVarFull.wait(lock, [this] { return queue.size() < maxCapacity; });
        queue.push(std::move(value));
        condVarEmpty.notify_one();
    }

    bool try_pop(T& result, std::chrono::milliseconds timeout = std::chrono::milliseconds(100)) {
        std::unique_lock<std::mutex> lock(mtx);
        if (!condVarEmpty.wait_for(lock, timeout, [this] { return !queue.empty(); }))
            return false;

        result = std::move(queue.front());
        queue.pop();
        condVarFull.notify_one();
        return true;
    }

    bool try_pop_nowait(T& result) {
        std::lock_guard<std::mutex> lock(mtx);
        if (queue.empty()) return false;
        result = std::move(queue.front());
        queue.pop();
        condVarFull.notify_one();
        return true;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mtx);
        return queue.size();
    }

private:
    mutable std::mutex mtx;
    std::condition_variable condVarEmpty;
    std::condition_variable condVarFull;
    std::queue<T> queue;
    size_t maxCapacity;
};

