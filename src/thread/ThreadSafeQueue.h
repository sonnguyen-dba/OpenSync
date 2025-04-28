#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>
#include "../logger/Logger.h"

template<typename T>
class ThreadSafeQueue {
public:
    explicit ThreadSafeQueue(size_t max_size = 0) : max_size_(max_size) {}

    void push(const T& item) {
        std::unique_lock<std::mutex> lock(mtx_);
        not_full_cv_.wait(lock, [this] {
            return max_size_ == 0 || queue_.size() < max_size_;
        });
        queue_.push(item);
	if (max_size_ > 0 && queue_.size() > max_size_ * 0.9) {
           //std::stringstream ss;
            //ss << "⚠️ Queue is almost full: {}/{}" << queue_.size() << "/ " << max_size_ << ";
            //logger::warn(ss.str());
           //logger::warn("⚠️ Queue is almost full: {}/{}");
	   Logger::warn("⚠️ Queue is almost full: {}/{}");
        }
        not_empty_cv_.notify_one();
    }

    void push(T&& item) {
        std::unique_lock<std::mutex> lock(mtx_);
        not_full_cv_.wait(lock, [this] {
            return max_size_ == 0 || queue_.size() < max_size_;
        });
        queue_.push(std::move(item));
        not_empty_cv_.notify_one();
    }

    T pop() {
        std::unique_lock<std::mutex> lock(mtx_);
        not_empty_cv_.wait(lock, [this] { return !queue_.empty(); });

        T item = std::move(queue_.front());
        queue_.pop();
        not_full_cv_.notify_one();
        return item;
    }

    bool try_pop(T& result) {
        std::lock_guard<std::mutex> lock(mtx_);
        if (queue_.empty()) return false;
        result = std::move(queue_.front());
        queue_.pop();
        not_full_cv_.notify_one();
        return true;
    }

    bool try_pop(T& result, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mtx_);
        if (queue_.empty()) {
            if (!not_empty_cv_.wait_for(lock, timeout, [this] { return !queue_.empty(); })) {
                return false;
            }
        }
        result = std::move(queue_.front());
        queue_.pop();
        not_full_cv_.notify_one();
        return true;
    }

    void wait_and_pop(T& value) {
        std::unique_lock<std::mutex> lock(mtx_);
        not_empty_cv_.wait(lock, [this] { return !queue_.empty(); });

        value = std::move(queue_.front());
        queue_.pop();
        not_full_cv_.notify_one();
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return queue_.empty();
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return queue_.size();
    }

private:
    mutable std::mutex mtx_;
    std::condition_variable not_empty_cv_;
    std::condition_variable not_full_cv_;
    std::queue<T> queue_;
    size_t max_size_;  // 0 = unbounded
};
