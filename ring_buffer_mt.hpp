#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <condition_variable>

#include "ring_buffer_st.hpp"   // reuse MarketTick

template <typename T, std::size_t Capacity>
class RingBufferMT {
    static_assert(Capacity >= 2, "capacity must be >= 2");
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "capacity must be a power of two");

public:
    // ---- Non-blocking variants (return false instead of waiting) ------
    bool try_push(const T& item) {
        std::lock_guard<std::mutex> lock(mutex_);
        const std::size_t next = (tail_ + 1) & kMask;
        if (next == head_) {              // full
            return false;
        }
        buffer_[tail_] = item;
        tail_ = next;
        not_empty_.notify_one();          // wake a waiting consumer, if any
        return true;
    }

    bool try_pop(T& out) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (head_ == tail_) {             // empty
            return false;
        }
        out = buffer_[head_];
        head_ = (head_ + 1) & kMask;
        not_full_.notify_one();           // wake a waiting producer, if any
        return true;
    }

    // ---- Blocking variants (wait until space / data is available) -----
    void push(const T& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_full_.wait(lock, [this] { return !is_full_unlocked(); });
        buffer_[tail_] = item;
        tail_ = (tail_ + 1) & kMask;
        not_empty_.notify_one();
    }

    void pop(T& out) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [this] { return head_ != tail_; });
        out = buffer_[head_];
        head_ = (head_ + 1) & kMask;
        not_full_.notify_one();
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return head_ == tail_;
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return (tail_ - head_) & kMask;
    }

    static constexpr std::size_t capacity() { return Capacity - 1; }

private:
    bool is_full_unlocked() const { return ((tail_ + 1) & kMask) == head_; }

    static constexpr std::size_t kMask = Capacity - 1;

    mutable std::mutex       mutex_;
    std::condition_variable  not_empty_;   // consumer waits on this
    std::condition_variable  not_full_;    // producer waits on this
    std::array<T, Capacity>  buffer_{};
    std::size_t head_ = 0;
    std::size_t tail_ = 0;
};