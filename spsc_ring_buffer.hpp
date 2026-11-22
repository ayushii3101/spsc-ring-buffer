#pragma once
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>          // std::hardware_destructive_interference_size

#include "ring_buffer_st.hpp"   // reuse MarketTick

// Cache-line size. The standard constant isn't available on every compiler,
// so fall back to the near-universal 64 bytes.
#ifdef __cpp_lib_hardware_interference_size
    inline constexpr std::size_t kCacheLine =
        std::hardware_destructive_interference_size;
#else
    inline constexpr std::size_t kCacheLine = 64;
#endif

template <typename T, std::size_t Capacity>
class SPSCRingBuffer {
    static_assert(Capacity >= 2, "capacity must be >= 2");
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "capacity must be a power of two");

public:
    SPSCRingBuffer() = default;
    SPSCRingBuffer(const SPSCRingBuffer&)            = delete;
    SPSCRingBuffer& operator=(const SPSCRingBuffer&) = delete;

    // ---- Producer thread only ----------------------------------------
    bool try_push(const T& item) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t next = (tail + 1) & kMask;

        // Is the buffer full? Compare against the consumer's head.
        // acquire: see the consumer's latest head before we trust "not full".
        if (next == head_.load(std::memory_order_acquire)) {
            return false;
        }

        buffer_[tail] = item;                          // write payload
        tail_.store(next, std::memory_order_release);  // publish the write
        return true;
    }

    // ---- Consumer thread only ----------------------------------------
    bool try_pop(T& out) {
        const std::size_t head = head_.load(std::memory_order_relaxed);

        // Is the buffer empty? Compare against the producer's tail.
        // acquire: see the producer's payload write before we read the slot.
        if (head == tail_.load(std::memory_order_acquire)) {
            return false;
        }

        out = buffer_[head];                           // read payload
        head_.store((head + 1) & kMask,
                    std::memory_order_release);        // publish the read
        return true;
    }

    // Approximate, safe to call from either thread (observers only).
    bool empty() const {
        return head_.load(std::memory_order_acquire) ==
               tail_.load(std::memory_order_acquire);
    }

    static constexpr std::size_t capacity() { return Capacity - 1; }

private:
    static constexpr std::size_t kMask = Capacity - 1;

    // --- Layout: keep producer state and consumer state on separate
    //     cache lines so the two threads never fight over the same line. ---
    alignas(kCacheLine) std::atomic<std::size_t> head_{0};  // written by consumer
    alignas(kCacheLine) std::atomic<std::size_t> tail_{0};  // written by producer
    alignas(kCacheLine) std::array<T, Capacity>  buffer_{};
};
