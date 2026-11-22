#pragma once
#include <array>
#include <cstddef>
#include <cstdint>

// ---- The payload: one market-data tick --------------------------------
// Keep it trivially copyable and small. We copy by value into the buffer,
// so no heap allocation ever happens in the hot path.
struct MarketTick {
    std::uint64_t timestamp_ns;   // event time
    std::uint32_t instrument_id;  // symbol id
    double        price;
    std::uint32_t quantity;
    char          side;           // 'B' or 'S'
};

// ---- Single-threaded ring buffer (ST) ---------------------------------
template <typename T, std::size_t Capacity>
class RingBufferST {
    static_assert(Capacity >= 2, "capacity must be >= 2");
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "capacity must be a power of two");

public:
    // try_push: returns false if the queue is full (never blocks, never throws)
    bool try_push(const T& item) {
        const std::size_t next = (tail_ + 1) & kMask;
        if (next == head_) {        // the slot we'd advance into is the read head -> full
            return false;
        }
        buffer_[tail_] = item;      // write into the current tail slot
        tail_ = next;               // publish: advance tail
        return true;
    }

    // try_pop: returns false if the queue is empty
    bool try_pop(T& out) {
        if (head_ == tail_) {       // nothing to read -> empty
            return false;
        }
        out = buffer_[head_];       // read from the head slot
        head_ = (head_ + 1) & kMask;
        return true;
    }

    bool empty() const { return head_ == tail_; }
    bool full()  const { return ((tail_ + 1) & kMask) == head_; }

    std::size_t size() const { return (tail_ - head_) & kMask; }

    // usable slots: one is always sacrificed to tell full from empty
    static constexpr std::size_t capacity() { return Capacity - 1; }

private:
    static constexpr std::size_t kMask = Capacity - 1;
    std::array<T, Capacity> buffer_{};
    std::size_t head_ = 0;   // read index  (consumer side)
    std::size_t tail_ = 0;   // write index (producer side)
};