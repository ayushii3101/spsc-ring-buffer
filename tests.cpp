// tests.cpp
#include "ring_buffer_st.hpp"
#include "ring_buffer_mt.hpp"
#include "spsc_ring_buffer.hpp"

#include <cassert>
#include <cstdio>
#include <thread>

// Single-threaded invariants for the ST buffer.
static void test_st_basic() {
    RingBufferST<MarketTick, 8> q;          // holds 7
    assert(q.empty() && !q.full());
    for (std::uint32_t i = 0; i < 7; ++i)
        assert(q.try_push(MarketTick{i, 100, 99.5, 10, 'B'}));
    assert(q.full());
    assert(!q.try_push(MarketTick{}));       // rejected when full
    MarketTick t;
    for (std::uint32_t i = 0; i < 7; ++i) {
        assert(q.try_pop(t) && t.timestamp_ns == i);   // FIFO order
    }
    assert(q.empty() && !q.try_pop(t));
    std::puts("test_st_basic: OK");
}

// Two-thread checksum: if every item crosses exactly once, in order,
// the sum of timestamps must equal 0+1+...+(N-1).
template <typename Queue>
static void test_mt_checksum(const char* name) {
    Queue q;
    constexpr std::uint64_t N = 2'000'000;
    const std::uint64_t expected = N * (N - 1) / 2;

    std::thread producer([&] {
        for (std::uint64_t i = 0; i < N; ) {
            if (q.try_push(MarketTick{i, 100, 99.5, 10, 'B'})) ++i;
        }
    });
    std::uint64_t checksum = 0;
    std::thread consumer([&] {
        MarketTick t;
        for (std::uint64_t i = 0; i < N; ) {
            if (q.try_pop(t)) { checksum += t.timestamp_ns; ++i; }
        }
    });
    producer.join();
    consumer.join();

    assert(checksum == expected);
    std::printf("test_mt_checksum[%s]: OK (checksum=%llu)\n",
                name, (unsigned long long)checksum);
}

int main() {
    test_st_basic();
    test_mt_checksum<RingBufferMT<MarketTick, 1024>>("mutex");
    test_mt_checksum<SPSCRingBuffer<MarketTick, 1024>>("lockfree");
    std::puts("all tests passed");
}
