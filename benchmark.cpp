// benchmark.cpp
#include "ring_buffer_mt.hpp"
#include "spsc_ring_buffer.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64)
  #include <immintrin.h>          // _mm_pause
  #define CPU_PAUSE() _mm_pause()
#else
  #define CPU_PAUSE() std::this_thread::yield()
#endif

using Clock = std::chrono::steady_clock;

// ----------------------------------------------------------------------
// THROUGHPUT: how many items/sec can we move through the queue?
// Producer pushes N items as fast as it can; consumer drains all N.
// We time the whole run and divide.
// ----------------------------------------------------------------------
template <typename Queue>
double throughput_bench(std::size_t N) {
    Queue q;
    std::atomic<bool> start{false};

    std::thread consumer([&] {
        while (!start.load(std::memory_order_acquire)) { }     // barrier
        MarketTick t;
        for (std::size_t i = 0; i < N; ) {
            if (q.try_pop(t)) ++i;
            else CPU_PAUSE();
        }
    });

    std::thread producer([&] {
        while (!start.load(std::memory_order_acquire)) { }     // barrier
        for (std::size_t i = 0; i < N; ) {
            MarketTick tick{static_cast<std::uint64_t>(i), 100, 99.5, 10, 'B'};
            if (q.try_push(tick)) ++i;
            else CPU_PAUSE();
        }
    });

    const auto t0 = Clock::now();
    start.store(true, std::memory_order_release);              // go!
    producer.join();
    consumer.join();
    const auto t1 = Clock::now();

    const double secs = std::chrono::duration<double>(t1 - t0).count();
    return static_cast<double>(N) / secs;                      // items/sec
}

// ----------------------------------------------------------------------
// LATENCY: how long from "producer writes tick" to "consumer reads it"?
// We stamp each tick with the producer's send time, then on the consumer
// measure (now - tick.timestamp_ns). Record every sample, report
// percentiles — the tail (p99, p99.9) is what matters in trading.
// ----------------------------------------------------------------------
template <typename Queue>
void latency_bench(std::size_t N) {
    Queue q;
    std::vector<std::uint64_t> samples;
    samples.reserve(N);
    std::atomic<bool> start{false};

    std::thread consumer([&] {
        while (!start.load(std::memory_order_acquire)) { }
        MarketTick t;
        for (std::size_t i = 0; i < N; ) {
            if (q.try_pop(t)) {
                const auto now = Clock::now().time_since_epoch();
                const auto now_ns = std::chrono::duration_cast<
                    std::chrono::nanoseconds>(now).count();
                samples.push_back(now_ns - t.timestamp_ns);    // one-way latency
                ++i;
            } else {
                CPU_PAUSE();
            }
        }
    });

    std::thread producer([&] {
        while (!start.load(std::memory_order_acquire)) { }
        for (std::size_t i = 0; i < N; ) {
            const auto now = Clock::now().time_since_epoch();
            const auto now_ns = std::chrono::duration_cast<
                std::chrono::nanoseconds>(now).count();
            MarketTick tick{static_cast<std::uint64_t>(now_ns), 100, 99.5, 10, 'B'};
            if (q.try_push(tick)) ++i;
            else CPU_PAUSE();
            // Pace the producer so the queue isn't always full: we want to
            // measure transit latency, not backpressure. Tune for your box.
            for (volatile int s = 0; s < 200; ++s) { }
        }
    });

    start.store(true, std::memory_order_release);
    producer.join();
    consumer.join();

    std::sort(samples.begin(), samples.end());
    auto pct = [&](double p) {
        return samples[static_cast<std::size_t>(p * (samples.size() - 1))];
    };
    std::printf("  p50=%llu ns  p99=%llu ns  p99.9=%llu ns  max=%llu ns\n",
                (unsigned long long)pct(0.50),
                (unsigned long long)pct(0.99),
                (unsigned long long)pct(0.999),
                (unsigned long long)samples.back());
}

int main() {
    constexpr std::size_t CAP = 1024;
    constexpr std::size_t N   = 10'000'000;

    using MT   = RingBufferMT<MarketTick, CAP>;
    using SPSC = SPSCRingBuffer<MarketTick, CAP>;

    // ---- WARMUP: first run touches cold caches, pages fault in, CPU
    //      ramps its frequency. Discard it so steady-state isn't polluted.
    throughput_bench<SPSC>(N);

    std::printf("Throughput (items/sec):\n");
    std::printf("  mutex   : %.2f M\n", throughput_bench<MT>(N)   / 1e6);
    std::printf("  lockfree: %.2f M\n", throughput_bench<SPSC>(N) / 1e6);

    std::printf("Latency (one-way, ns):\n");
    std::printf("  mutex   :"); latency_bench<MT>(1'000'000);
    std::printf("  lockfree:"); latency_bench<SPSC>(1'000'000);
}
