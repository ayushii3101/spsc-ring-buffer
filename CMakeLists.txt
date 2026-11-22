# CMakeLists.txt
cmake_minimum_required(VERSION 3.16)
project(spsc_ring_buffer CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)        # use -std=c++20, not -std=gnu++20

# Default to a real optimized build if the user didn't pick one.
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Tuned, optimized flags for the benchmark build.
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")

find_package(Threads REQUIRED)        # pulls in -pthread correctly

# ---- The benchmark executable (headers are header-only, just include) ----
add_executable(benchmark benchmark.cpp)
target_link_libraries(benchmark PRIVATE Threads::Threads)
# -march=native lets the compiler use your CPU's full instruction set
# (AVX, etc.). Great for local benchmarking; drop it if you ship binaries
# that must run on older CPUs than the one you compiled on.
target_compile_options(benchmark PRIVATE -march=native)

# ---- Correctness tests ----
add_executable(tests tests.cpp)
target_link_libraries(tests PRIVATE Threads::Threads)

# ---- Same tests, built with ThreadSanitizer, as a separate target ----
# TSan needs its own build (instrumentation + separate flags), so we make
# a distinct executable rather than toggling the main one.
add_executable(tests_tsan tests.cpp)
target_link_libraries(tests_tsan PRIVATE Threads::Threads)
target_compile_options(tests_tsan PRIVATE -fsanitize=thread -O1 -g)
target_link_options(tests_tsan    PRIVATE -fsanitize=thread)

enable_testing()
add_test(NAME correctness COMMAND tests)
