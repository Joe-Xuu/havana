cmake_minimum_required(VERSION 3.10)

project(havana VERSION 0.1)

# 必须 C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include_directories(include)

# 你的 benchmark
# add_executable(havana_concurrent src/concurrent_bench.cpp)
add_executable(chucking src/benchmark_chucking.cpp)
add_executable(shell src/shell.cpp)
add_executable(comp_benchmark src/benchmark.cpp)