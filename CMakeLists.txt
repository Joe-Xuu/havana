cmake_minimum_required(VERSION 3.10)

project(havana VERSION 0.1)

# C++17 standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# 包含头文件目录
include_directories(include)

# 添加所有的源文件
file(GLOB SOURCES "src/*.cpp")

# 生成可执行文件
add_executable(havana_db ${SOURCES})