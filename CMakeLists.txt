cmake_minimum_required(VERSION 3.0)
project(mktdata)

set(SRC_ROOT ${PROJECT_SOURCE_DIR})

set(CMAKE_CXX_STANDARD 20)
include_directories(${SRC_ROOT}/include/)
add_executable(mktdata main.cpp)
