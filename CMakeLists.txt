cmake_minimum_required(VERSION 3.0)
project(mktdata)

set(SRC_ROOT ${PROJECT_SOURCE_DIR})

if (CMAKE_BUILD_TYPE MATCHES Debug)
  message("DEBUG BUILD")
else ()
  message("RELEASE BUILD")
  add_compile_options(-Ofast)
endif ()
  
set(CMAKE_CXX_STANDARD 20)
add_executable(mktdata main.cpp)
target_include_directories(mktdata PRIVATE ${SRC_ROOT}/include/)
target_link_libraries(mktdata PRIVATE pthread boost_program_options)
