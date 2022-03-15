cmake_minimum_required(VERSION 3.16)
project(lsm-zns-device VERSION 1.0.1 DESCRIPTION "Interface for ZNS-device")
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

# Library
add_library(lsmzns_lib
    include/device.h
    src/device.cpp 
)

target_include_directories(lsmzns_lib PUBLIC include)
target_include_directories(lsmzns_lib PRIVATE src)

# Standalone
add_executable(lsmzns
    src/main.cpp
    include/device.h
    src/device.cpp
)

# Testing