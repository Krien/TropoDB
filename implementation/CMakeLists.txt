cmake_minimum_required(VERSION 3.16)
project(lsm-zns VERSION 1.0.1 DESCRIPTION "Implementation of an LSM-tree on a ZNS-device")
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

enable_testing()

# Create device to use in db
add_subdirectory(device)

# Create actual db
add_subdirectory(db)

add_executable(lsmzns standalone/main.cpp)