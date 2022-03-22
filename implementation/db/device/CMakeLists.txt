cmake_minimum_required(VERSION 3.16)
project(lsm-zns-device VERSION 1.0.1 DESCRIPTION "Interface for ZNS-device")
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/bin)
set(CMAKE_LIB_DIR ${CMAKE_SOURCE_DIR}/lib)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
set(LIBRARY_OUTPUT_PATH ${CMAKE_LIB_DIR})

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_COMPILER g++)
message("compiler is ${CMAKE_CXX_COMPILER_ID}   and name is ${CMAKE_CXX_COMPILER}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0  -ggdb -Wall -Wextra")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment")

include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/LinkSPDK.cmake")
link_directories("${SPDK_LIB_DIRS}")

function(SETUP_SPDK_PROJECT_STRUCTURE project_name)
    set_target_properties(${project_name} PROPERTIES LINKER_LANGUAGE CXX)
    target_include_directories(${project_name} PUBLIC include)
    target_include_directories(${project_name} PRIVATE src)
    target_link_libraries(${project_name} ${SPDK_LIBRARY_DEPENDENCIES})
    target_compile_options(
        ${project_name}
        PUBLIC "${SPDK_CFLAGS};${DPDK_CFLAGS}"
    )
endfunction()

set(include_dir "include")
set(common_sources
    src/device.cpp 
    "${include_dir}/device.h"
    "${include_dir}/utils.h"
)

# Library
add_library(znsdevice_lib STATIC
    "${common_sources}"
)
setup_spdk_project_structure(znsdevice_lib)

# Standalone CLI
add_executable(znsdevice
    "${common_sources}"
    src/main.cpp
)
setup_spdk_project_structure(znsdevice)

# Tests
add_executable(device_initial_test
    tests/device_initial_test.cpp
)
target_link_libraries(device_initial_test znsdevice_lib)
setup_spdk_project_structure(device_initial_test)

enable_testing()
add_test(
    NAME device_initial_test_runner
    COMMAND device_initial_test
)

# Default to "default" location of clang-format, override if necessary
if(DEFINED ENV{CLANG_PATH})
    set(CLANG_PATH "$ENV{CLANG_PATH}")
else(DEFINED ENV{CLANG_PATH})
    set(CLANG_PATH "/home/$ENV{USER}/bin/clang-format")
endif()
# format (DO NOT GLOB! instead let the formatter glob when necessary)
add_custom_target(
    format
    COMMAND ${CLANG_PATH}
    -i
    "./src/*.cpp" "./include/*.h"
    "-style=file"
)

set(SPDK_LIB_DIRS "${SPDK_LIB_DIRS}" PARENT_SCOPE)
set(SPDK_LIBRARY_DEPENDENCIES "${SPDK_LIBRARY_DEPENDENCIES}" PARENT_SCOPE)
