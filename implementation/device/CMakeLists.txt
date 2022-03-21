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
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0  -ggdb -Wl,--no-as-needed -Wall -Wextra")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment")

# Change to personal location.
if(DEFINED ENV{SPDK_DIR})
    set(SPDK_DIR "$ENV{SPDK_DIR}")
else(DEFINED ENV{SPDK_DIR})
    set(SPDK_DIR "/home/$ENV{USER}/local/spdk")
endif()
set(DPDK_LIB_DIR "${SPDK_DIR}/dpdk/build/lib")
message("looking for SPDK in ${SPDK_DIR}")

find_package(PkgConfig REQUIRED)
if(NOT PKG_CONFIG_FOUND)
    message(FATAL_ERROR "pkg-config command not found!" )
endif()

set(ENV{PKG_CONFIG_PATH} "$ENV{PKG_CONFIG_PATH}:${SPDK_DIR}/build/lib/pkgconfig/")
message("Looking for SPDK packages...")
pkg_search_module(SPDK REQUIRED IMPORTED_TARGET spdk_nvme)
pkg_search_module(DPDK REQUIRED IMPORTED_TARGET spdk_env_dpdk)
pkg_search_module(SYS REQUIRED IMPORTED_TARGET spdk_syslibs)

# TODO this is of course not portable or the clean method, please fix.
set(SPDK_LIB_FLAGS "")
set(SPDK_LIB_FLAGS "${SPDK_LIB_FLAGS} -L${SPDK_DIR}/build/lib -Wl,--whole-archive -Wl,--no-as-needed")
foreach(X in ${SPDK_LIBRARIES})
    if (NOT ${X} STREQUAL "in")
        set(SPDK_LIB_FLAGS "${SPDK_LIB_FLAGS} -l${X}")
    endif()
endforeach()
set(SPDK_LIB_FLAGS "${SPDK_LIB_FLAGS} -Wl,--no-whole-archive ${SPDK_DIR}/build/lib/libspdk_env_dpdk.a -Wl,-rpath-link ${DPDK_LIB_DIR} -Wl,--whole-archive")
set(SPDK_LIB_FLAGS "${SPDK_LIB_FLAGS}  ${DPDK_LIB_DIR}/librte_bus_pci.a ${DPDK_LIB_DIR}/librte_cryptodev.a ${DPDK_LIB_DIR}/librte_eal.a  ${DPDK_LIB_DIR}/librte_ethdev.a ${DPDK_LIB_DIR}/librte_hash.a ${DPDK_LIB_DIR}/librte_kvargs.a ${DPDK_LIB_DIR}/librte_mbuf.a ${DPDK_LIB_DIR}/librte_mempool.a   ${DPDK_LIB_DIR}/librte_mempool_ring.a ${DPDK_LIB_DIR}/librte_net.a ${DPDK_LIB_DIR}/librte_pci.a ${DPDK_LIB_DIR}/librte_power.a ${DPDK_LIB_DIR}/librte_rcu.a ${DPDK_LIB_DIR}/librte_ring.a ${DPDK_LIB_DIR}/librte_telemetry.a ${DPDK_LIB_DIR}/librte_vhost.a")
set(SYS_LIBRARIES " -lnuma -ldl  -L/home/user/spdk/isa-l/.libs -lisal -pthread -lrt -luuid -lcrypto -lm -laio")
set(SPDK_LIB_FLAGS "${SPDK_LIB_FLAGS} -Wl,--no-whole-archive ${SYS_LIBRARIES}")

set(include_dir "include")
set(common_sources
    src/device.cpp 
    "${include_dir}/device.h"
    "${include_dir}/utils.h"
)

# Library
add_library(znsdevice_lib
    "${common_sources}"
)
target_include_directories(znsdevice_lib PUBLIC include)
target_include_directories(znsdevice_lib PRIVATE src)

set_target_properties(znsdevice_lib PROPERTIES LINK_FLAGS ${SPDK_LIB_FLAGS})
add_definitions (${SPDK_CFLAGS} ${DPDK_CFLAGS})

# Standalone CLI
add_executable(znsdevice
    "${common_sources}"
    src/main.cpp
)
target_include_directories(znsdevice PUBLIC include)
target_include_directories(znsdevice PRIVATE src)

set_target_properties(znsdevice PROPERTIES LINK_FLAGS ${SPDK_LIB_FLAGS})
add_definitions (${SPDK_CFLAGS} ${DPDK_CFLAGS})

# Tests
add_executable(device_initial_test
    tests/device_initial_test.cpp
)
set_target_properties(device_initial_test PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(device_initial_test znsdevice_lib)
target_include_directories(device_initial_test PRIVATE include)
set_target_properties(device_initial_test PROPERTIES LINK_FLAGS ${LINK_FLAGS} ${SPDK_LIB_FLAGS})


# Testing