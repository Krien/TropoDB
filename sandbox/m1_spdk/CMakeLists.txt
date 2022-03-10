
cmake_minimum_required(VERSION 3.16)
project(m1-project VERSION 1.0.1 DESCRIPTION "M1 for SPDK")
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

# Dependens on your own installation location...
set(SPDK_DIR "/home/$ENV{USER}/spdk")
set(DPDK_LIB_DIR "${SPDK_DIR}/dpdk/build/lib")

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_PREFIX_PATH "/home/$ENV{USER}/local/")
set(CMAKE_LIBRARY_PATH "/home/$ENV{USER}/local/lib/")
set(CMAKE_INSTALL_PREFIX "/home/$ENV{USER}/local/")

find_package(PkgConfig REQUIRED)
if(NOT PKG_CONFIG_FOUND)
    message(FATAL_ERROR "pkg-config command not found!" )
endif()

set(PROJECT_SOURCE_DIR ${CMAKE_SOURCE_DIR}/src)
set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/bin)
set(CMAKE_LIB_DIR ${CMAKE_SOURCE_DIR}/lib)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
set(LIBRARY_OUTPUT_PATH ${CMAKE_LIB_DIR})

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_COMPILER g++)
message("compiler is ${CMAKE_CXX_COMPILER_ID}   and name is ${CMAKE_CXX_COMPILER}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0  -ggdb -Wl,--no-as-needed -Wall -Wextra")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment")


set(ENV{PKG_CONFIG_PATH} "$ENV{PKG_CONFIG_PATH}:${SPDK_DIR}/build/lib/pkgconfig/")
message("Looking for packages in $ENV{PKG_CONFIG_PATH}")
pkg_search_module(SPDK REQUIRED IMPORTED_TARGET spdk_nvme)
pkg_search_module(DPDK REQUIRED IMPORTED_TARGET spdk_env_dpdk)
pkg_search_module(SYS REQUIRED IMPORTED_TARGET spdk_syslibs)

add_definitions (${SPDK_CFLAGS} ${DPDK_CFLAGS})

# No one needs to know, but we hacked because .pc files do not function????...
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

include(GNUInstallDirs)
include_directories (${NVME_INCLUDE_DIRS})
link_directories (${NVME_LIBRARY_DIRS})
add_executable(m1 m1/device.cpp m1/device.h  m1/m1.cpp)
add_definitions (${NVME_CFLAGS})
set_target_properties(m1 PROPERTIES LINK_FLAGS ${SPDK_LIB_FLAGS})
