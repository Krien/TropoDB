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
set(SPDK_LIB_FLAGS "${SPDK_LIB_FLAGS} -Wl,--whole-archive -Wl,--no-as-needed -L${SPDK_LIBRARY_DIRS}")
foreach(X in ${SPDK_LIBRARIES})
    if (NOT ${X} STREQUAL "in")
        set(SPDK_LIB_FLAGS "${SPDK_LIB_FLAGS} -l${X}")
    endif()
endforeach()
set(SPDK_LIB_FLAGS "${SPDK_LIB_FLAGS} ${SPDK_DIR}/build/lib/libspdk_env_dpdk.a ")
string(CONCAT SPDK_LIB_FLAGS "${SPDK_LIB_FLAGS} ${DPDK_LIB_DIR}/librte_bus_pci.a " 
    "${DPDK_LIB_DIR}/librte_cryptodev.a ${DPDK_LIB_DIR}/librte_eal.a " 
    "${DPDK_LIB_DIR}/librte_ethdev.a ${DPDK_LIB_DIR}/librte_hash.a "
    "${DPDK_LIB_DIR}/librte_kvargs.a ${DPDK_LIB_DIR}/librte_mbuf.a "
    "${DPDK_LIB_DIR}/librte_mempool.a ${DPDK_LIB_DIR}/librte_mempool_ring.a "
    "${DPDK_LIB_DIR}/librte_net.a ${DPDK_LIB_DIR}/librte_pci.a "
    "${DPDK_LIB_DIR}/librte_power.a ${DPDK_LIB_DIR}/librte_rcu.a "
    "${DPDK_LIB_DIR}/librte_ring.a ${DPDK_LIB_DIR}/librte_telemetry.a " 
    "${DPDK_LIB_DIR}/librte_vhost.a") 
set(SYS_LIBRARIES "-L${SYS_STATIC_LIBRARY_DIRS} -pthread")
foreach(X in ${SYS_STATIC_LIBRARIES})
    if (NOT ${X} STREQUAL "in")
        set(SYS_LIBRARIES "${SYS_LIBRARIES} -l${X}")
    endif()
endforeach()
set(SPDK_LIB_FLAGS "${SPDK_LIB_FLAGS} -Wl,--no-whole-archive ${SYS_LIBRARIES}")
