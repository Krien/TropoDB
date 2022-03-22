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

# use ";" otherwise the second and third argument become one
set(SPDK_LIBRARY_DIRS "${SPDK_LIBRARY_DIRS};${DPDK_LIBRARY_DIRS};${SYS_STATIC_LIBRARY_DIRS}")
set(SPDK_LIBRARY_DEPENDENCIES 
    -Wl,--whole-archive -Wl,--as-needed
    "${SPDK_LIBRARIES}"
    "${DPDK_LIBRARIES}"
    -Wl,--no-whole-archive
    "${SYS_STATIC_LIBRARIES}" 
    -pthread
)
