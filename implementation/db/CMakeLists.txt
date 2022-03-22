# Copyright 2017 The LevelDB Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

cmake_minimum_required(VERSION 3.9)
# Keep the version below in sync with the one in db.h
project(leveldb VERSION 1.23.0 LANGUAGES C CXX)

# C standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_C_STANDARD)
  # This project can use C11, but will gracefully decay down to C89.
  set(CMAKE_C_STANDARD 11)
  set(CMAKE_C_STANDARD_REQUIRED OFF)
  set(CMAKE_C_EXTENSIONS OFF)
endif(NOT CMAKE_C_STANDARD)

# C++ standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_CXX_STANDARD)
  # This project requires C++11.
  set(CMAKE_CXX_STANDARD 11)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS ON)
endif(NOT CMAKE_CXX_STANDARD)


option(LEVELDB_BUILD_TESTS "Build LevelDB's unit tests" ON)
option(LEVELDB_BUILD_BENCHMARKS "Build LevelDB's benchmarks" OFF)
option(LEVELDB_INSTALL "Install LevelDB's header and library" OFF)

include(CheckIncludeFile)
check_include_file("unistd.h" HAVE_UNISTD_H)

include(CheckLibraryExists)
check_library_exists(crc32c crc32c_value "" HAVE_CRC32C)
check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
check_library_exists(tcmalloc malloc "" HAVE_TCMALLOC)

include(CheckCXXSymbolExists)
# Using check_cxx_symbol_exists() instead of check_c_symbol_exists() because
# we're including the header from C++, and feature detection should use the same
# compiler language that the project will use later. Principles aside, some
# versions of do not expose fdatasync() in <unistd.h> in standard C mode
# (-std=c11), but do expose the function in standard C++ mode (-std=c++11).
check_cxx_symbol_exists(fdatasync "unistd.h" HAVE_FDATASYNC)
check_cxx_symbol_exists(F_FULLFSYNC "fcntl.h" HAVE_FULLFSYNC)
check_cxx_symbol_exists(O_CLOEXEC "fcntl.h" HAVE_O_CLOEXEC)

if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
  # Disable C++ exceptions.
  string(REGEX REPLACE "/EH[a-z]+" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /EHs-c-")
  add_definitions(-D_HAS_EXCEPTIONS=0)

  # Disable RTTI.
  string(REGEX REPLACE "/GR" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /GR-")
else(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
  # Enable strict prototype warnings for C code in clang and gcc.
  if(NOT CMAKE_C_FLAGS MATCHES "-Wstrict-prototypes")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wstrict-prototypes")
  endif(NOT CMAKE_C_FLAGS MATCHES "-Wstrict-prototypes")

  # Disable C++ exceptions.
  string(REGEX REPLACE "-fexceptions" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")

  # Disable RTTI.
  string(REGEX REPLACE "-frtti" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-rtti")
endif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")

# Test whether -Wthread-safety is available. See
# https://clang.llvm.org/docs/ThreadSafetyAnalysis.html
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-Wthread-safety HAVE_CLANG_THREAD_SAFETY)

# Used by googletest.
check_cxx_compiler_flag(-Wno-missing-field-initializers
                        LEVELDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)

include(CheckCXXSourceCompiles)

# Test whether C++17 __has_include is available.
check_cxx_source_compiles("
#if defined(__has_include) &&  __has_include(<string>)
#include <string>
#endif
int main() { std::string str; return 0; }
" HAVE_CXX17_HAS_INCLUDE)

set(LEVELDB_PUBLIC_INCLUDE_DIR "include/lsm_zns")
set(LEVELDB_PORT_CONFIG_DIR "include/port")

configure_file(
  "port/port_config.h.in"
  "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
)

include_directories(
  "${PROJECT_BINARY_DIR}/include"
  "."
)

if(BUILD_SHARED_LIBS)
  # Only export LEVELDB_EXPORT symbols from the shared library.
  add_compile_options(-fvisibility=hidden)
endif(BUILD_SHARED_LIBS)

# Must be included before CMAKE_INSTALL_INCLUDEDIR is used.
include(GNUInstallDirs)

add_library(leveldb "")
target_sources(leveldb
  PRIVATE
    "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
    "db/db_impl.cc"
    "db/db_impl.h"
    "db/dbformat.cc"
    "db/dbformat.h"
    "db/snapshot.h"
    "port/port_stdcxx.h"
    "port/port.h"
    "port/thread_annotations.h"
    "util/arena.cc"
    "util/arena.h"
    "util/bloom.cc"
    "util/coding.cc"
    "util/coding.h"
    "util/logging.cc"
    "util/logging.h"
    "util/comparator.cc"
    "util/filter_policy.cc"
    "util/logging.cc"
    "util/logging.h"
    "util/mutexlock.h"
    "util/options.cc"
    "util/random.h"
    "util/status.cc"
    "util/env.cc"

  # Only CMake 3.3+ supports PUBLIC sources in targets exported by "install".
  $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>
    "${LEVELDB_PUBLIC_INCLUDE_DIR}/cache.h"
    "${LEVELDB_PUBLIC_INCLUDE_DIR}/comparator.h"
    "${LEVELDB_PUBLIC_INCLUDE_DIR}/db.h"
    "${LEVELDB_PUBLIC_INCLUDE_DIR}/env.h"
    "${LEVELDB_PUBLIC_INCLUDE_DIR}/export.h"
    "${LEVELDB_PUBLIC_INCLUDE_DIR}/filter_policy.h"
    "${LEVELDB_PUBLIC_INCLUDE_DIR}/iterator.h"
    "${LEVELDB_PUBLIC_INCLUDE_DIR}/options.h"
    "${LEVELDB_PUBLIC_INCLUDE_DIR}/slice.h"
    "${LEVELDB_PUBLIC_INCLUDE_DIR}/status.h"
    "${LEVELDB_PUBLIC_INCLUDE_DIR}/write_batch.h"
)

# target_sources(leveldb
#   PRIVATE
#     "helpers/memenv/memenv.cc"
#     "helpers/memenv/memenv.h"
# )

# if (WIN32)
#   target_sources(leveldb
#     PRIVATE
#       "util/env_windows.cc"
#       #"util/windows_logger.h"
#   )
# else (WIN32)
#   target_sources(leveldb
#     PRIVATE
#       "util/env_posix.cc"
#       "util/posix_logger.h"
#   )
# endif (WIN32)

target_include_directories(leveldb
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

set_target_properties(leveldb
  PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

# target_compile_definitions(leveldb
#   PRIVATE
#     # Used by include/export.h when building shared libraries.
#     # this line sometimes became -D=1 and -LEVELDB_COMPILE_LIBRARY, therefore joined in one string.
#     "LEVELDB_COMPILE_LIBRARY${LEVELDB_PLATFORM_NAME}=1"
#     # Used by port/port.h.
    
# )
if (NOT HAVE_CXX17_HAS_INCLUDE)
  target_compile_definitions(leveldb
    PRIVATE
      LEVELDB_HAS_PORT_CONFIG_H=1
  )
endif(NOT HAVE_CXX17_HAS_INCLUDE)

if(BUILD_SHARED_LIBS)
  target_compile_definitions(leveldb
    PUBLIC
      # Used by include/export.h.
      LEVELDB_SHARED_LIBRARY
  )
endif(BUILD_SHARED_LIBS)

if(HAVE_CLANG_THREAD_SAFETY)
  target_compile_options(leveldb
    PUBLIC
      -Werror -Wthread-safety)
endif(HAVE_CLANG_THREAD_SAFETY)

add_subdirectory("device")
link_directories("${SPDK_LIB_DIRS};${CMAKE_CURRENT_SOURCE_DIR}/device/lib")
target_link_libraries(leveldb znsdevice_lib)
target_link_libraries(leveldb ${SPDK_LIBRARY_DEPENDENCIES})

if(HAVE_CRC32C)
  target_link_libraries(leveldb crc32c)
endif(HAVE_CRC32C)
if(HAVE_SNAPPY)
  target_link_libraries(leveldb snappy)
endif(HAVE_SNAPPY)
if(HAVE_TCMALLOC)
  target_link_libraries(leveldb tcmalloc)
endif(HAVE_TCMALLOC)

# Needed by port_stdcxx.h
find_package(Threads REQUIRED)
target_link_libraries(leveldb Threads::Threads)

add_executable(leveldbutil
  "db/leveldbutil.cc"
)
target_link_libraries(leveldbutil leveldb)


if(LEVELDB_BUILD_TESTS)
  enable_testing()

add_executable(initialtest
  "tests/initial_test.cc"
)
target_link_libraries(initialtest leveldb)
target_link_libraries(initialtest ${SPDK_LIBRARY_DEPENDENCIES})
endif(LEVELDB_BUILD_TESTS)


if(LEVELDB_BUILD_BENCHMARKS)
  # This project uses Google benchmark for benchmarking.
  set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_ENABLE_EXCEPTIONS OFF CACHE BOOL "" FORCE)
  add_subdirectory("third_party/benchmark")

  function(leveldb_benchmark bench_file)
    get_filename_component(bench_target_name "${bench_file}" NAME_WE)

    add_executable("${bench_target_name}" "")
    target_sources("${bench_target_name}"
      PRIVATE
        "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
        "util/histogram.cc"
        "util/histogram.h"
        "util/testutil.cc"
        "util/testutil.h"

        "${bench_file}"
    )
    target_link_libraries("${bench_target_name}" leveldb gmock gtest benchmark)
    #target_compile_definitions("${bench_target_name}$" {LEVELDB_PLATFORM_NAME}=1")
    if (NOT HAVE_CXX17_HAS_INCLUDE)
      target_compile_definitions("${bench_target_name}"
        PRIVATE
          LEVELDB_HAS_PORT_CONFIG_H=1
      )
    endif(NOT HAVE_CXX17_HAS_INCLUDE)
  endfunction(leveldb_benchmark)

  if(NOT BUILD_SHARED_LIBS)
    leveldb_benchmark("benchmarks/db_bench.cc")
  endif(NOT BUILD_SHARED_LIBS)

  check_library_exists(sqlite3 sqlite3_open "" HAVE_SQLITE3)
  if(HAVE_SQLITE3)
    leveldb_benchmark("benchmarks/db_bench_sqlite3.cc")
    target_link_libraries(db_bench_sqlite3 sqlite3)
  endif(HAVE_SQLITE3)

  # check_library_exists is insufficient here because the library names have
  # different manglings when compiled with clang or gcc, at least when installed
  # with Homebrew on Mac.
  set(OLD_CMAKE_REQURED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES})
#   list(APPEND CMAKE_REQUIRED_LIBRARIES kyotocabinet)
#   check_cxx_source_compiles("
# #include <kcpolydb.h>

# int main() {
#   kyotocabinet::TreeDB* db = new kyotocabinet::TreeDB();
#   delete db;
#   return 0;
# }
#   "  HAVE_KYOTOCABINET)
#   set(CMAKE_REQUIRED_LIBRARIES ${OLD_CMAKE_REQURED_LIBRARIES})
#   if(HAVE_KYOTOCABINET)
#     leveldb_benchmark("benchmarks/db_bench_tree_db.cc")
#     target_link_libraries(db_bench_tree_db kyotocabinet)
#   endif(HAVE_KYOTOCABINET)
endif(LEVELDB_BUILD_BENCHMARKS)

if(LEVELDB_INSTALL)
  install(TARGETS leveldb
    EXPORT leveldbTargets
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  )
  install(
    FILES
      "${LEVELDB_PUBLIC_INCLUDE_DIR}/cache.h"
      "${LEVELDB_PUBLIC_INCLUDE_DIR}/comparator.h"
      "${LEVELDB_PUBLIC_INCLUDE_DIR}/db.h"
      "${LEVELDB_PUBLIC_INCLUDE_DIR}/dumpfile.h"
      "${LEVELDB_PUBLIC_INCLUDE_DIR}/env.h"
      "${LEVELDB_PUBLIC_INCLUDE_DIR}/export.h"
      "${LEVELDB_PUBLIC_INCLUDE_DIR}/filter_policy.h"
      "${LEVELDB_PUBLIC_INCLUDE_DIR}/iterator.h"
      "${LEVELDB_PUBLIC_INCLUDE_DIR}/options.h"
      "${LEVELDB_PUBLIC_INCLUDE_DIR}/slice.h"
      "${LEVELDB_PUBLIC_INCLUDE_DIR}/status.h"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/leveldb"
  )

  include(CMakePackageConfigHelpers)
  configure_package_config_file(
    "cmake/${PROJECT_NAME}Config.cmake.in"
    "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
  )
  write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake"
    COMPATIBILITY SameMajorVersion
  )
  install(
    EXPORT leveldbTargets
    NAMESPACE leveldb::
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
  )
  install(
    FILES
      "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake"
      "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
  )
endif(LEVELDB_INSTALL)
