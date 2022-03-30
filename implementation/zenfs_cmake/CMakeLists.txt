cmake_minimum_required(VERSION 3.4)

set(zenfs_SOURCES "fs/fs_zenfs.cc" "fs/zbd_zenfs.cc" "fs/io_zenfs.cc" PARENT_SCOPE)
set(zenfs_HEADER_DIR "fs" PARENT_SCOPE)
set(zenfs_CMAKE_EXE_LINKER_FLAGS "-u zenfs_filesystem_reg" PARENT_SCOPE)
set(zenfs_LIBS "zbd" PARENT_SCOPE)
