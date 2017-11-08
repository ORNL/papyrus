cmake_minimum_required(VERSION 2.8)

find_package(MPI REQUIRED)

project(PAPYRUS)

set(PAPYRUS_VERSION_MAJOR 1)
set(PAPYRUS_VERSION_MINOR 0)
set(PAPYRUS_VERSION_PATCH 0)

include(conf/default.cmake)

include(CTest)

add_subdirectory(include)
add_subdirectory(kv)
