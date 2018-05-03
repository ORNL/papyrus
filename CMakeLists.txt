cmake_minimum_required(VERSION 3.8)

project(PAPYRUS)

set(PAPYRUS_VERSION_MAJOR 1)
set(PAPYRUS_VERSION_MINOR 0)
set(PAPYRUS_VERSION_PATCH 0)

find_package(MPI REQUIRED)
if("${MPI_C_COMPILER}")
    set(CMAKE_C_COMPILER "${MPI_C_COMPILER}")
endif()
if("${MPI_CXX_COMPILER}")
    set(CMAKE_CXX_COMPILER "${MPI_CXX_COMPILER}")
endif()
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include(CTest)

add_subdirectory(include)
add_subdirectory(kv)
