cmake_minimum_required(VERSION 2.8)

find_package(MPI REQUIRED)

project(PAPYRUS)

set(PAPYRUS_VERSION_MAJOR 1)
set(PAPYRUS_VERSION_MINOR 0)
set(PAPYRUS_VERSION_PATCH 0)

cmake_host_system_information(RESULT MACHINE QUERY HOSTNAME)

if(${MACHINE} MATCHES "summitdev")
    message(STATUS "Configuration for SUMMITDEV")
    include(conf/summitdev.cmake)
elseif(${MACHINE} MATCHES "cori|nid")
    message(STATUS "Configuration for CORI")
    include(conf/cori.cmake)
elseif(${MACHINE} MATCHES "stampede2")
    message(STATUS "Configuration for STAMPEDE2")
    include(conf/stampede2.cmake)
else()
    message(STATUS "Configuration for DEFAULT")
    include(conf/default.cmake)
endif()

include(CTest)

add_subdirectory(include)
add_subdirectory(kv)
