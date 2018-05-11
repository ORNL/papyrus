include_directories(${PROJECT_SOURCE_DIR}/include)

set(PAPYRUSKV_SOURCES
    Bloom.cpp
    CAPI.cpp
    Cache.cpp
    Command.cpp
    Compactor.cpp
    DB.cpp
    Dispatcher.cpp
    Hasher.cpp
    Listener.cpp
    MemTable.cpp
    Message.cpp
    Platform.cpp
    Pool.cpp
    RemoteBuffer.cpp
    SSTable.cpp
    Signal.cpp
    Slice.cpp
    Thread.cpp
    Timer.cpp
    )

if(PAPYRUS_USE_FORTRAN)
   set(PAPYRUSKV_SOURCES ${PAPYRUSKV_SOURCES}
        C_Binding.f90
       )
   set(CMAKE_Fortran_MODULE_DIRECTORY ${PROJECT_BINARY_DIR}/include)
endif()

add_library(papyruskv ${PAPYRUSKV_SOURCES})

install(TARGETS papyruskv DESTINATION lib)

if(PAPYRUS_USE_FORTRAN)
   install(FILES ${PROJECT_BINARY_DIR}/include/papyrus.mod DESTINATION include)
endif()
