include_directories(${PROJECT_SOURCE_DIR}/include)

add_library(papyruskv
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

install(TARGETS papyruskv DESTINATION lib)
