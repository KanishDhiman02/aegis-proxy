cmake_minimum_required(VERSION 3.16)
project(aegis_proxy CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)
FetchContent_Declare(
    asio
    GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
    GIT_TAG asio-1-30-2
)
FetchContent_MakeAvailable(asio)

find_package(Threads REQUIRED)

add_executable(aegis_proxy
    main.cpp
    connection_pool.cpp
    session.cpp
)

target_include_directories(aegis_proxy PRIVATE ${asio_SOURCE_DIR}/asio/include)
target_compile_definitions(aegis_proxy PRIVATE ASIO_STANDALONE)
target_link_libraries(aegis_proxy PRIVATE Threads::Threads)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_compile_options(aegis_proxy PRIVATE -Wall -Wextra)
endif()
