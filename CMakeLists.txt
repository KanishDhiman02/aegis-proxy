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
    hash_ring.cpp
    rate_limiter.cpp
    circuit_breaker.cpp
)

target_include_directories(aegis_proxy PRIVATE ${asio_SOURCE_DIR}/asio/include)
target_compile_definitions(aegis_proxy PRIVATE ASIO_STANDALONE)
target_link_libraries(aegis_proxy PRIVATE Threads::Threads)

# Standalone test for the hash ring's distribution / reshuffle properties.
# Doesn't need Asio at all - pure data structure logic.
add_executable(test_hash_ring
    test_hash_ring.cpp
    hash_ring.cpp
)

# Standalone correctness + contention benchmark for the rate limiter.
# Also doesn't need Asio - pure data structure + threading logic.
add_executable(test_rate_limiter
    test_rate_limiter.cpp
    rate_limiter.cpp
)
target_link_libraries(test_rate_limiter PRIVATE Threads::Threads)

# Standalone circuit breaker state machine tests. No Asio dependency.
add_executable(test_circuit_breaker
    test_circuit_breaker.cpp
    circuit_breaker.cpp
)
target_link_libraries(test_circuit_breaker PRIVATE Threads::Threads)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_compile_options(aegis_proxy PRIVATE -Wall -Wextra)
    target_compile_options(test_hash_ring PRIVATE -Wall -Wextra -O2)
    target_compile_options(test_rate_limiter PRIVATE -Wall -Wextra -O2)
    target_compile_options(test_circuit_breaker PRIVATE -Wall -Wextra -O2)
endif()