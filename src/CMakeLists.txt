# src/CMakeLists.txt
cmake_minimum_required(VERSION 3.16)
project(OpenSync CXX)

# ✅ C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ✅ Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3 -Wall -Wextra -Wformat=2 -D_GLIBCXX_USE_CXX11_ABI=0")

# ✅ External include directories
include_directories(
    /opt/rapidjson/include
    /opt/librdkafka/include
    /opt/instantclient_19_18/sdk/include
)

# ✅ Include directories for source organization
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/core
    ${CMAKE_CURRENT_SOURCE_DIR}/config
    ${CMAKE_CURRENT_SOURCE_DIR}/kafka
    ${CMAKE_CURRENT_SOURCE_DIR}/db
    ${CMAKE_CURRENT_SOURCE_DIR}/sql
    ${CMAKE_CURRENT_SOURCE_DIR}/data
)

# ✅ External library directories
link_directories(
    /opt/librdkafka/lib
    /opt/instantclient_19_18
)

# Define source files by directory
# Main files
file(GLOB MAIN_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

# Core files
file(GLOB CORE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/core/*.cpp")

# Config files
file(GLOB CONFIG_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/config/*.cpp")

# Kafka files
file(GLOB KAFKA_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/kafka/*.cpp")

# DB files
file(GLOB DB_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/db/*.cpp")

# SQL files
file(GLOB SQL_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/sql/*.cpp")

# Data processing files
file(GLOB DATA_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/data/*.cpp")

# ✅ Combine all sources
set(ALL_SOURCES
    ${MAIN_SOURCES}
    ${CORE_SOURCES}
    ${CONFIG_SOURCES}
    ${KAFKA_SOURCES}
    ${DB_SOURCES}
    ${SQL_SOURCES}
    ${DATA_SOURCES}
)

# ✅ Output to bin/
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# ✅ Build target
add_executable(OpenSync ${ALL_SOURCES})

# ✅ Link libraries
target_link_libraries(OpenSync
    pq
    rdkafka
    pthread
    stdc++fs
    z
    dl
)

# ✅ Convenience: custom "run" target
add_custom_target(run
    COMMAND ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/OpenSync
    DEPENDS OpenSync
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

# ✅ Create config directory in build directory
add_custom_command(TARGET OpenSync POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/config
    COMMENT "Creating config directory"
)

# ✅ Copy config files (if they don't exist)
add_custom_command(TARGET OpenSync POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_SOURCE_DIR}/config/config.json
        ${CMAKE_BINARY_DIR}/config/
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_SOURCE_DIR}/config/filter_config.json
        ${CMAKE_BINARY_DIR}/config/
    COMMENT "Copying configuration files"
)

# ✅ Create checkpoints directory
add_custom_command(TARGET OpenSync POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/checkpoints
    COMMENT "Creating checkpoints directory"
)
