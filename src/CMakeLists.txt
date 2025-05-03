# src/CMakeLists.txt
cmake_minimum_required(VERSION 3.16)
project(OpenSync VERSION 1.0.0 LANGUAGES CXX)

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
    ${CMAKE_CURRENT_SOURCE_DIR}/app
    ${CMAKE_CURRENT_SOURCE_DIR}/common
    ${CMAKE_CURRENT_SOURCE_DIR}/config
    ${CMAKE_CURRENT_SOURCE_DIR}/db/oracle
    ${CMAKE_CURRENT_SOURCE_DIR}/dbconnector
    ${CMAKE_CURRENT_SOURCE_DIR}/exception
    ${CMAKE_CURRENT_SOURCE_DIR}/external
    ${CMAKE_CURRENT_SOURCE_DIR}/kafka
    ${CMAKE_CURRENT_SOURCE_DIR}/logger
    ${CMAKE_CURRENT_SOURCE_DIR}/metrics
    ${CMAKE_CURRENT_SOURCE_DIR}/monitor
    ${CMAKE_CURRENT_SOURCE_DIR}/schema
    ${CMAKE_CURRENT_SOURCE_DIR}/sqlbuilder
    ${CMAKE_CURRENT_SOURCE_DIR}/thread
    ${CMAKE_CURRENT_SOURCE_DIR}/time
    ${CMAKE_CURRENT_SOURCE_DIR}/utils
    ${CMAKE_CURRENT_SOURCE_DIR}/WriteDataToDB
    ${CMAKE_CURRENT_SOURCE_DIR}/writer
)

# ✅ External library directories
link_directories(
    /opt/librdkafka/lib
    /opt/instantclient_19_18
)

# Define source files by directory
# Main files
file(GLOB MAIN_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
file(GLOB APP_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/app/*.cpp")
file(GLOB COMMON_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/common/*.cpp")
file(GLOB CONFIG_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/config/*.cpp")
file(GLOB DB_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/db/oracle/*.cpp")
file(GLOB DBCONNECT_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/*.cpp")
file(GLOB EXCEPT_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/exception/*.cpp")
file(GLOB EXTEN_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/external/*.cpp")
file(GLOB KAFKA_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/kafka/*.cpp")
file(GLOB LOGGER_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/logger/*.cpp")
file(GLOB METRICS_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/metrics/*.cpp")
file(GLOB MONITOR_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/monitor/*.cpp")
file(GLOB SCHEMA_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/schema/*.cpp")
file(GLOB SQLBUILDER_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/sqlbuilder/*.cpp")
file(GLOB THREAD_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/thread/*.cpp")
file(GLOB DBTHREAD_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/thread/dbwriterthread/*.cpp")
file(GLOB WTHREAD_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/thread/workerthread/*.cpp")
file(GLOB MTHREAD_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/thread/monitorthread/*.cpp")
file(GLOB TIME_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/time/*.cpp")
file(GLOB UTILS_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/utils/*.cpp")
file(GLOB WRITEDATATODB_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/WriteDataToDB/*.cpp")
file(GLOB WRITER_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/writer/*.cpp")

# ✅ Combine all sources
set(ALL_SOURCES
    ${MAIN_SOURCES}
    ${APP_SOURCES}
    ${COMMON_SOURCES}
    ${CONFIG_SOURCES}
    ${DB_SOURCES}
    ${DBCONNECT_SOURCES}
    ${EXCEPT_SOURCES}
    ${EXTEN_SOURCES}
    ${KAFKA_SOURCES}
    ${LOGGER_SOURCES}
    ${METRICS_SOURCES}
    ${MONITOR_SOURCES}
    ${SCHEMA_SOURCES}
    ${SQLBUILDER_SOURCES}
    ${THREAD_SOURCES}
    ${DBTHREAD_SOURCES}
    ${WTHREAD_SOURCES}
    ${MTHREAD_SOURCES}
    ${TIME_SOURCES}
    ${UTILS_SOURCES}
    ${WRITEDATATODB_SOURCES}
    ${WRITER_SOURCES}
)

# ✅ Output to bin/
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# ✅ Build target
add_executable(OpenSync ${ALL_SOURCES})

# ✅ Link libraries
target_link_libraries(OpenSync occi clntsh rdkafka pq pthread stdc++fs)

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
