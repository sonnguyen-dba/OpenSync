cmake_minimum_required(VERSION 3.16)
project(OpenSync VERSION 1.0.0 DESCRIPTION "OpenSync, Consume data from Kafka Json by produce from @OpenLogReplicator to databases" LANGUAGES CXX)

string(TIMESTAMP CMAKE_BUILD_TIMESTAMP "%Y-%m-%d %H:%M" UTC)

# ===============================
# C++ standard
# ===============================
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ===============================
# Compiler flags
# ===============================
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3 -Wall -Wextra -Wformat=2 -D_GLIBCXX_USE_CXX11_ABI=0")

# ===============================
# External include directories
# ===============================
list(APPEND EXTERNAL_INCLUDE_DIRS
    /opt/rapidjson/include
    /opt/librdkafka/include
    /opt/instantclient_19_27/sdk/include
    /usr/local/pgsql-17.5/include
)

include_directories(${EXTERNAL_INCLUDE_DIRS})

# ===============================
# Source include directories
# ===============================
list(APPEND SOURCE_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/app
    ${CMAKE_CURRENT_SOURCE_DIR}/common
    ${CMAKE_CURRENT_SOURCE_DIR}/reader
    ${CMAKE_CURRENT_SOURCE_DIR}/schema
    ${CMAKE_CURRENT_SOURCE_DIR}/sqlbuilder
    ${CMAKE_CURRENT_SOURCE_DIR}/db/oracle
    ${CMAKE_CURRENT_SOURCE_DIR}/db/postgresql
    ${CMAKE_CURRENT_SOURCE_DIR}/kafka
    ${CMAKE_CURRENT_SOURCE_DIR}/logger
    ${CMAKE_CURRENT_SOURCE_DIR}/metrics
    ${CMAKE_CURRENT_SOURCE_DIR}/thread
    ${CMAKE_CURRENT_SOURCE_DIR}/utils
    ${CMAKE_CURRENT_SOURCE_DIR}/writer
)
include_directories(${SOURCE_INCLUDE_DIRS})

# ===============================
# External library directories
# ===============================
list(APPEND LIBRARY_DIRS
    /opt/librdkafka/lib
    /opt/instantclient_19_27
    /usr/local/pgsql-17.5/lib             # ✅ PostgreSQL lib
)
link_directories(${LIBRARY_DIRS})

# ===============================
# Source files
# ===============================
list(APPEND ListMain main.cpp)

list(APPEND ListApp app/AppInitializer.cpp)

list(APPEND ListCommon
    common/Queues.cpp
    common/TimeUtils.cpp
)

list(APPEND ListReader
    reader/ConfigLoader.cpp
    reader/FilterConfigLoader.cpp
)

list(APPEND ListSchema
    schema/OracleSchemaCache.cpp
    schema/PostgreSQLSchemaCache.cpp
)

list(APPEND ListSqlBuilder
    sqlbuilder/OracleSQLBuilder.cpp
    sqlbuilder/PostgreSQLSQLBuilder.cpp
)

list(APPEND ListDB
    db/DBConnectorFactory.cpp
    db/DBException.cpp
)

list(APPEND ListOracle
    db/oracle/OracleConnector.cpp
)


list(APPEND ListInitialLoad
	initialload/InitialLoaderOracleToPostgreSQL.cpp
)
list(APPEND ListPostgreSQL
    db/postgresql/PostgreSQLConnector.cpp
    db/postgresql/ConnectorUtils.cpp
)

list(APPEND ListKafka
    kafka/KafkaConsumer.cpp
    kafka/KafkaProcessor.cpp
    kafka/FileWatcher.cpp
)

list(APPEND ListLogger
    logger/Logger.cpp
)

list(APPEND ListMetrics
    metrics/MetricsServer.cpp
    metrics/MetricsExporter.cpp
    metrics/SystemMetricsUtils.cpp
    metrics/MonitorManager.cpp
)

list(APPEND ListThread
    thread/KafkaConsumerThread.cpp
    thread/dbwriterthread/DBWriterThread.cpp
    thread/dbwriterthread/ThreadPoolDBWriter.cpp
    thread/workerthread/WorkerThread.cpp
    thread/monitorthread/MonitorThread.cpp
)

list(APPEND ListUtils
    utils/BufferGCManager.cpp
    utils/MemoryUtils.cpp
    utils/SQLUtils.cpp
)

list(APPEND ListWriter
    writer/CheckpointManager.cpp
    writer/WriteDataToDB.cpp
)

# ===============================
# Object libraries
# ===============================
add_library(LibMain OBJECT ${ListMain})
add_library(LibApp OBJECT ${ListApp})
add_library(LibCommon OBJECT ${ListCommon})        
add_library(LibReader OBJECT ${ListReader})
add_library(LibSchema OBJECT ${ListSchema})
add_library(LibSqlBuilder OBJECT ${ListSqlBuilder})
add_library(LibOracle OBJECT ${ListOracle})
add_library(LibInitialLoad OBJECT ${ListInitialLoad})
add_library(LibPostgreSQL OBJECT ${ListPostgreSQL})
add_library(LibDB OBJECT ${ListDB})
add_library(LibKafka OBJECT ${ListKafka})
target_compile_options(LibKafka
    PRIVATE
    -Wno-ignored-qualifiers
)
add_library(LibLogger OBJECT ${ListLogger})
add_library(LibMetrics OBJECT ${ListMetrics})
add_library(LibThread OBJECT ${ListThread})
add_library(LibUtils OBJECT ${ListUtils})
add_library(LibWriter OBJECT ${ListWriter})

# PostgreSQL dependency (compiler + linker)
target_link_libraries(LibPostgreSQL PRIVATE pq)

# ===============================
# Executable
# ===============================
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(OpenSync ${ListMain})

target_link_libraries(OpenSync
    LibApp
    LibCommon
    LibReader
    LibSchema
    LibSqlBuilder
    LibDB
    LibOracle
    LibPostgreSQL
    LibInitialLoad
    LibKafka
    LibLogger
    LibMetrics
    LibThread
    LibUtils
    LibWriter
    occi
    clntsh
    rdkafka
    pq
    pthread
    stdc++fs
)

# ===============================
# Convenience targets
# ===============================
add_custom_target(run
    COMMAND ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/OpenSync
    DEPENDS OpenSync
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

add_custom_command(TARGET OpenSync POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/config
)

add_custom_command(TARGET OpenSync POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
            ${CMAKE_SOURCE_DIR}/config/config.json
            ${CMAKE_BINARY_DIR}/config/
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
            ${CMAKE_SOURCE_DIR}/config/filter_config.json
            ${CMAKE_BINARY_DIR}/config/
)

add_custom_command(TARGET OpenSync POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/checkpoints
)
