cmake_minimum_required(VERSION 3.16)
project(OpenSync VERSION 1.0.0 DESCRIPTION "OpenSync, Consume data from Kafka Json by produce from @OpenLogReplicator to databases" LANGUAGES CXX)

string(TIMESTAMP CMAKE_BUILD_TIMESTAMP "%Y-%m-%d %H:%M" UTC)

# ✅ C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ✅ Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3 -Wall -Wextra -Wformat=2 -D_GLIBCXX_USE_CXX11_ABI=0")

# ✅ External include directories

list(APPEND EXTERNAL_INCLUDE_DIRS
	/opt/rapidjson/include
	/opt/librdkafka/include
	/opt/instantclient_19_18/sdk/include)

include_directories(${EXTERNAL_INCLUDE_DIRS})

# ✅ Include directories for source organization

list(APPEND SOURCE_INCLUDE_DIRS
	${CMAKE_CURRENT_SOURCE_DIR}
	${CMAKE_CURRENT_SOURCE_DIR}/app
	${CMAKE_CURRENT_SOURCE_DIR}/common
	${CMAKE_CURRENT_SOURCE_DIR}/config
	${CMAKE_CURRENT_SOURCE_DIR}/db/oracle
	${CMAKE_CURRENT_SOURCE_DIR}/db/postgresql
	${CMAKE_CURRENT_SOURCE_DIR}/kafka
	${CMAKE_CURRENT_SOURCE_DIR}/logger
	${CMAKE_CURRENT_SOURCE_DIR}/metrics
	${CMAKE_CURRENT_SOURCE_DIR}/thread
	${CMAKE_CURRENT_SOURCE_DIR}/utils
	${CMAKE_CURRENT_SOURCE_DIR}/writer)
include_directories(${SOURCE_INCLUDE_DIRS})

# ✅ External library directories

list(APPEND LIBRARY_DIRS
	/opt/librdkafka/lib
	/opt/instantclient_19_18
)
link_directories(${LIBRARY_DIRS})

# Define source files by directory using list(APPEND)

list(APPEND ListMain main.cpp )
list(APPEND ListApp app/AppInitializer.cpp )
list(APPEND ListCommon common/Queues.cpp
											 common/TimeUtils.cpp)

list(APPEND ListConfig
	config/ConfigLoader.cpp
	config/FilterConfigLoader.cpp
)

list(APPEND ListDB db/DBConnectorFactory.cpp)
									 db/DBException.cpp

list(APPEND ListOracle db/oracle/OracleConnector.cpp
											 db/oracle/OracleSchemaCache.cpp
											 db/oracle//OracleSQLBuilder.cpp
list(APPEND ListPostgreSQL
            db/postgresql/PostgreSQLConnector.cpp
            db/postgresql/ConnectorUtils.cpp
						db/postgresql/PostgreSQLSchemaCache.cpp
						db/postgresql/PostgreSQLSQLBuilder.cpp)

list(APPEND ListKafka
	kafka/KafkaConsumer.cpp
	kafka/KafkaProcessor.cpp
	kafka/FileWatcher.cpp)

list(APPEND ListLogger
	logger/Logger.cpp)

list(APPEND ListMetrics
	metrics/MetricsServer.cpp
	metrics/MetricsExporter.cpp
	metrics/SystemMetricsUtils.cpp
	metrics/MonitorManager.cpp)

list(APPEND ListThread
  thread/KafkaConsumerThread.cpp
  thread/dbwriterthread/DBWriterThread.cpp
  thread/dbwriterthread/ThreadPoolDBWriter.cpp
  thread/workerthread/WorkerThread.cpp
  thread/monitorthread/MonitorThread.cpp)

list(APPEND ListUtils
	utils/BufferGCManager.cpp
	utils/MemoryUtils.cpp
	utils/SQLUtils.cpp
)

list(APPEND ListWriter writer/CheckpointManager.cpp
											 writer/WriteDataToDB.cpp)

# ✅ Create object libraries for each source list
add_library(LibMain OBJECT ${ListMain})
add_library(LibApp OBJECT ${ListApp})
add_library(LibConfig OBJECT ${ListConfig})
add_library(LibOracle OBJECT ${ListOracle})
add_library(LibPostgreSQL OBJECT ${ListPostgreSQL})
add_library(LibDB OBJECT ${ListDB})
add_library(LibKafka OBJECT ${ListKafka})
add_library(LibLogger OBJECT ${ListLogger})
add_library(LibMetrics OBJECT ${ListMetrics})
add_library(LibThread OBJECT ${ListThread})
add_library(LibUtils OBJECT ${ListUtils})
add_library(LibWriter OBJECT ${ListWriter})

# ✅ Output to bin/
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# ✅ Build target
add_executable(OpenSync ${ListMain})

# ✅ Link object libraries
target_link_libraries(OpenSync
    LibApp
    LibCommon
    LibConfig
		LibDB
    LibOracle
    LibPostgreSQL
    LibKafka
    LibLogger
    LibMetrics
    LibThread
    LibUtils
    LibWriter)

#add_executable(OpenSync ${ALL_SOURCES})

# ✅ Link libraries

list(APPEND LINK_LIBRARIES occi clntsh rdkafka pq pthread stdc++fs)
target_link_libraries(OpenSync ${LINK_LIBRARIES})

# ✅ Convenience: custom "run" target

add_custom_target(run COMMAND ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/OpenSync DEPENDS OpenSync WORKING_DIRECTORY ${CMAKE_BINARY_DIR} )

# ✅ Create config directory in build directory

add_custom_command(TARGET OpenSync POST_BUILD COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/config COMMENT "Creating config directory" )

# ✅ Copy config files (if they don't exist)

add_custom_command(TARGET OpenSync POST_BUILD COMMAND ${CMAKE_COMMAND} -E copy_if_different ${CMAKE_SOURCE_DIR}/config/config.json ${CMAKE_BINARY_DIR}/config/ COMMAND ${CMAKE_COMMAND} -E copy_if_different ${CMAKE_SOURCE_DIR}/config/filter_config.json ${CMAKE_BINARY_DIR}/config/ COMMENT "Copying configuration files" )

# ✅ Create checkpoints directory

add_custom_command(TARGET OpenSync POST_BUILD COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/checkpoints COMMENT "Creating checkpoints directory" )
