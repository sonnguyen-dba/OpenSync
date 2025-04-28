cmake_minimum_required(VERSION 3.10)
project(OpenSync CXX)

# C++ Standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Compiler Flags
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3 -Wall -Wextra -D_GLIBCXX_USE_CXX11_ABI=0")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3 -Wall -Wextra -Wformat=2 -D_GLIBCXX_USE_CXX11_ABI=0")

# Include Directories
include_directories(
    /opt/instantclient_19_18/sdk/include
    /opt/rapidjson/include
    /opt/librdkafka/include
)

# Tự động thêm tất cả thư mục con trong src/ vào include path (chỉ lấy thư mục chứa file .h)
file(GLOB_RECURSE HEADERS "src/*.h")
set(INCLUDE_DIRS "")
foreach(header ${HEADERS})
    get_filename_component(dir ${header} DIRECTORY)
    list(APPEND INCLUDE_DIRS ${dir})
endforeach()
list(REMOVE_DUPLICATES INCLUDE_DIRS)
include_directories(${INCLUDE_DIRS})

# Library Directories
link_directories(
    /opt/instantclient_19_18
    /opt/librdkafka/lib
)

# Find all source files in src/
file(GLOB_RECURSE SOURCES "src/*.cpp")

# Output binary directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Create Executable
add_executable(OpenSync ${SOURCES})

# Link Libraries
target_link_libraries(OpenSync occi clntsh rdkafka pq pthread stdc++fs)

# Custom Target: run
add_custom_target(run
    COMMAND ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/OpenSync
    DEPENDS OpenSync
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

