# OpenSync/CMakeLists.txt
cmake_minimum_required(VERSION 3.16)
project(OpenSync)

# Add include directories at the project level for all subdirectories
include_directories(
    ${CMAKE_SOURCE_DIR}/src/core
    ${CMAKE_SOURCE_DIR}/src/config
    ${CMAKE_SOURCE_DIR}/src/kafka
    ${CMAKE_SOURCE_DIR}/src/db
    ${CMAKE_SOURCE_DIR}/src/sql
    ${CMAKE_SOURCE_DIR}/src/data
)

add_subdirectory(src)
