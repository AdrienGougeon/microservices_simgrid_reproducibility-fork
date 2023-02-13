cmake_minimum_required(VERSION 3.17)

project(ElasticTask)

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -g")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/tools/cmake/")

find_package(SimGrid REQUIRED)

add_subdirectory(Elasticity)
if (WITH_EXAMPLES)
    add_subdirectory(examples)
endif()
