project(metricq-aggregator)

cmake_minimum_required(VERSION 3.9)

include(cmake/DefaultBuildType.cmake)
include(cmake/GitSubmoduleUpdate.cmake)
git_submodule_update()

add_subdirectory(lib/metricq)
add_subdirectory(lib/nitro)

set(SRCS src/main.cpp src/aggregation_metric.cpp src/aggragator.cpp src/log.cpp src/main.cpp)

add_executable(metricq-aggregator ${SRCS})
target_include_directories(metricq-aggregator
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_compile_features(metricq-aggregator PUBLIC cxx_std_17)
target_compile_options(metricq-aggregator PUBLIC -Wall -Wextra -pedantic)

target_link_libraries(metricq-aggregator
    PUBLIC
        metricq::transformer
        Nitro::env
        Nitro::log
        Nitro::jiffy
)

install(TARGETS metricq-aggregator RUNTIME DESTINATION bin)
