cmake_minimum_required(VERSION 3.9)

project(metricq-aggregator)

include(cmake/DefaultBuildType.cmake)
include(cmake/GitSubmoduleUpdate.cmake)
git_submodule_update()

add_subdirectory(lib/metricq)

set(SRCS src/main.cpp src/aggregation_metric.cpp src/aggragator.cpp src/main.cpp)

add_executable(metricq-aggregator ${SRCS})

target_compile_features(metricq-aggregator PUBLIC cxx_std_17)
target_compile_options(metricq-aggregator PUBLIC -Wall -Wextra -pedantic)

target_link_libraries(metricq-aggregator
    PUBLIC
        metricq::transformer
        metricq::logger-nitro
        Nitro::env
)

install(TARGETS metricq-aggregator RUNTIME DESTINATION bin)
