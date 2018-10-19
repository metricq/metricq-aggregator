project(metricq-aggregator)

cmake_minimum_required(VERSION 3.9)

include(cmake/DefaultBuildType.cmake)
include(cmake/GitSubmoduleUpdate.cmake)
git_submodule_update()

set(SRCS src/main.cpp)

add_executable(metricq-aggregator ${SRCS})
target_include_directories(metricq-aggregator
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
        )
target_compile_features(metricq-aggregator PUBLIC cxx_std_17)
target_compile_options(metricq-aggregator PUBLIC -Wall -Wextra -pedantic)

add_subdirectory(lib/metricq)
add_subdirectory(lib/hta)
add_subdirectory(lib/nitro)

target_link_libraries(metricq-aggregator
        PUBLIC
        metricq::sink
        metricq::source
        Nitro::nitro-env
        Nitro::nitro-log
        Nitro::nitro-jiffy
        )

install(TARGETS metricq-aggregator RUNTIME DESTINATION bin)
