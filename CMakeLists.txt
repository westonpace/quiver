# SPDX-License-Identifier: Apache-2.0

message(STATUS "Building using CMake version: ${CMAKE_VERSION}")
cmake_minimum_required(VERSION 3.16)

set(QUIVER_VERSION "0.1.0-SNAPSHOT")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" QUIVER_BASE_VERSION
             "${QUIVER_VERSION}")
project(quiver VERSION "${QUIVER_BASE_VERSION}")

set(QUIVER_VERSION_MAJOR "${quiver_VERSION_MAJOR}")
set(QUIVER_VERSION_MINOR "${quiver_VERSION_MINOR}")
set(QUIVER_VERSION_PATCH "${quiver_VERSION_PATCH}")

option(QUIVER_BUILD_TESTS "Build tests" OFF)
option(QUIVER_BUILD_BENCHMARKS "Build benchmarks" OFF)
option(QUIVER_PYTHON_BINDINGS "Build python bindings" OFF)
option(QUIVER_INSTALL_CXX "Install C++ library" ON)
option(QUIVER_SANITIZER "Enable sanitizers" OFF)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake-modules)
include(ClangTools)

option(QUIVER_CODE_COVERAGE "Enable coverage reporting" OFF)

configure_file(src/quiver/build_id.h.in generated/build_id.h)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(WIN32)
  # gcov does not exist on windows
  set(QUIVER_CODE_COVERAGE OFF)
endif()

if(QUIVER_CODE_COVERAGE)
  include(CodeCoverage)
endif()

find_package(spdlog CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)

# For testing and benchmarking we have a few extra dependencies
# Arrow-C++ for data generation and to test C data interface
# GTest for testing, but the macros and test utilities are sometimes
#   used in benchmarks too
# OpenMP for a thread pool that we use to test/benchmark our code
#   as if it is run by a user with threads
if(QUIVER_BUILD_TESTS OR QUIVER_BUILD_BENCHMARKS)
  find_package(GTest CONFIG REQUIRED)
  find_package(Arrow CONFIG REQUIRED)
  # OpenMP does not yet support CONFIG mode
  find_package(OpenMP MODULE REQUIRED)
endif()

if(QUIVER_BUILD_TESTS)
  enable_testing()
endif()

if(QUIVER_BUILD_BENCHMARKS)
  find_package(benchmark CONFIG REQUIRED)
endif()

# Have to enable clang-tidy after building 3rd party libs
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Global compiler config
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
    add_compile_options(-Wall -Werror -Wextra -Wpedantic -Wunused-result)
  elseif(CMAKE_C_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_C_COMPILER_ID
                                                      STREQUAL "Clang")
    add_compile_options(-Wall -Werror -Wextra -Wdocumentation
                            -Wshorten-64-to-32)
    if (QUIVER_SANITIZER)
      add_compile_options(-fsanitize=undefined -fsanitize=address)
      add_link_options(-fsanitize=undefined -fsanitize=address)
    endif()
  endif()
endif()


add_library(
  quiver_static STATIC
  src/quiver/lib.cc
  src/quiver/accumulator/accumulator.cc
  src/quiver/core/array.cc
  src/quiver/core/builder.cc
  src/quiver/core/io.cc
  src/quiver/datagen/datagen.cc
  src/quiver/hash/hasher.cc
  src/quiver/hashtable/hashtable.cc
  src/quiver/map/equality.cc
  src/quiver/map/hashmap.cc
  src/quiver/row/row_p.cc
  src/quiver/sort/sort.cc
  src/quiver/store/mmap_store.cc
  src/quiver/util/arrow_util.cc
  src/quiver/util/bitmap_ops.cc
  src/quiver/util/bit_util.cc
  src/quiver/util/config.cc
  src/quiver/util/cpu_info.cc
  src/quiver/util/local_allocator_p.cc
  src/quiver/util/logging_p.cc
  src/quiver/util/memory.cc
  src/quiver/util/memory_pool.cc
  src/quiver/util/random.cc
  src/quiver/util/status.cc
  src/quiver/util/string_builder.cc
  src/quiver/util/tracing.cc
  src/quiver/util/uri.cc)

set_target_properties(quiver_static PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_target_properties(quiver_static PROPERTIES VERSION ${QUIVER_VERSION})
set_target_properties(quiver_static PROPERTIES SOVERSION
                                               ${QUIVER_VERSION_MAJOR})
set_target_properties(quiver_static PROPERTIES PUBLIC_HEADER src/quiver/lib.h)

target_precompile_headers(quiver_static PRIVATE src/quiver/pch.h)

target_include_directories(quiver_static PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(quiver_static PRIVATE spdlog::spdlog fmt::fmt)
target_compile_definitions(quiver_static PRIVATE SPDLOG_FMT_EXTERNAL)
target_compile_definitions(quiver_static PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE)
if(QUIVER_CODE_COVERAGE)
  append_coverage_compiler_flags_to_target(quiver_static)
endif()

if(QUIVER_PYTHON_BINDINGS)
  add_subdirectory(src/python)
endif()

if(QUIVER_BUILD_TESTS OR QUIVER_BUILD_BENCHMARKS)
  add_library(
    quiver_testutil OBJECT
    src/quiver/testutil/test_util.cc src/quiver/testutil/gen.cc
    src/quiver/testutil/tmpfiles.cc src/quiver/testutil/main.cc)
  target_include_directories(quiver_testutil
                             PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
  target_link_libraries(quiver_testutil PRIVATE Arrow::arrow_shared)
  include(GoogleTest)
endif()

function(add_test TEST_NAME)
  set(options)
  set(one_value_args)
  set(multi_value_args SOURCES)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}"
                        "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(
      SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(QUIVER_BUILD_TESTS)
    add_executable(${TEST_NAME} ${ARG_SOURCES})
    target_link_libraries(
      ${TEST_NAME} PRIVATE quiver_static quiver_testutil Arrow::arrow_shared
                           GTest::gmock spdlog::spdlog 
                           OpenMP::OpenMP_CXX)
    gtest_discover_tests(${TEST_NAME})
  endif()
endfunction()

add_test(accumulator_test SOURCES src/quiver/accumulator/accumulator_test.cc)
add_test(core_test SOURCES src/quiver/core/array_test.cc)
add_test(hash_test SOURCES src/quiver/hash/hasher_test.cc)
add_test(hashtable_test SOURCES src/quiver/hashtable/hashtable_test.cc)
add_test(map_test SOURCES src/quiver/map/equality_test.cc
         src/quiver/map/hashmap_test.cc)
add_test(row_test SOURCES src/quiver/row/row_test.cc)
add_test(sort_test SOURCES src/quiver/sort/sort_test.cc)
add_test(store_test SOURCES src/quiver/store/mmap_store_test.cc)
add_test(utility_test SOURCES src/quiver/util/arrow_util_test.cc
         src/quiver/util/bit_util_test.cc src/quiver/util/cpu_info_test.cc
         src/quiver/util/local_allocator_p_test.cc
         src/quiver/util/memory_test.cc)

if(QUIVER_BUILD_BENCHMARKS)

  add_library(quiver_benchutil STATIC src/quiver/benchutil/datagen.cc)
  target_include_directories(quiver_benchutil
                             PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
  target_link_libraries(quiver_benchutil PRIVATE Arrow::arrow_shared
                                                 GTest::gmock
                                                 quiver_static
                                                 quiver_testutil)

function(add_benchmark BENCHMARK_NAME)
  set(options)
  set(one_value_args)
  set(multi_value_args SOURCES)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}"
                        "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(
      SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(QUIVER_BUILD_BENCHMARKS)
    add_executable(${BENCHMARK_NAME} ${ARG_SOURCES})
    target_link_libraries(
      ${BENCHMARK_NAME} PRIVATE quiver_static quiver_benchutil Arrow::arrow_shared
                          benchmark::benchmark OpenMP::OpenMP_CXX)
  endif()
endfunction()
                                               
  add_benchmark(hash_benchmarks SOURCES src/quiver/hash/hasher_benchmark.cc)
  add_benchmark(hashtable_benchmarks
                SOURCES src/quiver/hashtable/hashtable_benchmark.cc)
  add_benchmark(row_benchmarks SOURCES src/quiver/row/row_benchmarks.cc)
endif()

if(QUIVER_CODE_COVERAGE)
  setup_target_for_coverage_gcovr_html(
    NAME
    coverage
    EXECUTABLE
    ctest
    -j
    ${PROCESSOR_COUNT}
    DEPENDENCIES
    test_test
    EXCLUDE
    "build/*")
endif()

if(QUIVER_INSTALL_CPP)
  install(
    TARGETS quiver_static
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib/static
    PUBLIC_HEADER DESTINATION include/quiver)
endif()
