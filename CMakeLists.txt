# SPDX-License-Identifier: Apache-2.0

message(STATUS "Building using CMake version: ${CMAKE_VERSION}")
cmake_minimum_required(VERSION 3.16)

set(QUIVER_VERSION "0.1.0-SNAPSHOT")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" QUIVER_BASE_VERSION "${QUIVER_VERSION}")
project(quiver VERSION "${QUIVER_BASE_VERSION}")

set(QUIVER_VERSION_MAJOR "${quiver_VERSION_MAJOR}")
set(QUIVER_VERSION_MINOR "${quiver_VERSION_MINOR}")
set(QUIVER_VERSION_PATCH "${quiver_VERSION_PATCH}")

option(QUIVER_BUILD_TESTS "Build tests" OFF)
option(QUIVER_BUILD_BENCHMARKS "Build benchmarks" OFF)
option(QUIVER_BUNDLE "Create bundled quiver.h and quiver.c" OFF)
option(QUIVER_BUILD_ID "A string identifying this build" OFF)

if (NOT QUIVER_BUILD_ID)
    string(RANDOM LENGTH 10 QUIVER_BUILD_ID)
endif()

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

if(QUIVER_BUILD_TESTS)
    find_package(GTest CONFIG REQUIRED)
    find_package(Arrow CONFIG REQUIRED)
    enable_testing()
endif()

if(QUIVER_BUILD_BENCHMARKS)
    find_package(Arrow CONFIG REQUIRED)
    find_package(benchmark CONFIG REQUIRED)
endif()

# Have to enable clang-tidy after building 3rd party libs
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(
    quiver
    STATIC
    src/quiver/lib.cc
    src/quiver/core/array.cc
    src/quiver/core/io.cc
    src/quiver/hash/hasher.cc
    src/quiver/map/equality.cc
    src/quiver/row/row_p.cc
    src/quiver/store/mmap_store.cc
    src/quiver/util/arrow_util.cc
    src/quiver/util/cpu_info.cc
    src/quiver/util/logging_p.cc
    src/quiver/util/memory.cc
    src/quiver/util/memory_pool.cc
    src/quiver/util/status.cc
    src/quiver/util/string_builder.cc)

set_target_properties(quiver PROPERTIES VERSION ${QUIVER_VERSION})
set_target_properties(quiver PROPERTIES SOVERSION ${QUIVER_VERSION_MAJOR})
set_target_properties(quiver PROPERTIES PUBLIC_HEADER quiver/include/lib.h)

target_include_directories(quiver PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_include_directories(quiver PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/generated)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
        target_compile_options(
            quiver
            PRIVATE
            -Wall
            -Werror
            -Wextra
            -Wpedantic
            -Wunused-result)
    elseif(CMAKE_C_COMPILER_ID STREQUAL "AppleClang" OR
           CMAKE_C_COMPILER_ID STREQUAL "Clang")
        target_compile_options(
            quiver
            PRIVATE
            -Wall
            -Werror
            -Wextra
            -Wdocumentation
            -Wshorten-64-to-32)
    endif()
    if(QUIVER_CODE_COVERAGE)
        append_coverage_compiler_flags_to_target(quiver)
    endif()
# else()
#     if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
#     target_compile_options(
#         quiver
#         PRIVATE
#         -march=native)
#     elseif(CMAKE_C_COMPILER_ID STREQUAL "AppleClang" OR
#     CMAKE_C_COMPILER_ID STREQUAL "Clang")
#     target_compile_options(
#         quiver
#         PRIVATE
#         -march=native)
#     endif()
endif()

if(QUIVER_BUILD_TESTS)
    add_executable(
        core_test
        src/quiver/core/array_test.cc
        src/quiver/testutil/test_util.cc)
    add_executable(
        hash_test
        src/quiver/hash/hasher_test.cc
        src/quiver/testutil/gen.cc
        src/quiver/testutil/test_util.cc)
    add_executable(
        map_test
        src/quiver/map/equality_test.cc
        src/quiver/testutil/gen.cc
        src/quiver/testutil/test_util.cc)
    add_executable(
        row_test
        src/quiver/row/row_test.cc
        src/quiver/testutil/gen.cc
        src/quiver/testutil/test_util.cc)
    add_executable(
        store_test
        src/quiver/store/mmap_store_test.cc)
    add_executable(
        utility_test
        src/quiver/util/arrow_util_test.cc
        src/quiver/util/cpu_info_test.cc
        src/quiver/util/memory_test.cc)
    
    target_link_libraries(core_test PRIVATE quiver Arrow::arrow_shared GTest::gtest_main)
    target_link_libraries(hash_test PRIVATE quiver Arrow::arrow_shared GTest::gtest_main)
    target_link_libraries(map_test PRIVATE quiver Arrow::arrow_shared GTest::gtest_main)
    target_link_libraries(row_test PRIVATE quiver Arrow::arrow_shared GTest::gtest_main)
    target_link_libraries(store_test PRIVATE quiver GTest::gtest_main)
    target_link_libraries(utility_test PRIVATE quiver GTest::gtest_main)

    include(GoogleTest)
    gtest_discover_tests(core_test)
    gtest_discover_tests(hash_test)
    gtest_discover_tests(map_test)
    gtest_discover_tests(row_test)
    gtest_discover_tests(store_test)
    gtest_discover_tests(utility_test)
endif()

if(QUIVER_BUILD_BENCHMARKS)

    add_library(
        quiver_benchutil
        STATIC
        src/quiver/benchutil/datagen.cc)

    target_include_directories(quiver_benchutil PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(quiver_benchutil PRIVATE Arrow::arrow_shared quiver)

    add_executable(row_benchmarks src/quiver/row/row_benchmarks.cc)

    target_link_libraries(row_benchmarks quiver quiver_benchutil Arrow::arrow_shared benchmark::benchmark)
endif()

if(QUIVER_CODE_COVERAGE)
    setup_target_for_coverage_gcovr_html(
        NAME coverage
        EXECUTABLE ctest -j ${PROCESSOR_COUNT}
        DEPENDENCIES test_test
        EXCLUDE "build/*"
    )
endif()

install(TARGETS quiver DESTINATION lib)
install(DIRECTORY src/ DESTINATION include FILES_MATCHING PATTERN "*.h")
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/generated/build_id.h DESTINATION include/quiver)
