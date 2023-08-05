# Creates targets for the various clang tools if the clang tools are installed

# Adding clang-format target if executable is found
  find_program(CLANG_FORMAT_BINARY "clang-format")
  if (CLANG_FORMAT_BINARY)
    add_custom_target(
        clang-format
        COMMAND python scripts/run_clang_format.py
                --clang_format_binary ${CLANG_FORMAT_BINARY}
                --source_dir src
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    )
  else()
    message("Could not find clang-format binary, will not add clang-format target")
  endif()

  find_program(RUN_CLANG_TIDY_BINARY run-clang-tidy DOC "Path to run-clang-tidy")
  if (RUN_CLANG_TIDY_BINARY)
      add_custom_target(
        clang-tidy
        COMMAND ${RUN_CLANG_TIDY_BINARY} -header-filter=.*
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
        )
  else()
    message("Could not find run-clang-tidy.py, will not add clang-tidy target")
  endif()

  find_program(IWYU_TOOL_BINARY iwyu_tool.py DOC "Path to the include-what-you-use tool")
  if (IWYU_TOOL_BINARY)
    add_custom_target(
      iwyu
      COMMAND ${IWYU_TOOL_BINARY} -p . -- -Xiwyu --mapping_file=${PROJECT_SOURCE_DIR}/iwyu.json -Xiwyu --error
      WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
    )
  else()
    message("Could not find include-what-you-use binary, will not add IWYU support")
  endif()
