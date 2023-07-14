find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(_quiver quiver/_quiver.cc)

target_include_directories(_quiver PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(
  _quiver
  PRIVATE
  quiver_static
)

install(DIRECTORY "quiver/" 
        DESTINATION "quiver" 
        FILES_MATCHING PATTERN "*.py" )
install(TARGETS _quiver
        LIBRARY DESTINATION "quiver")
