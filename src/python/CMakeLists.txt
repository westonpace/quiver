find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(pyquiver pyquiver.cc)

target_include_directories(pyquiver PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(
  pyquiver
  PRIVATE
  quiver
)

install(TARGETS pyquiver
        LIBRARY DESTINATION .)
