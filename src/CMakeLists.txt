# Define the library
file(GLOB SOURCES "*.cpp" "util/*.cpp" "detail/*.cpp")
file(GLOB HEADERS "*.h" "util/*.h" "detail/*.h")

find_path(JEMALLOC_INCLUDE_DIR jemalloc/jemalloc.h PATHS /usr/include /usr/local/include)
find_library(JEMALLOC_LIBRARY jemalloc PATHS /usr/lib /usr/local/lib)

if(NOT JEMALLOC_INCLUDE_DIR OR NOT JEMALLOC_LIBRARY)
    message(FATAL_ERROR "jemalloc not found")
endif()

add_library(mabain SHARED ${SOURCES})

# Specify include directories for this target
target_include_directories(mabain PUBLIC ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/src/util ${JEMALLOC_INCLUDE_DIR})

# Always enable traversal step guard in find loops
target_compile_definitions(mabain PRIVATE MB_ENABLE_TRAVERSAL_LIMIT)

# Set compile options
option(MB_WERROR "Treat warnings as errors" OFF)
set(MB_WARN_OPTS -Wall -Wwrite-strings -Wsign-compare -Wcast-align -Wformat-security -fdiagnostics-show-option)
if(MB_WERROR)
    list(APPEND MB_WARN_OPTS -Werror)
endif()
target_compile_options(mabain PRIVATE
    ${MB_WARN_OPTS}
    -g -ggdb -fPIC -O3 -std=c++17 -D__LOCK_FREE__
    -march=native -fomit-frame-pointer
)

# Link-time optimization (optional)
option(ENABLE_IPO "Enable interprocedural optimization" OFF)
if(ENABLE_IPO)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT lto_supported OUTPUT lto_output)
  if(lto_supported)
    set_property(TARGET mabain PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
  endif()
endif()

# Conditionally add __DEBUG__ definition
if(ENABLE_DEBUG)
    target_compile_definitions(mabain PRIVATE __DEBUG__)
endif()

# Link libraries
target_link_libraries(mabain pthread ${JEMALLOC_LIBRARY})

# Specify the library output directory
set_target_properties(mabain PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
)
cmake_minimum_required(VERSION 3.13)
project(mabain CXX)
cmake_policy(SET CMP0069 NEW)
