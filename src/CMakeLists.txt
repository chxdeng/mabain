# Define the library
file(GLOB SOURCES "*.cpp" "util/*.cpp")
file(GLOB HEADERS "*.h" "util/*.h")

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
target_compile_options(mabain PRIVATE
    -Wall -Werror -Wwrite-strings -Wsign-compare -Wcast-align -Wformat-security
    -fdiagnostics-show-option -g -ggdb -fPIC -O3 -std=c++17 -D__LOCK_FREE__
    -march=native -fomit-frame-pointer
)

# Link-time optimization can improve inlining across TUs
include(CheckIPOSupported)
check_ipo_supported(RESULT lto_supported OUTPUT lto_output)
if(lto_supported)
  set_property(TARGET mabain PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
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
