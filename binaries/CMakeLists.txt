# Define the executable
file(GLOB BINARY_SOURCES "*.cpp")

add_executable(mbc ${BINARY_SOURCES})

# Link the executable to the mabain library
target_link_libraries(mbc mabain readline)

# Specify the executable output directory
set_target_properties(mbc PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# Install the executable with execution permissions
install(TARGETS mbc
    RUNTIME DESTINATION ${MABAIN_INSTALL_DIR}/bin
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
)