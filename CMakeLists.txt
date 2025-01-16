cmake_minimum_required(VERSION 3.10)
project(mabain)

# Set the installation directory
if(NOT DEFINED MABAIN_INSTALL_DIR)
    set(MABAIN_INSTALL_DIR /usr/local)
endif()

# Option to enable __DEBUG__ definition
option(ENABLE_DEBUG "Enable __DEBUG__ definition" OFF)

# Include directories
include_directories(${CMAKE_SOURCE_DIR}/src)

# Add subdirectories
add_subdirectory(src)
add_subdirectory(binaries)

# Install headers
install(FILES src/db.h src/mb_data.h src/mabain_consts.h src/lock.h src/error.h src/integer_4b_5b.h
        DESTINATION ${MABAIN_INSTALL_DIR}/include/mabain)

# Install shared library
install(FILES ${CMAKE_BINARY_DIR}/lib/libmabain.so DESTINATION ${MABAIN_INSTALL_DIR}/lib)

# Install binaries
install(FILES ${CMAKE_BINARY_DIR}/bin/mbc DESTINATION ${MABAIN_INSTALL_DIR}/bin)

# Custom target for uninstall
add_custom_target(uninstall
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${MABAIN_INSTALL_DIR}/include/mabain
    COMMAND ${CMAKE_COMMAND} -E remove ${MABAIN_INSTALL_DIR}/lib/libmabain.so
    COMMAND ${CMAKE_COMMAND} -E remove ${MABAIN_INSTALL_DIR}/bin/mbc
)

# Custom target for clean_all
add_custom_target(clean_all
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target clean
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}/doc
    COMMAND ${CMAKE_COMMAND} -E remove ${CMAKE_BINARY_DIR}/tags
)

# Custom target for distclean
add_custom_target(distclean
    DEPENDS clean_all
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}/doc
    COMMAND ${CMAKE_COMMAND} -E remove ${CMAKE_BINARY_DIR}/tags
)

# Custom target for index
add_custom_target(index
    COMMAND ctags -R *
    COMMAND doxygen doxygen.conf
    COMMAND ${CMAKE_COMMAND} -E echo "Generating documentation..."
    COMMAND ${CMAKE_COMMAND} -E echo "Use following index file : "
    COMMAND readlink -f doc/html/index.html
    COMMAND ${CMAKE_COMMAND} -E echo "Done with doxygen"
)

# Custom target for unit tests
add_custom_target(unit-test
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target build
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target unit-test
)