cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME ldbc_data_gen)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)
include_directories(duckdb/extension/icu/third_party/icu/common)
include_directories(duckdb/extension/icu/third_party/icu/i18n)

set(EXTENSION_SOURCES
    src/ldbc_data_gen_extension.cpp
    src/ldbc_java_random.cpp
    src/ldbc_datagen_config.cpp
    src/ldbc_unicode.cpp
    src/ldbc_person_dictionaries.cpp
    src/ldbc_person_generator.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${LOADABLE_EXTENSION_NAME} "$<LINK_ONLY:duckdb_icu_common>" "$<LINK_ONLY:duckdb_icu_i18n>"
                      "$<LINK_ONLY:duckdb_icu_stubdata>")
target_sources(${LOADABLE_EXTENSION_NAME} PRIVATE $<TARGET_OBJECTS:duckdb_icu_common_unity>
                                                  $<TARGET_OBJECTS:duckdb_icu_i18n_unity>)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
