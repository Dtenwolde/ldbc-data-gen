cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME ldbc_data_gen)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/generated)
include_directories(duckdb/extension/icu/third_party/icu/common)
include_directories(duckdb/extension/icu/third_party/icu/i18n)
include_directories(duckdb/extension/parquet/include)
include_directories(duckdb/third_party/parquet)
include_directories(duckdb/third_party/thrift)
include_directories(duckdb/third_party/snappy)
include_directories(duckdb/third_party/lz4)
include_directories(duckdb/third_party/brotli/include)

set(LDBC_BI_QUERY_DEFINITIONS "")
foreach(QUERY_NUMBER RANGE 1 20)
  set(QUERY_PATH "${CMAKE_CURRENT_SOURCE_DIR}/src/queries/bi-${QUERY_NUMBER}.sql")
  set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${QUERY_PATH}")
  file(READ "${QUERY_PATH}" QUERY_TEXT)
  string(APPEND LDBC_BI_QUERY_DEFINITIONS
         "    R\"LDBCBI(${QUERY_TEXT})LDBCBI\",\n")
endforeach()
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/generated")
configure_file(src/include/ldbc_bi_queries_generated.hpp.in
               "${CMAKE_CURRENT_BINARY_DIR}/generated/ldbc_bi_queries_generated.hpp" @ONLY)

set(EXTENSION_SOURCES
    src/ldbc_data_gen_extension.cpp
    src/ldbc_bi_queries.cpp
    src/ldbc_java_random.cpp
    src/ldbc_datagen_config.cpp
    src/ldbc_unicode.cpp
    src/ldbc_person_dictionaries.cpp
    src/ldbc_person_generator.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} "$<LINK_ONLY:parquet_extension>")
target_link_libraries(${LOADABLE_EXTENSION_NAME} "$<LINK_ONLY:parquet_extension>")
set(LDBC_ICU_OBJECT_TARGETS duckdb_icu_common duckdb_icu_common_unity duckdb_icu_i18n duckdb_icu_i18n_unity
                            duckdb_icu_stubdata)
foreach(ICU_OBJECT_TARGET IN LISTS LDBC_ICU_OBJECT_TARGETS)
  if(TARGET ${ICU_OBJECT_TARGET})
    target_sources(${LOADABLE_EXTENSION_NAME} PRIVATE $<TARGET_OBJECTS:${ICU_OBJECT_TARGET}>)
  endif()
endforeach()
if(NOT TARGET duckdb_icu_common)
  target_link_libraries(
    ${LOADABLE_EXTENSION_NAME}
    "$<LINK_ONLY:duckdb_icu_common>"
    "$<LINK_ONLY:duckdb_icu_i18n>"
    "$<LINK_ONLY:duckdb_icu_stubdata>")
else()
  unset(LDBC_ICU_OBJECT_TARGETS)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
