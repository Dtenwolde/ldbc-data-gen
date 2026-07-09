# This file is included by DuckDB's build system. It specifies which extension to load

# ICU is built first so this extension can use its vendored C++ normalization APIs.
duckdb_extension_load(icu)

# Extension from this repo
duckdb_extension_load(ldbc_data_gen
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
