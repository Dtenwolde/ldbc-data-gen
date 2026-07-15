# Updating DuckDB

This extension uses DuckDB's internal C++ API, which is not stable across commits. The main CI workflow therefore builds against an explicit DuckDB commit rather than the moving `main` branch.

To update DuckDB:

1. Choose and test a DuckDB commit deliberately.
2. Update the `duckdb` submodule to that commit.
3. Put the same full commit SHA in `.github/duckdb-version`. The submodule and CI pin must stay aligned.
4. Update `extension-ci-tools` only when its workflow or build contract requires it. Main CI currently consumes its `main` workflows.
5. Build and run `make test` and `make format-check` locally.
6. Run the SF1 checksum and BI-query parity command from the root README before merging.

Treat deprecation warnings as advance notice, but do not adopt a replacement API until the pinned DuckDB commit supports it and parity still passes. Useful upstream references are DuckDB's [release notes](https://github.com/duckdb/duckdb/releases), [core-extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions), and the history of the affected header.
