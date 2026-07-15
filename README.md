# DuckDB LDBC SNB Datagen

This extension is intended to generate LDBC SNB datasets with DuckDB instead of Spark.

This is an independent project and is not affiliated with, endorsed by, or maintained by the Linked Data Benchmark Council (LDBC).

The pinned source of truth for the first milestone is the Spark-based LDBC SNB Datagen:

```text
third_party/ldbc_snb_datagen_spark
```

The current target is Spark-compatible SNB BI output for the default `composite-merged-fk` layout. The Interactive workload remains out of scope.

## Quick Start

Build the extension and matching DuckDB shell from the repository root:

```sh
git submodule update --init --recursive
make
```

Open a persistent database with the repository build:

```sh
build/release/duckdb ldbc.duckdb
```

Then load the extension and generate a small development dataset:

```sql
LOAD ldbc_data_gen;

CALL ldbcgen(
    sf := 0.003,
    schema := 'ldbc',
    overwrite := true
);

SELECT count(*) AS persons
FROM ldbc.Person;
```

The repository's DuckDB binary has the extension statically available, so `LOAD ldbc_data_gen` is sufficient. To use another compatible DuckDB build, start its shell with `-unsigned` and load the compiled extension explicitly:

```sh
duckdb -unsigned ldbc.duckdb
```

```sql
LOAD '/absolute/path/to/ldbc-data-gen/build/release/extension/ldbc_data_gen/ldbc_data_gen.duckdb_extension';
```

DuckDB extensions are version- and platform-specific. Using `build/release/duckdb`, built from the pinned DuckDB submodule alongside the extension, avoids compatibility problems.

Run these examples from the repository root. The default dictionary path is relative to the process working directory; pass an absolute `dictionary_dir` when running elsewhere.

## Generating DuckDB Tables

Table output is the default. This creates the BI relations in the `ldbc` schema of the current database:

```sql
CALL ldbcgen(
    sf := 1,
    schema := 'ldbc',
    overwrite := true
);
```

The generator creates the 18 BI initial-snapshot tables using their BI relation names. Dynamic updates use `inserts_<relation>` and `deletes_<relation>` table names with lowercase relation suffixes, for example `inserts_post` and `deletes_person_likes_comment`.

Primary-key constraints are disabled by default because building them adds noticeable load time. Set `primary_keys := true` when the constraints are useful for the intended workload. If the destination already exists, generation fails unless `overwrite := true` is supplied.

Supported scale factors are `0.003`, `0.1`, `0.3`, `1`, `3`, `10`, `30`, `100`, `300`, `1000`, `3000`, `10000`, and `30000`. Scale factor `0.003` is intended for quick development and validation runs.

By default, generation uses DuckDB's configured thread count. Set DuckDB's global `threads` setting or pass `threads := N` to `ldbcgen` to cap generator parallelism.

## Generating Spark-Compatible Files

Set `target := 'files'` and provide an output directory:

```sql
CALL ldbcgen(
    sf := 1,
    target := 'files',
    output_dir := 'out/sf1',
    format := 'parquet',
    overwrite := true
);
```

This writes the Spark-compatible BI layout under `<output_dir>/graphs/<format>/bi/composite-merged-fk/`, including `initial_snapshot`, dynamic `inserts`, and dynamic `deletes`. Supported formats are `parquet` and `csv`; CSV output includes a header row.

Parquet snapshots are streamed directly to numbered part files for generated person/forum relations. Other relations and CSV output pass through a UUID-named temporary disk-backed DuckDB database that is removed on success or failure. Large generations therefore need temporary disk headroom in addition to the final output size. A returned `path` can identify either one file or a relation directory containing part files.

Generated data should stay out of git. The repository ignores `out/`, `generated/`, and `reference_data/`.

## API Reference

`ldbcgen` generates the BI initial snapshot plus BI insert and delete batches as DuckDB tables or Spark-compatible files. It accepts only named parameters:

| Parameter | Default | Meaning |
| --- | --- | --- |
| `sf` | `1.0` | Supported scale factor listed above |
| `catalog` | Current catalog | Catalog used for table output |
| `output_dir` | Empty | Required destination when `target := 'files'`; relative paths use the process working directory |
| `target` | `'tables'` | Generate DuckDB `tables` or Spark-compatible `files` |
| `schema` | Current schema | Schema used for table output |
| `format` | `'parquet'` | File format for `target := 'files'`: `parquet` or `csv` |
| `dictionary_dir` | Pinned Spark dictionary directory | Source dictionaries; relative paths use the process working directory |
| `threads` | DuckDB thread count | Generator worker count; must be greater than zero |
| `overwrite` | `false` | Replace an existing table or file destination |
| `primary_keys` | `false` | Add primary-key constraints to generated DuckDB tables; ignored for file output |

Returned columns:

| Column | Type | Meaning |
| --- | --- | --- |
| `relation_name` | `VARCHAR` | Generated relation or logical artifact name |
| `path` | `VARCHAR` | Qualified table name, or output file/directory path |
| `row_count` | `BIGINT` | Generated row count |
| `checksum` | `VARCHAR` | Reserved for checksum reporting; currently `NULL` |
| `format` | `VARCHAR` | `table`, `parquet`, or `csv` |
| `status` | `VARCHAR` | `created` or `recreated` |

Schema-only metadata is available through:

```sql
SELECT *
FROM ldbcgen_schema(format := 'parquet')
ORDER BY relation_name, operation, column_index;
```

This returns one row per BI column, including `relation_name`, `entity_path`, `kind`, `operation`, `snapshot_path`, `column_index`, `column_name`, `logical_type`, `nullable`, and `primary_key`. `operation` is one of `initial_snapshot`, `inserts`, or `deletes`.

Inspect the resolved generator configuration for a scale factor with:

```sql
SELECT parameter, value, logical_type, source
FROM ldbcgen_config(sf := 1)
ORDER BY parameter;
```

## Performance

An SF100 BI Parquet dataset completes with the default DuckDB thread count on a 36 GB laptop:

| SF | Threads | Wall time | Peak RSS | Output | Parquet files |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 18 (default) | 2:40 | 10.7 GiB | 20 GiB | 9,517 |

This local release-build result was measured on an 18-core Apple M5 Max with DuckDB `v1.6.0-dev10569`, using the current working tree based on extension commit `308909b`. Unrelated builds were active on the host, so the elapsed time is an indicative capability result rather than a controlled comparison. SF1 file output matched all 18 canonical relation checksums after the same direct-chunk change. See [the performance notes](docs/performance.md) for the command, measurement details, and historical results.

## BI Queries

The extension includes all 20 LDBC SNB BI queries. They can be run with a
TPC-H-style pragma against a table-output schema produced by `ldbcgen`:

```sql
PRAGMA ldbc_bi(
    1,
    schema = 'ldbc',
    datetime = TIMESTAMP '2011-12-01 00:00:00'
);
```

The optional `catalog` and `schema` arguments default to the current catalog
and schema. Named query parameters use snake case. The catalog lists each
query, its required parameters, and the underlying SQL:

```sql
SELECT query_nr, name, parameters
FROM ldbc_bi_queries()
ORDER BY query_nr;
```

The pragma derives the reference implementation's merged `Message`, message
tag/like, place subtype, organisation subtype, and query precomputation
relations as inline CTEs. No preparation step or additional persistent tables
are required.

The query SQL is adapted from the official
[LDBC SNB BI reference implementation](https://github.com/ldbc/ldbc_snb_bi/tree/main/umbra/queries)
under the Apache License 2.0.

## Reference Scope

The Spark BI writer stores output under:

```text
graphs/<format>/bi/<layout>/
```

For the default non-exploded layout this is:

```text
graphs/parquet/bi/composite-merged-fk/
graphs/csv/bi/composite-merged-fk/
```

Static and bulk-load data is written under `initial_snapshot/`. File output also writes dynamic `inserts/` and the Spark-default explicit `deletes/` relations.

The committed first-pass schema inventory lives in:

```text
test/fixtures/ldbc_snb_bi_static_schema.json
```

## Parity Harness

Run the fixture checker:

```sh
python3 tools/validate_ldbc_fixture.py test/fixtures/ldbc_snb_bi_static_schema.json
```

This check is Spark-free and CI-safe. It validates the pinned upstream commit metadata, relation inventory shape, primary keys, initial snapshot paths, and column definitions.

Generate static DuckDB parity checksums:

```sh
python3 tools/static_parity.py
```

Compare against Spark reference output:

```sh
python3 tools/static_parity.py \
  --reference reference_data/sf0.003/graphs/parquet/bi/composite-merged-fk/initial_snapshot
```

Compare currently implemented dynamic relations against Spark reference output:

```sh
python3 tools/dynamic_parity.py
```

This currently covers `Person`, `Person_hasInterest_Tag`, `Person_studyAt_University`,
`Person_workAt_Company`, and `Person_knows_Person`.

Compare all BI initial snapshot relations against Spark reference output:

```sh
python3 tools/bi_snapshot_parity.py
```

Compare BI insert/delete batches against Spark reference output:

```sh
python3 tools/bi_update_parity.py
```

Generate the pinned Spark reference output locally:

```sh
python3 tools/generate_spark_reference.py --dry-run
python3 tools/generate_spark_reference.py
```

This wraps `third_party/ldbc_snb_datagen_spark/tools/run.py` and defaults to `sf=0.003`, BI mode, Parquet, one Spark partition, and `reference_data/sf0.003`. The Spark generator requires Java 8 or 11, `sbt`, and Spark 3.2.x. If Spark is not installed, the wrapper can use the upstream helper to download Spark 3.2.2 into `${HOME}`:

```sh
python3 tools/generate_spark_reference.py --download-spark
```

The expected BI reference root for parity scripts is:

```text
reference_data/sf0.003/graphs/parquet/bi/composite-merged-fk/initial_snapshot
```

The SF1 CI gate also verifies the results of all 20 BI queries (28 parameter
variants) against a canonical PostgreSQL result fixture:

```sh
python3 tools/generated_checksums.py \
  --sf 1 \
  --threads 1 \
  --expected test/fixtures/ldbc_snb_bi_sf1_checksums.csv \
  --expected-bi-results test/fixtures/ldbc_snb_bi_sf1_results.json
```

The fixture was produced independently with PostgreSQL over the official
Spark SF1 `composite-merged-fk` dataset. See [the parity notes](docs/parity.md#sf1-bi-query-result-oracle)
for provenance, coverage, and regeneration instructions.

## Development

After completing the quick-start build, run the tests and formatting check with:

```sh
make test
make format-check
```

Before changing generation logic, also run the SF1 checksum and BI-result parity command shown above. CI repeats that check with one thread so deterministic output changes are explicit.
