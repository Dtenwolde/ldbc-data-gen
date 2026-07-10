# DuckDB LDBC SNB Datagen

This extension is intended to generate LDBC SNB datasets with DuckDB instead of Spark.

This is an independent project and is not affiliated with, endorsed by, or maintained by the Linked Data Benchmark Council (LDBC).

The pinned source of truth for the first milestone is the Spark-based LDBC SNB Datagen:

```text
third_party/ldbc_snb_datagen_spark
```

The initial target is BI static output parity. Incremental update generation and the Interactive workload are intentionally out of scope until the static BI parity harness is in place.

## Current API

The extension currently registers the north-star table-function contract:

```sql
LOAD ldbc_data_gen;

CALL ldbcgen(
    sf := 1.0,
    target := 'tables',
    schema := 'main',
    format := 'parquet',
    overwrite := false,
    dictionary_dir := 'third_party/ldbc_snb_datagen_spark/src/main/resources/dictionaries'
);
```

The current implementation validates parameters and generates the BI static initial snapshot relations either as DuckDB tables or as files.

Returned columns:

| Column | Type | Meaning |
| --- | --- | --- |
| `relation_name` | `VARCHAR` | Generated relation or logical artifact name |
| `path` | `VARCHAR` | Output schema for table generation, or output path for file generation |
| `row_count` | `BIGINT` | Generated row count |
| `checksum` | `VARCHAR` | Stable content checksum, nullable while planning |
| `format` | `VARCHAR` | `parquet` or `csv` |
| `status` | `VARCHAR` | `created` or `recreated` |

The default generation target is in-database DuckDB tables. File output is explicit:

```sql
CALL ldbcgen(
    sf := 1.0,
    target := 'files',
    output_dir := 'out/sf1',
    format := 'parquet'
);
```

File output writes one file per BI static relation under `<output_dir>/static/` and `<output_dir>/dynamic/`. Supported formats are `parquet` and `csv`; CSV output includes a header row. Use `overwrite := true` to replace existing files.

Schema-only metadata is available through:

```sql
SELECT *
FROM ldbcgen_schema(format := 'parquet')
ORDER BY relation_name, column_index;
```

This returns one row per BI static snapshot column, including `relation_name`, `entity_path`, `kind`, `snapshot_path`, `column_index`, `column_name`, `logical_type`, `nullable`, and `primary_key`.

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

Static and bulk-load data is written under `initial_snapshot/`. Incremental `inserts/` and `deletes/` exist in the Spark generator, but are deferred here.

The committed first-pass schema inventory lives in:

```text
test/fixtures/ldbc_snb_bi_static_schema.json
```

## Parity Harness

Run the fixture checker:

```sh
python3 tools/validate_ldbc_fixture.py test/fixtures/ldbc_snb_bi_static_schema.json
```

This check is Spark-free and CI-safe. It validates the pinned upstream commit metadata, relation inventory shape, primary keys, initial snapshot paths, and column definitions. Later generator chunks should extend this harness with row counts, stable checksums, and referential-integrity checks against generated `sf=0.003` Spark reference output.

Generated data should stay out of git. Use ignored directories such as `out/`, `generated/`, or `reference_data/`.

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

## Building And Testing

Initialize submodules:

```sh
git submodule update --init --recursive
```

Build and test the extension:

```sh
make
make test
```
