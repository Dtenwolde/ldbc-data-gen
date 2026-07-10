# DuckDB LDBC SNB Datagen

This extension is intended to generate LDBC SNB datasets with DuckDB instead of Spark.

This is an independent project and is not affiliated with, endorsed by, or maintained by the Linked Data Benchmark Council (LDBC).

The pinned source of truth for the first milestone is the Spark-based LDBC SNB Datagen:

```text
third_party/ldbc_snb_datagen_spark
```

The current target is Spark-compatible SNB BI output for the default `composite-merged-fk` layout. The Interactive workload remains out of scope.

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

The current implementation validates parameters and generates the BI initial snapshot relations plus BI insert and delete batches either as DuckDB tables or as files.

Returned columns:

| Column | Type | Meaning |
| --- | --- | --- |
| `relation_name` | `VARCHAR` | Generated relation or logical artifact name |
| `path` | `VARCHAR` | Output schema for table generation, or output path for file generation |
| `row_count` | `BIGINT` | Generated row count |
| `checksum` | `VARCHAR` | Stable content checksum, nullable while planning |
| `format` | `VARCHAR` | `parquet` or `csv` |
| `status` | `VARCHAR` | `created` or `recreated` |

The default generation target is in-database DuckDB tables. It creates the 18 BI initial snapshot relations using their BI relation names, and dynamic update relations using `inserts_<relation>` and `deletes_<relation>` table names with lowercase relation suffixes, for example `inserts_post` and `deletes_person_likes_comment`.

File output is explicit:

```sql
CALL ldbcgen(
    sf := 1.0,
    target := 'files',
    output_dir := 'out/sf1',
    format := 'parquet'
);
```

File output writes one file per BI static relation under `<output_dir>/static/` and `<output_dir>/dynamic/`. It also writes the Spark-compatible BI layout under `<output_dir>/graphs/<format>/bi/composite-merged-fk/`, including `initial_snapshot`, dynamic `inserts`, and dynamic `deletes`. Supported formats are `parquet` and `csv`; CSV output includes a header row. Use `overwrite := true` to replace existing files.

Schema-only metadata is available through:

```sql
SELECT *
FROM ldbcgen_schema(format := 'parquet')
ORDER BY relation_name, operation, column_index;
```

This returns one row per BI column, including `relation_name`, `entity_path`, `kind`, `operation`, `snapshot_path`, `column_index`, `column_name`, `logical_type`, `nullable`, and `primary_key`. `operation` is one of `initial_snapshot`, `inserts`, or `deletes`.

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
