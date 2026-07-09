# DuckDB LDBC SNB Datagen

This extension is intended to generate LDBC SNB datasets with DuckDB instead of Spark.

The pinned source of truth for the first milestone is the Spark-based LDBC SNB Datagen:

```text
third_party/ldbc_snb_datagen_spark
```

The initial target is BI static output parity. Incremental update generation and the Interactive workload are intentionally out of scope until the static BI parity harness is in place.

## Current API

The extension currently registers the north-star table-function contract:

```sql
LOAD ldbc_data_gen;

SELECT *
FROM ldbcgen(
    sf := 1.0,
    output_dir := 'out/sf1',
    format := 'parquet',
    overwrite := false
);
```

The current implementation validates parameters and returns one `planned` metadata row. It does not generate data yet.

Returned columns:

| Column | Type | Meaning |
| --- | --- | --- |
| `relation_name` | `VARCHAR` | Generated relation or logical artifact name |
| `path` | `VARCHAR` | Output path |
| `row_count` | `BIGINT` | Generated row count, nullable while planning |
| `checksum` | `VARCHAR` | Stable content checksum, nullable while planning |
| `format` | `VARCHAR` | `parquet` or `csv` |
| `status` | `VARCHAR` | Current generation status |

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
