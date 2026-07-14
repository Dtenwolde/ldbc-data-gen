#!/usr/bin/env python3
"""Load a Spark composite-merged-fk initial snapshot into PostgreSQL for BI validation."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import BinaryIO


TYPE_MAPPING = {
    "INTEGER": "BIGINT",
    "BIGINT": "BIGINT",
    "VARCHAR": "TEXT",
    "DATE": "DATE",
    "TIMESTAMP_MS": "TIMESTAMPTZ",
}


def fail(message: str) -> None:
    raise SystemExit(f"error: {message}")


def validate_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        fail(f"invalid PostgreSQL identifier: {value!r}")
    return value


def run_psql(psql: Path, dsn: str, sql: str, stdin: BinaryIO | None = None) -> None:
    command = [
        str(psql),
        "--no-psqlrc",
        "--quiet",
        "--set",
        "ON_ERROR_STOP=1",
        "--dbname",
        dsn,
        "--command",
        sql,
    ]
    environment = dict(os.environ)
    existing_options = environment.get("PGOPTIONS", "")
    environment["PGOPTIONS"] = f"{existing_options} -c timezone=UTC".strip()
    if stdin is None:
        returncode = subprocess.run(command, env=environment).returncode
    else:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, env=environment)
        assert process.stdin is not None
        shutil.copyfileobj(stdin, process.stdin)
        process.stdin.close()
        returncode = process.wait()
    if returncode != 0:
        fail(f"psql failed with exit code {returncode}")


def table_ddl(schema: str, relation: dict) -> str:
    columns = []
    for column in relation["columns"]:
        logical_type = column["type"]
        if logical_type not in TYPE_MAPPING:
            fail(f"unsupported type {logical_type!r} in {relation['name']}")
        columns.append(f"{validate_identifier(column['name'])} {TYPE_MAPPING[logical_type]}")
    return f"CREATE TABLE {schema}.{validate_identifier(relation['name'])} ({', '.join(columns)});"


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def export_parquet_relation(duckdb: Path, relation_path: Path, csv_path: Path) -> None:
    parquet_glob = relation_path / "*.parquet"
    sql = (
        f"COPY (SELECT * FROM read_parquet({sql_string(parquet_glob)}, union_by_name = true)) "
        f"TO {sql_string(csv_path)} (FORMAT csv, HEADER true, DELIMITER '|', NULL '');"
    )
    result = subprocess.run([str(duckdb), "-batch", "-c", sql])
    if result.returncode != 0:
        fail(f"DuckDB Parquet conversion failed for {relation_path}")


def load_relation(
    psql: Path,
    dsn: str,
    schema: str,
    snapshot: Path,
    relation: dict,
    input_format: str,
    duckdb: Path,
    conversion_dir: Path,
) -> None:
    relation_path = snapshot / relation["entity_path"]
    if input_format == "parquet":
        parquet_files = sorted(relation_path.glob("*.parquet"))
        if not parquet_files:
            fail(f"no Parquet files found under {relation_path}")
        converted_path = conversion_dir / f"{relation['name']}.csv"
        print(f"converting {relation['name']} from Parquet", file=sys.stderr)
        export_parquet_relation(duckdb, relation_path, converted_path)
        files = [converted_path]
    else:
        files = sorted(relation_path.glob("*.csv")) + sorted(relation_path.glob("*.csv.gz"))
    if not files and input_format == "csv":
        fail(f"no CSV files found under {relation_path}")
    table = f"{schema}.{validate_identifier(relation['name'])}"
    copy_sql = f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER '|', NULL '');"
    for csv_path in files:
        print(f"loading {relation['name']}: {csv_path.name}", file=sys.stderr)
        opener = gzip.open if csv_path.suffix == ".gz" else Path.open
        with opener(csv_path, "rb") as csv_file:
            run_psql(psql, dsn, copy_sql, stdin=csv_file)


def main(argv: list[str]) -> int:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--psql", type=Path, default=Path("psql"))
    parser.add_argument("--dsn", default="postgresql://postgres:postgres@localhost:5432/postgres")
    parser.add_argument("--schema", default="ldbc_bi_sf1")
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv")
    parser.add_argument("--duckdb", type=Path, default=root / "build/release/duckdb")
    parser.add_argument(
        "--fixture",
        type=Path,
        default=root / "test/fixtures/ldbc_snb_bi_static_schema.json",
    )
    parser.add_argument(
        "--prepare-sql",
        type=Path,
        default=root / "tools/postgres/prepare_ldbc_bi.sql",
    )
    args = parser.parse_args(argv)

    schema = validate_identifier(args.schema)
    fixture = json.loads(args.fixture.read_text(encoding="utf-8"))
    relations = fixture["relations"]
    ddl = [f"DROP SCHEMA IF EXISTS {schema} CASCADE;", f"CREATE SCHEMA {schema};"]
    ddl.extend(table_ddl(schema, relation) for relation in relations)
    run_psql(args.psql, args.dsn, "\n".join(ddl))

    with tempfile.TemporaryDirectory(prefix="ldbc-postgres-load-") as conversion_tmp:
        conversion_dir = Path(conversion_tmp)
        for relation in relations:
            load_relation(
                args.psql,
                args.dsn,
                schema,
                args.snapshot,
                relation,
                args.format,
                args.duckdb,
                conversion_dir,
            )

    prepare_sql = args.prepare_sql.read_text(encoding="utf-8").replace("@SCHEMA@", schema)
    run_psql(args.psql, args.dsn, prepare_sql)
    print(f"loaded {len(relations)} relations into PostgreSQL schema {schema}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
