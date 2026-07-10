#!/usr/bin/env python3
"""Generate LDBC data and print row counts/checksums for every relation."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import tempfile
from pathlib import Path


NULL_MARKER = "<LDBC_NULL>"
FIELD_SEP = "\\x1f"
ROW_SEP = "\\x1e"


def fail(message: str) -> None:
    raise SystemExit(f"error: {message}")


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def default_duckdb(root: Path) -> Path:
    return root / "build/release/duckdb"


def default_extension(root: Path) -> Path:
    return root / "build/release/extension/ldbc_data_gen/ldbc_data_gen.duckdb_extension"


def load_relations(fixture_path: Path) -> list[dict]:
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    relations = list(fixture["relations"])
    relations.sort(key=lambda relation: relation["name"])
    return relations


def run_duckdb(duckdb: Path, database: Path, sql: str) -> list[list[str]]:
    command = [str(duckdb), "-csv", "-noheader", str(database), "-c", sql]
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stdout)
        sys.stderr.write(exc.stderr)
        raise
    return list(csv.reader(result.stdout.splitlines()))


def canonical_expr(relation: dict, source: str) -> str:
    pieces: list[str] = []
    for idx, column in enumerate(relation["columns"]):
        if idx:
            pieces.append(f"'{FIELD_SEP}'")
        name = quote_ident(column["name"])
        pieces.append(f"coalesce(cast({name} as varchar), '{NULL_MARKER}')")
    row_expr = " || ".join(pieces)
    order_by = ", ".join(quote_ident(column) for column in relation["primary_key"])
    return (
        "SELECT count(*) AS row_count, "
        f"md5(coalesce(string_agg({row_expr}, '{ROW_SEP}' ORDER BY {order_by}), '')) AS checksum "
        f"FROM {source}"
    )


def relation_summary(duckdb: Path, database: Path, relation: dict, source: str) -> tuple[int, str]:
    rows = run_duckdb(duckdb, database, canonical_expr(relation, source))
    if len(rows) != 1 or len(rows[0]) != 2:
        fail(f"unexpected checksum result for {relation['name']}: {rows!r}")
    return int(rows[0][0]), rows[0][1]


def main(argv: list[str]) -> int:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", type=Path, default=default_duckdb(root))
    parser.add_argument("--extension", type=Path, default=default_extension(root))
    parser.add_argument("--fixture", type=Path, default=root / "test/fixtures/ldbc_snb_bi_static_schema.json")
    parser.add_argument("--schema", default="ldbc_generated_checksums")
    parser.add_argument("--sf", type=float, default=0.003)
    parser.add_argument("--threads", type=int, default=1)
    args = parser.parse_args(argv)

    if not args.duckdb.exists():
        fail(f"DuckDB binary not found: {args.duckdb}")
    if not args.extension.exists():
        fail(f"extension not found: {args.extension}; run make first")

    relations = load_relations(args.fixture)
    with tempfile.TemporaryDirectory(prefix="ldbc-generated-checksums-") as tmpdir:
        database = Path(tmpdir) / "checksums.duckdb"
        setup_sql = (
            f"LOAD {sql_string(args.extension)}; "
            f"CALL ldbcgen(sf := {args.sf}, schema := {sql_string(args.schema)}, "
            f"threads := {args.threads}, overwrite := true);"
        )
        run_duckdb(args.duckdb, database, setup_sql)

        writer = csv.writer(sys.stdout)
        writer.writerow(["relation", "rows", "checksum"])
        for relation in relations:
            source = f"{quote_ident(args.schema)}.{quote_ident(relation['name'])}"
            row_count, checksum = relation_summary(args.duckdb, database, relation, source)
            writer.writerow([relation["name"], row_count, checksum])

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
