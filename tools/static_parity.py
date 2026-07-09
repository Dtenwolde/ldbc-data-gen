#!/usr/bin/env python3
"""Compare DuckDB-generated static LDBC tables with optional Spark reference output."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import tempfile
from pathlib import Path


STATIC_RELATIONS = {"Organisation", "Place", "Tag", "TagClass"}
NULL_MARKER = "<LDBC_NULL>"
FIELD_SEP = "\\x1f"
ROW_SEP = "\\x1e"


def fail(message: str) -> None:
    raise SystemExit(f"error: {message}")


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def load_static_relations(fixture_path: Path) -> list[dict]:
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    relations = [relation for relation in fixture["relations"] if relation["name"] in STATIC_RELATIONS]
    relations.sort(key=lambda relation: relation["name"])
    return relations


def default_duckdb(root: Path) -> Path:
    return root / "build/release/duckdb"


def default_extension(root: Path) -> Path:
    return root / "build/release/extension/ldbc_data_gen/ldbc_data_gen.duckdb_extension"


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
        name = column["name"]
        pieces.append(f"coalesce(cast({name} as varchar), '{NULL_MARKER}')")
    row_expr = " || ".join(pieces)
    order_by = ", ".join(relation["primary_key"])
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


def reference_path(reference_root: Path, relation: dict, fmt: str) -> Path:
    return reference_root / relation["entity_path"]


def create_reference_view(duckdb: Path, database: Path, reference_root: Path, relation: dict, fmt: str) -> None:
    path = reference_path(reference_root, relation, fmt)
    if not path.exists():
        fail(f"missing reference path for {relation['name']}: {path}")
    view_name = f"ref_{relation['name']}"
    if fmt == "parquet":
        pattern = path / "**/*.parquet"
        scan = f"read_parquet({sql_string(pattern)}, union_by_name = true)"
    else:
        pattern = path / "**/*.csv*"
        columns = ", ".join(f"'{column['name']}': '{column['type']}'" for column in relation["columns"])
        scan = f"read_csv({sql_string(pattern)}, delim = '|', header = true, columns = {{{columns}}})"
    run_duckdb(duckdb, database, f"CREATE OR REPLACE TEMP VIEW {view_name} AS SELECT * FROM {scan};")


def main(argv: list[str]) -> int:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", type=Path, default=default_duckdb(root))
    parser.add_argument("--extension", type=Path, default=default_extension(root))
    parser.add_argument(
        "--fixture",
        type=Path,
        default=root / "test/fixtures/ldbc_snb_bi_static_schema.json",
    )
    parser.add_argument(
        "--reference",
        type=Path,
        help="Spark initial_snapshot root, e.g. graphs/parquet/bi/composite-merged-fk/initial_snapshot",
    )
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    parser.add_argument("--schema", default="ldbc_static_parity")
    parser.add_argument("--sf", type=float, default=0.003)
    args = parser.parse_args(argv)

    if not args.duckdb.exists():
        fail(f"DuckDB binary not found: {args.duckdb}")
    if not args.extension.exists():
        fail(f"extension not found: {args.extension}; run make first")

    relations = load_static_relations(args.fixture)
    with tempfile.TemporaryDirectory(prefix="ldbc-static-parity-") as tmpdir:
        database = Path(tmpdir) / "parity.duckdb"
        setup_sql = (
            f"LOAD {sql_string(args.extension)}; "
            f"CALL ldbcgen(sf := {args.sf}, schema := {sql_string(args.schema)}, overwrite := true);"
        )
        run_duckdb(args.duckdb, database, setup_sql)

        rows: list[tuple[str, int, str, int | None, str | None, bool | None]] = []
        for relation in relations:
            generated_count, generated_checksum = relation_summary(
                args.duckdb, database, relation, f"{args.schema}.{relation['name']}"
            )
            reference_count = None
            reference_checksum = None
            matches = None
            if args.reference:
                create_reference_view(args.duckdb, database, args.reference, relation, args.format)
                reference_count, reference_checksum = relation_summary(
                    args.duckdb, database, relation, f"ref_{relation['name']}"
                )
                matches = generated_count == reference_count and generated_checksum == reference_checksum
            rows.append((relation["name"], generated_count, generated_checksum, reference_count, reference_checksum, matches))

    writer = csv.writer(sys.stdout)
    writer.writerow(["relation", "generated_rows", "generated_checksum", "reference_rows", "reference_checksum", "match"])
    for row in rows:
        writer.writerow(row)

    if args.reference and not all(row[-1] for row in rows):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
