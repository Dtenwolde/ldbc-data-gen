#!/usr/bin/env python3
"""Compare DuckDB-generated forum relations with Spark reference output.

This is a focused debugging harness: it prints the usual row-count/checksum
summary and, for mismatches, the first generated-only and reference-only rows.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import tempfile
from pathlib import Path


FORUM_RELATIONS = {"Forum", "Forum_hasMember_Person", "Forum_hasTag_Tag"}
NULL_MARKER = "<LDBC_NULL>"
FIELD_SEP = "\\x1f"
ROW_SEP = "\\x1e"


def fail(message: str) -> None:
    raise SystemExit(f"error: {message}")


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def load_forum_relations(fixture_path: Path) -> list[dict]:
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    relations = [relation for relation in fixture["relations"] if relation["name"] in FORUM_RELATIONS]
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


def row_expr(relation: dict, source_alias: str = "") -> str:
    pieces: list[str] = []
    prefix = f"{source_alias}." if source_alias else ""
    for idx, column in enumerate(relation["columns"]):
        if idx:
            pieces.append(f"'{FIELD_SEP}'")
        name = quote_ident(column["name"])
        pieces.append(f"coalesce(cast({prefix}{name} as varchar), '{NULL_MARKER}')")
    return " || ".join(pieces)


def order_by_expr(relation: dict, source_alias: str = "") -> str:
    prefix = f"{source_alias}." if source_alias else ""
    return ", ".join(f"{prefix}{quote_ident(column)}" for column in relation["primary_key"])


def canonical_expr(relation: dict, source: str) -> str:
    expr = row_expr(relation)
    order_by = order_by_expr(relation)
    return (
        "SELECT count(*) AS row_count, "
        f"md5(coalesce(string_agg({expr}, '{ROW_SEP}' ORDER BY {order_by}), '')) AS checksum "
        f"FROM {source}"
    )


def relation_summary(duckdb: Path, database: Path, relation: dict, source: str) -> tuple[int, str]:
    rows = run_duckdb(duckdb, database, canonical_expr(relation, source))
    if len(rows) != 1 or len(rows[0]) != 2:
        fail(f"unexpected checksum result for {relation['name']}: {rows!r}")
    return int(rows[0][0]), rows[0][1]


def create_reference_view(duckdb: Path, database: Path, reference_root: Path, relation: dict, fmt: str) -> None:
    path = reference_root / relation["entity_path"]
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
    run_duckdb(duckdb, database, f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {scan};")


def first_diff_sql(relation: dict, left_source: str, right_source: str) -> str:
    left_columns = ", ".join(f"l.{quote_ident(column['name'])}" for column in relation["columns"])
    left_keys = " AND ".join(
        f"l.{quote_ident(column)} IS NOT DISTINCT FROM r.{quote_ident(column)}" for column in relation["primary_key"]
    )
    null_check = " AND ".join(f"r.{quote_ident(column)} IS NULL" for column in relation["primary_key"])
    return (
        f"SELECT {left_columns} "
        f"FROM {left_source} l "
        f"LEFT JOIN {right_source} r ON {left_keys} "
        f"WHERE {null_check} "
        f"ORDER BY {order_by_expr(relation, 'l')} "
        "LIMIT 1"
    )


def print_diff(writer: csv.writer, duckdb: Path, database: Path, relation: dict, left: str, right: str, label: str) -> None:
    rows = run_duckdb(duckdb, database, first_diff_sql(relation, left, right))
    if rows:
        writer.writerow([relation["name"], label, *rows[0]])


def main(argv: list[str]) -> int:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", type=Path, default=default_duckdb(root))
    parser.add_argument("--extension", type=Path, default=default_extension(root))
    parser.add_argument("--fixture", type=Path, default=root / "test/fixtures/ldbc_snb_bi_static_schema.json")
    parser.add_argument(
        "--reference",
        type=Path,
        default=root / "reference_data/sf0.003/graphs/parquet/bi/composite-merged-fk/initial_snapshot",
        help="Spark initial_snapshot root.",
    )
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    parser.add_argument("--schema", default="ldbc_forum_parity")
    parser.add_argument("--sf", type=float, default=0.003)
    args = parser.parse_args(argv)

    if not args.duckdb.exists():
        fail(f"DuckDB binary not found: {args.duckdb}")
    if not args.extension.exists():
        fail(f"extension not found: {args.extension}; run make first")

    relations = load_forum_relations(args.fixture)
    rows: list[tuple[str, int, str, int, str, bool]] = []
    diffs: list[tuple[dict, str, str]] = []

    with tempfile.TemporaryDirectory(prefix="ldbc-forum-parity-") as tmpdir:
        database = Path(tmpdir) / "parity.duckdb"
        setup_sql = (
            f"LOAD {sql_string(args.extension)}; "
            f"CALL ldbcgen(sf := {args.sf}, schema := {sql_string(args.schema)}, overwrite := true);"
        )
        run_duckdb(args.duckdb, database, setup_sql)

        for relation in relations:
            generated_source = f"{args.schema}.{quote_ident(relation['name'])}"
            reference_source = f"ref_{relation['name']}"
            create_reference_view(args.duckdb, database, args.reference, relation, args.format)
            generated_count, generated_checksum = relation_summary(args.duckdb, database, relation, generated_source)
            reference_count, reference_checksum = relation_summary(args.duckdb, database, relation, reference_source)
            matches = generated_count == reference_count and generated_checksum == reference_checksum
            rows.append((relation["name"], generated_count, generated_checksum, reference_count, reference_checksum, matches))
            if not matches:
                diffs.append((relation, generated_source, reference_source))

        writer = csv.writer(sys.stdout)
        writer.writerow(["relation", "generated_rows", "generated_checksum", "reference_rows", "reference_checksum", "match"])
        for row in rows:
            writer.writerow(row)

        if diffs:
            writer.writerow([])
            writer.writerow(["relation", "side", *[column["name"] for column in diffs[0][0]["columns"]]])
            for relation, generated_source, reference_source in diffs:
                print_diff(writer, args.duckdb, database, relation, generated_source, reference_source, "generated_only")
                print_diff(writer, args.duckdb, database, relation, reference_source, generated_source, "reference_only")

    return 1 if diffs else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
