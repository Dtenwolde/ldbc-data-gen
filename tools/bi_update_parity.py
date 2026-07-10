#!/usr/bin/env python3
"""Compare DuckDB-generated BI insert/delete batches with Spark reference output."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import tempfile
from pathlib import Path


DELETE_RELATIONS = {
    "Person",
    "Person_knows_Person",
    "Forum",
    "Forum_hasMember_Person",
    "Post",
    "Comment",
    "Person_likes_Post",
    "Person_likes_Comment",
}
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


def run_duckdb(duckdb: Path, database: Path, sql: str) -> list[list[str]]:
    command = [str(duckdb), "-csv", "-noheader", str(database), "-c", sql]
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stdout)
        sys.stderr.write(exc.stderr)
        raise
    return list(csv.reader(result.stdout.splitlines()))


def load_dynamic_relations(fixture_path: Path) -> list[dict]:
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    relations = [relation for relation in fixture["relations"] if relation["kind"] != "static_node"]
    relations.sort(key=lambda relation: relation["name"])
    return relations


def update_columns(relation: dict, operation: str) -> list[dict]:
    if operation == "inserts":
        return [*relation["columns"], {"name": "batch_id", "type": "DATE"}]
    return [
        {"name": "deletionDate", "type": "TIMESTAMP_MS"},
        *[column for column in relation["columns"] if column["name"] in relation["primary_key"]],
        {"name": "batch_id", "type": "DATE"},
    ]


def parquet_files(path: Path) -> list[Path]:
    return sorted(path.glob("**/*.parquet"))


def create_view(duckdb: Path, database: Path, view_name: str, root: Path, relation: dict, operation: str) -> None:
    path = root / operation / relation["entity_path"]
    columns = update_columns(relation, operation)
    if parquet_files(path):
        scan = f"read_parquet({sql_string(path / '**/*.parquet')}, union_by_name = true, hive_partitioning = true)"
        run_duckdb(duckdb, database, f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {scan};")
        return

    select_list = ", ".join(f"CAST(NULL AS {column['type']}) AS {quote_ident(column['name'])}" for column in columns)
    run_duckdb(duckdb, database, f"CREATE OR REPLACE VIEW {view_name} AS SELECT {select_list} WHERE false;")


def row_expr(columns: list[dict], source_alias: str = "") -> str:
    pieces: list[str] = []
    prefix = f"{source_alias}." if source_alias else ""
    for idx, column in enumerate(columns):
        if idx:
            pieces.append(f"'{FIELD_SEP}'")
        name = quote_ident(column["name"])
        pieces.append(f"coalesce(cast({prefix}{name} as varchar), '{NULL_MARKER}')")
    return " || ".join(pieces)


def order_columns(relation: dict, operation: str) -> list[str]:
    if operation == "inserts":
        return ["batch_id", "creationDate", *relation["primary_key"]]
    return ["batch_id", "deletionDate", *relation["primary_key"]]


def canonical_expr(relation: dict, operation: str, source: str) -> str:
    columns = update_columns(relation, operation)
    order_by = ", ".join(quote_ident(column) for column in order_columns(relation, operation))
    return (
        "SELECT count(*) AS row_count, "
        f"md5(coalesce(string_agg({row_expr(columns)}, '{ROW_SEP}' ORDER BY {order_by}), '')) AS checksum "
        f"FROM {source}"
    )


def relation_summary(duckdb: Path, database: Path, relation: dict, operation: str, source: str) -> tuple[int, str]:
    rows = run_duckdb(duckdb, database, canonical_expr(relation, operation, source))
    if len(rows) != 1 or len(rows[0]) != 2:
        fail(f"unexpected checksum result for {operation}/{relation['name']}: {rows!r}")
    return int(rows[0][0]), rows[0][1]


def main(argv: list[str]) -> int:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", type=Path, default=default_duckdb(root))
    parser.add_argument("--extension", type=Path, default=default_extension(root))
    parser.add_argument("--fixture", type=Path, default=root / "test/fixtures/ldbc_snb_bi_static_schema.json")
    parser.add_argument(
        "--reference-root",
        type=Path,
        default=root / "reference_data/sf0.003/graphs/parquet/bi/composite-merged-fk",
    )
    parser.add_argument("--sf", type=float, default=0.003)
    args = parser.parse_args(argv)

    if not args.duckdb.exists():
        fail(f"DuckDB binary not found: {args.duckdb}")
    if not args.extension.exists():
        fail(f"extension not found: {args.extension}; run make first")

    relations = load_dynamic_relations(args.fixture)
    rows: list[tuple[str, str, int, str, int, str, bool]] = []

    with tempfile.TemporaryDirectory(prefix="ldbc-bi-updates-") as tmpdir:
        tmp = Path(tmpdir)
        database = tmp / "parity.duckdb"
        generated_root = tmp / "generated" / "graphs/parquet/bi/composite-merged-fk"
        setup_sql = (
            f"LOAD {sql_string(args.extension)}; "
            f"CALL ldbcgen(sf := {args.sf}, target := 'files', "
            f"output_dir := {sql_string(tmp / 'generated')}, format := 'parquet', overwrite := true);"
        )
        run_duckdb(args.duckdb, database, setup_sql)

        for operation in ("inserts", "deletes"):
            for relation in relations:
                if operation == "deletes" and relation["name"] not in DELETE_RELATIONS:
                    continue
                generated_view = f"gen_{operation}_{relation['name']}"
                reference_view = f"ref_{operation}_{relation['name']}"
                create_view(args.duckdb, database, generated_view, generated_root, relation, operation)
                create_view(args.duckdb, database, reference_view, args.reference_root, relation, operation)
                generated_count, generated_checksum = relation_summary(
                    args.duckdb, database, relation, operation, generated_view
                )
                reference_count, reference_checksum = relation_summary(
                    args.duckdb, database, relation, operation, reference_view
                )
                rows.append(
                    (
                        operation,
                        relation["name"],
                        generated_count,
                        generated_checksum,
                        reference_count,
                        reference_checksum,
                        generated_count == reference_count and generated_checksum == reference_checksum,
                    )
                )

    writer = csv.writer(sys.stdout)
    writer.writerow(
        ["operation", "relation", "generated_rows", "generated_checksum", "reference_rows", "reference_checksum", "match"]
    )
    writer.writerows(rows)
    return 0 if all(row[-1] for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
