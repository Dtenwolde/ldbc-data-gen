#!/usr/bin/env python3
"""Generate and compare canonical LDBC BI query result fixtures."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


QUERY_VARIANTS = [
    "1",
    "2a",
    "2b",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8a",
    "8b",
    "9",
    "10a",
    "10b",
    "11",
    "12",
    "13",
    "14a",
    "14b",
    "15a",
    "15b",
    "16a",
    "16b",
    "17",
    "18",
    "19a",
    "19b",
    "20a",
    "20b",
]

# The names and types are from ldbc/ldbc_snb_bi common/result_mapping.py.
RESULT_MAPPING: dict[int, list[tuple[str, str]]] = {
    1: [("year", "INT32"), ("isComment", "BOOL"), ("lengthCategory", "INT32"),
        ("messageCount", "INT32"), ("averageMessageLength", "FLOAT32"),
        ("sumMessageLength", "INT32"), ("percentageOfMessages", "FLOAT32")],
    2: [("tag.name", "STRING"), ("countWindow1", "INT32"), ("countWindow2", "INT32"),
        ("diff", "INT32")],
    3: [("forum.id", "ID"), ("forum.title", "STRING"), ("forum.creationDate", "DATETIME"),
        ("person.id", "ID"), ("messageCount", "INT32")],
    4: [("person.id", "ID"), ("person.firstName", "STRING"), ("person.lastName", "STRING"),
        ("person.creationDate", "DATETIME"), ("messageCount", "INT32")],
    5: [("person.id", "ID"), ("replyCount", "INT32"), ("likeCount", "INT32"),
        ("messageCount", "INT32"), ("score", "INT32")],
    6: [("person1.id", "ID"), ("authorityScore", "INT32")],
    7: [("relatedTag.name", "STRING"), ("count", "INT32")],
    8: [("person.id", "ID"), ("score", "INT32"), ("friendsScore", "INT32")],
    9: [("person.id", "ID"), ("person.firstName", "STRING"), ("person.lastName", "STRING"),
        ("threadCount", "INT32"), ("messageCount", "INT32")],
    10: [("expertCandidatePerson.id", "ID"), ("tag.name", "STRING"), ("messageCount", "INT32")],
    11: [("count", "INT64")],
    12: [("messageCount", "INT32"), ("personCount", "INT32")],
    13: [("zombie.id", "ID"), ("zombieLikeCount", "INT32"), ("totalLikeCount", "INT32"),
         ("zombieScore", "FLOAT32")],
    14: [("person1.id", "ID"), ("person2.id", "ID"), ("city1.name", "STRING"),
         ("score", "INT32")],
    15: [("weight", "FLOAT32")],
    16: [("person.id", "ID"), ("messageCountA", "INT32"), ("messageCountB", "INT32")],
    17: [("person1.id", "ID"), ("messageCount", "INT32")],
    18: [("person1.id", "ID"), ("person2.id", "ID"), ("mutualFriendCount", "INT32")],
    19: [("person1.id", "ID"), ("person2.id", "ID"), ("totalWeight", "FLOAT32")],
    20: [("person1.id", "ID"), ("totalWeight", "INT64")],
}

# Official parameter name, pragma parameter name, and DuckDB literal type.
PRAGMA_PARAMETERS: dict[int, list[tuple[str, str, str]]] = {
    1: [("datetime", "datetime", "TIMESTAMP")],
    2: [("date", "date", "TIMESTAMP"), ("tagClass", "tag_class", "STRING")],
    3: [("tagClass", "tag_class", "STRING"), ("country", "country", "STRING")],
    4: [("date", "date", "TIMESTAMP")],
    5: [("tag", "tag", "STRING")],
    6: [("tag", "tag", "STRING")],
    7: [("tag", "tag", "STRING")],
    8: [("tag", "tag", "STRING"), ("startDate", "start_date", "TIMESTAMP"),
        ("endDate", "end_date", "TIMESTAMP")],
    9: [("startDate", "start_date", "TIMESTAMP"), ("endDate", "end_date", "TIMESTAMP")],
    10: [("personId", "person_id", "BIGINT"), ("country", "country", "STRING"),
         ("tagClass", "tag_class", "STRING")],
    11: [("country", "country", "STRING"), ("startDate", "start_date", "TIMESTAMP"),
         ("endDate", "end_date", "TIMESTAMP")],
    12: [("startDate", "start_date", "TIMESTAMP"),
         ("lengthThreshold", "length_threshold", "INT"), ("languages", "languages", "STRING[]")],
    13: [("country", "country", "STRING"), ("endDate", "end_date", "TIMESTAMP")],
    14: [("country1", "country1", "STRING"), ("country2", "country2", "STRING")],
    15: [("person1Id", "person1_id", "BIGINT"), ("person2Id", "person2_id", "BIGINT"),
         ("startDate", "start_date", "TIMESTAMP"), ("endDate", "end_date", "TIMESTAMP")],
    16: [("tagA", "tag_a", "STRING"), ("dateA", "date_a", "DATE"),
         ("tagB", "tag_b", "STRING"), ("dateB", "date_b", "DATE"),
         ("maxKnowsLimit", "max_knows_limit", "INT")],
    17: [("tag", "tag", "STRING"), ("delta", "delta", "INT")],
    18: [("tag", "tag", "STRING")],
    19: [("city1Id", "city1_id", "BIGINT"), ("city2Id", "city2_id", "BIGINT")],
    20: [("person2Id", "person2_id", "BIGINT"), ("company", "company", "STRING")],
}

VALIDATION_METADATA = {
    "scale_factor": 1,
    "spark_datagen_revision": "b3dc986898efac7c1676abba865a30865334922e",
    "ldbc_bi_revision": "47dd38b40844ecdb0e42e5a610c369535304786d",
    "ci_postgres_image": "postgres:16.14-bookworm",
    "spark_dataset_url": "https://datasets.ldbcouncil.org/snb-bi/bi-sf1-composite-merged-fk.tar.zst",
    "spark_dataset_sha256": "a72938e244e6aa9d99632fcd5065e50c669ecf4d00f60bd162b266df4a7aba13",
    "parameters_url": "https://datasets.ldbcouncil.org/snb-bi/ldbc-snb-bi-parameters-sf1-to-sf30000.zip",
    "parameters_sha256": "5552f840729afa2b29e1f78a6bc58c987d081d22e077543b17ee7767d1f74445",
}


def fail(message: str) -> None:
    raise SystemExit(f"error: {message}")


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def query_number(variant: str) -> int:
    return int(variant.rstrip("ab"))


def read_parameter_cases(parameter_dir: Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for variant in QUERY_VARIANTS:
        parameter_path = parameter_dir / f"bi-{variant}.csv"
        if not parameter_path.exists():
            fail(f"missing parameter file: {parameter_path}")
        with parameter_path.open(newline="", encoding="utf-8") as parameter_file:
            row = next(csv.DictReader(parameter_file, delimiter="|"), None)
        if row is None:
            fail(f"parameter file has no data rows: {parameter_path}")
        parameters = {key.split(":", 1)[0]: value for key, value in row.items()}
        cases.append({"query_nr": query_number(variant), "variant": variant, "parameters": parameters})
    return cases


def load_fixture(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_fixture(path: Path, fixture: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(fixture, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def parameter_literal(value: str, literal_type: str) -> str:
    if literal_type in {"INT", "BIGINT"}:
        return str(int(value))
    if literal_type == "STRING":
        return sql_string(value)
    if literal_type == "STRING[]":
        return "[" + ", ".join(sql_string(element) for element in value.split(";")) + "]"
    if literal_type == "DATE":
        return f"DATE {sql_string(value)}"
    if literal_type == "TIMESTAMP":
        timestamp = value.removesuffix("+00:00").replace("T", " ")
        return f"TIMESTAMP {sql_string(timestamp)}"
    fail(f"unsupported parameter type: {literal_type}")


def pragma_sql(query_nr: int, schema: str, parameters: dict[str, str]) -> str:
    arguments = [str(query_nr), f"schema = {sql_string(schema)}"]
    for official_name, pragma_name, literal_type in PRAGMA_PARAMETERS[query_nr]:
        if official_name not in parameters:
            fail(f"Q{query_nr} is missing parameter {official_name!r}")
        arguments.append(f"{pragma_name} = {parameter_literal(parameters[official_name], literal_type)}")
    return "PRAGMA ldbc_bi(" + ", ".join(arguments) + ");"


def normalize_datetime(value: str) -> str:
    parsed = dt.datetime.fromisoformat(value.replace(" ", "T").replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return parsed.isoformat(timespec="milliseconds") + "+00:00"


def normalize_value(value: str, result_type: str) -> Any:
    if result_type in {"ID", "INT", "INT32", "INT64"}:
        return int(value)
    if result_type in {"FLOAT", "FLOAT32", "FLOAT64"}:
        return float(value)
    if result_type == "BOOL":
        lowered = value.lower()
        if lowered not in {"true", "false", "t", "f"}:
            fail(f"unexpected boolean result: {value!r}")
        return lowered in {"true", "t"}
    if result_type == "DATETIME":
        return normalize_datetime(value)
    if result_type == "DATE":
        return dt.date.fromisoformat(value).isoformat()
    if result_type == "STRING":
        return value
    fail(f"unsupported result type: {result_type}")


def run_duckdb_case(
    duckdb: Path,
    extension: Path,
    database: Path,
    schema: str,
    case: dict[str, Any],
) -> list[dict[str, Any]]:
    query_nr = int(case["query_nr"])
    sql = f"LOAD {sql_string(extension)}; {pragma_sql(query_nr, schema, case['parameters'])}"
    command = [str(duckdb), "-csv", "-header", str(database), "-c", sql]
    result = subprocess.run(command, text=True, capture_output=True)
    if result.returncode != 0:
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        fail(f"DuckDB Q{case['variant']} failed with exit code {result.returncode}")

    csv_rows = list(csv.reader(result.stdout.splitlines()))
    if not csv_rows:
        fail(f"DuckDB Q{case['variant']} returned no header")
    mapping = RESULT_MAPPING[query_nr]
    if len(csv_rows[0]) != len(mapping):
        fail(
            f"DuckDB Q{case['variant']} returned {len(csv_rows[0])} columns; "
            f"expected {len(mapping)}"
        )
    rows: list[dict[str, Any]] = []
    for csv_row in csv_rows[1:]:
        if len(csv_row) != len(mapping):
            fail(f"DuckDB Q{case['variant']} returned a malformed row: {csv_row!r}")
        rows.append({
            name: normalize_value(csv_row[index], result_type)
            for index, (name, result_type) in enumerate(mapping)
        })
    return rows


def build_duckdb_fixture(
    duckdb: Path,
    extension: Path,
    database: Path,
    schema: str,
    cases: list[dict[str, Any]],
) -> dict[str, Any]:
    actual_cases: list[dict[str, Any]] = []
    for case in cases:
        print(f"running DuckDB Q{case['variant']}", file=sys.stderr)
        actual_case = {
            "query_nr": int(case["query_nr"]),
            "variant": case["variant"],
            "parameters": case["parameters"],
            "results": run_duckdb_case(duckdb, extension, database, schema, case),
        }
        actual_cases.append(actual_case)
    metadata = dict(VALIDATION_METADATA)
    metadata["result_source"] = "DuckDB"
    return {"metadata": metadata, "cases": actual_cases}


def postgres_parameter_literal(value: str, literal_type: str) -> str:
    if literal_type == "INT":
        return str(int(value))
    if literal_type == "BIGINT":
        return f"{int(value)}::bigint"
    if literal_type == "STRING":
        return sql_string(value)
    if literal_type == "STRING[]":
        return "(" + ", ".join(sql_string(element) for element in value.split(";")) + ")"
    if literal_type == "DATE":
        return f"{sql_string(value)}::date"
    if literal_type == "TIMESTAMP":
        timestamp = value.removesuffix("+00:00").replace("T", " ")
        return f"{sql_string(timestamp)}::timestamp"
    fail(f"unsupported PostgreSQL parameter type: {literal_type}")


def postgres_query(query_path: Path, query_nr: int, parameters: dict[str, str]) -> str:
    query = query_path.read_text(encoding="utf-8")
    if query_nr == 11:
        # PostgreSQL otherwise materializes this three-times-referenced CTE. At
        # SF1 that turns an indexed triangle lookup into a multi-minute join.
        query = query.replace(
            "Persons_of_country_w_friends AS (",
            "Persons_of_country_w_friends AS NOT MATERIALIZED (",
        )
    for official_name, _, literal_type in PRAGMA_PARAMETERS[query_nr]:
        if official_name not in parameters:
            fail(f"Q{query_nr} is missing parameter {official_name!r}")
        query = query.replace(
            f":{official_name}", postgres_parameter_literal(parameters[official_name], literal_type)
        )
    return query


def run_postgres_case(
    psql: Path,
    dsn: str,
    schema: str,
    queries: Path,
    case: dict[str, Any],
) -> list[dict[str, Any]]:
    query_nr = int(case["query_nr"])
    sql = postgres_query(queries / f"bi-{query_nr}.sql", query_nr, case["parameters"])
    command = [
        str(psql),
        "--no-psqlrc",
        "--quiet",
        "--csv",
        "--tuples-only",
        "--set",
        "ON_ERROR_STOP=1",
        "--dbname",
        dsn,
        "--command",
        sql,
    ]
    environment = dict(os.environ)
    existing_options = environment.get("PGOPTIONS", "")
    environment["PGOPTIONS"] = (
        f"{existing_options} -c search_path={schema} -c timezone=UTC"
    ).strip()
    result = subprocess.run(command, text=True, capture_output=True, env=environment)
    if result.returncode != 0:
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        fail(f"PostgreSQL Q{case['variant']} failed with exit code {result.returncode}")

    mapping = RESULT_MAPPING[query_nr]
    rows: list[dict[str, Any]] = []
    for csv_row in csv.reader(result.stdout.splitlines()):
        if len(csv_row) != len(mapping):
            fail(f"PostgreSQL Q{case['variant']} returned a malformed row: {csv_row!r}")
        rows.append({
            name: normalize_value(csv_row[index], result_type)
            for index, (name, result_type) in enumerate(mapping)
        })
    return rows


def build_postgres_fixture(
    psql: Path,
    dsn: str,
    schema: str,
    queries: Path,
    cases: list[dict[str, Any]],
) -> dict[str, Any]:
    actual_cases: list[dict[str, Any]] = []
    for case in cases:
        print(f"running PostgreSQL Q{case['variant']}", file=sys.stderr)
        start = time.monotonic()
        actual_cases.append({
            "query_nr": int(case["query_nr"]),
            "variant": case["variant"],
            "parameters": case["parameters"],
            "results": run_postgres_case(psql, dsn, schema, queries, case),
        })
        print(f"PostgreSQL Q{case['variant']} completed in {time.monotonic() - start:.2f}s", file=sys.stderr)
    metadata = dict(VALIDATION_METADATA)
    metadata["result_source"] = "PostgreSQL"
    metadata["postgres_version"] = postgres_server_version(psql, dsn)
    return {"metadata": metadata, "cases": actual_cases}


def postgres_server_version(psql: Path, dsn: str) -> str:
    command = [
        str(psql),
        "--no-psqlrc",
        "--quiet",
        "--tuples-only",
        "--no-align",
        "--set",
        "ON_ERROR_STOP=1",
        "--dbname",
        dsn,
        "--command",
        "SHOW server_version;",
    ]
    result = subprocess.run(command, text=True, capture_output=True)
    if result.returncode != 0:
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        fail(f"could not determine PostgreSQL version (exit code {result.returncode})")
    return result.stdout.strip()


def compare_value(expected: Any, actual: Any, path: str, failures: list[str]) -> None:
    if isinstance(expected, bool) or isinstance(actual, bool):
        if expected is not actual:
            failures.append(f"{path}: expected {expected!r}, got {actual!r}")
        return
    if isinstance(expected, float) or isinstance(actual, float):
        if not isinstance(expected, (int, float)) or not isinstance(actual, (int, float)):
            failures.append(f"{path}: expected {expected!r}, got {actual!r}")
        elif not math.isclose(float(expected), float(actual), rel_tol=0.0, abs_tol=1e-5):
            failures.append(f"{path}: expected {expected!r}, got {actual!r}")
        return
    if type(expected) is not type(actual):
        failures.append(f"{path}: expected {expected!r}, got {actual!r}")
        return
    if isinstance(expected, dict):
        if set(expected) != set(actual):
            failures.append(f"{path}: expected keys {sorted(expected)}, got {sorted(actual)}")
            return
        for key in expected:
            compare_value(expected[key], actual[key], f"{path}.{key}", failures)
        return
    if isinstance(expected, list):
        if len(expected) != len(actual):
            failures.append(f"{path}: expected {len(expected)} rows/items, got {len(actual)}")
            return
        for index, (expected_item, actual_item) in enumerate(zip(expected, actual)):
            compare_value(expected_item, actual_item, f"{path}[{index}]", failures)
        return
    if expected != actual:
        failures.append(f"{path}: expected {expected!r}, got {actual!r}")


def compare_fixtures(expected: dict[str, Any], actual: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    expected_cases = {case["variant"]: case for case in expected["cases"]}
    actual_cases = {case["variant"]: case for case in actual["cases"]}
    if set(expected_cases) != set(actual_cases):
        failures.append(
            f"variants: expected {sorted(expected_cases)}, got {sorted(actual_cases)}"
        )
        return failures
    for variant in QUERY_VARIANTS:
        expected_case = expected_cases[variant]
        actual_case = actual_cases[variant]
        compare_value(expected_case["query_nr"], actual_case["query_nr"], f"Q{variant}.query_nr", failures)
        compare_value(expected_case["parameters"], actual_case["parameters"], f"Q{variant}.parameters", failures)
        compare_value(expected_case["results"], actual_case["results"], f"Q{variant}.results", failures)
    return failures


def assert_fixtures_match(expected: dict[str, Any], actual: dict[str, Any]) -> None:
    failures = compare_fixtures(expected, actual)
    if failures:
        displayed = failures[:50]
        suffix = "" if len(failures) <= 50 else f"\n  ... {len(failures) - 50} more"
        fail("BI query result mismatch:\n  " + "\n  ".join(displayed) + suffix)


def default_duckdb(root: Path) -> Path:
    return root / "build/release/duckdb"


def default_extension(root: Path) -> Path:
    return root / "build/release/extension/ldbc_data_gen/ldbc_data_gen.duckdb_extension"


def main(argv: list[str]) -> int:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    duckdb_parser = subparsers.add_parser("duckdb", help="run DuckDB BI queries and write a result fixture")
    duckdb_parser.add_argument("--duckdb", type=Path, default=default_duckdb(root))
    duckdb_parser.add_argument("--extension", type=Path, default=default_extension(root))
    duckdb_parser.add_argument("--database", type=Path, required=True)
    duckdb_parser.add_argument("--schema", required=True)
    case_source = duckdb_parser.add_mutually_exclusive_group(required=True)
    case_source.add_argument("--parameters", type=Path, help="official parameters-sf1 directory")
    case_source.add_argument("--fixture", type=Path, help="existing fixture whose cases supply parameters")
    duckdb_parser.add_argument("--output", type=Path, required=True)

    postgres_parser = subparsers.add_parser("postgres", help="run PostgreSQL BI queries and write a fixture")
    postgres_parser.add_argument("--psql", type=Path, default=Path("psql"))
    postgres_parser.add_argument("--dsn", default="postgresql://postgres:postgres@localhost:5432/postgres")
    postgres_parser.add_argument("--schema", default="ldbc_bi_sf1")
    postgres_parser.add_argument("--queries", type=Path, required=True)
    postgres_case_source = postgres_parser.add_mutually_exclusive_group(required=True)
    postgres_case_source.add_argument("--parameters", type=Path)
    postgres_case_source.add_argument("--fixture", type=Path)
    postgres_parser.add_argument("--output", type=Path, required=True)

    compare_parser = subparsers.add_parser("compare", help="compare two result fixtures")
    compare_parser.add_argument("--expected", type=Path, required=True)
    compare_parser.add_argument("--actual", type=Path, required=True)

    args = parser.parse_args(argv)
    if args.command == "postgres":
        if args.parameters:
            cases = read_parameter_cases(args.parameters)
        else:
            cases = load_fixture(args.fixture)["cases"]
        fixture = build_postgres_fixture(args.psql, args.dsn, args.schema, args.queries, cases)
        write_fixture(args.output, fixture)
        return 0
    if args.command == "compare":
        assert_fixtures_match(load_fixture(args.expected), load_fixture(args.actual))
        print(f"verified BI query results against {args.expected}", file=sys.stderr)
        return 0

    if not args.duckdb.exists():
        fail(f"DuckDB binary not found: {args.duckdb}")
    if not args.extension.exists():
        fail(f"extension not found: {args.extension}; run make release first")
    if args.parameters:
        cases = read_parameter_cases(args.parameters)
    else:
        cases = load_fixture(args.fixture)["cases"]
    fixture = build_duckdb_fixture(args.duckdb, args.extension, args.database, args.schema, cases)
    write_fixture(args.output, fixture)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
