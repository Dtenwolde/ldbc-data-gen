#!/usr/bin/env python3
"""Validate the committed LDBC SNB BI static schema fixture."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


VALID_TYPES = {"BIGINT", "INTEGER", "VARCHAR", "DATE", "TIMESTAMP_MS"}
VALID_KINDS = {"static_node", "dynamic_node", "dynamic_edge"}


def fail(message: str) -> None:
    raise SystemExit(f"error: {message}")


def load_fixture(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        fail(f"{path}: invalid JSON: {exc}")


def git_head(path: Path) -> str | None:
    try:
        return subprocess.check_output(
            ["git", "-C", str(path), "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def validate_source(fixture: dict, root: Path) -> None:
    source = fixture.get("source")
    if not isinstance(source, dict):
        fail("source must be an object")

    submodule_path = source.get("submodule_path")
    commit = source.get("commit")
    if not submodule_path or not commit:
        fail("source.submodule_path and source.commit are required")

    submodule = root / submodule_path
    if submodule.exists():
        head = git_head(submodule)
        if head and head != commit:
            fail(f"{submodule_path} is at {head}, expected {commit}")


def validate_relations(fixture: dict) -> None:
    relations = fixture.get("relations")
    if not isinstance(relations, list) or not relations:
        fail("relations must be a non-empty list")

    names: set[str] = set()
    entity_paths: set[str] = set()
    for relation in relations:
        name = relation.get("name")
        entity_path = relation.get("entity_path")
        kind = relation.get("kind")
        primary_key = relation.get("primary_key")
        columns = relation.get("columns")

        if not name or name in names:
            fail(f"relation name is missing or duplicated: {name!r}")
        names.add(name)

        if not entity_path or entity_path in entity_paths:
            fail(f"{name}: entity_path is missing or duplicated")
        if not (entity_path.startswith("static/") or entity_path.startswith("dynamic/")):
            fail(f"{name}: entity_path must start with static/ or dynamic/")
        entity_paths.add(entity_path)

        if kind not in VALID_KINDS:
            fail(f"{name}: invalid kind {kind!r}")
        if not isinstance(primary_key, list) or not primary_key:
            fail(f"{name}: primary_key must be a non-empty list")
        if not isinstance(columns, list) or not columns:
            fail(f"{name}: columns must be a non-empty list")

        column_names: set[str] = set()
        for column in columns:
            column_name = column.get("name")
            column_type = column.get("type")
            if not column_name or column_name in column_names:
                fail(f"{name}: column name is missing or duplicated: {column_name!r}")
            if column_type not in VALID_TYPES:
                fail(f"{name}.{column_name}: invalid type {column_type!r}")
            if not isinstance(column.get("nullable"), bool):
                fail(f"{name}.{column_name}: nullable must be boolean")
            column_names.add(column_name)

        for key in primary_key:
            if key not in column_names:
                fail(f"{name}: primary key column {key!r} is not in columns")


def validate_layout(fixture: dict) -> None:
    if fixture.get("mode") != "bi":
        fail("mode must be bi")
    if fixture.get("layout") != "composite-merged-fk":
        fail("layout must be composite-merged-fk")
    if "{format}" not in fixture.get("snapshot_root", ""):
        fail("snapshot_root must contain {format}")
    if fixture.get("first_reference_scale_factor") != 0.003:
        fail("first_reference_scale_factor must be 0.003")


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: validate_ldbc_fixture.py <fixture.json>", file=sys.stderr)
        return 2

    fixture_path = Path(argv[1]).resolve()
    root = fixture_path.parents[2]
    fixture = load_fixture(fixture_path)
    validate_layout(fixture)
    validate_source(fixture, root)
    validate_relations(fixture)
    print(f"validated {len(fixture['relations'])} relations from {fixture_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
