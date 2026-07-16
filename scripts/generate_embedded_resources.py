#!/usr/bin/env python3
import argparse
from pathlib import Path


CHUNK_SIZE = 16 * 1024
DELIMITER = "LDBCRESOURCE"


def raw_literal(data: str) -> str:
    chunks = []
    for offset in range(0, len(data), CHUNK_SIZE):
        chunk = data[offset : offset + CHUNK_SIZE]
        if f"){DELIMITER}\"" in chunk:
            raise ValueError("resource chunk contains the raw string delimiter")
        chunks.append(f'R"{DELIMITER}({chunk}){DELIMITER}"')
    if not chunks:
        chunks.append('""')
    return "\n".join(chunks)


def symbol_name(relative_path: str) -> str:
    result = []
    for char in relative_path:
        if char.isalnum():
            result.append(char)
        else:
            result.append("_")
    return "LDBC_RESOURCE_" + "".join(result)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--resource-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    resource_dir = Path(args.resource_dir)
    output = Path(args.output)
    resources = sorted(path for path in resource_dir.rglob("*") if path.is_file())

    lines = [
        '#include "duckdb/common/common.hpp"',
        "",
        "namespace duckdb {",
        "",
    ]

    entries = []
    for path in resources:
        relative_path = path.relative_to(resource_dir).as_posix()
        data = path.read_text(encoding="utf-8")
        symbol = symbol_name(relative_path)
        lines.append(f"static const char {symbol}[] =")
        lines.append(raw_literal(data) + ";")
        lines.append("")
        entries.append((relative_path, symbol))

    lines.extend(
        [
            "bool LdbcGetEmbeddedResource(const string &relative_path, const char *&data, idx_t &size) {",
        ]
    )
    for relative_path, symbol in entries:
        lines.append(f'\tif (relative_path == "{relative_path}") {{')
        lines.append(f"\t\tdata = {symbol};")
        lines.append(f"\t\tsize = sizeof({symbol}) - 1;")
        lines.append("\t\treturn true;")
        lines.append("\t}")
    lines.extend(
        [
            "\tdata = nullptr;",
            "\tsize = 0;",
            "\treturn false;",
            "}",
            "",
            "} // namespace duckdb",
            "",
        ]
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
