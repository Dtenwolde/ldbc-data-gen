#!/usr/bin/env python3
"""Generate a local Spark LDBC SNB BI reference dataset.

This wraps the pinned upstream Spark datagen submodule so parity checks can be
run against data produced from the exact reference source in this repository.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
SPARK_DATAGEN = ROOT / "third_party" / "ldbc_snb_datagen_spark"
DEFAULT_SPARK_HOME = Path.home() / "spark-3.2.2-bin-hadoop3.2"
HOMEBREW_JAVA_11 = Path("/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home")


def run_command(cmd: list[str], *, cwd: Path, env: dict[str, str], dry_run: bool) -> None:
    print("+ " + " ".join(cmd))
    if dry_run:
        return
    subprocess.run(cmd, cwd=cwd, env=env, check=True)


def check_command(name: str) -> str | None:
    return shutil.which(name)


def capture_command(cmd: list[str], *, cwd: Path, env: dict[str, str]) -> str:
    result = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout.strip()


def parse_extra_args(argv: Iterable[str]) -> list[str]:
    args = list(argv)
    if args and args[0] == "--":
        return args[1:]
    return args


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a pinned Spark LDBC SNB reference dataset for parity checks."
    )
    parser.add_argument("--scale-factor", "--sf", dest="scale_factor", default="0.003")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "reference_data" / "sf0.003",
        help="Reference output directory. Defaults to reference_data/sf0.003.",
    )
    parser.add_argument("--format", default="parquet", choices=["parquet", "csv", "orc"])
    parser.add_argument("--mode", default="bi", choices=["bi", "raw"])
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--cores", default="1")
    parser.add_argument("--memory", default="8G")
    parser.add_argument("--jar", type=Path, help="Existing assembled datagen jar.")
    parser.add_argument(
        "--spark-home",
        type=Path,
        default=Path(os.environ.get("SPARK_HOME", DEFAULT_SPARK_HOME)),
        help="Spark 3.2.x installation. Defaults to SPARK_HOME or ~/spark-3.2.2-bin-hadoop3.2.",
    )
    parser.add_argument(
        "--java-home",
        type=Path,
        default=Path(os.environ["JAVA_HOME"]) if "JAVA_HOME" in os.environ else None,
        help="Java 8 or 11 home. Defaults to JAVA_HOME, then Homebrew openjdk@11 when present.",
    )
    parser.add_argument(
        "--download-spark",
        action="store_true",
        help="Download Spark 3.2.2 to ~/ using the pinned upstream helper when SPARK_HOME is missing.",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Use an existing jar instead of running sbt assembly.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the commands without building, downloading, or running Spark.",
    )
    parser.add_argument(
        "datagen_args",
        nargs=argparse.REMAINDER,
        help="Extra upstream datagen arguments after --, e.g. -- --epoch-millis.",
    )
    args = parser.parse_args(argv)

    if not SPARK_DATAGEN.exists():
        raise SystemExit(f"Missing Spark datagen submodule: {SPARK_DATAGEN}")

    env = dict(os.environ)
    spark_home = args.spark_home.expanduser().resolve()
    java_home = args.java_home.expanduser().resolve() if args.java_home else None
    if java_home is None and (HOMEBREW_JAVA_11 / "bin" / "java").exists():
        java_home = HOMEBREW_JAVA_11

    if java_home is not None:
        env["JAVA_HOME"] = str(java_home)
        env["PATH"] = f"{java_home / 'bin'}{os.pathsep}{env.get('PATH', '')}"

    cache_dir = ROOT / ".cache" / "spark_reference"
    sbt_global = cache_dir / "sbt-global"
    sbt_boot = cache_dir / "sbt-boot"
    ivy_home = cache_dir / "ivy2"
    coursier_cache = cache_dir / "coursier"
    env["COURSIER_CACHE"] = str(coursier_cache)
    sbt_opts = [
        f"-Dsbt.global.base={sbt_global}",
        f"-Dsbt.boot.directory={sbt_boot}",
        f"-Dsbt.ivy.home={ivy_home}",
        f"-Divy.home={ivy_home}",
    ]
    env["SBT_OPTS"] = " ".join([env.get("SBT_OPTS", ""), *sbt_opts]).strip()

    if not (spark_home / "bin" / "spark-submit").exists():
        if args.download_spark:
            run_command(
                ["scripts/get-spark-to-home.sh"],
                cwd=SPARK_DATAGEN,
                env=env,
                dry_run=args.dry_run,
            )
        elif not args.dry_run:
            raise SystemExit(
                f"Spark not found at {spark_home}. Install Spark 3.2.x, set SPARK_HOME, "
                "or rerun with --download-spark."
            )

    env["SPARK_HOME"] = str(spark_home)
    env["PATH"] = f"{spark_home / 'bin'}{os.pathsep}{env.get('PATH', '')}"

    jar = args.jar.expanduser().resolve() if args.jar else None
    if jar is None:
        if not args.skip_build:
            if check_command("sbt") is None and not args.dry_run:
                raise SystemExit("sbt is required to build the Spark datagen jar.")
            run_command(["sbt", "assembly"], cwd=SPARK_DATAGEN, env=env, dry_run=args.dry_run)

        if args.dry_run:
            jar = SPARK_DATAGEN / "target" / "<assembly-jar>"
        else:
            jar_text = capture_command(
                ["sbt", "-batch", "-error", "print assembly / assemblyOutputPath"],
                cwd=SPARK_DATAGEN,
                env=env,
            )
            jar = Path(jar_text).expanduser().resolve()

    if not args.dry_run and not jar.exists():
        raise SystemExit(f"Datagen jar does not exist: {jar}")

    output_dir = args.output_dir.expanduser()
    if not output_dir.is_absolute():
        output_dir = ROOT / output_dir
    output_dir = output_dir.resolve()

    extra_args = parse_extra_args(args.datagen_args)
    command = [
        sys.executable,
        "tools/run.py",
        "--jar",
        str(jar),
        "--cores",
        str(args.cores),
        "--memory",
        args.memory,
        "--parallelism",
        str(args.parallelism),
        "--",
        "--format",
        args.format,
        "--scale-factor",
        args.scale_factor,
        "--mode",
        args.mode,
        "--output-dir",
        str(output_dir),
        *extra_args,
    ]

    run_command(command, cwd=SPARK_DATAGEN, env=env, dry_run=args.dry_run)
    print(f"Spark reference output: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
