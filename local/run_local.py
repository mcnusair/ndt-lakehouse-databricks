"""Run the Databricks notebooks on a local Spark session without an Azure account

    python data_generator/generate_ndt_landing.py     # creates ./ndt_landing
    python local/run_local.py                          # runs notebooks 01 to 06

  * paths: the landing volume becomes the local ./ndt_landing folder
  * names: catalog.schema.table becomes schema.table (plain Spark has no Unity Catalog)
  * format: Delta tables become Parquet tables (Delta needs jars that are downloaded from Maven)
  * MERGE INTO is emulated with DataFrame operations (same result for the two MERGE shapes used here)
  * dbutils.fs.ls and display() are small stand-ins

"""
import argparse
import os
import re
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS = [
    "01_bronze_ingestion", "02_silver_reference_data", "03_silver_inspections",
    "04_silver_incremental", "05_gold_star_schema", "06_analytics",
]


def notebook_to_python(path):
    """Turn a Databricks source-format notebook into plain Python, inlining %run targets."""
    path = Path(path)
    out = []
    for line in path.read_text().splitlines():
        if line.startswith("# MAGIC %run"):
            target = line.split("%run", 1)[1].strip()
            out.append(notebook_to_python((path.parent / target).with_suffix(".py")))
        elif line.startswith("# MAGIC") or line.startswith("# COMMAND") or line == "# Databricks notebook source":
            continue
        else:
            out.append(line)
    return "\n".join(out)


def localise(src, landing):
    src = src.replace('"/Volumes/ndtdata/landing/files"', f'"{landing}"')
    src = re.sub(r'= f"\{CATALOG\}\.(\w+)"', r'= "\1"', src)          # BRONZE = "bronze"
    src = src.replace('format("delta")', 'format("parquet")')
    return src


class SparkShim:
    """Wraps a SparkSession and emulates the MERGE INTO statements used in the notebooks."""

    MERGE = re.compile(
        r"MERGE INTO\s+(\S+)\s+AS\s+\w+\s+USING\s+(\S+)\s+AS\s+\w+\s+ON\s+\w+\.(\w+)\s*=\s*\w+\.\w+\s+(.*)",
        re.S | re.I)

    def __init__(self, spark):
        self._spark = spark

    def __getattr__(self, name):
        return getattr(self._spark, name)

    def sql(self, query):
        m = self.MERGE.search(query)
        if not m:
            return self._spark.sql(query)
        target, source, key, actions = m.groups()
        t, s = self._spark.table(target), self._spark.table(source)
        if "THEN DELETE" in actions.upper():
            result = t.join(s.select(key).distinct(), key, "left_anti")
        else:  # UPDATE SET * / INSERT *
            result = t.join(s.select(key).distinct(), key, "left_anti").unionByName(s)
        tmp = "_merge_tmp"
        result.write.mode("overwrite").format("parquet").saveAsTable(tmp)
        self._spark.table(tmp).write.mode("overwrite").format("parquet").saveAsTable(target)
        self._spark.sql(f"DROP TABLE {tmp}")
        return self._spark.createDataFrame([], "num_affected_rows BIGINT")


def make_dbutils():
    def ls(path):
        return [SimpleNamespace(name=n + ("/" if (Path(path) / n).is_dir() else ""))
                for n in sorted(os.listdir(path))]
    return SimpleNamespace(fs=SimpleNamespace(ls=ls))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--landing", default=str(ROOT / "ndt_landing"))
    ap.add_argument("--warehouse", default=str(ROOT / ".local_warehouse"))
    ap.add_argument("--notebooks", nargs="*", default=NOTEBOOKS)
    args = ap.parse_args()

    if not Path(args.landing).exists():
        sys.exit(f"{args.landing} not found. Run: python data_generator/generate_ndt_landing.py")
    shutil.rmtree(args.warehouse, ignore_errors=True)

    spark = (SparkSession.builder.master("local[2]").appName("ndt-local")
             .config("spark.sql.warehouse.dir", args.warehouse)
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.ansi.enabled", "true")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    for schema in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")

    env = {"spark": SparkShim(spark), "dbutils": make_dbutils(),
           "display": lambda df: df.show(50, truncate=False), "__name__": "__main__"}
    for name in args.notebooks:
        path = ROOT / "notebooks" / f"{name}.py"
        print(f"\n{'=' * 8} {name} {'=' * 8}")
        exec(compile(localise(notebook_to_python(path), args.landing), str(path), "exec"), env)


if __name__ == "__main__":
    main()
