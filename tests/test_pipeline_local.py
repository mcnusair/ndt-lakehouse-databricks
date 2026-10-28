"""Runs the whole pipeline (notebooks 01 to 06) on a local Spark session and checks the results.

The notebooks contain their own assertions (row counts, quarantine split, gold integrity),
so a zero exit code already means those passed. The extra checks below pin the headline numbers.
"""
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_pipeline_runs_end_to_end(tmp_path):
    landing_parent = tmp_path / "data"
    landing_parent.mkdir()
    subprocess.run([sys.executable, str(ROOT / "data_generator" / "generate_ndt_landing.py")],
                   cwd=landing_parent, check=True, capture_output=True)

    run = subprocess.run(
        [sys.executable, str(ROOT / "local" / "run_local.py"),
         "--landing", str(landing_parent / "ndt_landing"),
         "--warehouse", str(tmp_path / "warehouse")],
        capture_output=True, text=True)
    out = run.stdout + run.stderr

    assert run.returncode == 0, out[-3000:]
    assert not re.search(r"^FAIL ", out, re.M), out[-3000:]   # status lines printed by the notebooks
    assert "OK: 1186 valid, 14 quarantined" in out          # full load: silver split
    assert "silver.inspections: 1266 | quarantine: 12" in out  # after the three incremental batches
    assert "gold.fact_inspections" in out and "1266 rows" in out
    assert "all durations positive: True" in out            # corrections applied by the MERGE
