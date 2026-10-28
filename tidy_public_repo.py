"""One-off tidy-up for the public repo. Run from the repo root: python tidy_public_repo.py"""
import re
from pathlib import Path

def edit(path, pairs):
    p = Path(path)
    s = p.read_text()
    for old, new in pairs:
        if old not in s:
            print(f"  skipped (already done?): {path}: {old[:50]!r}")
        s = s.replace(old, new)
    p.write_text(s)

# 1. generator: neutral wording instead of "answer key"
edit("data_generator/generate_ndt_landing.py", [
    ("ANSWER_KEY_planted_issues.md", "KNOWN_DATA_ISSUES.md"),
    ("# planted: calibration lapse", "# injected on purpose: calibration lapse"),
    ("# ---- planted data problems (documented in ANSWER_KEY file)",
     "# ---- defects injected on purpose (documented in KNOWN_DATA_ISSUES.md)"),
    ("# Planted data problems (answer key)\\n\\nOpen this only after you have profiled the data yourself.\\n\\n",
     "# Known data issues\\n\\nDefects injected on purpose into the synthetic data, with the affected inspection IDs.\\n\\n"),
    ("## Warning\n\nThe files are deliberately imperfect, like real source data. Profile them yourself first.\n"
     "A list of what was planted is in `KNOWN_DATA_ISSUES.md`. Do not open it until you\n"
     "have written your own data quality checks.\n",
     "## Known issues\n\nThe files are deliberately imperfect, like real source data. The defects injected on purpose\n"
     "are listed in `KNOWN_DATA_ISSUES.md`, with the affected inspection IDs.\n"),
])

# 2. docs: matching wording
edit("docs/running_on_databricks.md", [
    ("The README and answer-key files in it can be left out.", "The README and known-issues files in it can be left out."),
])

# 3. README: the learning folder is not part of the public repo
readme = Path("README.md")
readme.write_text(re.sub(r"^learning/.*\n", "", readme.read_text(), flags=re.M))

# 4. .gitignore: Windows leftovers
gi = Path(".gitignore")
if "Zone.Identifier" not in gi.read_text():
    gi.write_text(gi.read_text().rstrip("\n") + "\n*Zone.Identifier\n")

# 5. notebooks: documentation voice, no tutoring instructions
edit("notebooks/01_bronze_ingestion.py", [
    ("Shared names, readers and writers live in `_common`. Open it and read the readers: there is one per file shape.",
     "Shared configuration, file readers (one per file shape) and table writers live in `_common`."),
])
edit("notebooks/03_silver_inspections.py", [
    ("The rules live in `clean_inspections` in `_common` (read it, it is the heart of the silver layer).",
     "The rules live in `clean_inspections` in `_common`, the core of the silver layer."),
    ("These queries show the problems the rules deal with. Run them before reading the rules.",
     "These queries show the problems the rules deal with."),
])
edit("notebooks/06_analytics.py", [
    ("Use the table output or the chart button in Databricks to turn any of them into a visual for screenshots.",
     "Any result can be turned into a chart with the visualization button in Databricks."),
])
edit("notebooks/_common.py", [
    ("# folder names exactly as in your volume", "# folder names inside the landing volume"),
])
print("done")
