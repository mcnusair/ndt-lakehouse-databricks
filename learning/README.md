# Learning path

Use this folder to learn the project step by step, without an Azure account. Everything runs locally on plain Spark.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt        # needs Java 17
python data_generator/generate_ndt_landing.py
python local/run_local.py              # runs the whole pipeline once, to see the end result
```

Run `python local/run_local.py --notebooks 01_bronze_ingestion` to run a single stage. Later stages need the earlier ones to have run first.

## Step by step

Work through these in order. For each one, read the notebook, run it, then answer the questions without looking.

| Step | Notebook | You should be able to explain |
| --- | --- | --- |
| 1 | `01_bronze_ingestion` | Why explicit schemas beat inference. What `_corrupt_record` and `_metadata.file_path` give you. Why dates stay as strings here |
| 2 | `02_silver_reference_data` | What `clean_str` and `parse_date` do and why `parse_date` checks the format with a regex first. What `explode` does to the findings |
| 3 | `03_silver_inspections` and `_common.clean_inspections` | Each of the six steps. Why the quarantine keeps the reason. Why PT and VT may have no equipment |
| 4 | `04_silver_incremental` | How the notebook knows a batch is already loaded. What the two `MERGE` statements do and why a corrected row must leave quarantine |
| 5 | `05_gold_star_schema` | What SCD2 means, how `valid_from` and `valid_to` are built, and how the fact finds the right version by date |
| 6 | `06_analytics` | Which query needs the SCD2 design to be answerable, and why |

## Exercise

`03_silver_inspections_EXERCISE.py` is the silver inspections notebook with the three key cells removed: normalise, repair the measurements, and write the validation rules. The target is 1,186 valid rows and 14 quarantined. Do it before reading `clean_inspections` in `notebooks/_common.py`, then compare.

To run it locally, import the file as a notebook in Databricks, or adapt it into a script and run it with the local runner after steps 1 and 2.

## Rebuild from scratch

When you are comfortable, start with an empty folder and rebuild one stage at a time, running the checks at the end of each notebook as your target:

1. Read `facilities.csv` into bronze with an explicit schema. Then do the other CSV files.
2. Read the JSON lines files, including the nested `inspectors`.
3. Read `findings.json` (multi-line) and the two folders of files.
4. Write silver for the reference tables.
5. Write the inspections rules yourself.
6. Build `dim_inspector` with valid-from and valid-to.
7. Build the fact table with the date-window lookups.

## Questions to practise for interviews

- Why do bronze, silver and gold exist, and what would break if you skipped silver?
- What is the difference between a managed table and an external volume, and what happens to the files when each is dropped?
- A new batch re-sends a record you already loaded. What happens, and why?
- An inspector is promoted in June. How does an inspection from May still show the old level?
- An instrument's calibration lapsed for six weeks. How does the model let you find the inspections done in that window?
- Which two permission checks apply when a user reads a file through Unity Catalog?
