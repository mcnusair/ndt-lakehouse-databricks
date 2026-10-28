# Design decisions

Short notes on why the pipeline is built the way it is.

**Explicit schemas in bronze.** Schema inference reads data to guess types and breaks silently when a file changes. Explicit schemas make the contract visible, and `_corrupt_record` catches lines that do not match.

**Dates stay strings in bronze.** The source mixes `yyyy-MM-dd` and `dd/MM/yyyy`. Bronze is the raw copy, so parsing happens in silver where a failure can be quarantined instead of lost.

**Quarantine instead of dropping.** A row with a negative duration or an unknown component cannot go to gold, but losing it would hide a real problem. Each rejected row keeps its reasons as text, and a row can break several rules at once. The count is a data quality signal.

**Only repair what is certain.** A clean inspection with zero indications and a null size gets size 0. A null size with indications above zero is not guessed, it is quarantined.

**Equipment is optional only for PT and VT.** Penetrant and visual testing use consumables, not calibrated instruments. For UT, ET, MT and RT a missing instrument is an error.

**SCD2 for inspectors and equipment.** An inspection must keep pointing at the inspector level and instrument calibration that applied on its date. The fact table stores the surrogate key of the version valid on that date, found by joining on the business key and the date window.

**Equipment version windows run from one calibration to the day before the next.** If a calibration is late, the days in the gap stay with the old version, so the lookup still succeeds. The overdue inspections are then found by comparing the inspection date with `calibration_due`. A design where the gap has no version would lose those inspections from the fact table.

**Only calibration cycles that have started are loaded.** The source includes future cycles. Cycles that start after the latest inspection are left out so the newest loaded version is the current one.

**Surrogate keys with `row_number()`.** Gold is rebuilt completely on each run, so deterministic numbering is enough and works in a DataFrame write. A long-lived warehouse table would use `GENERATED ALWAYS AS IDENTITY`, as in `sql/ndt_star_schema.sql`.

**Gold follows the designed model, not the source.** Source columns that the model does not hold (`aircraft_reg`, `opened_date`, `technique_family`, `ndt_approval_ref`, `home_facility_id`) stay in silver. Missing component serial numbers become `UNKNOWN` because the model declares that column NOT NULL.

**Incremental loads by `file_date` and `MERGE`.** Each weekly folder is appended to bronze with its date. The notebook skips dates it has seen, so re-runs are safe. `MERGE` updates inspections that already exist, inserts new ones, and removes corrected rows from quarantine. The same validation function is used for full and incremental loads, so the rules cannot drift apart.

**External volume for landing, managed tables for the rest.** Landing files come from outside tools and must survive if the volume is dropped. Bronze, silver and gold are managed Delta tables that Unity Catalog owns.

**Shared code through `%run`.** Notebooks import `_common` with `%run`, which works the same in a Databricks Git folder and in the local runner.

**A local runner and a CI test.** The full pipeline runs on plain Spark in under a minute, so a change can be checked without a cluster. Delta-specific behaviour is not covered by that test.
