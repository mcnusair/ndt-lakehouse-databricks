# Design decisions

Notes on why I built the pipeline this way, including a few things I changed my mind about.

## Bronze

I use explicit schemas. Inference reads the data to guess types, and it breaks quietly when a file changes. With a written schema the contract is visible, and anything that does not match ends up in _corrupt_record.

Dates stay as strings. The source mixes yyyy-MM-dd and dd/MM/yyyy, and bronze is meant to be the raw copy. Parsing happens in silver, where a failure can be quarantined instead of lost.

## Silver

Bad rows go to quarantine instead of being dropped. A row with a negative duration or an unknown component cannot reach gold, but throwing it away would hide a real problem. Each rejected row keeps its reasons as text, and a row can break several rules at once. The size of the quarantine table is a useful signal on its own.

I only repair what is certain. A clean inspection with zero indications and a null size gets a size of 0. A null size on an inspection with indications is not guessed, it is quarantined.

Equipment is optional only for PT and VT. Penetrant and visual testing use consumables, not calibrated instruments. For UT, ET, MT and RT a missing instrument is an error.

## Gold

Inspectors and equipment are SCD Type 2. An inspection has to keep pointing at the inspector level and the instrument calibration that applied on its date. The fact table stores the surrogate key of the version valid on that date, found by joining on the business key and the date window.

For equipment, a version runs from one calibration to the day before the next one. If a calibration is late, the days in the gap stay with the old version, so the lookup still works. The overdue inspections are then found by comparing the inspection date with calibration_due. If the gap had no version, those inspections would disappear from the fact table, and they are exactly the ones an auditor asks about.

Only calibration cycles that have already started are loaded. The source also lists future cycles, and I left out any that begin after the latest inspection so that the newest loaded version is the current one.

Surrogate keys come from row_number(). Gold is rebuilt completely on each run, so deterministic numbering is enough and works with a DataFrame write. A warehouse table that lives for years would use GENERATED ALWAYS AS IDENTITY, as the DDL in sql/ndt_star_schema.sql does.

Gold follows the model I designed, not the source files. Source columns that the model does not hold (aircraft_reg, opened_date, technique_family, ndt_approval_ref, home_facility_id) stay in silver. Missing component serial numbers become UNKNOWN, because the model declares that column NOT NULL.

## Loading

Incremental loads work by file date and MERGE. Each weekly folder is appended to bronze with its date, and the notebook skips dates it has already seen, so re-runs are safe. MERGE updates inspections that exist, inserts new ones, and removes corrected rows from quarantine. The same validation function runs for full and incremental loads.

Landing is an external volume and everything else is a managed Delta table. Landing files come from outside tools and have to survive if the volume is dropped. Bronze, silver and gold are owned by Unity Catalog.

## Code and testing

The shared code lives in a _common notebook and is pulled in with %run. It works the same in a Databricks Git folder and in the local runner, and it keeps the validation rules in one place.

There is a local runner and a CI test. The whole pipeline runs on plain Spark in under a minute, so I can check a change without a cluster. That test does not cover Delta-specific behaviour, so MERGE was only really tested on Databricks.
