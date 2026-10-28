# Data quality

The source data is synthetic but contains the kinds of defects found in real maintenance records. This page lists each one, the rule that handles it and the outcome.

## Summary of the full load

| Stage | Rows |
| --- | --- |
| Bronze inspection lines | 1,210 |
| After removing exact duplicates | 1,200 |
| Valid, written to `silver.inspections` | 1,186 |
| Quarantined | 14 |

After the three January 2025 batches: 1,266 valid rows and 12 in quarantine. Two quarantined rows were corrected by the 2025-01-13 batch and moved to silver.

## Rules and outcomes

| Defect | Rows | Rule | Outcome |
| --- | --- | --- | --- |
| Exact duplicate lines | 10 | Window `row_number` per `inspection_id`, latest ingestion wins | Removed |
| Result in mixed case | about 12 percent | Upper-case, then `PASS` becomes true | Fixed |
| Dates as `dd/MM/yyyy` | 5 | Both formats parsed with explicit patterns | Fixed |
| Trailing space in `facility_id` | 7 | Trim | Fixed |
| Empty string instead of null | 8 | Empty becomes null | Then caught by the equipment rule below |
| Null size on a clean inspection | 6 | Set to 0 when `indication_count = 0` | Fixed |
| Instrument method without equipment | 8 | `missing_equipment` for UT, ET, MT, RT | Quarantined |
| Component not in the component table | 3 | `orphan_component` | Quarantined |
| Duration zero or negative | 3 | `invalid_duration` | Quarantined, 2 corrected later |
| Inspection done after calibration expired | 14 | Not rejected, kept for the audit | Found by the compliance query in notebook 06 |

## Why some defects are fixed and others quarantined

A defect is fixed only when the correct value is certain. A clean inspection has no indications, so its size is zero. A missing instrument or an unknown component cannot be recovered from the data, so the row waits in quarantine until a corrected record arrives.

The calibration lapse is different. The inspection happened and the record is valid, so it belongs in the fact table. The finding is about compliance, and the pipeline keeps the evidence so a query can surface it.

## Rules that are checked on every load

`clean_inspections` in `notebooks/_common.py` is used for the full load and for every incremental batch. The reasons it can assign are: `corrupt_record`, `missing_inspection_id`, `bad_date`, `unknown_result`, `invalid_duration`, `missing_equipment`, `orphan_equipment`, `missing_measurements`, `orphan_component`, `orphan_workorder`, `orphan_facility`, `orphan_method`, `orphan_inspector`.

## Integrity checks

Notebook 05 ends with checks that every foreign key in `fact_inspections` exists in its dimension, that no inspection ID is duplicated, and that the fact has one row for each valid silver inspection. The notebook fails if any check does.
