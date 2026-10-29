# Data quality

The data is synthetic, but I put in the kind of defects found in real maintenance records. This page lists each defect, what the pipeline does about it, and what comes out the other end.

## The numbers for the full load

- 1,210 inspection lines arrive in bronze.
- 1,200 are left after exact duplicates are removed.
- 1,186 are valid and go to silver.inspections.
- 14 are quarantined.

After the three January 2025 batches there are 1,266 valid rows and 12 in quarantine. Two of the quarantined rows were corrected by the batch from 2025-01-13, so they moved to silver.

## Defect by defect

- Exact duplicate lines (10): a window function numbers the rows for each inspection_id and the latest ingestion wins. The duplicates are removed.
- Result in mixed case (about 12 percent of rows): upper-cased, then PASS becomes true. Fixed.
- Dates written as dd/MM/yyyy (5): both formats are parsed with explicit patterns. Fixed.
- Trailing space in facility_id (7): trimmed. Fixed.
- Empty string instead of null (8): turned into null, which then gets caught by the equipment rule below.
- Null size on a clean inspection (6): set to 0, because zero indications means zero size. Fixed.
- Instrument-based method with no equipment (8): quarantined as missing_equipment. This only applies to UT, ET, MT and RT.
- Component that does not exist (3): quarantined as orphan_component.
- Duration of zero or less (3): quarantined as invalid_duration. Two of them were corrected later.
- Inspection done after the calibration expired (14): not rejected. The record is valid, so it stays in the fact table, and a query in notebook 06 finds it.

## Why some things are fixed and others quarantined

I only repair a value when the right answer is certain. A clean inspection has no indications, so its size is zero. A missing instrument or an unknown component cannot be worked out from the data, so the row waits in quarantine until a corrected record shows up.

The calibration lapse is a different case. The inspection happened and nothing is wrong with the record. The problem is a compliance one, so the pipeline keeps the evidence and leaves it to a query to surface it.

## One set of rules for every load

The function clean_inspections in notebooks/_common.py is used for the full load and for every weekly batch, so the two cannot drift apart. These are the reasons it can give a rejected row: corrupt_record, missing_inspection_id, bad_date, unknown_result, invalid_duration, missing_equipment, orphan_equipment, missing_measurements, orphan_component, orphan_workorder, orphan_facility, orphan_method and orphan_inspector. A row can have several.

## Checks at the end

Notebook 05 finishes with integrity checks. Every foreign key in the fact table has to exist in its dimension, no inspection ID can appear twice, and the fact table has to have one row for each valid silver inspection. If any check fails, the notebook stops with an error.
