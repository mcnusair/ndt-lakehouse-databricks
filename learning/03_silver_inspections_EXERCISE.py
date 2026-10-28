# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Silver: inspections (exercise version)
# MAGIC
# MAGIC This is the learning version of `notebooks/03_silver_inspections.py`. The cleaning rules are removed.
# MAGIC You write three cells (marked TODO): normalise, fix measurements, and the validation rules.
# MAGIC The other cells are given. The last cell tells you if your result is right.
# MAGIC
# MAGIC The finished version of these rules is the function `clean_inspections` in `notebooks/_common.py`.
# MAGIC Try first, then compare.
# MAGIC
# MAGIC Before you write anything, profile `bronze.inspections` yourself: count distinct `result` values,
# MAGIC look at the date formats, look for negative durations, empty strings and nulls.
# MAGIC
# MAGIC Run `01` and `02` first, so the bronze and silver reference tables exist.

# COMMAND ----------

# MAGIC %run ../notebooks/_common

# COMMAND ----------

raw = spark.table(f"{BRONZE}.inspections")
print("bronze rows:", raw.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 (TODO): normalise
# MAGIC Build `norm` with `raw.select(...)`. Keep these output columns, in this order:
# MAGIC
# MAGIC `inspection_id, inspection_date, inspection_date_raw, component_id, workorder_id, method_id, equipment_id,
# MAGIC facility_id, inspector_id, duration_min, result, indication_count, max_indication_length_mm,
# MAGIC max_indication_width_mm, next_inspection_due_hrs, ingestion_ts, source_file, _corrupt_record`
# MAGIC
# MAGIC Hints:
# MAGIC - Text columns: which helper trims and turns empty strings into nulls?
# MAGIC - `inspection_date` should be a real date, `inspection_date_raw` keeps the original string for the quarantine table.
# MAGIC - `result` has mixed casing. Make it consistent.

# COMMAND ----------

norm = None  # TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: deduplicate (given)
# MAGIC Several lines appear twice. Keep the most recently ingested row per `inspection_id`.
# MAGIC Be ready to explain what `row_number` over a window does here.

# COMMAND ----------

w = Window.partitionBy("inspection_id").orderBy(F.col("ingestion_ts").desc(), F.col("source_file"))
deduped = norm.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
print("after dedup:", deduped.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 (TODO): fix the measurements
# MAGIC Some inspections with `indication_count = 0` have null length and width.
# MAGIC A clean inspection has zero size, not unknown size. Fix only those rows, for both columns.
# MAGIC Do **not** fill nulls when `indication_count > 0`, because there the missing size is a real problem.
# MAGIC Build `fixed` from `deduped`.

# COMMAND ----------

fixed = None  # TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: referential checks (given)
# MAGIC For each foreign key we add a True/False column saying whether the key exists in the silver dimension.
# MAGIC A left join works at any table size, unlike `isin(list)`.

# COMMAND ----------

checked = fixed
for table, key, flag in [("components", "component_id", "ok_component"),
                         ("workorders", "workorder_id", "ok_workorder"),
                         ("facilities", "facility_id", "ok_facility"),
                         ("methods", "method_id", "ok_method"),
                         ("inspectors", "inspector_id", "ok_inspector"),
                         ("equipment", "equipment_id", "ok_equipment")]:
    checked = _key_flag(checked, table, key, flag)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 (TODO): validation rules
# MAGIC Build an array column `reasons` that lists every rule a row breaks. A row can break more than one.
# MAGIC
# MAGIC Pattern: an array of `F.when(condition, F.lit("reason_name"))`, then keep only the non-null entries
# MAGIC with `F.filter(array, lambda x: x.isNotNull())`.
# MAGIC
# MAGIC | reason | row is bad when |
# MAGIC | --- | --- |
# MAGIC | `corrupt_record` | `_corrupt_record` is not null |
# MAGIC | `missing_inspection_id` | key is null |
# MAGIC | `bad_date` | the parsed date is null |
# MAGIC | `unknown_result` | result is null or not PASS or FAIL |
# MAGIC | `invalid_duration` | duration is null or 0 or negative |
# MAGIC | `missing_equipment` | method is UT, ET, MT or RT (they need an instrument) and equipment is null |
# MAGIC | `orphan_equipment` | equipment is given but does not exist in the dimension |
# MAGIC | `missing_measurements` | indication count is above 0 and length or width is null |
# MAGIC | `orphan_component`, `orphan_workorder`, `orphan_facility`, `orphan_method`, `orphan_inspector` | the matching `ok_...` flag is False |
# MAGIC
# MAGIC Think about why PT and VT inspections are allowed to have no equipment.

# COMMAND ----------

reasons = None  # TODO

scored = checked.withColumn("reasons", reasons)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: split valid rows from quarantine (given)

# COMMAND ----------

c = F.col
valid = scored.filter(F.size("reasons") == 0).select(
    "inspection_id", "inspection_date", "component_id", "workorder_id", "method_id",
    "equipment_id", "facility_id", "inspector_id", "duration_min",
    (c("result") == "PASS").alias("pass_fail"),
    "indication_count", "max_indication_length_mm", "max_indication_width_mm",
    "next_inspection_due_hrs", "ingestion_ts")

quarantine = scored.filter(F.size("reasons") > 0).select(
    "inspection_id", "inspection_date_raw", "component_id", "workorder_id", "method_id",
    "equipment_id", "facility_id", "inspector_id", "duration_min", "result",
    "indication_count", "max_indication_length_mm", "max_indication_width_mm",
    "next_inspection_due_hrs", "source_file",
    F.concat_ws(",", "reasons").alias("reject_reason"),
    F.current_timestamp().alias("quarantined_ts"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check your result
# MAGIC Expected: after dedup 1200, valid 1186, quarantined 14.
# MAGIC Quarantine reasons: `orphan_component` 3, `missing_equipment` 8, `invalid_duration` 3.
# MAGIC Fewer quarantined rows means a rule is too weak, more means a rule is too strict.
# MAGIC
# MAGIC This exercise only builds the DataFrames. It does not overwrite your real silver tables.

# COMMAND ----------

n_valid, n_quar = valid.count(), quarantine.count()
print("valid:", n_valid, "| quarantined:", n_quar)
quarantine.groupBy("reject_reason").count().show(truncate=False)
print("OK" if (n_valid, n_quar) == (1186, 14) else "Not yet, compare your rules with the table in step 5")
