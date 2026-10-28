# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Gold: star schema
# MAGIC
# MAGIC Builds the dimensional model from silver: seven dimensions and `fact_inspections`.
# MAGIC The column names and types follow `sql/ndt_star_schema.sql`.
# MAGIC
# MAGIC **Grain of the fact table:** one NDT inspection event.
# MAGIC
# MAGIC **Design points**
# MAGIC - `dim_inspector` and `dim_equipment` are SCD Type 2. Each row is one version, valid for a date range.
# MAGIC - The fact table stores the **surrogate key of the version that was valid on the inspection date**.
# MAGIC   That is what makes the data auditable: an old inspection keeps pointing at the inspector level and the
# MAGIC   instrument calibration that applied at the time.
# MAGIC - Equipment version windows run from one calibration to the day before the next. If a calibration was late,
# MAGIC   the gap days belong to the previous version, and the inspection can then be flagged as out of calibration
# MAGIC   by comparing its date with `calibration_due` (see notebook 06).
# MAGIC - Surrogate keys here are numbered with `row_number()` because the whole gold layer is rebuilt on every run.
# MAGIC   On a long-lived warehouse table you would use `GENERATED ALWAYS AS IDENTITY` (see `sql/ndt_star_schema.sql`).
# MAGIC
# MAGIC Run after notebooks `01` to `04`.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

def with_key(df, key_name, *order_cols):
    """Number the rows 1..n in a stable order, as a BIGINT surrogate key in the first column."""
    w = Window.orderBy(*order_cols)
    keyed = df.withColumn(key_name, F.row_number().over(w).cast("bigint"))
    return keyed.select(key_name, *[c for c in df.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simple dimensions

# COMMAND ----------

# dim_date: one row per calendar day. No source file, so it is generated.
dim_date = (spark.sql("SELECT explode(sequence(to_date('2024-01-01'), to_date('2025-12-31'), interval 1 day)) AS full_date")
    .select(F.date_format("full_date", "yyyy-MM-dd").alias("date_id"),
            "full_date",
            F.dayofmonth("full_date").alias("day"),
            F.month("full_date").alias("month"),
            F.year("full_date").alias("year"),
            F.quarter("full_date").alias("quarter")))
write_gold(dim_date, "dim_date")

dim_facility = spark.table(f"{SILVER}.facilities").select("facility_id", "facility_name", "facility_type")
write_gold(dim_facility, "dim_facility")

dim_workorder = spark.table(f"{SILVER}.workorders").select("workorder_id", "task_card_reference", "workorder_type")
write_gold(dim_workorder, "dim_workorder")

# the model says serial number is NOT NULL, so missing serials become 'UNKNOWN'
dim_component = spark.table(f"{SILVER}.components").select(
    "component_id", "component_description", "component_partnumber",
    F.coalesce("component_serialnumber", F.lit("UNKNOWN")).alias("component_serialnumber"),
    "component_material_type", "component_ata_chapter")
write_gold(dim_component, "dim_component")

dim_method = with_key(spark.table(f"{SILVER}.methods").select("method_id", "method_name"),
                      "method_key", "method_id")
write_gold(dim_method, "dim_method")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_inspector (SCD2)
# MAGIC One row per certification version. The history comes from `certification_history`, the name from `inspectors`.

# COMMAND ----------

dim_inspector = with_key(
    spark.table(f"{SILVER}.certification_history").alias("h")
        .join(spark.table(f"{SILVER}.inspectors").select("inspector_id", "inspector_name"), "inspector_id")
        .select("inspector_id",
                "inspector_name",
                F.col("authorization_number").alias("inspector_authorization_num"),
                "authorization_expiry",
                F.col("max_en4179_level").alias("inspector_max_EN4179_level"),
                "valid_from",
                "valid_to",
                F.col("valid_to").isNull().alias("is_current")),
    "inspector_key", "inspector_id", "valid_from")
write_gold(dim_inspector, "dim_inspector")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_equipment (SCD2)
# MAGIC One row per calibration cycle. `valid_from` is the calibration date, `valid_to` is the day before the next
# MAGIC calibration. Only cycles that have started by the date of the latest inspection are included, so the last one
# MAGIC is the current version.

# COMMAND ----------

as_of = spark.table(f"{SILVER}.inspections").agg(F.max("inspection_date")).first()[0]
print("as-of date (latest inspection):", as_of)

w = Window.partitionBy("equipment_id").orderBy("calibrated_on")
equipment_versions = (spark.table(f"{SILVER}.calibration_records")
    .filter(F.col("calibrated_on") <= F.lit(as_of))
    .withColumn("next_calibrated_on", F.lead("calibrated_on").over(w))
    .join(spark.table(f"{SILVER}.equipment"), "equipment_id")
    .select("equipment_id",
            "equipment_description",
            "equipment_partnumber",
            "equipment_serialnumber",
            "calibrated_on",
            "calibration_due",
            F.col("calibrated_on").alias("valid_from"),
            F.date_sub("next_calibrated_on", 1).alias("valid_to"),
            F.col("next_calibrated_on").isNull().alias("is_current")))
dim_equipment = with_key(equipment_versions, "equipment_key", "equipment_id", "valid_from")
write_gold(dim_equipment, "dim_equipment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_inspections
# MAGIC For the two SCD2 dimensions the join is on the business key **and** the date window:
# MAGIC `valid_from <= inspection_date <= valid_to` (an open `valid_to` means the version is still current).

# COMMAND ----------

def in_window(date_col, vf, vt):
    return (F.col(date_col) >= F.col(vf)) & (F.col(vt).isNull() | (F.col(date_col) <= F.col(vt)))


insp = spark.table(f"{SILVER}.inspections")
di = dim_inspector.select(F.col("inspector_id").alias("di_id"), "inspector_key",
                          F.col("valid_from").alias("di_from"), F.col("valid_to").alias("di_to"))
de = dim_equipment.select(F.col("equipment_id").alias("de_id"), "equipment_key",
                          F.col("valid_from").alias("de_from"), F.col("valid_to").alias("de_to"))
dm = dim_method.select("method_id", "method_key")

joined = (insp
    .join(dm, "method_id", "left")
    .join(di, (insp["inspector_id"] == di["di_id"]) & in_window("inspection_date", "di_from", "di_to"), "left")
    .join(de, (insp["equipment_id"] == de["de_id"]) & in_window("inspection_date", "de_from", "de_to"), "left"))

fact = joined.select(
    "inspection_id",
    F.date_format("inspection_date", "yyyy-MM-dd").alias("date_id"),
    "component_id",
    "workorder_id",
    "method_key",
    "equipment_key",
    "facility_id",
    "inspector_key",
    F.col("duration_min").alias("inspection_duration_min"),
    "pass_fail",
    "indication_count",
    F.col("max_indication_length_mm").cast("float").alias("max_indication_length_mm"),
    F.col("max_indication_width_mm").cast("float").alias("max_indication_width_mm"),
    "next_inspection_due_hrs",
)

# rows whose SCD2 or method lookup failed cannot be loaded (their keys are NOT NULL in the model)
bad = fact.filter("method_key IS NULL OR inspector_key IS NULL")
missing_equipment_key = (fact.join(insp.select("inspection_id", "equipment_id"), "inspection_id")
                             .filter("equipment_id IS NOT NULL AND equipment_key IS NULL"))
print("rows with no method/inspector version:", bad.count())
print("rows with equipment but no equipment version:", missing_equipment_key.count())

write_gold(fact.join(bad.select("inspection_id"), "inspection_id", "left_anti"), "fact_inspections")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integrity checks
# MAGIC Every key in the fact table must exist in its dimension. Every inspection from silver must be in the fact.

# COMMAND ----------

f = spark.table(f"{GOLD}.fact_inspections")
checks = {
    "fact rows vs silver inspections": (f.count(), spark.table(f"{SILVER}.inspections").count()),
    "duplicate inspection_id (expect 0)": (f.count() - f.select("inspection_id").distinct().count(), 0),
    "orphan date_id": (f.join(spark.table(f"{GOLD}.dim_date"), "date_id", "left_anti").count(), 0),
    "orphan component_id": (f.join(spark.table(f"{GOLD}.dim_component"), "component_id", "left_anti").count(), 0),
    "orphan workorder_id": (f.join(spark.table(f"{GOLD}.dim_workorder"), "workorder_id", "left_anti").count(), 0),
    "orphan facility_id": (f.join(spark.table(f"{GOLD}.dim_facility"), "facility_id", "left_anti").count(), 0),
    "orphan method_key": (f.join(spark.table(f"{GOLD}.dim_method"), "method_key", "left_anti").count(), 0),
    "orphan inspector_key": (f.join(spark.table(f"{GOLD}.dim_inspector"), "inspector_key", "left_anti").count(), 0),
    "orphan equipment_key": (f.filter("equipment_key IS NOT NULL")
                              .join(spark.table(f"{GOLD}.dim_equipment"), "equipment_key", "left_anti").count(), 0),
}
ok = True
for name, (actual, expected) in checks.items():
    status = "OK  " if actual == expected else "FAIL"
    ok &= actual == expected
    print(f"{status} {name:<38} {actual}" + ("" if actual == expected else f" (expected {expected})"))
assert ok, "integrity check failed"

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD2 sanity: the two promoted inspectors have two versions each

# COMMAND ----------

spark.table(f"{GOLD}.dim_inspector").filter(
    F.col("inspector_id").isin("INS-002", "INS-007")).orderBy("inspector_id", "valid_from").show(truncate=False)
