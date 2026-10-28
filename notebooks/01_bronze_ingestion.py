# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze ingestion
# MAGIC
# MAGIC Reads every raw file from the landing volume and writes it to a managed Delta table in `bronze`.
# MAGIC
# MAGIC **Bronze rules used here**
# MAGIC 1. Explicit schemas, never inference.
# MAGIC 2. Keep the data raw. No cleaning, no renaming. Dates stay as strings because the source mixes formats.
# MAGIC 3. Add audit columns: `ingestion_ts` and `source_file`.
# MAGIC 4. Rows Spark cannot parse are kept in `_corrupt_record` instead of being dropped.
# MAGIC 5. Full overwrite, so the notebook can be re-run safely.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config and helpers
# MAGIC Shared configuration, file readers (one per file shape) and table writers live in `_common`.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference data (CSV)

# COMMAND ----------

write_bronze(read_csv(f"{LANDING}/facilities.csv", """
    facility_id STRING, facility_name STRING, facility_type STRING, ndt_approval_ref STRING
"""), "facilities")

write_bronze(read_csv(f"{LANDING}/components.csv", """
    component_id STRING, component_description STRING, component_partnumber STRING,
    component_serialnumber STRING, component_material_type STRING, component_ata_chapter STRING
"""), "components")

write_bronze(read_csv(f"{LANDING}/workorders.csv", """
    workorder_id STRING, task_card_reference STRING, workorder_type STRING,
    aircraft_reg STRING, opened_date STRING
"""), "workorders")

write_bronze(read_csv(f"{LANDING}/certification_history.csv", """
    inspector_id STRING, authorization_number STRING, max_en4179_level INT,
    authorization_expiry STRING, valid_from STRING, valid_to STRING
"""), "certification_history")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference data (JSON lines)
# MAGIC `inspectors` has nested structs. We keep them nested in bronze and flatten in silver.

# COMMAND ----------

write_bronze(read_jsonl(f"{LANDING}/methods.json", """
    method_id STRING, method_name STRING, technique_family STRING, _corrupt_record STRING
"""), "methods")

write_bronze(read_jsonl(f"{LANDING}/equipment.json", """
    equipment_id STRING, description STRING, partnumber STRING, serialnumber STRING,
    method_id STRING, calibrated_on STRING, calibration_due STRING, _corrupt_record STRING
"""), "equipment")

write_bronze(read_jsonl(f"{LANDING}/inspectors.json", """
    inspector_id STRING,
    name STRUCT<forename: STRING, surname: STRING>,
    home_facility_id STRING,
    employment_status STRING,
    authorization STRUCT<number: STRING, max_en4179_level: INT, expiry: STRING>,
    _corrupt_record STRING
"""), "inspectors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main fact source: inspections
# MAGIC `inspection_date` stays a string (mixed formats), `equipment_id` can be null or empty.

# COMMAND ----------

write_bronze(read_jsonl(f"{LANDING}/inspections.json", INSPECTION_SCHEMA), "inspections")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multi-line JSON: findings
# MAGIC One file, one big array, each element holds a nested array of indications.

# COMMAND ----------

write_bronze(read_json_multiline(f"{LANDING}/findings.json", FINDINGS_SCHEMA), "findings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Folders of many files
# MAGIC Spark reads a whole folder in one call. The file date comes from the file name.

# COMMAND ----------

readings = read_csv(f"{LANDING}/{UT_DIR}/", """
    reading_id STRING, inspection_id STRING, point_ref STRING,
    thickness_mm DOUBLE, nominal_mm DOUBLE, probe_id STRING
""", glob="*.csv")
write_bronze(readings, "ut_thickness_readings")

calibration = read_json_multiline(f"{LANDING}/{CAL_DIR}/", """
    certificate_no STRING, equipment_id STRING, calibrated_on STRING,
    calibration_due STRING, calibration_lab STRING, result STRING
""", glob="*.json")
write_bronze(calibration, "calibration_records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks
# MAGIC Row counts must match the files. Any `_corrupt_record` value means a line failed to parse.

# COMMAND ----------

EXPECTED = {
    "facilities": 6, "components": 120, "workorders": 185, "certification_history": 12,
    "methods": 6, "equipment": 9, "inspectors": 10, "inspections": 1210, "findings": 146,
    "ut_thickness_readings": 2434, "calibration_records": 27,
}
for name, expected in EXPECTED.items():
    actual = spark.table(f"{BRONZE}.{name}").count()
    status = "OK " if actual == expected else "FAIL"
    print(f"{status} {name:<24} expected {expected:>6}  got {actual:>6}")

corrupt = spark.table(f"{BRONZE}.inspections").filter("_corrupt_record IS NOT NULL").count()
print("corrupt inspection lines:", corrupt)
