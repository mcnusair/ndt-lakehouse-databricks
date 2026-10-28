# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 Silver: reference and supporting data
# MAGIC
# MAGIC Cleans every bronze table except `inspections` (that one is notebook 03, because it needs rules and a quarantine).
# MAGIC
# MAGIC things the notebook does:
# MAGIC 1. Trims text and turns empty strings into real nulls.
# MAGIC 2. Parses date strings into `DATE`.
# MAGIC 3. Flattens nested structs and arrays.
# MAGIC 4. Removes duplicate keys.
# MAGIC 5. Drops the bronze audit columns, silver tables are the clean version of the business data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simple dimensions

# COMMAND ----------

write_silver(
    spark.table(f"{BRONZE}.facilities").select(
        clean_str("facility_id").alias("facility_id"),
        clean_str("facility_name").alias("facility_name"),
        clean_str("facility_type").alias("facility_type"),
        clean_str("ndt_approval_ref").alias("ndt_approval_ref"),
    ).dropDuplicates(["facility_id"]),
    "facilities")

write_silver(
    spark.table(f"{BRONZE}.components").select(
        clean_str("component_id").alias("component_id"),
        clean_str("component_description").alias("component_description"),
        clean_str("component_partnumber").alias("component_partnumber"),
        clean_str("component_serialnumber").alias("component_serialnumber"),
        clean_str("component_material_type").alias("component_material_type"),
        clean_str("component_ata_chapter").alias("component_ata_chapter"),
    ).dropDuplicates(["component_id"]),
    "components")

write_silver(
    spark.table(f"{BRONZE}.workorders").select(
        clean_str("workorder_id").alias("workorder_id"),
        clean_str("task_card_reference").alias("task_card_reference"),
        clean_str("workorder_type").alias("workorder_type"),
        clean_str("aircraft_reg").alias("aircraft_reg"),
        parse_date("opened_date").alias("opened_date"),
    ).dropDuplicates(["workorder_id"]),
    "workorders")

write_silver(
    spark.table(f"{BRONZE}.methods").select(
        clean_str("method_id").alias("method_id"),
        clean_str("method_name").alias("method_name"),
        clean_str("technique_family").alias("technique_family"),
    ).dropDuplicates(["method_id"]),
    "methods")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Equipment and calibration history
# MAGIC These two feed the SCD2 equipment dimension in gold.

# COMMAND ----------

write_silver(
    spark.table(f"{BRONZE}.equipment").select(
        clean_str("equipment_id").alias("equipment_id"),
        clean_str("description").alias("equipment_description"),
        clean_str("partnumber").alias("equipment_partnumber"),
        clean_str("serialnumber").alias("equipment_serialnumber"),
        clean_str("method_id").alias("method_id"),
    ).dropDuplicates(["equipment_id"]),
    "equipment")

write_silver(
    spark.table(f"{BRONZE}.calibration_records").select(
        clean_str("certificate_no").alias("certificate_no"),
        clean_str("equipment_id").alias("equipment_id"),
        parse_date("calibrated_on").alias("calibrated_on"),
        parse_date("calibration_due").alias("calibration_due"),
        clean_str("calibration_lab").alias("calibration_lab"),
        clean_str("result").alias("result"),
    ).dropDuplicates(["certificate_no"]),
    "calibration_records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspectors and certification history
# MAGIC The nested `name` and `authorization` structs are flattened with dot notation.
# MAGIC These two feed the SCD2 inspector dimension in gold.

# COMMAND ----------

write_silver(
    spark.table(f"{BRONZE}.inspectors").select(
        clean_str("inspector_id").alias("inspector_id"),
        F.concat_ws(" ", clean_str("name.forename"), clean_str("name.surname")).alias("inspector_name"),
        clean_str("home_facility_id").alias("home_facility_id"),
        clean_str("employment_status").alias("employment_status"),
    ).dropDuplicates(["inspector_id"]),
    "inspectors")

write_silver(
    spark.table(f"{BRONZE}.certification_history").select(
        clean_str("inspector_id").alias("inspector_id"),
        clean_str("authorization_number").alias("authorization_number"),
        F.col("max_en4179_level").alias("max_en4179_level"),
        parse_date("authorization_expiry").alias("authorization_expiry"),
        parse_date("valid_from").alias("valid_from"),
        parse_date("valid_to").alias("valid_to"),
    ).dropDuplicates(["inspector_id", "authorization_number"]),
    "certification_history")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Findings: one row per indication
# MAGIC Bronze holds one row per inspection with an array inside. `explode` turns each array element into its own row.

# COMMAND ----------

findings = explode_findings(spark.table(f"{BRONZE}.findings"))
write_silver(findings, "findings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## UT thickness readings
# MAGIC The reading date lives only in the file name, so we pull it out of `source_file`.

# COMMAND ----------

readings = (spark.table(f"{BRONZE}.ut_thickness_readings").select(
        clean_str("reading_id").alias("reading_id"),
        clean_str("inspection_id").alias("inspection_id"),
        clean_str("point_ref").alias("point_ref"),
        F.col("thickness_mm"),
        F.col("nominal_mm"),
        clean_str("probe_id").alias("probe_id"),
        F.to_date(F.regexp_extract("source_file", r"readings_(\d{4}-\d{2}-\d{2})\.csv", 1)).alias("file_date"),
    ).dropDuplicates(["reading_id"]))
write_silver(readings, "ut_thickness_readings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks

# COMMAND ----------

checks = {
    "facilities null approval ref (expect 1)":
        spark.table(f"{SILVER}.facilities").filter("ndt_approval_ref IS NULL").count(),
    "workorders unparsed opened_date (expect 0)":
        spark.table(f"{SILVER}.workorders").filter("opened_date IS NULL").count(),
    "components null serial (expect 3)":
        spark.table(f"{SILVER}.components").filter("component_serialnumber IS NULL").count(),
    "calibration unparsed dates (expect 0)":
        spark.table(f"{SILVER}.calibration_records").filter("calibrated_on IS NULL OR calibration_due IS NULL").count(),
    "certification open-ended versions (expect 10)":
        spark.table(f"{SILVER}.certification_history").filter("valid_to IS NULL").count(),
    "findings rows":
        spark.table(f"{SILVER}.findings").count(),
    "readings with null thickness":
        spark.table(f"{SILVER}.ut_thickness_readings").filter("thickness_mm IS NULL").count(),
    "readings with no file_date (expect 0)":
        spark.table(f"{SILVER}.ut_thickness_readings").filter("file_date IS NULL").count(),
}
for k, v in checks.items():
    print(f"{k:<50} {v}")