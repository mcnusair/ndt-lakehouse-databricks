# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Analytics on the gold layer
# MAGIC
# MAGIC Nine business questions answered with SQL on the star schema. The same queries are in `sql/analytics_queries.sql`.
# MAGIC Any result can be turned into a chart with the visualization button in Databricks.
# MAGIC
# MAGIC Run after notebook `05`.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pass rate by NDT method

# COMMAND ----------

display(spark.sql(f"""
SELECT m.method_name,
       COUNT(*)                                        AS inspections,
       SUM(CASE WHEN f.pass_fail THEN 1 ELSE 0 END)    AS passed,
       ROUND(100.0 * AVG(CASE WHEN f.pass_fail THEN 1 ELSE 0 END), 1) AS pass_rate_pct
FROM {GOLD}.fact_inspections f
JOIN {GOLD}.dim_method m ON f.method_key = m.method_key
GROUP BY m.method_name
ORDER BY pass_rate_pct ASC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Inspector workload and pass rate, with the certification level held at the time
# MAGIC Because `dim_inspector` is SCD2, an inspector who was promoted appears once per level.

# COMMAND ----------

display(spark.sql(f"""
SELECT i.inspector_name,
       i.inspector_max_EN4179_level  AS level_at_time,
       i.valid_from,
       COUNT(*)                      AS inspections,
       ROUND(100.0 * AVG(CASE WHEN f.pass_fail THEN 1 ELSE 0 END), 1) AS pass_rate_pct
FROM {GOLD}.fact_inspections f
JOIN {GOLD}.dim_inspector i ON f.inspector_key = i.inspector_key
GROUP BY i.inspector_name, i.inspector_max_EN4179_level, i.valid_from
ORDER BY i.inspector_name, i.valid_from
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Failure rate by material and ATA chapter
# MAGIC Which aircraft systems and materials produce the most failed inspections?

# COMMAND ----------

display(spark.sql(f"""
SELECT c.component_material_type,
       c.component_ata_chapter,
       COUNT(*)                                                         AS inspections,
       SUM(CASE WHEN f.pass_fail THEN 0 ELSE 1 END)                     AS failed,
       ROUND(100.0 * AVG(CASE WHEN f.pass_fail THEN 0 ELSE 1 END), 1)   AS fail_rate_pct
FROM {GOLD}.fact_inspections f
JOIN {GOLD}.dim_component c ON f.component_id = c.component_id
GROUP BY c.component_material_type, c.component_ata_chapter
HAVING COUNT(*) >= 20
ORDER BY fail_rate_pct DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Component types with the largest average indication
# MAGIC Only inspections that found something.

# COMMAND ----------

display(spark.sql(f"""
SELECT c.component_description,
       COUNT(*)                                       AS inspections_with_findings,
       ROUND(AVG(f.max_indication_length_mm), 1)      AS avg_max_length_mm,
       ROUND(MAX(f.max_indication_length_mm), 1)      AS worst_length_mm
FROM {GOLD}.fact_inspections f
JOIN {GOLD}.dim_component c ON f.component_id = c.component_id
WHERE f.indication_count > 0
GROUP BY c.component_description
ORDER BY avg_max_length_mm DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Inspections per facility per quarter

# COMMAND ----------

display(spark.sql(f"""
SELECT d.year, d.quarter, fa.facility_name, COUNT(*) AS inspections
FROM {GOLD}.fact_inspections f
JOIN {GOLD}.dim_date d      ON f.date_id = d.date_id
JOIN {GOLD}.dim_facility fa ON f.facility_id = fa.facility_id
GROUP BY d.year, d.quarter, fa.facility_name
ORDER BY d.year, d.quarter, inspections DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Compliance: inspections done with an instrument whose calibration had expired
# MAGIC Joins each inspection to the equipment version it used and compares the inspection date with `calibration_due`.
# MAGIC This is the audit question the SCD2 design exists to answer.

# COMMAND ----------

display(spark.sql(f"""
SELECT f.inspection_id,
       d.full_date                           AS inspection_date,
       e.equipment_id,
       e.equipment_description,
       e.calibration_due,
       DATEDIFF(d.full_date, e.calibration_due) AS days_overdue
FROM {GOLD}.fact_inspections f
JOIN {GOLD}.dim_equipment e ON f.equipment_key = e.equipment_key
JOIN {GOLD}.dim_date d      ON f.date_id = d.date_id
WHERE d.full_date > e.calibration_due
ORDER BY days_overdue DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Equipment with calibration expiring in the next 90 days
# MAGIC "Today" is the date of the latest inspection in the data.

# COMMAND ----------

display(spark.sql(f"""
WITH as_of AS (
    SELECT MAX(d.full_date) AS today
    FROM {GOLD}.fact_inspections f JOIN {GOLD}.dim_date d ON f.date_id = d.date_id
)
SELECT e.equipment_id, e.equipment_description, e.calibration_due,
       DATEDIFF(e.calibration_due, a.today) AS days_left
FROM {GOLD}.dim_equipment e CROSS JOIN as_of a
WHERE e.is_current
  AND e.calibration_due BETWEEN a.today AND DATE_ADD(a.today, 90)
ORDER BY e.calibration_due
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Monthly inspection volume and pass rate

# COMMAND ----------

display(spark.sql(f"""
SELECT d.year, d.month,
       COUNT(*) AS inspections,
       ROUND(100.0 * AVG(CASE WHEN f.pass_fail THEN 1 ELSE 0 END), 1) AS pass_rate_pct,
       ROUND(AVG(f.inspection_duration_min), 0) AS avg_duration_min
FROM {GOLD}.fact_inspections f
JOIN {GOLD}.dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Data quality: what was quarantined and why

# COMMAND ----------

display(spark.sql(f"""
SELECT reject_reason, COUNT(*) AS rows
FROM {SILVER}.inspections_quarantine
GROUP BY reject_reason
ORDER BY rows DESC
"""))
