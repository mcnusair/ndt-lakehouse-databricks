# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Silver: inspections
# MAGIC
# MAGIC The main fact source is where the data problems are. This notebook cleans what can be cleaned and sends
# MAGIC the rest to a quarantine table with a reason, so no row disappears silently.
# MAGIC
# MAGIC The rules live in `clean_inspections` in `_common`, the core of the silver layer.
# MAGIC This notebook runs it, writes the results, and shows what happened.
# MAGIC
# MAGIC Run order: `01` bronze, `02` silver reference data, then this notebook.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

raw = spark.table(f"{BRONZE}.inspections")
print("bronze rows:", raw.count())
print("distinct inspection_id:", raw.select("inspection_id").distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profile before cleaning
# MAGIC These queries show the problems the rules deal with.

# COMMAND ----------

print("result values:")
raw.groupBy("result").count().orderBy("result").show()

print("date strings that are not yyyy-MM-dd:")
raw.filter(~F.col("inspection_date").rlike(r"^\d{4}-\d{2}-\d{2}$")).select("inspection_id", "inspection_date").show()

print("duration <= 0:")
raw.filter("duration_min <= 0").select("inspection_id", "duration_min").show()

print("facility_id values with surrounding whitespace:")
raw.filter(F.col("facility_id") != F.trim("facility_id")).select("inspection_id", "facility_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean and split

# COMMAND ----------

valid, quarantine = clean_inspections(raw)
write_silver(valid, "inspections")
write_silver(quarantine, "inspections_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result

# COMMAND ----------

n_valid = spark.table(f"{SILVER}.inspections").count()
n_quar = spark.table(f"{SILVER}.inspections_quarantine").count()
print(f"valid: {n_valid} | quarantined: {n_quar} | total: {n_valid + n_quar}")
spark.table(f"{SILVER}.inspections_quarantine").groupBy("reject_reason").count().show(truncate=False)

assert n_valid + n_quar == 1200, "bronze had 1210 lines, 10 duplicates, so 1200 distinct inspections"
assert n_valid == 1186 and n_quar == 14, "unexpected split, check the rules"
print("OK: 1186 valid, 14 quarantined")
