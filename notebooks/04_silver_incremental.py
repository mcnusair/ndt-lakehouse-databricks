# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Silver: incremental loads
# MAGIC
# MAGIC January 2025 arrives as weekly folders: `incremental/2025-01-06`, `2025-01-13`, `2025-01-20`.
# MAGIC Each folder has an `inspections.json` and a `findings.json`.
# MAGIC
# MAGIC **How it works**
# MAGIC 1. Find the folders that have not been loaded yet. The bronze table remembers every `file_date` it has seen,
# MAGIC    so running this notebook twice does nothing the second time.
# MAGIC 2. Append the raw batch to bronze, with its `file_date`.
# MAGIC 3. Run the same `clean_inspections` rules used for the full load.
# MAGIC 4. `MERGE` the valid rows into silver: existing inspections are updated, new ones inserted.
# MAGIC 5. Rows that were quarantined by the full load and now arrive corrected are removed from quarantine.
# MAGIC 6. Rows that fail the rules this time are appended to quarantine.
# MAGIC
# MAGIC The 2025-01-13 batch corrects two inspections that had a negative duration in the full load.
# MAGIC The 2025-01-20 batch re-sends a record that was already delivered, unchanged. The `MERGE` must handle both.
# MAGIC
# MAGIC Run after notebooks `01` to `03`.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which batches are still pending?

# COMMAND ----------

INC_BRONZE = f"{BRONZE}.inspections_incremental"

available = sorted(f.name.rstrip("/") for f in dbutils.fs.ls(f"{LANDING}/incremental"))
loaded = ({r[0] for r in spark.table(INC_BRONZE).select("file_date").distinct().collect()}
          if spark.catalog.tableExists(INC_BRONZE) else set())
pending = [d for d in available if d not in loaded]
print("available:", available)
print("already loaded:", sorted(loaded))
print("pending:", pending)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process each pending batch, oldest first
# MAGIC Order matters: a correction must be applied after the record it corrects.

# COMMAND ----------

for file_date in pending:
    folder = f"{LANDING}/incremental/{file_date}"
    print(f"\n=== batch {file_date} ===")

    # bronze: append the raw batch
    raw = (read_jsonl(f"{folder}/inspections.json", INSPECTION_SCHEMA)
           .withColumn("file_date", F.lit(file_date)))
    write_bronze(raw, "inspections_incremental", mode="append")

    findings_raw = (read_json_multiline(f"{folder}/findings.json", FINDINGS_SCHEMA)
                    .withColumn("file_date", F.lit(file_date)))
    write_bronze(findings_raw, "findings_incremental", mode="append")

    # silver: same rules as the full load
    batch = spark.table(INC_BRONZE).filter(F.col("file_date") == file_date)
    valid, quarantine = clean_inspections(batch)
    valid.createOrReplaceTempView("batch_valid")

    spark.sql(f"""
        MERGE INTO {SILVER}.inspections AS t
        USING batch_valid AS s
        ON t.inspection_id = s.inspection_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    # a corrected row leaves quarantine
    spark.sql(f"""
        MERGE INTO {SILVER}.inspections_quarantine AS q
        USING batch_valid AS s
        ON q.inspection_id = s.inspection_id
        WHEN MATCHED THEN DELETE
    """)

    # rows that fail the rules this time go to quarantine
    quarantine.write.format("delta").mode("append").saveAsTable(f"{SILVER}.inspections_quarantine")

    # findings: flatten and append (each batch is loaded once, so no duplicates)
    write_silver(explode_findings(spark.table(f"{BRONZE}.findings_incremental")
                                  .filter(F.col("file_date") == file_date)),
                 "findings", mode="append")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result

# COMMAND ----------

n_valid = spark.table(f"{SILVER}.inspections").count()
n_quar = spark.table(f"{SILVER}.inspections_quarantine").count()
print(f"silver.inspections: {n_valid} | quarantine: {n_quar}")
spark.table(f"{SILVER}.inspections_quarantine").groupBy("reject_reason").count().show(truncate=False)
spark.table(f"{BRONZE}.inspections_incremental").groupBy("file_date").count().orderBy("file_date").show()

# The two corrected inspections should now be valid and have a positive duration
corrected = spark.table(f"{SILVER}.inspections").filter("duration_min > 0").count() == n_valid
print("all durations positive:", corrected)
