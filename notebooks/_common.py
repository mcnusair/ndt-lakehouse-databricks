# Databricks notebook source
# MAGIC %md
# MAGIC # _common: shared config and helpers
# MAGIC
# MAGIC Every pipeline notebook starts with %run ./_common. The names in the config cell are the only things to change if your catalog or volume is called something else.
# MAGIC
# MAGIC This file holds the config, the text and date helpers, the file readers, the table writers, the column lists for inspections, and clean_inspections, the function with all the silver validation rules.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "ndtdata"                          # Unity Catalog catalog
LANDING = "/Volumes/ndtdata/landing/files"   # external volume over the landing folder
UT_DIR = "ut-thickness-readings"             # folder names inside the landing volume
CAL_DIR = "calibration-records"

BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Text and date helpers

# COMMAND ----------

def clean_str(c):
    """Trim whitespace, and turn an empty string into a null."""
    c = F.trim(F.col(c) if isinstance(c, str) else c)
    return F.when(c == "", None).otherwise(c)


def parse_date(c):
    """Parse a string that is yyyy-MM-dd or dd/MM/yyyy. Anything else becomes null.

    The regex checks keep this safe in strict (ANSI) mode, where a failed to_date
    would raise an error instead of returning null.
    """
    c = clean_str(c)
    return (F.when(c.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(c, "yyyy-MM-dd"))
             .when(c.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(c, "dd/MM/yyyy")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## File readers and table writers

# COMMAND ----------

def read_csv(path, schema, glob=None):
    """CSV with header. The source marks missing values as \\N.

    glob: when reading a whole folder, only pick files matching this pattern (for example "*.csv"),
    so stray files such as Windows ':Zone.Identifier' leftovers are ignored.
    """
    reader = spark.read.option("header", True).option("nullValue", "\\N").schema(schema)
    if glob:
        reader = reader.option("pathGlobFilter", glob)
    return reader.csv(path)


def read_jsonl(path, schema):
    """JSON lines, one object per line. Unparseable lines go to _corrupt_record."""
    return spark.read.option("mode", "PERMISSIVE").schema(schema).json(path)


def read_json_multiline(path, schema, glob=None):
    """Pretty-printed JSON where each file holds an array of objects. See read_csv for glob."""
    reader = spark.read.option("multiLine", True).schema(schema)
    if glob:
        reader = reader.option("pathGlobFilter", glob)
    return reader.json(path)


def add_audit(df):
    """Add the ingestion timestamp and the file each row came from."""
    return (df.withColumn("ingestion_ts", F.current_timestamp())
              .withColumn("source_file", F.col("_metadata.file_path")))


def write_bronze(df, name, mode="overwrite"):
    full = f"{BRONZE}.{name}"
    writer = add_audit(df).write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(full)
    print(f"{full:<40} {spark.table(full).count():>6} rows")


def write_silver(df, name, mode="overwrite"):
    full = f"{SILVER}.{name}"
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(full)
    print(f"{full:<40} {spark.table(full).count():>6} rows")


def write_gold(df, name):
    full = f"{GOLD}.{name}"
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(full))
    print(f"{full:<40} {spark.table(full).count():>6} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas shared by the full load and the incremental loads

# COMMAND ----------

INSPECTION_SCHEMA = """
    inspection_id STRING, inspection_date STRING, component_id STRING, workorder_id STRING,
    method_id STRING, equipment_id STRING, facility_id STRING, inspector_id STRING,
    duration_min INT, result STRING, indication_count INT,
    max_indication_length_mm DOUBLE, max_indication_width_mm DOUBLE,
    next_inspection_due_hrs INT, _corrupt_record STRING
"""

FINDINGS_SCHEMA = """
    finding_id STRING, inspection_id STRING,
    indications ARRAY<STRUCT<indication_no: INT, location: STRING, type: STRING,
                             length_mm: DOUBLE, width_mm: DOUBLE, disposition: STRING>>
"""


def explode_findings(df):
    """Bronze holds one row per inspection with an array inside. Return one row per indication."""
    return (df.select("finding_id", "inspection_id", F.explode("indications").alias("i"))
              .select(clean_str("finding_id").alias("finding_id"),
                      clean_str("inspection_id").alias("inspection_id"),
                      F.col("i.indication_no").alias("indication_no"),
                      clean_str("i.location").alias("location"),
                      clean_str("i.type").alias("indication_type"),
                      F.col("i.length_mm").alias("length_mm"),
                      F.col("i.width_mm").alias("width_mm"),
                      clean_str("i.disposition").alias("disposition"))
              .dropDuplicates(["finding_id", "indication_no"]))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver validation rules for inspections
# MAGIC
# MAGIC clean_inspections takes bronze inspection rows and returns two DataFrames, valid and quarantine. It works in six steps. It normalises first: text is trimmed, empty strings become null, both date formats are parsed and the result is upper-cased. Then it deduplicates, keeping one row per inspection_id with the most recently ingested one winning. It repairs only what is certain, so a clean inspection (0 indications) with a null size gets size 0 and nothing else is guessed. Next it checks every foreign key with a true or false flag, using left joins against the silver dimensions, and validates each rule, adding every broken one to a reasons array so one row can have several. The last step splits the rows: no reasons means valid, any reason means quarantine, with the reasons joined into text.
# MAGIC
# MAGIC PT and VT inspections legitimately have no equipment, because they use consumables, so a missing equipment_id is only an error for UT, ET, MT and RT.

# COMMAND ----------

INSTRUMENT_METHODS = ["UT", "ET", "MT", "RT"]


def _key_flag(df, table, key, flag):
    keys = spark.table(f"{SILVER}.{table}").select(F.col(key).alias(f"_{flag}_k")).distinct()
    return (df.join(keys, df[key] == keys[f"_{flag}_k"], "left")
              .withColumn(flag, F.col(f"_{flag}_k").isNotNull()).drop(f"_{flag}_k"))


def clean_inspections(raw):
    """Return (valid, quarantine) for a bronze inspections DataFrame."""
    c = F.col

    # 1. normalise
    norm = raw.select(
        clean_str("inspection_id").alias("inspection_id"),
        parse_date("inspection_date").alias("inspection_date"),
        clean_str("inspection_date").alias("inspection_date_raw"),
        clean_str("component_id").alias("component_id"),
        clean_str("workorder_id").alias("workorder_id"),
        clean_str("method_id").alias("method_id"),
        clean_str("equipment_id").alias("equipment_id"),
        clean_str("facility_id").alias("facility_id"),
        clean_str("inspector_id").alias("inspector_id"),
        "duration_min",
        F.upper(clean_str("result")).alias("result"),
        "indication_count",
        "max_indication_length_mm",
        "max_indication_width_mm",
        "next_inspection_due_hrs",
        "ingestion_ts",
        "source_file",
        "_corrupt_record",
    )

    # 2. deduplicate: latest ingested row per inspection_id
    w = Window.partitionBy("inspection_id").orderBy(c("ingestion_ts").desc(), c("source_file"))
    deduped = norm.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    # 3. repair: zero indications means zero size, not unknown size
    fixed = deduped
    for col in ("max_indication_length_mm", "max_indication_width_mm"):
        fixed = fixed.withColumn(
            col, F.when((c("indication_count") == 0) & c(col).isNull(), F.lit(0.0)).otherwise(c(col)))

    # 4. referential checks
    checked = fixed
    for table, key, flag in [("components", "component_id", "ok_component"),
                             ("workorders", "workorder_id", "ok_workorder"),
                             ("facilities", "facility_id", "ok_facility"),
                             ("methods", "method_id", "ok_method"),
                             ("inspectors", "inspector_id", "ok_inspector"),
                             ("equipment", "equipment_id", "ok_equipment")]:
        checked = _key_flag(checked, table, key, flag)

    # 5. validation rules, every broken rule is listed
    reasons = F.filter(F.array(
        F.when(c("_corrupt_record").isNotNull(), F.lit("corrupt_record")),
        F.when(c("inspection_id").isNull(), F.lit("missing_inspection_id")),
        F.when(c("inspection_date").isNull(), F.lit("bad_date")),
        F.when(c("result").isNull() | ~c("result").isin("PASS", "FAIL"), F.lit("unknown_result")),
        F.when(c("duration_min").isNull() | (c("duration_min") <= 0), F.lit("invalid_duration")),
        F.when(c("method_id").isin(INSTRUMENT_METHODS) & c("equipment_id").isNull(), F.lit("missing_equipment")),
        F.when(c("equipment_id").isNotNull() & ~c("ok_equipment"), F.lit("orphan_equipment")),
        F.when((c("indication_count") > 0)
               & (c("max_indication_length_mm").isNull() | c("max_indication_width_mm").isNull()),
               F.lit("missing_measurements")),
        F.when(~c("ok_component"), F.lit("orphan_component")),
        F.when(~c("ok_workorder"), F.lit("orphan_workorder")),
        F.when(~c("ok_facility"), F.lit("orphan_facility")),
        F.when(~c("ok_method"), F.lit("orphan_method")),
        F.when(~c("ok_inspector"), F.lit("orphan_inspector")),
    ), lambda x: x.isNotNull())
    scored = checked.withColumn("reasons", reasons)

    # 6. split
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

    return valid, quarantine
