# measurement_events_v2 EDA — event_cd catalog (Lumina YARN Spark, HDFS parquet)
# Goal: enumerate every event_cd in the Success Library events table so we can map codes
# to campaigns (PCD, PCQ, PCL, VBA, VBU) and repeat the CRV-style vintage validation.
# Only known code so far: 'p_card_installmt_purch' (CRV).
# Trino twin of this EDA: schemas/measurement_events_v2_eda.sql
#
# `spark` is pre-initialized by Lumina — do not build a SparkSession.
# NOTE: path is dataversion=dev (same as CRV validation) — confirm whether a
# dataversion=prod sibling exists before wiring this into production measurement.

import pyspark.sql.functions as F

EVENTS_BASE = "/prod/16131/app/ZP10/lab/data/tde/measurement/dataversion=dev/measurement_events_v2/"

def pread(path, partition_key, partition_value=""):
    full_path = str(path) + str(partition_key) + "=" + str(partition_value) + "*"
    return spark.read.option("basePath", path).parquet(full_path)


# ============================================================
# CELL 1 — schema + sample sanity
# Proves: the path reads, columns are what the schema doc says
# (clnt_no, acct_no, event_cd, event_date), and what acct_no actually looks like.
# ============================================================
ev = pread(EVENTS_BASE, "event_date")
ev.printSchema()
print("Sample rows (expect acct_no as zero-padded ~23-char string):")
ev.select("clnt_no", "acct_no", "event_cd", "event_date").show(5, truncate=False)


# ============================================================
# CELL 2 — Q0: full event_cd catalog
# Proves: which codes exist, their volume, client/account coverage, history depth.
# min/max event_date tells us whether a code covers each campaign's deployment window.
# ============================================================
ev = pread(EVENTS_BASE, "event_date")
cat_pdf = (
    ev.groupBy("event_cd")
      .agg(F.count("*").alias("row_ct"),
           F.countDistinct("clnt_no").alias("clnt_ct"),
           F.countDistinct("acct_no").alias("acct_ct"),
           F.min(F.col("event_date").cast("string")).alias("min_event_date"),
           F.max(F.col("event_date").cast("string")).alias("max_event_date"))
      .orderBy(F.desc("row_ct"))
      .toPandas()
)
print(f"Q0: {len(cat_pdf)} distinct event_cd values in measurement_events_v2 (full history):")
print(cat_pdf.to_string(index=False))


# ============================================================
# CELL 3 — Q1: event_cd x month grid (2025-01 onward)
# Proves: continuity and recency per code — a code that stopped populating months ago
# can't validate an in-flight campaign. Counts only; pool downstream.
# ============================================================
ev = pread(EVENTS_BASE, "event_date", "202")
grid_pdf = (
    ev.withColumn("event_date_s", F.col("event_date").cast("string"))
      .filter(F.col("event_date_s") >= "2025-01-01")
      .withColumn("event_month", F.substring("event_date_s", 1, 7))
      .groupBy("event_cd", "event_month")
      .agg(F.count("*").alias("row_ct"),
           F.countDistinct("acct_no").alias("acct_ct"))
      .orderBy("event_cd", "event_month")
      .toPandas()
)
print(f"Q1: event_cd x month grid, {len(grid_pdf)} rows:")
print(grid_pdf.to_string(index=False))

# pivot for a quick continuity scan (blank = code absent that month)
print(grid_pdf.pivot(index="event_cd", columns="event_month", values="row_ct").to_string())


# ============================================================
# CELL 4 — Q2: key format profile per event_cd (2026 partitions)
# Proves: how acct_no / clnt_no are populated per code, so we know the join-key
# normalization needed per campaign. CRV trap: acct_no is zero-padded varchar and
# only joined after CAST to DECIMAL(38,0) on both sides. acct_no_len = NaN row = nulls.
# ============================================================
ev = pread(EVENTS_BASE, "event_date", "2026")
key_pdf = (
    ev.groupBy("event_cd", F.length("acct_no").alias("acct_no_len"))
      .agg(F.count("*").alias("row_ct"),
           F.sum(F.when(F.col("clnt_no").isNull(), 1).otherwise(0)).alias("clnt_null_ct"))
      .orderBy("event_cd", "acct_no_len")
      .toPandas()
)
print("Q2: acct_no length profile per event_cd (2026):")
print(key_pdf.to_string(index=False))
