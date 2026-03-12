# %% [markdown]
# # IMT Campaign Pipeline — IRI & IPC
# Vintage curves for Interac Money Transfer success events
# Mirrors VVD v3 pipeline structure

# %% Cell 1: Configuration
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import *
from pyspark import StorageLevel
import pandas as pd
import base64
from IPython.display import HTML, display

spark = SparkSession.builder \
    .appName("IMT Pipeline") \
    .master("yarn") \
    .enableHiveSupport() \
    .getOrCreate()

# --- Source paths ---
TACTIC_EVNT_HIST_BASE = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
SUCCESS_ROOT = "/prod/sz/tsz/00050/data/DDNTA_EXT_CHNL_EVNT"

# --- Campaign config ---
YEARS = [2025, 2026]
IMT_MNES = ["IRI", "IPC"]
ACTION_GROUP = "TG4"
CONTROL_GROUP = "TG7"
DATA_END_DATE = "2026-03-01"

# Success event filters (from screenshots)
ACTVY_TYP_CD_FILTER = "031"
CHNL_TYP_CD_FILTER = ["021", "034"]
SRC_DTA_STORE_CD_FILTER = ["139", "140"]

# Measurement window
# IRI is trigger-based (30-day window), IPC is 90-day window
MNE_WINDOWS = {"IRI": 30, "IPC": 90}
MAX_DAYS = max(MNE_WINDOWS.values())  # 90, used for data pull range
MAX_WEEKS = 12  # 0..12 = 13 weeks

success_paths = [f"{SUCCESS_ROOT}/CAPTR_DT={y}*" for y in YEARS]

print("Configuration loaded.")
print(f"Campaigns: {IMT_MNES}")
print(f"Years: {YEARS}")
print(f"Action: {ACTION_GROUP}, Control: {CONTROL_GROUP}")
print(f"Data end date: {DATA_END_DATE}")


# %% Cell 2: M1 — Experiment Population
# Load tactic event history, derive MNE from TACTIC_ID, filter to IMT campaigns

raw_tactic = spark.read.option("basePath", TACTIC_EVNT_HIST_BASE).parquet(TACTIC_EVNT_HIST_BASE + "*")

tactic_df = (
    raw_tactic
    .filter(F.substring(F.col("TACTIC_ID"), 8, 3).isin(IMT_MNES))
    .withColumn("MNE", F.substring(F.col("TACTIC_ID"), 8, 3))
    .withColumn("CLNT_NO", F.regexp_replace(F.trim(F.col("TACTIC_EVNT_ID")), "^0+", ""))
    .withColumn("TST_GRP_CD", F.trim(F.col("TST_GRP_CD")))
    .withColumn("RPT_GRP_CD", F.trim(F.col("RPT_GRP_CD")))
    .filter(F.col("TST_GRP_CD").isin(ACTION_GROUP, CONTROL_GROUP))
    .filter(F.col("TREATMT_STRT_DT") >= "2025-01-01")
    .filter(F.col("TREATMT_STRT_DT") < DATA_END_DATE)
    .withColumn("WINDOW_DAYS", F.datediff(F.col("TREATMT_END_DT"), F.col("TREATMT_STRT_DT")))
    .withColumn("COHORT", F.date_format(F.col("TREATMT_STRT_DT"), "yyyy-MM"))
    .select(
        "CLNT_NO", "TACTIC_ID", "MNE", "TST_GRP_CD", "RPT_GRP_CD",
        "TREATMT_STRT_DT", "TREATMT_END_DT", "TREATMT_MN",
        "TACTIC_CELL_CD", "WINDOW_DAYS", "COHORT"
    )
    .distinct()
)

tactic_df.persist(StorageLevel.MEMORY_AND_DISK)

pop_count = tactic_df.count()
print(f"Experiment population: {pop_count:,} rows")

# Population breakdown
pop_summary = (
    tactic_df
    .groupBy("MNE", "COHORT", "TST_GRP_CD")
    .agg(F.countDistinct("CLNT_NO").alias("clients"))
    .orderBy("MNE", "COHORT", "TST_GRP_CD")
)
print("\nPopulation by MNE x Cohort x Test Group:")
pop_summary.show(100, truncate=False)


# %% Cell 3: Load Success Events
# Read external channel events, filter to IMT success criteria

raw_events = spark.read.option("basePath", SUCCESS_ROOT).parquet(*success_paths)

events_df = (
    raw_events
    .withColumn("CAPTR_DT_DATE", F.col("CAPTR_DT"))  # CAPTR_DT is already a date in schema
    .withColumn("EVENT_DATE", F.coalesce(F.col("EVNT_STS_END_DT"), F.col("CAPTR_DT_DATE")))
    .withColumn("CLNT_NO", F.col("CLNT_NO").cast("string"))
    # Trim and normalize filter fields
    .withColumn("ACTVY_TYP_CD", F.trim(F.col("ACTVY_TYP_CD")))
    .withColumn("CHNL_TYP_CD", F.trim(F.col("CHNL_TYP_CD")))
    .withColumn("SRC_DTA_STORE_CD", F.trim(F.col("SRC_DTA_STORE_CD")))
    # Apply filters
    .filter(F.col("ACTVY_TYP_CD") == ACTVY_TYP_CD_FILTER)
    .filter(F.col("CHNL_TYP_CD").isin(CHNL_TYP_CD_FILTER))
    .filter(F.col("SRC_DTA_STORE_CD").isin(SRC_DTA_STORE_CD_FILTER))
    .filter(F.col("CAPTR_DT_DATE").isNotNull())
    .filter(F.col("EVENT_DATE").isNotNull())
)

# Pre-filter to experiment clients only (massive performance boost)
events_df = events_df.join(
    tactic_df.select("CLNT_NO").distinct(),
    on="CLNT_NO",
    how="left_semi"
)

events_df.persist(StorageLevel.MEMORY_AND_DISK)
event_count = events_df.count()
print(f"Success events (filtered): {event_count:,}")


# %% Cell 4: Join Events to Tactics & Compute Day/Week Metrics

# Prepare tactic keys
tactic_keys = (
    tactic_df
    .select(
        "CLNT_NO", "TACTIC_ID", "MNE",
        "TREATMT_STRT_DT", "TREATMT_END_DT"
    )
    .withColumn("TREATMT_STRT_DT", F.to_date("TREATMT_STRT_DT"))
    .withColumn("TREATMT_END_DT", F.to_date("TREATMT_END_DT"))
    .dropDuplicates(["CLNT_NO", "TACTIC_ID", "TREATMT_STRT_DT"])
)

# Join events to tactics where event date is within treatment window
joined = (
    events_df.alias("e")
    .join(tactic_keys.alias("a"), on="CLNT_NO", how="inner")
    .where(
        (F.col("e.EVENT_DATE") >= F.col("a.TREATMT_STRT_DT")) &
        (F.col("e.EVENT_DATE") <= F.col("a.TREATMT_END_DT"))
    )
)

# Per-MNE window: IRI=30d, IPC=90d
_mne_max = F.lit(MAX_DAYS)
for _mne, _days in MNE_WINDOWS.items():
    _mne_max = F.when(F.col("a.MNE") == _mne, F.lit(_days)).otherwise(_mne_max)

with_days = (
    joined
    .withColumn("DAYS_SINCE_START", F.datediff(F.col("e.EVENT_DATE"), F.col("a.TREATMT_STRT_DT")))
    .withColumn("MNE_MAX_DAYS", _mne_max)
    .filter((F.col("DAYS_SINCE_START") >= 0) & (F.col("DAYS_SINCE_START") <= F.col("MNE_MAX_DAYS")))
    .withColumn("WEEK_INDEX", F.floor(F.col("DAYS_SINCE_START") / 7))
)

# Base DataFrame with unqualified column names
base = with_days.select(
    F.col("CLNT_NO").alias("CLNT_NO"),
    F.col("a.MNE").alias("MNE"),
    F.col("a.TACTIC_ID").alias("TACTIC_ID"),
    F.col("a.TREATMT_STRT_DT").alias("TREATMT_STRT_DT"),
    F.col("e.EVENT_DATE").alias("EVENT_DATE"),
    F.col("WEEK_INDEX").alias("WEEK_INDEX"),
    F.col("DAYS_SINCE_START").alias("DAYS_SINCE_START")
)

# Weekly counts of distinct events per week
weekly_counts = (
    base
    .groupBy("CLNT_NO", "MNE", "TACTIC_ID", "TREATMT_STRT_DT")
    .pivot("WEEK_INDEX", list(range(13)))  # 0..12
    .agg(F.countDistinct("EVENT_DATE"))
    .fillna(0)
)

# Non-accumulated weekly counts with aliases
non_acc_exprs = [F.col(str(i)).cast("long").alias(f"IMT_WK_{i:02d}") for i in range(13)]

# Accumulated weekly counts
cum_exprs = []
running_sum = F.lit(0).cast("long")
for i in range(13):
    running_sum = running_sum + F.col(str(i)).cast("long")
    cum_exprs.append(running_sum.alias(f"IMT_WK_CUM_{i:02d}"))

# Combine weekly counts and accumulated counts
weekly_shaped = (
    weekly_counts
    .select(
        "CLNT_NO", "MNE", "TACTIC_ID", "TREATMT_STRT_DT",
        *non_acc_exprs,
        *cum_exprs
    )
)

# Success statistics (count of events and first success date)
success_stats = (
    base
    .groupBy("CLNT_NO", "MNE", "TACTIC_ID", "TREATMT_STRT_DT")
    .agg(
        F.countDistinct("EVENT_DATE").alias("IMT_0_90"),
        F.min("EVENT_DATE").alias("first_success_date")
    )
    .withColumn("success_flag", F.when(F.col("IMT_0_90") > 0, F.lit(1)).otherwise(F.lit(0)))
)

# Final join to combine success stats and weekly metrics
result = (
    success_stats
    .join(weekly_shaped, on=["CLNT_NO", "MNE", "TACTIC_ID", "TREATMT_STRT_DT"], how="left")
    .fillna(0, subset=[f"IMT_WK_{i:02d}" for i in range(13)] + [f"IMT_WK_CUM_{i:02d}" for i in range(13)])
)

# Now join back the tactic metadata (TST_GRP_CD, RPT_GRP_CD, COHORT, WINDOW_DAYS, etc.)
result_df = (
    tactic_df
    .join(
        result,
        on=["CLNT_NO", "MNE", "TACTIC_ID", "TREATMT_STRT_DT"],
        how="left"
    )
    .fillna(0, subset=["IMT_0_90", "success_flag"] + [f"IMT_WK_{i:02d}" for i in range(13)] + [f"IMT_WK_CUM_{i:02d}" for i in range(13)])
)

result_df.persist(StorageLevel.MEMORY_AND_DISK)

events_df.unpersist()

total = result_df.count()
success = result_df.filter(F.col("success_flag") == 1).count()
print(f"Result: {total:,} rows, {success:,} successes ({100*success/total:.2f}%)")


# %% Cell 4b: Email Engagement — Send/Open/Click/Unsub
# Source: DT3V01.VENDOR_FEEDBACK_MASTER + VENDOR_FEEDBACK_EVENT (via EDW)
# Query ALL tactic IDs — join key is TREATMENT_ID = TACTIC_ID, no channel pre-filter needed

print("=== Loading Email Engagement ===")

# Query ALL tactic IDs — join key is TREATMENT_ID = TACTIC_ID, no channel pre-filter needed
# (TACTIC_CELL_CD may be empty for IMT campaigns; ADDNL_DECISN_DATA1 has channel info
#  but the vendor feedback tables already filter to email-only dispositions)
em_tactics = (
    result_df
    .select("TACTIC_ID")
    .distinct()
    .toPandas()["TACTIC_ID"]
    .tolist()
)

print(f"Querying email metrics for {len(em_tactics)} unique tactic IDs...")
print(f"  Sample tactic IDs: {em_tactics[:5]}")

# Diagnostic: check if vendor feedback has ANY data for our tactics
if len(em_tactics) > 0:
    diag_list = "','".join(em_tactics[:10])
    diag_sql = f"""
        SELECT COUNT(*) AS cnt, COUNT(DISTINCT TREATMENT_ID) AS tactic_cnt
        FROM DT3V01.VENDOR_FEEDBACK_MASTER
        WHERE TREATMENT_ID IN ('{diag_list}')
    """
    try:
        cursor = EDW.cursor()
        cursor.execute(diag_sql)
        diag_row = cursor.fetchone()
        cursor.close()
        print(f"  DIAGNOSTIC: vendor_feedback has {diag_row[0]} rows for first 10 tactics ({diag_row[1]} matched)")
        if diag_row[0] == 0:
            print("  WARNING: No vendor feedback data found. TREATMENT_ID may not match TACTIC_ID format.")
            print(f"  Check: SELECT DISTINCT TREATMENT_ID FROM DT3V01.VENDOR_FEEDBACK_MASTER WHERE TREATMENT_ID LIKE '%IRI%' OR TREATMENT_ID LIKE '%IPC%' LIMIT 10")
    except Exception as e:
        print(f"  DIAGNOSTIC query failed: {e}")

if len(em_tactics) > 0:
    # Query EDW for email feedback in batches (Trino has query size limits)
    email_rows = []
    batch_size = 50

    for i in range(0, len(em_tactics), batch_size):
        batch = em_tactics[i:i + batch_size]
        tactic_id_list = "','".join(batch)

        email_sql = f"""
            SELECT
                CAST(FEEDBACK_MASTER.CLNT_NO AS VARCHAR(20)) AS CLNT_NO,
                FEEDBACK_MASTER.TREATMENT_ID,
                MAX(CASE WHEN disposition_cd = 1 THEN 1 ELSE 0 END) AS EMAIL_SENT,
                MAX(CASE WHEN disposition_cd = 2 THEN 1 ELSE 0 END) AS EMAIL_OPENED,
                MAX(CASE WHEN disposition_cd = 3 THEN 1 ELSE 0 END) AS EMAIL_CLICKED,
                MAX(CASE WHEN disposition_cd = 4 THEN 1 ELSE 0 END) AS EMAIL_UNSUBSCRIBED,
                MAX(CASE WHEN disposition_cd = 1 THEN CAST(disposition_dt_tm AS DATE) END) AS EMAIL_SENT_DT,
                MAX(CASE WHEN disposition_cd = 2 THEN CAST(disposition_dt_tm AS DATE) END) AS EMAIL_OPENED_DT,
                MAX(CASE WHEN disposition_cd = 3 THEN CAST(disposition_dt_tm AS DATE) END) AS EMAIL_CLICKED_DT,
                MAX(CASE WHEN disposition_cd = 4 THEN CAST(disposition_dt_tm AS DATE) END) AS EMAIL_UNSUBSCRIBED_DT
            FROM DT3V01.VENDOR_FEEDBACK_MASTER FEEDBACK_MASTER
            INNER JOIN DT3V01.VENDOR_FEEDBACK_EVENT FEEDBACK_EVENT
                ON FEEDBACK_MASTER.consumer_id_hashed = FEEDBACK_EVENT.consumer_id_hashed
                AND FEEDBACK_MASTER.TREATMENT_ID = FEEDBACK_EVENT.TREATMENT_ID
            WHERE FEEDBACK_MASTER.TREATMENT_ID IN ('{tactic_id_list}')
            GROUP BY FEEDBACK_MASTER.CLNT_NO, FEEDBACK_MASTER.TREATMENT_ID
        """

        cursor = EDW.cursor()
        cursor.execute(email_sql)
        batch_rows = cursor.fetchall()
        cursor.close()

        for r in batch_rows:
            email_rows.append({
                "E_CLNT_NO": str(r[0]).strip().lstrip('0'),
                "E_TREATMENT_ID": str(r[1]).strip(),
                "EMAIL_SENT": int(r[2]) if r[2] else 0,
                "EMAIL_OPENED": int(r[3]) if r[3] else 0,
                "EMAIL_CLICKED": int(r[4]) if r[4] else 0,
                "EMAIL_UNSUBSCRIBED": int(r[5]) if r[5] else 0,
                "EMAIL_SENT_DT": r[6],
                "EMAIL_OPENED_DT": r[7],
                "EMAIL_CLICKED_DT": r[8],
                "EMAIL_UNSUBSCRIBED_DT": r[9]
            })

        print(f"  Batch {i//batch_size + 1}: {len(batch)} tactics → {len(batch_rows)} rows")

    print(f"Total email rows: {len(email_rows)}")

    if len(email_rows) > 0:
        email_pdf = pd.DataFrame(email_rows)
        email_sdf = spark.createDataFrame(email_pdf)

        # Join email data to result_df
        old_result = result_df
        result_df = (
            old_result
            .join(
                email_sdf,
                (F.col("CLNT_NO") == F.col("E_CLNT_NO")) &
                (F.col("TACTIC_ID") == F.col("E_TREATMENT_ID")),
                "left"
            )
            .drop("E_CLNT_NO", "E_TREATMENT_ID")
            .withColumn("EMAIL_SENT", F.coalesce(F.col("EMAIL_SENT"), F.lit(0)))
            .withColumn("EMAIL_OPENED", F.coalesce(F.col("EMAIL_OPENED"), F.lit(0)))
            .withColumn("EMAIL_CLICKED", F.coalesce(F.col("EMAIL_CLICKED"), F.lit(0)))
            .withColumn("EMAIL_UNSUBSCRIBED", F.coalesce(F.col("EMAIL_UNSUBSCRIBED"), F.lit(0)))
        )

        result_df.persist(StorageLevel.MEMORY_AND_DISK)
        old_result.unpersist()

        # Email summary
        email_summary = result_df.agg(
            F.sum("EMAIL_SENT").alias("sent"),
            F.sum("EMAIL_OPENED").alias("opened"),
            F.sum("EMAIL_CLICKED").alias("clicked"),
            F.sum("EMAIL_UNSUBSCRIBED").alias("unsubscribed")
        ).toPandas()
        print("\nEmail Summary:")
        print(email_summary.to_string(index=False))

        sent_total = int(email_summary["sent"].iloc[0])
        if sent_total > 0:
            print(f"  Open rate: {int(email_summary['opened'].iloc[0])/sent_total*100:.1f}%")
            print(f"  Click rate: {int(email_summary['clicked'].iloc[0])/sent_total*100:.1f}%")
            print(f"  Unsub rate: {int(email_summary['unsubscribed'].iloc[0])/sent_total*100:.1f}%")
    else:
        # No email data found, add zero columns
        result_df = (
            result_df
            .withColumn("EMAIL_SENT", F.lit(0))
            .withColumn("EMAIL_OPENED", F.lit(0))
            .withColumn("EMAIL_CLICKED", F.lit(0))
            .withColumn("EMAIL_UNSUBSCRIBED", F.lit(0))
            .withColumn("EMAIL_SENT_DT", F.lit(None).cast("date"))
            .withColumn("EMAIL_OPENED_DT", F.lit(None).cast("date"))
            .withColumn("EMAIL_CLICKED_DT", F.lit(None).cast("date"))
            .withColumn("EMAIL_UNSUBSCRIBED_DT", F.lit(None).cast("date"))
        )
else:
    print("No tactic IDs found — skipping email metrics.")
    result_df = (
        result_df
        .withColumn("EMAIL_SENT", F.lit(0))
        .withColumn("EMAIL_OPENED", F.lit(0))
        .withColumn("EMAIL_CLICKED", F.lit(0))
        .withColumn("EMAIL_UNSUBSCRIBED", F.lit(0))
        .withColumn("EMAIL_SENT_DT", F.lit(None).cast("date"))
        .withColumn("EMAIL_OPENED_DT", F.lit(None).cast("date"))
        .withColumn("EMAIL_CLICKED_DT", F.lit(None).cast("date"))
        .withColumn("EMAIL_UNSUBSCRIBED_DT", F.lit(None).cast("date"))
    )


# %% Cell 5: Population & SRM Overview

# Population by MNE x TST_GRP_CD
print("=== Population by MNE x Test Group ===")
result_df.groupBy("MNE", "TST_GRP_CD").agg(
    F.countDistinct("CLNT_NO").alias("clients"),
    F.count("*").alias("rows"),
    F.sum("success_flag").alias("successes"),
    (F.sum("success_flag") / F.count("*") * 100).cast("decimal(5,2)").alias("success_rate_pct")
).orderBy("MNE", "TST_GRP_CD").show(truncate=False)

# SRM check: Action vs Control ratio
print("=== SRM Check: Expected ~ratio Action:Control ===")
result_df.groupBy("MNE").pivot("TST_GRP_CD").agg(
    F.countDistinct("CLNT_NO")
).show(truncate=False)

# By cohort
print("=== Population by Cohort ===")
result_df.groupBy("MNE", "COHORT", "TST_GRP_CD").agg(
    F.countDistinct("CLNT_NO").alias("clients"),
    F.sum("success_flag").alias("successes"),
    (F.sum("success_flag") / F.count("*") * 100).cast("decimal(5,2)").alias("rate_pct")
).orderBy("MNE", "COHORT", "TST_GRP_CD").show(200, truncate=False)


# %% Cell 6: Campaign Performance — Lift & Significance

print("=== Campaign Lift: Action vs Control ===")

perf = (
    result_df
    .groupBy("MNE", "COHORT", "TST_GRP_CD")
    .agg(
        F.countDistinct("CLNT_NO").alias("clients"),
        F.sum("success_flag").alias("successes")
    )
)
perf_pd = perf.toPandas()

# Compute lift per MNE x COHORT
import numpy as np
from scipy import stats

lift_rows = []
for (mne, cohort), grp in perf_pd.groupby(["MNE", "COHORT"]):
    action = grp[grp["TST_GRP_CD"] == ACTION_GROUP]
    control = grp[grp["TST_GRP_CD"] == CONTROL_GROUP]
    if len(action) == 0 or len(control) == 0:
        continue
    a_n, a_s = int(action["clients"].iloc[0]), int(action["successes"].iloc[0])
    c_n, c_s = int(control["clients"].iloc[0]), int(control["successes"].iloc[0])
    a_rate = a_s / a_n if a_n > 0 else 0
    c_rate = c_s / c_n if c_n > 0 else 0
    lift = a_rate - c_rate
    # Two-proportion z-test
    p_pool = (a_s + c_s) / (a_n + c_n) if (a_n + c_n) > 0 else 0
    se = np.sqrt(p_pool * (1 - p_pool) * (1/a_n + 1/c_n)) if p_pool > 0 else 0
    z = lift / se if se > 0 else 0
    p_val = 2 * (1 - stats.norm.cdf(abs(z)))
    sig = "***" if p_val < 0.01 else "**" if p_val < 0.05 else "*" if p_val < 0.1 else ""
    lift_rows.append({
        "MNE": mne, "COHORT": cohort,
        "action_n": a_n, "action_successes": a_s, "action_rate": round(a_rate * 100, 2),
        "control_n": c_n, "control_successes": c_s, "control_rate": round(c_rate * 100, 2),
        "lift_pct": round(lift * 100, 2), "p_value": round(p_val, 4), "sig": sig
    })

lift_df = pd.DataFrame(lift_rows)
print(lift_df.to_string(index=False))

# Overall lift by MNE
print("\n=== Overall Lift by MNE ===")
for mne in IMT_MNES:
    subset = perf_pd[perf_pd["MNE"] == mne]
    action = subset[subset["TST_GRP_CD"] == ACTION_GROUP]
    control = subset[subset["TST_GRP_CD"] == CONTROL_GROUP]
    a_n, a_s = int(action["clients"].sum()), int(action["successes"].sum())
    c_n, c_s = int(control["clients"].sum()), int(control["successes"].sum())
    a_rate = a_s / a_n if a_n > 0 else 0
    c_rate = c_s / c_n if c_n > 0 else 0
    lift = a_rate - c_rate
    print(f"  {mne}: Action {a_rate*100:.2f}% vs Control {c_rate*100:.2f}% → Lift {lift*100:.2f}pp")


# %% Cell 7: Vintage Curves
# Day-by-day cumulative success rate, matching VVD v3 format:
# MNE | COHORT | TST_GRP_CD | RPT_GRP_CD | METRIC | DAY | WINDOW_DAYS | CLIENT_CNT | SUCCESS_CNT | RATE

print("=== Building Vintage Curves ===")

# Download helper (inline, no def)
# We'll use this pattern at the end

# Get DAYS_TO_SUCCESS for each client
days_to_success = (
    result_df
    .filter(F.col("success_flag") == 1)
    .withColumn("DAYS_TO_SUCCESS", F.datediff(F.col("first_success_date"), F.col("TREATMT_STRT_DT")))
    .select("CLNT_NO", "MNE", "TACTIC_ID", "TREATMT_STRT_DT", "TST_GRP_CD", "RPT_GRP_CD", "COHORT", "WINDOW_DAYS", "DAYS_TO_SUCCESS")
)

# Group dimensions for vintage curves
group_cols = ["MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD"]

# Client counts per group
client_counts = (
    result_df
    .groupBy(*group_cols)
    .agg(
        F.countDistinct("CLNT_NO").alias("CLIENT_CNT"),
        F.expr("percentile_approx(WINDOW_DAYS, 0.5)").alias("WINDOW_DAYS_MEDIAN")
    )
)

# Generate day-by-day success counts
# Create a days reference (0 to MAX_DAYS)
days_ref = spark.range(0, MAX_DAYS + 1).withColumnRenamed("id", "DAY")

# Cross join groups with days
groups_with_days = client_counts.crossJoin(days_ref)

# For each group x day, count successes where DAYS_TO_SUCCESS <= DAY
success_by_day = (
    days_to_success
    .groupBy(*group_cols, "DAYS_TO_SUCCESS")
    .agg(F.countDistinct("CLNT_NO").alias("day_successes"))
)

# Window to compute cumulative sum
w = Window.partitionBy(*group_cols).orderBy("DAYS_TO_SUCCESS").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cumulative_successes = (
    success_by_day
    .withColumn("CUM_SUCCESSES", F.sum("day_successes").over(w))
    .withColumnRenamed("DAYS_TO_SUCCESS", "DAY")
)

# Join with groups_with_days to fill in all days
vintage_raw = (
    groups_with_days
    .join(
        cumulative_successes.select(*group_cols, "DAY", "CUM_SUCCESSES"),
        on=group_cols + ["DAY"],
        how="left"
    )
)

# Forward-fill cumulative successes (carry forward last known value)
w_fill = Window.partitionBy(*group_cols).orderBy("DAY").rowsBetween(Window.unboundedPreceding, Window.currentRow)

vintage_filled = (
    vintage_raw
    .withColumn("SUCCESS_CNT", F.last(F.col("CUM_SUCCESSES"), ignorenulls=True).over(w_fill))
    .fillna(0, subset=["SUCCESS_CNT"])
    .withColumn("SUCCESS_CNT", F.col("SUCCESS_CNT").cast("long"))
    .withColumn("RATE", (F.col("SUCCESS_CNT") / F.col("CLIENT_CNT") * 100).cast("decimal(6,2)"))
    .withColumn("METRIC", F.lit("imt_success"))
    .withColumn("WINDOW_DAYS", F.col("WINDOW_DAYS_MEDIAN"))
    .select(
        "MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD", "METRIC",
        "DAY", "WINDOW_DAYS", "CLIENT_CNT", "SUCCESS_CNT", "RATE"
    )
    .orderBy("MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD", "DAY")
)

# Filter to per-MNE window (IRI keeps days 0-30, IPC keeps 0-90)
_vint_max = F.lit(MAX_DAYS)
for _mne, _days in MNE_WINDOWS.items():
    _vint_max = F.when(F.col("MNE") == _mne, F.lit(_days)).otherwise(_vint_max)

vintage_filled = vintage_filled.filter(F.col("DAY") <= _vint_max)

vintage_pd = vintage_filled.toPandas()

print(f"Vintage curves: {len(vintage_pd):,} rows")
print(f"Groups: {vintage_pd.groupby(['MNE', 'COHORT', 'TST_GRP_CD']).ngroups}")
print("\nSample (first 20 rows):")
print(vintage_pd.head(20).to_string(index=False))

# --- Email Vintage Curves ---
# Add email_sent, email_open, email_click, email_unsub as separate METRIC values
print("\n--- Building Email Vintage Curves ---")

email_metrics = [
    ("email_sent", "EMAIL_SENT", "EMAIL_SENT_DT"),
    ("email_open", "EMAIL_OPENED", "EMAIL_OPENED_DT"),
    ("email_click", "EMAIL_CLICKED", "EMAIL_CLICKED_DT"),
    ("email_unsub", "EMAIL_UNSUBSCRIBED", "EMAIL_UNSUBSCRIBED_DT"),
]

email_vintage_parts = []

# Use full population — vendor feedback tables already filter to email-only dispositions
email_result = result_df
email_pop = email_result.count()
print(f"Email vintage population (all tactics): {email_pop:,}")

if email_pop > 0:
    for metric_name, flag_col, date_col in email_metrics:
        # Client counts per group (email population only)
        email_client_counts = (
            email_result
            .groupBy(*group_cols)
            .agg(
                F.countDistinct("CLNT_NO").alias("CLIENT_CNT"),
                F.expr("percentile_approx(WINDOW_DAYS, 0.5)").alias("WINDOW_DAYS_MEDIAN")
            )
        )

        # Days to email event
        email_days = (
            email_result
            .filter(F.col(flag_col) == 1)
            .filter(F.col(date_col).isNotNull())
            .withColumn("DAYS_TO_SUCCESS", F.datediff(F.col(date_col), F.col("TREATMT_STRT_DT")))
            .filter((F.col("DAYS_TO_SUCCESS") >= 0))  # per-MNE window applied after vintage generation
            .select("CLNT_NO", *group_cols, "DAYS_TO_SUCCESS")
        )

        # Success by day
        em_success_by_day = (
            email_days
            .groupBy(*group_cols, "DAYS_TO_SUCCESS")
            .agg(F.countDistinct("CLNT_NO").alias("day_successes"))
        )

        # Cumulative
        w_em = Window.partitionBy(*group_cols).orderBy("DAYS_TO_SUCCESS").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        em_cumulative = (
            em_success_by_day
            .withColumn("CUM_SUCCESSES", F.sum("day_successes").over(w_em))
            .withColumnRenamed("DAYS_TO_SUCCESS", "DAY")
        )

        # Cross join groups with days and join
        em_groups_with_days = email_client_counts.crossJoin(days_ref)

        em_vintage_raw = (
            em_groups_with_days
            .join(
                em_cumulative.select(*group_cols, "DAY", "CUM_SUCCESSES"),
                on=group_cols + ["DAY"],
                how="left"
            )
        )

        # Forward-fill
        w_em_fill = Window.partitionBy(*group_cols).orderBy("DAY").rowsBetween(Window.unboundedPreceding, Window.currentRow)

        em_vintage_filled = (
            em_vintage_raw
            .withColumn("SUCCESS_CNT", F.last(F.col("CUM_SUCCESSES"), ignorenulls=True).over(w_em_fill))
            .fillna(0, subset=["SUCCESS_CNT"])
            .withColumn("SUCCESS_CNT", F.col("SUCCESS_CNT").cast("long"))
            .withColumn("RATE", (F.col("SUCCESS_CNT") / F.col("CLIENT_CNT") * 100).cast("decimal(6,2)"))
            .withColumn("METRIC", F.lit(metric_name))
            .withColumn("WINDOW_DAYS", F.col("WINDOW_DAYS_MEDIAN"))
            .select(
                "MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD", "METRIC",
                "DAY", "WINDOW_DAYS", "CLIENT_CNT", "SUCCESS_CNT", "RATE"
            )
        )

        # Filter to per-MNE window
        _em_vint_max = F.lit(MAX_DAYS)
        for _mne, _days in MNE_WINDOWS.items():
            _em_vint_max = F.when(F.col("MNE") == _mne, F.lit(_days)).otherwise(_em_vint_max)
        em_vintage_filled = em_vintage_filled.filter(F.col("DAY") <= _em_vint_max)

        email_vintage_parts.append(em_vintage_filled)
        print(f"  {metric_name}: done")

    # Union all email curves
    from functools import reduce
    email_vintage_union = reduce(lambda a, b: a.unionAll(b), email_vintage_parts)
    email_vintage_pd = email_vintage_union.orderBy("MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD", "METRIC", "DAY").toPandas()

    # Combine with IMT success curves
    vintage_pd = pd.concat([vintage_pd, email_vintage_pd], ignore_index=True)
    print(f"\nCombined vintage curves: {len(vintage_pd):,} rows (IMT success + email)")
else:
    print("No email data found — skipping email vintage curves.")


# %% Cell 8: Export CSV with Download Link

csv_data = vintage_pd.to_csv(index=False)
size_mb = len(csv_data.encode('utf-8')) / (1024 * 1024)

if size_mb > 50:
    print(f"Data too large ({size_mb:.1f} MB). Filter before exporting.")
else:
    b64 = base64.b64encode(csv_data.encode()).decode()
    filename = "imt_vintage_curves.csv"
    link = f'<a download="{filename}" href="data:text/csv;base64,{b64}" target="_blank" style="font-size:16px; padding:10px 20px; background:#264f78; color:white; text-decoration:none; border-radius:4px;">📥 Download {filename} ({size_mb:.1f} MB)</a>'
    display(HTML(link))
    print(f"\n{filename}: {len(vintage_pd):,} rows, {size_mb:.1f} MB")

# Also save to HDFS as backup
try:
    import os
    hdfs_path = "/user/427966379/eda_output/imt_vintage_curves.csv"
    local_path = "/tmp/imt_vintage_curves.csv"
    vintage_pd.to_csv(local_path, index=False)
    os.system(f"hdfs dfs -mkdir -p /user/427966379/eda_output")
    os.system(f"hdfs dfs -put -f {local_path} {hdfs_path}")
    print(f"HDFS backup: {hdfs_path}")
except Exception as e:
    print(f"HDFS backup failed: {e}")


# %% Cell 9: Mega Output — Summary by MNE x Cohort x RPT_GRP_CD

print("=== Mega Output: MNE x Cohort x RPT_GRP_CD ===")

mega = (
    result_df
    .groupBy("MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD")
    .agg(
        F.countDistinct("CLNT_NO").alias("clients"),
        F.sum("success_flag").alias("successes"),
        (F.sum("success_flag") / F.count("*") * 100).cast("decimal(5,2)").alias("rate_pct"),
        F.avg("IMT_0_90").cast("decimal(6,2)").alias("avg_events_per_client"),
        F.avg("WINDOW_DAYS").cast("decimal(6,1)").alias("avg_window_days"),
        F.sum("EMAIL_SENT").alias("email_sent"),
        F.sum("EMAIL_OPENED").alias("email_opened"),
        F.sum("EMAIL_CLICKED").alias("email_clicked"),
        F.sum("EMAIL_UNSUBSCRIBED").alias("email_unsubscribed")
    )
    .withColumn("email_open_rate",
        F.when(F.col("email_sent") > 0,
               (F.col("email_opened") / F.col("email_sent") * 100).cast("decimal(5,2)"))
        .otherwise(F.lit(None)))
    .withColumn("email_click_rate",
        F.when(F.col("email_sent") > 0,
               (F.col("email_clicked") / F.col("email_sent") * 100).cast("decimal(5,2)"))
        .otherwise(F.lit(None)))
    .withColumn("email_unsub_rate",
        F.when(F.col("email_sent") > 0,
               (F.col("email_unsubscribed") / F.col("email_sent") * 100).cast("decimal(5,2)"))
        .otherwise(F.lit(None)))
    .orderBy("MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD")
)

mega_pd = mega.toPandas()
print(mega_pd.to_string(index=False))

# Download mega output
csv_data_mega = mega_pd.to_csv(index=False)
size_mb_mega = len(csv_data_mega.encode('utf-8')) / (1024 * 1024)
b64_mega = base64.b64encode(csv_data_mega.encode()).decode()
filename_mega = "imt_mega_output.csv"
link_mega = f'<a download="{filename_mega}" href="data:text/csv;base64,{b64_mega}" target="_blank" style="font-size:16px; padding:10px 20px; background:#264f78; color:white; text-decoration:none; border-radius:4px;">📥 Download {filename_mega} ({size_mb_mega:.1f} MB)</a>'
display(HTML(link_mega))


# %% Cell 10: Cleanup

result_df.unpersist()
tactic_df.unpersist()
print("=== IMT Pipeline Complete ===")
