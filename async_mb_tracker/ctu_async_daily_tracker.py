# =============================================================================
# CTU Async Banner — Daily Performance Tracker (PySpark / HDFS)
# =============================================================================
#
# PySpark equivalent of ctu_async_daily_tracker.sql
# Connects to YARN Spark, reads from HDFS paths directly.
#
# Purpose:
#   Delivers all four metrics requested by Avanthi Jayaratna (starting
#   Monday April 13) in a single result set:
#     1. Available CTU Leads (test vs control)
#     2. Banner Views
#     3. Banner Clicks
#     4. Banner CTR
#
# Context:
#   - Jira: NBA-12268 (same async tracker initiative as PCD)
#   - NBA load final prod file: April 9, 2026
#   - Live to clients in mobile: April 10, 2026
#   - Async launch exposure: Week of April 6 (25% exposure — validate
#     engagement and CTR before full ramp)
#   - Lead on async side: Kabir Bajaj
#   - Daily stats requestor: Avanthi Jayaratna (asked Daniel Chin)
#   - Promo names confirmed by Rajani Singineedi
#
# NOTE: Only 1 promo tag confirmed — additional tags may be provided by
# Rajani post-launch. Update CTU_PROMO_NAMES in config when received.
#
# Mobile-only filter (confirmed by CIDM for CTU):
#   positions 121-150 of TACTIC_DECISN_VRB_INFO contain channel indicator
#   'MB' = mobile. CTU-specific — do NOT assume this applies to O2P or PCD.
#
# Confirmed CTU async promo names (it_item_name):
#   1. PB_CHEQ_ALL_26_03_RBC_CTU_PDA_Product_Page
#
# Join key: ep_srf_id2 (int, cast to string) = CLNT_NO (stripped of leading zeros)
#   *** NOT YET VALIDATED — run Sections 6-7 post-launch to confirm ***
#   If ep_srf_id2 doesn't match, try user_id as fallback.
#
# Run order:
#   1. Section 2 (Discovery) — find TACTIC_ID, update config
#   2. Sections 3-4 (Tactic population) — confirm mobile population counts
#   3. Sections 5-7 (GA4 + validation) — confirm promo fires and join works
#   4. Section 8 (Production query) — final metrics
#
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


# =============================================================================
# SECTION 0: Config & Spark Session
# =============================================================================

# ---------------------------------------------------------------------------
# CONFIG — update TACTIC_ID after running Section 2 (Discovery)
# ---------------------------------------------------------------------------

TACTIC_ID = "<<TACTIC_ID>>"  # Replace after running Section 2

TACTIC_EVNT_HIST_BASE = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
GA4_ECOM_BASE = "/prod/sz/tsz/00198/data/ga4-ecommerce"

# GA4 month partitions to read — extend as campaign runs
GA4_MONTHS = ["04", "05", "06"]

# Confirmed promo names from Rajani Singineedi — add more when received
CTU_PROMO_NAMES = [
    "PB_CHEQ_ALL_26_03_RBC_CTU_PDA_Product_Page",
    # NOTE: Additional promo tags may follow from Rajani post-launch.
    # Add them here when confirmed.
]

CTU_MNEMONIC = "CTU"

# ---------------------------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------------------------

spark = SparkSession.builder \
    .appName("CTU Async MB Tracker") \
    .master("yarn") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# =============================================================================
# SECTION 1: Read Tactic Events
# =============================================================================
# Reads the full tactic event history from HDFS.
# Applies column transformations that all downstream sections rely on.
# Run once — downstream sections filter from raw_tactic.

raw_tactic = spark.read \
    .option("basePath", TACTIC_EVNT_HIST_BASE) \
    .parquet(TACTIC_EVNT_HIST_BASE + "*")

tactic_events = raw_tactic.select(
    # Strip leading zeros from TACTIC_EVNT_ID to get CLNT_NO
    F.regexp_replace(F.trim(F.col("TACTIC_EVNT_ID")), "^0+", "").alias("CLNT_NO"),
    F.col("TACTIC_ID"),
    F.trim(F.col("TST_GRP_CD")).alias("TST_GRP_CD"),
    F.trim(F.col("RPT_GRP_CD")).alias("RPT_GRP_CD"),
    F.to_date(F.col("TREATMT_STRT_DT")).alias("TREATMT_STRT_DT"),
    F.to_date(F.col("TREATMT_END_DT")).alias("TREATMT_END_DT"),
    # TACTIC_DECISN_VRB_INFO is a 150-byte packed field — do NOT trim,
    # byte positions are meaningful (e.g. 121-150 = channel indicator)
    F.col("TACTIC_DECISN_VRB_INFO"),
    # Mnemonic: positions 8-10 of TACTIC_ID (1-indexed → Spark substr: pos 8, len 3)
    F.substring(F.col("TACTIC_ID"), 8, 3).alias("mnemonic"),
    # Channel indicator: positions 121-150 of TACTIC_DECISN_VRB_INFO
    F.substring(F.col("TACTIC_DECISN_VRB_INFO"), 121, 30).alias("channel_indicator"),
)


# =============================================================================
# SECTION 2: Discovery Query
# =============================================================================
# Run ONCE post-deployment to identify the CTU tactic ID.
# Replace TACTIC_ID in the config block above, then run remaining sections.

discovery = tactic_events \
    .filter(
        (F.col("mnemonic") == CTU_MNEMONIC) &
        (F.col("TREATMT_STRT_DT") >= F.lit("2026-04-01"))
    ) \
    .groupBy(
        "TACTIC_ID",
        "mnemonic",
        "TST_GRP_CD",
        "RPT_GRP_CD",
        "TREATMT_STRT_DT",
        "TREATMT_END_DT",
        "channel_indicator",
    ) \
    .agg(F.countDistinct("CLNT_NO").alias("unique_clients")) \
    .orderBy(F.col("TREATMT_STRT_DT").desc())

print("=== SECTION 2: Discovery — Find TACTIC_ID ===")
print("Find the TACTIC_ID for CTU April deployment.")
print("Replace TACTIC_ID in the config above, then run remaining sections.\n")
discovery.show(truncate=False)


# =============================================================================
# SECTION 3: Tactic Population (mobile only)
# =============================================================================
# Filters to the specific tactic ID from config and the mobile channel.
# Mobile filter: positions 121-150 of TACTIC_DECISN_VRB_INFO contain 'MB'
# This is CTU-specific — do NOT reuse for O2P or PCD.

tactic_pop = tactic_events \
    .filter(
        (F.col("TACTIC_ID") == TACTIC_ID) &
        F.substring(F.col("TACTIC_DECISN_VRB_INFO"), 121, 30).contains("MB")
    ) \
    .select(
        "CLNT_NO",
        "TST_GRP_CD",
        "RPT_GRP_CD",
        "TREATMT_STRT_DT",
        "TREATMT_END_DT",
    )

tactic_pop.persist(StorageLevel.MEMORY_AND_DISK)

print("=== SECTION 3: Tactic Population (mobile only) ===")
pop_count = tactic_pop.count()
print(f"Total mobile-deployed clients: {pop_count:,}")
tactic_pop.show(10, truncate=False)


# =============================================================================
# SECTION 4: Population Summary
# =============================================================================
# Count of available leads per test/control group — this is the denominator
# for CTR. Verify these counts match what the deployment team expects.

pop_summary = tactic_pop \
    .groupBy("TST_GRP_CD", "RPT_GRP_CD") \
    .agg(F.countDistinct("CLNT_NO").alias("available_leads"))

print("=== SECTION 4: Population Summary (available leads per group) ===")
pop_summary.show(truncate=False)


# =============================================================================
# SECTION 5: Read GA4 Ecommerce
# =============================================================================
# Reads GA4 ecommerce data with partition pruning via explicit path list.
#
# NOTE: Verify the GA4 partition folder structure post-launch.
# Expected: year=YYYY/month=MM/day=DD
# If this path pattern throws an error, fall back to:
#   raw_ga4 = spark.read.option("basePath", GA4_ECOM_BASE).parquet(GA4_ECOM_BASE)
#   and filter: .filter((F.col("year") == "2026") & (F.col("month").isin(GA4_MONTHS)))

ga4_paths = [f"{GA4_ECOM_BASE}/year=2026/month={m}/*" for m in GA4_MONTHS]
raw_ga4 = spark.read \
    .option("basePath", GA4_ECOM_BASE) \
    .parquet(*ga4_paths)

ga4_filtered = raw_ga4 \
    .filter(
        F.col("it_item_name").isin(CTU_PROMO_NAMES) &
        (F.col("ip_sf_campaign_mnemonic") == "CTU") &
        F.col("event_name").isin("view_promotion", "select_promotion")
    ) \
    .select(
        F.col("event_date"),
        F.col("event_name"),
        # ep_srf_id2 is integer in GA4 — cast to string for join to CLNT_NO
        F.col("ep_srf_id2").cast("string").alias("srf_id2_str"),
        F.col("ep_srf_id2"),
        F.col("it_item_name"),
        F.col("platform"),
    )

ga4_filtered.persist(StorageLevel.MEMORY_AND_DISK)

print("=== SECTION 5: GA4 Ecommerce — CTU Banner Events ===")
ga4_count = ga4_filtered.count()
print(f"Total CTU banner events (view + click): {ga4_count:,}")


# =============================================================================
# SECTION 6: Validation — Platform Check
# =============================================================================
# Verify which platforms the CTU promo tag fires on.
# If non-mobile platforms appear, add a platform filter to Section 8.
# Mobile-only is expected — flag anything else before trusting the numbers.

print("=== SECTION 6: Validation — Platform Check ===")
print("If non-mobile platforms appear, add platform filter to Section 8.\n")

ga4_filtered \
    .groupBy("platform", "event_name") \
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("ep_srf_id2").alias("unique_users"),
    ) \
    .orderBy(F.col("event_count").desc()) \
    .show(truncate=False)


# =============================================================================
# SECTION 7: Validation — Join Key Check
# =============================================================================
# Confirm that ep_srf_id2 (as string) matches CLNT_NO in tactic population.
# Run after populating TACTIC_ID in config.
#
# If Step A > 0 but Step B = 0 → join key is wrong.
# Try user_id instead of ep_srf_id2 as fallback.

print("=== SECTION 7: Validation — Join Key Check ===")

# Step A: Total CTU banner events in GA4 (no join to tactic)
step_a = ga4_filtered.agg(
    F.count("*").alias("total_events"),
    F.countDistinct("srf_id2_str").alias("unique_srf_ids"),
)
print("Step A — Total CTU banner events in GA4 (before join to tactic):")
step_a.show(truncate=False)

# Step B: How many of those match a tactic-deployed client
joined_check = ga4_filtered.join(
    tactic_pop,
    ga4_filtered["srf_id2_str"] == tactic_pop["CLNT_NO"],
    how="inner",
)
step_b = joined_check.agg(
    F.count("*").alias("matched_events"),
    F.countDistinct(ga4_filtered["ep_srf_id2"]).alias("matched_users"),
)
print("Step B — Events matched to a tactic-deployed client (inner join):")
step_b.show(truncate=False)

print("If Step A shows events but Step B shows 0 → join key mismatch.")
print("Try user_id instead of ep_srf_id2 as fallback join key.\n")


# =============================================================================
# SECTION 8: Production Query — Daily Metrics
# =============================================================================
# Full production result: all 4 metrics joined per day and test/control group.
# Only run after validating Sections 6 and 7.
#
# Output columns:
#   event_date | test_control | report_group | available_leads |
#   view_users | view_events | click_users | click_events | ctr_pct

banner_events = ga4_filtered.join(
    tactic_pop,
    ga4_filtered["srf_id2_str"] == tactic_pop["CLNT_NO"],
    how="inner",
).select(
    ga4_filtered["event_date"],
    ga4_filtered["event_name"],
    ga4_filtered["ep_srf_id2"],
    tactic_pop["TST_GRP_CD"].alias("test_control"),
    tactic_pop["RPT_GRP_CD"].alias("report_group"),
)

daily_metrics = banner_events.groupBy(
    "event_date", "test_control", "report_group"
).agg(
    F.countDistinct(
        F.when(F.col("event_name") == "view_promotion", F.col("ep_srf_id2"))
    ).alias("view_users"),
    F.count(
        F.when(F.col("event_name") == "view_promotion", F.lit(1))
    ).alias("view_events"),
    F.countDistinct(
        F.when(F.col("event_name") == "select_promotion", F.col("ep_srf_id2"))
    ).alias("click_users"),
    F.count(
        F.when(F.col("event_name") == "select_promotion", F.lit(1))
    ).alias("click_events"),
)

final = daily_metrics.join(
    pop_summary,
    (daily_metrics["test_control"] == pop_summary["TST_GRP_CD"]) &
    (daily_metrics["report_group"] == pop_summary["RPT_GRP_CD"]),
    how="inner",
).select(
    daily_metrics["event_date"],
    daily_metrics["test_control"],
    daily_metrics["report_group"],
    pop_summary["available_leads"],
    daily_metrics["view_users"],
    daily_metrics["view_events"],
    daily_metrics["click_users"],
    daily_metrics["click_events"],
    F.round(
        F.col("click_users")
        / F.when(F.col("view_users") == 0, None).otherwise(F.col("view_users"))
        * 100,
        2,
    ).alias("ctr_pct"),
).orderBy("event_date", "test_control")

print("=== SECTION 8: Production Query — Daily Metrics ===")
final.show(100, truncate=False)

# To export to pandas for Excel:
# df = final.toPandas()
# df.to_excel("ctu_async_daily_tracker.xlsx", index=False)


# =============================================================================
# SECTION 9: Cleanup
# =============================================================================

tactic_pop.unpersist()
ga4_filtered.unpersist()

spark.stop()
