# =============================================================================
# CTU Async Banner — Daily Performance Tracker (PySpark / HDFS)
# =============================================================================
#
# PySpark equivalent of ctu_async_daily_tracker.sql
# Connects to YARN Spark, reads from HDFS paths directly.
#
# Context:
#   - Jira: NBA-12268
#   - Live to clients in mobile: April 10, 2026
#   - Lead on async side: Kabir Bajaj
#   - Daily stats requestor: Avanthi Jayaratna
#   - Promo names confirmed by Rajani Singineedi
#
# Mobile-only filter (confirmed by CIDM for CTU):
#   positions 121-150 of TACTIC_DECISN_VRB_INFO contain 'MB'
#   CTU-specific — do NOT assume this applies to O2P or PCD.
#
# Join key: ep_srf_id2 (int → string) = CLNT_NO (TACTIC_EVNT_ID stripped of leading zeros)
#   If join returns 0 matches, try user_id as fallback.
#
# HDFS paths:
#   Tactic events: /prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/
#   GA4 ecommerce: /prod/sz/tsz/00198/data/ga4-ecommerce
#
# GA4 partition structure: year=YYYY/month=MM/day=DD
#   If path pattern throws error, fall back to reading full path + filter:
#   spark.read.option("basePath", GA4_ECOM_BASE).parquet(GA4_ECOM_BASE)
#   then .filter((F.col("year") == "2026") & F.col("month").isin(GA4_MONTHS))
#
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

# --- Config ---
TACTIC_ID = "<<TACTIC_ID>>"
TACTIC_EVNT_HIST_BASE = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
GA4_ECOM_BASE = "/prod/sz/tsz/00198/data/ga4-ecommerce"
GA4_MONTHS = ["04", "05", "06"]

CTU_PROMO_NAMES = [
    "PB_CHEQ_ALL_26_03_RBC_CTU_PDA_Product_Page",
    # Add promo tags here when received from Rajani
]

spark = SparkSession.builder \
    .appName("CTU Async MB Tracker") \
    .master("yarn") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# =============================================================================
# Discovery — run first to find TACTIC_ID, then update config above
# =============================================================================

raw_tactic = spark.read \
    .option("basePath", TACTIC_EVNT_HIST_BASE) \
    .parquet(TACTIC_EVNT_HIST_BASE + "*")

raw_tactic \
    .filter(F.substring(F.col("TACTIC_ID"), 8, 3) == "CTU") \
    .filter(F.to_date(F.col("TREATMT_STRT_DT")) >= "2026-04-01") \
    .withColumn("CLNT_NO", F.regexp_replace(F.trim(F.col("TACTIC_EVNT_ID")), "^0+", "")) \
    .groupBy(
        "TACTIC_ID",
        F.trim(F.col("TST_GRP_CD")).alias("TST_GRP_CD"),
        F.trim(F.col("RPT_GRP_CD")).alias("RPT_GRP_CD"),
        F.to_date(F.col("TREATMT_STRT_DT")).alias("TREATMT_STRT_DT"),
        F.to_date(F.col("TREATMT_END_DT")).alias("TREATMT_END_DT"),
        F.substring(F.col("TACTIC_DECISN_VRB_INFO"), 121, 30).alias("channel_indicator"),
    ) \
    .agg(F.countDistinct("CLNT_NO").alias("unique_clients")) \
    .orderBy(F.col("TREATMT_STRT_DT").desc()) \
    .show(truncate=False)


# =============================================================================
# Tactic population — mobile-only CTU clients
# =============================================================================

tactic_pop = raw_tactic \
    .filter(
        (F.col("TACTIC_ID") == TACTIC_ID) &
        F.substring(F.col("TACTIC_DECISN_VRB_INFO"), 121, 30).contains("MB")
    ) \
    .select(
        F.regexp_replace(F.trim(F.col("TACTIC_EVNT_ID")), "^0+", "").alias("CLNT_NO"),
        F.trim(F.col("TST_GRP_CD")).alias("TST_GRP_CD"),
        F.trim(F.col("RPT_GRP_CD")).alias("RPT_GRP_CD"),
    ) \
    .persist(StorageLevel.MEMORY_AND_DISK)

pop_summary = tactic_pop \
    .groupBy("TST_GRP_CD", "RPT_GRP_CD") \
    .agg(F.countDistinct("CLNT_NO").alias("available_leads"))

print(f"Mobile-deployed clients: {tactic_pop.count():,}")
pop_summary.show(truncate=False)


# =============================================================================
# GA4 ecommerce — CTU banner events
# =============================================================================

ga4_paths = [f"{GA4_ECOM_BASE}/year=2026/month={m}/*" for m in GA4_MONTHS]

ga4_filtered = spark.read \
    .option("basePath", GA4_ECOM_BASE) \
    .parquet(*ga4_paths) \
    .filter(
        F.col("it_item_name").isin(CTU_PROMO_NAMES) &
        (F.col("ip_sf_campaign_mnemonic") == "CTU") &
        F.col("event_name").isin("view_promotion", "select_promotion")
    ) \
    .select(
        "event_date", "event_name", "it_item_name", "platform",
        F.col("ep_srf_id2"),
        F.col("ep_srf_id2").cast("string").alias("srf_id2_str"),
    ) \
    .persist(StorageLevel.MEMORY_AND_DISK)


# =============================================================================
# Validation — platform check + join key check
# =============================================================================

# Platform check: confirm promo fires on mobile only
ga4_filtered \
    .groupBy("platform", "event_name") \
    .agg(F.count("*").alias("events"), F.countDistinct("ep_srf_id2").alias("users")) \
    .orderBy(F.col("events").desc()) \
    .show(truncate=False)

# Join key check: compare total GA4 events vs matched to tactic population
total = ga4_filtered.agg(F.count("*").alias("events"), F.countDistinct("srf_id2_str").alias("users"))
matched = ga4_filtered.join(tactic_pop, ga4_filtered["srf_id2_str"] == tactic_pop["CLNT_NO"]) \
    .agg(F.count("*").alias("events"), F.countDistinct(ga4_filtered["ep_srf_id2"]).alias("users"))

print("GA4 total (before join):")
total.show(truncate=False)
print("Matched to tactic population:")
matched.show(truncate=False)


# =============================================================================
# Daily metrics — production query
# =============================================================================
# Output: event_date | test_control | report_group | available_leads |
#         view_users | view_events | click_users | click_events | ctr_pct

banner_events = ga4_filtered.join(
    tactic_pop,
    ga4_filtered["srf_id2_str"] == tactic_pop["CLNT_NO"],
).select(
    ga4_filtered["event_date"],
    ga4_filtered["event_name"],
    ga4_filtered["ep_srf_id2"],
    tactic_pop["TST_GRP_CD"].alias("test_control"),
    tactic_pop["RPT_GRP_CD"].alias("report_group"),
)

daily_metrics = banner_events.groupBy("event_date", "test_control", "report_group").agg(
    F.countDistinct(F.when(F.col("event_name") == "view_promotion", F.col("ep_srf_id2"))).alias("view_users"),
    F.count(F.when(F.col("event_name") == "view_promotion", F.lit(1))).alias("view_events"),
    F.countDistinct(F.when(F.col("event_name") == "select_promotion", F.col("ep_srf_id2"))).alias("click_users"),
    F.count(F.when(F.col("event_name") == "select_promotion", F.lit(1))).alias("click_events"),
)

final = daily_metrics.join(
    pop_summary,
    (daily_metrics["test_control"] == pop_summary["TST_GRP_CD"]) &
    (daily_metrics["report_group"] == pop_summary["RPT_GRP_CD"]),
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
        * 100, 2
    ).alias("ctr_pct"),
).orderBy("event_date", "test_control")

final.show(100, truncate=False)

# To export: df = final.toPandas(); df.to_excel("ctu_daily_tracker.xlsx", index=False)


# =============================================================================
# Cleanup
# =============================================================================

tactic_pop.unpersist()
ga4_filtered.unpersist()
spark.stop()
