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

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

# --- Config ---
TACTIC_ID = "<<TACTIC_ID>>"
TACTIC_EVNT_HIST_BASE = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
TACTIC_YEARS = [2026]  # Partition filter — extend if campaign spans years
GA4_ECOM_BASE = "/prod/sz/tsz/00198/data/ga4-ecommerce"
GA4_MONTHS = ["04", "05", "06"]

CTU_PROMO_NAMES = [
    "PB_CHEQ_ALL_26_03_RBC_CTU_PDA_Product_Page",
    # Add promo tags here when received from Rajani
]

# SparkSession is pre-initialized by Lumina as 'spark' — no builder needed.
# If running outside Lumina (e.g. spark-submit), uncomment the block below:
#
# spark = SparkSession.builder \
#     .appName("CTU Async MB Tracker") \
#     .master("yarn") \
#     .enableHiveSupport() \
#     .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# =============================================================================
# Discovery — run first to find TACTIC_ID, then update config above
# =============================================================================

tactic_paths = [f"{TACTIC_EVNT_HIST_BASE}EVNT_STRT_DT={y}*" for y in TACTIC_YEARS]
raw_tactic = spark.read \
    .option("basePath", TACTIC_EVNT_HIST_BASE) \
    .parquet(*tactic_paths)

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

ga4_paths = [f"{GA4_ECOM_BASE}/YEAR=2026/Month={m}/*" for m in GA4_MONTHS]

ga4_filtered = spark.read \
    .option("basePath", GA4_ECOM_BASE) \
    .parquet(*ga4_paths) \
    .filter(
        F.col("it_item_name").isin(CTU_PROMO_NAMES) &
        F.col("event_name").isin("view_promotion", "select_promotion")
    ) \
    .select(
        "event_date", "event_name", "it_item_name", "platform",
        F.col("ep_srf_id2"),
        F.col("ep_srf_id2").cast("string").alias("srf_id2_str"),
    ) \
    .persist(StorageLevel.MEMORY_AND_DISK)


# =============================================================================
# STEP 1: Validate tactic side — what do CLNT_NO values look like?
# =============================================================================

print("=== TACTIC: sample CLNT_NO values ===")
tactic_pop.select("CLNT_NO").distinct().show(10, truncate=False)
print("CLNT_NO length distribution:")
tactic_pop.select(F.length("CLNT_NO").alias("len")) \
    .groupBy("len").count().orderBy("len").show()


# =============================================================================
# STEP 2: Validate GA4 side — what do ep_srf_id2 values look like?
# =============================================================================

ga4_total = ga4_filtered.count()
ga4_null = ga4_filtered.filter(F.col("ep_srf_id2").isNull()).count()
print(f"=== GA4: ep_srf_id2 diagnostics ===")
print(f"Total events:     {ga4_total:,}")
print(f"ep_srf_id2 NULL:  {ga4_null:,}")
print(f"ep_srf_id2 filled:{ga4_total - ga4_null:,}")
print()
print("Sample ep_srf_id2 (raw) vs srf_id2_str (cast to string):")
ga4_filtered.select("ep_srf_id2", "srf_id2_str") \
    .filter(F.col("ep_srf_id2").isNotNull()) \
    .distinct().show(10, truncate=False)

print("Platform x event_name:")
ga4_filtered.groupBy("platform", "event_name") \
    .agg(F.count("*").alias("events"), F.countDistinct("ep_srf_id2").alias("users")) \
    .orderBy(F.col("events").desc()).show(truncate=False)


# =============================================================================
# STEP 3: Join test — ep_srf_id2 (cast to long, avoids string mismatch)
# =============================================================================

tactic_longs = tactic_pop.select(
    F.col("CLNT_NO").cast("long").alias("join_key")
).filter(F.col("join_key").isNotNull()).distinct()

ga4_longs = ga4_filtered.filter(F.col("ep_srf_id2").isNotNull()).select(
    F.col("ep_srf_id2").cast("long").alias("join_key")
).filter(F.col("join_key").isNotNull()).distinct()

tac_n = tactic_longs.count()
ga4_n = ga4_longs.count()
ep_match = ga4_longs.join(tactic_longs, "join_key").count()

print(f"=== JOIN TEST: ep_srf_id2 (as long) ===")
print(f"Tactic distinct clients:    {tac_n:,}")
print(f"GA4 distinct ep_srf_id2:    {ga4_n:,}")
print(f"Matched:                    {ep_match:,}")


# =============================================================================
# STEP 4: If ep_srf_id2 failed, try user_id
# =============================================================================

JOIN_COL = "ep_srf_id2"

if ep_match == 0:
    print("\nep_srf_id2 returned 0 matches. Trying user_id...")

    ga4_with_uid = spark.read \
        .option("basePath", GA4_ECOM_BASE) \
        .parquet(*ga4_paths) \
        .filter(
            F.col("it_item_name").isin(CTU_PROMO_NAMES) &
            F.col("event_name").isin("view_promotion", "select_promotion")
        ) \
        .select("event_date", "event_name", "it_item_name", "platform",
                "ep_srf_id2", "user_id")

    uid_total = ga4_with_uid.count()
    uid_null = ga4_with_uid.filter(F.col("user_id").isNull()).count()
    print(f"user_id NULL:    {uid_null:,} out of {uid_total:,}")
    print("Sample user_id values:")
    ga4_with_uid.select("user_id").filter(F.col("user_id").isNotNull()) \
        .distinct().show(10, truncate=False)

    ga4_uid_longs = ga4_with_uid.filter(F.col("user_id").isNotNull()).select(
        F.col("user_id").cast("long").alias("join_key")
    ).filter(F.col("join_key").isNotNull()).distinct()

    uid_match = ga4_uid_longs.join(tactic_longs, "join_key").count()
    print(f"GA4 distinct user_id:       {ga4_uid_longs.count():,}")
    print(f"Matched (user_id):          {uid_match:,}")

    if uid_match > 0:
        JOIN_COL = "user_id"
        ga4_filtered.unpersist()
        ga4_filtered = ga4_with_uid.persist(StorageLevel.MEMORY_AND_DISK)
        print(f"\nUsing user_id as join key.")
    else:
        JOIN_COL = None
        print("\nBOTH join keys returned 0. Check sample values above.")

print(f"\nJoin key: {JOIN_COL}")


# =============================================================================
# STEP 5: Daily metrics (only runs if join key resolved)
# =============================================================================

if JOIN_COL:
    banner_events = ga4_filtered.join(
        tactic_pop,
        F.col(JOIN_COL).cast("long") == F.col("CLNT_NO").cast("long"),
    ).select(
        ga4_filtered["event_date"],
        ga4_filtered["event_name"],
        F.col(JOIN_COL).alias("client_id"),
        tactic_pop["TST_GRP_CD"].alias("test_control"),
        tactic_pop["RPT_GRP_CD"].alias("report_group"),
    )

    matched_events = banner_events.count()
    matched_users = banner_events.select("client_id").distinct().count()
    print(f"Joined banner events: {matched_events:,}")
    print(f"Joined unique users:  {matched_users:,}")

    daily_metrics = banner_events.groupBy("event_date", "test_control", "report_group").agg(
        F.countDistinct(F.when(F.col("event_name") == "view_promotion", F.col("client_id"))).alias("view_users"),
        F.count(F.when(F.col("event_name") == "view_promotion", F.lit(1))).alias("view_events"),
        F.countDistinct(F.when(F.col("event_name") == "select_promotion", F.col("client_id"))).alias("click_users"),
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

    # --- Excel + HTML output ---
    df = final.toPandas()
    total_mobile = tactic_pop.select("CLNT_NO").distinct().count()

    xlsx_path = "/tmp/ctu_daily_tracker.xlsx"
    try:
        df.to_excel(xlsx_path, index=False, engine="xlsxwriter")
    except ModuleNotFoundError:
        df.to_csv(xlsx_path.replace(".xlsx", ".csv"), index=False)
        xlsx_path = xlsx_path.replace(".xlsx", ".csv")
    print(f"Saved: {xlsx_path}")

    html_path = "/tmp/ctu_daily_tracker.html"
    table_html = df.to_html(index=False, border=0, classes="t")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>CTU Daily Tracker</title>
<style>
  body {{ font-family: Arial; margin: 30px; }}
  .t {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
  .t th {{ background: #003366; color: #fff; padding: 8px 12px; border: 1px solid #ccc; text-align: left; }}
  .t td {{ padding: 7px 12px; border: 1px solid #ddd; }}
  .t tr:nth-child(even) td {{ background: #f4f7fb; }}
</style></head><body>
<h2>CTU Async Banner — Daily Tracker</h2>
<p>Mobile-deployed clients: <strong>{total_mobile:,}</strong></p>
{table_html}
</body></html>""")
    print(f"HTML saved: {html_path}")

else:
    print("Cannot build daily tracker — no working join key found.")


# =============================================================================
# Cleanup
# =============================================================================

tactic_pop.unpersist()
ga4_filtered.unpersist()
# Don't call spark.stop() — Lumina manages the session lifecycle
