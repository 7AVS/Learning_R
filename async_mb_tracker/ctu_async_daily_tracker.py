# =============================================================================
# CTU Async Banner — Daily Performance Tracker (PySpark / HDFS)
# =============================================================================
# Jira: NBA-12268 | Live: April 10, 2026
# Promo: PB_CHEQ_ALL_26_03_RBC_CTU_PDA_Product_Page
# Mobile filter: SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MB%'
# Join: CAST(ep_srf_id2 AS LONG) = CAST(CLNT_NO AS LONG)
# =============================================================================

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

# --- Config ---
TACTIC_ID = "<<TACTIC_ID>>"
TACTIC_EVNT_HIST_BASE = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
TACTIC_YEARS = [2026]
GA4_ECOM_BASE = "/prod/sz/tsz/00198/data/ga4-ecommerce"
GA4_START_MONTH = "04"

CTU_PROMO_NAMES = [
    "PB_CHEQ_ALL_26_03_RBC_CTU_PDA_Product_Page",
]

spark.sparkContext.setLogLevel("WARN")


# %% Discovery — find TACTIC_ID, then update config above

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


# %% Tactic population — mobile-only CTU clients

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


# %% GA4 ecommerce — CTU banner events

ga4_paths = [f"{GA4_ECOM_BASE}/YEAR=2026/Month=*/*"]

ga4_filtered = spark.read \
    .option("basePath", GA4_ECOM_BASE) \
    .parquet(*ga4_paths) \
    .filter(
        (F.col("Month") >= GA4_START_MONTH) &
        F.col("it_item_name").isin(CTU_PROMO_NAMES) &
        F.col("event_name").isin("view_promotion", "select_promotion")
    ) \
    .select(
        "event_date", "event_name", "it_item_name", "platform",
        F.col("ep_srf_id2"),
    ) \
    .persist(StorageLevel.MEMORY_AND_DISK)

print(f"GA4 CTU events: {ga4_filtered.count():,}")
print(f"Distinct ep_srf_id2: {ga4_filtered.select('ep_srf_id2').distinct().count():,}")
ga4_filtered.groupBy("event_name", "platform") \
    .agg(F.count("*").alias("events"), F.countDistinct("ep_srf_id2").alias("users")) \
    .show(truncate=False)


# %% Join — ep_srf_id2 to CLNT_NO

JOIN_COL = "ep_srf_id2"

match_count = ga4_filtered.filter(F.col("ep_srf_id2").isNotNull()) \
    .select(F.col("ep_srf_id2").cast("long").alias("k")).distinct() \
    .join(
        tactic_pop.select(F.col("CLNT_NO").cast("long").alias("k")).distinct(),
        "k"
    ).count()

print(f"ep_srf_id2 matched to tactic: {match_count:,}")

if match_count == 0:
    print("Trying user_id...")
    ga4_filtered.unpersist()
    ga4_filtered = spark.read \
        .option("basePath", GA4_ECOM_BASE) \
        .parquet(*ga4_paths) \
        .filter(
            (F.col("Month") >= GA4_START_MONTH) &
            F.col("it_item_name").isin(CTU_PROMO_NAMES) &
            F.col("event_name").isin("view_promotion", "select_promotion")
        ) \
        .select("event_date", "event_name", "it_item_name", "platform",
                "ep_srf_id2", "user_id") \
        .persist(StorageLevel.MEMORY_AND_DISK)

    uid_match = ga4_filtered.filter(F.col("user_id").isNotNull()) \
        .select(F.col("user_id").cast("long").alias("k")).distinct() \
        .join(
            tactic_pop.select(F.col("CLNT_NO").cast("long").alias("k")).distinct(),
            "k"
        ).count()
    print(f"user_id matched to tactic: {uid_match:,}")

    if uid_match > 0:
        JOIN_COL = "user_id"
    else:
        JOIN_COL = None
        print("No working join key found.")


# %% Daily metrics

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

    # --- Output ---
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
{df.to_html(index=False, border=0, classes="t")}
</body></html>""")
    print(f"HTML saved: {html_path}")


# %% Cleanup

tactic_pop.unpersist()
ga4_filtered.unpersist()
