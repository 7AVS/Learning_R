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
GA4_START_MONTH = "04"  # Campaign started April — reads all months from here forward

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

# Show all available columns — check if CLNT_NO exists directly
print(f"Tactic columns ({len(raw_tactic.columns)}):")
print(sorted(raw_tactic.columns))

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

tactic_pop_raw = raw_tactic \
    .filter(
        (F.col("TACTIC_ID") == TACTIC_ID) &
        F.substring(F.col("TACTIC_DECISN_VRB_INFO"), 121, 30).contains("MB")
    )

# Show ALL columns available in tactic parquet for this CTU record
print("=== ALL TACTIC COLUMNS — sample CTU mobile row ===")
sample_row = tactic_pop_raw.limit(1).collect()[0]
for c in sorted(tactic_pop_raw.columns):
    v = sample_row[c]
    if v is not None and str(v).strip() != "":
        print(f"  {c} = [{v}]")

# Keep raw TACTIC_EVNT_ID alongside the transformed version for debugging
tactic_pop = tactic_pop_raw \
    .select(
        F.col("TACTIC_EVNT_ID").alias("RAW_EVNT_ID"),
        F.trim(F.col("TACTIC_EVNT_ID")).alias("TRIMMED_EVNT_ID"),
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
        F.col("ep_srf_id2").cast("string").alias("srf_id2_str"),
    ) \
    .persist(StorageLevel.MEMORY_AND_DISK)


# =============================================================================
# STEP 1: Validate tactic side — what do CLNT_NO values look like?
# =============================================================================

print("=== TACTIC: RAW vs TRIMMED vs CLNT_NO (after zero strip) ===")
tactic_pop.select("RAW_EVNT_ID", "TRIMMED_EVNT_ID", "CLNT_NO").distinct().show(10, truncate=False)
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
# STEP 3: Join tests — try every combination to find what matches
# =============================================================================

ga4_keys = ga4_filtered.filter(F.col("ep_srf_id2").isNotNull()) \
    .select(F.col("ep_srf_id2").cast("string").alias("ga4_key")).distinct()
ga4_n = ga4_keys.count()

# Test A: ep_srf_id2 vs CLNT_NO (zero-stripped)
tac_a = tactic_pop.select(F.col("CLNT_NO").alias("tac_key")).distinct()
match_a = ga4_keys.join(tac_a, ga4_keys["ga4_key"] == tac_a["tac_key"]).count()

# Test B: ep_srf_id2 vs TRIMMED_EVNT_ID (trimmed, zeros kept)
tac_b = tactic_pop.select(F.col("TRIMMED_EVNT_ID").alias("tac_key")).distinct()
match_b = ga4_keys.join(tac_b, ga4_keys["ga4_key"] == tac_b["tac_key"]).count()

# Test C: ep_srf_id2 vs RAW_EVNT_ID (completely raw)
tac_c = tactic_pop.select(F.col("RAW_EVNT_ID").cast("string").alias("tac_key")).distinct()
match_c = ga4_keys.join(tac_c, ga4_keys["ga4_key"] == tac_c["tac_key"]).count()

# Test D: both cast to long (handles type mismatches)
ga4_longs = ga4_keys.select(F.col("ga4_key").cast("long").alias("lk")).filter(F.col("lk").isNotNull()).distinct()
tac_longs = tactic_pop.select(F.col("CLNT_NO").cast("long").alias("lk")).filter(F.col("lk").isNotNull()).distinct()
match_d = ga4_longs.join(tac_longs, "lk").count()

# Test E: ep_srf_id2 vs RAW_EVNT_ID both cast to long
tac_raw_longs = tactic_pop.select(F.col("RAW_EVNT_ID").cast("long").alias("lk")).filter(F.col("lk").isNotNull()).distinct()
match_e = ga4_longs.join(tac_raw_longs, "lk").count()

print(f"=== JOIN TESTS: GA4 distinct ep_srf_id2 = {ga4_n:,} ===")
print(f"A) ep_srf_id2 str  vs CLNT_NO (zero-stripped str):  {match_a:,}")
print(f"B) ep_srf_id2 str  vs TRIMMED_EVNT_ID (str):        {match_b:,}")
print(f"C) ep_srf_id2 str  vs RAW_EVNT_ID (str):            {match_c:,}")
print(f"D) ep_srf_id2 long vs CLNT_NO long:                 {match_d:,}")
print(f"E) ep_srf_id2 long vs RAW_EVNT_ID long:             {match_e:,}")
print()
if max(match_a, match_b, match_c, match_d, match_e) > 0:
    best = max([(match_a,"A"),(match_b,"B"),(match_c,"C"),(match_d,"D"),(match_e,"E")])[1]
    print(f"Best match: Test {best}")
else:
    print("ALL ZERO — ep_srf_id2 values do not exist in TACTIC_EVNT_ID in any form.")
    print("Printing data type info:")
    print(f"  ep_srf_id2 spark type: {ga4_filtered.schema['ep_srf_id2'].dataType}")
    print(f"  TACTIC_EVNT_ID spark type: {tactic_pop_raw.schema['TACTIC_EVNT_ID'].dataType}")

ep_match = match_d  # for Step 4 fallback logic


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
            (F.col("Month") >= GA4_START_MONTH) &
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
# STEP 4B: If both keys failed — search ALL GA4 columns for CLNT_NO
# =============================================================================

if JOIN_COL is None:
    from pyspark.sql.types import StringType, LongType, IntegerType, DoubleType

    sample_clnt = [r.CLNT_NO for r in tactic_pop.select("CLNT_NO").limit(5).collect()]
    print(f"Sample CLNT_NO from tactic: {sample_clnt}")

    ga4_full = spark.read \
        .option("basePath", GA4_ECOM_BASE) \
        .parquet(*ga4_paths) \
        .filter(
            (F.col("Month") >= GA4_START_MONTH) &
            F.col("it_item_name").isin(CTU_PROMO_NAMES) &
            F.col("event_name").isin("view_promotion", "select_promotion")
        )

    print(f"\nGA4 columns ({len(ga4_full.columns)} total):")
    for c in sorted(ga4_full.columns):
        print(f"  {c}")

    print("\nSample full GA4 row (populated fields only):")
    row = ga4_full.limit(1).collect()[0]
    for c in sorted(ga4_full.columns):
        v = row[c]
        if v is not None and str(v).strip() != "":
            print(f"  {c} = {v}")

    print("\n=== Searching GA4 columns for CLNT_NO matches ===")
    str_cols = [f.name for f in ga4_full.schema.fields
                if isinstance(f.dataType, (StringType, LongType, IntegerType, DoubleType))]

    for col_name in str_cols:
        hits = ga4_full.filter(
            F.col(col_name).cast("string").isin(sample_clnt)
        ).count()
        if hits > 0:
            print(f"  MATCH: {col_name} has {hits} rows matching sample CLNT_NO")

    print("\nIf no MATCH found, the GA4 table may use a different ID system.")
    print("A crosswalk table (SRF_ID → CLNT_NO) would be needed.")


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
