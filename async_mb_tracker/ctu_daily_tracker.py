# =============================================================================
# CTU Async Banner — Daily Tracker (Lumina PySpark Notebook)
# =============================================================================
#
# Context:
#   - Campaign: Credit to Upgrade (CTU), deployed April 10, 2026
#   - Promo tag: PB_CHEQ_ALL_26_03_RBC_CTU_PDA_Product_Page
#   - Mobile-only filter: positions 121-150 of TACTIC_DECISN_VRB_INFO contain 'MB'
#   - ip_sf_campaign_mnemonic is BLANK for CTU — never filter on it
#   - Join key: CAST(ep_srf_id2 AS BIGINT) = CLNT_NO (try ep_srf_id2 first, fallback to user_id)
#   - spark pre-initialized by Lumina — no builder or spark.stop()
#
# GA4 source:  prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
# Tactic source: dg6v01.tactic_evnt_ip_ar_hist
# =============================================================================

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

CTU_PROMO_TAG = "PB_CHEQ_ALL_26_03_RBC_CTU_PDA_Product_Page"
GA4_TABLE = "prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce"
TACTIC_TABLE = "dg6v01.tactic_evnt_ip_ar_hist"

spark.sparkContext.setLogLevel("WARN")


# %% -------------------------------------------------------------------------
# CTU GA4 Banner Events — confirm events exist
# ---------------------------------------------------------------------------

ctu_events = spark.sql(f"""
    SELECT
        event_date,
        event_name,
        ep_srf_id2,
        user_id,
        it_item_name,
        platform,
        ip_sf_campaign_mnemonic
    FROM {GA4_TABLE}
    WHERE year = '2026'
      AND month = '04'
      AND it_item_name = '{CTU_PROMO_TAG}'
      AND event_name IN ('view_promotion', 'select_promotion')
""").persist(StorageLevel.MEMORY_AND_DISK)

total_events = ctu_events.count()
print(f"Total CTU GA4 events: {total_events:,}")

print("\n-- event_name x platform --")
ctu_events.groupBy("event_name", "platform") \
    .agg(F.count("*").alias("events"), F.countDistinct("ep_srf_id2").alias("users")) \
    .orderBy("event_name", "platform") \
    .show(truncate=False)

print("-- ip_sf_campaign_mnemonic distribution (should be blank/null) --")
ctu_events.groupBy("ip_sf_campaign_mnemonic") \
    .agg(F.count("*").alias("events")) \
    .orderBy(F.col("events").desc()) \
    .show(20, truncate=False)


# %% -------------------------------------------------------------------------
# Join key diagnostics — ep_srf_id2 and user_id
# ---------------------------------------------------------------------------

# ep_srf_id2 diagnostics
total_rows = ctu_events.count()

ep_null_count = ctu_events.filter(F.col("ep_srf_id2").isNull()).count()
ep_blank_count = ctu_events.filter(F.trim(F.col("ep_srf_id2")) == "").count()
ep_distinct = ctu_events.filter(F.col("ep_srf_id2").isNotNull()).select("ep_srf_id2").distinct().count()
ep_numeric_count = ctu_events.filter(F.col("ep_srf_id2").rlike("^[0-9]+$")).count()
ep_nonnumeric_count = ctu_events.filter(
    F.col("ep_srf_id2").isNotNull() & ~F.col("ep_srf_id2").rlike("^[0-9]+$")
).count()

print("=== ep_srf_id2 ===")
print(f"  Nulls:            {ep_null_count:,}")
print(f"  Blanks (trimmed): {ep_blank_count:,}")
print(f"  Distinct non-null:{ep_distinct:,}")
print(f"  Numeric values:   {ep_numeric_count:,}")
print(f"  Non-numeric:      {ep_nonnumeric_count:,}")

print("\n  Sample distinct ep_srf_id2 values:")
ctu_events.filter(F.col("ep_srf_id2").isNotNull()) \
    .select("ep_srf_id2") \
    .distinct() \
    .limit(20) \
    .show(truncate=False)

print("  Length distribution:")
ctu_events.filter(F.col("ep_srf_id2").isNotNull()) \
    .groupBy(F.length("ep_srf_id2").alias("len")) \
    .agg(F.count("*").alias("n")) \
    .orderBy("len") \
    .show(truncate=False)

# user_id diagnostics
uid_null_count = ctu_events.filter(F.col("user_id").isNull()).count()
uid_blank_count = ctu_events.filter(F.trim(F.col("user_id")) == "").count()
uid_distinct = ctu_events.filter(F.col("user_id").isNotNull()).select("user_id").distinct().count()
uid_numeric_count = ctu_events.filter(F.col("user_id").rlike("^[0-9]+$")).count()
uid_nonnumeric_count = ctu_events.filter(
    F.col("user_id").isNotNull() & ~F.col("user_id").rlike("^[0-9]+$")
).count()

print("\n=== user_id ===")
print(f"  Nulls:            {uid_null_count:,}")
print(f"  Blanks (trimmed): {uid_blank_count:,}")
print(f"  Distinct non-null:{uid_distinct:,}")
print(f"  Numeric values:   {uid_numeric_count:,}")
print(f"  Non-numeric:      {uid_nonnumeric_count:,}")

print("\n  Sample distinct user_id values:")
ctu_events.filter(F.col("user_id").isNotNull()) \
    .select("user_id") \
    .distinct() \
    .limit(20) \
    .show(truncate=False)

print("  Length distribution:")
ctu_events.filter(F.col("user_id").isNotNull()) \
    .groupBy(F.length("user_id").alias("len")) \
    .agg(F.count("*").alias("n")) \
    .orderBy("len") \
    .show(truncate=False)

ep_usable = ep_numeric_count
uid_usable = uid_numeric_count
print(f"\nSummary:")
print(f"  ep_srf_id2 usable (numeric): {ep_usable:,} out of {total_rows:,}")
print(f"  user_id    usable (numeric): {uid_usable:,} out of {total_rows:,}")


# %% -------------------------------------------------------------------------
# Tactic population — CTU mobile deployed
# ---------------------------------------------------------------------------

TACTIC_AVAILABLE = False
tactic_pop = None

try:
    tactic_pop = spark.sql(f"""
        SELECT
            CLNT_NO,
            TST_GRP_CD,
            RPT_GRP_CD,
            TACTIC_ID,
            TREATMT_STRT_DT
        FROM {TACTIC_TABLE}
        WHERE SUBSTR(TACTIC_ID, 8, 3) = 'CTU'
          AND TREATMT_STRT_DT >= '2026-04-01'
          AND SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MB%'
    """).persist(StorageLevel.MEMORY_AND_DISK)

    tactic_count = tactic_pop.count()
    print(f"CTU mobile tactic population: {tactic_count:,}")

    print("\n-- TST_GRP_CD x RPT_GRP_CD --")
    tactic_pop.groupBy("TST_GRP_CD", "RPT_GRP_CD") \
        .agg(F.countDistinct("CLNT_NO").alias("clients")) \
        .orderBy("TST_GRP_CD", "RPT_GRP_CD") \
        .show(truncate=False)

    TACTIC_AVAILABLE = True

except Exception as e:
    print(f"ERROR loading tactic table: {e}")
    print()
    print("Tactic table not accessible from this environment.")
    print("Load the population manually using one of:")
    print('  tactic_pop = spark.read.option("header", True).csv("/path/to/tactic_pop.csv")')
    print('  tactic_pop = spark.read.parquet("/path/to/tactic_pop.parquet")')
    print("Then re-run this cell and set TACTIC_AVAILABLE = True at the bottom.")

# If you exported tactic_pop earlier as parquet, uncomment and update:
# tactic_pop = spark.read.parquet("/tmp/ctu_tactic_pop.parquet").persist(StorageLevel.MEMORY_AND_DISK)
# TACTIC_AVAILABLE = True


# %% -------------------------------------------------------------------------
# Join test — ep_srf_id2 vs user_id
# ---------------------------------------------------------------------------

if not TACTIC_AVAILABLE:
    print("ERROR: tactic_pop not loaded. Run Cell 3 first.")
else:
    # --- ep_srf_id2 ---
    ga4_ep = ctu_events \
        .filter(F.col("ep_srf_id2").rlike("^[0-9]+$")) \
        .select(F.col("ep_srf_id2").cast("bigint").alias("join_key")) \
        .distinct()

    tac_keys = tactic_pop \
        .select(F.col("CLNT_NO").cast("bigint").alias("join_key")) \
        .distinct()

    ga4_ep_count = ga4_ep.count()
    tac_count = tac_keys.count()
    ep_matches = ga4_ep.join(tac_keys, "join_key").count()

    print(f"GA4 distinct numeric ep_srf_id2: {ga4_ep_count:,}")
    print(f"Tactic distinct CLNT_NO:         {tac_count:,}")
    print(f"Matched (ep_srf_id2):            {ep_matches:,}")

    JOIN_KEY = None

    if ep_matches > 0:
        print("\nVerdict: ep_srf_id2 works. Using ep_srf_id2 as join key.")
        JOIN_KEY = "ep_srf_id2"
    else:
        print("\nep_srf_id2 returned 0 matches. Trying user_id...")

        ga4_uid = ctu_events \
            .filter(F.col("user_id").rlike("^[0-9]+$")) \
            .select(F.col("user_id").cast("bigint").alias("join_key")) \
            .distinct()

        ga4_uid_count = ga4_uid.count()
        uid_matches = ga4_uid.join(tac_keys, "join_key").count()

        print(f"GA4 distinct numeric user_id:    {ga4_uid_count:,}")
        print(f"Tactic distinct CLNT_NO:         {tac_count:,}")
        print(f"Matched (user_id):               {uid_matches:,}")

        if uid_matches > 0:
            print("\nVerdict: user_id works. Using user_id as join key.")
            JOIN_KEY = "user_id"
        else:
            print("\nVerdict: BOTH keys return 0 matches.")
            print("Check: (1) CLNT_NO format in tactic table, (2) GA4 ID population, (3) date overlap.")
            JOIN_KEY = None


# %% -------------------------------------------------------------------------
# CTU Daily Tracker — Excel + HTML output
# ---------------------------------------------------------------------------

if not TACTIC_AVAILABLE:
    print("ERROR: tactic_pop not loaded.")
elif JOIN_KEY is None:
    print("ERROR: no working join key. Resolve join diagnostics in Cell 4 first.")
else:
    # --- available leads by group ---
    pop_summary = tactic_pop.groupBy("TST_GRP_CD", "RPT_GRP_CD") \
        .agg(F.countDistinct("CLNT_NO").alias("available_leads"))

    total_mobile = tactic_pop.select("CLNT_NO").distinct().count()

    # --- join GA4 to tactic ---
    if JOIN_KEY == "ep_srf_id2":
        ga4_keyed = ctu_events \
            .filter(F.col("ep_srf_id2").rlike("^[0-9]+$")) \
            .withColumn("_join_key", F.col("ep_srf_id2").cast("bigint"))
        tac_keyed = tactic_pop \
            .withColumn("_join_key", F.col("CLNT_NO").cast("bigint"))
    else:
        ga4_keyed = ctu_events \
            .filter(F.col("user_id").rlike("^[0-9]+$")) \
            .withColumn("_join_key", F.col("user_id").cast("bigint"))
        tac_keyed = tactic_pop \
            .withColumn("_join_key", F.col("CLNT_NO").cast("bigint"))

    banner_events = ga4_keyed.join(
        tac_keyed.select("_join_key", "TST_GRP_CD", "RPT_GRP_CD"),
        "_join_key"
    ).select(
        ga4_keyed["event_date"],
        ga4_keyed["event_name"],
        ga4_keyed["_join_key"].alias("client_key"),
        F.col("TST_GRP_CD").alias("test_control"),
        F.col("RPT_GRP_CD").alias("report_group"),
    )

    daily_raw = banner_events.groupBy("event_date", "test_control", "report_group").agg(
        F.countDistinct(
            F.when(F.col("event_name") == "view_promotion", F.col("client_key"))
        ).alias("view_users"),
        F.count(
            F.when(F.col("event_name") == "view_promotion", F.lit(1))
        ).alias("view_events"),
        F.countDistinct(
            F.when(F.col("event_name") == "select_promotion", F.col("client_key"))
        ).alias("click_users"),
        F.count(
            F.when(F.col("event_name") == "select_promotion", F.lit(1))
        ).alias("click_events"),
    )

    final = daily_raw.join(
        pop_summary,
        (daily_raw["test_control"] == pop_summary["TST_GRP_CD"]) &
        (daily_raw["report_group"] == pop_summary["RPT_GRP_CD"]),
    ).select(
        daily_raw["event_date"],
        daily_raw["test_control"],
        daily_raw["report_group"],
        pop_summary["available_leads"],
        daily_raw["view_users"],
        daily_raw["view_events"],
        daily_raw["click_users"],
        daily_raw["click_events"],
        F.round(
            F.col("click_users") /
            F.when(F.col("view_users") == 0, None).otherwise(F.col("view_users")) * 100,
            2
        ).alias("ctr_pct"),
    ).orderBy("event_date", "test_control")

    # --- pandas conversion ---
    df = final.toPandas()
    print(df.to_string(index=False))

    # --- Excel output ---
    xlsx_path = "/tmp/ctu_daily_tracker.xlsx"
    df.to_excel(xlsx_path, index=False, engine="openpyxl")

    # --- HTML output ---
    html_path = "/tmp/ctu_daily_tracker.html"

    table_html = df.to_html(index=False, border=0, classes="tracker-table")

    html_content = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>CTU Async Banner — Daily Tracker</title>
<style>
  body {{ font-family: Arial, sans-serif; margin: 30px; color: #222; }}
  h2 {{ margin-bottom: 4px; }}
  p.meta {{ color: #555; font-size: 13px; margin-top: 0; margin-bottom: 16px; }}
  table.tracker-table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
  table.tracker-table th {{
    background-color: #003366;
    color: #fff;
    padding: 8px 12px;
    text-align: left;
    border: 1px solid #ccc;
  }}
  table.tracker-table td {{
    padding: 7px 12px;
    border: 1px solid #ddd;
  }}
  table.tracker-table tr:nth-child(even) td {{ background-color: #f4f7fb; }}
  table.tracker-table tr:nth-child(odd)  td {{ background-color: #ffffff; }}
  table.tracker-table tr:hover td {{ background-color: #e8f0fe; }}
</style>
</head>
<body>
<h2>CTU Async Banner — Daily Tracker</h2>
<p class="meta">Total mobile-deployed clients: {total_mobile:,} &nbsp;|&nbsp; Join key: {JOIN_KEY} &nbsp;|&nbsp; Promo: {CTU_PROMO_TAG}</p>
{table_html}
</body>
</html>
"""

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"\nExcel: {xlsx_path}")
    print(f"HTML:  {html_path}")
