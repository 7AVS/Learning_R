# =============================================================================
# CTU Async Banner — Daily Tracker (PySpark / HDFS)
# =============================================================================
# Jira: NBA-12268 | Live: April 10, 2026
# Join: t.CLNT_NO = b.up_srf_id2_value
# GA4 filter: it_item_id = 'i_300102'
# Tactic: TACTIC_ID = '2026098CTU', mobile = SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MB%'
# =============================================================================

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

# --- Config ---
TACTIC_ID = "2026098CTU"
TACTIC_EVNT_HIST_BASE = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
TACTIC_YEARS = [2026]
GA4_ECOM_BASE = "/prod/sz/tsz/00198/data/ga4-ecommerce"
GA4_START_MONTH = "04"
CAMPAIGN = "CTU"
IT_ITEM_ID = "i_300102"


# %% Tactic population — mobile-only CTU clients

tactic_paths = [f"{TACTIC_EVNT_HIST_BASE}EVNT_STRT_DT={y}*" for y in TACTIC_YEARS]
raw_tactic = spark.read \
    .option("basePath", TACTIC_EVNT_HIST_BASE) \
    .parquet(*tactic_paths)

tactic_pop = raw_tactic \
    .filter(
        (F.col("TACTIC_ID") == TACTIC_ID) &
        F.substring(F.col("TACTIC_DECISN_VRB_INFO"), 121, 30).contains("MB")
    ) \
    .select(
        F.col("CLNT_NO"),
        F.trim(F.col("TST_GRP_CD")).alias("TST_GRP_CD"),
        F.trim(F.col("RPT_GRP_CD")).alias("RPT_GRP_CD"),
    ) \
    .persist(StorageLevel.MEMORY_AND_DISK)

total_pop = tactic_pop.select("CLNT_NO").distinct().count()
print(f"Mobile-deployed clients: {total_pop:,}")
tactic_pop.groupBy("TST_GRP_CD", "RPT_GRP_CD") \
    .agg(F.countDistinct("CLNT_NO").alias("clients")) \
    .show(truncate=False)


# %% GA4 banner events joined to tactic population

ga4_paths = [f"{GA4_ECOM_BASE}/YEAR=2026/Month=*/*"]

banner_events = spark.read \
    .option("basePath", GA4_ECOM_BASE) \
    .parquet(*ga4_paths) \
    .filter(
        (F.col("Month") >= GA4_START_MONTH) &
        (F.lower(F.col("it_item_id")) == IT_ITEM_ID) &
        F.lower(F.col("event_name")).isin("view_promotion", "select_promotion")
    ) \
    .select(
        F.col("event_date"),
        F.lower(F.col("event_name")).alias("event_name"),
        F.col("up_srf_id2_value"),
        F.col("platform"),
    ) \
    .join(tactic_pop, F.col("up_srf_id2_value") == F.col("CLNT_NO")) \
    .persist(StorageLevel.MEMORY_AND_DISK)

matched_users = banner_events.select("up_srf_id2_value").distinct().count()
print(f"Matched banner users: {matched_users:,}")


# %% OUTPUT 1: Daily tracker

daily = banner_events.groupBy("event_date").agg(
    F.countDistinct(
        F.when(F.col("event_name") == "view_promotion", F.col("up_srf_id2_value"))
    ).alias("view_users"),
    F.countDistinct(
        F.when(F.col("event_name") == "select_promotion", F.col("up_srf_id2_value"))
    ).alias("click_users"),
).orderBy("event_date")

daily_df = daily.toPandas()
daily_df.insert(0, "campaign", CAMPAIGN)
daily_df.insert(1, "total_population", total_pop)

print("\n=== DAILY TRACKER ===")
print(daily_df.to_string(index=False))


# %% OUTPUT 2: YTD cumulative summary

ytd_views = banner_events.filter(F.col("event_name") == "view_promotion") \
    .select("up_srf_id2_value").distinct().count()
ytd_clicks = banner_events.filter(F.col("event_name") == "select_promotion") \
    .select("up_srf_id2_value").distinct().count()
date_from = banner_events.agg(F.min("event_date")).collect()[0][0]
date_to = banner_events.agg(F.max("event_date")).collect()[0][0]

print(f"\n=== YTD SUMMARY ({CAMPAIGN}) ===")
print(f"Date from inception:    {date_from}")
print(f"Date last event:        {date_to}")
print(f"Total population:       {total_pop:,}")
print(f"Unique view users:      {ytd_views:,}")
print(f"Unique click users:     {ytd_clicks:,}")


# %% Save outputs

import pandas as pd

xlsx_path = "/tmp/ctu_daily_tracker.xlsx"
try:
    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        daily_df.to_excel(writer, sheet_name="Daily", index=False)
        ytd_df = pd.DataFrame([{
            "campaign": CAMPAIGN,
            "date_from_inception": str(date_from),
            "date_last_event": str(date_to),
            "total_population": total_pop,
            "unique_view_users": ytd_views,
            "unique_click_users": ytd_clicks,
        }])
        ytd_df.to_excel(writer, sheet_name="YTD Summary", index=False)
    print(f"Saved: {xlsx_path}")
except ModuleNotFoundError:
    daily_df.to_csv("/tmp/ctu_daily_tracker.csv", index=False)
    print("Saved: /tmp/ctu_daily_tracker.csv")

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
  .summary {{ font-size: 14px; margin-bottom: 20px; }}
</style></head><body>
<h2>CTU Async Banner — Daily Tracker</h2>
<div class="summary">
<p>Campaign: <strong>{CAMPAIGN}</strong> | Population: <strong>{total_pop:,}</strong></p>
<p>Period: <strong>{date_from}</strong> to <strong>{date_to}</strong></p>
<p>YTD unique views: <strong>{ytd_views:,}</strong> | YTD unique clicks: <strong>{ytd_clicks:,}</strong></p>
</div>
{daily_df.to_html(index=False, border=0, classes="t")}
</body></html>""")
print(f"HTML saved: {html_path}")


# %% Cleanup

tactic_pop.unpersist()
banner_events.unpersist()
