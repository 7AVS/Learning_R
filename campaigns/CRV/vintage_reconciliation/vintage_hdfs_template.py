# vintage_hdfs_template.py
# Config-driven production vintage on YARN Spark / HDFS. Reusable across campaigns:
# swap the CFG block below, the logic underneath stays the same.
# Env: Lumina/AI Farm -- `spark` is pre-initialized (no builder/stop).
#
# Learnings baked in (CRV, 2026-07-09):
#   - cohort/day-0/window-start = EFFECTIVE date (eff_date_field), NOT the start date
#     (start runs a few days later -> drifts ~1.7% of accounts into the next month).
#   - account-key join normalized to numeric (events acct is zero-padded ~23-char string).
#   - arm from explicit action/control code lists; other groups EXCLUDED (not lumped).
#   - real cohort_month; no hardcoded 0..90 day cap (per-cohort window bound).
#   - TACTIC_EVNT_ID is the DERIVED CLIENT NUMBER (used as the client-grain key).

from pyspark.sql import functions as F, Window
from pyspark.sql.types import DecimalType
import base64
from IPython.display import display, HTML

# ============================================================================
# CFG -- the ONLY campaign-specific block. Everything below is generic.
# ============================================================================
CFG = {
    # campaign identity
    "mnemonic":          "CRV",                     # TACTIC_ID positions 8-10
    "success_event_cd":  "p_card_installmt_purch",  # event_cd in the measurement table

    # measurement period: deployments whose END date falls in this window
    "end_window":        ("2026-05-01", "2026-07-31"),
    "years":             ["2026"],                  # partition years to scan

    # arm detection (VARIES by campaign)
    "arm_field":         "TST_GRP_CD",              # some campaigns use a different field
    "action_codes":      ["TG4"],
    "control_codes":     ["TG8"],
    "split_by_group":    False,                     # True -> keep each raw group as its own arm

    # grain + keys on the tactic table
    "grain":             "account",                 # "account" or "client"
    "acct_key_field":    "VISA_ACCT_NO",            # account-grain key
    "client_key_field":  "TACTIC_EVNT_ID",          # client-grain key = DERIVED client number
    "eff_date_field":    "TREATMT_EFF_DT",          # cohort/day-0/window start
    "end_date_field":    "TREATMT_END_DT",          # window close
    "tactic_id_field":   "TACTIC_ID",

    # HDFS sources
    "tactic_base":       "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/",
    "tactic_partition":  "EVNT_STRT_DT",
    "events_base":       "/prod/16131/app/ZP10/lab/data/tde/measurement/dataversion=dev/measurement_events_v2/",
    "events_partition":  "event_date",
    "events_acct_field": "acct_no",                 # joins to acct_key_field
    "events_client_field": "clnt_no",               # joins to client_key_field
    "events_evcd_field": "event_cd",
    "events_date_field": "event_date",
}

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)   # campaign-scale join guard


def _tactic_key(cfg):
    return cfg["acct_key_field"] if cfg["grain"] == "account" else cfg["client_key_field"]

def _events_key(cfg):
    return cfg["events_acct_field"] if cfg["grain"] == "account" else cfg["events_client_field"]


# ============================================================================
# CELL 1 -- SCHEMA CHECK (run first). Confirms the CFG fields exist on the tables.
# ============================================================================
def check_schema(cfg=CFG):
    tac = spark.read.parquet(*[cfg["tactic_base"] + f"{cfg['tactic_partition']}={y}*" for y in cfg["years"]])
    ev  = spark.read.option("basePath", cfg["events_base"]).parquet(cfg["events_base"] + f"{cfg['events_partition']}=2026*")
    need_tac = [cfg["tactic_id_field"], cfg["arm_field"], cfg["eff_date_field"],
                cfg["end_date_field"], _tactic_key(cfg)]
    need_ev  = [_events_key(cfg), cfg["events_evcd_field"], cfg["events_date_field"]]
    print("TACTIC columns:", sorted(tac.columns))
    for c in need_tac:
        print(f"   {c:18s} present={c in tac.columns}")
    print("EVENTS columns:", sorted(ev.columns))
    for c in need_ev:
        print(f"   {c:18s} present={c in ev.columns}")

# check_schema()   # <-- run and confirm all present=True before building


# ============================================================================
# CELL 2 -- BUILD THE VINTAGE (generic)
# ============================================================================
def build_vintage_hdfs(cfg=CFG):
    end_lo, end_hi = cfg["end_window"]
    tkey, ekey = _tactic_key(cfg), _events_key(cfg)

    tac = (
        spark.read.parquet(*[cfg["tactic_base"] + f"{cfg['tactic_partition']}={y}*" for y in cfg["years"]])
        .filter(F.substring(F.col(cfg["tactic_id_field"]), 8, 3) == cfg["mnemonic"])
        .filter(F.col(cfg["end_date_field"]).between(F.lit(end_lo), F.lit(end_hi)))
        .filter(F.col(cfg["eff_date_field"]).isNotNull())
        .filter(F.col(cfg["arm_field"]).isin(cfg["action_codes"] + cfg["control_codes"]))
        .withColumn("k", F.col(tkey).cast(DecimalType(38, 0)))
        .withColumn("cohort_month", F.trunc(F.col(cfg["eff_date_field"]), "month"))
    )
    if cfg["split_by_group"]:
        tac = tac.withColumn("arm", F.col(cfg["arm_field"]))
    else:
        tac = tac.withColumn("arm", F.when(F.col(cfg["arm_field"]).isin(cfg["action_codes"]), F.lit("Action"))
                                     .otherwise(F.lit("Control")))
    cohort = (
        tac.groupBy("k", "cohort_month", "arm")
        .agg(F.min(cfg["eff_date_field"]).alias("eff_dt"),
             F.max(cfg["end_date_field"]).alias("end_dt"))
        .persist()
    )
    cohort_cells = (
        cohort.groupBy("cohort_month", "arm")
        .agg(F.countDistinct("k").alias("cohort_size"),
             F.max(F.datediff("end_dt", "eff_dt")).alias("cohort_max_day"))
    )

    events = (
        spark.read.option("basePath", cfg["events_base"]).parquet(cfg["events_base"] + f"{cfg['events_partition']}=2026*")
        .filter(F.col(cfg["events_evcd_field"]) == cfg["success_event_cd"])
        .withColumn("k", F.col(ekey).cast(DecimalType(38, 0)))
        .select("k", F.col(cfg["events_date_field"]).alias("event_date"))
    )
    hits = (
        cohort.join(events, "k", "inner")
        .filter(F.col("event_date").between(F.col("eff_dt"), F.col("end_dt")))
        .withColumn("vintage_day", F.datediff("event_date", "eff_dt"))
        .groupBy("k", "cohort_month", "arm").agg(F.min("vintage_day").alias("vintage_day"))
    )
    daily = hits.groupBy("cohort_month", "arm", "vintage_day").agg(F.countDistinct("k").alias("n"))

    max_day = cohort_cells.agg(F.max("cohort_max_day")).first()[0] or 0
    spine = spark.range(0, int(max_day) + 1).withColumnRenamed("id", "vintage_day")
    grid = cohort_cells.crossJoin(spine).filter(F.col("vintage_day") <= F.col("cohort_max_day"))

    w = Window.partitionBy("cohort_month", "arm").orderBy("vintage_day").rowsBetween(Window.unboundedPreceding, 0)
    vintage = (
        grid.join(daily, ["cohort_month", "arm", "vintage_day"], "left").fillna(0, ["n"])
        .withColumn("cum_responders", F.sum("n").over(w))
        .select(F.lit(cfg["mnemonic"]).alias("campaign"),
                "cohort_month", "arm", "vintage_day", "cohort_size", "cum_responders")
        .orderBy("cohort_month", "arm", "vintage_day")
    )
    pdf = vintage.toPandas()
    cohort.unpersist()
    return pdf


# ============================================================================
# CELL 3 -- RUN + DOWNLOAD (plain button)
# ============================================================================
def download_csv(pdf, filename=None):
    filename = filename or f"{CFG['mnemonic'].lower()}_vintage.csv"
    b64 = base64.b64encode(pdf.to_csv(index=False).encode()).decode()
    html = (f'<a download="{filename}" href="data:text/csv;base64,{b64}" '
            f'style="display:inline-block;padding:8px 16px;border:1px solid #333;'
            f'border-radius:3px;color:#111;text-decoration:none;'
            f'font-family:sans-serif;font-size:13px;">Download {filename} ({len(pdf):,} rows)</a>')
    display(HTML(html))

# vintage_pdf = build_vintage_hdfs()
# download_csv(vintage_pdf)
