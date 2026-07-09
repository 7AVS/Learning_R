# crv_vintage_hdfs_spark.py
# CRV production vintage on YARN Spark / HDFS (avoids Trino cross-lake federation).
# Corrected with today's learnings vs the old one-shot notebook:
#   - ACCOUNT grain via VISA_ACCT_NO (old notebook wrongly derived CLNT_NO from TACTIC_EVNT_ID)
#   - cohort anchor / day-0 / window start = TREATMT_EFF_DT  (NOT TREATMT_STRT_DT: strt runs a
#     few days later and drifts ~1.7% of accounts into the next cohort month)
#   - window = [TREATMT_EFF_DT, TREATMT_END_DT]; inclusion filter on TREATMT_END_DT
#   - arm: TG4 -> Action, TG8 -> Control, EXCLUDE all other tst_grp_cd
#   - account-key join normalized to numeric (events acct_no is zero-padded ~23-char string)
#   - real cohort_month (not raw wave date); no hardcoded 0..90 day cap
#
# UNVERIFIED (schema-check cell below proves/refutes): that the HDFS tactic table
# DTZTA_T_TACTIC_EVNT_HIST actually carries VISA_ACCT_NO and TREATMT_EFF_DT. If it does NOT,
# pull tactic from EDW via cursor instead and keep Spark only for the events + curve.
# Also: events path is dataversion=dev -- confirm dev vs prod is what you want to measure.
#
# Env: Lumina/AI Farm -- `spark` is pre-initialized (no builder/stop).

from pyspark.sql import functions as F, Window
from pyspark.sql import types as T
import base64
from IPython.display import display, HTML

TACTIC_BASE  = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
EVENTS_BASE  = "/prod/16131/app/ZP10/lab/data/tde/measurement/dataversion=dev/measurement_events_v2/"
YEARS        = ["2026"]
MNE          = "CRV"
SUCCESS_CD   = "p_card_installmt_purch"
END_LO, END_HI = "2026-05-01", "2026-07-31"   # inclusion: TREATMT_END_DT in this window

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)   # campaign-scale join guard


# ============================================================================
# CELL 1 -- SCHEMA CHECK (run first). Confirms the fields the corrected logic needs.
# ============================================================================
def check_schema():
    tac = spark.read.parquet(*[TACTIC_BASE + f"EVNT_STRT_DT={y}*" for y in YEARS])
    ev  = spark.read.option("basePath", EVENTS_BASE).parquet(EVENTS_BASE + "event_date=2026*")
    print("TACTIC columns:")
    print("  ", sorted(tac.columns))
    for c in ["VISA_ACCT_NO", "TREATMT_EFF_DT", "TREATMT_END_DT", "TST_GRP_CD", "TACTIC_ID"]:
        print(f"   {c:16s} present={c in tac.columns}")
    print("EVENTS columns:")
    print("  ", sorted(ev.columns))
    for c in ["acct_no", "clnt_no", "event_cd", "event_date"]:
        print(f"   {c:16s} present={c in ev.columns}")

# check_schema()   # <-- run this and confirm VISA_ACCT_NO + TREATMT_EFF_DT exist before building


# ============================================================================
# CELL 2 -- BUILD THE VINTAGE
# ============================================================================
def build_crv_vintage_hdfs():
    # --- tactic cohort: account grain, EFF-date anchor, TG4/TG8 only -------------
    tac = (
        spark.read.parquet(*[TACTIC_BASE + f"EVNT_STRT_DT={y}*" for y in YEARS])
        .filter(F.substring(F.col("TACTIC_ID"), 8, 3) == MNE)
        .filter(F.col("TREATMT_END_DT").between(F.lit(END_LO), F.lit(END_HI)))
        .filter(F.col("TREATMT_EFF_DT").isNotNull())
        .filter(F.col("TST_GRP_CD").isin("TG4", "TG8"))
        .withColumn("acct_key", F.col("VISA_ACCT_NO").cast(T.DecimalType(38, 0)))
        .withColumn("cohort_month", F.trunc(F.col("TREATMT_EFF_DT"), "month"))
        .withColumn("arm", F.when(F.col("TST_GRP_CD") == "TG4", F.lit("Action"))
                            .otherwise(F.lit("Control")))
    )
    cohort = (
        tac.groupBy("acct_key", "cohort_month", "arm")
        .agg(F.min("TREATMT_EFF_DT").alias("eff_dt"),
             F.max("TREATMT_END_DT").alias("end_dt"))
    )
    cohort = cohort.persist()

    cohort_cells = (
        cohort.groupBy("cohort_month", "arm")
        .agg(F.countDistinct("acct_key").alias("cohort_size"),
             F.max(F.datediff("end_dt", "eff_dt")).alias("cohort_max_day"))
    )

    # --- success: in-window p_card_installmt_purch, joined on normalized acct key ---
    events = (
        spark.read.option("basePath", EVENTS_BASE).parquet(EVENTS_BASE + "event_date=2026*")
        .filter(F.col("event_cd") == SUCCESS_CD)
        .withColumn("acct_key", F.col("acct_no").cast(T.DecimalType(38, 0)))
        .select("acct_key", "event_date")
    )
    hits = (
        cohort.join(events, "acct_key", "inner")
        .filter(F.col("event_date").between(F.col("eff_dt"), F.col("end_dt")))
        .withColumn("vintage_day", F.datediff("event_date", "eff_dt"))
        .groupBy("acct_key", "cohort_month", "arm")
        .agg(F.min("vintage_day").alias("vintage_day"))          # first in-window success
    )
    daily = (
        hits.groupBy("cohort_month", "arm", "vintage_day")
        .agg(F.countDistinct("acct_key").alias("n_accts"))
    )

    # --- day spine 0..max, capped per cohort at its own window -------------------
    max_day = cohort_cells.agg(F.max("cohort_max_day")).first()[0] or 0
    spine = spark.range(0, int(max_day) + 1).withColumnRenamed("id", "vintage_day")
    grid = (
        cohort_cells.crossJoin(spine)
        .filter(F.col("vintage_day") <= F.col("cohort_max_day"))
    )

    w = (Window.partitionBy("cohort_month", "arm")
         .orderBy("vintage_day").rowsBetween(Window.unboundedPreceding, 0))
    vintage = (
        grid.join(daily, ["cohort_month", "arm", "vintage_day"], "left")
        .fillna(0, ["n_accts"])
        .withColumn("cum_responders", F.sum("n_accts").over(w))
        .select(F.lit("CRV").alias("campaign"),
                "cohort_month", "arm", "vintage_day", "cohort_size", "cum_responders")
        .orderBy("cohort_month", "arm", "vintage_day")
    )
    pdf = vintage.toPandas()
    cohort.unpersist()
    return pdf


# ============================================================================
# CELL 3 -- RUN + DOWNLOAD (plain button, no emoji/box)
# ============================================================================
def download_csv(pdf, filename="crv_vintage_production.csv"):
    b64 = base64.b64encode(pdf.to_csv(index=False).encode()).decode()
    html = (f'<a download="{filename}" href="data:text/csv;base64,{b64}" '
            f'style="display:inline-block;padding:8px 16px;border:1px solid #333;'
            f'border-radius:3px;color:#111;text-decoration:none;'
            f'font-family:sans-serif;font-size:13px;">'
            f'Download {filename} ({len(pdf):,} rows)</a>')
    display(HTML(html))

# vintage_pdf = build_crv_vintage_hdfs()
# download_csv(vintage_pdf)
