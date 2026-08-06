# pm_asks_recompute.py — PM asks #2 (attrition) and #3 (population-fixed
# profit). SELF-CONTAINED: run top-to-bottom in ANY pod Jupyter kernel with
# Spark — no other notebook or file needed. Reads only the landed HDFS
# parquet from the 2026-08-03e run; Teradata is never touched.
# One assertion per cell; each cell rerunnable.

from pyspark.sql import functions as F

# ---------------------------------------------------------------- Cell 0
# Landed data locations (from unsub_unified.py build 03e, schema v3).
BASE = "hdfs:///user/427966379/unsub_unified/"
BCOHORT_DIR = BASE + "b_cohort_v3/"
BUCP_DIR = BASE + "b_ucp_v3/"

b_coh = (spark.read.parquet(BCOHORT_DIR + "bite_?")
         .withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long")))
b_ucp = (spark.read.parquet(BUCP_DIR + "bite_?")
         .withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long")))
print(f"b_cohort rows: {b_coh.count():,}  (expect ~4,783,193)")
print(f"b_ucp rows:    {b_ucp.count():,}  (expect same order)")

# ---------------------------------------------------------------- Cell 1
# Join + population accounting. THEN population (the cohort) is the fixed
# base; LEFT join so nobody drops.
need_coh = {"clnt_no", "cards_unsub_by_anchor"}
need_ucp = {"clnt_no", "prof_then", "prof_now", "held_c_then", "held_c_now"}
assert need_coh <= set(b_coh.columns), f"b_cohort missing {need_coh - set(b_coh.columns)}"
assert need_ucp <= set(b_ucp.columns), f"b_ucp missing {need_ucp - set(b_ucp.columns)}"

j = (b_coh.select("clnt_no", "cards_unsub_by_anchor")
     .join(b_ucp.select(*need_ucp), "clnt_no", "left")
     .withColumn("grp", F.when(F.col("cards_unsub_by_anchor") == 1, "leaver")
                         .otherwise("stayer")))
n_coh, n_j = b_coh.count(), j.count()
print(f"cohort {n_coh:,} -> joined {n_j:,} (must match; LEFT join keeps base fixed)")
assert n_j == n_coh, "join changed the population - STOP"
j.cache()

# ---------------------------------------------------------------- Cell 2
# Population ledger per group: matched then / matched now / vanished-now.
# 'vanished_now' (matched then, missing now) is the survivorship leak the
# PM flagged. See it before any profit number.
(j.groupBy("grp").agg(
    F.count("*").alias("clients_then_pop"),
    F.sum(F.when(F.col("prof_then").isNotNull(), 1).otherwise(0)).alias("matched_then"),
    F.sum(F.when(F.col("prof_now").isNotNull(), 1).otherwise(0)).alias("matched_now"),
    F.sum(F.when(F.col("prof_then").isNotNull() & F.col("prof_now").isNull(), 1)
           .otherwise(0)).alias("vanished_now"),
)).show()

# ---------------------------------------------------------------- Cell 3
# PROFIT three ways, per group. (a) survivors-only = the published basis
# (replication check). (b) population-fixed, vanished at $0 now — the
# PM's requested basis; if (b) disagrees with (a), (b) is what we report.
# (c) then-profit of the vanished, so the reader sees what walked out.
(j.filter(F.col("prof_then").isNotNull()).groupBy("grp").agg(
    F.count("*").alias("n_then_matched"),
    F.avg("prof_then").alias("avg_then_all"),
    F.avg(F.when(F.col("prof_now").isNotNull(), F.col("prof_then"))).alias("avg_then_survivors"),
    F.avg("prof_now").alias("avg_now_survivors"),
    F.avg(F.coalesce(F.col("prof_now"), F.lit(0.0))).alias("avg_now_zerofill"),
    F.avg(F.when(F.col("prof_now").isNull(), F.col("prof_then"))).alias("avg_then_of_vanished"),
)).show()
print("READ: (a) avg_now_survivors - avg_then_survivors = old claim;"
      " (b) avg_now_zerofill - avg_then_all = population-fixed delta.")

# ---------------------------------------------------------------- Cell 4
# ATTRITION (ask #2). Base: held the card category at THEN (held_c_then=1).
# lost_cards = held_c_now = 0 (visible in UCP, category gone);
# vanished   = held_c_now = -1 (no UCP match at now — left-bank PROXY,
#              labeled proxy, not proven exit).
(j.filter(F.col("held_c_then") == 1).groupBy("grp").agg(
    F.count("*").alias("held_cards_then"),
    F.sum(F.when(F.col("held_c_now") == 0, 1).otherwise(0)).alias("lost_cards_now"),
    F.sum(F.when(F.col("held_c_now") == -1, 1).otherwise(0)).alias("vanished_from_ucp_now"),
)).show()
print("READ: rates = lost/held and vanished/held, leaver vs stayer."
      " Descriptive, not causal (unsubbers skew younger / 4-7yr tenure).")

# ---------------------------------------------------------------- Cell 5
# EXPORT for local plotting: the three tables above, stacked long
# (table, grp, metric, value) into ONE small CSV (~40 rows) in
# ~/unsub_unified_out/. Download it and plot locally - no pod needed
# after this.
import os, pandas as pd

def _long(sdf, table):
    pdf = sdf.toPandas()
    out = pdf.melt(id_vars=["grp"], var_name="metric", value_name="value")
    out.insert(0, "table", table)
    return out

_ledger = (j.groupBy("grp").agg(
    F.count("*").alias("clients_then_pop"),
    F.sum(F.when(F.col("prof_then").isNotNull(), 1).otherwise(0)).alias("matched_then"),
    F.sum(F.when(F.col("prof_now").isNotNull(), 1).otherwise(0)).alias("matched_now"),
    F.sum(F.when(F.col("prof_then").isNotNull() & F.col("prof_now").isNull(), 1)
           .otherwise(0)).alias("vanished_now")))
_prof = (j.filter(F.col("prof_then").isNotNull()).groupBy("grp").agg(
    F.count("*").alias("n_then_matched"),
    F.avg("prof_then").alias("avg_then_all"),
    F.avg(F.when(F.col("prof_now").isNotNull(), F.col("prof_then"))).alias("avg_then_survivors"),
    F.avg("prof_now").alias("avg_now_survivors"),
    F.avg(F.coalesce(F.col("prof_now"), F.lit(0.0))).alias("avg_now_zerofill"),
    F.avg(F.when(F.col("prof_now").isNull(), F.col("prof_then"))).alias("avg_then_of_vanished")))
_att = (j.filter(F.col("held_c_then") == 1).groupBy("grp").agg(
    F.count("*").alias("held_cards_then"),
    F.sum(F.when(F.col("held_c_now") == 0, 1).otherwise(0)).alias("lost_cards_now"),
    F.sum(F.when(F.col("held_c_now") == -1, 1).otherwise(0)).alias("vanished_from_ucp_now")))

_out = pd.concat([_long(_ledger, "population_ledger"),
                  _long(_prof, "profit_three_ways"),
                  _long(_att, "card_attrition")], ignore_index=True)
_dest = os.path.expanduser("~/unsub_unified_out/pm_asks_results.csv")
_out.to_csv(_dest, index=False)
print(f"WROTE {_dest} ({len(_out)} rows) - download this for local plotting.")
