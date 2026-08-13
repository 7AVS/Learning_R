# %% [markdown]
# # 34b — CPC 1012 flips -> nearest prior VENDOR unsub, off the HDFS reservoir (Spark)
#
# Reservoir fallback for pack 34 (34_cpc_pref_vendor_unsub_gap.py), whose cell [1] gets killed
# live by TDWM on the heavy EVENT->MASTER join. Same question, same windows, same buckets.
# VENDOR unsubs come from the reservoir (unsub_base + unsub_topup); the CPC flips are a SMALL
# windowed slice pulled straight from Teradata here (the reservoir stays vendor-feedback-only).
#
# **Descriptive only - no claims.** Proximity, not attribution, in either direction.
# PREREQ: cpc_reservoir_extract.py cells [3]-[6] (unsub_base) and [24]-[27] (unsub_topup) landed.

# %% [0] sources - reservoir vendor unsubs + direct CPC flips pull
from pyspark.sql import functions as F, Window as W
import pandas as pd
import matplotlib.pyplot as plt
import getpass

spark.sparkContext.setLogLevel("ERROR")          # silence Spark WARN noise - cosmetic only
_l4j = spark._jvm.org.apache.log4j                # the red boxes (CommandsHarvester etc.) log
_l4j.LogManager.getRootLogger().setLevel(_l4j.Level.ERROR)   # from the JVM - kill WARN there too
import logging, warnings
logging.getLogger("py4j").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")
BASE = "hdfs:///user/427966379/unsub_cpc/"

WIN_FLOOR  = "2025-02-01"   # flips window - same pin as pack 34
LOOK_FLOOR = "2024-11-01"   # unsub lookback: 3 months before the window (Q4-LB convention)

ub = spark.read.option("recursiveFileLookup", "true").parquet(BASE + "unsub_base")
ut = spark.read.option("recursiveFileLookup", "true").parquet(BASE + "unsub_topup")
unsub = (ub.select("CLNT_NO", "unsub_tm", "TREATMENT_ID")
           .unionByName(ut.select("CLNT_NO", "unsub_tm", "TREATMENT_ID"))
           .dropDuplicates(["CLNT_NO", "unsub_tm"]))

# CPC flips: windowed current-state slice, pulled live (small - a few hundred K rows)
try:
    import teradatasql
except ImportError:
    get_ipython().system("pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")
    import teradatasql
username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")
EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com", user=username,
                          password=password, logmech="LDAP")
cur = EDW.cursor()
cur.execute(f"""
SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS chg_dt, APP_SYS_CD
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002       -- email switch, standing = No
  AND CHG_TMSTMP >= DATE '{WIN_FLOOR}'                  -- window only: small pull
""")
_cols = [d[0] for d in cur.description]
pref_pdf = pd.DataFrame(cur.fetchall(), columns=_cols)
cur.close()
pref = spark.createDataFrame(pref_pdf)   # one row per CLNT_NO: windowed 1012=No standing

# PROOF, not prints - ONE pass per dataset: cache, aggregate everything in a single agg
unsub = unsub.cache()
pref = pref.cache()
_un = unsub.agg(F.count("*").alias("n"), F.countDistinct("CLNT_NO").alias("nc"),
                F.min("unsub_tm").alias("mn"), F.max("unsub_tm").alias("mx")).collect()[0]
_pf = pref.agg(F.count("*").alias("n"), F.countDistinct("CLNT_NO").alias("nc"),
               F.min("chg_dt").alias("mn"), F.max("chg_dt").alias("mx")).collect()[0]

print("[0] unsub (union, deduped CLNT_NO+unsub_tm):", _un["n"], "rows |", _un["mn"], "to", _un["mx"],
      "| distinct clients:", _un["nc"])
print("[0] flips pull (1012=No, chg_dt >=", WIN_FLOOR, "):", _pf["n"], "rows |",
      _pf["mn"], "to", _pf["mx"], "| distinct clients:", _pf["nc"])
assert _un["n"] > 0, \
    "reservoir empty - run cpc_reservoir_extract.py cells [3]-[6] and [24]-[27] first"
assert _pf["n"] > 0, "flips pull returned nothing - check the Teradata connection"
assert _pf["n"] == _pf["nc"], "flips slice is not 1 row per client"

# %% [1] flips (chg_dt >= WIN_FLOOR) -> nearest prior unsub per client (unsub_dt <= flip_dt, MAX unsub_dt)
flips = pref.filter(F.col("chg_dt") >= WIN_FLOOR).select("CLNT_NO", F.col("chg_dt").alias("flip_dt"))

unsub_l = unsub.filter(F.col("unsub_tm") >= LOOK_FLOOR).withColumn("unsub_dt", F.to_date("unsub_tm"))

_w = W.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").desc())
nearest = (flips.join(unsub_l, "CLNT_NO")
                 .filter(F.col("unsub_dt") <= F.col("flip_dt"))
                 .withColumn("rn", F.row_number().over(_w))
                 .filter("rn = 1")
                 .select("CLNT_NO", "unsub_dt"))

gapped = (flips.join(nearest, "CLNT_NO", "left")             # LEFT: keep flips with no unsub (gap NULL)
                .withColumn("gap_days", F.datediff(F.col("flip_dt"), F.col("unsub_dt")))
                .cache())

print("[1] flips (1012 -> No, chg_dt >=", WIN_FLOOR, "):", flips.count())
print("[1] flips matched to a nearest prior unsub (lookback >=", LOOK_FLOOR, "):", nearest.count())
print("[1] flips with NO prior unsub found:", gapped.filter(F.col("gap_days").isNull()).count())

# %% [2] gap buckets - rollup + horizontal bar (same buckets/labels as pack 34 cell [3])
gapped_b = gapped.withColumn("gap_bucket",
    F.when(F.col("gap_days").isNull(), F.lit("6_no_unsub_found"))
     .when(F.col("gap_days") <= 1, F.lit("1_same_or_next_day"))
     .when(F.col("gap_days") <= 7, F.lit("2_within_week"))
     .when(F.col("gap_days") <= 30, F.lit("3_within_month"))
     .when(F.col("gap_days") <= 90, F.lit("4_within_quarter"))
     .otherwise(F.lit("5_over_90_days")))

roll = gapped_b.groupBy("gap_bucket").agg(F.count("*").alias("n_clients")).toPandas()
roll["share_pct"] = (roll["n_clients"] / roll["n_clients"].sum() * 100).round(1)
display(roll)

order = ["1_same_or_next_day", "2_within_week", "3_within_month",
         "4_within_quarter", "5_over_90_days", "6_no_unsub_found"]
labels = ["same/next day", "2-7 days", "8-30 days", "31-90 days", ">90 days", "no unsub found"]
r = roll.set_index("gap_bucket").reindex(order)
fig, ax = plt.subplots(figsize=(10, 5))
ax.barh(labels[::-1], r["n_clients"][::-1].fillna(0), color="#2a78d6")
for i, v in enumerate(r["n_clients"][::-1]):
    if pd.notna(v):
        ax.text(v, i, f" {int(v):,} ({r['share_pct'][::-1].iloc[i]}%)", va="center", fontsize=10)
ax.set_xlabel("clients")
ax.set_title("Where the nearest vendor unsub (disp=4) sits relative to the 1012 change\n"
             "(reservoir: unsub_base + unsub_topup)", fontweight="bold")
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# %% [3] day-level 0-90 histogram (same style as pack 34 cell [4])
vdy = (gapped.filter(F.col("gap_days").isNotNull() & (F.col("gap_days") <= 90))
             .groupBy("gap_days").agg(F.count("*").alias("n_clients"))
             .orderBy("gap_days").toPandas())
display(vdy.head(15))

fig, ax = plt.subplots(figsize=(12, 5))
ax.bar(vdy["gap_days"], vdy["n_clients"], width=0.9, color="#2a78d6")
d01 = vdy[vdy["gap_days"] <= 1]["n_clients"].sum()
ax.annotate(f"day 0-1: {d01:,}", xy=(1, vdy[vdy["gap_days"] <= 1]["n_clients"].max() if d01 else 0),
            xytext=(8, vdy["n_clients"].max() * 0.9), fontsize=11, fontweight="bold",
            arrowprops=dict(arrowstyle="->", color="#52514e"))
ax.set_xlabel("days from vendor unsub to the 1012 change")
ax.set_ylabel("clients")
ax.set_title("1012 standing became No - days since nearest prior vendor unsub (disp=4)\n"
             "Descriptive timing only; no attribution implied. (reservoir)", fontweight="bold")
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# %% [4] reverse direction - of vendor unsubbers (first unsub >= WIN_FLOOR), % who flip 1012 within
# 90 days after. The windowed flips slice suffices: any flip within 90d of a >=WIN_FLOOR unsub is
# itself >= WIN_FLOOR by construction.
first_unsub = (unsub.filter(F.col("unsub_tm") >= WIN_FLOOR)
                     .withColumn("unsub_dt", F.to_date("unsub_tm"))
                     .groupBy("CLNT_NO").agg(F.min("unsub_dt").alias("first_unsub_dt")))

all_flips = pref.select("CLNT_NO", F.col("chg_dt").alias("flip_dt"))

rev = (first_unsub.join(all_flips, "CLNT_NO", "left")
                   .withColumn("flipped_90d",
                       F.when((F.col("flip_dt") >= F.col("first_unsub_dt")) &
                              (F.col("flip_dt") < F.date_add(F.col("first_unsub_dt"), 90)), 1)
                        .otherwise(0)))

n_unsub_clients = first_unsub.select("CLNT_NO").distinct().count()
n_flipped = rev.filter("flipped_90d = 1").select("CLNT_NO").distinct().count()
pct = round(100.0 * n_flipped / n_unsub_clients, 2) if n_unsub_clients else 0
print("[4] unsub clients (first unsub >=", WIN_FLOOR, "):", n_unsub_clients)
print("[4] of those, flipped 1012 -> No within 90 days after:", n_flipped, "(", pct, "% )")
print("[4] caveat: flips visible here are arrivals into CURRENT standing only - re-consented clients don't count.")

# %% [5] BONUS - writers cut: APP_SYS_CD (the system that wrote the 1012 flip) for flips WITH an
# unsub within 7 days vs flips with NO unsub found. Label: who writes the vendor-proximate No's vs the rest.
gap_writer = gapped.join(pref.select("CLNT_NO", "APP_SYS_CD"), "CLNT_NO", "left")

within7 = gap_writer.filter(F.col("gap_days").isNotNull() & (F.col("gap_days") <= 7)).cache()
no_unsub = gap_writer.filter(F.col("gap_days").isNull()).cache()
n7, nn = within7.count(), no_unsub.count()

print("[5] writers cut - APP_SYS_CD distribution, flips WITH an unsub within 7 days (n =", n7, "):")
display((within7.groupBy("APP_SYS_CD").agg(F.count("*").alias("clients"))
                 .withColumn("pct", F.round(100.0 * F.col("clients") / n7, 1))
                 .orderBy(F.desc("clients")).toPandas()))

print("[5] writers cut - APP_SYS_CD distribution, flips with NO unsub found (n =", nn, "):")
display((no_unsub.groupBy("APP_SYS_CD").agg(F.count("*").alias("clients"))
                  .withColumn("pct", F.round(100.0 * F.col("clients") / nn, 1))
                  .orderBy(F.desc("clients")).toPandas()))

print("[5] read: who writes the vendor-proximate No's vs the rest - a writer that dominates 'within 7")
print("    days' but not 'no unsub found' is a candidate pipe; even split across both = coincidence.")

# %% [6] BRIDGE ATTRIBUTION BY CAMPAIGN - which mnemonics does the pipe carry?
# Rule: attribution window = ALL unsub events 0-1 days before the flip (the pipe is a
# next-day batch - see [3]); one attribution decision per client (flips are 1 row per
# client); several distinct MNEs in-window -> explicit MULTI bucket, never an arbitrary
# winner. MNE = SUBSTR(TREATMENT_ID, 8, 3); 'DEFAULT'/unparseable -> UNTAGGED.
ATTR_DAYS = 1   # primary window; sensitivity below reruns at 7

def bridge_attribution(attr_days):
    ev = (flips.join(unsub_l.select("CLNT_NO", "unsub_dt", "TREATMENT_ID"), "CLNT_NO")
               .withColumn("d", F.datediff(F.col("flip_dt"), F.col("unsub_dt")))
               .filter((F.col("d") >= 0) & (F.col("d") <= attr_days))
               .withColumn("mne", F.when(F.upper(F.col("TREATMENT_ID")) == "DEFAULT", F.lit("UNTAGGED"))
                                   .otherwise(F.upper(F.substring("TREATMENT_ID", 8, 3))))
               .withColumn("mne", F.when(F.col("mne").rlike("^[A-Z0-9]{3}$"), F.col("mne"))
                                   .otherwise(F.lit("UNTAGGED"))))
    per_client = (ev.groupBy("CLNT_NO")
                    .agg(F.countDistinct("mne").alias("n_mnes"),
                         F.min("mne").alias("only_mne"),
                         F.concat_ws("+", F.sort_array(F.collect_set("mne"))).alias("mne_combo"),
                         F.count("*").alias("n_events"))
                    .withColumn("attributed_mne",
                                F.when(F.col("n_mnes") == 1, F.col("only_mne")).otherwise(F.lit("MULTI"))))
    return ev, per_client

ev1, pc1 = bridge_attribution(ATTR_DAYS)
n_bridged = pc1.count()
print(f"[6] bridged clients (>=1 unsub within {ATTR_DAYS}d before flip): {n_bridged:,} - each counted ONCE")
print("[6] attribution by campaign (distinct clients; MULTI = several MNEs in-window, shown as-is):")
display(pc1.groupBy("attributed_mne").agg(F.count("*").alias("clients"))
          .withColumn("pct", F.round(100.0 * F.col("clients") / n_bridged, 1))
          .orderBy(F.desc("clients")).toPandas().head(25))
print("[6] the MULTI combos (top 15) - the batch multi-list clicks:")
display(pc1.filter("attributed_mne = 'MULTI'").groupBy("mne_combo")
          .agg(F.count("*").alias("clients")).orderBy(F.desc("clients")).toPandas().head(15))

# sensitivity: same attribution at <=7 days - if the campaign ranking holds, the
# window choice is not driving the story
_, pc7 = bridge_attribution(7)
n7 = pc7.count()
print(f"[6] sensitivity <=7d: {n7:,} bridged clients; attribution:")
display(pc7.groupBy("attributed_mne").agg(F.count("*").alias("clients"))
          .withColumn("pct", F.round(100.0 * F.col("clients") / n7, 1))
          .orderBy(F.desc("clients")).toPandas().head(25))
