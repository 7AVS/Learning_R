# CPC RT3 AUDIT - evidence cells that answer Red Team v3 (red_team_review_round3_fable.md).
# Reads the HDFS reservoir (Spark). NO Teradata.
# PREREQ: re-run cpc_reservoir_extract.py first - cell [15] now re-pulls cpc_landing_allsw with a wider floor, and [17] lands unsub_match_diag.
# Each cell is one museum output feeding one deck fix. Counts = distinct clients unless labelled.

# %% [load]
from pyspark.sql import functions as F, Window as W
spark.sparkContext.setLogLevel("ERROR")
import logging, warnings
logging.getLogger("py4j").setLevel(logging.ERROR); warnings.filterwarnings("ignore")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
BASE = "hdfs:///user/427966379/unsub_cpc/"

ub      = spark.read.parquet(BASE + "unsub_base/*")
cpc     = spark.read.option("recursiveFileLookup", "true").parquet(BASE + "cpc_pref")
rec     = spark.read.parquet(BASE + "q2_recipients/*").distinct()
landing = spark.read.option("recursiveFileLookup", "true").parquet(BASE + "cpc_landing_allsw")

first_unsub = ub.groupBy("CLNT_NO").agg(F.min("unsub_tm").alias("u_tm"))
got = rec.select("CLNT_NO").distinct().withColumn("got", F.lit(1))

def standing3(pref, as_of):
    # latest row per client on `pref` before as_of; classify Yes(5001)/No(5002)/blank(else)
    w = W.partitionBy("CLNT_NO").orderBy(F.col("CHG_TMSTMP").desc())
    b = cpc.filter("PREF_ID = " + str(pref) + " AND CHG_TMSTMP < " + as_of)
    latest = b.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
    return latest.withColumn("st", F.when(F.col("CLNT_CONSENT_TYP") == 5002, "No")
                                    .when(F.col("CLNT_CONSENT_TYP") == 5001, "Yes").otherwise("blank")) \
                 .select("CLNT_NO", "st", "CHG_TMSTMP")

print("[load] cohort:", first_unsub.count(), "| landing rows:", landing.count(), "| recipients:", got.count())

# %% [A1] RT3 Obj1 - DAY-LAG of the post-unsub CPC writes. A real pipe spikes 0-7d; coincidence smears across 90d.
j = (landing.join(first_unsub, "CLNT_NO")
     .withColumn("lag", F.datediff("CHG_TMSTMP", "u_tm"))
     .filter((F.col("lag") >= 0) & (F.col("lag") <= 90))
     .withColumn("bucket", F.when(F.col("lag") <= 1, "a_0-1d").when(F.col("lag") <= 7, "b_2-7d")
                 .when(F.col("lag") <= 30, "c_8-30d").otherwise("d_31-90d")))
print("[A1] lag from unsubscribe to the CPC write (distinct clients per bucket):")
j.groupBy("bucket").agg(F.countDistinct("CLNT_NO").alias("clients")).orderBy("bucket").show()

# %% [A2] RT3 Obj1 - PLACEBO baseline. Same clients, a 90d window BEFORE the unsub vs the 90d AFTER. Similar => background, not caused.
jj = landing.join(first_unsub, "CLNT_NO").withColumn("lag", F.datediff("CHG_TMSTMP", "u_tm"))
coh = first_unsub.count()
post = jj.filter((F.col("lag") >= 0)   & (F.col("lag") <= 90)).select("CLNT_NO").distinct().count()
plac = jj.filter((F.col("lag") >= -180) & (F.col("lag") <= -90)).select("CLNT_NO").distinct().count()
print("[A2] placebo baseline (cohort =", coh, "):")
print("     POST-unsub 90d with a No/blank CPC write :", post, "(", round(100.0 * post / coh, 3), "% )")
print("     PRE-unsub  90d (placebo) with one        :", plac, "(", round(100.0 * plac / coh, 3), "% )")
print("     -> similar rates => the post-unsub writes are background activity, not triggered by the unsubscribe")

# %% [A3] RT3 Obj1/7 - the post-unsub writes by WRITER and DIRECTION. Opt-ins (5001) were excluded at extract - never counted.
j0 = (landing.join(first_unsub, "CLNT_NO").withColumn("lag", F.datediff("CHG_TMSTMP", "u_tm"))
      .filter((F.col("lag") >= 0) & (F.col("lag") <= 90))
      .withColumn("dir", F.when(F.col("CLNT_CONSENT_TYP") == 5002, "No (5002)").otherwise("blank (5003/null)")))
print("[A3] post-unsub writes by writer (APP_SYS_CD) x direction - is any ESP/automated writer present?:")
j0.groupBy("APP_SYS_CD", "dir").agg(F.countDistinct("CLNT_NO").alias("clients")).orderBy(F.desc("clients")).show(25, False)

# %% [A4] RT3 Obj3/9 - THREE-WAY standing (Yes / explicit-No / blank) x got-email, for 1002, 1012, 1014.
for pref in [1002, 1012, 1014]:
    d = standing3(pref, "DATE '2026-04-01'").join(got, "CLNT_NO", "left").fillna({"got": 0})
    print("[A4] switch", pref, "- got-email by three-way standing (blank shown separately, NOT folded into No):")
    (d.groupBy("st").agg(F.countDistinct("CLNT_NO").alias("clients"), F.sum("got").alias("got_email"))
       .withColumn("rate_pct", F.round(100.0 * F.col("got_email") / F.col("clients"), 1)).orderBy("st").show())

# %% [A5] RT3 Obj4 - 1002=No leak by RECENCY of the No flag x got-email (a just-set No = latency; an old No = a real miss).
no1002 = (standing3(1002, "DATE '2026-04-01'").filter("st = 'No'")
          .withColumn("age_days", F.datediff(F.lit("2026-04-01").cast("date"), F.col("CHG_TMSTMP")))
          .withColumn("recency", F.when(F.col("age_days") <= 180, "a_<=6mo").when(F.col("age_days") <= 365, "b_6-12mo")
                      .when(F.col("age_days") <= 730, "c_1-2yr").otherwise("d_2yr+")))
d5 = no1002.join(got, "CLNT_NO", "left").fillna({"got": 0})
print("[A5] 1002=No: got-email by how recently the No was set:")
(d5.groupBy("recency").agg(F.countDistinct("CLNT_NO").alias("clients"), F.sum("got").alias("got_email"))
   .withColumn("rate_pct", F.round(100.0 * F.col("got_email") / F.col("clients"), 1)).orderBy("recency").show())

# %% [A6] RT3 Obj4 - CHURN. Of clients No on 1002 at Apr 1, how many flipped to Yes during Apr-Jun (would look like a leak but isn't).
apr_no = standing3(1002, "DATE '2026-04-01'").filter("st = 'No'").select("CLNT_NO")
to_yes = (cpc.filter("PREF_ID = 1002 AND CLNT_CONSENT_TYP = 5001 "
                     "AND CHG_TMSTMP >= DATE '2026-04-01' AND CHG_TMSTMP < DATE '2026-07-01'")
          .select("CLNT_NO").distinct())
n_no = apr_no.count(); n_churn = apr_no.join(to_yes, "CLNT_NO").count()
print("[A6] churn: of", n_no, "clients No on 1002 at Apr 1,", n_churn, "flipped to Yes during Apr-Jun",
      "(", round(100.0 * n_churn / n_no, 2), "% )")

# %% [A7] RT3 Obj6 - re-cut the 0.3% on unsubs with a FULL 90-day runway only (removes right-censoring bias).
full = first_unsub.filter("u_tm < DATE '2026-04-15'")
nfull = full.count()
land_full = (landing.join(full, "CLNT_NO").withColumn("lag", F.datediff("CHG_TMSTMP", "u_tm"))
             .filter((F.col("lag") >= 0) & (F.col("lag") <= 90)).select("CLNT_NO").distinct().count())
print("[A7] full-runway re-cut (unsub before 2026-04-15): cohort", nfull,
      "| with a post-unsub CPC write", land_full, "(", round(100.0 * land_full / nfull, 3), "% )")

# %% [A8] RT3 Obj5 - email->client match: distinct unsub email-ids vs distinct clients (is the join ~1:1?).
try:
    md = spark.read.parquet(BASE + "unsub_match_diag").collect()[0]
    n_clients = ub.select("CLNT_NO").distinct().count()
    print("[A8] unsub email-ids:", md["unsub_email_ids"], "| unsub events:", md["unsub_events"],
          "| distinct clients (matched):", n_clients,
          "| email-ids per client:", round(md["unsub_email_ids"] / n_clients, 3))
except Exception as ex:
    print("[A8] run cpc_reservoir_extract.py [17] first. (", ex, ")")

# %% [A9] RT3 Obj4 - program mix of the do-not-solicit (1002) clients who received sends (already landed in gate_mne_agg).
try:
    gm = spark.read.parquet(BASE + "gate_mne_agg")
    print("[A9] 1002=No clients' Apr-Jun sends by campaign (mnemonic):")
    gm.filter("PREF_ID = 1002").orderBy(F.desc("clients")).show(15, False)
except Exception as ex:
    print("[A9] gate_mne_agg not found (", ex, ")")

# %% [SUMMARY]
print("=" * 78)
print("RT3 AUDIT OUTPUTS | A1 day-lag  A2 placebo baseline  A3 writer/direction of the 889")
print("A4 three-way Yes/No/blank x email (1002/1012/1014)  A5 recency-of-No  A6 churn")
print("A7 full-runway re-cut  A8 email->client match  A9 program mix")
print("Each answers a Red Team 3 objection; feeds the next deck build.")
print("=" * 78)
