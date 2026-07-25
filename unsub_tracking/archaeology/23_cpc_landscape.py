# CPC LANDSCAPE (archaeology) - runs 100% off the HDFS reservoir. NO Teradata.
# T1 = where do post-unsub clients land in CPC across ALL switches (blind-spot test)
# T2 = multi-switch journeys (can a client be No on one switch, Yes on another; is 1014 an umbrella)
# T3 = does 1014=No actually mean less email, or is it sharing-only
# PREREQ: T1 needs cpc_reservoir_extract.py cell [15] run once (lands cpc_landing_allsw). T2/T3 need nothing new.

# %% [load] reservoir + helpers (shared by all cells below)
from pyspark.sql import functions as F, Window as W
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
BASE = "hdfs:///user/427966379/unsub_cpc/"

ub  = spark.read.parquet(BASE + "unsub_base/*")
cpc = spark.read.option("recursiveFileLookup", "true").parquet(BASE + "cpc_pref")
rec = spark.read.parquet(BASE + "q2_recipients/*").distinct()

SWITCHES = [1002, 1006, 1012, 1014]              # what the reservoir holds
print("[load] unsub clients:", ub.select("CLNT_NO").distinct().count(),
      "| cpc rows:", cpc.count(), "| Apr-Jun recipients:", rec.count(),
      "| NOTE 1015 (the other blank=No share switch) is NOT in the reservoir")

def cpc_standing(as_of):
    # latest row per (CLNT_NO, PREF_ID); as_of = "DATE '2026-04-01'" or None for all-time
    w = W.partitionBy("CLNT_NO", "PREF_ID").orderBy(F.col("CHG_TMSTMP").desc())
    b = cpc.filter("CHG_TMSTMP < " + as_of) if as_of else cpc
    return b.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

def is_no(pref_col, consent_col):
    # dictionary rule: on 1014/1015 blank counts as No; everywhere else blank = Yes
    blank = consent_col.isNull() | (~consent_col.isin(5001, 5002, 5004))
    return F.when(pref_col.isin(1014, 1015), (consent_col == 5002) | blank).otherwise(consent_col == 5002)

std = (cpc_standing("DATE '2026-04-01'").filter(F.col("PREF_ID").isin(SWITCHES))
       .withColumn("st", F.when(is_no(F.col("PREF_ID"), F.col("CLNT_CONSENT_TYP")), "No").otherwise("Yes")))

def state_on(pref):
    return std.filter(F.col("PREF_ID") == pref).select("CLNT_NO", F.col("st").alias("st_" + str(pref)))

# %% [T1a] LANDING MAP - after a vendor unsub, where in CPC does the client show up (ALL switches, 0-90d, No/blank writes)
try:
    landing = spark.read.option("recursiveFileLookup", "true").parquet(BASE + "cpc_landing_allsw")
    fu = ub.groupBy("CLNT_NO").agg(F.min("unsub_tm").alias("first_unsub_tm"))
    cohort_n = fu.count()
    j = (landing.join(fu, "CLNT_NO")
         .withColumn("lag_days", F.datediff(F.col("CHG_TMSTMP"), F.col("first_unsub_tm")))
         .filter((F.col("lag_days") >= 0) & (F.col("lag_days") <= 90)))
    landed_clients = j.select("CLNT_NO").distinct().count()
    print("[T1a] cohort:", cohort_n, "| clients with ANY CPC No/blank event 0-90d after unsub:",
          landed_clients, "(", round(100.0 * landed_clients / cohort_n, 3), "% )")
    nonw = j.filter(~F.col("PREF_ID").isin(SWITCHES))
    print("[T1a] BLIND-SPOT TEST - landings on switches we do NOT watch:",
          nonw.select("CLNT_NO").distinct().count(), "clients across",
          nonw.select("PREF_ID").distinct().count(), "unwatched switches")
    print("[T1a] ranked landing: PREF_ID x consent x writer (distinct clients) - a real pipe = one switch dominating")
    (j.groupBy("PREF_ID", "CLNT_CONSENT_TYP", "APP_SYS_CD")
       .agg(F.countDistinct("CLNT_NO").alias("clients"))
       .orderBy(F.desc("clients")).show(40, False))
except Exception as ex:
    print("[T1a] cpc_landing_allsw not found -> run cpc_reservoir_extract.py cell [15] first. (", ex, ")")

# %% [T1b] CLUSTER TEST - a real pipe spikes 0-7d after the unsub; coincidence smears across 90d
try:
    landing = spark.read.option("recursiveFileLookup", "true").parquet(BASE + "cpc_landing_allsw")
    fu = ub.groupBy("CLNT_NO").agg(F.min("unsub_tm").alias("first_unsub_tm"))
    j = (landing.join(fu, "CLNT_NO")
         .withColumn("lag_days", F.datediff(F.col("CHG_TMSTMP"), F.col("first_unsub_tm")))
         .filter((F.col("lag_days") >= 0) & (F.col("lag_days") <= 90))
         .withColumn("bucket", F.when(F.col("lag_days") <= 1, "a_0-1d")
                     .when(F.col("lag_days") <= 7, "b_2-7d")
                     .when(F.col("lag_days") <= 30, "c_8-30d").otherwise("d_31-90d")))
    print("[T1b] lag from unsub to CPC event, per switch (tight 0-7d cluster on one switch = candidate pipe; even smear = coincidence):")
    (j.groupBy("PREF_ID", "bucket").agg(F.countDistinct("CLNT_NO").alias("clients"))
       .orderBy("PREF_ID", "bucket").show(60, False))
except Exception as ex:
    print("[T1b] needs cpc_landing_allsw -> run extract cell [15] first. (", ex, ")")

# %% [T2a] MULTI-SWITCH STANDING - can a client be No on one switch and Yes/absent on another (standing as of 2026-04-01)
c1002 = state_on(1002)
c1014 = state_on(1014)
allc = std.select("CLNT_NO").distinct()
x = (allc.join(c1002, "CLNT_NO", "left").join(c1014, "CLNT_NO", "left")
        .fillna("absent", subset=["st_1002", "st_1014"]))
print("[T2a] clients with any standing on", SWITCHES, ":", allc.count())
print("[T2a] 1002 (do-not-solicit) x 1014 (sharing) cross-tab - distinct client counts:")
x.groupBy("st_1002", "st_1014").count().orderBy(F.desc("count")).show()

# %% [T2b] UMBRELLA - is 1014=No basically a superset of 1002=No, or do they cross?
n1002 = c1002.filter("st_1002 = 'No'").select("CLNT_NO")
n1014 = c1014.filter("st_1014 = 'No'").select("CLNT_NO")
a = n1002.count()
b = n1014.count()
both = n1002.join(n1014, "CLNT_NO").count()
print("[T2b] No on 1002:", a, "| No on 1014:", b, "| No on BOTH:", both)
print("[T2b] of 1002-No, share ALSO No on 1014:", both, "/", a, "=", round(100.0 * both / a, 1) if a else 0, "%")
print("[T2b] of 1014-No, share ALSO No on 1002:", both, "/", b, "=", round(100.0 * both / b, 1) if b else 0, "%")
print("[T2b] read: high first %/low second % => 1014 is the broad umbrella; both low => they cross, not nested")

# %% [T2c] SEQUENCE - for clients with an EXPLICIT No on both 1002 and 1014, which came first (descriptive, not causal)
fno = (cpc.filter("CLNT_CONSENT_TYP = 5002").filter(F.col("PREF_ID").isin(1002, 1014))
          .groupBy("CLNT_NO", "PREF_ID").agg(F.min("CHG_TMSTMP").alias("first_no")))
p = (fno.groupBy("CLNT_NO").pivot("PREF_ID", [1002, 1014]).agg(F.min("first_no"))
        .withColumnRenamed("1002", "t1002").withColumnRenamed("1014", "t1014")
        .filter("t1002 is not null and t1014 is not null")
        .withColumn("order", F.when(F.col("t1002") < F.col("t1014"), "1002_first")
                    .when(F.col("t1014") < F.col("t1002"), "1014_first").otherwise("same_day")))
print("[T2c] among clients with explicit-No on BOTH 1002 and 1014, which No is dated earlier:")
p.groupBy("order").count().orderBy(F.desc("count")).show()
print("[T2c] note: explicit-No only - blanks are onboarding stamps, not decisions with a meaningful event time")

# %% [T3] SEMANTICS - does 1014=No actually associate with getting LESS email? (1002 = email-switch contrast)
got = rec.select("CLNT_NO").distinct().withColumn("got_email", F.lit(1))
def receipt_by(pref):
    base = cpc.filter(F.col("PREF_ID") == pref).select("CLNT_NO").distinct()   # anyone with a standing on this switch
    d = (base.join(state_on(pref), "CLNT_NO", "left").join(got, "CLNT_NO", "left")
             .fillna({"got_email": 0}).fillna("absent", subset=["st_" + str(pref)]))
    print("[T3] switch", pref, "- clients by standing and whether they got ANY Apr-Jun send:")
    (d.groupBy("st_" + str(pref)).agg(F.count("*").alias("clients"), F.sum("got_email").alias("got_email"))
       .orderBy("st_" + str(pref)).show())
receipt_by(1014)
receipt_by(1002)
print("[T3] read: if 1014=No suppresses email its got_email ~ 0; if 1014 is sharing-only, No clients still receive ~ like Yes")
print("[T3] 1002=No is the email-gate benchmark - the contrast decides whether the 1014-umbrella logic is valid")

# %% [SUMMARY] one screen
print("=" * 72)
print("CPC LANDSCAPE (archaeology) | reservoir switches 1002/1006/1012/1014 | 1015 not landed")
print("T1a landing map + T1b cluster : blind-spot = any UNwatched switch with a 0-7d cluster after unsub")
print("T2a crosstab | T2b umbrella containment | T2c which No came first")
print("T3 semantics : 1014=No got-email rate vs 1002=No -> does 1014 gate email or only sharing")
print("all counts are distinct clients unless labelled rows; rates shown with numerator + denominator")
print("=" * 72)
