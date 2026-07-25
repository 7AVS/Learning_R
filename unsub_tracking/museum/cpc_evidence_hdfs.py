# EVIDENCE - runs 100% off the HDFS reservoir (parquet landed by cpc_reservoir_extract.py). NO Teradata connection needed.
# E1-E8 = the deck evidence (validated vs SQL pack 2026-07-24). R1-R3 = red-team closers (objections #3, #12, #6).

# %% [0] Load reservoir + derive (pure Spark from here)
from pyspark.sql import functions as F, Window as W
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

BASE = "hdfs:///user/427966379/unsub_cpc/"
ub  = spark.read.parquet(BASE + "unsub_base/*")
rec = spark.read.parquet(BASE + "q2_recipients/*").distinct()
cpc = spark.read.option("recursiveFileLookup", "true").parquet(BASE + "cpc_pref")
ps  = spark.read.parquet(BASE + "postunsub_sends")
gm  = spark.read.parquet(BASE + "gate_mne_agg")

w = W.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").asc(), F.col("TREATMENT_ID").asc())
uf = (ub.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
        .select("CLNT_NO", "unsub_tm", F.expr("substring(TREATMENT_ID, 8, 3)").alias("unsub_mne")))
uf.cache(); print("unsub_first clients:", uf.count())

def cpc_standing(as_of_expr):
    ww = W.partitionBy("CLNT_NO", "PREF_ID").orderBy(F.col("CHG_TMSTMP").desc())
    base = cpc.filter("CHG_TMSTMP < " + as_of_expr) if as_of_expr else cpc
    return base.withColumn("rn", F.row_number().over(ww)).filter("rn = 1")

uf.createOrReplaceTempView("uf"); cpc.createOrReplaceTempView("cpc")

# %% [1] EVIDENCE 1 - two consent worlds, monthly volumes (35x claim; run confirmed 35.3x: 319,733 vs 9,052)
spark.sql("""
SELECT 'email_unsub' AS consent_world, date_format(unsub_tm, 'yyyyMM') AS month_yyyymm, COUNT(*) AS clients
FROM uf GROUP BY 2
UNION ALL
SELECT 'cpc_optout', date_format(CHG_TMSTMP, 'yyyyMM'), COUNT(DISTINCT CLNT_NO)
FROM cpc
WHERE CLNT_CONSENT_TYP = 5002 AND PREF_ID IN (1002, 1012, 1014)
  AND CHG_TMSTMP >= DATE '2025-07-01' AND CHG_TMSTMP < DATE '2026-07-01'
GROUP BY 2
ORDER BY 1, 2
""").show(30, False)

# %% [2] EVIDENCE 2 - the blind gate + before/after split (run confirmed: 1,387 = 1,252 + 135; 99.57% blind)
st = cpc_standing(None).filter("CLNT_CONSENT_TYP = 5002 AND PREF_ID IN (1002, 1012, 1014)")
earliest = st.groupBy("CLNT_NO").agg(F.min("CHG_TMSTMP").alias("first_optout_tm"))
j = uf.join(earliest, "CLNT_NO", "left")
total   = j.count()
with_ex = j.filter("first_optout_tm IS NOT NULL").count()
before  = j.filter("first_optout_tm <  unsub_tm").count()
after   = j.filter("first_optout_tm >= unsub_tm").count()
print("unsub_clients_total          ", total)
print("with_explicit_cpc_optout     ", with_ex)
print("without_explicit_cpc_optout  ", total - with_ex)
print("optout_recorded_before_unsub ", before)
print("optout_recorded_after_unsub  ", after)
assert before + after == with_ex

# %% [3] EVIDENCE 3 - no bridge: flips by writer x had-prior-unsub
spark.sql("""
WITH flips AS (
  SELECT CLNT_NO, PREF_ID, APP_SYS_CD, CHG_TMSTMP FROM cpc
  WHERE CLNT_CONSENT_TYP = 5002 AND PREF_ID IN (1002, 1012, 1014)
    AND CHG_TMSTMP >= DATE '2025-07-01' AND CHG_TMSTMP < DATE '2026-07-01')
SELECT f.PREF_ID, f.APP_SYS_CD,
       CASE WHEN u.CLNT_NO IS NOT NULL AND u.unsub_tm < f.CHG_TMSTMP THEN 'Y' ELSE 'N' END AS had_prior_unsub,
       COUNT(*) AS flips
FROM flips f LEFT JOIN uf u ON u.CLNT_NO = f.CLNT_NO
GROUP BY 1, 2, 3 ORDER BY 1, 4 DESC
""").show(40, False)

# %% [4] EVIDENCE 4 - the leaking gate, per switch x exclusivity (run confirmed: 1012-only 47.1%; 1002 total 19.2%)
gates = cpc_standing("DATE '2026-04-01'").filter("CLNT_CONSENT_TYP = 5002 AND PREF_ID IN (1002, 1012, 1014)") \
        .select("CLNT_NO", "PREF_ID")
nflags = gates.groupBy("CLNT_NO").agg(F.count("*").alias("n"))
gates = gates.join(nflags, "CLNT_NO").withColumn("exclusivity", F.when(F.col("n") == 1, "only_this_flag").otherwise("multi_flag"))
got = rec.withColumn("got", F.lit(1))
(gates.join(got, "CLNT_NO", "left")
      .groupBy("PREF_ID", "exclusivity")
      .agg(F.countDistinct("CLNT_NO").alias("optout_clients"), F.sum("got").alias("got_email_apr_jun"))
      .orderBy("PREF_ID", "exclusivity")).show(10, False)
allsw = gates.select("CLNT_NO").distinct().join(got, "CLNT_NO", "left")
print("ALL_SWITCHES:", allsw.count(), "optout clients,", allsw.filter("got = 1").count(), "got email Apr-Jun")

# %% [5] EVIDENCE 5 - does the channel honor unsubs? (run measured 50.5% got ANY email - E8 splits it: 46% cross-program by design, 3.9% in-program)
pre = uf.filter("unsub_tm < DATE '2026-04-01'"); pre.cache()
pre_n = pre.count()
print("unsub_before_apr_clients ", pre_n)
print("got_email_apr_jun        ", pre.join(got, "CLNT_NO", "inner").count())

# %% [6] EVIDENCE 6+7 - which campaigns reach opted-out clients, per switch (from server-side aggregate)
we7 = W.partitionBy("PREF_ID").orderBy(F.col("clients").desc())
gm.withColumn("rk", F.row_number().over(we7)).filter("rk <= 12").orderBy("PREF_ID", "rk").show(48, False)

# %% [7] EVIDENCE 8 - the post-unsub waterfall (exclusion order fixed; labels match SQL version)
print("0 unsubscribed before Apr 2026 (cohort)                 ", pre_n)
r1 = ps.filter("disposition_cd = 1")
print("1 gross: received any send Apr-Jun                      ", r1.select("CLNT_NO").distinct().count())
r2 = r1.filter("disposition_dt_tm >= unsub_tm + INTERVAL 14 DAYS")
print("2 excl. sends within 14 days of unsub (CASL proxy)      ", r2.select("CLNT_NO").distinct().count())
bounced = ps.filter("disposition_cd = 5").select("CLNT_NO", "mne").distinct().withColumn("b", F.lit(1))
r3 = r2.join(bounced, ["CLNT_NO", "mne"], "left").filter("b IS NULL")
print("3 excl. clients whose every send hardbounced (mne proxy)", r3.select("CLNT_NO").distinct().count())
rec5001 = (cpc.filter("CLNT_CONSENT_TYP = 5001 AND PREF_ID IN (1002, 1012)")
              .select("CLNT_NO", "CHG_TMSTMP")
              .join(pre.select("CLNT_NO", "unsub_tm"), "CLNT_NO")
              .filter("CHG_TMSTMP > unsub_tm AND CHG_TMSTMP < DATE '2026-07-01'")
              .select("CLNT_NO").distinct().withColumn("rc", F.lit(1)))
r4 = r3.join(rec5001, "CLNT_NO", "left").filter("rc IS NULL")
print("4 excl. CPC-side re-consents after unsub                ", r4.select("CLNT_NO").distinct().count())
same = r4.filter("mne = unsub_mne").select("CLNT_NO").distinct()
cross = r4.select("CLNT_NO").distinct().subtract(same)
print("5 residual: cross-campaign only (different mne)         ", cross.count())
print("6 residual: same campaign as unsubbed (in-program leak) ", same.count())

# %% [8] RED-TEAM R1 - the blank-MNE bucket (objection #3's last hole: is it marketing or service mail?)
# Size it from the cohort sends, then identify it from the landed sample (subject lines settle it).
blank = ps.filter(F.trim(F.col("mne")) == "")
print("blank-mne rows (cohort sends Apr-Jun):", blank.count(), "| clients:", blank.select("CLNT_NO").distinct().count())
blank.groupBy("disposition_cd").count().orderBy("disposition_cd").show()
gm.filter(F.trim(F.col("mne")) == "").orderBy("PREF_ID").show(10, False)
try:
    bs = spark.read.parquet(BASE + "blank_mne_sample")
    print("top blank-mne treatments by send volume (subject line = marketing vs service verdict):")
    bs.orderBy(F.col("send_rows").desc()).show(30, False)
except Exception:
    print("blank_mne_sample NOT landed - run extract [13] in cpc_reservoir_extract.py first")

# %% [9] RED-TEAM R2 - opt-out recency distribution (objection #12: live breach vs stale-state noise)
g2 = cpc_standing("DATE '2026-04-01'").filter("CLNT_CONSENT_TYP = 5002 AND PREF_ID IN (1002, 1012, 1014, 1006)")
g2 = g2.withColumn("age_mo", F.months_between(F.to_date(F.lit("2026-04-01")), F.col("CHG_TMSTMP")))
g2 = g2.withColumn("age_bucket",
     F.when(F.col("age_mo") < 6, "a_0-6mo").when(F.col("age_mo") < 12, "b_6-12mo")
      .when(F.col("age_mo") < 24, "c_1-2yr").when(F.col("age_mo") < 60, "d_2-5yr").otherwise("e_5yr+"))
g2.groupBy("PREF_ID").pivot("age_bucket").agg(F.count("*")).orderBy("PREF_ID").show(10, False)

# %% [10] RED-TEAM R3 - the post-unsub opt-outs: pipe or coincidence? (objection #6) A pipe clusters 0-7d; coincidence smears
lag = (j.filter("first_optout_tm >= unsub_tm")
        .withColumn("days", F.datediff("first_optout_tm", "unsub_tm"))
        .withColumn("lag_bucket",
            F.when(F.col("days") <= 1, "a_0-1d").when(F.col("days") <= 7, "b_2-7d")
             .when(F.col("days") <= 30, "c_8-30d").when(F.col("days") <= 90, "d_31-90d").otherwise("e_90d+")))
lag.groupBy("lag_bucket").agg(F.count("*").alias("clients")).orderBy("lag_bucket").show(10, False)

# %% [11] ONE-SCREEN SUMMARY - every headline number in one compact block. Zoom the font, photograph THIS cell only, straight-on.
opt12 = (cpc.filter("CLNT_CONSENT_TYP = 5002 AND PREF_ID IN (1002, 1012, 1014)")
            .filter("CHG_TMSTMP >= DATE '2025-07-01' AND CHG_TMSTMP < DATE '2026-07-01'")
            .select("CLNT_NO").distinct().count())
g_all = allsw.count(); g_got = allsw.filter("got = 1").count()
g1002 = gates.filter("PREF_ID = 1002").select("CLNT_NO").distinct().join(got, "CLNT_NO", "left")
n1002 = g1002.count(); y1002 = g1002.filter("got = 1").count()
gross = r1.select("CLNT_NO").distinct().count()
neat  = r4.select("CLNT_NO").distinct().count()
print("=" * 62)
print("CPC CONSENT - ONE SCREEN (windows: unsubs Jul25-Jun26; sends Apr-Jun26)")
print("E1  unsubs 12mo %d  | cpc optouts 12mo %d" % (total, opt12))
print("E2  crossover %d = before %d + after %d  | blind %d" % (with_ex, before, after, total - with_ex))
print("E4  optout std %d got mail %d  | 1002: %d of %d" % (g_all, g_got, y1002, n1002))
print("E5  cohort %d  gross recipients %d" % (pre_n, gross))
print("E8  after exclusions %d = cross %d + same-mne %d" % (neat, cross.count(), same.count()))
print("R1  blank-mne rows %d clients %d (identity: see sample above)" % (blank.count(), blank.select("CLNT_NO").distinct().count()))
print("R3  flip lag 0-1/2-7/8-30/31-90/90+ d: see cell [10] - smear = no pipe")
print("=" * 62)

# %% [12] R4 - full-history flip map: which switch moves most, which direction, which writer, when (pure Spark, no extract)
r4h = (cpc.withColumn("yr", F.year("CHG_TMSTMP"))
          .withColumn("direction", F.when(F.col("CLNT_CONSENT_TYP") == 5002, "No")
                                    .when(F.col("CLNT_CONSENT_TYP") == 5001, "Yes").otherwise("other"))
          .groupBy("PREF_ID", "APP_SYS_CD", "direction", "yr")
          .agg(F.count("*").alias("flips")))
print("top 25 switch x writer x direction x year by volume:")
r4h.orderBy(F.col("flips").desc()).show(25, False)
print("per switch x direction totals (full history):")
r4h.groupBy("PREF_ID", "direction").agg(F.sum("flips").alias("flips")).orderBy("PREF_ID", "direction").show(20, False)
print("per writer totals (full history, all switches):")
r4h.groupBy("APP_SYS_CD").agg(F.sum("flips").alias("flips")).orderBy(F.col("flips").desc()).show(20, False)

# %% [13] E9 - NAMED-campaign-only recut (red-team #3 carve-out: TREATMENT_ID='DEFAULT' stream excluded from headline claims)
# Requires q2_recipients_named landed by extract [14]. Blank-MNE verdict 2026-07-25: DEFAULT = service + broken-template + untagged marketing.
recn = spark.read.parquet(BASE + "q2_recipients_named/*").distinct().withColumn("gotn", F.lit(1))
alln = gates.select("CLNT_NO").distinct().join(recn, "CLNT_NO", "left")
print("E4 recut  ALL_SWITCHES:", alln.count(), "optout clients,", alln.filter("gotn = 1").count(), "got NAMED-campaign mail Apr-Jun")
g1002n = gates.filter("PREF_ID = 1002").select("CLNT_NO").distinct().join(recn, "CLNT_NO", "left")
print("E4 recut  1002:", g1002n.filter("gotn = 1").count(), "of", g1002n.count())
print("E5 recut  cohort", pre_n, "got NAMED-campaign mail:", pre.join(recn.select("CLNT_NO"), "CLNT_NO", "inner").count())
psn = ps.filter("disposition_cd = 1 AND trim(mne) != ''")
print("E8 recut  gross named-only:", psn.select("CLNT_NO").distinct().count())
print("E8 recut  same-mne, blank-blank pairs excluded:", r4.filter("mne = unsub_mne AND trim(mne) != ''").select("CLNT_NO").distinct().count())
