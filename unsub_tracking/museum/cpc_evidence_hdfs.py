# EVIDENCE - runs 100% off the HDFS reservoir (parquet landed by cpc_reservoir_extract.py). NO Teradata connection needed.
# E1-E11 = deck evidence. R1-R4 = red-team closers. Every output is LABELED with the rule it uses.
#
# ============================ THE BLANK RULE (EDW dictionary, read 2026-07-25) ============================
# CLNT_CONSENT_TYP: 5001 Yes / 5002 No / 5003 blank / 5004. Blank = "client never explicitly indicated yes or no".
# OFFICIAL RULE: blank is treated as YES for all preferences EXCEPT Share-for-Services (1015) and
# Share-for-Marketing (1014) across RBC, where blank MUST be treated as NO.
# PREF_ID taxonomy (dictionary): 1002 = RBC Royal Bank entity DO-NOT-SOLICIT (email-relevant, blank=Yes);
# 1012 = banking email channel consent (email-relevant, blank=Yes); 1014 = CROSS-ENTITY sharing gate
# (NOT a same-entity email gate; blank=No); 1006 = group unconfirmed (verify on dictionary page).
# LABELS: [causation] = did the unsub CAUSE a CPC record (explicit events only - blanks are feed defaults, not
# client responses). [protection] = what the client's standing state means UNDER THE RULE. [explicit-No] = counts
# of clients/events with CLNT_CONSENT_TYP = 5002 only - exact legal boundary on blank=Yes switches, floor on 1014/1015.
# ==========================================================================================================

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

# %% [1] E1 - two consent worlds, monthly volumes
print("[E1 | explicit-No CLIENT DECISIONS per month - 5003 blank feed events are NOT decisions and are excluded]")
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

# %% [2] E2 - the blind gate + before/after split
print("[E2 | causation, EXPLICIT RECORDS ONLY - answers 'did an explicit CPC opt-out ever follow the unsub'.")
print("      Slide language must say 'no EXPLICIT record', never 'no protection' - protection is E2b.]")
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

# %% [3] E2b - rule-based standing of the unsubscribers, per switch
print("[E2b | protection UNDER THE DICTIONARY RULE - blank=NO on 1014/1015, blank=YES elsewhere.")
print("       Pairs with E2 on the slide: E2 = no pipe exists; E2b = what the rule makes of their standing state.]")
for _p in [1002, 1006, 1012, 1014]:
    stp = cpc_standing(None).filter("PREF_ID = " + str(_p)).select("CLNT_NO", "CLNT_CONSENT_TYP")
    jj = uf.join(stp, "CLNT_NO", "left")
    n_no    = jj.filter("CLNT_CONSENT_TYP = 5002").count()
    n_yes   = jj.filter("CLNT_CONSENT_TYP = 5001").count()
    n_blank = jj.filter("CLNT_CONSENT_TYP IS NOT NULL AND CLNT_CONSENT_TYP NOT IN (5001, 5002)").count()
    n_norow = jj.filter("CLNT_CONSENT_TYP IS NULL").count()
    rule = "blank=NO (share switch)" if _p in (1014, 1015) else "blank=YES"
    print("%d [%s]  of %d unsubscribers: explicit_no %d | blank %d | explicit_yes %d | no_row %d"
          % (_p, rule, total, n_no, n_blank, n_yes, n_norow))
    if _p in (1014, 1015):
        print("      -> RULE-PROTECTED on %d (explicit_no + blank): %d" % (_p, n_no + n_blank))
    else:
        print("      -> rule reads as CONTACTABLE (yes + blank + no_row): %d" % (n_yes + n_blank + n_norow))

# %% [4] E3 - no bridge: flips by writer x had-prior-unsub
print("[E3 | explicit-No EVENTS by writer system - blanks are 7999 feed writes, not client flips (see diagnosis D1)]")
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

# %% [5] E4 - the leaking gate, per switch x exclusivity
print("[E4 | standing explicit-No as of 2026-04-01. 1002/1012 = email-relevant and EXACT under blank=Yes rule.")
print("      1014 rows = CONTEXT ONLY - dictionary says 1014 gates cross-entity sharing, NOT same-entity email.]")
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
print("ALL_SWITCHES (mixed-meaning union, context only):", allsw.count(), "optout clients,",
      allsw.filter("got = 1").count(), "got email Apr-Jun")

# %% [6] E5 - does the channel honor unsubs?
print("[E5 | vendor-side cohort, no CPC rule involved. Gross = ANY send incl. DEFAULT stream - E9 is the named-only recut]")
pre = uf.filter("unsub_tm < DATE '2026-04-01'"); pre.cache()
pre_n = pre.count()
print("unsub_before_apr_clients ", pre_n)
print("got_email_apr_jun        ", pre.join(got, "CLNT_NO", "inner").count())

# %% [7] E6+E7 - which campaigns reach opted-out clients, per switch
print("[E6/E7 | standing explicit-No gates x named campaign sends. Blank mne row = DEFAULT stream (see R1).")
print("        1002 = do-not-solicit breach candidates; 1014 = sharing-gate context only.]")
we7 = W.partitionBy("PREF_ID").orderBy(F.col("clients").desc())
gm.withColumn("rk", F.row_number().over(we7)).filter("rk <= 12").orderBy("PREF_ID", "rk").show(48, False)

# %% [8] E8 - the post-unsub waterfall
print("[E8 | vendor-side, mne-based. Same-mne line includes blank-blank pairs - E9 prints the cleaned version]")
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

# %% [9] R1 - the blank-MNE bucket (red-team #3)
print("[R1 | DEFAULT-stream identification. Verdict 2026-07-25: TREATMENT_ID='DEFAULT' = service + broken-template +")
print("      UNTAGGED MARKETING - outside campaign taxonomy, invisible to MNE-based suppression. Headline claims -> E9.]")
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

# %% [10] R2 - opt-out recency distribution (red-team #12)
print("[R2 | standing explicit-No as of 2026-04-01, age of the standing answer. Explicit cohort = exact on 1002/1006/1012]")
g2 = cpc_standing("DATE '2026-04-01'").filter("CLNT_CONSENT_TYP = 5002 AND PREF_ID IN (1002, 1012, 1014, 1006)")
g2 = g2.withColumn("age_mo", F.months_between(F.to_date(F.lit("2026-04-01")), F.col("CHG_TMSTMP")))
g2 = g2.withColumn("age_bucket",
     F.when(F.col("age_mo") < 6, "a_0-6mo").when(F.col("age_mo") < 12, "b_6-12mo")
      .when(F.col("age_mo") < 24, "c_1-2yr").when(F.col("age_mo") < 60, "d_2-5yr").otherwise("e_5yr+"))
g2.groupBy("PREF_ID").pivot("age_bucket").agg(F.count("*")).orderBy("PREF_ID").show(10, False)

# %% [11] R3 - the post-unsub explicit opt-outs: pipe or coincidence? (red-team #6)
print("[R3 | causation follow-up on E2's 135 EXPLICIT flips. A pipe clusters 0-7d; coincidence smears.")
print("      Blank-write variant of the same question = E11.]")
lag = (j.filter("first_optout_tm >= unsub_tm")
        .withColumn("days", F.datediff("first_optout_tm", "unsub_tm"))
        .withColumn("lag_bucket",
            F.when(F.col("days") <= 1, "a_0-1d").when(F.col("days") <= 7, "b_2-7d")
             .when(F.col("days") <= 30, "c_8-30d").when(F.col("days") <= 90, "d_31-90d").otherwise("e_90d+")))
lag.groupBy("lag_bucket").agg(F.count("*").alias("clients")).orderBy("lag_bucket").show(10, False)

# %% [12] ONE-SCREEN SUMMARY - zoom the font, photograph THIS cell only, straight-on
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
print("rules: counts = explicit-No unless labeled; blank=NO only on 1014/1015; blank=YES elsewhere")
print("E1  unsubs 12mo %d  | cpc explicit optouts 12mo %d (distinct clients)" % (total, opt12))
print("E2  explicit crossover %d = before %d + after %d  | no explicit record %d" % (with_ex, before, after, total - with_ex))
print("E4  optout std %d got mail %d  | 1002: %d of %d" % (g_all, g_got, y1002, n1002))
print("E5  cohort %d  gross recipients %d (any send incl DEFAULT stream)" % (pre_n, gross))
print("E8  after exclusions %d = cross %d + same-mne %d (raw; cleaned in E9)" % (neat, cross.count(), same.count()))
print("R1  blank-mne rows %d clients %d (DEFAULT stream - see sample)" % (blank.count(), blank.select("CLNT_NO").distinct().count()))
print("R3  flip lag: see cell [11] - smear = no pipe | E2b rule-based standing: see cell [3]")
print("=" * 62)

# %% [13] R4 - full-history flip map: which switch moves most, which direction, which writer, when
print("[R4 | ALL events full history. direction: No=5002, Yes=5001, blank=5003, other=5004 - blanks shown as what they are]")
r4h = (cpc.withColumn("yr", F.year("CHG_TMSTMP"))
          .withColumn("direction", F.when(F.col("CLNT_CONSENT_TYP") == 5002, "No")
                                    .when(F.col("CLNT_CONSENT_TYP") == 5001, "Yes")
                                    .when(F.col("CLNT_CONSENT_TYP") == 5003, "blank").otherwise("other_5004"))
          .groupBy("PREF_ID", "APP_SYS_CD", "direction", "yr")
          .agg(F.count("*").alias("flips")))
print("top 25 switch x writer x direction x year by volume:")
r4h.orderBy(F.col("flips").desc()).show(25, False)
print("per switch x direction totals (full history):")
r4h.groupBy("PREF_ID", "direction").agg(F.sum("flips").alias("flips")).orderBy("PREF_ID", "direction").show(20, False)
print("per writer totals (full history, all switches):")
r4h.groupBy("APP_SYS_CD").agg(F.sum("flips").alias("flips")).orderBy(F.col("flips").desc()).show(20, False)

# %% [14] E9 - NAMED-campaign-only recut (red-team #3 carve-out)
print("[E9 | headline claims recut on NAMED campaign mail only - DEFAULT/service stream excluded server-side.")
print("      Requires q2_recipients_named landed by extract [14]. THESE are the deck's citable leak numbers.]")
recn = spark.read.parquet(BASE + "q2_recipients_named/*").distinct().withColumn("gotn", F.lit(1))
alln = gates.select("CLNT_NO").distinct().join(recn, "CLNT_NO", "left")
print("E4 recut  ALL_SWITCHES:", alln.count(), "optout clients,", alln.filter("gotn = 1").count(), "got NAMED-campaign mail Apr-Jun")
g1002n = gates.filter("PREF_ID = 1002").select("CLNT_NO").distinct().join(recn, "CLNT_NO", "left")
print("E4 recut  1002:", g1002n.filter("gotn = 1").count(), "of", g1002n.count())
print("E5 recut  cohort", pre_n, "got NAMED-campaign mail:", pre.join(recn.select("CLNT_NO"), "CLNT_NO", "inner").count())
psn = ps.filter("disposition_cd = 1 AND trim(mne) != ''")
print("E8 recut  gross named-only:", psn.select("CLNT_NO").distinct().count())
print("E8 recut  same-mne, blank-blank pairs excluded:", r4.filter("mne = unsub_mne AND trim(mne) != ''").select("CLNT_NO").distinct().count())

# %% [15] E10 - where the unsubs come from: volume by source campaign mnemonic
print("[E10 | vendor-side, 12mo Jul25-Jun26. Category rollup lands here once Andre's mnemonic->category map arrives]")
u_mne = uf.withColumn("mne", F.when(F.trim(F.col("unsub_mne")) == "", F.lit("(DEFAULT/untagged)")).otherwise(F.col("unsub_mne")))
u_mne.groupBy("mne").agg(F.count("*").alias("unsub_clients")).orderBy(F.col("unsub_clients").desc()).show(30, False)

# %% [16] E11 - blank-write bridge test (Andre 2026-07-25)
print("[E11 | causation, BLANK variant: could a pipe record unsubs as 5003 rows? Only meaningful as honoring-the-no")
print("       on 1014/1015 (blank=No there). Pipe = 0-7d spike + one batch writer; background = smear.]")
bl_ev = cpc.filter("CLNT_CONSENT_TYP = 5003").select("CLNT_NO", "PREF_ID", "APP_SYS_CD", "CHG_TMSTMP")
jb = (uf.join(bl_ev, "CLNT_NO").filter("CHG_TMSTMP >= unsub_tm")
        .withColumn("days", F.datediff("CHG_TMSTMP", "unsub_tm")))
first_bl = jb.groupBy("CLNT_NO", "PREF_ID").agg(F.min("days").alias("days"))
print("unsub clients with a LATER blank event, per switch (any lag):")
first_bl.groupBy("PREF_ID").count().orderBy("PREF_ID").show(10, False)
lagb = first_bl.withColumn("lag_bucket",
       F.when(F.col("days") <= 7, "a_0-7d").when(F.col("days") <= 30, "b_8-30d")
        .when(F.col("days") <= 90, "c_31-90d").otherwise("d_90d+"))
print("lag from unsub to FIRST blank event (a pipe spikes a_0-7d):")
lagb.groupBy("PREF_ID", "lag_bucket").count().orderBy("PREF_ID", "lag_bucket").show(20, False)
print("writer of blank events within 30d of an unsub:")
jb.filter("days <= 30").groupBy("PREF_ID", "APP_SYS_CD").count().orderBy(F.col("count").desc()).show(10, False)
