# cpc_evidence_spark.py - PySpark reproduction of museum/cpc_evidence.sql (E1-E8 + SUMMARY)
# Architecture: extract each Teradata bite to HDFS parquet ONCE, then every Evidence
# cell runs pure Spark against the landed parquet - no repeat Teradata scans, ever.
# Resume after failure: re-run ONLY the failed cell - Phase 1 cells SKIP instantly if
# already landed; Phase 2/3 cells are pure Spark and safe to re-run from the top of a
# fresh session (Phase 1 will skip in seconds since the data is already on HDFS).
# Env: Lumina/AI Farm YARN-Spark, `spark` is pre-initialized (no builder/stop needed).
# Teradata access is via JDBC (td_read below) - connection details are PLACEHOLDERS, edit before running.

from pyspark.sql import functions as F, Window

BASE = "hdfs:///user/427966379/unsub_cpc/"

# big joins in this file (postunsub_sends x cohort, gate_cohorts x q2_send_detail_all)
# can be large - disable auto-broadcast per house rule, do not re-enable per-cell
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


def path_exists(path):
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(jpath)


# -- swap in your environment's standard Teradata connection here (you said the
# connector exists; paste its url/driver) - everything below is a PLACEHOLDER
TD_JDBC_URL = "jdbc:teradata://EDW_HOST_PLACEHOLDER/DATABASE=DBC"
TD_USER = "YOUR_USER_PLACEHOLDER"
TD_PASSWORD = "YOUR_PASSWORD_PLACEHOLDER"
TD_DRIVER = "com.teradata.jdbc.TeraDriver"  # default guess - confirm against your working driver class


def td_read(sql):
    return (spark.read.format("jdbc")
            .option("url", TD_JDBC_URL)
            .option("driver", TD_DRIVER)
            .option("user", TD_USER)
            .option("password", TD_PASSWORD)
            .option("query", sql)
            .load())


def land(path, sql, desc):
    # if already landed: SKIP (idempotent). Else: pull via JDBC, write parquet, assert non-empty.
    # each bite still runs under Teradata's governor once it fires - but it only ever
    # fires ONCE; a killed bite is re-run by re-executing this cell alone, nothing
    # already landed is touched.
    if path_exists(path):
        n = spark.read.parquet(path).count()
        print(f"SKIP {desc}: already landed at {path} ({n:,} rows)")
        return
    df = td_read(sql)
    df.write.mode("overwrite").parquet(path)
    n = spark.read.parquet(path).count()
    assert n > 0, f"{desc}: landed 0 rows from Teradata - check the bite SQL/window before proceeding"
    print(f"{desc}: landed {n:,} rows -> {path}")


print(f"Config loaded. BASE={BASE}")


# %% [1] EXTRACT ext_cpc_pref - CPC_RB_PREF_LOG full history, 5 switches (no floor - small table)
SQL_CPC_PREF = """
SELECT
    CLNT_NO,
    PREF_ID,
    CLNT_CONSENT_TYP,
    CHG_TMSTMP,
    APP_SYS_CD
FROM DDWV01.CPC_RB_PREF_LOG
WHERE PREF_ID IN (1002, 1012, 1014, 1006, 1007)
"""
land(BASE + "cpc_pref_slice", SQL_CPC_PREF, "ext_cpc_pref")


# %% [2] EXTRACT ext_unsub_base chunk1 - EVENT disp=4 Jul2025-Jun2026 x MASTER load_tm Jun-Oct 2025
SQL_UNSUB_BASE_1 = """
SELECT DISTINCT
    m.CLNT_NO,
    e.disposition_dt_tm AS unsub_tm,
    m.TREATMENT_ID
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  m.consumer_id_hashed = e.consumer_id_hashed
    AND m.TREATMENT_ID       = e.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2025-07-01'
  AND e.disposition_dt_tm <  DATE '2026-07-01'
  AND m.load_tm           >= DATE '2025-06-01'
  AND m.load_tm           <  DATE '2025-10-01'
"""
land(BASE + "unsub_base/chunk1", SQL_UNSUB_BASE_1, "ext_unsub_base chunk1 (load_tm Jun-Oct 2025)")


# %% [3] EXTRACT ext_unsub_base chunk2 - same EVENT window, MASTER load_tm Oct 2025-Feb 2026
SQL_UNSUB_BASE_2 = """
SELECT DISTINCT
    m.CLNT_NO,
    e.disposition_dt_tm AS unsub_tm,
    m.TREATMENT_ID
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  m.consumer_id_hashed = e.consumer_id_hashed
    AND m.TREATMENT_ID       = e.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2025-07-01'
  AND e.disposition_dt_tm <  DATE '2026-07-01'
  AND m.load_tm           >= DATE '2025-10-01'
  AND m.load_tm           <  DATE '2026-02-01'
"""
land(BASE + "unsub_base/chunk2", SQL_UNSUB_BASE_2, "ext_unsub_base chunk2 (load_tm Oct 2025-Feb 2026)")


# %% [4] EXTRACT ext_unsub_base chunk3 - same EVENT window, MASTER load_tm Feb-May 2026
SQL_UNSUB_BASE_3 = """
SELECT DISTINCT
    m.CLNT_NO,
    e.disposition_dt_tm AS unsub_tm,
    m.TREATMENT_ID
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  m.consumer_id_hashed = e.consumer_id_hashed
    AND m.TREATMENT_ID       = e.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2025-07-01'
  AND e.disposition_dt_tm <  DATE '2026-07-01'
  AND m.load_tm           >= DATE '2026-02-01'
  AND m.load_tm           <  DATE '2026-05-01'
"""
land(BASE + "unsub_base/chunk3", SQL_UNSUB_BASE_3, "ext_unsub_base chunk3 (load_tm Feb-May 2026)")


# %% [5] EXTRACT ext_unsub_base chunk4 - same EVENT window, MASTER load_tm May-Aug 2026
SQL_UNSUB_BASE_4 = """
SELECT DISTINCT
    m.CLNT_NO,
    e.disposition_dt_tm AS unsub_tm,
    m.TREATMENT_ID
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  m.consumer_id_hashed = e.consumer_id_hashed
    AND m.TREATMENT_ID       = e.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2025-07-01'
  AND e.disposition_dt_tm <  DATE '2026-07-01'
  AND m.load_tm           >= DATE '2026-05-01'
  AND m.load_tm           <  DATE '2026-08-01'
"""
land(BASE + "unsub_base/chunk4", SQL_UNSUB_BASE_4, "ext_unsub_base chunk4 (load_tm May-Aug 2026)")


# %% [6] EXTRACT ext_q2_send_detail m202604 - EVENT disp IN (1,5) April 2026, MASTER load_tm +/-1mo margin
SQL_Q2_SEND_APR = """
SELECT
    m.CLNT_NO,
    SUBSTR(m.TREATMENT_ID, 8, 3) AS mne,
    e.disposition_cd,
    e.disposition_dt_tm
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  m.consumer_id_hashed = e.consumer_id_hashed
    AND m.TREATMENT_ID       = e.TREATMENT_ID
WHERE e.disposition_cd IN (1, 5)
  AND e.disposition_dt_tm >= DATE '2026-04-01'
  AND e.disposition_dt_tm <  DATE '2026-05-01'
  AND m.load_tm           >= DATE '2026-03-01'
  AND m.load_tm           <  DATE '2026-06-01'
"""
land(BASE + "q2_send_detail/m202604", SQL_Q2_SEND_APR, "ext_q2_send_detail m202604")


# %% [7] EXTRACT ext_q2_send_detail m202605 - EVENT disp IN (1,5) May 2026, MASTER load_tm +/-1mo margin
SQL_Q2_SEND_MAY = """
SELECT
    m.CLNT_NO,
    SUBSTR(m.TREATMENT_ID, 8, 3) AS mne,
    e.disposition_cd,
    e.disposition_dt_tm
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  m.consumer_id_hashed = e.consumer_id_hashed
    AND m.TREATMENT_ID       = e.TREATMENT_ID
WHERE e.disposition_cd IN (1, 5)
  AND e.disposition_dt_tm >= DATE '2026-05-01'
  AND e.disposition_dt_tm <  DATE '2026-06-01'
  AND m.load_tm           >= DATE '2026-04-01'
  AND m.load_tm           <  DATE '2026-07-01'
"""
land(BASE + "q2_send_detail/m202605", SQL_Q2_SEND_MAY, "ext_q2_send_detail m202605")


# %% [8] EXTRACT ext_q2_send_detail m202606 - EVENT disp IN (1,5) June 2026, MASTER load_tm +/-1mo margin
SQL_Q2_SEND_JUN = """
SELECT
    m.CLNT_NO,
    SUBSTR(m.TREATMENT_ID, 8, 3) AS mne,
    e.disposition_cd,
    e.disposition_dt_tm
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  m.consumer_id_hashed = e.consumer_id_hashed
    AND m.TREATMENT_ID       = e.TREATMENT_ID
WHERE e.disposition_cd IN (1, 5)
  AND e.disposition_dt_tm >= DATE '2026-06-01'
  AND e.disposition_dt_tm <  DATE '2026-07-01'
  AND m.load_tm           >= DATE '2026-05-01'
  AND m.load_tm           <  DATE '2026-08-01'
"""
land(BASE + "q2_send_detail/m202606", SQL_Q2_SEND_JUN, "ext_q2_send_detail m202606")


# %% [9] DERIVE unsub_first - union 4 chunks, dedup to first unsub per client (mirrors vt_unsub_first)
unsub_base_all = (spark.read.parquet(BASE + "unsub_base/chunk1")
                   .unionByName(spark.read.parquet(BASE + "unsub_base/chunk2"))
                   .unionByName(spark.read.parquet(BASE + "unsub_base/chunk3"))
                   .unionByName(spark.read.parquet(BASE + "unsub_base/chunk4"))
                   .distinct())

w_first = Window.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").asc(), F.col("TREATMENT_ID").asc())
unsub_first = (unsub_base_all
               .withColumn("rn", F.row_number().over(w_first))
               .filter(F.col("rn") == 1)
               .withColumn("unsub_mne", F.substring(F.col("TREATMENT_ID"), 8, 3))
               .select("CLNT_NO", "unsub_tm", "unsub_mne")
               .cache())
unsub_first.createOrReplaceTempView("unsub_first")

n_rows = unsub_first.count()
n_clients = unsub_first.select("CLNT_NO").distinct().count()
assert n_rows == n_clients, (
    f"unsub_first: {n_rows:,} rows but {n_clients:,} distinct clients - "
    f"dedup rn=1 did not collapse to 1-per-client, investigate before proceeding")
print(f"unsub_first: {n_rows:,} rows, {n_clients:,} distinct clients (1-per-client confirmed)")


# %% [10] DERIVE cpc_pref_slice load + cpc_standing_latest(as_of) helper (mirrors cpc_latest/cpc_gate idiom)
cpc_pref_slice = spark.read.parquet(BASE + "cpc_pref_slice").cache()


def cpc_standing_latest(df, pref_ids, as_of=None):
    # latest row per (CLNT_NO, PREF_ID); as_of=None -> full history ever recorded (E2's
    # generous read); as_of='YYYY-MM-DD' -> standing strictly before that date (E4/E6/E7)
    filtered = df.filter(F.col("PREF_ID").isin(pref_ids))
    if as_of is not None:
        filtered = filtered.filter(F.col("CHG_TMSTMP") < F.lit(as_of).cast("timestamp"))
    w = Window.partitionBy("CLNT_NO", "PREF_ID").orderBy(F.col("CHG_TMSTMP").desc())
    return (filtered.withColumn("rn", F.row_number().over(w))
                     .filter(F.col("rn") == 1)
                     .drop("rn"))


n_slice = cpc_pref_slice.count()
n_pref_ids = cpc_pref_slice.select("PREF_ID").distinct().count()
assert n_pref_ids == 5, (
    f"cpc_pref_slice: expected 5 PREF_IDs (1002,1012,1014,1006,1007), found {n_pref_ids} - check extraction filter")
print(f"cpc_pref_slice: {n_slice:,} rows across {n_pref_ids} PREF_IDs")


# %% [11] DERIVE gate_cohorts (per-switch 5002 standing before 2026-04-01) + dns_1002 subset (mirrors vt_gate_cohorts / vt_dns_1002)
gate_cohorts = (cpc_standing_latest(cpc_pref_slice, [1002, 1012, 1014, 1006], as_of="2026-04-01")
                .filter(F.col("CLNT_CONSENT_TYP") == 5002)
                .select("CLNT_NO", "PREF_ID")
                .cache())
gate_cohorts.createOrReplaceTempView("gate_cohorts")

dns_1002 = gate_cohorts.filter(F.col("PREF_ID") == 1002).select("CLNT_NO").distinct().cache()

n_gate = gate_cohorts.count()
n_dns = dns_1002.count()
assert n_dns <= n_gate, f"dns_1002 ({n_dns:,}) should be a subset of gate_cohorts ({n_gate:,}) rows"
print(f"gate_cohorts: {n_gate:,} rows across 4 switches; dns_1002 (entity DNS only): {n_dns:,} clients")


# %% [12] DERIVE q2_send_detail_all (union 3 months) + q2_sends distinct disp=1 clients (mirrors vt_q2_send_detail / vt_q2_sends)
q2_send_detail_all = (spark.read.parquet(BASE + "q2_send_detail/m202604")
                       .unionByName(spark.read.parquet(BASE + "q2_send_detail/m202605"))
                       .unionByName(spark.read.parquet(BASE + "q2_send_detail/m202606"))
                       .cache())
q2_send_detail_all.createOrReplaceTempView("q2_send_detail_all")

q2_sends = q2_send_detail_all.filter(F.col("disposition_cd") == 1).select("CLNT_NO").distinct().cache()

n_detail = q2_send_detail_all.count()
n_sends = q2_sends.count()
assert n_sends > 0, "q2_sends: 0 clients with disp=1 in Apr-Jun 2026 - check q2_send_detail extraction bites"
print(f"q2_send_detail_all: {n_detail:,} rows (disp 1+5, Apr-Jun 2026); q2_sends: {n_sends:,} distinct disp=1 clients")


# %% [13] DERIVE postunsub_sends - disp IN (1,5) restricted to the pre-Apr-2026 unsub cohort (mirrors vt_postunsub_sends)
cohort_preapr = (unsub_first
                  .filter(F.col("unsub_tm") < F.lit("2026-04-01").cast("timestamp"))
                  .select("CLNT_NO", "unsub_tm", "unsub_mne")
                  .cache())

postunsub_sends = (q2_send_detail_all
                    .join(cohort_preapr.select("CLNT_NO"), on="CLNT_NO", how="inner")
                    .select("CLNT_NO", "mne", "disposition_cd", "disposition_dt_tm")
                    .distinct()
                    .cache())

n_cohort = cohort_preapr.count()
n_post = postunsub_sends.count()
assert n_cohort > 0, "cohort_preapr: 0 clients unsubscribed before Apr 2026 - check unsub_first / date floor"
print(f"cohort_preapr: {n_cohort:,} clients unsubscribed before Apr 2026; postunsub_sends: {n_post:,} distinct send rows")


# %% [14] EVIDENCE 1 - two consent worlds, monthly volumes (email unsubs vs CPC opt-outs)
e1_email = (unsub_first
            .withColumn("month_yyyymm", F.year("unsub_tm") * 100 + F.month("unsub_tm"))
            .groupBy("month_yyyymm")
            .agg(F.countDistinct("CLNT_NO").alias("clients"))
            .withColumn("consent_world", F.lit("email_unsub")))

e1_cpc = (cpc_pref_slice
          .filter(F.col("PREF_ID").isin([1002, 1012, 1014]) &
                  (F.col("CLNT_CONSENT_TYP") == 5002) &
                  (F.col("CHG_TMSTMP") >= F.lit("2025-07-01")) &
                  (F.col("CHG_TMSTMP") < F.lit("2026-07-01")))
          .withColumn("month_yyyymm", F.year("CHG_TMSTMP") * 100 + F.month("CHG_TMSTMP"))
          .groupBy("month_yyyymm")
          .agg(F.countDistinct("CLNT_NO").alias("clients"))
          .withColumn("consent_world", F.lit("cpc_optout")))

e1 = (e1_email.unionByName(e1_cpc)
      .select("consent_world", "month_yyyymm", "clients")
      .orderBy("consent_world", "month_yyyymm"))

n_e1 = e1.count()
assert n_e1 > 0, "Evidence 1: 0 rows - check unsub_first / cpc_pref_slice windows"
print("EVIDENCE 1 - two consent worlds, monthly volumes:")
e1.show(50, truncate=False)


# %% [15] EVIDENCE 2 - the blind gate, 5 rows incl. before/after unsub split
cpc_latest_full = cpc_standing_latest(cpc_pref_slice, [1002, 1012, 1014], as_of=None)
cpc_optout_detail = cpc_latest_full.filter(F.col("CLNT_CONSENT_TYP") == 5002).select("CLNT_NO", "CHG_TMSTMP")
cpc_optout = cpc_optout_detail.select("CLNT_NO").distinct()
cpc_optout_earliest = cpc_optout_detail.groupBy("CLNT_NO").agg(F.min("CHG_TMSTMP").alias("optout_chg_tmstmp"))

flagged = (unsub_first
           .join(cpc_optout.withColumn("has_cpc_optout", F.lit(1)), on="CLNT_NO", how="left")
           .join(cpc_optout_earliest, on="CLNT_NO", how="left")
           .withColumn("has_cpc_optout", F.coalesce(F.col("has_cpc_optout"), F.lit(0)))
           .withColumn("optout_before_unsub",
                       F.when(F.col("optout_chg_tmstmp").isNotNull() &
                              (F.col("optout_chg_tmstmp") < F.col("unsub_tm")), 1).otherwise(0))
           .withColumn("optout_after_unsub",
                       F.when(F.col("optout_chg_tmstmp").isNotNull() &
                              (F.col("optout_chg_tmstmp") >= F.col("unsub_tm")), 1).otherwise(0))
           .cache())

flagged_totals = flagged.agg(
    F.count("*").alias("unsub_clients_total"),
    F.sum("has_cpc_optout").alias("with_explicit_cpc_optout"),
    F.sum(1 - F.col("has_cpc_optout")).alias("without_explicit_cpc_optout"),
    F.sum("optout_before_unsub").alias("optout_recorded_before_unsub"),
    F.sum("optout_after_unsub").alias("optout_recorded_after_unsub")
).first()

e2_rows = [
    ("unsub_clients_total", int(flagged_totals["unsub_clients_total"])),
    ("with_explicit_cpc_optout", int(flagged_totals["with_explicit_cpc_optout"])),
    ("without_explicit_cpc_optout", int(flagged_totals["without_explicit_cpc_optout"])),
    ("optout_recorded_before_unsub", int(flagged_totals["optout_recorded_before_unsub"])),
    ("optout_recorded_after_unsub", int(flagged_totals["optout_recorded_after_unsub"])),
]
e2 = spark.createDataFrame(e2_rows, ["metric", "clients"]).orderBy("metric")
assert e2.count() == 5, "Evidence 2: expected exactly 5 rows"
print("EVIDENCE 2 - the blind gate:")
e2.show(truncate=False)


# %% [16] EVIDENCE 3 - no automated bridge (CPC flips matched against any prior unsub)
cpc_flips = (cpc_pref_slice
             .filter(F.col("PREF_ID").isin([1002, 1012, 1014]) &
                     (F.col("CLNT_CONSENT_TYP") == 5002) &
                     (F.col("CHG_TMSTMP") >= F.lit("2025-07-01")))
             .select("CLNT_NO", "PREF_ID", "APP_SYS_CD", "CHG_TMSTMP"))

nearest_prior = (cpc_flips.alias("f")
                 .join(unsub_first.alias("u"),
                       (F.col("u.CLNT_NO") == F.col("f.CLNT_NO")) & (F.col("u.unsub_tm") < F.col("f.CHG_TMSTMP")),
                       how="left")
                 .select(F.col("f.CLNT_NO").alias("flip_clnt_no"), F.col("f.PREF_ID"), F.col("f.APP_SYS_CD"),
                         F.col("f.CHG_TMSTMP"), F.col("u.CLNT_NO").alias("matched_unsub"), F.col("u.unsub_tm")))

w_np = Window.partitionBy("flip_clnt_no", "PREF_ID", "CHG_TMSTMP").orderBy(F.col("unsub_tm").desc())
nearest_prior_ranked = (nearest_prior
                         .withColumn("rn", F.row_number().over(w_np))
                         .filter(F.col("rn") == 1)
                         .withColumn("had_prior_unsub", F.when(F.col("matched_unsub").isNotNull(), "Y").otherwise("N")))

e3 = (nearest_prior_ranked.groupBy("PREF_ID", "APP_SYS_CD", "had_prior_unsub")
      .agg(F.count("*").alias("flips"))
      .orderBy("PREF_ID", F.desc("flips")))

n_e3 = e3.count()
assert n_e3 > 0, "Evidence 3: 0 rows - check cpc_flips / unsub_first join"
print("EVIDENCE 3 - no automated bridge:")
e3.show(50, truncate=False)


# %% [17] EVIDENCE 4 - the leaking gate (opt-out standing before Apr 1 -> campaign email Apr-Jun)
cpc_gate_e4 = gate_cohorts.filter(F.col("PREF_ID").isin([1002, 1012, 1014]))

gate_pivot = (cpc_gate_e4.groupBy("CLNT_NO")
              .agg(F.max(F.when(F.col("PREF_ID") == 1002, 1).otherwise(0)).alias("out_1002"),
                   F.max(F.when(F.col("PREF_ID") == 1012, 1).otherwise(0)).alias("out_1012"),
                   F.max(F.when(F.col("PREF_ID") == 1014, 1).otherwise(0)).alias("out_1014")))
gate_pivot = gate_pivot.withColumn("flag_count", F.col("out_1002") + F.col("out_1012") + F.col("out_1014"))

gate_long = (gate_pivot.filter(F.col("out_1002") == 1).select("CLNT_NO", F.lit(1002).alias("pref_id"), "flag_count")
             .unionByName(gate_pivot.filter(F.col("out_1012") == 1).select("CLNT_NO", F.lit(1012).alias("pref_id"), "flag_count"))
             .unionByName(gate_pivot.filter(F.col("out_1014") == 1).select("CLNT_NO", F.lit(1014).alias("pref_id"), "flag_count")))

gate_long_flagged = (gate_long
                      .withColumn("exclusivity", F.when(F.col("flag_count") == 1, "only_this_flag").otherwise("multi_flag"))
                      .join(q2_sends.select("CLNT_NO").withColumn("got_email", F.lit(1)), on="CLNT_NO", how="left")
                      .withColumn("got_email", F.coalesce(F.col("got_email"), F.lit(0))))

e4_by_pref = (gate_long_flagged.groupBy("pref_id", "exclusivity")
              .agg(F.count("*").alias("optout_clients"), F.sum("got_email").alias("got_email_apr_jun"))
              .withColumn("pref_id", F.col("pref_id").cast("string")))

e4_all = (gate_long.select("CLNT_NO").distinct()
          .join(q2_sends.select("CLNT_NO").withColumn("got_email", F.lit(1)), on="CLNT_NO", how="left")
          .withColumn("got_email", F.coalesce(F.col("got_email"), F.lit(0)))
          .agg(F.count("*").alias("optout_clients"), F.sum("got_email").alias("got_email_apr_jun"))
          .withColumn("pref_id", F.lit("ALL_SWITCHES"))
          .withColumn("exclusivity", F.lit("any_flag")))

e4 = (e4_by_pref.select("pref_id", "exclusivity", "optout_clients", "got_email_apr_jun")
      .unionByName(e4_all.select("pref_id", "exclusivity", "optout_clients", "got_email_apr_jun"))
      .orderBy("pref_id", "exclusivity"))

n_e4 = e4.count()
assert 4 <= n_e4 <= 7, (
    f"Evidence 4: expected 4-7 rows (3 switches x up to 2 exclusivity levels + ALL_SWITCHES), got {n_e4}")
print("EVIDENCE 4 - the leaking gate:")
e4.show(truncate=False)


# %% [18] EVIDENCE 5 - does the vendor honor unsubscribes (unsub pre-Apr -> got email Apr-Jun)
e5_total = cohort_preapr.count()
e5_got_email = cohort_preapr.join(q2_sends, on="CLNT_NO", how="inner").count()
e5 = spark.createDataFrame(
    [("unsub_before_apr_clients", e5_total), ("got_email_apr_jun", e5_got_email)],
    ["metric", "clients"])
assert e5.count() == 2, "Evidence 5: expected exactly 2 rows"
print("EVIDENCE 5 - does the vendor honor unsubscribes:")
e5.show(truncate=False)


# %% [19] EVIDENCE 6 - which campaigns reach 1002 (entity DNS) clients, top 20 by clients
e6 = (q2_send_detail_all.filter(F.col("disposition_cd") == 1)
      .join(dns_1002, on="CLNT_NO", how="inner")
      .groupBy("mne")
      .agg(F.countDistinct("CLNT_NO").alias("clients"), F.count("*").alias("send_rows"))
      .orderBy(F.desc("clients"))
      .limit(20))

n_e6 = e6.count()
assert 0 < n_e6 <= 20, f"Evidence 6: expected 1-20 mne rows, got {n_e6} - check dns_1002 / q2_send_detail_all join"
print("EVIDENCE 6 - campaigns reaching 1002 DNS clients (top 20):")
e6.show(20, truncate=False)


# %% [20] EVIDENCE 7 - gate x mne, top 12 per switch (mirrors vt_gate_cohorts x vt_q2_send_detail QUALIFY)
e7_counts = (gate_cohorts
             .join(q2_send_detail_all.filter(F.col("disposition_cd") == 1), on="CLNT_NO", how="inner")
             .groupBy("PREF_ID", "mne")
             .agg(F.countDistinct("CLNT_NO").alias("clients")))

w_e7 = Window.partitionBy("PREF_ID").orderBy(F.desc("clients"))
e7 = (e7_counts.withColumn("rk", F.row_number().over(w_e7))
      .filter(F.col("rk") <= 12)
      .select(F.col("PREF_ID").cast("string").alias("pref_id"), F.col("mne"), F.col("clients"))
      .orderBy("pref_id", F.desc("clients")))

n_e7 = e7.count()
assert 0 < n_e7 <= 48, f"Evidence 7: expected 1-48 rows (4 switches x top 12), got {n_e7}"
print("EVIDENCE 7 - gate x mne, top 12 per switch:")
e7.show(50, truncate=False)


# %% [21] EVIDENCE 8 - the 7-row ladder (CASL lag -> hardbounce -> CPC re-consent -> cross/same-campaign residual)
sent = (postunsub_sends.filter(F.col("disposition_cd") == 1)
        .join(cohort_preapr, on="CLNT_NO", how="inner")
        .select("CLNT_NO", "mne", "disposition_dt_tm", "unsub_tm", "unsub_mne"))

bounced = postunsub_sends.filter(F.col("disposition_cd") == 5).select("CLNT_NO", "mne").distinct()

sent_flagged = (sent
                 .join(bounced.withColumn("bounced_flag", F.lit(1)), on=["CLNT_NO", "mne"], how="left")
                 .withColumn("in_casl_window",
                             F.expr("CASE WHEN disposition_dt_tm <= unsub_tm + INTERVAL 14 DAYS THEN 1 ELSE 0 END"))
                 .withColumn("mne_bounced", F.coalesce(F.col("bounced_flag"), F.lit(0)))
                 .select("CLNT_NO", "mne", "unsub_mne", "in_casl_window", "mne_bounced")
                 .cache())

client_rollup = (sent_flagged.groupBy("CLNT_NO")
                  .agg(F.min("in_casl_window").alias("all_in_casl_window"),
                       F.min("mne_bounced").alias("all_bounced")))

step2 = client_rollup.filter(F.col("all_in_casl_window") == 0).select("CLNT_NO")
step3 = step2.join(client_rollup, on="CLNT_NO", how="inner").filter(F.col("all_bounced") == 0).select("CLNT_NO")

reconsent = (cpc_pref_slice
             .filter(F.col("PREF_ID").isin([1002, 1012]) &
                     (F.col("CLNT_CONSENT_TYP") == 5001) &
                     (F.col("CHG_TMSTMP") >= F.lit("2025-07-01")) &
                     (F.col("CHG_TMSTMP") < F.lit("2026-07-01")))
             .join(cohort_preapr.select("CLNT_NO", "unsub_tm"), on="CLNT_NO", how="inner")
             .filter(F.col("CHG_TMSTMP") > F.col("unsub_tm"))
             .select("CLNT_NO").distinct())

step4 = step3.join(reconsent, on="CLNT_NO", how="left_anti")

same_campaign_residual = (sent_flagged.join(step4, on="CLNT_NO", how="inner")
                           .filter((F.col("in_casl_window") == 0) & (F.col("mne_bounced") == 0) &
                                   (F.col("mne") == F.col("unsub_mne")))
                           .select("CLNT_NO").distinct())

cross_campaign_residual = step4.join(same_campaign_residual, on="CLNT_NO", how="left_anti")

e8_rows = [
    ("0 unsubscribed before Apr 2026 (cohort)", cohort_preapr.count()),
    ("1 gross: received any send Apr-Jun", sent.select("CLNT_NO").distinct().count()),
    ("2 excl. sends within 14 days of unsub (CASL proxy)", step2.count()),
    ("3 excl. clients whose every send hardbounced (mne proxy)", step3.count()),
    ("4 excl. CPC-side re-consents after unsub", step4.count()),
    ("5 residual: cross-campaign only (different mne)", cross_campaign_residual.count()),
    ("6 residual: same campaign as unsubbed (in-program leak)", same_campaign_residual.count()),
]
e8 = spark.createDataFrame(e8_rows, ["step", "clients"]).orderBy("step")
assert e8.count() == 7, "Evidence 8: expected exactly 7 ladder rows"
print("EVIDENCE 8 - the 7-row ladder:")
e8.show(truncate=False)


# %% [22] SUMMARY - the 12-row story table (reuses cells 9-21's DataFrames, no fresh Teradata scans)
unsub_first_total = unsub_first.count()
cpc_flips_12mo = (cpc_pref_slice
                   .filter(F.col("PREF_ID").isin([1002, 1012, 1014]) &
                           (F.col("CLNT_CONSENT_TYP") == 5002) &
                           (F.col("CHG_TMSTMP") >= F.lit("2025-07-01")) &
                           (F.col("CHG_TMSTMP") < F.lit("2026-07-01")))
                   .select("CLNT_NO").distinct().count())
dns_1002_total = dns_1002.count()
dns_1002_got_email = dns_1002.join(q2_sends, on="CLNT_NO", how="inner").count()
cohort_preapr_total = cohort_preapr.count()
cohort_preapr_got_email = cohort_preapr.join(q2_sends, on="CLNT_NO", how="inner").count()
sent_gross = sent.select("CLNT_NO").distinct().count()
same_campaign_leak = same_campaign_residual.count()

flagged_sums = flagged.agg(
    F.sum("has_cpc_optout").alias("w_optout"),
    F.sum("optout_before_unsub").alias("before"),
    F.sum("optout_after_unsub").alias("after")
).first()

summary_rows = [
    ("email unsubs, 12-mo total", "Jul 2025 - Jun 2026", unsub_first_total, None),
    ("cpc opt-out flips, 12-mo total", "Jul 2025 - Jun 2026", cpc_flips_12mo, None),
    ("unsubscribers, total", "Jul 2025 - Jun 2026", unsub_first_total, None),
    ("w/ explicit CPC opt-out", "any time", int(flagged_sums["w_optout"]), unsub_first_total),
    ("opt-out recorded before unsub", "any time", int(flagged_sums["before"]), unsub_first_total),
    ("opt-out recorded after unsub", "any time", int(flagged_sums["after"]), unsub_first_total),
    ("do-not-solicit 1002, standing", "before Apr 1 2026", dns_1002_total, None),
    ("1002 standing, got campaign email", "Apr - Jun 2026", dns_1002_got_email, dns_1002_total),
    ("unsubscribed before Apr 2026", "Jul 2025 - Mar 2026", cohort_preapr_total, None),
    ("unsub pre-Apr, got campaign email", "Apr - Jun 2026", cohort_preapr_got_email, cohort_preapr_total),
    ("post-unsub receivers, gross", "Apr - Jun 2026", sent_gross, cohort_preapr_total),
    ("post-unsub receivers, in-program leak", "Apr - Jun 2026", same_campaign_leak, cohort_preapr_total),
]
summary = spark.createDataFrame(summary_rows, ["what", "time_window", "clients", "of_population"])
assert summary.count() == 12, "SUMMARY: expected exactly 12 rows"
print("SUMMARY - the story in one table:")
summary.show(50, truncate=False)
