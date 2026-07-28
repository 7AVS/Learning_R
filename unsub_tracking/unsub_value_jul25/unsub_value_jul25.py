# UNSUB VALUE - JULY 2025 COHORT, 11-MONTH FOLLOW-UP
#
# Question: among clients mailed in July 2025, did the ones who unsubscribed diverge from the ones who
# did not - in whether they are still here, and in what they are worth?
#
# ===================== DESIGN, SETTLED - DO NOT RE-DERIVE =====================
# UNIVERSE      clients with a vendor send (disposition_cd = 1) in July 2025.
# TREATMENT     of those, unsubscribed (disposition_cd = 4) in July 2025.
# CONTROL       of those, did not.
# BASELINE      UCP partition MONTH_END_DATE=2025-07-31   (at treatment)
# FOLLOW-UP     UCP partition MONTH_END_DATE=2026-06-30   (11 months later; 2026-07-31 not yet landed)
# OUTCOME 1     present in the follow-up partition = still with us. Nobody is dropped for absence.
# OUTCOME 2     change in PROF_TOT_ANNUAL, among clients present in both.
# ESTIMATOR     difference-in-differences, STRATIFIED BY BASELINE PROFITABILITY DECILE. The decile
#               stratification is not decoration - PROF_TOT_ANNUAL is heavily skewed and regression to
#               the mean will manufacture a difference if the groups sit at different baselines.
#
# 1. Attrition is an OUTCOME, not a filter. Never restrict to survivors before measuring profitability.
# 2. Counts always ship next to any rate, so every rate can be recomputed from this file.
# 3. Two UCP partitions, fixed, shared by every client. Per-client anchoring fans out and kills YARN.
# 4. PROF_TOT_ANNUAL's definition (current-year vs lifetime) is NOT documented. Deltas are directional.
# =============================================================================

# %% [0] Bootstrap - teradatasql from artifactory; run ONCE per kernel
get_ipython().system("./environment/bin/python -m pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")

# %% [1] Connections + helpers
import getpass, time
import pandas as pd
import teradatasql
from pyspark.sql import functions as F, Window as W

if not hasattr(pd.DataFrame, "iteritems"):
    pd.DataFrame.iteritems = pd.DataFrame.items

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")
EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com", user=username, password=password, logmech="LDAP")

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

UCP_BASE  = "/prod/sz/tsz/00172/data/ucp4/"
OUT       = "hdfs:///user/427966379/unsub_value_jul25/"
BASELINE  = "2025-07-31"
FOLLOWUP  = "2026-06-30"

_LOG = []
def log(step, label, source, filt, clients, rows, note=""):
    _LOG.append((step, label, source, filt, clients, rows, note))
    print("[%02d] %-34s clients=%-12s rows=%-12s %s" % (step, label, f"{clients:,}" if isinstance(clients,int) else clients,
                                                        f"{rows:,}" if isinstance(rows,int) else rows, note))

def edw_pd(sql, chunksize=1_000_000):
    parts, n, t0 = [], 0, time.time()
    for c in pd.read_sql(sql, EDW, chunksize=chunksize):
        parts.append(c); n += len(c)
        print("   ...", f"{n:,}", "rows pulled,", int(time.time() - t0), "s elapsed", flush=True)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

def key_pd(sr, label=""):
    """EDW CLNT_NO -> canonical string key. Two traps: pandas float64 renders 1.56314759E8 and joins to
    nothing, and a NULL CLNT_NO makes .astype(int64) raise IntCastingNaNError. Nulls become NaN here and
    are dropped by the caller, which reports how many."""
    n = pd.to_numeric(sr, errors="coerce")
    bad = int(n.isna().sum())
    if bad:
        print("   key_pd(%s): %s of %s CLNT_NO are null or non-numeric - these rows are dropped"
              % (label, f"{bad:,}", f"{len(n):,}"))
    return n.round(0).astype("Int64").astype("string")

# ---- HDFS cache: land each Teradata pull once, re-pull only when the SQL text changes ----
CACHE  = OUT + "_cache/"
REPULL = False   # flip True to force a fresh pull regardless of what is landed

def cached(name, sql_text, puller):
    import hashlib, re as _re, datetime as _dt
    key  = hashlib.md5(_re.sub(r"\s+", " ", sql_text).strip().upper().encode()).hexdigest()
    path, meta = CACHE + name, CACHE + "_meta/" + name
    if not REPULL:
        try:
            old = spark.read.parquet(meta).collect()[0]
            if old["sql_md5"] == key:
                out = spark.read.parquet(path).toPandas()
                print("CACHE HIT   %-12s %s rows (landed %s)" % (name, f"{len(out):,}", old["landed_at"]))
                return out
            print("CACHE STALE %-12s SQL changed since %s - re-pulling" % (name, old["landed_at"]))
        except Exception:
            print("CACHE MISS  %-12s - pulling from Teradata" % name)
    out = puller()
    spark.createDataFrame(out).write.mode("overwrite").parquet(path)
    spark.createDataFrame([(name, key, _dt.datetime.now().isoformat(), len(out))],
                          ["name", "sql_md5", "landed_at", "rows"]).write.mode("overwrite").parquet(meta)
    print("LANDED      %-12s %s rows -> %s" % (name, f"{len(out):,}", path))
    return out

def key_sp(col):
    """UCP CLNT_NO -> same canonical form: trimmed, leading zeros stripped."""
    return F.regexp_replace(F.trim(col.cast("string")), "^0+", "")

def ucp(anchor, fields):
    df = spark.read.option("basePath", UCP_BASE).parquet(UCP_BASE + "MONTH_END_DATE=" + anchor + "/")
    have = set(df.columns)
    missing = [f for f in fields if f not in have]
    if missing:
        raise RuntimeError("UCP %s is missing %s - check references/ucp/field_catalog_personal.md" % (anchor, missing))
    out = df.select(key_sp(F.col("CLNT_NO")).alias("clnt_key"), *[F.col(f) for f in fields])
    n_raw = out.count(); n_key = out.select("clnt_key").distinct().count()
    print("UCP %s: %s rows, %s distinct clients%s" % (anchor, f"{n_raw:,}", f"{n_key:,}",
          "" if n_raw == n_key else "  !! NOT unique per client - deduping on clnt_key"))
    if n_raw != n_key:
        w = W.partitionBy("clnt_key").orderBy(F.col(fields[0]).desc_nulls_last())
        out = out.withColumn("_r", F.row_number().over(w)).filter("_r = 1").drop("_r")
    return out.cache()

def save(sdf, name):
    path = OUT + name
    sdf.coalesce(1).write.mode("overwrite").option("header", True).csv(path)
    n = spark.read.option("header", True).csv(path).count()
    print("SAVED %-18s -> %s  (%d rows, readback confirms)" % (name, path, n))

cur = EDW.cursor(); cur.execute("SELECT USER, CURRENT_TIMESTAMP")
print("EDW round-trip returned:", cur.fetchall()); cur.close()

# %% [2] EXTRACT the July 2025 cohort, in 10 bites.
# Spool note (2026-07-28): the single-statement version hit Teradata error 2646. It materialised the
# EVENT x MASTER join three times - once per CTE, then again for the outer LEFT JOIN - and carried a
# DISTINCT and a window function on top. This version joins ONCE, derives both flags with conditional
# aggregation, and splits on CLNT_NO MOD 10 so each bite holds a tenth of the spool. Restartable per bite.
COHORT_TMPL = """
SELECT m.CLNT_NO,
       MAX(CASE WHEN e.disposition_cd = 1 THEN 1 ELSE 0 END) AS mailed,
       MAX(CASE WHEN e.disposition_cd = 4 THEN 1 ELSE 0 END) AS unsubbed
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd IN (1, 4)
  AND e.disposition_dt_tm >= DATE '2025-07-01' AND e.disposition_dt_tm < DATE '2025-08-01'
  AND m.load_tm >= DATE '2025-06-01' AND m.load_tm < DATE '2025-09-01'
  AND ABS(m.CLNT_NO) MOD 10 = %d
GROUP BY m.CLNT_NO
"""
def _pull_cohort():
    bites = []
    for i in range(10):
        print("bite %d/10" % (i + 1), flush=True)
        b = edw_pd(COHORT_TMPL % i)
        assert len(b) > 0, "bite %d returned zero rows - investigate before continuing" % i
        bites.append(b)
    out = pd.concat(bites, ignore_index=True)
    out["clnt_key"] = key_pd(out["CLNT_NO"], "cohort")
    n0 = len(out); out = out[out["clnt_key"].notna()].copy()
    if len(out) < n0:
        print("   dropped %s rows with a null CLNT_NO" % f"{n0 - len(out):,}")
    out["clnt_key"] = out["clnt_key"].astype(str)
    out["mailed"]   = out["mailed"].astype("int32")
    out["unsubbed"] = out["unsubbed"].astype("int32")
    return out[["clnt_key", "mailed", "unsubbed"]]

raw = cached("cohort_raw", COHORT_TMPL, _pull_cohort)

n_raw       = len(raw)
n_unsub_any = int((raw["unsubbed"] == 1).sum())
pdf         = raw[raw["mailed"] == 1].copy()
n_all       = len(pdf)
n_uns       = int(pdf["unsubbed"].sum())

log(1, "send or unsub event, Jul 2025", "VENDOR_FEEDBACK", "disposition_cd IN (1,4)", n_raw, n_raw, "10 bites")
log(2, "mailed in Jul 2025", "VENDOR_FEEDBACK", "mailed = 1", n_all, n_all, "THE UNIVERSE")
log(3, "of those, unsubscribed Jul 2025", "VENDOR_FEEDBACK", "unsubbed = 1", n_uns, n_uns, "treatment group")
log(4, "of those, did not unsubscribe", "VENDOR_FEEDBACK", "unsubbed = 0", n_all - n_uns, n_all - n_uns, "control group")
log(5, "unsubscribed but no Jul send", "VENDOR_FEEDBACK", "unsubbed=1 AND mailed=0", n_unsub_any - n_uns,
    n_unsub_any - n_uns, "excluded - left the universe undefined for them")
display(pdf.head(5))

# %% [2b] Triggering mnemonic, unsubscribers only. Small population, so a window function is affordable here.
MNE_SQL = """
SELECT CLNT_NO, unsub_mne FROM (
  SELECT m.CLNT_NO, SUBSTR(m.TREATMENT_ID, 8, 3) AS unsub_mne,
         ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO ORDER BY e.disposition_dt_tm ASC, m.TREATMENT_ID ASC) AS rn
  FROM DTZV01.VENDOR_FEEDBACK_EVENT e
  INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
  WHERE e.disposition_cd = 4
    AND e.disposition_dt_tm >= DATE '2025-07-01' AND e.disposition_dt_tm < DATE '2025-08-01'
    AND m.load_tm >= DATE '2025-06-01' AND m.load_tm < DATE '2025-09-01'
) x WHERE rn = 1
"""
def _pull_mne():
    out = edw_pd(MNE_SQL)
    out["clnt_key"] = key_pd(out["CLNT_NO"], "mne")
    out = out[out["clnt_key"].notna()].copy()
    out["clnt_key"] = out["clnt_key"].astype(str)
    out["unsub_mne"] = out["unsub_mne"].fillna("").astype(str).str.strip()
    return out[["clnt_key", "unsub_mne"]]

mne = cached("unsub_mne", MNE_SQL, _pull_mne)
pdf = pdf.merge(mne[["clnt_key", "unsub_mne"]], on="clnt_key", how="left")
pdf["unsub_mne"] = pdf["unsub_mne"].fillna("")
assert len(pdf) == n_all, "mnemonic merge changed the row count: %d -> %d" % (n_all, len(pdf))
print("mnemonic attached to", f"{int((pdf['unsub_mne'] != '').sum()):,}", "of", f"{n_uns:,}", "unsubscribers")

# %% [3] Hand to Spark in-session. Nothing is written to disk here.
cohort = (spark.createDataFrame(pdf[["clnt_key", "unsubbed", "unsub_mne"]])
               .withColumn("grp", F.when(F.col("unsubbed") == 1, "unsubscribed").otherwise("mailed_not_unsub"))
               .dropDuplicates(["clnt_key"]).cache())
n_cohort = cohort.count()
assert n_cohort == n_all, "cohort lost rows crossing to Spark: %d -> %d" % (n_all, n_cohort)
print("cohort in Spark:", f"{n_cohort:,}", "clients |", cohort.filter("unsubbed = 1").count(), "unsubscribed")

# %% [4] UCP at both anchors. PROOF: the join must not be zero - that is the known CLNT_NO failure mode.
UF = ["PROF_TOT_ANNUAL", "TENURE_RBC_YEARS", "ACTV_PROD_CNT"]
base = ucp(BASELINE, UF)
foll = ucp(FOLLOWUP, ["PROF_TOT_ANNUAL"]).withColumnRenamed("PROF_TOT_ANNUAL", "PROF_FOLLOWUP")

b = cohort.join(base, "clnt_key", "left")
n_matched = b.filter(F.col("PROF_TOT_ANNUAL").isNotNull()).count()
assert n_matched > 0, "ZERO baseline matches - CLNT_NO normalisation failed, compare key_pd() vs key_sp()"
log(4, "matched to UCP %s" % BASELINE, "ucp4", "left join on clnt_key", n_matched, n_matched,
    "%.1f%% of the universe" % (100.0 * n_matched / n_cohort))

# %% [5] Baseline deciles, cut across BOTH groups together so the bands mean the same thing on each side.
_w = W.orderBy(F.col("PROF_TOT_ANNUAL").asc_nulls_last())
b = (b.withColumn("dec", F.when(F.col("PROF_TOT_ANNUAL").isNull(), F.lit(None))
                          .otherwise(F.ntile(10).over(_w)))
      .withColumn("baseline_prof_decile", F.coalesce(F.col("dec").cast("string"), F.lit("no_baseline_ucp")))
      .drop("dec"))
panel = b.join(foll, "clnt_key", "left").withColumn("present_followup",
                F.when(F.col("PROF_FOLLOWUP").isNotNull(), 1).otherwise(0)).cache()
n_present = panel.filter("present_followup = 1").count()
log(5, "present in UCP %s" % FOLLOWUP, "ucp4", "left join on clnt_key", n_present, n_present,
    "%.1f%% still present" % (100.0 * n_present / n_cohort))
display(panel.groupBy("grp").agg(F.count("*").alias("clients"),
        F.sum("present_followup").alias("present_followup")).toPandas())

# %% [6] 01_cohort.csv - the audit trail
c01 = spark.createDataFrame(pd.DataFrame(_LOG, columns=["step_no","step_label","source","filter_applied",
                                                        "clients_remaining","rows_remaining","note"]))
save(c01, "01_cohort")

# %% [7] 02_balance.csv - were the two groups comparable at baseline
def q(col, p):  return F.expr("percentile_approx(%s, %s)" % (col, p))
_bal = (panel.groupBy("grp").agg(
            F.count("*").alias("clients"),
            F.sum(F.when(F.col("PROF_TOT_ANNUAL").isNull(), 1).otherwise(0)).alias("missing_baseline_ucp"),
            q("PROF_TOT_ANNUAL", 0.25).alias("p25_prof_baseline"),
            q("PROF_TOT_ANNUAL", 0.50).alias("median_prof_baseline"),
            q("PROF_TOT_ANNUAL", 0.75).alias("p75_prof_baseline"),
            q("TENURE_RBC_YEARS", 0.50).alias("median_tenure_years"),
            q("ACTV_PROD_CNT",   0.50).alias("median_active_products")).toPandas().set_index("grp"))
_rows = [(m, float(_bal.loc["unsubscribed", m]), float(_bal.loc["mailed_not_unsub", m]),
          float(_bal.loc["unsubscribed", m]) - float(_bal.loc["mailed_not_unsub", m])) for m in _bal.columns]
c02 = spark.createDataFrame(pd.DataFrame(_rows, columns=["metric","unsub","control","difference"]))
save(c02, "02_balance"); display(c02.toPandas())

# %% [8] 03_attrition.csv - OUTCOME 1. Nobody dropped. Counts beside the rate.
_att = (panel.groupBy("baseline_prof_decile", "grp")
             .agg(F.count("*").alias("clients_at_baseline"),
                  F.sum("present_followup").alias("clients_present_jun2026"))
             .withColumn("clients_absent_jun2026", F.col("clients_at_baseline") - F.col("clients_present_jun2026"))
             .withColumn("pct_present", F.round(100.0 * F.col("clients_present_jun2026") / F.col("clients_at_baseline"), 2)))
_all = (panel.groupBy("grp").agg(F.count("*").alias("clients_at_baseline"),
                                 F.sum("present_followup").alias("clients_present_jun2026"))
             .withColumn("clients_absent_jun2026", F.col("clients_at_baseline") - F.col("clients_present_jun2026"))
             .withColumn("pct_present", F.round(100.0 * F.col("clients_present_jun2026") / F.col("clients_at_baseline"), 2))
             .withColumn("baseline_prof_decile", F.lit("ALL")))
c03 = _att.unionByName(_all.select(_att.columns)).orderBy("baseline_prof_decile", "grp")
save(c03, "03_attrition"); display(c03.toPandas())

# %% [9] 04_profit.csv - OUTCOME 2. Among clients present in BOTH partitions.
both = panel.filter("present_followup = 1 AND PROF_TOT_ANNUAL IS NOT NULL") \
            .withColumn("delta", F.col("PROF_FOLLOWUP") - F.col("PROF_TOT_ANNUAL"))
def prof(gcols):
    return (both.groupBy(*gcols).agg(
        F.count("*").alias("clients_both_partitions"),
        q("PROF_TOT_ANNUAL", 0.50).alias("median_prof_baseline"),
        q("PROF_FOLLOWUP",   0.50).alias("median_prof_followup"),
        q("delta", 0.50).alias("median_delta"),
        F.round(F.avg("PROF_TOT_ANNUAL"), 2).alias("mean_prof_baseline"),
        F.round(F.avg("PROF_FOLLOWUP"),   2).alias("mean_prof_followup"),
        F.round(F.avg("delta"), 2).alias("mean_delta"),
        q("delta", 0.25).alias("p25_delta"),
        q("delta", 0.75).alias("p75_delta")))
_p = prof(["baseline_prof_decile", "grp"])
_pa = prof(["grp"]).withColumn("baseline_prof_decile", F.lit("ALL")).select(_p.columns)
_p = _p.unionByName(_pa)
_ctl = _p.filter("grp = 'mailed_not_unsub'").select("baseline_prof_decile", F.col("median_delta").alias("_c"))
c04 = (_p.join(_ctl, "baseline_prof_decile", "left")
         .withColumn("delta_vs_control", F.when(F.col("grp") == "unsubscribed",
                                                F.round(F.col("median_delta") - F.col("_c"), 2)))
         .drop("_c").orderBy("baseline_prof_decile", "grp"))
save(c04, "04_profit"); display(c04.toPandas())

# %% [10] 05_by_mne.csv - every mnemonic ships; n_sufficient is a flag, not a filter.
_ctl_all = float(_p.filter("grp = 'mailed_not_unsub' AND baseline_prof_decile = 'ALL'")
                   .select("median_delta").collect()[0][0] or 0.0)
c05 = (panel.filter("unsubbed = 1")
            .withColumn("mne", F.when(F.trim(F.col("unsub_mne")) == "", F.lit("(untagged)")).otherwise(F.col("unsub_mne")))
            .withColumn("delta", F.col("PROF_FOLLOWUP") - F.col("PROF_TOT_ANNUAL"))
            .groupBy(F.col("mne").alias("unsub_mne"))
            .agg(F.count("*").alias("clients_unsub_jul2025"),
                 F.round(100.0 * F.avg("present_followup"), 2).alias("pct_present_jun2026"),
                 q("PROF_TOT_ANNUAL", 0.50).alias("median_prof_baseline"),
                 q("PROF_FOLLOWUP",   0.50).alias("median_prof_followup"),
                 q("delta", 0.50).alias("median_delta"))
            .withColumn("control_median_delta", F.lit(_ctl_all))
            .withColumn("n_sufficient", F.when(F.col("clients_unsub_jul2025") >= 100, "Y").otherwise("N"))
            .orderBy(F.col("clients_unsub_jul2025").desc()))
save(c05, "05_by_mne"); display(c05.toPandas().head(20))

print("\nAll five CSVs under", OUT, "- each is a folder holding one part-*.csv")
