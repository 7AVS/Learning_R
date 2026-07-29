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
#
# CELL LAYOUT - the pull and the analysis are already separate cells in this one notebook:
#   TO ANALYSE   run  [A1] .. [A10]                  <- START HERE. Self-contained.
#   TO PULL      run  [0] [1] [1b] [2] [2b] [2c]     only when you need fresh data. ~25 min.
#
# [A1] defines everything it needs and reads the cohort back from HDFS. Delete everything above it
# if you only ever want to run the analysis.
# =============================================================================

# %% [0] Bootstrap - teradatasql from artifactory; run ONCE per kernel
get_ipython().system("./environment/bin/python -m pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")

# %% [1] Connections + helpers
import getpass, time, warnings
import pandas as pd
import teradatasql
from pyspark.sql import functions as F, Window as W

# pandas only certifies SQLAlchemy connections for read_sql. A raw teradatasql connection works - the
# repo has pulled hundreds of millions of rows through it - but pandas warns once per chunk. Same
# suppression as cpc_reservoir_extract.py.
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

if not hasattr(pd.DataFrame, "iteritems"):
    pd.DataFrame.iteritems = pd.DataFrame.items

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

UCP_BASE  = "/prod/sz/tsz/00172/data/ucp4/"
OUT       = "hdfs:///user/427966379/unsub_value_jul25/"
PULL_OUT  = OUT + "cache/"   # where the completed run actually landed
BASELINE  = "2025-07-31"
FOLLOWUP  = "2026-06-30"

_LOG = []
def log(step, label, source, filt, clients, rows, note=""):
    _LOG.append((step, label, source, filt, clients, rows, note))
    print("[%02d] %-34s clients=%-12s rows=%-12s %s" % (step, label, f"{clients:,}" if isinstance(clients,int) else clients,
                                                        f"{rows:,}" if isinstance(rows,int) else rows, note))

def key_sp(col):
    """UCP CLNT_NO -> same canonical form: trimmed, leading zeros stripped."""
    return F.regexp_replace(F.trim(col.cast("string")), "^0+", "")

def ucp(anchor, fields):
    df = spark.read.option("basePath", UCP_BASE).parquet(UCP_BASE + "MONTH_END_DATE=" + anchor + "/")
    have = set(df.columns)
    missing = [f for f in fields if f not in have]
    if missing:
        raise RuntimeError("UCP %s is missing %s - check references/ucp/field_catalog_personal.md" % (anchor, missing))
    out = df.select(key_sp(F.col("CLNT_NO")).alias("clnt_key"), F.lit(1).alias("in_ucp"),
                    *[F.col(f) for f in fields])
    n_raw = out.count(); n_key = out.select("clnt_key").distinct().count()
    print("UCP %s: %s rows, %s distinct clients%s" % (anchor, f"{n_raw:,}", f"{n_key:,}",
          "" if n_raw == n_key else "  !! NOT unique per client - deduping on clnt_key"))
    if n_raw != n_key:
        w = W.partitionBy("clnt_key").orderBy(F.col(fields[0]).desc_nulls_last())
        out = out.withColumn("_r", F.row_number().over(w)).filter("_r = 1").drop("_r")
    return out.cache()

def land(pdf, name):
    """Cell [2] writes here. Cell [3] reads from here. That is the split - HDFS, not kernel memory."""
    path = PULL_OUT + name
    spark.createDataFrame(pdf).coalesce(8).write.mode("overwrite").option("header", True).csv(path)
    n = spark.read.option("header", True).csv(path).count()
    assert n == len(pdf), "%s readback mismatch: wrote %d read %d" % (name, len(pdf), n)
    print("LANDED %-12s %s rows -> %s" % (name, f"{len(pdf):,}", path))

def save(sdf, name):
    path = OUT + name
    sdf.coalesce(1).write.mode("overwrite").option("header", True).csv(path)
    n = spark.read.option("header", True).csv(path).count()
    print("SAVED %-18s -> %s  (%d rows, readback confirms)" % (name, path, n))

# %% [1b] EDW CONNECTION - only needed for the pull. SKIP THIS CELL to run analysis only.
username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")
EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com", user=username, password=password, logmech="LDAP")

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

raw = _pull_cohort()

n_raw       = len(raw)
n_unsub_any = int((raw["unsubbed"] == 1).sum())
pdf         = raw[raw["mailed"] == 1].copy()
n_all       = len(pdf)
n_uns       = int(pdf["unsubbed"].sum())

land(raw, "cohort_raw")

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

mne = _pull_mne()
land(mne, "unsub_mne")

# %% [2c] DIAG - how many mnemonics does one client touch in the window?
# Two things ride on this. (a) If an unsubscribe carries ONE treatment id, then a client who left via VRE
# but was also mailed by PCL is a "stayer" under PCL - which is only defensible if unsubscribes really are
# programme-scoped, still an open question. If it carries several, they are a leaver everywhere and there
# is no third category to invent. (b) mean n_mne on the mailed side x 7.35M sizes the client x mne pull.
MNE_SPREAD_SQL = """
SELECT 'unsub  (disp 4)' AS which, n_mne, COUNT(*) AS clients FROM (
  SELECT m.CLNT_NO, COUNT(DISTINCT SUBSTR(m.TREATMENT_ID, 8, 3)) AS n_mne
  FROM DTZV01.VENDOR_FEEDBACK_EVENT e
  INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
  WHERE e.disposition_cd = 4
    AND e.disposition_dt_tm >= DATE '2025-07-01' AND e.disposition_dt_tm < DATE '2025-08-01'
    AND m.load_tm >= DATE '2025-06-01' AND m.load_tm < DATE '2025-09-01'
  GROUP BY m.CLNT_NO) x GROUP BY 1, 2
UNION ALL
SELECT 'mailed (disp 1)', n_mne, COUNT(*) FROM (
  SELECT m.CLNT_NO, COUNT(DISTINCT SUBSTR(m.TREATMENT_ID, 8, 3)) AS n_mne
  FROM DTZV01.VENDOR_FEEDBACK_EVENT e
  INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
  WHERE e.disposition_cd = 1
    AND e.disposition_dt_tm >= DATE '2025-07-01' AND e.disposition_dt_tm < DATE '2025-08-01'
    AND m.load_tm >= DATE '2025-06-01' AND m.load_tm < DATE '2025-09-01'
  GROUP BY m.CLNT_NO) y GROUP BY 1, 2
ORDER BY 1, 2
"""
spread = edw_pd(MNE_SPREAD_SQL)
spread["clients"] = spread["clients"].astype("int64")
spread["n_mne"]   = spread["n_mne"].astype("int64")
display(spread)

for _w in spread["which"].unique():
    _s = spread[spread["which"] == _w]
    _tot  = int(_s["clients"].sum())
    _one  = int(_s.loc[_s["n_mne"] == 1, "clients"].sum())
    _pairs = int((_s["n_mne"] * _s["clients"]).sum())
    print("%s  clients=%-12s  exactly 1 mnemonic=%-12s (%5.1f%%)  mean n_mne=%.2f  client-mne pairs=%s"
          % (_w, f"{_tot:,}", f"{_one:,}", 100.0 * _one / _tot, _pairs / float(_tot), f"{_pairs:,}"))
print("\nIf 'unsub' is ~100%% at 1 mnemonic, a client who left via one campaign is NOT a leaver under the")
print("others - decide whether they are a stayer there or a separate 'unsubscribed_elsewhere' bucket.")
print("The mailed row's client-mne pairs is the row count the client x mne pull has to carry.")

# % [A1] ANALYSIS STARTS HERE. SELF-CONTAINED - run this cell first, then A2 to A10.
# Nothing above this line is needed. No imports from [1], no EDW, no password, no pip.
import pandas as pd
from pyspark.sql import functions as F, Window as W

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

UCP_BASE  = "/prod/sz/tsz/00172/data/ucp4/"
OUT       = "hdfs:///user/427966379/unsub_value_jul25/"
PULL_OUT  = OUT + "cache/"   # where the completed run actually landed
BASELINE  = "2025-07-31"
FOLLOWUP  = "2026-06-30"

_LOG = []
def log(step, label, source, filt, clients, rows, note=""):
    _LOG.append((step, label, source, filt, clients, rows, note))
    print("[%02d] %-34s clients=%-12s rows=%-12s %s" % (step, label, f"{clients:,}" if isinstance(clients,int) else clients,
                                                        f"{rows:,}" if isinstance(rows,int) else rows, note))

def key_sp(col):
    return F.regexp_replace(F.trim(col.cast("string")), "^0+", "")

def ucp(anchor, fields):
    df = spark.read.option("basePath", UCP_BASE).parquet(UCP_BASE + "MONTH_END_DATE=" + anchor + "/")
    missing = [f for f in fields if f not in set(df.columns)]
    if missing:
        raise RuntimeError("UCP %s is missing %s" % (anchor, missing))
    out = df.select(key_sp(F.col("CLNT_NO")).alias("clnt_key"), F.lit(1).alias("in_ucp"),
                    *[F.col(f) for f in fields])
    n_raw = out.count(); n_key = out.select("clnt_key").distinct().count()
    print("UCP %s: %s rows, %s distinct clients%s" % (anchor, f"{n_raw:,}", f"{n_key:,}",
          "" if n_raw == n_key else "  !! NOT unique per client - deduping"))
    if n_raw != n_key:
        w = W.partitionBy("clnt_key").orderBy(F.col(fields[0]).desc_nulls_last())
        out = out.withColumn("_r", F.row_number().over(w)).filter("_r = 1").drop("_r")
    return out.cache()

def save(sdf, name):
    path = OUT + name
    sdf.coalesce(1).write.mode("overwrite").option("header", True).csv(path)
    print("SAVED %-18s -> %s  (%d rows)" % (name, path, spark.read.option("header", True).csv(path).count()))

def q(col, p):
    return F.expr("percentile_approx(%s, %s)" % (col, p))

def _read(name):
    """Reads whichever format landed. Earlier runs wrote parquet, later ones CSV; this stops the
    format being one more thing that can cost a re-pull."""
    path = PULL_OUT + name
    for how in ("parquet", "csv"):
        try:
            df = spark.read.parquet(path) if how == "parquet" else spark.read.option("header", True).csv(path)
            n = df.count()
            print("READ %-12s %s rows from %s (%s)" % (name, f"{n:,}", path, how))
            return df
        except Exception as ex:
            print("     %-12s not %s (%s)" % (name, how, type(ex).__name__))
    raise RuntimeError("could not read %s as parquet or csv - list the folder and tell me what is in it" % path)

_c = _read("cohort_raw")
_m = _read("unsub_mne")
cohort = (_c.withColumn("mailed",   F.col("mailed").cast("int"))
            .withColumn("unsubbed", F.col("unsubbed").cast("int"))
            .filter("mailed = 1")
            .join(_m, "clnt_key", "left")
            .withColumn("unsub_mne", F.coalesce(F.col("unsub_mne"), F.lit("")))
            .withColumn("grp", F.when(F.col("unsubbed") == 1, "unsubscribed").otherwise("mailed_not_unsub"))
            .dropDuplicates(["clnt_key"]).cache())
n_cohort = cohort.count()
n_uns    = cohort.filter("unsubbed = 1").count()
_LOG = []
log(1, "mailed in Jul 2025",              "pull/cohort_raw", "mailed = 1",   n_cohort, n_cohort, "THE UNIVERSE")
log(2, "of those, unsubscribed Jul 2025", "pull/cohort_raw", "unsubbed = 1", n_uns,    n_uns,    "treatment group")
log(3, "of those, did not unsubscribe",   "pull/cohort_raw", "unsubbed = 0", n_cohort - n_uns, n_cohort - n_uns, "control group")
print("mnemonic attached to", f'{cohort.filter("unsub_mne <> char(39)char(39)").count():,}' if False else
      f'{cohort.filter(F.col("unsub_mne") != "").count():,}', "of", f"{n_uns:,}", "unsubscribers")

# % [A2] UCP at both anchors. PROOF: the join must not be zero - that is the known CLNT_NO failure mode.
# DEPTH is ACTV_PROD_CNT (how many). BREADTH is T/I/B/C (which lines of business). They are
# different measures, not substitutes: 8 products in one category is not 4 across four.
UF   = ["PROF_TOT_ANNUAL", "TENURE_RBC_YEARS", "ACTV_PROD_CNT", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT"]
base = ucp(BASELINE, UF)
foll = ucp(FOLLOWUP, ["PROF_TOT_ANNUAL", "ACTV_PROD_CNT", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT"])
_REN = {"in_ucp": "in_ucp_followup", "PROF_TOT_ANNUAL": "PROF_FOLLOWUP"}   # everything else gets _F
for _c in ["in_ucp", "PROF_TOT_ANNUAL", "ACTV_PROD_CNT", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT"]:
    foll = foll.withColumnRenamed(_c, _REN.get(_c, _c + "_F"))
assert "PROF_FOLLOWUP" in foll.columns and "ACTV_PROD_CNT_F" in foll.columns, foll.columns

b = cohort.join(base.withColumnRenamed("in_ucp", "in_ucp_baseline"), "clnt_key", "left")
n_row  = b.filter("in_ucp_baseline = 1").count()
n_prof = b.filter(F.col("PROF_TOT_ANNUAL").isNotNull()).count()
assert n_row > 0, "ZERO baseline matches - CLNT_NO normalisation failed, compare key_pd() vs key_sp()"
log(4, "has a UCP row at %s" % BASELINE, "ucp4", "left join on clnt_key", n_row, n_row,
    "%.1f%% of the universe" % (100.0 * n_row / n_cohort))
log(5, "of those, PROF_TOT_ANNUAL is null", "ucp4", "row present, value null", n_row - n_prof, n_row - n_prof,
    "a UCP row with no profitability is NOT the same as no client")
log(6, "no UCP row at all at %s" % BASELINE, "ucp4", "-", n_cohort - n_row, n_cohort - n_row,
    "outside UCP personal - check CLNT_TYP")

# % [A3] Baseline deciles, cut across BOTH groups together so the bands mean the same thing on each side.
# ntile MUST run over non-null rows only. asc_nulls_last still ranks the nulls, so they land in the top
# tiles and eat them: the first run put 630,531 nulls into decile 10, leaving it 104,609 real clients
# against ~733,000 in every other decile - the top 1.5% of the distribution wearing a "decile 10" label.
_nn  = b.filter(F.col("PROF_TOT_ANNUAL").isNotNull())
_dec = _nn.withColumn("dec", F.ntile(10).over(W.orderBy(F.col("PROF_TOT_ANNUAL").asc()))).select("clnt_key", "dec")
b = (b.join(_dec, "clnt_key", "left")
      .withColumn("baseline_prof_decile", F.coalesce(F.col("dec").cast("string"), F.lit("no_baseline_ucp")))
      .drop("dec"))
_chk = (b.filter("baseline_prof_decile <> 'no_baseline_ucp'").groupBy("baseline_prof_decile")
          .count().toPandas()["count"])
print("decile sizes: min %s max %s spread %.2f%%" % (f"{_chk.min():,}", f"{_chk.max():,}",
      100.0 * (_chk.max() - _chk.min()) / _chk.mean()))
assert (_chk.max() - _chk.min()) / _chk.mean() < 0.01, "deciles are not equal-sized - ntile is picking up nulls again"
# STILL WITH US = has a UCP row at follow-up. NOT "has a non-null profitability" - a client can hold a
# row with a null PROF_TOT_ANNUAL and is plainly still a client. Testing the value inflates attrition.
panel = (b.join(foll, "clnt_key", "left")
           .withColumn("present_followup", F.coalesce(F.col("in_ucp_followup"), F.lit(0)))
           .withColumn("prod_delta", F.col("ACTV_PROD_CNT_F") - F.col("ACTV_PROD_CNT"))).cache()
n_present = panel.filter("present_followup = 1").count()
n_pf      = panel.filter(F.col("PROF_FOLLOWUP").isNotNull()).count()
log(7, "has a UCP row at %s" % FOLLOWUP, "ucp4", "left join on clnt_key", n_present, n_present,
    "%.1f%% still present - THIS is the attrition denominator" % (100.0 * n_present / n_cohort))
log(8, "of those, PROF_TOT_ANNUAL is null", "ucp4", "row present, value null", n_present - n_pf,
    n_present - n_pf, "present but unpriced")
display(panel.groupBy("grp").agg(F.count("*").alias("clients"),
        F.sum("present_followup").alias("present_followup")).toPandas())

# % [A4] BREADTH. n_cats = how many of Transaction / Investment / Borrow / Credit carry a non-zero count.
# Depth and breadth move independently - a client can shed products without leaving a line of business,
# or leave one entirely while the total barely moves. Both are needed to read "deepened the relationship".
def _cats(cols):
    return sum([F.when(F.coalesce(F.col(c), F.lit(0)) > 0, 1).otherwise(0) for c in cols])
_B = ["T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT"]
panel = (panel
    .withColumn("n_cats_base", _cats(_B))
    .withColumn("n_cats_foll", _cats([c + "_F" for c in _B]))
    .withColumn("cats_delta",  F.col("n_cats_foll") - F.col("n_cats_base"))
    .withColumn("lost_a_category", F.when(F.col("cats_delta") < 0, 1).otherwise(0))
    .withColumn("exited_cards", F.when((F.coalesce(F.col("C_TOT_CNT"), F.lit(0)) > 0) &
                                       (F.coalesce(F.col("C_TOT_CNT_F"), F.lit(0)) == 0), 1).otherwise(0))
    .cache())
display(panel.filter("present_followup = 1 AND in_ucp_baseline = 1")
             .groupBy("grp").agg(F.count("*").alias("clients"),
                                 F.round(F.avg("n_cats_base"), 3).alias("mean_cats_baseline"),
                                 F.round(F.avg("n_cats_foll"), 3).alias("mean_cats_followup"),
                                 F.round(100.0 * F.avg("lost_a_category"), 2).alias("pct_lost_a_category"),
                                 F.round(100.0 * F.avg("exited_cards"), 2).alias("pct_exited_cards")).toPandas())

# % [A5] 01_cohort.csv - the audit trail
c01 = spark.createDataFrame(pd.DataFrame(_LOG, columns=["step_no","step_label","source","filter_applied",
                                                        "clients_remaining","rows_remaining","note"]))
save(c01, "01_cohort")

# % [A6] 02_balance.csv - were the two groups comparable at baseline
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

# % [A7] 03_attrition.csv - OUTCOME 1. Nobody dropped. Counts beside the rate.
_att = (panel.groupBy("baseline_prof_decile", "grp")
             .agg(F.count("*").alias("clients_at_baseline"),
                  F.sum("present_followup").alias("clients_present_jun2026"))
             .withColumn("clients_absent_jun2026", F.col("clients_at_baseline") - F.col("clients_present_jun2026"))
             .withColumn("pct_present", F.round(100.0 * F.col("clients_present_jun2026") / F.col("clients_at_baseline"), 2))
             .withColumn("pct_attrited", F.round(100.0 * F.col("clients_absent_jun2026") / F.col("clients_at_baseline"), 2)))
_all = (panel.groupBy("grp").agg(F.count("*").alias("clients_at_baseline"),
                                 F.sum("present_followup").alias("clients_present_jun2026"))
             .withColumn("clients_absent_jun2026", F.col("clients_at_baseline") - F.col("clients_present_jun2026"))
             .withColumn("pct_present", F.round(100.0 * F.col("clients_present_jun2026") / F.col("clients_at_baseline"), 2))
             .withColumn("pct_attrited", F.round(100.0 * F.col("clients_absent_jun2026") / F.col("clients_at_baseline"), 2))
             .withColumn("baseline_prof_decile", F.lit("ALL")))
c03 = _att.unionByName(_all.select(_att.columns)).orderBy("baseline_prof_decile", "grp")
save(c03, "03_attrition"); display(c03.toPandas())

# % [A8] 04_profit.csv - OUTCOME 2. Among clients present in BOTH partitions.
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

# % [A9] 05_by_mne.csv - every mnemonic ships; n_sufficient is a flag, not a filter.
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
save(c05, "05_by_mne"); display(c05.toPandas())   # full table - the CSV has the same 107 rows

# % [A10] 06_relationship.csv - DEPTH and BREADTH side by side, the pair that answers "did they deepen
# or shrink the relationship". Depth alone cannot tell 8 cards in one category from 4 products across
# four; breadth alone cannot tell how much sits inside each. Neither is a substitute for the other.
def rel(gcols):
    return (panel.filter("present_followup = 1 AND in_ucp_baseline = 1").groupBy(*gcols).agg(
        F.count("*").alias("clients_both_partitions"),
        q("ACTV_PROD_CNT",   0.50).alias("median_depth_baseline"),
        q("ACTV_PROD_CNT_F", 0.50).alias("median_depth_followup"),
        F.round(F.avg("prod_delta"), 3).alias("mean_depth_delta"),
        F.round(F.avg("n_cats_base"), 3).alias("mean_breadth_baseline"),
        F.round(F.avg("n_cats_foll"), 3).alias("mean_breadth_followup"),
        F.round(F.avg("cats_delta"), 4).alias("mean_breadth_delta"),
        F.round(100.0 * F.avg("lost_a_category"), 2).alias("pct_lost_a_category"),
        F.round(100.0 * F.avg("exited_cards"), 2).alias("pct_exited_cards")))
_r  = rel(["baseline_prof_decile", "grp"])
_ra = rel(["grp"]).withColumn("baseline_prof_decile", F.lit("ALL")).select(_r.columns)
_r  = _r.unionByName(_ra)
_rc = _r.filter("grp = 'mailed_not_unsub'").select("baseline_prof_decile",
        F.col("mean_breadth_delta").alias("_b"), F.col("mean_depth_delta").alias("_d"))
c06 = (_r.join(_rc, "baseline_prof_decile", "left")
         .withColumn("breadth_delta_vs_control", F.when(F.col("grp") == "unsubscribed",
                     F.round(F.col("mean_breadth_delta") - F.col("_b"), 4)))
         .withColumn("depth_delta_vs_control", F.when(F.col("grp") == "unsubscribed",
                     F.round(F.col("mean_depth_delta") - F.col("_d"), 3)))
         .drop("_b", "_d").orderBy("baseline_prof_decile", "grp"))
save(c06, "06_relationship"); display(c06.toPandas())

print("\nAll six CSVs under", OUT, "- each is a folder holding one part-*.csv")
