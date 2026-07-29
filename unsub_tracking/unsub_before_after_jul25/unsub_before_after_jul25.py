# UNSUB BEFORE-AND-AFTER - JULY 2025 COHORT, 11-MONTH FOLLOW-UP
#
# NOT the same as unsub_tracking/museum/unsub_value_museum.py. Tell them apart by anchor count:
#   museum        ONE UCP anchor (2026-02-28)  -> a snapshot. WHO unsubscribes.
#   this file     TWO UCP anchors (2025-07-31, 2026-06-30) -> a change. WHAT HAPPENS AFTER.
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
# 5. NOT CAUSAL. Treatment is self-selected: people unsubscribe BECAUSE they are disengaging. Decile
#    stratification removes baseline skew and regression to the mean; it does not remove that. Every
#    delta_vs_control in this file is an association, and must be reported as one.
#
# ===================== THE THREE CONFOUND CHECKS (added 2026-07-29) ==========
# Every finding above rests on the assumption that the two groups differ in that they unsubscribed
# and in nothing else that matters. These three test that assumption, and the first one can overturn
# the headline on its own.
#
# C1  CONTACT VOLUME.  Were unsubscribers simply mailed harder? If so the divergence is a
#     contact-intensity effect wearing an unsubscribe label. -> 07_contact.csv, and the DiD is
#     re-cut by contact band so you can see whether the gap survives inside a band.
# C2  ENGAGEMENT.      Did they open and click before leaving? "Losing the listeners, keeping the
#     deaf" - an engaged leaver is a different loss from a dormant one. -> 08_engagement.csv
# C3  ALREADY OUT.     Clients mailed in Jul 2025 despite unsubscribing BEFORE it. They never chose
#     to stay, so they do not belong in the control group. -> 09_already_out.csv
#
# C1 and C2 cost NOTHING extra to pull: cell [2] already scans EVENT x MASTER over this window, and
# they are additional aggregates on that same scan. C3 needs its own scan - a different date range.
# =============================================================================
#
# CELL LAYOUT - the pull and the analysis are already separate cells in this one notebook:
#   TO ANALYSE   run  [A1] .. [A13]                      <- START HERE. Self-contained.
#   TO PULL      run  [0] [1] [1b] [2] [2b] [2c] [2d]    only when you need fresh data. ~25 min.
#
# A1 detects whether the cache carries the C1/C2 columns and whether prior_unsub landed, and says so
# at the top. A11-A13 skip cleanly if it does not; A1-A10 never depend on them.
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
OUT       = "hdfs:///user/427966379/unsub_value_jul25/"   # HDFS path unchanged - the pull already landed here
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
#
# C1 and C2 ride on this scan. disposition_cd widens from (1,4) to (1,2,3,4) and six aggregates are
# added; the join, the window and the bites are untouched. One COUNT(DISTINCT) only - [2c] already
# runs one over the same window without spooling, so the cost class is proven. The rest are SUMs.
#   1 = sent   2 = opened   3 = clicked   4 = unsubscribed
COHORT_TMPL = """
SELECT m.CLNT_NO,
       MAX(CASE WHEN e.disposition_cd = 1 THEN 1 ELSE 0 END) AS mailed,
       MAX(CASE WHEN e.disposition_cd = 4 THEN 1 ELSE 0 END) AS unsubbed,
       SUM(CASE WHEN e.disposition_cd = 1 THEN 1 ELSE 0 END) AS n_send_events,
       COUNT(DISTINCT CASE WHEN e.disposition_cd = 1 THEN e.TREATMENT_ID END) AS n_treatments,
       SUM(CASE WHEN e.disposition_cd = 2 THEN 1 ELSE 0 END) AS n_opens,
       SUM(CASE WHEN e.disposition_cd = 3 THEN 1 ELSE 0 END) AS n_clicks,
       MAX(CASE WHEN e.disposition_cd = 2 THEN 1 ELSE 0 END) AS opened,
       MAX(CASE WHEN e.disposition_cd = 3 THEN 1 ELSE 0 END) AS clicked
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd IN (1, 2, 3, 4)
  AND e.disposition_dt_tm >= DATE '2025-07-01' AND e.disposition_dt_tm < DATE '2025-08-01'
  AND m.load_tm >= DATE '2025-06-01' AND m.load_tm < DATE '2025-09-01'
  AND ABS(m.CLNT_NO) MOD 10 = %d
GROUP BY m.CLNT_NO
"""
COHORT_COLS = ["mailed", "unsubbed", "n_send_events", "n_treatments", "n_opens", "n_clicks",
               "opened", "clicked"]
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
    for _col in COHORT_COLS:
        out[_col] = pd.to_numeric(out[_col], errors="coerce").fillna(0).astype("int32")
    return out[["clnt_key"] + COHORT_COLS]

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

# C1/C2 first look, straight off the pull - before any UCP join can be blamed for it. If the two
# n_send_events means are far apart, the DiD downstream is measuring contact intensity as much as
# unsubscribing, and 07_contact.csv is the table that decides it.
print("\nC1/C2 at the pull:")
display(pdf.groupby(pdf["unsubbed"].map({1: "unsubscribed", 0: "mailed_not_unsub"}))
        .agg(clients=("clnt_key", "size"), mean_sends=("n_send_events", "mean"),
             mean_treatments=("n_treatments", "mean"), pct_opened=("opened", "mean"),
             pct_clicked=("clicked", "mean")).round(3))

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

# %% [2d] C3 - PRIOR UNSUBSCRIBERS. Clients who unsubscribed BEFORE July 2025 and were mailed in it
# anyway. This is the only one of the three that needs its own scan: a different date range, so it
# cannot ride on [2]. Floored at 2024-01-01 per the repo's data floor - an 18-month look-back is
# enough to separate "left recently and was mailed anyway" from "left years ago".
#
# Why it matters to the estimator and not just to compliance: a client who opted out in March 2025
# and got mail in July 2025 is sitting in the CONTROL group labelled "chose not to unsubscribe".
# They chose nothing. They were already gone. Leaving them in makes the control look worse than it
# is, which biases every delta_vs_control in the optimistic direction.
PRIOR_SQL = """
SELECT m.CLNT_NO,
       MAX(CAST(e.disposition_dt_tm AS DATE)) AS last_prior_unsub_dt,
       COUNT(*) AS n_prior_unsub_events
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2024-01-01' AND e.disposition_dt_tm < DATE '2025-07-01'
  AND m.load_tm >= DATE '2023-12-01' AND m.load_tm < DATE '2025-08-01'
  AND ABS(m.CLNT_NO) MOD 5 = %d
GROUP BY m.CLNT_NO
"""


def _pull_prior():
    bites = []
    for i in range(5):
        print("prior bite %d/5" % (i + 1), flush=True)
        bites.append(edw_pd(PRIOR_SQL % i))
    out = pd.concat(bites, ignore_index=True)
    out["clnt_key"] = key_pd(out["CLNT_NO"], "prior")
    out = out[out["clnt_key"].notna()].copy()
    out["clnt_key"] = out["clnt_key"].astype(str)
    out["last_prior_unsub_dt"] = pd.to_datetime(out["last_prior_unsub_dt"]).dt.strftime("%Y-%m-%d")
    out["n_prior_unsub_events"] = pd.to_numeric(out["n_prior_unsub_events"],
                                                errors="coerce").fillna(0).astype("int32")
    return out[["clnt_key", "last_prior_unsub_dt", "n_prior_unsub_events"]]


prior = _pull_prior()
land(prior, "prior_unsub")
print("\nprior_unsub: %s clients unsubscribed between 2024-01-01 and 2025-07-01." % f"{len(prior):,}")

# The overlap with the cohort is the actual finding, but it needs [2] in the same kernel. This cell
# has to stand alone - the whole point of the pull/analysis split - so it reports only if [2] ran.
# The full version, off HDFS and independent of kernel state, is [A13].
if "pdf" in dir():
    _ov = pdf.merge(prior[["clnt_key"]], on="clnt_key", how="inner")
    print("C3: %s of %s mailed clients (%.2f%%) had ALREADY unsubscribed before Jul 2025 and were "
          "mailed anyway." % (f"{len(_ov):,}", f"{len(pdf):,}", 100.0 * len(_ov) / len(pdf)))
    print("    Of those, %s sit in the CONTROL group." % f"{int((_ov['unsubbed'] == 0).sum()):,}")
else:
    print("       (run [2] in this kernel for the cohort overlap, or just run [A13])")

# % [A1] ANALYSIS STARTS HERE. SELF-CONTAINED - run this cell first, then A2 to A10.
# Nothing above this line is needed. No imports from [1], no EDW, no password, no pip.
import pandas as pd
from pyspark.sql import functions as F, Window as W

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

UCP_BASE  = "/prod/sz/tsz/00172/data/ucp4/"
OUT       = "hdfs:///user/427966379/unsub_value_jul25/"   # HDFS path unchanged - the pull already landed here
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

# The three confound checks need columns cell [2] only started pulling on 2026-07-29. A cache landed
# before that read back fine and then fails 200 lines down inside a groupBy. Decide it HERE, name the
# missing columns, and let the analysis run without them rather than die at A11.
_C12_COLS = ["n_send_events", "n_treatments", "n_opens", "n_clicks", "opened", "clicked"]
_missing12 = [c for c in _C12_COLS if c not in _c.columns]
HAVE_C12 = not _missing12
if not HAVE_C12:
    print("!! C1/C2 UNAVAILABLE - cohort_raw is missing", _missing12)
    print("   That cache predates the contact/engagement columns. Re-run cell [2] to get them.")
    print("   A1-A10 run normally; A11 and A12 will skip.")

try:
    _pr = _read("prior_unsub").select("clnt_key",
                                      F.col("last_prior_unsub_dt"),
                                      F.col("n_prior_unsub_events").cast("int"))
    HAVE_C3 = True
except Exception:
    _pr, HAVE_C3 = None, False
    print("!! C3 UNAVAILABLE - no prior_unsub landing. Run cell [2d]. A13 will skip.")

cohort = (_c.withColumn("mailed",   F.col("mailed").cast("int"))
            .withColumn("unsubbed", F.col("unsubbed").cast("int")))
for _col in (_C12_COLS if HAVE_C12 else []):
    cohort = cohort.withColumn(_col, F.coalesce(F.col(_col).cast("int"), F.lit(0)))

cohort = (cohort.filter("mailed = 1")
            .join(_m, "clnt_key", "left")
            .withColumn("unsub_mne", F.coalesce(F.col("unsub_mne"), F.lit("")))
            .withColumn("grp", F.when(F.col("unsubbed") == 1, "unsubscribed").otherwise("mailed_not_unsub")))

if HAVE_C3:
    cohort = (cohort.join(_pr, "clnt_key", "left")
              .withColumn("already_out",
                          F.when(F.col("last_prior_unsub_dt").isNotNull(), 1).otherwise(0))
              # _read gives CSV, so the date arrives as a string - datediff on it returns null
              .withColumn("days_since_prior_unsub",
                          F.datediff(F.to_date(F.lit("2025-07-01")),
                                     F.to_date(F.col("last_prior_unsub_dt")))))
else:
    cohort = cohort.withColumn("already_out", F.lit(0))

if HAVE_C12:
    # Contact bands, not raw counts: the DiD has to be re-cut INSIDE a band, and a band needs enough
    # clients to hold a decile split. Cut points are ours - edit here and nowhere else.
    cohort = cohort.withColumn(
        "contact_band",
        F.when(F.col("n_send_events") <= 1, "1")
         .when(F.col("n_send_events") <= 3, "2-3")
         .when(F.col("n_send_events") <= 6, "4-6")
         .when(F.col("n_send_events") <= 12, "7-12").otherwise("13+"))
    cohort = cohort.withColumn("engaged", F.when((F.col("opened") == 1) | (F.col("clicked") == 1),
                                                 1).otherwise(0))

cohort = cohort.dropDuplicates(["clnt_key"]).cache()
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
# The residual bucket used to be one label, "no_baseline_ucp", and that label was FALSE for most of
# the clients in it. dec is null when PROF_TOT_ANNUAL is null - which is two different populations:
# a client with no UCP row at all, and a client who HAS a row carrying a null profitability. Log
# line 5 above already draws that distinction in words ("a UCP row with no profitability is NOT the
# same as no client") and this line then collapsed it under the name of only one of them.
#
# It showed up in 06_relationship, which filters to in_ucp_baseline = 1: 267 clients appeared under
# "no_baseline_ucp" in a table where every row provably HAS a baseline UCP row. Same numbers, wrong
# name. Split, so the label says which one.
b = (b.join(_dec, "clnt_key", "left")
      .withColumn("baseline_prof_decile",
                  F.when(F.col("dec").isNotNull(), F.col("dec").cast("string"))
                   .when(F.col("in_ucp_baseline").isNull(), F.lit("no_ucp_row"))
                   .otherwise(F.lit("ucp_row_null_prof")))
      .drop("dec"))
NON_DECILE = ["no_ucp_row", "ucp_row_null_prof"]
_chk = (b.filter(~F.col("baseline_prof_decile").isin(*NON_DECILE)).groupBy("baseline_prof_decile")
          .count().toPandas()["count"])
print("decile sizes: min %s max %s spread %.2f%%" % (f"{_chk.min():,}", f"{_chk.max():,}",
      100.0 * (_chk.max() - _chk.min()) / _chk.mean()))
assert (_chk.max() - _chk.min()) / _chk.mean() < 0.01, "deciles are not equal-sized - ntile is picking up nulls again"
display(b.filter(F.col("baseline_prof_decile").isin(*NON_DECILE))
         .groupBy("baseline_prof_decile", "grp").count()
         .orderBy("baseline_prof_decile", "grp").toPandas())
print("no_ucp_row        = not in UCP personal at all at", BASELINE, "- no attributes exist.")
print("ucp_row_null_prof = IS a UCP client, profitability is null. Depth and breadth ARE measurable")
print("                    for these, which is why they appear in 06_relationship.")
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

# % [A11] 07_contact.csv - C1, CONTACT VOLUME. The check that can overturn the headline.
#
# Two questions, in order, and the second only matters if the first says yes:
#   (a) Were unsubscribers mailed harder than controls? -> the top block.
#   (b) If they were, does the profitability gap SURVIVE inside a contact band? -> the bottom block.
# If delta_vs_control collapses toward zero once contact is held fixed, the finding was contact
# intensity all along. If it holds inside every band, contact is not the explanation and the
# unsubscribe signal stands on its own.
if not HAVE_C12:
    print("A11 skipped - C1/C2 columns are not in this cache. Re-run cell [2].")
else:
    _ct = (panel.groupBy("grp").agg(
        F.count("*").alias("clients"),
        F.round(F.avg("n_send_events"), 3).alias("mean_send_events"),
        q("n_send_events", 0.50).alias("median_send_events"),
        q("n_send_events", 0.90).alias("p90_send_events"),
        F.round(F.avg("n_treatments"), 3).alias("mean_distinct_treatments")))
    display(_ct.toPandas())

    # the DiD, re-cut by contact band instead of by decile
    _cb = both.groupBy("contact_band", "grp").agg(
        F.count("*").alias("clients_both_partitions"),
        q("PROF_TOT_ANNUAL", 0.50).alias("median_prof_baseline"),
        q("delta", 0.50).alias("median_delta"))
    _cba = (both.groupBy("grp").agg(
        F.count("*").alias("clients_both_partitions"),
        q("PROF_TOT_ANNUAL", 0.50).alias("median_prof_baseline"),
        q("delta", 0.50).alias("median_delta"))
        .withColumn("contact_band", F.lit("ALL")).select(_cb.columns))
    _cb = _cb.unionByName(_cba)
    _cbc = _cb.filter("grp = 'mailed_not_unsub'").select("contact_band",
                                                         F.col("median_delta").alias("_c"))
    # attrition rides along - a contact band that loses clients outright matters as much as one that
    # loses dollars, and 03_attrition cannot show it because it is cut by decile.
    _cbat = (panel.groupBy("contact_band", "grp")
             .agg(F.round(100.0 * F.avg("present_followup"), 2).alias("pct_present_jun2026")))
    c07 = (_cb.join(_cbc, "contact_band", "left")
             .join(_cbat, ["contact_band", "grp"], "left")
             .withColumn("delta_vs_control", F.when(F.col("grp") == "unsubscribed",
                                                    F.round(F.col("median_delta") - F.col("_c"), 2)))
             .drop("_c").orderBy("contact_band", "grp"))
    save(c07, "07_contact"); display(c07.toPandas())
    print("READ THIS AS: if delta_vs_control is flat across bands, contact volume is NOT the "
          "explanation. If it shrinks toward 0 as the band widens, it is.")

# % [A12] 08_engagement.csv - C2, ENGAGEMENT. "Losing the listeners, keeping the deaf."
#
# An unsubscribe from someone who never opened an email is a list-hygiene event. An unsubscribe from
# someone who opened and clicked is a client telling you something. They are not the same loss and
# should not share a number. This splits every outcome by whether the client engaged before leaving.
if not HAVE_C12:
    print("A12 skipped - C1/C2 columns are not in this cache. Re-run cell [2].")
else:
    _eng = (panel.groupBy("grp").agg(
        F.count("*").alias("clients"),
        F.round(100.0 * F.avg("opened"), 2).alias("pct_opened"),
        F.round(100.0 * F.avg("clicked"), 2).alias("pct_clicked"),
        F.round(100.0 * F.avg("engaged"), 2).alias("pct_engaged"),
        F.round(F.avg("n_opens"), 3).alias("mean_opens"),
        F.round(F.avg("n_clicks"), 3).alias("mean_clicks")))
    display(_eng.toPandas())

    _e = (panel.withColumn("engagement",
                           F.when(F.col("clicked") == 1, "clicked")
                            .when(F.col("opened") == 1, "opened_not_clicked")
                            .otherwise("never_opened"))
          .groupBy("engagement", "grp").agg(
              F.count("*").alias("clients"),
              F.round(100.0 * F.avg("present_followup"), 2).alias("pct_present_jun2026"),
              q("PROF_TOT_ANNUAL", 0.50).alias("median_prof_baseline")))
    _ed = (both.withColumn("engagement",
                           F.when(F.col("clicked") == 1, "clicked")
                            .when(F.col("opened") == 1, "opened_not_clicked")
                            .otherwise("never_opened"))
           .groupBy("engagement", "grp").agg(q("delta", 0.50).alias("median_delta")))
    _edc = (_ed.filter("grp = 'mailed_not_unsub'")
            .select("engagement", F.col("median_delta").alias("_c")))
    c08 = (_e.join(_ed, ["engagement", "grp"], "left")
             .join(_edc, "engagement", "left")
             .withColumn("delta_vs_control", F.when(F.col("grp") == "unsubscribed",
                                                    F.round(F.col("median_delta") - F.col("_c"), 2)))
             .drop("_c").orderBy("engagement", "grp"))
    save(c08, "08_engagement"); display(c08.toPandas())
    print("READ THIS AS: compare the 'clicked' unsubscriber row against 'never_opened'. If the "
          "engaged leaver is worth more and attrites harder, that is the expensive segment.")

# % [A13] 09_already_out.csv - C3, THE CLIENTS WHO NEVER CHOSE TO STAY.
#
# Mailed in Jul 2025 despite unsubscribing before it. They are in the control group labelled "did not
# unsubscribe", which is false - they had already left and were mailed anyway. Every delta_vs_control
# in 04_profit and 06_relationship carries them.
#
# This cell measures the contamination; it does NOT silently re-cut the estimates. If the control
# group is materially contaminated, that is a decision about the design and it is Andre's to make -
# the honest fix is to exclude them and re-run, not to patch a number in place.
if not HAVE_C3:
    print("A13 skipped - no prior_unsub landing. Run cell [2d].")
else:
    _ao = (panel.groupBy("grp", "already_out").agg(
        F.count("*").alias("clients"),
        F.round(100.0 * F.avg("present_followup"), 2).alias("pct_present_jun2026"),
        q("PROF_TOT_ANNUAL", 0.50).alias("median_prof_baseline"),
        q("days_since_prior_unsub", 0.50).alias("median_days_since_prior_unsub")))
    _aod = (both.groupBy("grp", "already_out").agg(q("delta", 0.50).alias("median_delta")))
    c09 = _ao.join(_aod, ["grp", "already_out"], "left").orderBy("grp", "already_out")
    save(c09, "09_already_out"); display(c09.toPandas())

    _n_ctl = panel.filter("grp = 'mailed_not_unsub'").count()
    _n_ctl_out = panel.filter("grp = 'mailed_not_unsub' AND already_out = 1").count()
    _pct = 100.0 * _n_ctl_out / _n_ctl if _n_ctl else 0.0
    print("\nCONTROL GROUP CONTAMINATION: %s of %s controls (%.2f%%) had already unsubscribed "
          "before Jul 2025." % (f"{_n_ctl_out:,}", f"{_n_ctl:,}", _pct))
    print("VERDICT:", "material - exclude them and re-run before quoting any delta_vs_control."
          if _pct >= 1.0 else
          "immaterial at this size - the estimates stand, but state the number.")

    # per-mnemonic: WHICH programmes mailed people who had already opted out
    c09b = (panel.filter("already_out = 1")
            .withColumn("mne", F.when(F.trim(F.col("unsub_mne")) == "", F.lit("(untagged)"))
                        .otherwise(F.col("unsub_mne")))
            .groupBy(F.col("mne").alias("prior_unsub_mne"))
            .agg(F.count("*").alias("clients_mailed_after_unsub"),
                 q("days_since_prior_unsub", 0.50).alias("median_days_after_unsub"))
            .orderBy(F.col("clients_mailed_after_unsub").desc()))
    save(c09b, "09b_already_out_by_mne"); display(c09b.limit(25).toPandas())

print("\nCSVs under", OUT, "- each is a folder holding one part-*.csv")
print("  01_cohort  02_balance  03_attrition  04_profit  05_by_mne  06_relationship")
print("  07_contact (C1)  08_engagement (C2)  09_already_out + 09b_by_mne (C3)")
