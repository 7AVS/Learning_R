# %% [0] HEADER
# UNSUB VALUE MUSEUM - "the value of an unsubscribe", auditable and reproducible end to end.
#
# PHASE 1 REWRITE (Andre, 2026-07-27): the bank-wide SEND pull is KILLED. The probe run showed
# 143M send events / ~99M distinct client-MNE pairs across Jan-May, hours of pandas transfer over
# teradatasql, and ~80% of that volume was campaign detail for programs entirely outside cards.
# That is not a "chunk it smaller" problem - it is the wrong shape of pull for what this file can
# actually afford. Work is now split explicitly into two phases:
#   PHASE 1 (this file, now)  - LEAVERS ONLY. Every unsubscriber, bank-wide, cards and non-cards,
#                                12 months. No send pull, no stayer pool, no denominator. Volume
#                                and composition only - "who leaves, and what do they look like."
#   PHASE 2 (later, stubbed at the bottom of this file, no code yet) - STAYERS, scoped to CARDS
#                                MNEs only (the cards Q2 send pull was 6.3M rows - tractable; the
#                                bank-wide version was not). Adds denominators, and therefore rates.
#
# PURPOSE: pulls unsub events STRAIGHT FROM EDW (no reservoir dependency - unlike
# cpc_reservoir_extract.py, this file owns its own Teradata pulls) and client attributes from HDFS
# UCP. There is no EDW client-attribute table - UCP is the only source for age/tenure/product-
# count/profitability, so this file necessarily spans two engines: Teradata (EDW, vendor feedback)
# and Spark/YARN (HDFS, UCP + this file's own landings).
# SUPERSEDES archaeology/28_unsub_value_ucp.py's V1 (bank-wide leaver profile) with a longer,
# bank-wide-by-construction window; borrows its CARDS_MNES constant, its MNE/program mapping
# (including the DEFAULT stream), and its Julian TREATMENT_ID launch-date decode + validation
# pattern almost verbatim.
#
# SCOPE (Andre, 2026-07-27):
#   - Window: unsubs (disposition_cd = 4) with disposition_dt_tm >= 2025-07-01 AND < 2026-07-01 -
#     TWELVE months, not the send side's five. Leavers are cheap to pull (no 143M-row send scan
#     behind them) and a full year gives a bigger, more stable sample per campaign than five months
#     would. The unsub_tm of each client's attributed event is CARRIED IN THE OUTPUT (leaver_spine),
#     so Andre can cut any sub-window (a quarter, a single month) later by filtering that column -
#     WITHOUT re-pulling EDW.
#   - Bank-wide, ALL programs, no MNE filter anywhere in the SQL - the whole point of Phase 1 is to
#     see where the bank loses clients before deciding what to do about the cards slice of it.
#   - FOUR client variables only: age, tenure, TIBC product counts, PROF_TOT_ANNUAL. No scope creep.
#   - Own HDFS namespace: hdfs:///user/427966379/unsub_value_museum/ - unchanged from the prior
#     version of this file, must not collide with unsub_cpc/ (cpc_reservoir_extract.py's namespace).
#
# GRAIN: one row per client. A client can log several qualifying unsub events (different
# treatments, different times); this file keeps only their LAST one (row_number over CLNT_NO
# ordered by unsub_tm desc, TREATMENT_ID desc as tie-break) and calls that treatment their
# attribution - the campaign whose link they clicked last is the one credited/blamed. How many
# clients had more than one qualifying event is printed before the collapse (cell [6]), not thrown
# away silently.
#
# NO RATES ANYWHERE IN THIS FILE. Every output table below is volume and composition - counts,
# medians, percentage-of-leavers shares - never a rate against a denominator, because there is no
# denominator (no stayer pool) until Phase 2 exists. Every printed/saved table repeats this in its
# own label; this header states it once for the whole file.
#
# THE OUTPUT TABLES:
#   M0 - COVERAGE & NULLS. Prints before any finding - proves or kills the "who's unmatched in UCP"
#        question before trusting anything downstream of it.
#   L1 - WHERE: leaver volume and composition by program then top-20 MNE.
#   L2 - WHO: cards vs non-cards leavers, composition-vs-composition (no denominator needed for
#        this one - it is a legitimate comparison of two populations against each other).
#   L3 - DOOR-CLOSING: single-product leavers, i.e. clients who close the ONLY cross-sell channel
#        cards had into every other product they don't hold.
#   L4 - HIGH POTENTIAL LOST: count/share of leavers meeting the high_potential definition.
#   L5 - PROF VETTING: reconfirms PROF_TOT_ANNUAL is current-year contribution, not LTV.
#   CUBE - mne x program x age_band x tenure_band x prod_band x prof_quintile, counts only, for
#        Excel pivoting. Dual-saved (xlsx download + HDFS parquet).
#   leaver_spine - THE durable artifact, client grain, one row each. Every table above re-derives
#        from this alone, without touching EDW or UCP again.
#
# OUTPUT CONTRACT: printed tables carry counts and medians, never a bare "done". Every printed
# number is backed by a server round-trip or an assertion (house hard rule, unchanged).
#
# DURABLE vs SCRATCH: leaver_spine and cube (both saved in cell [19]; cube is ALSO downloaded as
# unsub_value_cube.xlsx via the Jupyter file browser) are the DURABLE artifacts. The raw EDW
# landing (unsubs_12m/q1..q4) is SCRATCH - it exists only so a kernel death mid-pull doesn't cost a
# re-pull from EDW, and is removable once leaver_spine and cube are verified landed (opt-in
# CLEANUP cell, updated for this file's actual pull paths).
#
# Engine split: Teradata-direct (DTZV01.VENDOR_FEEDBACK_EVENT / _MASTER, EDW pulls in cells
# [3]-[4]) feeds this file's OWN HDFS reservoir; everything from cell [5] onward is Spark/YARN
# only, reading this file's landings plus UCP (/prod/sz/tsz/00172/data/ucp4/).
#
# Date: 2026-07-27.

# %% [1] SETUP - bootstrap, EDW connect, land()/landed(), T(), norm_clnt. Unchanged from the prior
# version of this file (cloned originally from cpc_reservoir_extract.py + cpc_evidence_hdfs.py).

# Bootstrap - install teradatasql from RBC artifactory; run ONCE per kernel.
get_ipython().system("./environment/bin/python -m pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")

import getpass
import hashlib
import re
import pandas as pd
import teradatasql
from IPython.display import display, Markdown
from pyspark.sql import functions as F, Window

# pandas>=2.0 removed DataFrame.iteritems but Spark 3.3's createDataFrame still calls it
if not hasattr(pd.DataFrame, "iteritems"):
    pd.DataFrame.iteritems = pd.DataFrame.items

# Same class of clash on the numpy side: numpy>=1.24 removed the np.bool/np.object/np.int/np.float
# aliases, but Spark 3.3's pandas->Spark conversion (pyspark/sql/pandas/conversion.py) still
# references np.bool for BooleanType columns. Observed as
# "AttributeError: module 'numpy' has no attribute 'bool'" on this kernel (2026-07-27).
# Restoring the aliases is the minimal fix - it changes nothing for code that does not use them.
import numpy as np
for _alias, _builtin in (("bool", bool), ("object", object), ("int", int),
                         ("float", float), ("str", str)):
    if not hasattr(np, _alias):
        setattr(np, _alias, _builtin)

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.max_rows", 60)

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")

TD_HOST = "Teradata-dns-sysa.fg.rbc.com"  # from PROD profile; resolves to 10.174.185.83
EDW = teradatasql.connect(host=TD_HOST, user=username, password=password, logmech="LDAP")

def edw_pd(sql, chunksize=1_000_000):
    import time
    parts, n, t0 = [], 0, time.time()
    for c in pd.read_sql(sql, EDW, chunksize=chunksize):
        parts.append(c); n += len(c)
        print("  ...", n, "rows pulled,", int(time.time() - t0), "s elapsed", flush=True)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

# PROOF, not prints: round-trip the EDW connection and show what the SERVER returned
_cur = EDW.cursor()
_cur.execute("SELECT USER, SESSION, CURRENT_TIMESTAMP")
print("EDW round-trip (teradatasql) returned:", _cur.fetchall())
_cur.close()

# own namespace - must not collide with unsub_cpc/
BASE = "hdfs:///user/427966379/unsub_value_museum/"
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # every join below is a real join, no broadcast

def _sqlkey(sql):
    return hashlib.md5(re.sub(r"\s+", " ", sql).strip().upper().encode()).hexdigest()

def landed(name):
    # "absent" and "cannot check" are NOT the same thing. Genuinely missing path -> False (pull
    # it). ANY other failure -> STOP THE RUN, do not silently treat a broken session as empty.
    try:
        spark.read.parquet(BASE + name).limit(1).collect()
        return True
    except Exception as e:
        msg = str(e)
        if ("Path does not exist" in msg) or ("PATH_NOT_FOUND" in msg) or ("FileNotFound" in msg):
            return False
        raise RuntimeError(name + ": cannot VERIFY HDFS state - refusing to pull anything. "
                           "Fix the spark/HDFS session first. Underlying error: " + msg[:300])

def _to_spark(pdf, name):
    allnull = [c for c in pdf.columns if pdf[c].isna().all()]
    if not allnull:
        return spark.createDataFrame(pdf)
    print(name, ": columns 100% NULL in this pull ->", allnull, "(landed as string nulls)")
    sdf = spark.createDataFrame(pdf[[c for c in pdf.columns if c not in allnull]])
    for c in allnull:
        sdf = sdf.withColumn(c, F.lit(None).cast("string"))
    return sdf.select(*[c for c in pdf.columns])

def land(name, sql, replace=False):
    # Skips ONLY when the exact same SQL already landed with a verified manifest. Changed SQL
    # re-pulls and overwrites automatically. A readable path WITHOUT a manifest = partial/killed
    # write -> treated as not landed, re-pulled.
    meta = BASE + "_meta/" + name.replace("/", "_")
    key = _sqlkey(sql)
    if landed(name) and not replace:
        try:
            old = spark.read.parquet(meta).collect()[0]
            if old["sql_md5"] == key:
                print(name, ": already landed with SAME query,", old["rows"], "rows (", old["landed_at"], ") - SKIP")
                return
            print(name, ": QUERY CHANGED since it landed", old["landed_at"], "(", old["rows"],
                  "rows ) - re-pulling with the new query and OVERWRITING")
        except Exception as e:
            msg = str(e)
            if ("Path does not exist" in msg) or ("PATH_NOT_FOUND" in msg) or ("FileNotFound" in msg):
                print(name, ": readable but NO manifest (partial/killed write) - re-pulling and OVERWRITING")
            else:
                raise RuntimeError(name + ": data is readable but the manifest CANNOT BE VERIFIED - "
                                   "refusing to re-pull on an unverifiable state. Underlying error: " + msg[:300])
    pdf = edw_pd(sql)
    assert len(pdf) > 0, name + " pulled zero rows - investigate before proceeding"
    _to_spark(pdf, name).write.mode("overwrite").parquet(BASE + name)
    nback = spark.read.parquet(BASE + name).count()
    assert nback == len(pdf), name + " HDFS readback mismatch: pulled " + str(len(pdf)) + " readback " + str(nback)
    spark.createDataFrame([(name, key, sql, __import__("datetime").datetime.now().isoformat(), len(pdf))],
                          ["name", "sql_md5", "sql_text", "landed_at", "rows"]).write.mode("overwrite").parquet(meta)
    print(name, ": landed", len(pdf), "rows, HDFS readback confirms", nback, "| manifest written")

def norm_clnt(col):
    # Route through decimal(18,0) FIRST: reservoir CLNT_NO arrives as pandas float64, and
    # float->string renders scientific notation ('1.56314759E8'), which never matches UCP's
    # integer strings. decimal cast is exact for 14-digit client numbers; a genuinely non-numeric
    # CLNT_NO becomes null (visible in match-rate tables, not silently wrong).
    return F.regexp_replace(F.trim(col.cast("decimal(18,0)").cast("string")), "^0+", "")

def T(label, df):
    """Render a titled table AND return it. Timestamp columns are stringified first - pandas 2.x
    rejects Spark's unit-less datetime64. Cloned verbatim from cpc_evidence_hdfs.py."""
    if hasattr(df, "toPandas"):
        for f in df.schema.fields:
            if f.dataType.typeName() in ("timestamp", "date", "timestamp_ntz"):
                df = df.withColumn(f.name, F.date_format(F.col(f.name), "yyyy-MM-dd HH:mm:ss"))
        out = df.toPandas()
    else:
        out = df
    display(Markdown("**" + label + "**  ·  " + str(len(out)) + " rows"))
    display(out)
    return out

print("helpers defined: land()/landed() with SQL manifest | BASE =", BASE, "| UCP_BASE =", UCP_BASE)

# %% [2] FAIL-FAST GATE - prove spark+HDFS are readable before any pull. First-ever run: this
# namespace won't exist yet (handled gracefully). ANY other failure raises. Unchanged.
try:
    spark
except NameError:
    raise RuntimeError("`spark` is not defined - this script requires a Lumina/AI-Farm Spark+YARN "
                       "kernel with a pre-initialized SparkSession named `spark`. There is no "
                       "SparkSession.builder call here by design - do not run this file outside "
                       "that kernel.")

# No raw JVM access: spark._jvm.org.apache.hadoop.fs.FileSystem throws "does not exist in the JVM"
# on this kernel (2026-07-27). The gate proves two things independently.
# 1) the session executes at all.
_ = spark.range(1).count()

# 2) HDFS is genuinely reachable - proven against UCP_BASE, which always exists. Doing this against
# our OWN namespace was wrong: an empty/absent path raises "Unable to infer schema for Parquet",
# which is indistinguishable from a real failure and blocked the first-ever run (2026-07-27).
try:
    _ = spark.read.option("basePath", UCP_BASE).parquet(UCP_BASE).select("MONTH_END_DATE").limit(1).collect()
except Exception as e:
    raise RuntimeError("Cannot read UCP at " + UCP_BASE + " - HDFS is not reachable from this "
                       "session. Fix it before running any pull cell. Underlying error: " + str(e)[:300])

# 3) our own namespace: absent on a first run, which is expected and NOT an error. Every "empty or
# missing" signal Spark emits is treated as absent; land() creates the namespace on its first write.
try:
    spark.read.parquet(BASE).limit(1).collect()
    _base_exists = True
except Exception as e:
    _msg = str(e)
    _absent = ("Path does not exist" in _msg or "PATH_NOT_FOUND" in _msg or "FileNotFound" in _msg
               or "Unable to infer schema" in _msg or "UNABLE_TO_INFER_SCHEMA" in _msg)
    if not _absent:
        raise RuntimeError("Unexpected failure reading " + BASE + " - investigate before pulling. "
                           "Underlying error: " + _msg[:300])
    _base_exists = False

if _base_exists:
    print("HDFS round-trip OK - own namespace", BASE, "already exists (not the first run).")
else:
    # NOT an alarm: datasets live in SUBPATHS (unsubs_12m/q1, _meta/...), so BASE itself holds no
    # parquet and Spark cannot infer a schema there even when landings exist. The authoritative
    # per-dataset status is what land() prints below - trust that, not this line.
    print("No parquet directly at", BASE, "- normal, datasets live in subpaths. Per-dataset status "
          "(SKIP vs landed) is reported by land() below; that is the authoritative check.")
print("GATE PASSED - spark session live, HDFS reachable.")

# %% [3] SIZE PROBE - EVENT-ONLY COUNT(*), disposition_cd = 4 (unsubs), per quarter. Gated on
# landed() so it costs nothing once all 4 quarters are landed - only unlanded quarters get sized.
# This is a fraction of the send-side probe's cost by construction: unsubs are a small slice of
# sends, which is exactly why Phase 1 can afford 12 months where the send pull could not afford 5.
_P1_QUARTERS = ["q1", "q2", "q3", "q4"]
_P1_BOUNDS = {
    "q1": ("2025-07-01", "2025-10-01"),
    "q2": ("2025-10-01", "2026-01-01"),
    "q3": ("2026-01-01", "2026-04-01"),
    "q4": ("2026-04-01", "2026-07-01"),
}
_p1_unlanded = [q for q in _P1_QUARTERS if not landed("unsubs_12m/" + q)]

if not _p1_unlanded:
    print("SIZE PROBE - all 4 unsubs_12m quarters already landed - SKIP (nothing left to size)")
else:
    _probe_rows = []
    for _q in _p1_unlanded:
        _ds, _de = _P1_BOUNDS[_q]
        _pdf = edw_pd("""
SELECT COUNT(*) AS event_rows
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_cd = 4
  AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
""" % (_ds, _de))
        _probe_rows.append({"quarter": _q, "event_rows": int(_pdf["event_rows"][0])})
    T("SIZE PROBE - disp_cd=4 unsub events, unlanded quarters only (event-only, no join, no DISTINCT)",
      pd.DataFrame(_probe_rows))
    print("\nSTOP RULE: if any quarter's event_rows is unexpectedly huge (order of magnitude above")
    print("the others), split that quarter's land() cell further (e.g. by month) before pulling.")

# %% [4] PULL unsubs_12m/q1..q4 - disposition_cd=4, 12-month bank-wide window, quarterly chunks.
# DEDUPE-BEFORE-JOIN derived-table shape: `ek` pre-filters+DISTINCTs VENDOR_FEEDBACK_EVENT on
# disposition_cd=4 + the quarter's date window BEFORE joining VENDOR_FEEDBACK_MASTER on
# (consumer_id_hashed, TREATMENT_ID) - the join runs against a deduped key set, not the raw event
# scan. MASTER load_tm is bounded ~+/-1 month around each quarter's edges (same convention as the
# rest of this repo's EDW pulls). Output columns are exactly CLNT_NO, TREATMENT_ID, unsub_tm - NO
# server-side MNE decode and NO aggregation to one-row-per-client here; that collapse happens in
# Spark (cell [6]) so the tie-break logic is auditable and re-runnable without a re-pull.
_unsubs_12m_chunks = [
    ("q1", "2025-07-01", "2025-10-01", "2025-06-01", "2025-11-01"),
    ("q2", "2025-10-01", "2026-01-01", "2025-09-01", "2026-02-01"),
    ("q3", "2026-01-01", "2026-04-01", "2025-12-01", "2026-05-01"),
    ("q4", "2026-04-01", "2026-07-01", "2026-03-01", "2026-08-01"),
]

def _unsubs_12m_sql(ev_start, ev_end, ld_start, ld_end):
    return """
SELECT DISTINCT m.CLNT_NO, ek.TREATMENT_ID, ek.disposition_dt_tm AS unsub_tm
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '%s' AND m.load_tm < DATE '%s'
""" % (ev_start, ev_end, ld_start, ld_end)

for _q, _es, _ee, _ls, _le in _unsubs_12m_chunks:
    land("unsubs_12m/" + _q, _unsubs_12m_sql(_es, _ee, _ls, _le))

# %% [5] READ BACK all 4 quarters, normalize CLNT_NO, cross-quarter dedup. Cross-quarter dupes are
# possible only in principle (the quarter EVENT windows are disjoint by construction), but the
# same defensive dropDuplicates convention used for sends_q2 in archaeology/28 costs nothing here.
unsubs_12m_raw = (spark.read.parquet(BASE + "unsubs_12m/*")
                   .withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO"))))
_n_raw = unsubs_12m_raw.count()
unsubs_12m_dedup = unsubs_12m_raw.dropDuplicates(["CLNT_NO", "TREATMENT_ID", "unsub_tm"])
_n_dedup = unsubs_12m_dedup.count()
unsubs_12m_dedup.cache()

T("READBACK - unsubs_12m/*, all 4 quarters, after CLNT_NO normalization and cross-quarter dedup",
  pd.DataFrame([{"rows_before_dedup": _n_raw, "rows_after_dedup": _n_dedup,
                 "cross_quarter_duplicates_removed": _n_raw - _n_dedup,
                 "distinct_clients": unsubs_12m_dedup.select("CLNT_NO").distinct().count()}]))
assert _n_dedup > 0, "unsubs_12m read back zero rows after dedup - a land() cell above failed silently"

# %% [6] GRAIN REDUCTION - one row per client. A client can log several qualifying unsub events
# (different treatments, different times, or the same treatment logged more than once); this file
# keeps only their LAST one and calls that treatment their attribution. Print how many clients had
# more than one qualifying event BEFORE collapsing, so the size of this judgment call is visible.
_events_per_client = unsubs_12m_dedup.groupBy("CLNT_NO").agg(F.count("*").alias("n_events"))
_n_multi_event = _events_per_client.filter(F.col("n_events") > 1).count()
_n_distinct_clients = _events_per_client.count()
print("clients with >1 qualifying unsub event in the 12-month window:", _n_multi_event, "of",
      _n_distinct_clients, "(", round(100.0 * _n_multi_event / _n_distinct_clients, 2),
      "% ) - each collapses to their LAST event below; the rest are unaffected by the collapse.")

_w_last = Window.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").desc(), F.col("TREATMENT_ID").desc())
leaver_last = (unsubs_12m_dedup.withColumn("rn", F.row_number().over(_w_last))
               .filter("rn = 1").drop("rn"))
_n_leaver_last = leaver_last.count()
assert _n_leaver_last == _n_distinct_clients, (
    "post-collapse row count (" + str(_n_leaver_last) + ") != distinct clients (" +
    str(_n_distinct_clients) + ") - the window function did not collapse to exactly one row per client")
leaver_last.cache()
print("leaver_last: one row per client, LAST qualifying unsub event -", _n_leaver_last, "rows")

# %% [7] MNE / PROGRAM MAPPING - CARDS_MNES sourced from the repo's own MNE catalog, not invented
# here (same set used by cpc_reservoir_extract.py and archaeology/28_unsub_value_ucp.py - keep all
# three copies in sync if this set ever changes). Source: UNSUB_TRACKING_KNOWLEDGE.md section 4
# "MNE tracking scope", Cards rows: PCQ, PCL, PCD, AUH, CLI, MVP, CRV.
#
# RULING (Andre, 2026-07-26, final): CTU and O2P are NOT cards - they are programs that involve
# cards, reported inside async, but out of the cards package. They stay OUT of CARDS_MNES and
# remain visible under their own MNEs (NON_CARDS here) - nothing is hidden, just not pre-labeled Cards.
CARDS_MNES = frozenset({"PCQ", "PCL", "PCD", "AUH", "CLI", "MVP", "CRV"})

# DEFAULT stream (per UNSUB_TRACKING_KNOWLEDGE.md): TREATMENT_ID = 'DEFAULT' literally, or a blank
# MNE substring - both mean "mail outside campaign taxonomy" (service + broken-template + untagged
# marketing), invisible to MNE-based suppression, a governance finding in its own right. Kept as
# its own program label, never collapsed into NON_CARDS.
def add_mne_program(df):
    df = df.withColumn(
        "mne",
        F.when((F.trim(F.col("TREATMENT_ID")) == "DEFAULT") |
               (F.trim(F.substring(F.col("TREATMENT_ID"), 8, 3)) == ""), F.lit("DEFAULT"))
         .otherwise(F.trim(F.substring(F.col("TREATMENT_ID"), 8, 3))))
    df = df.withColumn(
        "program",
        F.when(F.col("mne") == "DEFAULT", F.lit("DEFAULT"))
         .when(F.col("mne").isin(list(CARDS_MNES)), F.lit("CARDS"))
         .otherwise(F.lit("NON_CARDS")))
    return df

leaver_mne = add_mne_program(leaver_last)
leaver_mne.cache()
T("PROGRAM MIX - leaver counts by program, straight off the attributed (last) TREATMENT_ID",
  leaver_mne.groupBy("program").agg(F.count("*").alias("leaver_clients")).orderBy(F.desc("leaver_clients")))

# %% [8] UCP PARTITION PROBE - available MONTH_END_DATE partitions, min/max, before computing any
# anchor. Needed because this window spans 12 months of unsub dates behind treatments that could
# have launched well before that, so (unlike the prior version of this file) the set of anchor
# months needed cannot be enumerated in advance - it is discovered from the decoded launch dates,
# then clamped to whatever UCP actually has on disk.
_ucp_parts = (spark.read.option("basePath", UCP_BASE).parquet(UCP_BASE)
              .select("MONTH_END_DATE").distinct().toPandas())
_ucp_avail = sorted(_ucp_parts["MONTH_END_DATE"].astype(str).tolist())
assert len(_ucp_avail) > 0, "no UCP partitions visible under " + UCP_BASE + " - check the path before proceeding"
UCP_MIN, UCP_MAX = _ucp_avail[0], _ucp_avail[-1]
print("UCP partitions available:", len(_ucp_avail), "| min:", UCP_MIN, "| max:", UCP_MAX)

# %% [9] TREATMENT_ID DECODE - launch date. DOCUMENTED: MNE = SUBSTR(TREATMENT_ID, 8, 3)
# (UNSUB_TRACKING_KNOWLEDGE.md:145). The julian-date piece is NAMED but never given exact
# positions anywhere in the repo. WORKING ASSUMPTION (carried from archaeology/28, unverified
# beyond that file's own empirical check): positions 1-7 = julian launch date YYYYDDD (4-digit
# year + 3-digit day-of-year). Validated empirically below over THIS file's own distinct
# TREATMENT_IDs (the attributed/last-unsub treatment per client), not trusted blind.
# FALLBACK if validation fails: decode launch date from DTZV01.TACTIC_EVNT_IP_AR_H60M instead
# (has real deployment dates per TACTIC_ID) - out of scope for this file, would need a fresh pull.
def add_launch_decode(df):
    df = df.withColumn("_yr", F.substring(F.col("TREATMENT_ID"), 1, 4).cast("int"))
    df = df.withColumn("_doy", F.substring(F.col("TREATMENT_ID"), 5, 3).cast("int"))
    df = df.withColumn(
        "_launch_dt_raw",
        F.when(F.col("_yr").isNotNull() & F.col("_doy").between(1, 366),
               F.expr("date_add(to_date(concat(_yr, '-01-01')), _doy - 1)")))
    df = df.withColumn(
        "_valid",
        F.col("_launch_dt_raw").isNotNull() &
        (F.col("_launch_dt_raw") >= F.lit("2020-01-01")) &
        (F.col("_launch_dt_raw") < F.lit("2027-01-01")))
    df = df.withColumn("launch_dt", F.when(F.col("_valid"), F.col("_launch_dt_raw")))
    return df.drop("_yr", "_doy", "_launch_dt_raw")

leaver_decoded = add_launch_decode(leaver_mne)
leaver_decoded.cache()

_decode_check = leaver_decoded.select("TREATMENT_ID", "_valid").dropDuplicates(["TREATMENT_ID"])
_n_ids = _decode_check.count()
_n_valid = _decode_check.filter(F.col("_valid")).count()
_pct_valid = round(100.0 * _n_valid / _n_ids, 2)
T("DECODE1 - TREATMENT_ID launch-date decode validation (distinct attributed TREATMENT_IDs, "
  "positions 1-7 = YYYYDDD)",
  pd.DataFrame([{"distinct_treatment_ids": _n_ids, "decoded_valid": _n_valid, "pct_valid": _pct_valid}]))

if _n_valid < _n_ids:
    _fail_reason = (
        F.when(F.trim(F.col("TREATMENT_ID")) == "DEFAULT", F.lit("DEFAULT literal"))
         .otherwise(F.lit("non-numeric prefix or decoded outside [2020-01-01, 2027-01-01)")))
    T("DECODE2 - decode failures by reason (distinct TREATMENT_IDs)",
      _decode_check.filter(~F.col("_valid")).withColumn("_fail_reason", _fail_reason)
                    .groupBy("_fail_reason").agg(F.count("*").alias("distinct_treatment_ids")))

assert _pct_valid >= 99.0, (
    "TREATMENT_ID launch-date decode only validated for " + str(_pct_valid) + "% of distinct "
    "TREATMENT_IDs (need >= 99%). The positions-1-7=YYYYDDD assumption is WRONG or needs "
    "adjustment - do not proceed with the anchor built on this decode. See the fallback note above.")
print("\nDECODE VALIDATED >= 99% - launch_dt trusted for the rest of this run.")
leaver_decoded = leaver_decoded.drop("_valid")

# %% [10] ANCHOR - month-end before the client's last-unsub TREATMENT's decoded launch date,
# clamped to available UCP partitions. PRE-TREATMENT SNAPSHOT: this is the client's profile the
# month before the campaign that (eventually) unsubbed them ever launched - Phase 2's stayers will
# anchor at FIRST EXPOSURE for the same reason, and the two are comparable because both precede
# the treatment that acted on the client, even though "the treatment that acted" means something
# different for each group (attributed unsub vs. any mailing).
leaver_anchored = leaver_decoded.withColumn(
    "_anchor_raw", F.when(F.col("launch_dt").isNotNull(), F.last_day(F.add_months(F.col("launch_dt"), -1))))
leaver_anchored = leaver_anchored.withColumn(
    "anchor",
    F.when(F.col("_anchor_raw").isNotNull(),
           F.greatest(F.least(F.col("_anchor_raw"), F.lit(UCP_MAX).cast("date")),
                      F.lit(UCP_MIN).cast("date")))).drop("_anchor_raw")
leaver_anchored.cache()

_n_no_launch = leaver_anchored.filter(F.col("launch_dt").isNull()).count()
print("clients with no decodable launch_dt (excluded from anchor/UCP, per DECODE1's <1% floor):",
      _n_no_launch)

_n_clamped = leaver_anchored.filter(
    F.col("launch_dt").isNotNull() &
    (F.last_day(F.add_months(F.col("launch_dt"), -1)) != F.col("anchor"))).count()
T("ANCHOR CLAMP CHECK - rows where the raw month-before-launch anchor got clamped to UCP_MIN/"
  "UCP_MAX (their UCP snapshot is NOT the true month-before-launch)",
  pd.DataFrame([{"clamped_rows": _n_clamped, "ucp_min": UCP_MIN, "ucp_max": UCP_MAX}]))

_anchor_months = sorted(r["a"] for r in
                         leaver_anchored.filter(F.col("anchor").isNotNull())
                         .select(F.date_format("anchor", "yyyy-MM-dd").alias("a")).distinct().collect())
print("distinct anchor months needed:", len(_anchor_months), "-", _anchor_months)

# %% [10b] UCP SCHEMA PROBE - hard gate, ported from archaeology/28_unsub_value_ucp.py's cell [2].
# Nothing downstream may assume a column exists. Reads the LATEST available UCP partition (UCP_MAX,
# computed in cell [8]) and checks it for every column this file needs BEFORE cell [11] reads any
# anchor-month partition off that assumption.
_latest_ucp = spark.read.parquet(UCP_BASE + "MONTH_END_DATE=" + UCP_MAX)
print("schema of latest UCP partition (" + UCP_MAX + "):")
_latest_ucp.printSchema()

REQUESTED_UCP_COLS = ["CLNT_NO", "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT",
                      "C_TOT_CNT", "PROF_TOT_ANNUAL"]
_actual_ucp_cols = set(_latest_ucp.columns)
_present_tbl = pd.DataFrame(
    {"column": REQUESTED_UCP_COLS,
     "status": ["PRESENT" if c in _actual_ucp_cols else "MISSING" for c in REQUESTED_UCP_COLS]})
T("SCHEMA PROBE - UCP requested column presence check (latest partition " + UCP_MAX + ")", _present_tbl)

UCP_COLS = [c for c in REQUESTED_UCP_COLS if c in _actual_ucp_cols]
_dropped_ucp_cols = [c for c in REQUESTED_UCP_COLS if c not in _actual_ucp_cols]
print("\nUCP_COLS (usable, intersection with actual schema):", UCP_COLS)
if _dropped_ucp_cols:
    print("DROPPED (requested but not in schema):", _dropped_ucp_cols)
else:
    print("nothing dropped - all requested columns present")

# CLNT_TYP is not in the documented UCP field list but the read below filters on it - probe for it
# explicitly rather than assuming either way.
HAS_CLNT_TYP = "CLNT_TYP" in _actual_ucp_cols
print("\nHAS_CLNT_TYP =", HAS_CLNT_TYP,
      "(controls whether the UCP read in cell [11] applies the Personal-client filter)")

# CRITICAL MINIMUM - the four client variables this file exists to report on (age, tenure, TIBC
# product counts, PROF_TOT_ANNUAL) plus the join key. PROF_TOT_ANNUAL is the value axis (L5,
# prof_quintile banding) - without it the analysis is pointless. Fail loud here rather than
# silently degrading several cells from now.
_critical_ucp_cols = ["CLNT_NO", "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT",
                      "C_TOT_CNT", "PROF_TOT_ANNUAL"]
_missing_critical_ucp = [c for c in _critical_ucp_cols if c not in _actual_ucp_cols]
assert not _missing_critical_ucp, (
    "CRITICAL UCP COLUMNS MISSING from schema: " + str(_missing_critical_ucp) + ". Check the UCP "
    "field catalog before touching anything else - this file cannot band or measure age, tenure, "
    "product counts, or PROF_TOT_ANNUAL without these.")

print("\nUCP SCHEMA PROBE PASSED - UCP_COLS and HAS_CLNT_TYP are now fixed for the rest of this run.")

# %% [11] UCP READ - ONE multi-partition read + ONE join, not a per-anchor loop. A 12-month leaver
# window fans out to ~30 distinct anchor months (row index 29 in the old per-anchor summary table).
# The prior version looped per anchor (read partition, filter, norm, semi-join, append), then
# unionByName'd ~30 frames and forced the whole 30-branch DAG with a single .cache()+.count() -
# ~450M rows scanned across 30 branches in one action. That killed the YARN session on 2026-07-27
# (Py4JJavaError / SparkException: Job 558 cancelled because SparkContext was shut down). CLNT_TYP
# == 'Personal' is the verified filter value, applied only when HAS_CLNT_TYP (schema probe, cell
# [10b]) says the column exists on this UCP snapshot. UCP_COLS also comes from that probe - the
# intersection with the real schema, not a hardcoded list.

# REQUESTS - (CLNT_NO, ucp_anchor) needed pairs, built from leaver_anchored alone (no UCP touched
# yet). This replaces the old per-anchor "_needed" query inside the loop.
_requests = (leaver_anchored.filter(F.col("anchor").isNotNull())
             .withColumn("ucp_anchor", F.date_format(F.col("anchor"), "yyyy-MM-dd"))
             .select("CLNT_NO", "ucp_anchor")
             .distinct())

# assert every anchor month this run needs is actually a UCP partition on disk (cell [8]'s
# _ucp_avail) - fail loud with the exact missing months, rather than a bare Spark
# AnalysisException out of the multi-path read below.
_missing_anchor_parts = sorted(set(_anchor_months) - set(_ucp_avail))
assert not _missing_anchor_parts, (
    "anchor months not present as UCP partitions: " + str(_missing_anchor_parts) +
    " - re-check the UCP_MIN/UCP_MAX clamp in cell [10] before proceeding.")

# needed-client counts per anchor, computed from REQUESTS (cheap - no UCP read involved). Feeds
# the UCP1 summary table below.
_needed_by_anchor = (_requests.groupBy("ucp_anchor")
                      .agg(F.countDistinct("CLNT_NO").alias("needed_distinct_clients")))

# SINGLE read across every needed partition. basePath makes MONTH_END_DATE come back as a real
# partition COLUMN instead of being consumed by the path.
_ucp_paths = [UCP_BASE + "MONTH_END_DATE=" + _m for _m in _anchor_months]
raw = spark.read.option("basePath", UCP_BASE).parquet(*_ucp_paths)
assert "MONTH_END_DATE" in raw.columns, (
    "MONTH_END_DATE missing from the multi-partition UCP read's columns - basePath partition "
    "discovery did not work as expected. Columns seen: " + str(raw.columns))

if HAS_CLNT_TYP:
    raw = raw.filter(F.trim(F.col("CLNT_TYP")) == "Personal")
raw = (raw.select(*UCP_COLS, "MONTH_END_DATE")
          .withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO")))
          .withColumn("ucp_month_end", F.col("MONTH_END_DATE").cast("string"))
          .drop("MONTH_END_DATE"))

# alias the requests side so the join condition is unambiguous (both frames have a CLNT_NO-shaped
# column) - leftsemi only ever returns raw's columns, but the join CONDITION still needs to
# distinguish the two frames.
_requests_join = _requests.withColumnRenamed("CLNT_NO", "_req_clnt_no").withColumnRenamed("ucp_anchor", "_req_anchor")

ucp_spine_lineage = raw.join(
    _requests_join,
    (raw["CLNT_NO"] == _requests_join["_req_clnt_no"]) & (raw["ucp_month_end"] == _requests_join["_req_anchor"]),
    "leftsemi")

# WRITE-THEN-READ instead of .cache() - writing severs the 30-partition-read lineage so every
# downstream action reads a flat parquet file instead of recomputing the scan. Worst case, a
# session death costs re-running this one write, not the whole multi-branch DAG.
_n_spine_written = ucp_spine_lineage.count()
ucp_spine_lineage.write.mode("overwrite").parquet(BASE + "ucp_spine")
ucp_spine = spark.read.parquet(BASE + "ucp_spine")
_n_spine_read_back = ucp_spine.count()
assert _n_spine_written == _n_spine_read_back, (
    "ucp_spine HDFS readback mismatch: wrote " + str(_n_spine_written) + " read back " +
    str(_n_spine_read_back))
print("ucp_spine written to", BASE + "ucp_spine", "-", _n_spine_written, "rows written,",
      _n_spine_read_back, "rows read back (match confirmed)")

# fan-out guard: one row per (CLNT_NO, ucp_month_end) - a client must not appear twice at the same
# anchor. Runs on the READ-BACK frame. Dedup fallback commented below if it ever trips.
_n_spine = ucp_spine.count()
_n_spine_distinct = ucp_spine.select("CLNT_NO", "ucp_month_end").distinct().count()
assert _n_spine == _n_spine_distinct, (
    "UCP spine has " + str(_n_spine) + " rows but only " + str(_n_spine_distinct) +
    " distinct (CLNT_NO, ucp_month_end) pairs - UCP is not unique per (CLNT_NO, MONTH_END_DATE).")
# DEDUP FALLBACK (uncomment if the assert above trips):
# _w = Window.partitionBy("CLNT_NO", "ucp_month_end").orderBy(F.lit(1))
# ucp_spine = ucp_spine.withColumn("_rn", F.row_number().over(_w)).filter("_rn = 1").drop("_rn")
print("UCP spine fan-out guard PASSED -", _n_spine, "rows, one per (CLNT_NO, ucp_month_end)")

# UCP1 SUMMARY - one groupBy on the spine (one shuffle) instead of ~30 separate per-anchor counts.
_matched_by_anchor = ucp_spine.groupBy("ucp_month_end").agg(F.count("*").alias("matched_rows"))
_ucp_read_summary = (
    _needed_by_anchor.withColumnRenamed("ucp_anchor", "ucp_anchor")
    .join(_matched_by_anchor.withColumnRenamed("ucp_month_end", "ucp_anchor"), "ucp_anchor", "outer")
    .fillna(0, subset=["needed_distinct_clients", "matched_rows"])
    .orderBy("ucp_anchor"))
T("UCP1 - per-anchor-month read summary (needed vs matched)", _ucp_read_summary)

leaver_ucp = (leaver_anchored
              .withColumn("anchor_str", F.date_format("anchor", "yyyy-MM-dd"))
              .join(ucp_spine.withColumnRenamed("CLNT_NO", "_u_clnt"),
                    (F.col("CLNT_NO") == F.col("_u_clnt")) & (F.col("anchor_str") == F.col("ucp_month_end")),
                    "left")
              .drop("_u_clnt"))

_n_before_ucp_join = leaver_anchored.count()
_n_after_ucp_join = leaver_ucp.count()
assert _n_before_ucp_join == _n_after_ucp_join, (
    "FAN-OUT on client x UCP join: " + str(_n_before_ucp_join) + " -> " + str(_n_after_ucp_join) +
    " rows. See the UCP spine fan-out guard above.")
leaver_ucp = leaver_ucp.withColumn("ucp_matched", F.col("AGE").isNotNull())
leaver_ucp.cache()
print("leaver_ucp assembled:", _n_after_ucp_join, "rows (one per leaver client)")

# %% [12] M0 - COVERAGE & NULLS. This prints BEFORE any finding. VOLUME + MIX ONLY - NO RATES (no
# denominator until Phase 2).
_n_leavers_total = leaver_ucp.count()
_n_matched_total = leaver_ucp.filter(F.col("ucp_matched")).count()

_cov_by_program = (leaver_ucp.groupBy("program")
                    .agg(F.count("*").alias("leaver_clients"),
                         F.sum(F.col("ucp_matched").cast("int")).alias("matched"))
                    .withColumn("unmatched", F.col("leaver_clients") - F.col("matched"))
                    .withColumn("match_pct", F.round(100.0 * F.col("matched") / F.col("leaver_clients"), 1)))
T("M0a - UCP match rate by program | VOLUME + MIX ONLY - NO RATES (no denominator until Phase 2)",
  _cov_by_program.orderBy(F.desc("leaver_clients")))

# UNMATCHED BREAKDOWN - this is where the "business client" theory for unmatched gets proved or
# killed. Unlike a SENDS pull (where "newly acquired mid-window" explains unmatched-at-anchor),
# every client here already unsubscribed from real mail, so they existed as clients well before
# their anchor month. A concentration of unmatched rows in specific anchor months or specific MNEs
# would instead point to CLNT_TYP != 'Personal' (business/commercial clients, filtered out by the
# UCP read's Personal-only filter) rather than a genuine data gap - proved or killed by M0b/M0c,
# not assumed either way.
_unmatched = leaver_ucp.filter(~F.col("ucp_matched")).select("CLNT_NO", "anchor_str")
_cov_by_anchor = (_unmatched.groupBy("anchor_str").agg(F.count("*").alias("unmatched_clients"))
                   .orderBy("anchor_str"))
T("M0b - unmatched clients by anchor month - see the 'business client' theory note in the comment "
  "above this cell", _cov_by_anchor)

_unmatched_mne = (_unmatched.join(leaver_ucp.select("CLNT_NO", "mne"), "CLNT_NO", "inner")
                   .groupBy("mne").agg(F.count("*").alias("unmatched_clients"))
                   .orderBy(F.desc("unmatched_clients")).limit(15))
T("M0c - unmatched clients by top-15 MNE (their attributed/last-unsub treatment's MNE) - same "
  "business-client theory, cut by campaign instead of by month", _unmatched_mne)

_null_rows = []
for _c in UCP_COLS:
    _n_null = leaver_ucp.filter(F.col("ucp_matched") & F.col(_c).isNull()).count()
    _null_rows.append({"field": _c, "null_among_matched": _n_null,
                        "pct_null_among_matched": (
                            round(100.0 * _n_null / _n_matched_total, 2) if _n_matched_total else 0.0)})
_null_df = pd.DataFrame(_null_rows)
T("M0d - null counts among MATCHED rows, per UCP field (CLNT_NO included for completeness - "
  "structurally 0% by construction of the join key)", _null_df)

_worst_field = _null_df.loc[_null_df["pct_null_among_matched"].idxmax()]
if _worst_field["pct_null_among_matched"] > 5.0:
    print("VERDICT: '" + _worst_field["field"] + "' is", _worst_field["pct_null_among_matched"],
          "% null among matched rows (> 5%) - every median downstream using this field is suspect. "
          "Check M0d before trusting L1/L2/L3/L4/L5/CUBE.")
else:
    print("VERDICT: all UCP fields under 5% null among matched rows (worst =",
          _worst_field["field"], "at", _worst_field["pct_null_among_matched"], "%).")

# %% [13] BANDING - one function, unchanged from the prior version of this file. EDITABLE
# PARAMETERS, one place: band cut points and the high_potential thresholds are OUR CHOICE, not
# documented anywhere else in the repo.
TENURE_EDGES = [2, 5, 10, 20]     # years; bins: 0-2, 3-5, 6-10, 11-20, 20+
AGE_EDGES = [24, 34, 44, 54, 64]  # bins: <25, 25-34, 35-44, 45-54, 55-64, 65+
HP_AGE_MAX = 35        # high_potential: AGE < this
HP_TENURE_MAX = 5      # high_potential: TENURE_RBC_YEARS <= this
HP_PROD_MAX = 2        # high_potential: prod_cnt <= this
# high_potential is the thesis as a countable definition: decades of banking life ahead, not yet
# embedded, most of the wallet unsold, and email is now closed.

# PROF_QUINTILE CUT POINTS COME FROM THE LEAVER POPULATION ITSELF (Phase 1 has no mailed/reachable
# base to rank against - that base doesn't exist until Phase 2). This is a WITHIN-LEAVER ranking,
# NOT value relative to the reachable base: "top prof quintile" here means "top-earning among
# people who left", not "top-earning among everyone cards could have mailed". Phase 2 replaces
# these cuts with ones computed from the mailed/stayer base once it exists.
_prof_cut_base = leaver_ucp.filter(F.col("ucp_matched") & F.col("PROF_TOT_ANNUAL").isNotNull())
PROF_CUTS = _prof_cut_base.approxQuantile("PROF_TOT_ANNUAL", [0.2, 0.4, 0.6, 0.8], 0.01)
assert len(PROF_CUTS) == 4, (
    "no non-null PROF_TOT_ANNUAL among matched leavers - cannot cut quintiles; check M0's null "
    "table before going further")
T("PROF CUTS - PROF_TOT_ANNUAL quintile cut points, computed over the LEAVER population itself "
  "(WITHIN-LEAVER ranking, NOT value relative to the reachable base - Phase 2 replaces this with "
  "cuts from the mailed base), n = " + str(_prof_cut_base.count()),
  pd.DataFrame([{"p20": PROF_CUTS[0], "p40": PROF_CUTS[1], "p60": PROF_CUTS[2], "p80": PROF_CUTS[3]}]))

def apply_bands(df):
    df = df.withColumn(
        "prod_cnt",
        F.coalesce(F.col("T_TOT_CNT"), F.lit(0)) + F.coalesce(F.col("I_TOT_CNT"), F.lit(0)) +
        F.coalesce(F.col("B_TOT_CNT"), F.lit(0)) + F.coalesce(F.col("C_TOT_CNT"), F.lit(0)))
    # NOTE: '0' is added to the brief's '1','2','3-4','5+' list (a real, reachable value: leaver
    # clients holding no T/I/B/C product on file) so no client is silently misclassified into '1'.
    df = df.withColumn(
        "prod_band",
        F.when(F.col("prod_cnt") == 0, "0").when(F.col("prod_cnt") == 1, "1")
         .when(F.col("prod_cnt") == 2, "2").when(F.col("prod_cnt") <= 4, "3-4").otherwise("5+"))

    has_t = F.coalesce(F.col("T_TOT_CNT"), F.lit(0)) > 0
    has_i = F.coalesce(F.col("I_TOT_CNT"), F.lit(0)) > 0
    has_b = F.coalesce(F.col("B_TOT_CNT"), F.lit(0)) > 0
    has_c = F.coalesce(F.col("C_TOT_CNT"), F.lit(0)) > 0
    n_held = has_t.cast("int") + has_i.cast("int") + has_b.cast("int") + has_c.cast("int")
    # deterministic label rules (our choice, no repo precedent): 0 held -> "none", 1 held -> "<X>
    # only", 2 held -> "<X>+<Y>" in fixed T,I,B,C priority order, 3+ held -> "multi"
    df = df.withColumn(
        "tibc_mix",
        F.when(n_held == 0, "none")
         .when(n_held == 1,
               F.when(has_t, "T only").when(has_i, "I only").when(has_b, "B only").otherwise("C only"))
         .when(n_held == 2,
               F.when(has_t & has_i, "T+I").when(has_t & has_b, "T+B").when(has_t & has_c, "T+C")
                .when(has_i & has_b, "I+B").when(has_i & has_c, "I+C").otherwise("B+C"))
         .otherwise("multi"))

    e = TENURE_EDGES
    df = df.withColumn(
        "tenure_band",
        F.when(F.col("TENURE_RBC_YEARS").isNull(), "unknown")
         .when(F.col("TENURE_RBC_YEARS") <= e[0], "0-2").when(F.col("TENURE_RBC_YEARS") <= e[1], "3-5")
         .when(F.col("TENURE_RBC_YEARS") <= e[2], "6-10").when(F.col("TENURE_RBC_YEARS") <= e[3], "11-20")
         .otherwise("20+"))

    a = AGE_EDGES
    df = df.withColumn(
        "age_band",
        F.when(F.col("AGE").isNull(), "unknown")
         .when(F.col("AGE") <= a[0], "<25").when(F.col("AGE") <= a[1], "25-34")
         .when(F.col("AGE") <= a[2], "35-44").when(F.col("AGE") <= a[3], "45-54")
         .when(F.col("AGE") <= a[4], "55-64").otherwise("65+"))

    df = df.withColumn(
        "prof_quintile",
        F.when(F.col("PROF_TOT_ANNUAL").isNull(), "unknown")
         .when(F.col("PROF_TOT_ANNUAL") <= PROF_CUTS[0], "1")
         .when(F.col("PROF_TOT_ANNUAL") <= PROF_CUTS[1], "2")
         .when(F.col("PROF_TOT_ANNUAL") <= PROF_CUTS[2], "3")
         .when(F.col("PROF_TOT_ANNUAL") <= PROF_CUTS[3], "4")
         .otherwise("5"))

    df = df.withColumn(
        "high_potential",
        F.when(F.col("AGE").isNull() | F.col("TENURE_RBC_YEARS").isNull(), F.lit(False))
         .otherwise((F.col("AGE") < HP_AGE_MAX) & (F.col("TENURE_RBC_YEARS") <= HP_TENURE_MAX) &
                    (F.col("prod_cnt") <= HP_PROD_MAX)))
    return df

leaver_banded = apply_bands(leaver_ucp)
leaver_banded.cache()
print("leaver_banded:", leaver_banded.count(), "rows, bands applied")

# %% [14] L1 - WHERE. GRAIN: one row per leaver client, bank-wide, 12-month window (2025-07 to
# 2026-07). VOLUME + MIX ONLY - NO RATES (no denominator until Phase 2).
def _l1_metrics(df, keys):
    total = df.groupBy(*keys).agg(F.count("*").alias("leaver_clients"))
    total = total.withColumn("share_of_all_leavers_pct",
                              F.round(100.0 * F.col("leaver_clients") / _n_leavers_total, 2))
    matched = (df.filter(F.col("ucp_matched")).groupBy(*keys)
               .agg(F.expr("percentile_approx(AGE, 0.5)").alias("median_age"),
                    F.expr("percentile_approx(TENURE_RBC_YEARS, 0.5)").alias("median_tenure_years"),
                    F.expr("percentile_approx(prod_cnt, 0.5)").alias("median_prod_cnt"),
                    F.expr("percentile_approx(PROF_TOT_ANNUAL, 0.5)").alias("median_prof_annual"),
                    F.round(100.0 * F.sum(F.when(F.col("prod_band") == "1", 1).otherwise(0)) / F.count("*"), 1)
                    .alias("pct_single_product"),
                    F.round(100.0 * F.sum(F.when(F.col("prof_quintile") == "5", 1).otherwise(0)) / F.count("*"), 1)
                    .alias("pct_top_prof_quintile"),
                    F.round(100.0 * F.sum(F.col("high_potential").cast("int")) / F.count("*"), 1)
                    .alias("pct_high_potential")))
    return total.join(matched, list(keys), "left")

l1_program = _l1_metrics(leaver_banded, ["program"]).orderBy(F.desc("leaver_clients"))
T("L1 - WHERE, by program | window: unsub_tm in [2025-07-01, 2026-07-01), bank-wide | GRAIN: "
  "distinct client, last qualifying unsub event | VOLUME + MIX ONLY - NO RATES (no denominator "
  "until Phase 2)", l1_program)

l1_mne = (_l1_metrics(leaver_banded, ["mne", "program"])
          .withColumn("is_cards", F.col("program") == F.lit("CARDS"))
          .orderBy(F.desc("leaver_clients")).limit(20))
T("L1 - WHERE, top-20 MNEs by leaver-client volume | is_cards flags CARDS-program rows | VOLUME + "
  "MIX ONLY - NO RATES (no denominator until Phase 2)", l1_mne)

# %% [15] L2 - WHO, cards vs non-cards leavers. Composition-against-composition, no denominator
# required - THIS IS a legitimate comparison: it answers whether the people cards loses differ
# from the people the rest of the bank loses. It does NOT say either group unsubscribes MORE - that
# would need a mailed/reachable denominator per group, which does not exist until Phase 2.
_l2_dims = ["prod_band", "tenure_band", "age_band", "prof_quintile", "high_potential"]
_cards = leaver_banded.filter(F.col("program") == "CARDS")
_noncards = leaver_banded.filter(F.col("program") != "CARDS")
_n_cards_total = _cards.count()
_n_noncards_total = _noncards.count()

_l2_frames = []
for _dim in _l2_dims:
    cb = (_cards.groupBy(_dim).count()
          .withColumn("pct_cards_leavers", F.round(100.0 * F.col("count") / _n_cards_total, 2))
          .withColumnRenamed("count", "cards_n"))
    nb = (_noncards.groupBy(_dim).count()
          .withColumn("pct_noncards_leavers", F.round(100.0 * F.col("count") / _n_noncards_total, 2))
          .withColumnRenamed("count", "noncards_n"))
    m = (cb.join(nb, _dim, "outer")
         .withColumn("pct_cards_leavers", F.coalesce(F.col("pct_cards_leavers"), F.lit(0.0)))
         .withColumn("pct_noncards_leavers", F.coalesce(F.col("pct_noncards_leavers"), F.lit(0.0)))
         .withColumn("cards_n", F.coalesce(F.col("cards_n"), F.lit(0)))
         .withColumn("noncards_n", F.coalesce(F.col("noncards_n"), F.lit(0)))
         .withColumn("ratio", F.round(F.col("pct_cards_leavers") /
                                       F.when(F.col("pct_noncards_leavers") == 0, None)
                                        .otherwise(F.col("pct_noncards_leavers")), 2))
         .withColumn("segment_dim", F.lit(_dim))
         .withColumnRenamed(_dim, "segment_value")
         .withColumn("segment_value", F.col("segment_value").cast("string"))
         .select("segment_dim", "segment_value", "pct_cards_leavers", "pct_noncards_leavers",
                  "ratio", "cards_n", "noncards_n"))
    _l2_frames.append(m)

l2 = _l2_frames[0]
for _f in _l2_frames[1:]:
    l2 = l2.unionByName(_f)
_w_l2 = Window.partitionBy("segment_dim").orderBy(F.desc("ratio"))
T("L2 - WHO, cards vs non-cards leavers | ratio = pct_cards_leavers / pct_noncards_leavers, ratio "
  "> 1 = over-represented among cards leavers relative to non-cards leavers | composition vs "
  "composition, NO denominator, does NOT claim either group unsubscribes more often",
  l2.withColumn("_rn", F.row_number().over(_w_l2)).orderBy("segment_dim", "_rn").drop("_rn"))

# %% [16] L3 - DOOR-CLOSING. GRAIN: distinct leaver client. A client who leaves while holding one
# product closes the ONLY email door cards had into cross-selling every OTHER product they don't
# hold. VOLUME + MIX ONLY - NO RATES.
_single_overall = leaver_banded.filter(F.col("prod_band") == "1").count()
display(Markdown("**L3 - DOOR-CLOSING** · single-product leavers as % of ALL leavers, bank-wide: **" +
                  str(round(100.0 * _single_overall / _n_leavers_total, 1)) + "%** (" +
                  str(_single_overall) + " of " + str(_n_leavers_total) + ") | VOLUME + MIX ONLY - "
                  "NO RATES (no denominator until Phase 2)"))

l3_by_program = (leaver_banded.groupBy("program")
                  .agg(F.count("*").alias("leaver_clients"),
                       F.sum(F.when(F.col("prod_band") == "1", 1).otherwise(0)).alias("single_product_clients"))
                  .withColumn("pct_single_product", F.round(100.0 * F.col("single_product_clients") / F.col("leaver_clients"), 1))
                  .orderBy(F.desc("leaver_clients")))
T("L3 - DOOR-CLOSING, by program", l3_by_program)

l3_by_mne = (leaver_banded.filter(F.col("program") == "CARDS").groupBy("mne")
             .agg(F.count("*").alias("leaver_clients"),
                  F.sum(F.when(F.col("prod_band") == "1", 1).otherwise(0)).alias("single_product_clients"))
             .withColumn("pct_single_product", F.round(100.0 * F.col("single_product_clients") / F.col("leaver_clients"), 1))
             .orderBy(F.desc("leaver_clients")).limit(20))
T("L3 - DOOR-CLOSING, top cards MNEs", l3_by_mne)

l3_tibc = (leaver_banded.filter(F.col("prod_band") == "1").groupBy("tibc_mix")
           .agg(F.count("*").alias("single_product_clients")).orderBy(F.desc("single_product_clients")))
T("L3 - DOOR-CLOSING, single-product leavers by category held, bank-wide", l3_tibc)

# %% [17] L4 - HIGH POTENTIAL LOST. GRAIN: distinct leaver client. VOLUME + MIX ONLY - NO RATES.
_HP_LABEL = ("high_potential = AGE < " + str(HP_AGE_MAX) + " AND TENURE_RBC_YEARS <= " +
             str(HP_TENURE_MAX) + " AND prod_cnt <= " + str(HP_PROD_MAX))
_n_hp_total = leaver_banded.filter(F.col("high_potential")).count()
display(Markdown("**L4 - HIGH POTENTIAL LOST** · " + _HP_LABEL + " · **" +
                  str(round(100.0 * _n_hp_total / _n_leavers_total, 1)) + "%** of all leavers (" +
                  str(_n_hp_total) + " of " + str(_n_leavers_total) + ") | VOLUME + MIX ONLY - NO "
                  "RATES (no denominator until Phase 2)"))

l4_by_program = (leaver_banded.groupBy("program")
                  .agg(F.count("*").alias("leaver_clients"),
                       F.sum(F.col("high_potential").cast("int")).alias("high_potential_clients"))
                  .withColumn("pct_high_potential", F.round(100.0 * F.col("high_potential_clients") / F.col("leaver_clients"), 1))
                  .orderBy(F.desc("leaver_clients")))
T("L4 - HIGH POTENTIAL LOST, by program | " + _HP_LABEL, l4_by_program)

l4_by_mne = (leaver_banded.filter(F.col("program") == "CARDS").groupBy("mne")
             .agg(F.count("*").alias("leaver_clients"),
                  F.sum(F.col("high_potential").cast("int")).alias("high_potential_clients"))
             .withColumn("pct_high_potential", F.round(100.0 * F.col("high_potential_clients") / F.col("leaver_clients"), 1))
             .orderBy(F.desc("leaver_clients")).limit(20))
T("L4 - HIGH POTENTIAL LOST, top cards MNEs | " + _HP_LABEL, l4_by_mne)

# %% [18] L5 - PROF VETTING. PROF_TOT_ANNUAL percentiles by tenure_band. VOLUME + MIX ONLY - NO RATES.
display(Markdown("**L5 - PROF VETTING** · 2026-07-27 FINDING (unchanged in this rewrite): "
                  "PROF_TOT_ANNUAL medians RISE across tenure bands - roughly 19 -> 71 -> 144 -> "
                  "366 -> 805 across 0-2 / 3-5 / 6-10 / 11-20 / 20+ years. This field is CURRENT-"
                  "YEAR CONTRIBUTION, not LTV - label it **'annual profitability'** on any slide, "
                  "never 'value' or 'LTV'. A leaver flagged 'low value' by this field partly just "
                  "means 'young by construction', not low-potential."))

l5 = (leaver_banded.filter(F.col("ucp_matched") & F.col("PROF_TOT_ANNUAL").isNotNull())
      .groupBy("tenure_band")
      .agg(F.count("*").alias("leaver_clients"),
           F.expr("percentile_approx(PROF_TOT_ANNUAL, 0.25)").alias("p25_prof_annual"),
           F.expr("percentile_approx(PROF_TOT_ANNUAL, 0.5)").alias("median_prof_annual"),
           F.expr("percentile_approx(PROF_TOT_ANNUAL, 0.75)").alias("p75_prof_annual"))
      .orderBy(F.expr("CASE tenure_band "
                       "WHEN '0-2' THEN 1 WHEN '3-5' THEN 2 WHEN '6-10' THEN 3 "
                       "WHEN '11-20' THEN 4 WHEN '20+' THEN 5 ELSE 6 END")))
T("L5 - PROF_TOT_ANNUAL percentiles by tenure_band (matched leavers only)", l5)

# %% [19] CUBE + leaver_spine SAVE. CUBE GRAIN: mne x program x age_band x tenure_band x prod_band
# x prof_quintile, COUNTS ONLY (leaver_clients) - rates are a pivot-time calculation in Excel,
# never baked in here. leaver_spine is THE durable artifact - every table above re-derives from it
# alone, without touching EDW or UCP again.
cube = (leaver_banded.groupBy("mne", "program", "age_band", "tenure_band", "prod_band", "prof_quintile")
        .agg(F.count("*").alias("leaver_clients")))
_n_cube_rows = cube.count()
_n_distinct_mnes = leaver_banded.select("mne").distinct().count()
print("distinct MNEs in this run:", _n_distinct_mnes)
print("CUBE row count:", _n_cube_rows)
# This file is bank-wide/all-programs. Cube grain is mne x program x age_band x tenure_band x
# prod_band x prof_quintile - roughly 1,260 possible rows per distinct MNE (5 age x 5 tenure x 5
# prod x 5 prof, times program, is an upper bound before sparsity thins it), so past ~160 MNEs this
# assert fires on CORRECT data. If it fires: check _n_distinct_mnes (printed above) FIRST - a
# bank-wide run legitimately carrying many MNEs is the likely cause, not a band producing more
# distinct values than intended.
assert _n_cube_rows < 2_000_000, (
    "cube has " + str(_n_cube_rows) + " rows (>= 2,000,000). CHECK FIRST: distinct MNE count (" +
    str(_n_distinct_mnes) + ", printed above) - a bank-wide run spans many MNEs and ~1,260 rows/MNE "
    "is expected, not a bug. Only suspect a band explosion (age/tenure/prod/prof producing far more "
    "distinct values than intended) if MNE cardinality does not explain the row count.")

cube_pd = cube.orderBy("mne", "program", "age_band", "tenure_band", "prod_band", "prof_quintile").toPandas()

# DURABLE-FIRST SAVE ORDER - the HDFS parquet is THE artifact; csv/xlsx below are download
# conveniences for Andre's Jupyter file browser and must never be able to lose the run. No script
# in this repo uses to_excel/openpyxl/xlsxwriter, so that engine may not be installed - if to_excel
# raised BEFORE the parquet writes, an ImportError would kill this cell with neither durable copy
# on disk. Parquet (both tables) now writes and reads back FIRST; csv has no dependency risk and
# comes next; xlsx is attempted last, inside a try/except that can never raise past this point.
cube.write.mode("overwrite").parquet(BASE + "cube")
_n_cube_after = spark.read.parquet(BASE + "cube").count()
assert _n_cube_after == _n_cube_rows, (
    "cube HDFS readback mismatch: wrote " + str(_n_cube_rows) + " read back " + str(_n_cube_after))
print("CUBE written to", BASE + "cube", "(", _n_cube_after, "rows, readback confirmed )")

leaver_spine = leaver_banded.select(
    "CLNT_NO", "TREATMENT_ID", "mne", "program", "unsub_tm", "launch_dt", "anchor", "ucp_month_end",
    "ucp_matched", "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT",
    "PROF_TOT_ANNUAL", "prod_cnt", "prod_band", "tibc_mix", "tenure_band", "age_band",
    "prof_quintile", "high_potential")

_n_spine_before = leaver_spine.count()
leaver_spine.write.mode("overwrite").parquet(BASE + "leaver_spine")
_n_spine_after = spark.read.parquet(BASE + "leaver_spine").count()
assert _n_spine_after == _n_spine_before, (
    "leaver_spine HDFS readback mismatch: wrote " + str(_n_spine_before) + " read back " + str(_n_spine_after))
print("leaver_spine saved to", BASE + "leaver_spine", "-", _n_spine_after, "rows, readback confirmed.")
print("unsub_tm is carried in this spine - any sub-window (a quarter, a single month) can be cut")
print("later by filtering this column, WITHOUT re-pulling EDW.")

# CSV DOWNLOAD COPY - no dependency risk (stdlib), unlike xlsx below. Both durable parquet copies
# (cube, leaver_spine) are already written and readback-verified above this point.
cube_pd.to_csv("unsub_value_cube.csv", index=False)
print("CUBE - wrote", len(cube_pd), "rows to unsub_value_cube.csv (download copy)")

# XLSX DOWNLOAD COPY - BEST EFFORT ONLY. openpyxl/xlsxwriter is not a dependency used anywhere else
# in this repo, so it may not be installed in this kernel. Never allowed to raise past this point -
# the durable parquet artifacts and the csv download copy are already safely on disk regardless of
# what happens here.
try:
    cube_pd.to_excel("unsub_value_cube.xlsx", index=False)
    print("CUBE - wrote", len(cube_pd), "rows to unsub_value_cube.xlsx (download copy)")
except Exception as _xlsx_err:
    print("CUBE - SKIPPED unsub_value_cube.xlsx (no working Excel writer engine - install openpyxl "
          "to enable this download copy). csv and parquet copies above are unaffected. Error was:",
          repr(_xlsx_err))

# %% [20] ONE-SCREEN SUMMARY
display(Markdown("## UNSUB VALUE MUSEUM - PHASE 1 (LEAVERS ONLY) SUMMARY"))

_summary_rows = [
    ("Leaver clients, bank-wide, 12-month window (2025-07-01 to 2026-07-01)", _n_leavers_total),
    ("Of which CARDS program", leaver_banded.filter(F.col("program") == "CARDS").count()),
    ("Of which NON_CARDS program", leaver_banded.filter(F.col("program") == "NON_CARDS").count()),
    ("Of which DEFAULT stream", leaver_banded.filter(F.col("program") == "DEFAULT").count()),
    ("Clients with >1 qualifying unsub event (collapsed to their last)", _n_multi_event),
]
T("SUMMARY - headline sizes | NO RATES ANYWHERE IN THIS TABLE", pd.DataFrame(_summary_rows, columns=["figure", "value"]))

_overall_match_pct = round(100.0 * _n_matched_total / _n_leavers_total, 1)
print("Overall UCP match rate:", _overall_match_pct, "% (see M0a for the per-program breakdown)")

_l2_pd = l2.toPandas() if hasattr(l2, "toPandas") else l2
_top_over = _l2_pd.sort_values("ratio", ascending=False).iloc[0]
print("L2 headline - strongest OVER-represented segment among CARDS leavers vs NON-CARDS leavers:",
      _top_over["segment_dim"], "=", _top_over["segment_value"], "ratio", _top_over["ratio"])

print("\nCAVEATS:")
print("- NO RATES ANYWHERE IN THIS FILE. Every number above is a count, a median, or a share of")
print("  leavers - never unsubs-per-mailed. There is no stayer pool in Phase 1, so there is no")
print("  denominator to build a rate from. Do not compute one from this file's outputs.")
print("- LAST-TOUCH ATTRIBUTION: the MNE on a leaver's attributed unsub event is the email that")
print("  carried the click/unsubscribe action - a client on several lists who unsubscribes once is")
print("  attributed to whichever campaign's link they used, which is not necessarily the campaign")
print("  that drove the decision. Heavy senders structurally inherit more of the blame.")
print("- PROF_TOT_ANNUAL is CURRENT-YEAR CONTRIBUTION, not LTV (see L5). Label it 'annual")
print("  profitability' on any slide, never 'value' or 'LTV'.")
print("- prof_quintile cut points (cell [13]) come from the LEAVER population itself - a WITHIN-")
print("  LEAVER ranking, not value relative to the reachable base. Phase 2 replaces these with cuts")
print("  from the mailed base.")
print("- Band cut points (TENURE_EDGES, AGE_EDGES) and the high_potential thresholds (HP_AGE_MAX,")
print("  HP_TENURE_MAX, HP_PROD_MAX) are OUR PARAMETERS, editable in cell [13] - not documented or")
print("  standardised anywhere else in the repo.")
print("- Clients unmatched in UCP are counted in M0, not silently dropped - see M0a/b/c for the")
print("  size and shape of this population before trusting any median in L1/L2/L3/L4/L5/CUBE.")

# %% [21] SYNTAX CHECK - parse this file's own source before handing it off. BEST EFFORT: __file__
# is undefined in a Jupyter cell, so the fallback is a bare relative filename - if the kernel's cwd
# differs from this file's directory, open() raises FileNotFoundError. That should never surface as
# a red error at the end of an otherwise-successful run, so it is caught and downgraded to a skip
# message rather than allowed to raise.
try:
    import ast

    with open(__file__ if "__file__" in dir() else "unsub_value_museum.py", "r", encoding="utf-8") as _f:
        _source = _f.read()
    ast.parse(_source)
    print("ast.parse PASSED - script is syntactically valid.")
except Exception as _syntax_check_err:
    print("SYNTAX CHECK SKIPPED - could not locate/parse this file's own source from the current "
          "working directory (harmless; every cell above already ran successfully). Error was:",
          repr(_syntax_check_err))

# %% [22] OPT-IN CLEANUP - drop the raw pull, keep the outputs. unsubs_12m/q1..q4 is SCRATCH: it
# exists only so a kernel death mid-run doesn't cost a re-pull from EDW. Once leaver_spine and cube
# (cell [19]) are verified landed, every downstream table (M0/L1-L5/CUBE) re-derives from
# leaver_spine alone - the raw pull is then redundant weight sitting on HDFS. Manifests under
# _meta/ are KEPT regardless (tiny, and they ARE the audit trail).

RUN_CLEANUP = False  # flip to True once you're done pulling for the day, then run this cell.
                     # Andre runs this whole file top to bottom every time - if this defaulted to
                     # True, cleanup would fire on every run and force a re-pull on the very next one.

if not RUN_CLEANUP:
    print("CLEANUP skipped (RUN_CLEANUP = False) - raw pull retained for restartability")
else:
    # GUARD FIRST - refuse to delete anything unless BOTH durable outputs read back non-empty.
    _spine_landed = landed("leaver_spine")
    _cube_landed = landed("cube")
    _spine_ok = _spine_landed and spark.read.parquet(BASE + "leaver_spine").count() > 0
    _cube_ok = _cube_landed and spark.read.parquet(BASE + "cube").count() > 0

    if not (_spine_ok and _cube_ok):
        print("REFUSAL - CLEANUP STOPPED. leaver_spine readable-and-nonempty:", _spine_ok,
              "| cube readable-and-nonempty:", _cube_ok)
        raise RuntimeError(
            "Refusing to delete the raw pull: leaver_spine and/or cube did not read back non-empty "
            "from HDFS. Re-run cell [19] and confirm BOTH readback prints succeed before running "
            "CLEANUP again. NOTHING WAS DELETED.")

    print("GUARD PASSED - leaver_spine and cube both read back non-empty. Proceeding with cleanup.")

    # build the exact raw paths from the SAME constant the pull cell uses, so this list can never
    # drift out of sync with what was actually landed - no shell wildcards, every path listed.
    _raw_paths = ["unsubs_12m/" + _q for _q in _P1_QUARTERS]

    print("Deleting", len(_raw_paths), "raw landing paths under", BASE, ":")
    for _p in _raw_paths:
        _full = BASE + _p
        print("  rm -r -f", _full)
        get_ipython().system("hdfs dfs -rm -r -f " + _full)

    print("\n--- BASE listing after cleanup ---")
    get_ipython().system("hdfs dfs -ls " + BASE)

    # re-verify: the two durable outputs must STILL read back fine after the deletes above
    _spine_after = spark.read.parquet(BASE + "leaver_spine").count()
    _cube_after = spark.read.parquet(BASE + "cube").count()
    assert _spine_after > 0, ("leaver_spine unreadable/empty AFTER cleanup - this should be "
                               "impossible (cleanup only touches the raw path), investigate immediately")
    assert _cube_after > 0, ("cube unreadable/empty AFTER cleanup - this should be impossible "
                              "(cleanup only touches the raw path), investigate immediately")
    print("POST-CLEANUP CHECK PASSED - leaver_spine (", _spine_after, "rows ) and cube (",
          _cube_after, "rows ) both still read back fine.")

    _cleanup_report = pd.DataFrame(
        [{"dataset": _p, "status": "REMOVED"} for _p in _raw_paths]
        + [{"dataset": "leaver_spine", "status": "KEPT (durable output)"},
           {"dataset": "cube", "status": "KEPT (durable output)"},
           {"dataset": "ucp_spine", "status": "KEPT (derived - regenerable, but cheap to keep and saves the 30-partition UCP scan)"},
           {"dataset": "_meta/* manifests", "status": "KEPT (audit trail)"}]
    )
    T("CLEANUP REPORT - dataset -> KEPT / REMOVED", _cleanup_report)

# %% [23] PHASE 2 STUB - stayers, rates, indirect standardisation. NOT BUILT YET. Comments only,
# no code - this is a fill-in for the next session, not a redesign.
#
# WHY PHASE 2 IS SEPARATE: the bank-wide SEND pull that would give Phase 1 a denominator was
# killed 2026-07-27 - the probe showed 143M send events / ~99M distinct client-MNE pairs across
# Jan-May, hours of pandas transfer, and ~80% of that volume was campaign detail for programs
# entirely outside cards. Do NOT retry that pull blind - those are the numbers that killed it.
#
# WHAT PHASE 2 ADDS, scoped to make it affordable:
#   - STAYER PULL, CARDS MNEs ONLY (not bank-wide) - the cards-Q2 send pull in
#     archaeology/28_unsub_value_ucp.py / cpc_reservoir_extract.py was 6.3M rows, i.e. tractable at
#     roughly two orders of magnitude smaller than the bank-wide attempt. Server-side MNE filter
#     (SUBSTR(TREATMENT_ID,8,3) IN CARDS_MNES) applied in the extract SQL itself, same convention
#     as that file's cell [18]-[21].
#   - STAYER DEFINITION: mailed (disposition_cd=1) in the Phase 2 window AND no unsub event in that
#     window AND no PRIOR unsub (any program, any time) - i.e. the "excluded" bucket from earlier
#     versions of this file returns, scoped to cards only this time.
#   - ANCHOR: first exposure (first disposition_cd=1 event for that client x cards-MNE population),
#     NOT last-unsub-treatment launch. This is intentionally NOT the same anchor rule as Phase 1's
#     leavers (which anchor at their attributed/last-unsub treatment's launch) - see cell [10]'s
#     comment for why the two are still comparable: both are pre-treatment snapshots relative to
#     the treatment that acted on that client, even though "the treatment that acted" means
#     something different for each side.
#   - RATES: leavers / (leavers + stayers), computed per band and per campaign, once stayers exist
#     as a real population to divide by. This is the FIRST point in this project where a rate is
#     defensible - Phase 1 has none because it has no denominator.
#   - INDIRECT STANDARDISATION on the targeting mix (age_band x tenure_band x prod_band), same
#     logic as the retired Table B's expected_per_1000: campaigns reach different populations by
#     construction, so a raw rate conflates targeting with performance. Rebuild that standardisation
#     once the stayer pool exists.
#   - PROF_QUINTILE CUTS should be recomputed from the CARDS-Q2 MAILED base (leavers + stayers
#     combined, matched clients only) at that point, replacing Phase 1's leaver-only cuts (cell
#     [13]) - state the new cut points explicitly when they change, exactly as this file's caveats
#     section states Phase 1's.
#   - This file's leaver_spine (cell [19]) is Phase 1's contribution to Phase 2 - re-read it rather
#     than re-pulling the 12-month leaver window from EDW; Phase 2 only needs to ADD the stayer pull.
