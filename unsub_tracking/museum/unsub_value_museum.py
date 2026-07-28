# UNSUB VALUE - MUSEUM. The value of an unsubscribe: who leaves, who stays, and how campaigns compare.
#
# WINDOW: sends and unsubs in March, April, May 2026. Bank-wide.
# ANCHOR: ONE UCP snapshot, 2026-02-28 - the month-end immediately before the window. Every client
#   on every side of every comparison is profiled at the same pre-treatment moment, which is what
#   makes leaver-vs-stayer a real comparison. The 12-month version of this file anchored each client
#   at their own treatment's launch, which fanned out to 30 UCP partitions and got the session killed
#   by YARN (2026-07-27); that machinery is gone. Git holds it.
#
# THREE SENDER TIERS, and why:
#   T1 senders_by_mne  - server-side COUNT(DISTINCT client) per MNE. ~200 rows. Bank-wide.
#                        Gives unsub_per_1000 for EVERY campaign in the bank at no transfer cost.
#   T2 senders_base    - DISTINCT CLNT_NO, bank-wide, one column. The mailed population, for the
#                        bank-wide baseline profile.
#   T3 senders_cards   - DISTINCT (CLNT_NO, mne), CARDS MNEs only, filtered server-side. Per-campaign
#                        sender/stayer profiles. Cards-only because bank-wide client x MNE is ~99M
#                        pairs off 143M send events - that pull was killed 2026-07-27. Non-cards
#                        campaigns therefore get counts and a rate, but no sender profile.
#
# POPULATIONS (one row per client):
#   leaver      - mailed in window AND unsubscribed in window
#   already_out - mailed in window, no unsub in window, but unsubscribed BEFORE it (the leak; 53,726
#                 clients in the Q2 run). Split out, NOT counted as a stayer - they were never
#                 reachable, so calling them "chose to stay" would be false.
#   stayer      - mailed, no unsub in window, no unsub before it
#
# ENGINE SPLIT: EDW (teradatasql) for vendor feedback; HDFS/Spark for UCP. There is no EDW
#   client-attribute table - the split is unavoidable, not a preference.
#
# OUTPUT CONTRACT
#   DURABLE : client_spine, summary (parquet), summary_csv - all on HDFS.
#   SCRATCH : the five raw EDW landings. They exist only so a kernel death mid-pull costs nothing.
#             Removable via the opt-in cleanup cell once the durable outputs verify.
#   NOTHING is written to the notebook working directory - local writes do not work from this
#   YARN/Spark kernel. pandas .to_csv / .to_excel appear nowhere in this file.
#
# Date: 2026-07-27.

# %% [1] SETUP - bootstrap, EDW connect, land()/landed(), T(), norm_clnt.
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

# numpy>=1.24 removed np.bool/np.object/np.int/np.float, but Spark 3.3's pandas->Spark conversion
# still references np.bool for BooleanType columns ("AttributeError: module 'numpy' has no
# attribute 'bool'", seen on this kernel 2026-07-27). Restoring the aliases is the minimal fix.
import numpy as np
for _alias, _builtin in (("bool", bool), ("object", object), ("int", int),
                         ("float", float), ("str", str)):
    if not hasattr(np, _alias):
        setattr(np, _alias, _builtin)

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 220)
pd.set_option("display.max_rows", 80)

# ---- WINDOW. Every SQL bound below is derived from these two constants. Change them here only. ----
WIN_START = "2026-03-01"
WIN_END = "2026-06-01"
UCP_ANCHOR = "2026-02-28"          # month-end BEFORE the window: pre-treatment for every client
PRIOR_FLOOR = "2024-01-01"         # house rule: no scan reaches below 2024
WIN_MONTHS = [("m2026_03", "2026-03-01", "2026-04-01", "2026-02-01", "2026-05-01"),
              ("m2026_04", "2026-04-01", "2026-05-01", "2026-03-01", "2026-06-01"),
              ("m2026_05", "2026-05-01", "2026-06-01", "2026-04-01", "2026-07-01")]

# CARDS_MNES: sourced from UNSUB_TRACKING_KNOWLEDGE.md section 4 and cross-checked against
# archaeology/email_active_mnes.md. CTU and O2P are deliberately EXCLUDED per Andre 2026-07-26 -
# they involve cards but are reported inside async, out of the cards package.
CARDS_MNES = frozenset({"PCQ", "PCL", "PCD", "AUH", "CLI", "MVP", "CRV"})
CARDS_SQL_LIST = ", ".join("'" + m + "'" for m in sorted(CARDS_MNES))

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")

TD_HOST = "Teradata-dns-sysa.fg.rbc.com"
EDW = teradatasql.connect(host=TD_HOST, user=username, password=password, logmech="LDAP")


def edw_pd(sql, chunksize=1_000_000):
    import time
    parts, n, t0 = [], 0, time.time()
    for c in pd.read_sql(sql, EDW, chunksize=chunksize):
        parts.append(c); n += len(c)
        print("  ...", n, "rows pulled,", int(time.time() - t0), "s elapsed", flush=True)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


# PROOF, not prints: round-trip the connection and show what the SERVER returned.
_cur = EDW.cursor()
_cur.execute("SELECT USER, SESSION, CURRENT_TIMESTAMP")
print("EDW round-trip (teradatasql) returned:", _cur.fetchall())
_cur.close()

BASE = "hdfs:///user/427966379/unsub_value_museum/"
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


def _sqlkey(sql):
    return hashlib.md5(re.sub(r"\s+", " ", sql).strip().upper().encode()).hexdigest()


def landed(name):
    # "absent" and "cannot check" are NOT the same thing. A broken session must stop the run, not
    # look like an empty reservoir - that mistake re-pulled everything once already (2026-07-27).
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
    # Same SQL already landed -> SKIP. Changed SQL -> re-pull and overwrite. Readable path with no
    # manifest = a killed mid-write -> treated as not landed. The SQL here is the source of truth.
    meta = BASE + "_meta/" + name.replace("/", "_")
    key = _sqlkey(sql)
    if landed(name) and not replace:
        try:
            old = spark.read.parquet(meta).collect()[0]
            if old["sql_md5"] == key:
                print(name, ": already landed with SAME query,", old["rows"], "rows (", old["landed_at"], ") - SKIP")
                return
            print(name, ": QUERY CHANGED since it landed", old["landed_at"], "- re-pulling and OVERWRITING")
        except Exception as e:
            msg = str(e)
            if ("Path does not exist" in msg) or ("PATH_NOT_FOUND" in msg) or ("FileNotFound" in msg):
                print(name, ": readable but NO manifest (partial/killed write) - re-pulling and OVERWRITING")
            else:
                raise RuntimeError(name + ": data readable but manifest CANNOT BE VERIFIED - refusing "
                                   "to re-pull on an unverifiable state. Underlying error: " + msg[:300])
    pdf = edw_pd(sql)
    assert len(pdf) > 0, name + " pulled zero rows - investigate before proceeding"
    _to_spark(pdf, name).write.mode("overwrite").parquet(BASE + name)
    nback = spark.read.parquet(BASE + name).count()
    assert nback == len(pdf), name + " HDFS readback mismatch: pulled " + str(len(pdf)) + " readback " + str(nback)
    spark.createDataFrame([(name, key, sql, __import__("datetime").datetime.now().isoformat(), len(pdf))],
                          ["name", "sql_md5", "sql_text", "landed_at", "rows"]).write.mode("overwrite").parquet(meta)
    print(name, ": landed", len(pdf), "rows, HDFS readback confirms", nback, "| manifest written")


def norm_clnt(col):
    # decimal(18,0) FIRST: CLNT_NO arrives as pandas float64 and float->string renders scientific
    # notation ('1.56314759E8'), which never matches UCP's integer strings. Cost a full debug cycle.
    return F.regexp_replace(F.trim(col.cast("decimal(18,0)").cast("string")), "^0+", "")


def T(label, df):
    """Render a titled table AND return it. Timestamps are stringified first - pandas 2.x rejects
    Spark's unit-less datetime64."""
    out = df
    if hasattr(out, "toPandas"):
        for f in out.schema.fields:
            if f.dataType.typeName() in ("timestamp", "date"):
                out = out.withColumn(f.name, F.col(f.name).cast("string"))
        out = out.toPandas()
    display(Markdown("**" + label + "**  ·  " + str(len(out)) + " rows"))
    display(out)
    return out


print("helpers defined | BASE =", BASE, "| window", WIN_START, "->", WIN_END, "| UCP anchor", UCP_ANCHOR)

# %% [2] FAIL-FAST GATE - prove the session and HDFS before any pull.
_ = spark.range(1).count()
try:
    _ = (spark.read.option("basePath", UCP_BASE).parquet(UCP_BASE + "MONTH_END_DATE=" + UCP_ANCHOR)
         .limit(1).collect())
except Exception as e:
    raise RuntimeError("Cannot read the UCP anchor partition " + UCP_ANCHOR + " at " + UCP_BASE +
                       " - either HDFS is unreachable from this session or that month-end does not "
                       "exist. Fix before pulling. Underlying error: " + str(e)[:300])
print("GATE PASSED - spark live, HDFS reachable, UCP anchor partition", UCP_ANCHOR, "readable.")

# %% [3] SIZE PROBE - event-only COUNT(*), no join, no DISTINCT. Gated on landed() so a completed
# reservoir costs nothing on a whole-file rerun.
_unlanded = [m for m in WIN_MONTHS if not landed("unsubs_3m/" + m[0])]
if not _unlanded:
    print("SIZE PROBE - all months already landed - SKIP")
else:
    _rows = []
    for _name, _ds, _de, _ls, _le in _unlanded:
        for _cd, _lab in ((1, "sends"), (4, "unsubs")):
            _p = edw_pd("""
SELECT COUNT(*) AS event_rows
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_cd = %d
  AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
""" % (_cd, _ds, _de))
            _rows.append({"month": _name, "kind": _lab, "event_rows": int(_p["event_rows"][0])})
    T("SIZE PROBE - raw event volume per month (upper bound on each pull; the pulls dedupe below this)",
      pd.DataFrame(_rows))

# %% [4] PULL - unsubs, one cell per month. disposition_cd=4. The derived table dedupes EVENT keys
# BEFORE joining MASTER, which is what keeps this off spool error 2646.
_UNSUB_SQL = """
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
"""
for _name, _ds, _de, _ls, _le in WIN_MONTHS:
    land("unsubs_3m/" + _name, _UNSUB_SQL % (_ds, _de, _ls, _le))

# %% [5] PULL - TIER 1: senders per MNE, aggregated SERVER-SIDE. ~200 rows come back. We never pull
# client-level sender rows bank-wide: that is ~99M client x MNE pairs off 143M send events, and it
# killed the session on 2026-07-27. Counting how many clients a campaign mailed never needed them.
land("senders_by_mne", """
SELECT SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne, COUNT(DISTINCT m.CLNT_NO) AS senders
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '2026-02-01' AND m.load_tm < DATE '2026-07-01'
GROUP BY 1
""" % (WIN_START, WIN_END))

# %% [6] PULL - TIER 2: the mailed population, bank-wide. DISTINCT CLNT_NO only - one narrow column.
# Monthly so a kill costs one month. This is the baseline every comparison in this file rests on.
_SENDBASE_SQL = """
SELECT DISTINCT m.CLNT_NO
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '%s' AND m.load_tm < DATE '%s'
"""
for _name, _ds, _de, _ls, _le in WIN_MONTHS:
    land("senders_base/" + _name, _SENDBASE_SQL % (_ds, _de, _ls, _le))

# %% [7] PULL - TIER 3: client x MNE for CARDS campaigns only, filtered server-side. This is what
# makes per-campaign sender and stayer profiles possible. Cards-only by necessity, not preference.
_SENDCARDS_SQL = """
SELECT DISTINCT m.CLNT_NO, SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
      AND SUBSTR(TREATMENT_ID, 8, 3) IN (%s)
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '%s' AND m.load_tm < DATE '%s'
"""
for _name, _ds, _de, _ls, _le in WIN_MONTHS:
    land("senders_cards/" + _name, _SENDCARDS_SQL % (_ds, _de, CARDS_SQL_LIST, _ls, _le))

# %% [8] PULL - prior unsubscribers. Needed to keep the stayer pool honest: a client who opted out
# last year and still got mail is not someone who "chose to stay".
land("prior_unsubs", """
SELECT DISTINCT m.CLNT_NO
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '2023-12-01' AND m.load_tm < DATE '2026-04-01'
""" % (PRIOR_FLOOR, WIN_START))

# %% [9] READBACK - normalise CLNT_NO everywhere, prove every landing is present and non-empty.
def _rd(path):
    return spark.read.parquet(BASE + path).withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO")))


unsubs_raw = _rd("unsubs_3m/*")
senders_all_raw = _rd("senders_base/*").select("CLNT_NO").distinct()
senders_cards_raw = _rd("senders_cards/*").select("CLNT_NO", "mne").distinct()
prior_raw = _rd("prior_unsubs").select("CLNT_NO").distinct()
senders_mne = spark.read.parquet(BASE + "senders_by_mne")

_counts = [
    {"dataset": "unsubs_3m (client x treatment)", "rows": unsubs_raw.count(),
     "distinct_clients": unsubs_raw.select("CLNT_NO").distinct().count()},
    {"dataset": "senders_base (distinct clients mailed)", "rows": senders_all_raw.count(),
     "distinct_clients": senders_all_raw.count()},
    {"dataset": "senders_cards (client x cards MNE)", "rows": senders_cards_raw.count(),
     "distinct_clients": senders_cards_raw.select("CLNT_NO").distinct().count()},
    {"dataset": "prior_unsubs (clients out before window)", "rows": prior_raw.count(),
     "distinct_clients": prior_raw.count()},
    {"dataset": "senders_by_mne (aggregate)", "rows": senders_mne.count(), "distinct_clients": None},
]
T("READBACK - every landing, rows and distinct clients", pd.DataFrame(_counts))
for _c in _counts:
    assert _c["rows"] > 0, _c["dataset"] + " read back empty - investigate before proceeding"

# %% [10] MNE / PROGRAM - one derivation, used on BOTH sides of every join. The senders aggregate
# and the leaver frame previously derived mne differently (raw SUBSTR vs a DEFAULT-aware rule),
# which mis-joins the blank/DEFAULT stream. Same rule for everyone now.
def add_mne_program(df, tid_col=None, mne_col=None):
    if mne_col is None:
        df = df.withColumn("mne_raw", F.trim(F.substring(F.col(tid_col), 8, 3)))
    else:
        df = df.withColumn("mne_raw", F.trim(F.col(mne_col)))
    df = df.withColumn("mne", F.when((F.col("mne_raw") == "") | F.col("mne_raw").isNull(),
                                     F.lit("DEFAULT")).otherwise(F.col("mne_raw")))
    return df.withColumn(
        "program",
        F.when(F.col("mne") == "DEFAULT", F.lit("DEFAULT"))
         .when(F.col("mne").isin(*sorted(CARDS_MNES)), F.lit("CARDS"))
         .otherwise(F.lit("NON_CARDS"))).drop("mne_raw")


senders_mne = add_mne_program(senders_mne, mne_col="mne")
senders_cards_raw = add_mne_program(senders_cards_raw, mne_col="mne")

# %% [11] UCP SCHEMA PROBE - hard gate. Nothing downstream assumes a column name.
_REQUESTED = ["CLNT_NO", "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT",
              "C_TOT_CNT", "PROF_TOT_ANNUAL"]
_ucp_probe = spark.read.option("basePath", UCP_BASE).parquet(UCP_BASE + "MONTH_END_DATE=" + UCP_ANCHOR)
_actual = set(_ucp_probe.columns)
T("UCP SCHEMA PROBE - requested columns vs the live schema at " + UCP_ANCHOR,
  pd.DataFrame([{"column": c, "status": "PRESENT" if c in _actual else "MISSING"} for c in _REQUESTED]))
UCP_COLS = [c for c in _REQUESTED if c in _actual]
HAS_CLNT_TYP = "CLNT_TYP" in _actual
_missing = [c for c in _REQUESTED if c not in _actual]
assert not _missing, ("UCP is missing required columns " + str(_missing) + " at " + UCP_ANCHOR +
                      " - the four measured attributes cannot be built without them.")
print("UCP_COLS fixed:", UCP_COLS, "| HAS_CLNT_TYP =", HAS_CLNT_TYP)

# %% [12] UCP READ - ONE partition, one join. Written to HDFS rather than cached: writing severs the
# lineage, so a session death costs this step alone and never re-runs the scan.
_ucp = _ucp_probe
if HAS_CLNT_TYP:
    _ucp = _ucp.filter(F.trim(F.col("CLNT_TYP")) == "Personal")
_ucp = _ucp.select(*UCP_COLS).withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO")))

# UCP is EXPECTED to be one row per client per month-end but is not guaranteed to be: the
# 2026-02-28 partition carries 1 duplicate in 15.3M (2026-07-27). Dedupe deterministically and
# REPORT it - halting the run over a stray row would be absurd. Hard-fail only if duplication is
# material (>0.1%), which would mean the grain is not what we think it is.
_n_ucp_raw = _ucp.count()
_dedup_w = Window.partitionBy("CLNT_NO").orderBy(
    *[F.col(c).asc_nulls_last() for c in UCP_COLS if c != "CLNT_NO"])
_ucp = (_ucp.withColumn("_rn", F.row_number().over(_dedup_w))
        .filter(F.col("_rn") == 1).drop("_rn"))
_n_ucp_pre = _ucp.count()
_dupes = _n_ucp_raw - _n_ucp_pre
_dupe_pct = 100.0 * _dupes / _n_ucp_raw if _n_ucp_raw else 0.0
print("UCP at", UCP_ANCHOR, ":", _n_ucp_raw, "rows ->", _n_ucp_pre, "after dedupe on CLNT_NO (",
      _dupes, "duplicate rows dropped, %.4f%% )" % _dupe_pct)
assert _dupe_pct < 0.1, ("UCP duplication at " + UCP_ANCHOR + " is %.3f%%" % _dupe_pct +
                         " - that is structural, not stray rows. The grain is not one row per "
                         "client per month-end; investigate before trusting any attribute.")

_ucp.write.mode("overwrite").parquet(BASE + "ucp_spine")
ucp = spark.read.parquet(BASE + "ucp_spine")
_n_ucp = ucp.count()
assert _n_ucp == _n_ucp_pre, "ucp_spine readback mismatch: wrote " + str(_n_ucp_pre) + " read " + str(_n_ucp)
print("ucp_spine:", _n_ucp, "rows at", UCP_ANCHOR, "- readback confirmed, one row per client.")

# %% [13] POPULATIONS - one row per client, three mutually exclusive buckets.
_w = Window.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").desc(), F.col("TREATMENT_ID").desc())
leaver_last = (add_mne_program(unsubs_raw, tid_col="TREATMENT_ID")
               .withColumn("_rn", F.row_number().over(_w)).filter(F.col("_rn") == 1).drop("_rn")
               .select("CLNT_NO", "TREATMENT_ID", "mne", "program", "unsub_tm"))

mailed = senders_all_raw.select("CLNT_NO")
clients = (mailed
           .join(leaver_last, "CLNT_NO", "left")
           .join(prior_raw.withColumn("_prior", F.lit(1)), "CLNT_NO", "left")
           .withColumn("bucket",
                       F.when(F.col("unsub_tm").isNotNull(), F.lit("leaver"))
                        .when(F.col("_prior") == 1, F.lit("already_out"))
                        .otherwise(F.lit("stayer")))
           .drop("_prior"))

_n_mailed = mailed.count()
_bucket_counts = clients.groupBy("bucket").count().toPandas()
_bucket_counts["pct_of_mailed"] = (100.0 * _bucket_counts["count"] / _n_mailed).round(2)
T("BUCKETS - every client mailed in " + WIN_START + ".." + WIN_END + " (leaver / already_out / stayer)",
  _bucket_counts)
assert int(_bucket_counts["count"].sum()) == _n_mailed, (
    "buckets do not sum to the mailed population - a client fell into none or several")
print("already_out = mailed despite an unsub before the window (the leak). Kept OUT of the stayer",
      "pool: they were not reachable, so counting them as having 'chosen to stay' would be false.")

# %% [14] JOIN UCP. Clients ACQUIRED during Mar-May cannot exist in the 2026-02-28 snapshot and will
# be unmatched - that is a real limit of a single pre-window anchor, counted in M0, never hidden.
_before = clients.count()
clients = clients.join(ucp, "CLNT_NO", "left").withColumn(
    "ucp_matched", F.when(F.col("AGE").isNotNull(), F.lit(True)).otherwise(F.lit(False)))
_after = clients.count()
assert _after == _before, ("UCP join fanned out: " + str(_before) + " -> " + str(_after) +
                           " - UCP is not unique per client at this anchor")
clients = clients.cache()

# %% [15] M0 - COVERAGE & NULLS. Prints BEFORE any finding.
m0a = (clients.groupBy("bucket")
       .agg(F.count("*").alias("clients"),
            F.sum(F.col("ucp_matched").cast("int")).alias("matched"))
       .withColumn("unmatched", F.col("clients") - F.col("matched"))
       .withColumn("match_pct", F.round(100.0 * F.col("matched") / F.col("clients"), 1)))
T("M0a - UCP match rate by bucket | anchor " + UCP_ANCHOR, m0a)

T("M0b - unmatched clients by MNE (leavers only - the only bucket carrying an MNE). SBB and other "
  "business-banking programs matched 0% on 2026-07-27: personal UCP cannot contain business clients",
  (clients.filter((~F.col("ucp_matched")) & F.col("mne").isNotNull())
   .groupBy("mne", "program").count().orderBy(F.desc("count")).limit(15)))

_n_matched = clients.filter(F.col("ucp_matched")).count()
_nulls = []
for _c in UCP_COLS:
    _n = clients.filter(F.col("ucp_matched") & F.col(_c).isNull()).count()
    _nulls.append({"field": _c, "null_among_matched": _n,
                   "pct_null_among_matched": round(100.0 * _n / _n_matched, 2) if _n_matched else 0.0})
_nulls_pd = pd.DataFrame(_nulls)
T("M0c - nulls among MATCHED clients, per UCP field (n matched = " + str(_n_matched) + ")", _nulls_pd)
_worst = _nulls_pd["pct_null_among_matched"].max()
print(("VERDICT: worst field is %.2f%% null among matched - " % _worst) +
      ("acceptable, medians below are trustworthy." if _worst < 5
       else "OVER 5% - every median downstream using that field is suspect."))

# %% [16] BANDING - one function, applied identically to every population. Cut points are OUR choice;
# none are documented in the repo. Edit them here and nowhere else.
HP_AGE, HP_TENURE, HP_PRODS = 35, 5, 2          # high_potential thresholds
_prof_src = clients.filter(F.col("ucp_matched") & F.col("PROF_TOT_ANNUAL").isNotNull())
PROF_CUTS = _prof_src.approxQuantile("PROF_TOT_ANNUAL", [0.2, 0.4, 0.6, 0.8], 0.01)
assert len(PROF_CUTS) == 4, ("no non-null PROF_TOT_ANNUAL among matched clients - cannot cut "
                             "quintiles; check M0c before going further")
T("PROF QUINTILE CUT POINTS - computed over ALL MAILED matched clients (the reachable base), so "
  "'top quintile' means valuable relative to who we can actually reach",
  pd.DataFrame([{"p20": PROF_CUTS[0], "p40": PROF_CUTS[1], "p60": PROF_CUTS[2], "p80": PROF_CUTS[3]}]))


def apply_bands(df):
    df = df.withColumn("prod_cnt", sum(F.coalesce(F.col(c), F.lit(0))
                                       for c in ("T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT")))
    df = df.withColumn("prod_band",
                       F.when(F.col("prod_cnt") <= 0, "0").when(F.col("prod_cnt") == 1, "1")
                        .when(F.col("prod_cnt") == 2, "2").when(F.col("prod_cnt") <= 4, "3-4")
                        .otherwise("5+"))
    _held = [(F.coalesce(F.col(c), F.lit(0)) > 0).cast("int") for c in
             ("T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT")]
    df = df.withColumn("_n_cat", _held[0] + _held[1] + _held[2] + _held[3])
    df = df.withColumn("tibc_mix",
                       F.when(F.col("_n_cat") > 1, "multi")
                        .when(F.coalesce(F.col("T_TOT_CNT"), F.lit(0)) > 0, "T only")
                        .when(F.coalesce(F.col("I_TOT_CNT"), F.lit(0)) > 0, "I only")
                        .when(F.coalesce(F.col("B_TOT_CNT"), F.lit(0)) > 0, "B only")
                        .when(F.coalesce(F.col("C_TOT_CNT"), F.lit(0)) > 0, "C only")
                        .otherwise("none")).drop("_n_cat")
    df = df.withColumn("tenure_band",
                       F.when(F.col("TENURE_RBC_YEARS").isNull(), "unknown")
                        .when(F.col("TENURE_RBC_YEARS") <= 2, "0-2")
                        .when(F.col("TENURE_RBC_YEARS") <= 5, "3-5")
                        .when(F.col("TENURE_RBC_YEARS") <= 10, "6-10")
                        .when(F.col("TENURE_RBC_YEARS") <= 20, "11-20").otherwise("20+"))
    df = df.withColumn("age_band",
                       F.when(F.col("AGE").isNull(), "unknown")
                        .when(F.col("AGE") < 25, "<25").when(F.col("AGE") < 35, "25-34")
                        .when(F.col("AGE") < 45, "35-44").when(F.col("AGE") < 55, "45-54")
                        .when(F.col("AGE") < 65, "55-64").otherwise("65+"))
    df = df.withColumn("prof_quintile",
                       F.when(F.col("PROF_TOT_ANNUAL").isNull(), "unknown")
                        .when(F.col("PROF_TOT_ANNUAL") <= PROF_CUTS[0], "1")
                        .when(F.col("PROF_TOT_ANNUAL") <= PROF_CUTS[1], "2")
                        .when(F.col("PROF_TOT_ANNUAL") <= PROF_CUTS[2], "3")
                        .when(F.col("PROF_TOT_ANNUAL") <= PROF_CUTS[3], "4").otherwise("5"))
    return df.withColumn("high_potential",
                         (F.col("AGE") < HP_AGE) & (F.col("TENURE_RBC_YEARS") <= HP_TENURE) &
                         (F.col("prod_cnt") <= HP_PRODS))


banded = apply_bands(clients).cache()
matched = banded.filter(F.col("ucp_matched")).cache()
print("banded:", banded.count(), "clients |", matched.count(), "matched (all band stats below use "
      "the MATCHED denominator - unmatched clients have no bands and must not dilute a percentage)")

# %% [17] L1 - WHERE, by program and by MNE (leavers only). Every median and percentage uses the
# MATCHED denominator; matched_clients is a visible column so the reader can see it.
def _leaver_profile(df, keys):
    return (df.filter(F.col("bucket") == "leaver")
            .groupBy(*keys)
            .agg(F.count("*").alias("leaver_clients"),
                 F.sum(F.col("ucp_matched").cast("int")).alias("matched_clients"),
                 F.expr("percentile_approx(AGE, 0.5)").alias("median_age"),
                 F.expr("percentile_approx(TENURE_RBC_YEARS, 0.5)").alias("median_tenure_years"),
                 F.expr("percentile_approx(prod_cnt, 0.5)").alias("median_prod_cnt"),
                 F.expr("percentile_approx(PROF_TOT_ANNUAL, 0.5)").alias("median_prof_annual"),
                 F.round(100.0 * F.sum(F.when(F.col("ucp_matched") & (F.col("prod_band") == "1"), 1)
                                        .otherwise(0)) /
                         F.sum(F.col("ucp_matched").cast("int")), 1).alias("pct_single_product"),
                 F.round(100.0 * F.sum(F.when(F.col("ucp_matched") & (F.col("prof_quintile") == "5"), 1)
                                        .otherwise(0)) /
                         F.sum(F.col("ucp_matched").cast("int")), 1).alias("pct_top_prof_quintile"),
                 F.round(100.0 * F.sum(F.when(F.col("ucp_matched") & F.col("high_potential"), 1)
                                        .otherwise(0)) /
                         F.sum(F.col("ucp_matched").cast("int")), 1).alias("pct_high_potential")))


T("L1 - WHERE, by program | leavers only | medians and percentages on MATCHED clients",
  _leaver_profile(banded, ["program"]).orderBy(F.desc("leaver_clients")))
T("L1 - WHERE, top 20 MNEs by leaver volume | leavers only | matched denominators",
  _leaver_profile(banded, ["mne", "program"]).orderBy(F.desc("leaver_clients")).limit(20))

# %% [18] L6 - CAMPAIGN SUMMARY. senders (T1, bank-wide) joined to leavers: THE rate in this file.
_leavers_by_mne = (banded.filter(F.col("bucket") == "leaver").groupBy("mne", "program")
                   .agg(F.count("*").alias("unsubs")))
_prof_by_mne = _leaver_profile(banded, ["mne", "program"]).drop("leaver_clients")

l6 = (senders_mne.select("mne", "program", "senders")
      .join(_leavers_by_mne.drop("program"), "mne", "outer")
      .join(_prof_by_mne.drop("program"), "mne", "left")
      .withColumn("unsubs", F.coalesce(F.col("unsubs"), F.lit(0)))
      .withColumn("unsub_per_1000",
                  F.when(F.col("senders") > 0, F.round(1000.0 * F.col("unsubs") / F.col("senders"), 2)))
      .orderBy(F.desc("senders")))

_orphan_send = senders_mne.join(_leavers_by_mne, "mne", "left_anti").count()
_orphan_leave = _leavers_by_mne.join(senders_mne, "mne", "left_anti").count()
print("join check - MNEs with senders but no unsubs:", _orphan_send,
      "| MNEs with unsubs but no senders:", _orphan_leave,
      "(the second should be ~0; anything else means the two sides label MNE differently)")

T("L6 - CAMPAIGN SUMMARY | senders = distinct clients mailed " + WIN_START + ".." + WIN_END +
  " | profile columns describe that campaign's LEAVERS (matched only) | top 25 by senders, the CSV "
  "has every MNE | rows OVERLAP: a client mailed by several campaigns appears in several rows, so "
  "columns do not sum",
  l6.limit(25))

# %% [19] L7 - LEAVERS vs STAYERS, bank-wide. The comparison this file exists for. Both sides
# matched-only, so the denominators are equal in kind.
_DIMS = ["age_band", "tenure_band", "prod_band", "prof_quintile"]
_n_lv = matched.filter(F.col("bucket") == "leaver").count()
_n_st = matched.filter(F.col("bucket") == "stayer").count()

_frames = []
for _d in _DIMS + ["high_potential"]:
    _lv = (matched.filter(F.col("bucket") == "leaver").groupBy(F.col(_d).cast("string").alias("segment_value"))
           .agg(F.count("*").alias("leavers_n")))
    _st = (matched.filter(F.col("bucket") == "stayer").groupBy(F.col(_d).cast("string").alias("segment_value"))
           .agg(F.count("*").alias("stayers_n")))
    _frames.append(_lv.join(_st, "segment_value", "outer")
                   .withColumn("segment_dim", F.lit(_d))
                   .withColumn("leavers_n", F.coalesce(F.col("leavers_n"), F.lit(0)))
                   .withColumn("stayers_n", F.coalesce(F.col("stayers_n"), F.lit(0))))
l7 = _frames[0]
for _f in _frames[1:]:
    l7 = l7.unionByName(_f)
l7 = (l7.withColumn("pct_leavers", F.round(100.0 * F.col("leavers_n") / _n_lv, 2))
      .withColumn("pct_stayers", F.round(100.0 * F.col("stayers_n") / _n_st, 2))
      .withColumn("ratio", F.round(F.col("pct_leavers") / F.col("pct_stayers"), 2))
      .select("segment_dim", "segment_value", "pct_leavers", "pct_stayers", "ratio",
              "leavers_n", "stayers_n")
      .orderBy("segment_dim", F.desc("ratio")))
T("L7 - LEAVERS vs STAYERS, bank-wide | matched clients only (leavers n = " + str(_n_lv) +
  ", stayers n = " + str(_n_st) + ") | ratio > 1 = over-represented among LEAVERS", l7)

# %% [20] L8 - CARDS, per campaign: leavers vs that campaign's OWN mailed base. This is what
# separates "this campaign LOSES young single-product clients" from "this campaign MAILS them".
cards_send = (senders_cards_raw.join(banded.select("CLNT_NO", "bucket", "ucp_matched", "AGE",
                                                   "TENURE_RBC_YEARS", "prod_cnt", "PROF_TOT_ANNUAL",
                                                   "prod_band", "high_potential"),
                                     "CLNT_NO", "inner")
              .filter(F.col("ucp_matched")))


def _side(df, label):
    return (df.groupBy("mne")
            .agg(F.count("*").alias(label + "_clients"),
                 F.expr("percentile_approx(AGE, 0.5)").alias(label + "_median_age"),
                 F.expr("percentile_approx(TENURE_RBC_YEARS, 0.5)").alias(label + "_median_tenure"),
                 F.expr("percentile_approx(prod_cnt, 0.5)").alias(label + "_median_prod"),
                 F.expr("percentile_approx(PROF_TOT_ANNUAL, 0.5)").alias(label + "_median_prof"),
                 F.round(100.0 * F.avg(F.when(F.col("prod_band") == "1", 1.0).otherwise(0.0)), 1)
                 .alias(label + "_pct_single_product"),
                 F.round(100.0 * F.avg(F.col("high_potential").cast("double")), 1)
                 .alias(label + "_pct_high_potential")))


l8 = (_side(cards_send, "mailed")
      .join(_side(cards_send.filter(F.col("bucket") == "leaver"), "leaver"), "mne", "left")
      .join(senders_mne.select("mne", "senders"), "mne", "left")
      .withColumn("unsub_per_1000",
                  F.when(F.col("senders") > 0,
                         F.round(1000.0 * F.col("leaver_clients") / F.col("senders"), 2)))
      .withColumn("delta_median_age", F.col("leaver_median_age") - F.col("mailed_median_age"))
      .withColumn("delta_median_tenure", F.col("leaver_median_tenure") - F.col("mailed_median_tenure"))
      .withColumn("delta_median_prod", F.col("leaver_median_prod") - F.col("mailed_median_prod"))
      .withColumn("delta_pct_single_product",
                  F.round(F.col("leaver_pct_single_product") - F.col("mailed_pct_single_product"), 1))
      .orderBy(F.desc("senders")))
T("L8 - CARDS campaigns: LEAVERS vs that campaign's OWN mailed base | matched clients only | a "
  "positive delta_pct_single_product means the campaign loses single-product clients at a higher "
  "rate than it mails them - the campaign's own contribution, not its targeting", l8)

# %% [21] THE CSV - long format, three roles x four metrics, per MNE and bank-wide. Pivots natively:
# put metric on rows and role on columns and the leaver-vs-stayer gap is immediate.
# ONE groupBy, not thousands of unions. The first version of this cell looped in Python over every
# MNE x role x metric and unionByName'd the one-row results - ~2,400 stacked unions, each with its
# own count(). The driver OOM'd building the plan (java.lang.OutOfMemoryError, 2026-07-28). The
# shape below is: label each client row with its role(s), melt the four metrics into rows with
# stack(), then a single grouped aggregation. Three unions total, one shuffle.
_ROLE_ARR = F.when(F.col("bucket").isin("leaver", "stayer"),
                   F.array(F.lit("sender"), F.col("bucket"))).otherwise(F.array(F.lit("sender")))
_KEEP = ["mne", "program", "role", "CLNT_NO", "AGE", "TENURE_RBC_YEARS", "prod_cnt",
         "PROF_TOT_ANNUAL"]

# cards MNEs: client x mne, so a client mailed by two cards campaigns contributes to both.
# already_out clients are 'sender' only - they were mailed, but they are neither leaver nor stayer.
_cards_roles = (cards_send.withColumn("role", F.explode(_ROLE_ARR)).select(*_KEEP))

# bank-wide block, one row per client
_all_roles = (matched.withColumn("mne", F.lit("(ALL)")).withColumn("program", F.lit("(ALL)"))
              .withColumn("role", F.explode(_ROLE_ARR)).select(*_KEEP))

# every OTHER MNE: leavers only. Tier 3 is cards-only, so non-cards sender/stayer rows cannot be
# measured at this grain - they are emitted below with NULL stats rather than omitted, because an
# absent row reads as "no data" and that is not the same as "not measurable here".
_noncards_roles = (matched.filter((F.col("bucket") == "leaver") &
                                  (~F.col("mne").isin(*sorted(CARDS_MNES))))
                   .withColumn("role", F.lit("leaver")).select(*_KEEP))

_roles = _cards_roles.unionByName(_all_roles).unionByName(_noncards_roles)

_STACK = ("stack(4, 'age', CAST(AGE AS double), 'tenure', CAST(TENURE_RBC_YEARS AS double), "
          "'prod', CAST(prod_cnt AS double), 'prof', CAST(PROF_TOT_ANNUAL AS double)) "
          "AS (metric, value)")
csv_long = (_roles.select("mne", "program", "role", "CLNT_NO", F.expr(_STACK))
            .groupBy("mne", "program", "role", "metric")
            .agg(F.countDistinct("CLNT_NO").alias("clients"),
                 F.round(F.avg("value"), 4).alias("mean"),
                 F.expr("percentile_approx(value, 0.25)").alias("p25"),
                 F.expr("percentile_approx(value, 0.50)").alias("p50"),
                 F.expr("percentile_approx(value, 0.75)").alias("p75")))

# the null-stat placeholder rows for non-cards sender/stayer, so the CSV is honest about coverage
_placeholder = (_leavers_by_mne.filter(~F.col("mne").isin(*sorted(CARDS_MNES)))
                .select("mne", "program")
                .crossJoin(spark.createDataFrame([("sender",), ("stayer",)], ["role"]))
                .crossJoin(spark.createDataFrame([("age",), ("tenure",), ("prod",), ("prof",)],
                                                 ["metric"]))
                .withColumn("clients", F.lit(None).cast("long"))
                .withColumn("mean", F.lit(None).cast("double"))
                .withColumn("p25", F.lit(None).cast("double"))
                .withColumn("p50", F.lit(None).cast("double"))
                .withColumn("p75", F.lit(None).cast("double")))
csv_long = csv_long.unionByName(_placeholder)

csv_long = (csv_long
            .join(senders_mne.select("mne", "senders"), "mne", "left")
            .join(_leavers_by_mne.select("mne", "unsubs"), "mne", "left")
            .withColumn("unsub_per_1000",
                        F.when(F.col("senders") > 0,
                               F.round(1000.0 * F.col("unsubs") / F.col("senders"), 2)))
            .select("mne", "program", "role", "clients", "senders", "unsubs", "unsub_per_1000",
                    "metric", "mean", "p25", "p50", "p75")
            .orderBy("mne", "role", "metric"))

_n_csv = csv_long.count()
print("CSV long-format rows:", _n_csv, "(3 roles x 4 metrics per cards MNE, leaver-only for the rest)")

csv_long.write.mode("overwrite").parquet(BASE + "summary")
assert spark.read.parquet(BASE + "summary").count() == _n_csv, "summary parquet readback mismatch"
csv_long.coalesce(1).write.mode("overwrite").option("header", True).csv(BASE + "summary_csv")
assert spark.read.option("header", True).csv(BASE + "summary_csv").count() == _n_csv, \
    "summary_csv readback mismatch"
print("summary written to", BASE + "summary", "and", BASE + "summary_csv", "- readback confirmed.")
print("To fetch it for Excel, from a TERMINAL (not this kernel):")
print("  hdfs dfs -getmerge /user/427966379/unsub_value_museum/summary_csv summary.csv")

# %% [22] SAVE - client_spine. THE durable artifact: every table above re-derives from it without
# touching EDW or UCP again.
client_spine = banded.select("CLNT_NO", "bucket", "mne", "program", "TREATMENT_ID", "unsub_tm",
                             "ucp_matched", "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT",
                             "B_TOT_CNT", "C_TOT_CNT", "PROF_TOT_ANNUAL", "prod_cnt", "prod_band",
                             "tibc_mix", "tenure_band", "age_band", "prof_quintile", "high_potential")
_n_cs = client_spine.count()
client_spine.write.mode("overwrite").parquet(BASE + "client_spine")
assert spark.read.parquet(BASE + "client_spine").count() == _n_cs, "client_spine readback mismatch"
print("client_spine saved to", BASE + "client_spine", "-", _n_cs, "rows, readback confirmed.")

# %% [23] ONE-SCREEN SUMMARY
display(Markdown("## UNSUB VALUE MUSEUM - " + WIN_START + " to " + WIN_END + " - SUMMARY"))
T("SUMMARY - sizes", pd.DataFrame([
    {"figure": "Clients mailed in window (bank-wide)", "value": _n_mailed},
    {"figure": "Leavers (unsubscribed in window)", "value": _n_lv},
    {"figure": "Stayers (no unsub, none prior)", "value": _n_st},
    {"figure": "Already out (mailed despite an earlier unsub - the leak)",
     "value": int(_bucket_counts.loc[_bucket_counts["bucket"] == "already_out", "count"].sum())},
    {"figure": "UCP matched", "value": _n_matched},
]))
T("SUMMARY - strongest leaver-vs-stayer signals (|ratio| furthest from 1)",
  l7.filter(F.col("stayers_n") > 100).orderBy(F.desc("ratio")).limit(8))

display(Markdown("**CAVEATS**"))
for _c in [
    "NO RATE except unsub_per_1000 in L6/L8. Everything else is a count, a median or a share.",
    "LAST-TOUCH ATTRIBUTION: the MNE on a leaver is whichever campaign's email carried the click. "
    "A client on many lists who unsubscribes once is attributed to one campaign; heavy senders "
    "structurally inherit blame they may not have earned.",
    "OVERLAPPING DENOMINATORS in L6 and L8: a client mailed by several campaigns sits in several "
    "rows. Read down a column, never across - the columns do not sum to the bank total.",
    "PROF_TOT_ANNUAL is CURRENT-YEAR CONTRIBUTION, not LTV. Proven 2026-07-27: medians rise "
    "-25.53 -> 35.96 -> 111.54 -> 257.70 -> 555.93 across tenure bands. Label it 'annual "
    "profitability' on any slide. A client flagged low-value is partly just young.",
    "PER-CAMPAIGN SENDER AND STAYER PROFILES ARE CARDS-ONLY. Bank-wide client x MNE is ~99M pairs "
    "and was abandoned; non-cards campaigns carry counts and a rate but null sender/stayer stats.",
    "CLIENTS ACQUIRED DURING THE WINDOW cannot exist in the " + UCP_ANCHOR + " snapshot and come "
    "back unmatched. Counted in M0a, never dropped silently.",
    "BAND CUT POINTS and the high_potential thresholds (age<" + str(HP_AGE) + ", tenure<=" +
    str(HP_TENURE) + ", products<=" + str(HP_PRODS) + ") are our parameters, not standards.",
]:
    display(Markdown("- " + _c))

# %% [24] CLEANUP - OPT-IN. The raw pulls are scratch; client_spine and summary are the artifacts.
# Left False by default: this file is run top to bottom, and firing cleanup mid-iteration would
# force a re-pull on the next run.
RUN_CLEANUP = False

if not RUN_CLEANUP:
    print("CLEANUP skipped (RUN_CLEANUP = False) - raw pulls retained for restartability.")
else:
    for _art in ("client_spine", "summary"):
        assert landed(_art) and spark.read.parquet(BASE + _art).count() > 0, \
            _art + " is not safely landed - refusing to delete any raw pull."
    _raw = (["unsubs_3m/" + m[0] for m in WIN_MONTHS] +
            ["senders_base/" + m[0] for m in WIN_MONTHS] +
            ["senders_cards/" + m[0] for m in WIN_MONTHS] +
            ["senders_by_mne", "prior_unsubs"])
    for _p in _raw:
        get_ipython().system("hdfs dfs -rm -r -f /user/427966379/unsub_value_museum/" + _p)
    T("CLEANUP - what survived", pd.DataFrame(
        [{"dataset": a, "status": "KEPT" if landed(a) else "MISSING - INVESTIGATE"}
         for a in ("client_spine", "summary", "summary_csv", "ucp_spine")] +
        [{"dataset": p, "status": "REMOVED" if not landed(p) else "STILL PRESENT"} for p in _raw]))
    print("housekeeping complete - raw pulls removed, durable outputs verified intact.")

# OPEN QUESTIONS (2026-07-27, appended post-run, no code changed)
#
# ANCHOR RULE ANDRE SPECIFIED: per-client UCP anchoring - each client's UCP snapshot taken at the
#   month-end matching THEIR OWN unsub date, not a date shared across clients.
#
# WHAT WAS IMPLEMENTED INSTEAD: ONE common anchor, UCP_ANCHOR = "2026-02-28", used for every client
#   regardless of bucket (leaver/already_out/stayer). See cell [1]/[12] above.
#
# WHY: stayers have no unsub event to anchor to. Per-client anchoring would put leavers and stayers
#   at different points in their own timelines, biasing the L7/L8 leaver-vs-stayer comparisons.
#   2026-02-28 is also the last month-end strictly before the Mar-May send window, so nothing in it
#   can be a consequence of the treatment. The 12-month version of this file DID try per-client
#   anchoring - it fanned out to 30 UCP partitions and got the session killed by YARN (2026-07-27).
#
# STATUS: Andre's ruling 2026-07-27 - accepted FOR THIS RUN ONLY. Staleness is ~2 months and the
#   four measured fields (age, tenure, product count, annual profitability) move slowly at that
#   horizon. He was explicit that this deviated from his stated instruction and was under-flagged
#   when it happened (buried in a design comment, not called out prominently).
#
# REVISIT ON NEXT ITERATION: either implement true per-client unsub-date anchoring, or get explicit
#   sign-off from Andre to keep the common anchor permanently. Do not silently carry this forward.
