# %% [0] HEADER
# UNSUB VALUE MUSEUM - "the value of an unsubscribe", auditable and reproducible end to end.
#
# PURPOSE: pulls campaign send/unsub data STRAIGHT FROM EDW (no reservoir dependency - unlike
# cpc_reservoir_extract.py, this file owns its own Teradata pulls) and client attributes from
# HDFS UCP. Answers: who leaves vs who stays, and how campaigns compare. There is no EDW
# client-attribute table - UCP is the only source for age/tenure/product-count/profitability, so
# this file necessarily spans two engines: Teradata (EDW, vendor feedback) and Spark/YARN (HDFS,
# UCP + this file's own landings). SUPERSEDES archaeology/28_unsub_value_ucp.py, which read off
# the unsub_cpc reservoir and was scoped to Q2-2026 cards-only; this file is bank-wide and pulls
# its own EDW data directly.
#
# SCOPE (Andre, 2026-07-27):
#   - Window: sends Jan 1 2026 - Jun 1 2026 (Jan/Feb/Mar/Apr/May 2026), BANK-WIDE (no MNE filter).
#   - Unsub response: UNBOUNDED FORWARD (Jan 2026 through run date) - a March cohort's June
#     unsub still counts.
#   - FOUR client variables only: age, tenure, TIBC product counts, PROF_TOT_ANNUAL. No scope creep.
#   - Own HDFS namespace: hdfs:///user/427966379/unsub_value_museum/ - must not collide with
#     unsub_cpc/ (cpc_reservoir_extract.py's namespace).
#
# THE TWO TABLES:
#   TABLE A - WHO leaves vs who stays. Grain: one row per mailed client (bank-wide, Jan-May 2026).
#   TABLE B - WHERE, campaign comparison. Grain: distinct client x MNE (a client mailed by several
#             campaigns appears in several rows, so its columns do NOT sum to Table A's totals).
#   TABLE C - the same cube pre-aggregated for Excel pivoting (mne x age_band x tenure_band x
#             prod_band x prof_quintile, counts only).
#
# OUTPUT CONTRACT: printed tables carry counts and medians. The saved Excel cube (Table C) is
# COUNTS ONLY - rates are a pivot-time calculation, never baked into the file. Every printed
# number is backed by a server round-trip or an assertion; no bare "done" prints (house hard rule).
#
# Engine split: Teradata-direct (DTZV01.VENDOR_FEEDBACK_EVENT / _MASTER, EDW pulls in cells
# [4]-[11]) feeds this file's OWN HDFS reservoir; everything from cell [12] onward is Spark/YARN
# only, reading this file's landings plus UCP (/prod/sz/tsz/00172/data/ucp4/).
#
# Date: 2026-07-27.

# %% [1] SETUP - bootstrap, EDW connect, land()/landed() (FIXED versions, 2026-07-26 fix: changed
# SQL auto-repulls; landed() raises on unverifiable HDFS state instead of returning False), T(),
# norm_clnt (FIXED decimal form - float64 CLNT_NO renders scientific notation otherwise, cost a
# full debugging cycle in archaeology/28). Cloned from cpc_reservoir_extract.py + cpc_evidence_hdfs.py.

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
# namespace won't exist yet (handled gracefully). ANY other failure raises.
try:
    spark
except NameError:
    raise RuntimeError("`spark` is not defined - this script requires a Lumina/AI-Farm Spark+YARN "
                       "kernel with a pre-initialized SparkSession named `spark`. There is no "
                       "SparkSession.builder call here by design - do not run this file outside "
                       "that kernel.")

try:
    _hconf = spark._jsc.hadoopConfiguration()
    _fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(_hconf)
    _base_exists = _fs.exists(spark._jvm.org.apache.hadoop.fs.Path(BASE))
except Exception as e:
    raise RuntimeError("Cannot reach HDFS to check " + BASE + " - fix the spark/HDFS session "
                       "before running any pull cell. Underlying error: " + str(e)[:300])

if _base_exists:
    print("HDFS round-trip OK - own namespace", BASE, "already exists (not the first run).")
else:
    print("FIRST-EVER RUN - own namespace", BASE, "does not exist yet on HDFS. Expected before any "
          "land() call below has run; land() will create it. If you expected prior landings here, "
          "STOP and investigate before proceeding.")
print("GATE PASSED - spark session live, HDFS reachable.")

# %% [3] SIZE PROBE - EVENT-ONLY (no join, no COUNT DISTINCT of pairs). Gated on landed() so it
# costs nothing once P1 is fully landed - only unlanded months get sized.
_P1_MONTHS = ["m2026_01", "m2026_02", "m2026_03", "m2026_04", "m2026_05"]
_P1_BOUNDS = {
    "m2026_01": ("2026-01-01", "2026-02-01"),
    "m2026_02": ("2026-02-01", "2026-03-01"),
    "m2026_03": ("2026-03-01", "2026-04-01"),
    "m2026_04": ("2026-04-01", "2026-05-01"),
    "m2026_05": ("2026-05-01", "2026-06-01"),
}
_p1_unlanded = [m for m in _P1_MONTHS if not landed("sends_mne/" + m)]

if not _p1_unlanded:
    print("SIZE PROBE - all 5 sends_mne months already landed - SKIP (nothing left to size)")
else:
    _probe_rows = []
    for _m in _p1_unlanded:
        _ds, _de = _P1_BOUNDS[_m]
        _pdf = edw_pd("""
SELECT COUNT(*) AS event_rows
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_cd = 1
  AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
""" % (_ds, _de))
        _probe_rows.append({"month": _m, "event_rows": int(_pdf["event_rows"][0])})
    T("SIZE PROBE - disp_cd=1 send events, unlanded months only (event-only, no join, no DISTINCT)",
      pd.DataFrame(_probe_rows))

    _pdf_whole = edw_pd("""
SELECT COUNT(DISTINCT consumer_id_hashed) AS distinct_senders
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_cd = 1
  AND disposition_dt_tm >= DATE '2026-01-01' AND disposition_dt_tm < DATE '2026-06-01'
""")
    print("distinct consumer_id_hashed, disp_cd=1, whole Jan-May 2026 window:",
          int(_pdf_whole["distinct_senders"][0]))
    print("\nSTOP RULE: if any month's event_rows is unexpectedly huge (order of magnitude above the")
    print("others), split that month's land() cell further (e.g. by week) before pulling - the")
    print("dedupe-before-join derived-table shape in P1 keeps the JOIN spool-safe regardless, but a")
    print("single EVENT scan that large is still worth knowing about before committing to the pull.")

# %% [4] EXTRACT sends_mne 2026-01 (load_tm 2025-12..2026-03). Collapsing to MNE at the server is
# deliberate: a client mailed 12 times by one campaign becomes ONE row - this is Table B's grain
# and it shrinks the pull by an order of magnitude vs client x treatment.
land("sends_mne/m2026_01", """
SELECT DISTINCT m.CLNT_NO, SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '2026-01-01' AND disposition_dt_tm < DATE '2026-02-01'
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '2025-12-01' AND m.load_tm < DATE '2026-03-01'
""")

# %% [5] EXTRACT sends_mne 2026-02 (load_tm 2026-01..2026-04)
land("sends_mne/m2026_02", """
SELECT DISTINCT m.CLNT_NO, SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '2026-02-01' AND disposition_dt_tm < DATE '2026-03-01'
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '2026-01-01' AND m.load_tm < DATE '2026-04-01'
""")

# %% [6] EXTRACT sends_mne 2026-03 (load_tm 2026-02..2026-05)
land("sends_mne/m2026_03", """
SELECT DISTINCT m.CLNT_NO, SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '2026-03-01' AND disposition_dt_tm < DATE '2026-04-01'
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '2026-02-01' AND m.load_tm < DATE '2026-05-01'
""")

# %% [7] EXTRACT sends_mne 2026-04 (load_tm 2026-03..2026-06)
land("sends_mne/m2026_04", """
SELECT DISTINCT m.CLNT_NO, SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '2026-04-01' AND disposition_dt_tm < DATE '2026-05-01'
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '2026-03-01' AND m.load_tm < DATE '2026-06-01'
""")

# %% [8] EXTRACT sends_mne 2026-05 (load_tm 2026-04..2026-07)
land("sends_mne/m2026_05", """
SELECT DISTINCT m.CLNT_NO, SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '2026-05-01' AND disposition_dt_tm < DATE '2026-06-01'
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '2026-04-01' AND m.load_tm < DATE '2026-07-01'
""")

# %% [9] EXTRACT first_send - MIN(disposition_dt_tm) per client, whole Jan-May window, server-side
# aggregate (one row per client, small). Dedupe-before-join: EVENT pre-aggregated to
# (consumer_id_hashed, TREATMENT_ID) -> MIN(disposition_dt_tm) BEFORE the join, so the join runs on
# a much smaller key set than the raw EVENT scan.
land("first_send", """
SELECT m.CLNT_NO, MIN(ek.first_dt) AS first_send_tm
FROM (
    SELECT consumer_id_hashed, TREATMENT_ID, MIN(disposition_dt_tm) AS first_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '2026-01-01' AND disposition_dt_tm < DATE '2026-06-01'
    GROUP BY consumer_id_hashed, TREATMENT_ID
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '2025-12-01' AND m.load_tm < DATE '2026-07-01'
GROUP BY m.CLNT_NO
""")

# %% [10] EXTRACT unsubs_window - disposition_cd=4, disposition_dt_tm >= 2026-01-01, NO UPPER BOUND
# (this IS the response window - a March cohort's June unsub still counts). Chunked by quarter
# (JUDGMENT CALL: not spelled out verbatim in the brief, which says "chunk by quarter if the probe
# says it's large" - the window is open-ended toward the run date, so an unbounded single pull only
# grows on every future rerun; quarterly chunks keep each EVENT scan and MASTER load_tm bound small
# and independently restartable). The last chunk has NO upper bound on either side, matching the
# open EVENT window.
_unsubs_window_chunks = [
    ("q1", "2026-01-01", "2026-04-01", "2025-12-01", "2026-05-01"),
    ("q2", "2026-04-01", "2026-07-01", "2026-03-01", "2026-08-01"),
    ("q3", "2026-07-01", None, "2026-06-01", None),
]

def _unsubs_window_sql(ev_start, ev_end, ld_start, ld_end):
    ev_upper = "AND disposition_dt_tm < DATE '%s'" % ev_end if ev_end else ""
    ld_upper = "AND m.load_tm < DATE '%s'" % ld_end if ld_end else ""
    return """
SELECT m.CLNT_NO, SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne, MIN(ek.first_dt) AS unsub_tm
FROM (
    SELECT consumer_id_hashed, TREATMENT_ID, MIN(disposition_dt_tm) AS first_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '%s' %s
    GROUP BY consumer_id_hashed, TREATMENT_ID
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '%s' %s
GROUP BY m.CLNT_NO, SUBSTR(ek.TREATMENT_ID, 8, 3)
""" % (ev_start, ev_upper, ld_start, ld_upper)

for _q, _es, _ee, _ls, _le in _unsubs_window_chunks:
    land("unsubs_window/" + _q, _unsubs_window_sql(_es, _ee, _ls, _le))

# %% [11] EXTRACT prior_unsubs - disposition_cd=4, 2024-01-01 <= disposition_dt_tm < 2026-01-01
# (2024 floor is a hard house rule). Single pull with a commented quarterly fallback if TDWM kills
# it (same fallback shape as cpc_reservoir_extract.py's cpc_landing_allsw).
land("prior_unsubs", """
SELECT DISTINCT m.CLNT_NO
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '2024-01-01' AND disposition_dt_tm < DATE '2026-01-01'
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '2023-12-01' AND m.load_tm < DATE '2026-02-01'
""")
# FALLBACK if TDWM kills the single pull: land per disposition_dt_tm quarter, then cell [12] reads
# prior_unsubs/* instead of prior_unsubs:
# for _a, _b in [("2024-01-01","2024-04-01"), ("2024-04-01","2024-07-01"), ("2024-07-01","2024-10-01"),
#                ("2024-10-01","2025-01-01"), ("2025-01-01","2025-04-01"), ("2025-04-01","2025-07-01"),
#                ("2025-07-01","2025-10-01"), ("2025-10-01","2026-01-01")]:
#     land("prior_unsubs/" + _a[:7], """... same shape, EVENT bounds _a/_b, load_tm _a-1mo/_b+1mo ...""")

# %% [12] READ BACK all landings into Spark, normalize CLNT_NO on every one, print a labeled table.
sends_mne_raw = (spark.read.parquet(BASE + "sends_mne/*")
                  .withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO"))))
first_send_raw = (spark.read.parquet(BASE + "first_send")
                   .withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO"))))
unsubs_window_raw = (spark.read.parquet(BASE + "unsubs_window/*")
                      .withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO"))))
prior_unsubs_raw = (spark.read.parquet(BASE + "prior_unsubs")
                     .withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO"))))

sends_mne_raw.cache(); first_send_raw.cache(); unsubs_window_raw.cache(); prior_unsubs_raw.cache()

_readback_rows = []
for _name, _df in [("sends_mne", sends_mne_raw), ("first_send", first_send_raw),
                    ("unsubs_window", unsubs_window_raw), ("prior_unsubs", prior_unsubs_raw)]:
    _n = _df.count()
    _dc = _df.select("CLNT_NO").distinct().count()
    assert _n > 0, _name + " read back ZERO rows - a land() cell above failed to actually land data"
    _readback_rows.append({"dataset": _name, "rows": _n, "distinct_clients": _dc})

T("READBACK - all 4 landed datasets, rows and distinct clients after CLNT_NO normalization",
  pd.DataFrame(_readback_rows))

# %% [13] BUCKETS - one row per mailed client: leaver / excluded / stayer.
mailed = sends_mne_raw.select("CLNT_NO").distinct()
_n_mailed = mailed.count()

# every (client, mne) pair this client was actually mailed - the matching key for "unsub from an
# mne they were mailed by in-window"
mailed_pairs = sends_mne_raw.select("CLNT_NO", "mne").distinct()

# collapse unsubs_window's quarterly chunks to one row per (CLNT_NO, mne): earliest unsub_tm across
# chunks (a client can have qualifying unsub events for the same mne split across quarter boundaries)
unsub_by_mne = unsubs_window_raw.groupBy("CLNT_NO", "mne").agg(F.min("unsub_tm").alias("unsub_tm"))

# leaver = mailed AND has an unsub event on an mne they were mailed by; take the EARLIEST
# qualifying (mne, unsub_tm) pair per client as THE unsub_mne/unsub_tm for the bucket
_qualifying = mailed_pairs.join(unsub_by_mne, ["CLNT_NO", "mne"], "inner")
_w_earliest = Window.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").asc(), F.col("mne").asc())
leaver_rows = (_qualifying.withColumn("rn", F.row_number().over(_w_earliest))
               .filter("rn = 1").drop("rn")
               .withColumnRenamed("mne", "unsub_mne")
               .select("CLNT_NO", "unsub_mne", "unsub_tm"))

_leaver_flag = leaver_rows.select("CLNT_NO").distinct().withColumn("_is_leaver", F.lit(True))
_excluded_flag = (mailed.join(_leaver_flag, "CLNT_NO", "left")
                   .filter(F.col("_is_leaver").isNull())
                   .join(prior_unsubs_raw.select("CLNT_NO").distinct(), "CLNT_NO", "inner")
                   .select("CLNT_NO").distinct().withColumn("_is_excluded", F.lit(True)))

client_bucket = (
    mailed
    .join(_leaver_flag, "CLNT_NO", "left")
    .join(_excluded_flag, "CLNT_NO", "left")
    .join(leaver_rows, "CLNT_NO", "left")
    .withColumn("bucket",
                F.when(F.col("_is_leaver").isNotNull(), "leaver")
                 .when(F.col("_is_excluded").isNotNull(), "excluded")
                 .otherwise("stayer"))
    .select("CLNT_NO", "bucket", "unsub_mne", "unsub_tm"))
client_bucket.cache()

_n_bucketed = client_bucket.count()
assert _n_bucketed == _n_mailed, (
    "bucket assignment row count (" + str(_n_bucketed) + ") != mailed (" + str(_n_mailed) +
    ") - a client got duplicated or dropped in the bucket build")

_bucket_counts = (client_bucket.groupBy("bucket").agg(F.count("*").alias("clients"))
                   .withColumn("pct_of_mailed", F.round(100.0 * F.col("clients") / _n_mailed, 2)))
_bucket_pd = T("BUCKETS - leaver / excluded / stayer, one row per mailed client, % of mailed",
               _bucket_counts.orderBy(F.desc("clients")))
assert int(_bucket_pd["clients"].sum()) == _n_mailed, (
    "bucket counts sum (" + str(int(_bucket_pd["clients"].sum())) + ") != mailed (" +
    str(_n_mailed) + ") - the three buckets must exactly partition mailed")
print("BUCKETS PASSED - leaver + excluded + stayer sums to mailed.")

# %% [14] ANCHORS - last day of the month BEFORE first_send_tm's month. BOTH leavers and stayers
# are anchored at FIRST EXPOSURE so every client is measured pre-treatment at the same point in
# their own timeline. This replaces archaeology/28's asymmetric anchoring (last-unsub for leavers,
# last-deployment for stayers), which let a leaver's UCP profile drift post-exposure while a
# stayer's anchor stayed near their own first mail - biased in the leaver's favour or against it
# depending on direction, and not comparable across the two groups either way.
client_anchor = (client_bucket.join(first_send_raw.select("CLNT_NO", "first_send_tm"), "CLNT_NO", "left")
                  .withColumn("anchor", F.last_day(F.add_months(F.col("first_send_tm"), -1))))

_n_no_first_send = client_anchor.filter(F.col("first_send_tm").isNull()).count()
assert _n_no_first_send == 0, (
    str(_n_no_first_send) + " mailed clients have no first_send_tm - first_send (P2) must cover "
    "every client in sends_mne (P1); check the pull windows match")

_anchor_counts = (client_anchor.withColumn("anchor_str", F.date_format("anchor", "yyyy-MM-dd"))
                   .groupBy("anchor_str").agg(F.count("*").alias("clients")).orderBy("anchor_str"))
T("ANCHORS - clients per anchor month-end (expect exactly 5: 2025-12-31 .. 2026-04-30)", _anchor_counts)

_expected_anchors = {"2025-12-31", "2026-01-31", "2026-02-28", "2026-03-31", "2026-04-30"}
_actual_anchors = set(r["anchor_str"] for r in
                       client_anchor.withColumn("anchor_str", F.date_format("anchor", "yyyy-MM-dd"))
                       .select("anchor_str").distinct().collect())
assert _actual_anchors == _expected_anchors, (
    "anchors present " + str(sorted(_actual_anchors)) + " != expected " + str(sorted(_expected_anchors)))
print("ANCHOR SET CONFIRMED:", sorted(_actual_anchors))

# assert every anchor partition exists in UCP before reading anything
_ucp_parts = (spark.read.option("basePath", UCP_BASE).parquet(UCP_BASE)
              .select("MONTH_END_DATE").distinct().toPandas())
_ucp_avail = set(_ucp_parts["MONTH_END_DATE"].astype(str).tolist())
_missing_ucp = sorted(_expected_anchors - _ucp_avail)
assert not _missing_ucp, (
    "UCP partition(s) missing for anchor(s) " + str(_missing_ucp) + " under " + UCP_BASE +
    " - available partitions: " + str(sorted(_ucp_avail)))
print("UCP PARTITION CHECK PASSED - all 5 anchor month-ends present under", UCP_BASE)

client_anchor.cache()

# %% [15] UCP READ - per anchor month-end, semi-join to only the clients needing that anchor.
# CLNT_TYP == 'Personal' is the verified filter value (confirmed present 2026-07-27); all 8
# selected fields confirmed present the same day.
UCP_COLS = ["CLNT_NO", "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT",
            "C_TOT_CNT", "PROF_TOT_ANNUAL"]

def read_ucp_anchor(anchor_str, needed_clients):
    raw = (spark.read.option("basePath", UCP_BASE).parquet(UCP_BASE + "MONTH_END_DATE=" + anchor_str)
           .withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO")))
           .filter(F.trim(F.col("CLNT_TYP")) == "Personal"))
    raw = raw.join(needed_clients, "CLNT_NO", "leftsemi")
    return raw.select(*UCP_COLS).withColumn("ucp_month_end", F.lit(anchor_str))

_ucp_frames, _ucp_read_summary = [], []
for _a in sorted(_expected_anchors):
    _needed = (client_anchor.withColumn("anchor_str", F.date_format("anchor", "yyyy-MM-dd"))
               .filter(F.col("anchor_str") == _a).select("CLNT_NO").distinct())
    _n_needed = _needed.count()
    _sel = read_ucp_anchor(_a, _needed)
    _n_matched = _sel.count()
    _ucp_read_summary.append({"anchor": _a, "needed_distinct_clients": _n_needed, "matched_rows": _n_matched})
    _ucp_frames.append(_sel)

ucp_spine = _ucp_frames[0]
for _f in _ucp_frames[1:]:
    ucp_spine = ucp_spine.unionByName(_f)
T("UCP1 - per-anchor-month read summary (needed vs matched)", pd.DataFrame(_ucp_read_summary))
ucp_spine.cache()

# fan-out guard: one row per (CLNT_NO, ucp_month_end) - a client must not appear twice at the
# same anchor. Dedup fallback commented below if it ever trips.
_n_spine = ucp_spine.count()
_n_spine_distinct = ucp_spine.select("CLNT_NO", "ucp_month_end").distinct().count()
assert _n_spine == _n_spine_distinct, (
    "UCP spine has " + str(_n_spine) + " rows but only " + str(_n_spine_distinct) +
    " distinct (CLNT_NO, ucp_month_end) pairs - UCP is not unique per (CLNT_NO, MONTH_END_DATE).")
# DEDUP FALLBACK (uncomment if the assert above trips):
# _w = Window.partitionBy("CLNT_NO", "ucp_month_end").orderBy(F.lit(1))
# ucp_spine = ucp_spine.withColumn("_rn", F.row_number().over(_w)).filter("_rn = 1").drop("_rn")
print("UCP spine fan-out guard PASSED -", _n_spine, "rows, one per (CLNT_NO, ucp_month_end)")

client_ucp = (client_anchor
              .withColumn("anchor_str", F.date_format("anchor", "yyyy-MM-dd"))
              .join(ucp_spine.withColumnRenamed("CLNT_NO", "_u_clnt"),
                    (F.col("CLNT_NO") == F.col("_u_clnt")) & (F.col("anchor_str") == F.col("ucp_month_end")),
                    "left")
              .drop("_u_clnt"))

_n_before_ucp_join = client_anchor.count()
_n_after_ucp_join = client_ucp.count()
assert _n_before_ucp_join == _n_after_ucp_join, (
    "FAN-OUT on client x UCP join: " + str(_n_before_ucp_join) + " -> " + str(_n_after_ucp_join) +
    " rows. See the UCP spine fan-out guard above.")
client_ucp = client_ucp.withColumn("ucp_matched", F.col("AGE").isNotNull())
client_ucp.cache()
print("client_ucp assembled:", _n_after_ucp_join, "rows (one per mailed client)")

# %% [16] M0 - COVERAGE & NULLS. This prints BEFORE any finding.
_cov_by_bucket = (client_ucp.groupBy("bucket")
                   .agg(F.count("*").alias("clients"),
                        F.sum(F.col("ucp_matched").cast("int")).alias("matched"))
                   .withColumn("unmatched", F.col("clients") - F.col("matched"))
                   .withColumn("match_pct", F.round(100.0 * F.col("matched") / F.col("clients"), 1)))
T("M0a - UCP match rate by bucket", _cov_by_bucket.orderBy(F.desc("clients")))

_unmatched = client_ucp.filter(~F.col("ucp_matched")).select("CLNT_NO", "anchor_str")
_cov_by_anchor = (_unmatched.groupBy("anchor_str").agg(F.count("*").alias("unmatched_clients"))
                   .orderBy("anchor_str"))
T("M0b - unmatched clients by anchor month - proves/kills the hypothesis that unmatched clients "
  "are newly-acquired (mailed inside their own first-send month, before that month's UCP snapshot "
  "could reflect them)", _cov_by_anchor)

_unmatched_mne = (_unmatched.select("CLNT_NO").distinct()
                   .join(mailed_pairs, "CLNT_NO", "inner")
                   .groupBy("mne").agg(F.count("*").alias("unmatched_clients"))
                   .orderBy(F.desc("unmatched_clients")).limit(15))
T("M0c - unmatched clients by top-15 MNE they were mailed by (a client mailed by several MNEs "
  "counts once per MNE, same convention as Table B) - proves/kills the hypothesis that unmatched "
  "clients concentrate in specific campaigns (e.g. business/non-Personal-heavy programs)",
  _unmatched_mne)

_n_matched_total = client_ucp.filter(F.col("ucp_matched")).count()
_null_rows = []
for _c in UCP_COLS:
    _n_null = client_ucp.filter(F.col("ucp_matched") & F.col(_c).isNull()).count()
    _null_rows.append({"field": _c, "null_among_matched": _n_null,
                        "pct_null_among_matched": round(100.0 * _n_null / _n_matched_total, 2)})
_null_df = pd.DataFrame(_null_rows)
T("M0d - null counts among MATCHED rows, per UCP field (CLNT_NO included for completeness - "
  "structurally 0% by construction of the join key)", _null_df)

_worst_field = _null_df.loc[_null_df["pct_null_among_matched"].idxmax()]
if _worst_field["pct_null_among_matched"] > 5.0:
    print("VERDICT: '" + _worst_field["field"] + "' is", _worst_field["pct_null_among_matched"],
          "% null among matched rows (> 5%) - every median downstream using this field is suspect. "
          "Check M0d before trusting Table A/B/C.")
else:
    print("VERDICT: all UCP fields under 5% null among matched rows (worst =",
          _worst_field["field"], "at", _worst_field["pct_null_among_matched"], "%).")

# %% [17] BANDING - one function, applied identically to every bucket. EDITABLE PARAMETERS, one
# place: band cut points and the high_potential thresholds are OUR CHOICE, not documented anywhere
# else in the repo.
TENURE_EDGES = [2, 5, 10, 20]     # years; bins: 0-2, 3-5, 6-10, 11-20, 20+
AGE_EDGES = [24, 34, 44, 54, 64]  # bins: <25, 25-34, 35-44, 45-54, 55-64, 65+
HP_AGE_MAX = 35       # high_potential: AGE < this
HP_TENURE_MAX = 5     # high_potential: TENURE_RBC_YEARS <= this
HP_PROD_MAX = 2        # high_potential: prod_cnt <= this
# high_potential is the thesis as a countable definition: decades of banking life ahead, not yet
# embedded, most of the wallet unsold, and email is now closed.

_prof_cut_base = client_ucp.filter(F.col("ucp_matched") & F.col("PROF_TOT_ANNUAL").isNotNull())
PROF_CUTS = _prof_cut_base.approxQuantile("PROF_TOT_ANNUAL", [0.2, 0.4, 0.6, 0.8], 0.01)
T("PROF CUTS - PROF_TOT_ANNUAL quintile cut points, computed over ALL MAILED matched clients (the "
  "reachable base), n = " + str(_prof_cut_base.count()),
  pd.DataFrame([{"p20": PROF_CUTS[0], "p40": PROF_CUTS[1], "p60": PROF_CUTS[2], "p80": PROF_CUTS[3]}]))

def apply_bands(df):
    df = df.withColumn(
        "prod_cnt",
        F.coalesce(F.col("T_TOT_CNT"), F.lit(0)) + F.coalesce(F.col("I_TOT_CNT"), F.lit(0)) +
        F.coalesce(F.col("B_TOT_CNT"), F.lit(0)) + F.coalesce(F.col("C_TOT_CNT"), F.lit(0)))
    # NOTE: the brief's prod_band list is '1','2','3-4','5+' - '0' is added here too (a real,
    # reachable value: mailed clients holding no T/I/B/C product on file) so no client is silently
    # misclassified into '1'.
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

client_banded = apply_bands(client_ucp)
client_banded.cache()
print("client_banded:", client_banded.count(), "rows, bands applied")

# %% [18] TABLE A - WHO. GRAIN: one row per mailed client, bank-wide, Jan-May 2026 sends.
_n_mailed_total = client_banded.count()
_n_leavers_total = client_banded.filter(F.col("bucket") == "leaver").count()
_n_stayers_total = client_banded.filter(F.col("bucket") == "stayer").count()
_n_excluded_total = client_banded.filter(F.col("bucket") == "excluded").count()

a1 = (client_banded.groupBy("bucket").agg(F.count("*").alias("clients"))
      .withColumn("pct_of_mailed", F.round(100.0 * F.col("clients") / _n_mailed_total, 2))
      .withColumn("leavers_per_1000_mailed",
                  F.when(F.col("bucket") == "leaver",
                         F.round(1000.0 * F.col("clients") / _n_mailed_total, 2))))
T("TABLE A1 - WHO, bucket totals | GRAIN: one row per mailed client, Jan-May 2026 sends, bank-wide",
  a1.orderBy(F.desc("clients")))

_a2_dims = ["prod_band", "tenure_band", "age_band", "high_potential"]
_a2_frames = []
_lv = client_banded.filter(F.col("bucket") == "leaver")
_st = client_banded.filter(F.col("bucket") == "stayer")
for _dim in _a2_dims:
    lb = (_lv.groupBy(_dim).count()
          .withColumn("pct_leavers", F.round(100.0 * F.col("count") / _n_leavers_total, 2))
          .withColumnRenamed("count", "leavers_n"))
    sb = (_st.groupBy(_dim).count()
          .withColumn("pct_stayers", F.round(100.0 * F.col("count") / _n_stayers_total, 2))
          .withColumnRenamed("count", "stayers_n"))
    m = (lb.join(sb, _dim, "outer")
         .withColumn("pct_leavers", F.coalesce(F.col("pct_leavers"), F.lit(0.0)))
         .withColumn("pct_stayers", F.coalesce(F.col("pct_stayers"), F.lit(0.0)))
         .withColumn("leavers_n", F.coalesce(F.col("leavers_n"), F.lit(0)))
         .withColumn("stayers_n", F.coalesce(F.col("stayers_n"), F.lit(0)))
         .withColumn("ratio", F.round(F.col("pct_leavers") /
                                       F.when(F.col("pct_stayers") == 0, None).otherwise(F.col("pct_stayers")), 2))
         .withColumn("segment_dim", F.lit(_dim))
         .withColumnRenamed(_dim, "segment_value")
         .withColumn("segment_value", F.col("segment_value").cast("string"))
         .select("segment_dim", "segment_value", "pct_leavers", "pct_stayers", "ratio", "leavers_n", "stayers_n"))
    _a2_frames.append(m)

a2 = _a2_frames[0]
for _f in _a2_frames[1:]:
    a2 = a2.unionByName(_f)
_w_ratio = Window.partitionBy("segment_dim").orderBy(F.desc("ratio"))
T("TABLE A2 - WHO, leavers vs stayers across band dims + high_potential | GRAIN: one row per "
  "mailed client | ratio = pct_leavers / pct_stayers, ratio > 1 = over-represented among leavers "
  "relative to stayers | sorted within dim by ratio desc",
  a2.withColumn("_rn", F.row_number().over(_w_ratio)).orderBy("segment_dim", "_rn").drop("_rn"))

# %% [19] TABLE B - WHERE (campaign comparison). GRAIN: distinct client x MNE - a client mailed by
# several campaigns appears in several rows here, so this table's columns do NOT sum to Table A's
# totals (Table A is client grain, one bucket per client, picking their EARLIEST qualifying unsub
# across campaigns; Table B asks "for THIS campaign specifically, who among ITS recipients unsubbed
# from IT").
_pair_unsub = (mailed_pairs.join(unsub_by_mne, ["CLNT_NO", "mne"], "left")
               .withColumn("is_unsub_this_mne", F.col("unsub_tm").isNotNull()))

_client_attrs = client_banded.select(
    "CLNT_NO", "AGE", "TENURE_RBC_YEARS", "prod_cnt", "PROF_TOT_ANNUAL",
    "prod_band", "tenure_band", "age_band", "prof_quintile", "high_potential", "bucket")

_pair_profile = _pair_unsub.join(_client_attrs, "CLNT_NO", "left")
_pair_profile.cache()

b_base = (_pair_profile.groupBy("mne")
          .agg(F.countDistinct("CLNT_NO").alias("clients_mailed"),
               F.countDistinct(F.when(F.col("is_unsub_this_mne"), F.col("CLNT_NO"))).alias("unsub_clients")))
b_base = b_base.withColumn("unsub_per_1000", F.round(1000.0 * F.col("unsub_clients") / F.col("clients_mailed"), 2))

_leaver_pairs = _pair_profile.filter(F.col("is_unsub_this_mne"))
b_profile = (_leaver_pairs.groupBy("mne")
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

# FAIRNESS: indirect standardisation on (age_band x tenure_band x prod_band) targeting mix.
# Campaigns target different populations (an acquisition campaign's clients are young/new/
# single-product BY CONSTRUCTION), so raw unsub_per_1000 conflates targeting with performance.
# expected_per_1000 = the rate this campaign would show if each of ITS mailed clients unsubscribed
# at the OVERALL rate for their own cell; actual_minus_expected is the campaign's own contribution,
# net of who it reaches. Cell rates computed over ALL MAILED clients (client grain, Table A's
# population), joined back onto every (client, mne) pair, then averaged per mne.
_cell_rate = (client_banded.groupBy("age_band", "tenure_band", "prod_band")
              .agg(F.count("*").alias("cell_mailed"),
                   F.sum(F.when(F.col("bucket") == "leaver", 1).otherwise(0)).alias("cell_leavers"))
              .withColumn("cell_rate", F.col("cell_leavers") / F.col("cell_mailed")))
T("FAIRNESS CELLS - overall leaver rate per (age_band x tenure_band x prod_band) cell, over ALL "
  "mailed clients (client grain) - the standardisation reference for Table B's expected_per_1000",
  _cell_rate.orderBy(F.desc("cell_mailed")))

_pair_cells = _pair_profile.join(
    _cell_rate.select("age_band", "tenure_band", "prod_band", "cell_rate"),
    ["age_band", "tenure_band", "prod_band"], "left")
b_expected = (_pair_cells.groupBy("mne")
              .agg(F.round(1000.0 * F.avg("cell_rate"), 2).alias("expected_per_1000")))

table_b = (b_base.join(b_profile, "mne", "left").join(b_expected, "mne", "left")
           .withColumn("actual_minus_expected", F.round(F.col("unsub_per_1000") - F.col("expected_per_1000"), 2)))
_B_COLS = ["mne", "clients_mailed", "unsub_clients", "unsub_per_1000", "median_age",
           "median_tenure_years", "median_prod_cnt", "median_prof_annual", "pct_single_product",
           "pct_top_prof_quintile", "pct_high_potential", "expected_per_1000", "actual_minus_expected"]
table_b_pd = T("TABLE B - WHERE (campaign comparison) | GRAIN: distinct client x MNE | "
    "expected_per_1000 = indirect standardisation (see FAIRNESS CELLS above) | "
    "actual_minus_expected = the campaign's own contribution, net of who it targets",
    table_b.select(*_B_COLS).orderBy(F.desc("clients_mailed")))

# TOTAL row - same definitions, computed over the whole pool rather than per mne.
_tot_base = _pair_profile.agg(
    F.count("*").alias("clients_mailed"),
    F.sum(F.col("is_unsub_this_mne").cast("int")).alias("unsub_clients")).collect()[0]
_tot_clients_mailed = _tot_base["clients_mailed"]
_tot_unsub_clients = _tot_base["unsub_clients"]
_tot_unsub_per_1000 = round(1000.0 * _tot_unsub_clients / _tot_clients_mailed, 2)

_tot_profile = _leaver_pairs.agg(
    F.expr("percentile_approx(AGE, 0.5)").alias("median_age"),
    F.expr("percentile_approx(TENURE_RBC_YEARS, 0.5)").alias("median_tenure_years"),
    F.expr("percentile_approx(prod_cnt, 0.5)").alias("median_prod_cnt"),
    F.expr("percentile_approx(PROF_TOT_ANNUAL, 0.5)").alias("median_prof_annual"),
    F.round(100.0 * F.sum(F.when(F.col("prod_band") == "1", 1).otherwise(0)) / F.count("*"), 1).alias("pct_single_product"),
    F.round(100.0 * F.sum(F.when(F.col("prof_quintile") == "5", 1).otherwise(0)) / F.count("*"), 1).alias("pct_top_prof_quintile"),
    F.round(100.0 * F.sum(F.col("high_potential").cast("int")) / F.count("*"), 1).alias("pct_high_potential"),
).collect()[0]

_tot_expected = _pair_cells.agg(F.round(1000.0 * F.avg("cell_rate"), 2).alias("e")).collect()[0]["e"]
_tot_actual_minus_expected = round(_tot_unsub_per_1000 - _tot_expected, 2)

_total_row = pd.DataFrame([{
    "mne": "TOTAL", "clients_mailed": _tot_clients_mailed, "unsub_clients": _tot_unsub_clients,
    "unsub_per_1000": _tot_unsub_per_1000, "median_age": _tot_profile["median_age"],
    "median_tenure_years": _tot_profile["median_tenure_years"],
    "median_prod_cnt": _tot_profile["median_prod_cnt"],
    "median_prof_annual": _tot_profile["median_prof_annual"],
    "pct_single_product": _tot_profile["pct_single_product"],
    "pct_top_prof_quintile": _tot_profile["pct_top_prof_quintile"],
    "pct_high_potential": _tot_profile["pct_high_potential"],
    "expected_per_1000": _tot_expected, "actual_minus_expected": _tot_actual_minus_expected,
}])[_B_COLS]
table_b_full = pd.concat([table_b_pd, _total_row], ignore_index=True)
display(Markdown("**TABLE B + TOTAL row**"))
display(table_b_full)

# %% [20] TABLE C - THE CUBE, for Excel. GRAIN: mne x age_band x tenure_band x prod_band x
# prof_quintile. COUNTS ONLY - rates are a pivot-time calculation in Excel, never baked in here.
table_c = (_pair_profile.groupBy("mne", "age_band", "tenure_band", "prod_band", "prof_quintile")
           .agg(F.count("*").alias("clients_mailed"),
                F.sum(F.col("is_unsub_this_mne").cast("int")).alias("unsub_clients")))
_n_cube_rows = table_c.count()
print("TABLE C cube row count:", _n_cube_rows)
assert _n_cube_rows < 200_000, (
    "cube has " + str(_n_cube_rows) + " rows (>= 200k) - Excel write would be unwieldy, investigate "
    "before writing (likely a band producing far more distinct values than intended)")

table_c_pd = (table_c.orderBy("mne", "age_band", "tenure_band", "prod_band", "prof_quintile")
              .toPandas())
print("TABLE C - writing", len(table_c_pd), "rows to unsub_value_cube.xlsx")
table_c_pd.to_excel("unsub_value_cube.xlsx", index=False)
print("TABLE C written: unsub_value_cube.xlsx (", len(table_c_pd), "rows,",
      list(table_c_pd.columns), ")")

# client-grain spine, saved so Andre can re-cut without re-pulling EDW or re-reading UCP.
client_spine = client_banded.select(
    "CLNT_NO", "bucket", "unsub_mne", "unsub_tm", "anchor", "ucp_month_end", "ucp_matched",
    "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT", "PROF_TOT_ANNUAL",
    "prod_cnt", "prod_band", "tibc_mix", "tenure_band", "age_band", "prof_quintile", "high_potential")

_n_spine_before = client_spine.count()
client_spine.write.mode("overwrite").parquet(BASE + "client_spine")
_n_spine_after = spark.read.parquet(BASE + "client_spine").count()
assert _n_spine_after == _n_spine_before, (
    "client_spine HDFS readback mismatch: wrote " + str(_n_spine_before) + " read back " + str(_n_spine_after))
print("client_spine saved to", BASE + "client_spine", "-", _n_spine_after,
      "rows, readback confirmed. Re-cut without re-pulling: spark.read.parquet('" +
      BASE + "client_spine')")

# %% [21] ONE-SCREEN SUMMARY
display(Markdown("## UNSUB VALUE MUSEUM SUMMARY"))

_summary_rows = [
    ("Mailed clients, Jan-May 2026, bank-wide", _n_mailed_total),
    ("Leavers", _n_leavers_total),
    ("Excluded (prior unsub 2024-2025, not counted as leaver or stayer)", _n_excluded_total),
    ("Stayers", _n_stayers_total),
    ("Leaver rate per 1,000 mailed",
     round(1000.0 * _n_leavers_total / _n_mailed_total, 2)),
]
T("SUMMARY - headline sizes", pd.DataFrame(_summary_rows, columns=["figure", "value"]))

_overall_match_pct = round(100.0 * _n_matched_total / _n_mailed_total, 1)
print("Overall UCP match rate:", _overall_match_pct, "% (see M0a for the per-bucket breakdown)")

_a2_pd = a2.toPandas() if hasattr(a2, "toPandas") else a2
_top_over = _a2_pd.sort_values("ratio", ascending=False).iloc[0]
_top_under = _a2_pd.sort_values("ratio", ascending=True).iloc[0]
print("TABLE A headline - strongest OVER-represented segment among leavers:",
      _top_over["segment_dim"], "=", _top_over["segment_value"], "ratio", _top_over["ratio"])
print("TABLE A headline - strongest UNDER-represented segment among leavers:",
      _top_under["segment_dim"], "=", _top_under["segment_value"], "ratio", _top_under["ratio"])

_b_no_total = table_b_full[table_b_full["mne"] != "TOTAL"].copy()
_top3_volume = _b_no_total.sort_values("clients_mailed", ascending=False).head(3)
_top3_ame = _b_no_total.sort_values("actual_minus_expected", ascending=False).head(3)
T("SUMMARY - top 3 campaigns by clients_mailed", _top3_volume[["mne", "clients_mailed", "unsub_per_1000"]])
T("SUMMARY - top 3 campaigns by actual_minus_expected (over-performing their targeting mix)",
  _top3_ame[["mne", "unsub_per_1000", "expected_per_1000", "actual_minus_expected"]])

print("\nCAVEATS:")
print("- LAST-TOUCH ATTRIBUTION: the MNE on an unsub event is the email that carried the click/")
print("  unsubscribe action - a client on several lists who unsubscribes once is attributed to")
print("  whichever campaign's link they used, which is not necessarily the campaign that drove the")
print("  decision. Heavy senders structurally inherit more of the blame under this rule.")
print("- OVERLAPPING DENOMINATORS IN TABLE B: clients_mailed sums to MORE than Table A's mailed")
print("  total, because a client mailed by 3 campaigns contributes a row to each of the 3. Table B")
print("  rows are not independent samples of the same population.")
print("- PROF_TOT_ANNUAL is CURRENT-YEAR CONTRIBUTION, not LTV (proven 2026-07-27 in")
print("  archaeology/28_unsub_value_ucp.py V6: median rises 19 -> 71 -> 144 -> 366 -> 805 across")
print("  tenure bands 0-2/3-5/6-10/11-20/20+). Label it 'annual profitability' on any slide, never")
print("  'value' or 'LTV' - low current-year profitability partly just means young-by-construction,")
print("  not low-potential.")
print("- Clients acquired inside their own first-send month cannot match UCP at their anchor (the")
print("  month-end BEFORE first exposure predates their existence as a client) - see M0b for the")
print("  size of this population by anchor month.")
print("- Band cut points (TENURE_EDGES, AGE_EDGES) and the high_potential thresholds (HP_AGE_MAX,")
print("  HP_TENURE_MAX, HP_PROD_MAX) are OUR PARAMETERS, editable in cell [17] - not documented or")
print("  standardised anywhere else in the repo.")
print("- prof_quintile cut points are relative to THIS run's mailed population (Jan-May 2026) - a")
print("  re-run over a different window shifts them; Table A/B/C numbers are not comparable")
print("  run-to-run without re-stating the cut points printed in cell [17].")

# %% [22] OPEN QUESTIONS (unverified, flag before this ships anywhere)
# - P3 QUARTERLY CHUNKING: not spelled out verbatim in the brief ("chunk by quarter if the probe
#   says it's large" describes a decision rule, but the SIZE PROBE in cell [3] only sizes P1's
#   disp_cd=1 sends, not P3's disp_cd=4 unsubs). This file chunks P3 into quarters UNCONDITIONALLY
#   as a judgment call, reasoning that an unbounded-forward EVENT window only grows on every future
#   rerun and quarterly land() calls keep each chunk small and independently restartable. Revisit
#   if Andre wants a literal size-probe-then-decide gate on P3 specifically.
# - LEAVER ATTRIBUTION (Table A) vs PER-CAMPAIGN ATTRIBUTION (Table B): Table A's bucket picks ONE
#   mne per client (their earliest qualifying unsub, across all campaigns that mailed them). Table B
#   asks a per-(client, mne) question instead - a client can be a "leaver" for one campaign's row
#   and NOT a leaver for another campaign's row in the same table, by design (see Table B's header
#   comment). These two views are NOT reconcilable into a single "who caused the unsub" number.
# - prod_band '0': the brief's literal list is '1','2','3-4','5+' - '0' was added (cell [17]) so
#   zero-product mailed clients are not silently folded into '1'. If Andre wants '0' folded into
#   '1' instead (i.e. exactly 4 bands as literally listed), that's a one-line change in apply_bands().
# - CLNT_TYP == 'Personal' filter is applied WITHOUT a runtime presence probe (unlike
#   archaeology/28's HAS_CLNT_TYP defensive check) - the brief states the field and value are
#   "confirmed present 2026-07-27", so this file trusts that rather than re-verifying. If UCP's
#   schema has since changed, cell [15] will raise a hard AnalysisException on the missing column,
#   not silently skip the filter.
# - UCP uniqueness per (CLNT_NO, MONTH_END_DATE) is asserted by cell [15]'s fan-out guard, not
#   independently verified beyond that; the dedup fallback is commented immediately below the assert.
# - EXCLUDED bucket definition checks ANY prior unsub 2024-2025 bank-wide (P4, disposition_cd=4,
#   no MNE scoping) - it is not restricted to the same mne the client was mailed by in Jan-May 2026.
#   A client who unsubbed from an unrelated program in 2025 and is mailed by a totally different
#   campaign in 2026 is still "excluded", not "stayer". This matches the brief's literal P4
#   definition (SELECT DISTINCT CLNT_NO, no mne column) - flagged here as a modelling choice, not
#   independently confirmed with Andre.
# - Table B's expected_per_1000 standardises on (age_band, tenure_band, prod_band) only - NOT
#   prof_quintile, which is highly correlated with age/tenure by construction (see the "PROF_TOT_
#   ANNUAL rises with tenure" caveat above). Leaving prof_quintile out of the standardisation cells
#   was a judgment call to keep the reference population per cell reasonably sized; revisit if a
#   4-dimension standardisation is wanted instead.
# - MASTER load_tm bounds throughout this file use a SYMMETRIC +/-1 month buffer around each EVENT
#   window's edges (not the -1/+2 buffer cpc_reservoir_extract.py used for sends_cards_q2) - chosen
#   to match the brief's literal "~+/-1 month" wording. If a pull ever undercounts because a late-
#   loading MASTER record falls outside this +/-1 month window, widen the buffer for that pull.

# %% [23] SYNTAX CHECK - parse this file's own source before handing it off.
import ast

with open(__file__ if "__file__" in dir() else "unsub_value_museum.py", "r", encoding="utf-8") as _f:
    _source = _f.read()
ast.parse(_source)
print("ast.parse PASSED - script is syntactically valid.")
