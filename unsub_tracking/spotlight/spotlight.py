# unsub_tracking/spotlight/spotlight.py
#
# Power Pack Phase 1 (due 2026-08-02), Maya Patel's brief. Two spotlights, scope locked to:
#   Spotlight 1 - (a) unsubs by cards campaign, absolute + frequency; (b) unsubs by breadth
#     (n campaigns) and by n sends; (c) do campaigns spike unsubs alone or in combination.
#   Spotlight 2 - unsubs by product depth (1-4 TIBC categories held).
# Nothing else is in scope. No value-of-an-unsub, no before/after, no CPC consent chain.
#
# FRESH BUILD, not a continuation of unsub_tracking/museum or archaeology (Andre's instruction).
# Only TECHNICAL facts were carried over from there - table/column names, the MNE substring rule,
# the Spark/Teradata connection idiom, the bite/land pattern, the CARDS_MNES v2 list. The cell
# layout, bucket edges, and analysis structure below are new, built to this brief only.
#
# ============================== VERIFIED FACTS (file:line evidence) ==========================
#
# UNSUB EVENT = DTZV01.VENDOR_FEEDBACK_EVENT.disposition_cd = 4.
#   unsub_tracking/UNSUB_TRACKING_KNOWLEDGE.md:130 ("disposition_cd ... 4=unsubscribed").
#   Used identically in unsub_tracking/museum/unsub_value_museum.py:448,635,764.
#
# DENOMINATOR (clients mailed) = disposition_cd = 1 (sent) on the SAME table, same join path.
#   UNSUB_TRACKING_KNOWLEDGE.md:117,130. unsub_value_museum.py:465-469 (Tier 1 senders pull).
#
# MNE = SUBSTR(TREATMENT_ID, 8, 3). TREATMENT_ID = TACTIC_ID (same field, two table names).
#   UNSUB_TRACKING_KNOWLEDGE.md:145. unsub_value_museum.py:462,500,567,609,692 (identical SUBSTR
#   call in every pull). Exact IN-list match on the extracted 3-char code, never a substring
#   match on the raw TREATMENT_ID.
#
# JOIN: EVENT.(consumer_id_hashed, TREATMENT_ID) = MASTER.(consumer_id_hashed, TREATMENT_ID).
#   MASTER carries CLNT_NO - EVENT does not. UNSUB_TRACKING_KNOWLEDGE.md:151-165.
#   TREATMENT_ID is unique per deployment wave (encodes MNE + julian date), so NO time-window
#   join condition is needed or wanted - UNSUB_TRACKING_KNOWLEDGE.md:163.
#
# ENGINE: Cell [1] is Teradata-direct (DTZV01.* tables, no catalog prefix, Teradata syntax,
#   teradatasql connector). Every cell after it is PySpark (YARN, Lumina pre-initialized
#   session) reading landed parquet off HDFS - no further EDW connection is opened.
#   references/query_engine_guidelines.md "PySpark (YARN) - Known Gotchas".
#
# CARDS_MNES v2 (12 MNEs) - carried VERBATIM from unsub_value_museum.py:119-120 (Andre,
#   2026-07-31, today): PCQ, PCL, PCD, AUH, CLI, CRV, VBA, VBU, CRO, CEC, VIF, MET.
#   CTU/O2P deliberately EXCLUDED (unsub_value_museum.py:117-118 - reported under async, not
#   the cards package). MVP is NOT in this list (dropped 2026-07-31 - Borealis orchestration,
#   not a cards campaign - supersedes the 7-MNE list in UNSUB_TRACKING_KNOWLEDGE.md section 4).
#
# PRODUCT DEPTH (Spotlight 2) = COUNT of NONZERO UCP TIBC category columns (T_TOT_CNT,
#   I_TOT_CNT, B_TOT_CNT, C_TOT_CNT), NOT their sum. unsub_value_museum.py:1126-1174 (BAND_VERSION
#   v4, dated 2026-07-31 - today) documents exactly this distinction after getting it wrong once:
#   summing the four counts measures ACCOUNT VOLUME (three chequing accounts scores 3, same as
#   chequing+investment+mortgage) - it is not depth. COUNT of categories held (0-4) is what the
#   brief's "1 through 4" is asking for. This is a semantic/schema fact, not borrowed analysis
#   structure, and the brief needs it to avoid silently building the wrong metric.
#
# UCP SOURCE: /prod/sz/tsz/00172/data/ucp4/ (personal clients only), partitioned by
#   MONTH_END_DATE (a PATH segment, not a column - references/ucp/README.md:56-64). Filter
#   trim(CLNT_TYP) == 'Personal' defensively even though the path is nominally personal-only
#   (references/ucp/README.md:97, gotchas.md #6). The 4 TIBC columns + CLNT_NO + CLNT_TYP are
#   part of an 11-column core CONFIRMED against a live printSchema() 2026-07-27
#   (references/ucp/README.md:14-21) - not a guess, but this file still schema-probes before
#   trusting them (Cell [4]), because "confirmed on one date" is not "confirmed forever".
#
# ============================== OPEN QUESTIONS - see delivery report for the full list =========
# - Whether disposition_cd=4 is a per-list (per-MNE) unsub or a global/ESP-level opt-out is NOT
#   settled anywhere in UNSUB_TRACKING_KNOWLEDGE.md. Cell [5]'s "unsub_both/a_only/b_only" reads
#   as "did this client unsub from ANY tracked cards MNE", not "did they unsub specifically from
#   both a and b" - flagged inline at Cell [5] and in the report.
# - Cell [3]'s cohort_month is an ENTRY-COHORT (first send month per client x mne), not a
#   calendar-month send-volume trend - Cell [1]'s base grain collapses every send date to
#   MIN/MAX per client x mne, so true month-by-month volume is not recoverable from it without a
#   second, bigger pull. Flagged at Cell [3] and in the report.


# %% [0] CONFIG - every tunable lives here. No literal below this cell is hand-typed elsewhere.

# ---- Analysis window ----
WIN_FLOOR = "2025-08-01"   # 12 months. Answers the whole brief; ~55% less scan than the 2024 floor.
                           # Repo hard rule is a FLOOR of 2024-01-01 - never go below it.
# No fixed end date on purpose - every pull is "floor to now", so a rerun next sprint picks up new
# sends/unsubs without editing this file. Add WIN_END here and thread it into Cell [1] if a fixed
# cutoff is ever wanted instead.

# ---- Cards MNE scope: CARDS_MNES v2, 12 MNEs (see file header for citation) ----
CARDS_MNES = frozenset({"PCQ", "PCL", "PCD", "AUH", "CLI", "CRV",
                        "VBA", "VBU", "CRO", "CEC", "VIF", "MET"})
assert len(CARDS_MNES) == 12, "CARDS_MNES v2 should hold exactly 12 MNEs - recount before running"
CARDS_SQL_LIST = ", ".join("'" + m + "'" for m in sorted(CARDS_MNES))

# ---- Bite plan for the one expensive pull (Cell [1]) ----
N_BITES = 10   # MOD(CLNT_NO, N_BITES) - one independent Teradata pull per bite, each landed and
               # checked before running, so a killed run resumes at the next un-landed bite.

SMOKE = True   # True  -> pull bite 0 only (10% client sample, ~20 min). Every cell below runs and
               # prints real numbers; rates are identical, counts are 1/10.
               # False -> all 10 bites, full population. Flip once the output looks right.

# ---- Bucket edges (Cell [4]) - edit here only, never inline in a groupBy ----
# (lo, hi, label) - hi=None means "lo and above".
NCAMP_EDGES = [(1, 1, "1"), (2, 2, "2"), (3, 4, "3-4"), (5, 8, "5-8"), (9, 12, "9+")]
NSEND_EDGES = [(1, 1, "1"), (2, 3, "2-3"), (4, 6, "4-6"), (7, 12, "7-12"), (13, None, "13+")]
# Product depth (TIBC categories held) is 0-4 by construction - no edges needed, the integer IS
# the bucket (see file header note on prod_cnt vs prod_cat_cnt).

# ---- Paths ----
BASE = "hdfs:///user/427966379/unsub_spotlight/"   # reference_andre_hdfs_user_path.md
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"          # references/ucp/README.md - personal only
OUT_DIR = "./spotlight_output/"                      # CSV landing spot for Cells [3] and [4]

import os
os.makedirs(OUT_DIR, exist_ok=True)
print("CONFIG loaded - floor:", WIN_FLOOR, "| cards MNEs:", len(CARDS_MNES), "| bites:", N_BITES)
print("MNE list:", sorted(CARDS_MNES))


# %% [1] PULL - the ONE expensive cell. Teradata-direct EDW pull, bitten by MOD(CLNT_NO,10),
# landed to HDFS one bite at a time so a killed run resumes at the next missing bite.
# ENGINE: Teradata-direct (DTZV01.* tables, no catalog prefix, Teradata syntax).
#
# Grain landed: one row per (clnt_no, mne). Columns: clnt_no, mne, n_sends, first_send_dt,
# last_send_dt, unsub_flag, unsub_dt. See file header for the unsub-definition and join-key
# citations - this query implements exactly that chain, nothing else.

import getpass
import time
import pandas as pd
import teradatasql
from pyspark.sql import functions as F

# PySpark <3.4 calls pdf.iteritems() inside createDataFrame; pandas 2.0 removed it.
if not hasattr(pd.DataFrame, "iteritems"):
    pd.DataFrame.iteritems = pd.DataFrame.items

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")
EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com", user=username, password=password,
                          logmech="LDAP")

# PROOF, not a print: round-trip the connection before trusting any pull off it.
_cur = EDW.cursor()
_cur.execute("SELECT USER, SESSION, CURRENT_TIMESTAMP")
print("EDW round-trip returned:", _cur.fetchall())
_cur.close()


def edw_pd(sql, chunksize=1_000_000):
    parts, n, t0 = [], 0, time.time()
    for c in pd.read_sql(sql, EDW, chunksize=chunksize):
        parts.append(c); n += len(c)
        print("  ...", n, "rows,", int(time.time() - t0), "s elapsed", flush=True)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _landed(path):
    try:
        spark.read.parquet(BASE + path).limit(1).collect()
        return True
    except Exception as e:
        msg = str(e).lower()
        if any(s in msg for s in ("path does not exist", "path_not_found", "filenotfound",
                                  "unable to infer schema")):
            return False
        raise RuntimeError(path + ": cannot verify HDFS state, refusing to guess. " + str(e)[:300])


def land_bite(bite):
    """One bite = one CLNT_NO-mod slice, independently resumable. Skips if already landed - this
    is what makes a killed 10-bite run safe to just rerun top to bottom."""
    name = "base/bite_%d" % bite
    if _landed(name):
        n = spark.read.parquet(BASE + name).count()
        print(name, ": already landed,", n, "rows - SKIP")
        return
    sql = """
    SELECT m.CLNT_NO AS clnt_no,
           SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne,
           SUM(CAST(CASE WHEN ek.disposition_cd = 1 THEN 1 ELSE 0 END AS BIGINT)) AS n_sends,
           MIN(CASE WHEN ek.disposition_cd = 1 THEN CAST(ek.disposition_dt_tm AS DATE) END) AS first_send_dt,
           MAX(CASE WHEN ek.disposition_cd = 1 THEN CAST(ek.disposition_dt_tm AS DATE) END) AS last_send_dt,
           MAX(CASE WHEN ek.disposition_cd = 4 THEN 1 ELSE 0 END) AS unsub_flag,
           MIN(CASE WHEN ek.disposition_cd = 4 THEN CAST(ek.disposition_dt_tm AS DATE) END) AS unsub_dt
    FROM (
        SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, disposition_cd, disposition_dt_tm
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd IN (1, 4)
          AND disposition_dt_tm >= DATE '%s'
          AND SUBSTR(TREATMENT_ID, 8, 3) IN (%s)
    ) ek
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
      ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
    WHERE m.load_tm >= DATE '%s'
      AND MOD(m.CLNT_NO, %d) = %d
    GROUP BY m.CLNT_NO, SUBSTR(ek.TREATMENT_ID, 8, 3)
    """ % (WIN_FLOOR, CARDS_SQL_LIST, WIN_FLOOR, N_BITES, bite)
    pdf = edw_pd(sql)
    assert len(pdf) > 0, name + " pulled zero rows for bite " + str(bite) + " - investigate before proceeding"
    spark.createDataFrame(pdf).write.mode("overwrite").parquet(BASE + name)
    nback = spark.read.parquet(BASE + name).count()
    assert nback == len(pdf), name + " HDFS readback mismatch: pulled %d, read back %d" % (len(pdf), nback)
    print(name, ": landed", len(pdf), "rows, HDFS readback confirms", nback)


for _b in (range(1) if SMOKE else range(N_BITES)):
    land_bite(_b)

print("Cell [1] done - base grain landed at", BASE + "base/*", "| one row per (clnt_no, mne).")


# %% [2] Q1a - unsubs by cards campaign: absolute counts (numerator/denominator both present, no
# division here - Andre computes rates in the Excel pivot). 12 rows, print only, no CSV.
# ENGINE: PySpark (YARN), reads landed base - no new EDW connection.
#
# median_days_between_sends is an APPROXIMATION: (last_send_dt - first_send_dt) / (n_sends - 1)
# per client, then the median of that across clients. This is NOT the exact inter-send gap - the
# base grain only carries MIN/MAX send dates, not every send timestamp, and pulling every send
# timestamp bank-wide is a much bigger extract than this spotlight's scope calls for. It is a
# directional read on cadence (weekly vs biweekly vs monthly), not an exact figure.
# clients_used_for_gap (n_sends >= 2 only) is reported alongside so the reader sees the real
# denominator for that one column - it is smaller than clients_mailed by construction.

base = spark.read.parquet(BASE + "base/*")

_gap = (base.filter(F.col("n_sends") >= 2)
        .withColumn("days_span", F.datediff(F.col("last_send_dt"), F.col("first_send_dt")))
        .withColumn("approx_gap_days", F.col("days_span") / (F.col("n_sends") - 1)))

q1a = (base.groupBy("mne")
       .agg(F.countDistinct("clnt_no").alias("clients_mailed"),
            F.sum("n_sends").alias("sends"),
            F.sum(F.col("unsub_flag")).alias("unsub_clients"))
       .join(_gap.groupBy("mne")
             .agg(F.expr("percentile_approx(approx_gap_days, 0.5)").alias("median_days_between_sends"),
                  F.count("*").alias("clients_used_for_gap")),
             "mne", "left")
       .orderBy("mne"))

q1a_pd = q1a.toPandas()
print("Q1a - unsubs by cards campaign | grain: one row per mne | %d rows" % len(q1a_pd))
print(q1a_pd.to_string(index=False))


# %% [3] Q1a cadence + trend - mne x cohort_month. ~250 rows (fewer if a campaign started late).
# Print .head(30) AND write CSV. ENGINE: PySpark (YARN).
#
# COHORT_MONTH HERE = the month a client FIRST appears as sent-to for that mne (month of
# first_send_dt) - an ENTRY-COHORT trend, not a per-calendar-month send-volume trend. Cell [1]'s
# base grain collapses every send date to MIN/MAX per client x mne, so "how many sends happened
# in June" is not recoverable from it - that needs a second EDW pull grained by month, out of
# scope for this pass and flagged as an open question in the delivery report. What this cell DOES
# answer: when clients started being mailed by each campaign, and how many of that entry cohort
# have since unsubscribed - a real trend read, just not a send-volume one.

q1a_trend = (base
             .withColumn("cohort_month", F.date_format(F.col("first_send_dt"), "yyyy-MM"))
             .groupBy("mne", "cohort_month")
             .agg(F.countDistinct("clnt_no").alias("clients_entered"),
                  F.sum("n_sends").alias("sends"),
                  F.sum(F.col("unsub_flag")).alias("unsub_clients"))
             .orderBy("mne", "cohort_month"))

q1a_trend_pd = q1a_trend.toPandas()
print("Q1a trend - mne x cohort_month (entry-cohort) | grain: one row per mne x cohort_month | "
      "%d rows" % len(q1a_trend_pd))
print(q1a_trend_pd.head(30).to_string(index=False))

_csv_path = OUT_DIR + "q1a_cadence_trend.csv"
q1a_trend_pd.to_csv(_csv_path, index=False)
print("written:", _csv_path, "|", len(q1a_trend_pd), "rows")


# %% [4] Q1b + Q2 - n_campaigns_bucket x n_sends_bucket x prod_cnt_bucket. CSV, plus the three
# 1-D marginals printed separately (each <10 rows) so the answer is visible without opening Excel.
# ENGINE: PySpark (YARN) for the client rollup; the UCP read is Spark/HDFS parquet
# (references/ucp/README.md) - no EDW connection in this cell.
#
# n_campaigns_bucket / n_sends_bucket use the edges from Cell [0] (NCAMP_EDGES / NSEND_EDGES).
# prod_cnt_bucket = COUNT of nonzero UCP TIBC columns (T_TOT_CNT, I_TOT_CNT, B_TOT_CNT,
# C_TOT_CNT) per client, 0-4 by construction. This is the brief's "1 through 4" - see file header
# for why the sum of the four counts is the WRONG metric here.
#
# With the default 5x5x5(-ish) edges above this table lands well under the brief's ~600-row
# ballpark (most combinations, many empty) - noted here rather than silently padding the bucket
# count to hit a target that was only ever an estimate. Loosen NCAMP_EDGES/NSEND_EDGES in Cell [0]
# if finer granularity is wanted.

import datetime


def _edge_bucket(col, edges):
    expr = None
    for lo, hi, label in edges:
        cond = (col >= lo) if hi is None else ((col >= lo) & (col <= hi))
        expr = F.when(cond, label) if expr is None else expr.when(cond, label)
    return expr.otherwise("unbucketed")


base = spark.read.parquet(BASE + "base/*").withColumn("clnt_no", F.col("clnt_no").cast("string"))

client_roll = (base.groupBy("clnt_no")
               .agg(F.countDistinct("mne").alias("n_campaigns"),
                    F.sum("n_sends").alias("n_sends_total"),
                    F.max("unsub_flag").alias("any_unsub"))
               .withColumn("n_campaigns_bucket", _edge_bucket(F.col("n_campaigns"), NCAMP_EDGES))
               .withColumn("n_sends_bucket", _edge_bucket(F.col("n_sends_total"), NSEND_EDGES)))

# UCP SCHEMA PROBE - hard gate. Confirmed live 2026-07-27 (references/ucp/README.md:14-21), but
# this file still checks before trusting it - "confirmed on one date" is not "confirmed forever".
_last_month_end = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
_ucp_anchor = _last_month_end.strftime("%Y-%m-%d")
_ucp_path = UCP_BASE + "MONTH_END_DATE=" + _ucp_anchor
_TIBC_COLS = ["T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT"]
_ucp_probe = spark.read.option("basePath", UCP_BASE).parquet(_ucp_path)
_missing = [c for c in _TIBC_COLS + ["CLNT_NO", "CLNT_TYP"] if c not in _ucp_probe.columns]
assert not _missing, "UCP missing required columns at " + _ucp_anchor + ": " + str(_missing)
print("UCP SCHEMA PROBE at", _ucp_anchor, "- all required columns present:", _TIBC_COLS + ["CLNT_NO", "CLNT_TYP"])

_held = [(F.coalesce(F.col(c), F.lit(0)) > 0).cast("int") for c in _TIBC_COLS]
ucp = (_ucp_probe.filter(F.trim(F.col("CLNT_TYP")) == "Personal")
       .select("CLNT_NO", *_TIBC_COLS)
       .withColumn("clnt_no",
                   F.regexp_replace(F.trim(F.col("CLNT_NO").cast("decimal(18,0)").cast("string")), "^0+", ""))
       .withColumn("prod_cnt_bucket", (_held[0] + _held[1] + _held[2] + _held[3]).cast("string"))
       .select("clnt_no", "prod_cnt_bucket"))

client_roll = (client_roll
               .join(ucp, "clnt_no", "left")
               .withColumn("prod_cnt_bucket", F.coalesce(F.col("prod_cnt_bucket"), F.lit("no_ucp_match")))
               .cache())

cube = (client_roll.groupBy("n_campaigns_bucket", "n_sends_bucket", "prod_cnt_bucket")
        .agg(F.count("*").alias("clients"),
             F.sum(F.col("any_unsub")).alias("unsub_clients"))
        .orderBy("n_campaigns_bucket", "n_sends_bucket", "prod_cnt_bucket"))

cube_pd = cube.toPandas()
print("Q1b+Q2 cube - n_campaigns_bucket x n_sends_bucket x prod_cnt_bucket | grain: one row per "
      "bucket combination | %d rows" % len(cube_pd))
_csv_path = OUT_DIR + "q1b_q2_cube.csv"
cube_pd.to_csv(_csv_path, index=False)
print("written:", _csv_path)

for _dim in ("n_campaigns_bucket", "n_sends_bucket", "prod_cnt_bucket"):
    _marg = (client_roll.groupBy(_dim)
             .agg(F.count("*").alias("clients"), F.sum(F.col("any_unsub")).alias("unsub_clients"))
             .orderBy(_dim)).toPandas()
    print("\nMarginal -", _dim, "| grain: one row per", _dim, "| %d rows" % len(_marg))
    print(_marg.to_string(index=False))


# %% [5] Q1c - do specific cards campaigns spike unsubs alone, or when run together? All pairs of
# the 12 tracked cards MNEs (C(12,2) = 66 pairs). 66 rows, print only. ENGINE: PySpark (YARN).
#
# unsub_any = the client clicked unsub on AT LEAST ONE of the 12 tracked cards campaigns' emails
# in the window - a CLIENT-level flag, not a per-pair one.
# UNVERIFIED (see delivery report): whether a disposition_cd=4 unsub is list-specific (opts out of
# only that campaign's sends) or ESP/global (opts out of all RBC marketing email from that
# provider) is not settled anywhere in UNSUB_TRACKING_KNOWLEDGE.md. Read "unsub_both" as "clients
# mailed by both a and b who unsubscribed from at least one tracked cards campaign", NOT as
# "unsubscribed specifically from both a and b".

base = spark.read.parquet(BASE + "base/*")
per_client = (base.groupBy("clnt_no")
              .agg(F.collect_set("mne").alias("mne_set"),
                   F.max("unsub_flag").alias("unsub_any"))
              .cache())
_n_clients = per_client.count()
print("Q1c base population:", _n_clients, "clients mailed by >=1 tracked cards MNE in the window")

_mnes = sorted(CARDS_MNES)
_pairs = [(a, b) for i, a in enumerate(_mnes) for b in _mnes[i + 1:]]
assert len(_pairs) == 66, "expected C(12,2)=66 pairs, got " + str(len(_pairs))

_rows = []
for a, b in _pairs:
    has_a = F.array_contains(F.col("mne_set"), a)
    has_b = F.array_contains(F.col("mne_set"), b)
    agg_both = (per_client.filter(has_a & has_b)
                .agg(F.count("*").alias("c"), F.sum("unsub_any").alias("u")).collect()[0])
    agg_a = (per_client.filter(has_a & ~has_b)
             .agg(F.count("*").alias("c"), F.sum("unsub_any").alias("u")).collect()[0])
    agg_b = (per_client.filter(has_b & ~has_a)
             .agg(F.count("*").alias("c"), F.sum("unsub_any").alias("u")).collect()[0])
    _rows.append({"mne_a": a, "mne_b": b,
                  "clients_both": agg_both["c"], "unsub_both": agg_both["u"] or 0,
                  "clients_a_only": agg_a["c"], "unsub_a_only": agg_a["u"] or 0,
                  "clients_b_only": agg_b["c"], "unsub_b_only": agg_b["u"] or 0})

q1c_pd = pd.DataFrame(_rows)
print("Q1c - all pairs of the 12 tracked cards MNEs | grain: one row per (mne_a, mne_b) | %d rows"
      % len(q1c_pd))
print(q1c_pd.to_string(index=False))
