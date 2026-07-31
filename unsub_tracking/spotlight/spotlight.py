# unsub_tracking/spotlight/spotlight.py
#
# Power Pack Phase 1 (due 2026-08-02), Maya Patel's brief. Two spotlights, scope locked to:
#   Spotlight 1 - (a) unsubs by campaign, absolute + frequency; (b) unsubs by breadth (n
#     campaigns) and by n emails; (c) do campaigns spike unsubs alone or in combination.
#   Spotlight 2 - unsubs by product depth (1-4 TIBC categories held).
# Nothing else is in scope. No value-of-an-unsub, no before/after, no CPC consent chain.
#
# ============================== 2026-07-31 REDESIGN (5 fixes, Andre's review) ================
# 1. Cell [1]'s pull is NO LONGER filtered to the 12 cards MNEs. Andre: a client hit by a cards
#    campaign is simultaneously hit by other bank campaigns, and total contact load is the thing
#    being measured - filtering the pull to cards would hide that load. is_cards / is_regulatory /
#    is_branded are now CLIENT-SIDE Spark tags on every landed row, computed after the pull, not
#    a SQL WHERE clause. Andre does the cards selection himself in the Excel pivot.
# 2. Every output now lands on HDFS via Spark (BASE + "out/..."), never pandas.to_csv to local
#    disk - local disk does not exist from the YARN Spark session's point of view.
# 3. Cube 1 (profiling) rebuilt to the exact dimension/measure spec below - no quintile, no
#    tibc_mix, no depth_band. prod_cat_cnt is the bare integer (0-4), not a banded string.
# 4. The 66 mne-pair cell is DELETED (Andre: "why do I need pairs"). Replaced by Cube 2, a
#    contact-frequency cube: how many branded campaigns and how many emails each client got,
#    stayer vs leaver. This required adding trailing-3m/6m send counts to Cell [1]'s SQL directly
#    (the old MIN/MAX-per-client-mne grain cannot be aggregated into a trailing window after the
#    fact) - no second pull was added.
# 5. The UCP join (Cell [4]) now casts CLNT_NO through decimal(18,0)->long on BOTH sides before
#    any comparison, prints 5 sample ids from each side before joining, and HARD-ERRORS if match
#    rate < 90%. See memory note pandas_float_keys_scientific_notation: pandas float64 client ids
#    render in scientific notation on a straight string cast, which silently zero-matches a join.
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
#   2026-07-31). PCQ, PCL, PCD, AUH, CLI, CRV, VBA, VBU, CRO, CEC, VIF, MET. Used ONLY to tag
#   is_cards client-side (Fix 1) - never as a SQL filter.
#
# REGULATORY_MNES (22 MNEs) - transcribed from a pivot on ACTION_TYPE='Regulatory', carried
#   VERBATIM from unsub_tracking/museum/RUN_2026-07-30_REGULATORY.md:12-33 ("The 22" table).
#   That file's own closing note (lines 69-73) flags that ACTION_TYPE, if it can be reached and
#   joined on TACTIC_ID/MNE, should REPLACE this hardcoded list permanently - it goes stale the
#   moment a campaign is added or reclassified. OPEN QUESTION FOR ANDRE, carried forward
#   unchanged: which table holds ACTION_TYPE, and can it be joined here.
#
# PRODUCT DEPTH (Spotlight 2) = COUNT of NONZERO UCP TIBC category columns (T_TOT_CNT,
#   I_TOT_CNT, B_TOT_CNT, C_TOT_CNT), NOT their sum. unsub_value_museum.py:1126-1174 (BAND_VERSION
#   v4) documents exactly this distinction - summing the four counts measures ACCOUNT VOLUME, not
#   depth. COUNT of categories held (0-4) is the brief's "1 through 4". Andre separately rejected
#   any further TIBC "mix" derivation on top of this count (2026-07-31) - Cube 1 uses the bare
#   integer 0-4 as the dimension, no band, no mix label.
#
# UCP SOURCE: /prod/sz/tsz/00172/data/ucp4/ (personal clients only), partitioned by
#   MONTH_END_DATE (a PATH segment, not a column - references/ucp/README.md:56-64,88-97). Filter
#   trim(CLNT_TYP) == 'Personal' defensively (README.md:97, "defensive filter, see gotchas.md #6";
#   gotchas.md:160-165 confirms CLNT_TYP exists live on ucp4 as of 2026-07-27, filter still
#   applied even though the path is nominally personal-only). Snapshot month = last CLOSED month
#   (README.md:86-89, gotchas.md #1) - never a fixed hardcoded date, current month has no
#   partition yet.
#
# ============================== OPEN QUESTIONS - see delivery report for the full list =========
# - Whether disposition_cd=4 is a per-list (per-MNE) unsub or a global/ESP-level opt-out is NOT
#   settled anywhere in UNSUB_TRACKING_KNOWLEDGE.md. Cube 1's "bucket" (STAYER/LEAVER) is read at
#   (clnt_no, mne) grain using that row's own unsub_flag - if disposition_cd=4 is actually a
#   global opt-out that gets logged against whichever campaign's send triggered it, a LEAVER tag
#   on one mne does not necessarily mean the client unsubscribed FROM that mne specifically.
# - Which table holds ACTION_TYPE and whether it joins to TACTIC_ID/MNE (see REGULATORY_MNES
#   note above) - would let REGULATORY_MNES be derived instead of hardcoded.


# %% [0] CONFIG - every tunable lives here. No literal below this cell is hand-typed elsewhere.

import calendar
import datetime

# ---- Analysis window ----
WIN_FLOOR = "2025-08-01"   # 12 months. Repo hard rule is a FLOOR of 2024-01-01 - never go below.
# No fixed end date on purpose - every pull is "floor to now", so a rerun next sprint picks up new
# sends/unsubs without editing this file.

# ---- MNE tag sets - CLIENT-SIDE tags only (Fix 1). Never used as a SQL filter. ----
CARDS_MNES = frozenset({"PCQ", "PCL", "PCD", "AUH", "CLI", "CRV",
                        "VBA", "VBU", "CRO", "CEC", "VIF", "MET"})
assert len(CARDS_MNES) == 12, "CARDS_MNES v2 should hold exactly 12 MNEs - recount before running"

# 22 regulatory MNEs, verbatim from unsub_tracking/museum/RUN_2026-07-30_REGULATORY.md:12-33.
# All 22 rows of that table are accounted for below - none dropped, none guessed.
REGULATORY_MNES = frozenset({
    "AFD", "BPU", "BUK", "CFR", "EOE", "FNE", "FSA", "FSO", "FXR", "GAF", "HFC",
    "HPN", "IOO", "NST", "OTC", "PUK", "ROP", "TWI", "VMF", "VOA", "ZDC", "ZHX",
})
assert len(REGULATORY_MNES) == 22, "REGULATORY_MNES should hold exactly 22 MNEs - recount vs museum file"

# ---- Bite plan for the one expensive pull (Cell [1]) ----
N_BITES = 10   # MOD(CLNT_NO, N_BITES) - one independent Teradata pull per bite, each landed and
               # checked before running, so a killed run resumes at the next un-landed bite.

SMOKE = True   # True  -> pull bite 0 only. Bank-wide (no MNE filter) makes even one bite bigger
               # than the old cards-only pull - check bite 0's row count before flipping this.
               # False -> all 10 bites, full population.

# ---- Trailing send-frequency windows (Fix 4) - months, tunable in one place ----
FREQ_WINDOWS = [3, 6]   # -> n_sends_3m, n_sends_6m columns in Cell [1]; n_emails_3m/6m downstream.


def _months_ago(n_months, anchor=None):
    """Calendar-month subtraction, stdlib only (no dateutil dependency)."""
    anchor = anchor or datetime.date.today()
    total = anchor.year * 12 + (anchor.month - 1) - n_months
    y, m = divmod(total, 12)
    m += 1
    day = min(anchor.day, calendar.monthrange(y, m)[1])
    return datetime.date(y, m, day)


# Computed ONCE, at Cell [0] run time, and baked into every bite's SQL in Cell [1] - all bites in
# one run share the same cutoff dates, which is what "trailing N months as of today" should mean.
FREQ_CUTOFFS = {n: _months_ago(n).isoformat() for n in FREQ_WINDOWS}

# ---- Band edges (Cube 1: age_band, tenure_band) - carried from the only prior in-repo precedent,
# references/ucp/gotchas.md #7 (lines 172-178, unsub_tracking/archaeology/15_unsub_value_enrichment.py
# :220-233). No house standard exists anywhere in the repo - these are an editable assumption, not
# a confirmed standard. (lo, hi, label); lo=None means "< hi", hi=None means ">= lo".
AGE_EDGES = [(None, 25, "<25"), (25, 34, "25-34"), (35, 49, "35-49"), (50, 64, "50-64"), (65, None, "65+")]
TENURE_EDGES = [(None, 1, "<1yr"), (1, 3, "1-3yr"), (4, 7, "4-7yr"), (8, 15, "8-15yr"), (16, None, "16yr+")]

# ---- Bucket edges (Cube 2: contact-frequency dims) - FIRST-CUT assumptions, unverified against
# the real bank-wide distribution (that distribution is exactly what the SMOKE run will show).
# Tune after looking at bite-0 output, not before. No house standard exists for any of these.
BRANDED_CAMP_EDGES = [(0, 0, "0"), (1, 1, "1"), (2, 3, "2-3"), (4, 7, "4-7"), (8, None, "8+")]
EMAILS_3M_EDGES = [(0, 0, "0"), (1, 3, "1-3"), (4, 8, "4-8"), (9, 20, "9-20"), (21, None, "21+")]
EMAILS_6M_EDGES = [(0, 0, "0"), (1, 5, "1-5"), (6, 15, "6-15"), (16, 35, "16-35"), (36, None, "36+")]
PROMO_EMAILS_6M_EDGES = EMAILS_6M_EDGES
REG_EMAILS_6M_EDGES = [(0, 0, "0"), (1, 1, "1"), (2, 3, "2-3"), (4, None, "4+")]

# ---- Paths - HDFS only (Fix 2). No local disk path anywhere in this file. ----
BASE = "hdfs:///user/427966379/unsub_spotlight/"   # reference_andre_hdfs_user_path.md
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"          # references/ucp/README.md - personal only

# Landed-schema version. The _landed() cache guard checks path existence, NOT columns - so a
# schema change silently reuses stale parquet from a prior design. Bump this whenever Cell [1]'s
# output columns change; the new schema lands in a fresh directory and cannot collide.
# v1 = cards-filtered, untagged. v2 = bank-wide, is_cards/is_regulatory/is_branded, n_sends_3m/6m.
SCHEMA_VERSION = 2

# Floor for the UCP join guard. Catches a broken join key (which shows near-zero), NOT ordinary
# non-match: UCP is personal-only on one closed-month snapshot, so business/commercial recipients
# and clients who closed after their last send never match. Observed 2026-07-31: 87%.
UCP_MATCH_FLOOR = 70.0
BASE_DIR = BASE + "base_v%d/" % SCHEMA_VERSION

BASE_COLS = ["clnt_no", "mne", "n_sends", "first_send_dt", "last_send_dt", "unsub_flag",
             "unsub_dt", "is_cards", "is_regulatory", "is_branded"]


def read_base():
    """Every downstream cell reads the base through here so a stale/short schema fails loudly."""
    sdf = spark.read.parquet(BASE_DIR + "*")
    missing = [c for c in BASE_COLS if c not in sdf.columns]
    if missing:
        raise RuntimeError(
            "base at %s is missing %s. Found: %s. This is stale parquet from an older schema - "
            "delete %s and rerun Cell [1]." % (BASE_DIR, missing, sdf.columns, BASE_DIR))
    return sdf.withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))

print("CONFIG loaded - floor:", WIN_FLOOR, "| cards MNEs:", len(CARDS_MNES),
      "| regulatory MNEs:", len(REGULATORY_MNES), "| bites:", N_BITES, "| SMOKE:", SMOKE)
print("FREQ_WINDOWS:", FREQ_WINDOWS, "-> cutoffs:", FREQ_CUTOFFS)


# %% [1] PULL - the ONE expensive cell. Teradata-direct EDW pull, bitten by MOD(CLNT_NO,10),
# landed to HDFS one bite at a time so a killed run resumes at the next missing bite.
# ENGINE: Teradata-direct (DTZV01.* tables, no catalog prefix, Teradata syntax).
#
# Grain landed: one row per (clnt_no, mne), BANK-WIDE - every mnemonic, not just cards (Fix 1).
# Columns: clnt_no, mne, n_sends, n_sends_3m, n_sends_6m (per FREQ_WINDOWS), first_send_dt,
# last_send_dt, unsub_flag, unsub_dt, is_cards, is_regulatory, is_branded. The last three are
# added CLIENT-SIDE in Spark right before landing, not in the SQL (Fix 1).

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


def _tag_mne_client_side(sdf):
    """Fix 1: is_cards / is_regulatory / is_branded computed in Spark, never in SQL."""
    mne_u = F.upper(F.trim(F.col("mne")))
    unbranded = mne_u.isNull() | (mne_u == "") | (mne_u == "DEFAULT")
    return (sdf
            .withColumn("is_branded", F.when(unbranded, F.lit(0)).otherwise(F.lit(1)))
            .withColumn("is_cards", F.when(mne_u.isin(sorted(CARDS_MNES)), F.lit(1)).otherwise(F.lit(0)))
            .withColumn("is_regulatory", F.when(mne_u.isin(sorted(REGULATORY_MNES)), F.lit(1)).otherwise(F.lit(0))))


def land_bite(bite):
    """One bite = one CLNT_NO-mod slice, independently resumable. Skips if already landed - this
    is what makes a killed 10-bite run safe to just rerun top to bottom."""
    name = "base_v%d/bite_%d" % (SCHEMA_VERSION, bite)
    if _landed(name):
        n = spark.read.parquet(BASE + name).count()
        print(name, ": already landed,", n, "rows - SKIP")
        return

    freq_cols_sql = "\n".join(
        "       SUM(CAST(CASE WHEN ek.disposition_cd = 1 AND CAST(ek.disposition_dt_tm AS DATE) "
        ">= DATE '%s' THEN 1 ELSE 0 END AS BIGINT)) AS n_sends_%dm," % (FREQ_CUTOFFS[n], n)
        for n in FREQ_WINDOWS
    )

    sql = """
    SELECT m.CLNT_NO AS clnt_no,
           SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne,
           SUM(CAST(CASE WHEN ek.disposition_cd = 1 THEN 1 ELSE 0 END AS BIGINT)) AS n_sends,
%s
           MIN(CASE WHEN ek.disposition_cd = 1 THEN CAST(ek.disposition_dt_tm AS DATE) END) AS first_send_dt,
           MAX(CASE WHEN ek.disposition_cd = 1 THEN CAST(ek.disposition_dt_tm AS DATE) END) AS last_send_dt,
           MAX(CASE WHEN ek.disposition_cd = 4 THEN 1 ELSE 0 END) AS unsub_flag,
           MIN(CASE WHEN ek.disposition_cd = 4 THEN CAST(ek.disposition_dt_tm AS DATE) END) AS unsub_dt
    FROM (
        SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, disposition_cd, disposition_dt_tm
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd IN (1, 4)
          AND disposition_dt_tm >= DATE '%s'
    ) ek
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
      ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
    WHERE m.load_tm >= DATE '%s'
      AND MOD(m.CLNT_NO, %d) = %d
    GROUP BY m.CLNT_NO, SUBSTR(ek.TREATMENT_ID, 8, 3)
    """ % (freq_cols_sql, WIN_FLOOR, WIN_FLOOR, N_BITES, bite)

    pdf = edw_pd(sql)
    assert len(pdf) > 0, name + " pulled zero rows for bite " + str(bite) + " - investigate before proceeding"
    sdf = _tag_mne_client_side(spark.createDataFrame(pdf))
    sdf.write.mode("overwrite").parquet(BASE + name)
    nback = spark.read.parquet(BASE + name).count()
    assert nback == len(pdf), name + " HDFS readback mismatch: pulled %d, read back %d" % (len(pdf), nback)
    print(name, ": landed", len(pdf), "rows (bank-wide, all mnemonics), HDFS readback confirms", nback)


for _b in (range(1) if SMOKE else range(N_BITES)):
    land_bite(_b)

print("Cell [1] done - base grain landed at", BASE_DIR + "*",
      "| one row per (clnt_no, mne), bank-wide, tagged is_cards/is_regulatory/is_branded.")


# %% [2] Q_MNE - unsubs by campaign, absolute counts, BANK-WIDE (Andre filters to cards in the
# Excel pivot using the is_cards column). Print only, no CSV - row count is however many distinct
# mnemonics sent mail bank-wide in the window, still small enough to print in full.
# ENGINE: PySpark (YARN), reads landed base - no new EDW connection.
#
# median_days_between_sends is an APPROXIMATION: (last_send_dt - first_send_dt) / (n_sends - 1)
# per client, then the median of that across clients - the base grain only carries MIN/MAX send
# dates, not every send timestamp. Directional cadence read, not an exact figure.

base = read_base()

_gap = (base.filter(F.col("n_sends") >= 2)
        .withColumn("days_span", F.datediff(F.col("last_send_dt"), F.col("first_send_dt")))
        .withColumn("approx_gap_days", F.col("days_span") / (F.col("n_sends") - 1)))

q_mne = (base.groupBy("mne", "is_cards", "is_regulatory", "is_branded")
         .agg(F.countDistinct("clnt_no").alias("clients_mailed"),
              F.sum("n_sends").alias("sends"),
              F.sum(F.col("unsub_flag")).alias("unsub_clients"))
         .join(_gap.groupBy("mne")
               .agg(F.expr("percentile_approx(approx_gap_days, 0.5)").alias("median_days_between_sends"),
                    F.count("*").alias("clients_used_for_gap")),
               "mne", "left")
         .orderBy("mne"))

q_mne_pd = q_mne.toPandas()
print("Q_MNE - unsubs by campaign, bank-wide | grain: one row per mne | %d rows" % len(q_mne_pd))
print(q_mne_pd.to_string(index=False))


# %% [3] Q_TREND - mne x cohort_month, BANK-WIDE. Print .head(30) and land CSV to HDFS.
# ENGINE: PySpark (YARN).
#
# COHORT_MONTH HERE = the month a client FIRST appears as sent-to for that mne (month of
# first_send_dt) - an ENTRY-COHORT trend, not a per-calendar-month send-volume trend. Cell [1]'s
# base grain collapses every send date to MIN/MAX per client x mne, so "how many sends happened
# in June" is not recoverable from it for the FULL history - only the trailing 3m/6m windows
# added in Cell [1] give windowed volume, and those are used in Cube 2, not here.

base = read_base()

q_trend = (base
           .withColumn("cohort_month", F.date_format(F.col("first_send_dt"), "yyyy-MM"))
           .groupBy("mne", "is_cards", "is_regulatory", "cohort_month")
           .agg(F.countDistinct("clnt_no").alias("clients_entered"),
                F.sum("n_sends").alias("sends"),
                F.sum(F.col("unsub_flag")).alias("unsub_clients"))
           .orderBy("mne", "cohort_month"))

q_trend_pd = q_trend.toPandas()
print("Q_TREND - mne x cohort_month (entry-cohort), bank-wide | grain: one row per mne x "
      "cohort_month | %d rows" % len(q_trend_pd))
print(q_trend_pd.head(30).to_string(index=False))

_trend_path = BASE + "out/q_trend"
q_trend.coalesce(1).write.mode("overwrite").option("header", True).csv(_trend_path)
print("written to HDFS:", _trend_path, "|", len(q_trend_pd), "rows")


# %% [4] UCP JOIN - the single most important guard in this file (Fix 5). Casts CLNT_NO through
# decimal(18,0)->long on BOTH sides (never str(float) - that renders scientific notation and
# silently zero-matches, see memory note pandas_float_keys_scientific_notation). Prints 5 sample
# ids per side BEFORE joining, then HARD-ERRORS if match rate < 90%.
#
# Snapshot: last CLOSED month-end (references/ucp/README.md:86-89, gotchas.md #1) - current month
# never has a partition. CLNT_TYP == 'Personal' filter applied per README.md:97 ("defensive
# filter, see gotchas.md #6"); gotchas.md:160-165 confirms CLNT_TYP is live on personal ucp4 as of
# the 2026-07-27 printSchema probe. Fields used: CLNT_NO, AGE, TENURE_RBC_YEARS, PROF_TOT_ANNUAL,
# T_TOT_CNT/I_TOT_CNT/B_TOT_CNT/C_TOT_CNT - all CONFIRMED live (references/ucp/field_catalog_personal.md
# RELIABILITY BANNER, lines 5-16).
# ENGINE: PySpark (YARN) reading HDFS parquet - not Trino, not Teradata.

import datetime as _dt

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

_ucp_anchor_date = _dt.date.today().replace(day=1) - _dt.timedelta(days=1)   # last closed month-end
_ucp_anchor = _ucp_anchor_date.strftime("%Y-%m-%d")
_ucp_path = UCP_BASE + "MONTH_END_DATE=" + _ucp_anchor
_TIBC_COLS = ["T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT"]
_UCP_COLS = ["CLNT_NO", "CLNT_TYP", "AGE", "TENURE_RBC_YEARS", "PROF_TOT_ANNUAL"] + _TIBC_COLS

_ucp_raw = spark.read.option("basePath", UCP_BASE).parquet(_ucp_path)
_missing = [c for c in _UCP_COLS if c not in _ucp_raw.columns]
assert not _missing, "UCP missing required columns at " + _ucp_anchor + ": " + str(_missing)
print("UCP SCHEMA PROBE at", _ucp_anchor, "- all required columns present:", _UCP_COLS)

ucp_sel = (_ucp_raw
           .filter(F.trim(F.col("CLNT_TYP")) == "Personal")
           .select(*_UCP_COLS)
           .withColumn("clnt_no_long", F.col("CLNT_NO").cast("decimal(18,0)").cast("long")))

base_ids = (read_base()
            .withColumn("clnt_no_long", F.col("clnt_no").cast("decimal(18,0)").cast("long"))
            .select("clnt_no_long").distinct())

# Sample BEFORE joining, so a format mismatch is visible immediately.
print("Sample clnt_no (base, 5):", [r.clnt_no_long for r in base_ids.limit(5).collect()])
print("Sample CLNT_NO (UCP, 5):", [r.clnt_no_long for r in ucp_sel.select("clnt_no_long").limit(5).collect()])

_ucp_ids = ucp_sel.select("clnt_no_long").distinct()

_left_n = base_ids.count()
_matched_n = base_ids.join(_ucp_ids, "clnt_no_long", "inner").count()
_match_pct = 100.0 * _matched_n / _left_n if _left_n else 0.0
print("UCP JOIN MATCH RATE - base clients:", _left_n, "| matched to UCP:", _matched_n,
      "| match pct: %.1f%%" % _match_pct)

# The guard catches a BROKEN KEY (format mismatch -> near-zero match), not ordinary attrition.
# UCP is personal-only and anchored to one closed month, so business/commercial recipients and
# clients who closed after their last send legitimately do not match. A real key break shows up
# in the single digits, not the eighties.
assert _match_pct >= UCP_MATCH_FLOOR, (
    "UCP join match rate %.1f%% (%d/%d) is below the %.0f%% floor - this looks like a broken key, "
    "not attrition. Check clnt_no normalization on both sides (decimal(18,0)->long) and the UCP "
    "snapshot date (%s)." % (_match_pct, _matched_n, _left_n, UCP_MATCH_FLOOR, _ucp_anchor)
)

# The rate itself is not the risk - BIAS in who fails to match is. Unmatched clients keep their
# rows as "no_ucp_match" (left join below), but if leavers match at a materially different rate
# than stayers, every profiling cut in Cube 1 is tilted. Print it; do not silently proceed.
_bucket_ids = (read_base()
               .groupBy("clnt_no").agg(F.max("unsub_flag").alias("any_unsub"))
               .withColumn("clnt_no_long", F.col("clnt_no").cast("decimal(18,0)").cast("long"))
               .withColumn("bucket", F.when(F.col("any_unsub") == 1, "LEAVER").otherwise("STAYER")))
_bias = (_bucket_ids
         .join(_ucp_ids.withColumn("in_ucp", F.lit(1)), "clnt_no_long", "left")
         .groupBy("bucket")
         .agg(F.count("*").alias("clients"),
              F.sum(F.coalesce(F.col("in_ucp"), F.lit(0))).alias("matched")))
print("UCP MATCH BIAS CHECK - match rate by bucket (grain: one row per STAYER/LEAVER):")
_bias.withColumn("match_pct", F.round(100.0 * F.col("matched") / F.col("clients"), 1)).show(truncate=False)
print("If STAYER and LEAVER match rates differ by more than a few points, Cube 1's profiling cuts "
      "are biased and the 'no_ucp_match' rows must be reported, not filtered out.")

# WHY clients fail to match: UCP is filtered to CLNT_TYP='Personal', so a business/commercial
# client can never match regardless of key quality. That predicts a BIMODAL distribution across
# campaigns - consumer campaigns near 100%, business campaigns near 0%, little in between.
# Random data loss would instead put every campaign near the overall rate. This cell decides which.
_mne_match = (read_base().select("mne", "is_cards", "clnt_no").distinct()
              .withColumn("clnt_no_long", F.col("clnt_no").cast("decimal(18,0)").cast("long"))
              .join(_ucp_ids.withColumn("in_ucp", F.lit(1)), "clnt_no_long", "left")
              .groupBy("mne", "is_cards")
              .agg(F.countDistinct("clnt_no_long").alias("clients"),
                   F.sum(F.coalesce(F.col("in_ucp"), F.lit(0))).alias("matched"))
              .withColumn("match_pct", F.round(100.0 * F.col("matched") / F.col("clients"), 1)))

_n_business = _mne_match.filter(F.col("match_pct") < 20).count()
_n_consumer = _mne_match.filter(F.col("match_pct") >= 80).count()
_n_middle = _mne_match.filter((F.col("match_pct") >= 20) & (F.col("match_pct") < 80)).count()
print("UCP MATCH BY CAMPAIGN - shape test. mnes <20%% matched:", _n_business,
      "| 20-80%%:", _n_middle, "| >=80%%:", _n_consumer)
print("Bimodal (big counts at both ends, small middle) => non-match is CLIENT TYPE, not a bug. "
      "Flat (everything clustered near the overall rate) => investigate the join key.")

print("\nWORST 25 campaigns by UCP match (candidates for business/commercial audiences):")
_mne_match.orderBy("match_pct").show(25, truncate=False)
print("CARDS campaigns only - these are the ones that matter for the brief:")
_mne_match.filter(F.col("is_cards") == 1).orderBy("match_pct").show(20, truncate=False)

_mne_match.coalesce(1).write.mode("overwrite").option("header", True).csv(BASE + "out/ucp_match_by_mne")
print("Landed:", BASE + "out/ucp_match_by_mne", "| grain: one row per mne.")


def _band(col, edges):
    expr = None
    for lo, hi, label in edges:
        if lo is None:
            cond = col < hi
        elif hi is None:
            cond = col >= lo
        else:
            cond = (col >= lo) & (col <= hi)
        expr = F.when(cond, label) if expr is None else expr.when(cond, label)
    return expr.otherwise("unbucketed")


_held = [(F.coalesce(F.col(c), F.lit(0)) > 0).cast("int") for c in _TIBC_COLS]

ucp_enriched = (ucp_sel
                 .withColumn("age_band", _band(F.col("AGE"), AGE_EDGES))
                 .withColumn("tenure_band", _band(F.col("TENURE_RBC_YEARS"), TENURE_EDGES))
                 .withColumn("prod_cat_cnt", (_held[0] + _held[1] + _held[2] + _held[3]))
                 .withColumnRenamed("TENURE_RBC_YEARS", "tenure_years")
                 .withColumnRenamed("PROF_TOT_ANNUAL", "prof_tot_annual")
                 .select("clnt_no_long", "age_band", "tenure_band", "prod_cat_cnt",
                         "tenure_years", "prof_tot_annual"))

# Left-join onto the FULL base client universe now, so downstream cube cells just read this table
# and get "no_ucp_match" for anyone who fell out of the >=90% - no repeated join logic downstream.
ucp_enriched_full = (base_ids
                      .join(ucp_enriched, "clnt_no_long", "left")
                      .withColumn("age_band", F.coalesce(F.col("age_band"), F.lit("no_ucp_match")))
                      .withColumn("tenure_band", F.coalesce(F.col("tenure_band"), F.lit("no_ucp_match")))
                      .withColumn("prod_cat_cnt_str", F.coalesce(F.col("prod_cat_cnt").cast("string"), F.lit("no_ucp_match")))
                      .drop("prod_cat_cnt")
                      .withColumnRenamed("prod_cat_cnt_str", "prod_cat_cnt")
                      .withColumnRenamed("clnt_no_long", "clnt_no"))

_ucp_path_out = BASE + "ucp_enriched"
ucp_enriched_full.write.mode("overwrite").parquet(_ucp_path_out)
_n_ucp = spark.read.parquet(_ucp_path_out).count()
print("Cell [4] done - ucp_enriched landed at", _ucp_path_out, "| grain: one row per clnt_no "
      "(full base universe, UCP misses labeled no_ucp_match) |", _n_ucp, "rows")


# %% [5] CUBE 1 - profiling cube (Fix 3). EXACT spec, nothing added:
# Dims:    mne, is_cards, is_regulatory, age_band, tenure_band, prod_cat_cnt (bare integer 0-4 as
#          string, or no_ucp_match)
# Measures: stayers, leavers, clients_total, mean_tenure_stayers/leavers,
#          median_prof_stayers/leavers (percentile_approx .5, NOT quintile)
#
# WIDE, not long (Andre, 2026-07-31). stayers and leavers are COLUMNS, not values of a "bucket"
# dimension. In long format a cell with leavers but zero stayers has no STAYER row at all, so a
# pivot silently divides by the wrong denominator. Wide guarantees both numbers on every row and
# halves the row count. Rate stays out of the file - Andre derives leavers/(stayers+leavers).
#
# Grain: one row per (clnt_no, mne) from base, so a client counts as a leaver against the mne
# their unsub_flag sits on - see the open-question note at the top of this file on what a per-mne
# leaver tag actually means if disposition_cd=4 turns out to be a global opt-out.
# ENGINE: PySpark (YARN).

base = read_base()
ucp_enriched_full = spark.read.parquet(BASE + "ucp_enriched")

cube1_src = base.join(ucp_enriched_full, "clnt_no", "left")

_stay = F.col("unsub_flag") == 0
_leave = F.col("unsub_flag") == 1

cube1 = (cube1_src
         .groupBy("mne", "is_cards", "is_regulatory", "age_band", "tenure_band", "prod_cat_cnt")
         .agg(F.sum(F.when(_stay, 1).otherwise(0)).alias("stayers"),
              F.sum(F.when(_leave, 1).otherwise(0)).alias("leavers"),
              F.count("*").alias("clients_total"),
              F.avg(F.when(_stay, F.col("tenure_years"))).alias("mean_tenure_stayers"),
              F.avg(F.when(_leave, F.col("tenure_years"))).alias("mean_tenure_leavers"),
              F.expr("percentile_approx(CASE WHEN unsub_flag = 0 THEN prof_tot_annual END, 0.5)")
               .alias("median_prof_stayers"),
              F.expr("percentile_approx(CASE WHEN unsub_flag = 1 THEN prof_tot_annual END, 0.5)")
               .alias("median_prof_leavers"))
         .orderBy("mne", "age_band", "tenure_band", "prod_cat_cnt"))

cube1_pd = cube1.toPandas()
print("CUBE 1 - profiling | grain: one row per (mne, is_cards, is_regulatory, age_band, "
      "tenure_band, prod_cat_cnt) | stayers and leavers are COLUMNS | %d rows" % len(cube1_pd))
print(cube1_pd.head(30).to_string(index=False))

_cube1_path = BASE + "out/cube1_profiling"
cube1.coalesce(1).write.mode("overwrite").option("header", True).csv(_cube1_path)
print("written to HDFS:", _cube1_path, "|", len(cube1_pd), "rows")


# %% [6] CUBE 2 - contact-frequency cube (Fix 4, replaces the deleted 66-pair cell). Andre: "why
# do I need pairs" - the real question is how many campaigns and how many emails a client got, and
# whether that differs between stayers and leavers.
# Dims:    bucket (STAYER/LEAVER, CLIENT-level - any_unsub across ALL mnemonics),
#          n_branded_campaigns_bucket, n_emails_3m_bucket, n_emails_6m_bucket,
#          n_promo_emails_6m_bucket, n_regulatory_emails_6m_bucket, prod_cat_cnt
# Measures: clients
# n_branded_campaigns = distinct mne where is_branded=1. n_emails_3m/6m = SUM of the trailing-
# window send counts added to Cell [1]'s SQL directly (no second pull). promotional = branded AND
# NOT regulatory; regulatory = in REGULATORY_MNES. n_emails_all is also reported per client so
# Andre can compare full-window volume, stayer vs leaver, not just the trailing windows.
# ENGINE: PySpark (YARN).

base = read_base()
ucp_enriched_full = spark.read.parquet(BASE + "ucp_enriched")


def _band(col, edges):
    expr = None
    for lo, hi, label in edges:
        if lo is None:
            cond = col < hi
        elif hi is None:
            cond = col >= lo
        else:
            cond = (col >= lo) & (col <= hi)
        expr = F.when(cond, label) if expr is None else expr.when(cond, label)
    return expr.otherwise("unbucketed")


_is_promo = (F.col("is_branded") == 1) & (F.col("is_regulatory") == 0)

client_roll = (base.groupBy("clnt_no")
               .agg(F.countDistinct(F.when(F.col("is_branded") == 1, F.col("mne"))).alias("n_branded_campaigns"),
                    F.sum("n_sends").alias("n_emails_all"),
                    F.sum("n_sends_3m").alias("n_emails_3m"),
                    F.sum("n_sends_6m").alias("n_emails_6m"),
                    F.sum(F.when(_is_promo, F.col("n_sends_6m")).otherwise(0)).alias("n_promo_emails_6m"),
                    F.sum(F.when(F.col("is_regulatory") == 1, F.col("n_sends_6m")).otherwise(0)).alias("n_regulatory_emails_6m"),
                    F.max("unsub_flag").alias("any_unsub"))
               .withColumn("bucket", F.when(F.col("any_unsub") == 1, "LEAVER").otherwise("STAYER"))
               .withColumn("n_branded_campaigns_bucket", _band(F.col("n_branded_campaigns"), BRANDED_CAMP_EDGES))
               .withColumn("n_emails_3m_bucket", _band(F.col("n_emails_3m"), EMAILS_3M_EDGES))
               .withColumn("n_emails_6m_bucket", _band(F.col("n_emails_6m"), EMAILS_6M_EDGES))
               .withColumn("n_promo_emails_6m_bucket", _band(F.col("n_promo_emails_6m"), PROMO_EMAILS_6M_EDGES))
               .withColumn("n_regulatory_emails_6m_bucket", _band(F.col("n_regulatory_emails_6m"), REG_EMAILS_6M_EDGES))
               .join(ucp_enriched_full.select("clnt_no", "prod_cat_cnt"), "clnt_no", "left")
               .withColumn("prod_cat_cnt", F.coalesce(F.col("prod_cat_cnt"), F.lit("no_ucp_match"))))

cube2 = (client_roll
         .groupBy("n_branded_campaigns_bucket", "n_emails_3m_bucket", "n_emails_6m_bucket",
                   "n_promo_emails_6m_bucket", "n_regulatory_emails_6m_bucket", "prod_cat_cnt")
         .agg(F.sum(F.when(F.col("any_unsub") == 0, 1).otherwise(0)).alias("stayers"),
              F.sum(F.when(F.col("any_unsub") == 1, 1).otherwise(0)).alias("leavers"),
              F.count("*").alias("clients_total"))
         .orderBy("n_branded_campaigns_bucket", "n_emails_6m_bucket"))

cube2_pd = cube2.toPandas()
print("CUBE 2 - contact frequency | grain: one row per (n_branded_campaigns_bucket, "
      "n_emails_3m_bucket, n_emails_6m_bucket, n_promo_emails_6m_bucket, "
      "n_regulatory_emails_6m_bucket, prod_cat_cnt) | stayers and leavers are COLUMNS "
      "| %d rows" % len(cube2_pd))
print(cube2_pd.head(30).to_string(index=False))

_cube2_path = BASE + "out/cube2_frequency"
cube2.coalesce(1).write.mode("overwrite").option("header", True).csv(_cube2_path)
print("written to HDFS:", _cube2_path, "|", len(cube2_pd), "rows")

# n_emails_all is on client_roll (used to build cube2) but not in cube2's own dims/measures list
# per spec - land it separately as its own small summary so the stayer/leaver full-volume compare
# Andre asked for ("that comparison is the entire point of this cube") is directly visible.
_s = F.col("any_unsub") == 0
_l = F.col("any_unsub") == 1
q_emails_all = client_roll.agg(
    F.sum(F.when(_s, 1).otherwise(0)).alias("stayers"),
    F.sum(F.when(_l, 1).otherwise(0)).alias("leavers"),
    F.sum(F.when(_s, F.col("n_emails_all")).otherwise(0)).alias("total_emails_stayers"),
    F.sum(F.when(_l, F.col("n_emails_all")).otherwise(0)).alias("total_emails_leavers"),
    F.expr("percentile_approx(CASE WHEN any_unsub = 0 THEN n_emails_all END, 0.5)")
     .alias("median_emails_stayers"),
    F.expr("percentile_approx(CASE WHEN any_unsub = 1 THEN n_emails_all END, 0.5)")
     .alias("median_emails_leavers"))
q_emails_all_pd = q_emails_all.toPandas()
print("\nn_emails_all summary, stayers vs leavers side by side | 1 row | %d rows" % len(q_emails_all_pd))
print(q_emails_all_pd.to_string(index=False))

_emails_all_path = BASE + "out/q_emails_all_summary"
q_emails_all.coalesce(1).write.mode("overwrite").option("header", True).csv(_emails_all_path)
print("written to HDFS:", _emails_all_path, "|", len(q_emails_all_pd), "rows")
