# unsub_tracking/spotlight/unsub_unified.py
#
# Unified Unsub Brief pipeline. Replaces spotlight.py + spotlight2.py as the single source for
# the combined Power-Pack / Workstream-2 brief (unsub_tracking/UNIFIED_BRIEF.md, drafted
# 2026-08-02). One code file, three pieces, two deliberate time windows, per that brief's
# "Additional decisions" section ("One code file... No second file").
#
# SCOPE ANCHOR (brief, top section): Cards is the subject, enterprise-wide is the
# comparator, never the story. Population rule for EVERY pull: campaign email only —
# TREATMENT_ID carries a valid 10-char mnemonic shape. Non-campaign/default/unflagged mail is
# excluded everywhere (Andre 2026-08-02).
#
# ============================================================================================
# COVERAGE TABLE — brief ask -> cell -> output. A blank cell is the run-gate failure per the
# brief's "Pre-run coverage gate" — there are no blanks below.
# ============================================================================================
#
#  PIECE A (Window A = Jan-Apr 2026, in-window, NOT trailing-from-today. UCP snapshot for this
#  piece is UCP_MONTH_A, Cell [0] - HARDCODED to Window A close, not derived from run date.)
#  ---------------------------------------------------------------------------------------------
#  A1 unique enterprise unsub clients + per-mne share      -> Cell [6]  -> a1_mne_share.csv
#    NOTE: LOB (mne) rollups of A1's per-mne client counts double-count multi-list clients within
#    an LOB - the ENTERPRISE_TOTAL row is the deduped truth; any per-LOB ratio built from mne sums
#    is an UPPER BOUND, not exact. CARDS_TOTAL_UNIQUE_CLIENTS is the ONE exception: it reads
#    cards_unsub_flag directly off a1_client (not a sum of per-mne A2 rows), so Cards-vs-rest is
#    EXACT, same dedup guarantee as ENTERPRISE_TOTAL. Every OTHER per-LOB ratio remains an upper
#    bound.
#  A2 mne x {senders, unsubs_attributed, leavers_exposed}  -> Cell [7]  -> a2_mne_rates.csv
#  A3 in-window contact load, banded, x unsub x cards_unsub-> Cell [8]  -> a3_contact_cube.csv
#  A4 age x tenure x T/I/B/C(separate) x depth x stay/leave-> Cell [9]  -> a4_profile_cube.csv
#    x leavers_cards_unsub (cards-view subset, rides beside leavers)
#
#  PIECE B (anchor 2025-08-31 HARDCODED, remeasure +12m = 2026-08-31, Cards-mailed cohort only)
#  ---------------------------------------------------------------------------------------------
#  cohort + leaver flags (any-list, cards subset), scoped  -> Cell [12] -> b_cohort_v1/ (landed)
#  BEFORE pulling DFP/BHV, not post-hoc
#  spend (DFP trailing-12m) at t0/t12, cohort-scoped        -> Cell [13] -> b_dfp_v1/ (landed)
#  revolver/transactor at t0/t12, cohort-scoped              -> Cell [14] -> b_bhv_v1/ (landed)
#  spend_tier x spend_tier_at_offset x usg_bhvr_seg x        -> Cell [15] -> b_before_after_cube.csv
#  {stayers,leavers} x {t0,t12}  (spend_tier = held fixed at t0 terciles; spend_tier_at_offset =
#  the SAME t0 cutpoints applied to the offset-appropriate spend value, t0 or p12 - answers the
#  spend/tier trajectory question spend_tier alone cannot)
#
#  PIECE C (trailing 12 months, monthly, its own time axis — outside Windows A and B)
#  ---------------------------------------------------------------------------------------------
#  sends + unsubs_attributed by CALENDAR month of the event -> Cell [10] -> c_monthly_curve.csv
#  x mne, server-side aggregate, no client-grain landing
#
#  DELIVERY
#  ---------------------------------------------------------------------------------------------
#  one xlsx bundling all six CSVs above                     -> Cell [16] -> spotlight_unified.xlsx
#  coverage / row-count self-check                          -> Cell [17] -> printed only
#
# ============================================================================================
# TRAP LEDGER — which trap, guarded where (brief TRAPS section + AUDIT_2026-08-02.md)
# ============================================================================================
#
#  1. Mnemonic/shape filter everywhere        -> TACTIC_ID_SQL appended to every ek/cohort_ek
#                                                 CTE's WHERE (Cells 2,3,4,12,13,14).
#  2. Attribution vs exposure conflation       -> A2 (Cell 7) carries senders / unsubs_attributed /
#                                                 leavers_exposed as three NAMED columns, never a
#                                                 bare "unsubs" column.
#  3. Per-list unsub (~97% evidence)           -> Piece A's unsub_flag_any is ANY-list (all mnes,
#                                                 unfiltered by mne in the ek CTE) -> Cell [2]/[3].
#                                                 A1's enterprise total (Cell 6) is a CLIENT-GRAIN
#                                                 dedup, not a sum of per-mne counts (which would
#                                                 double-count multi-list unsubscribers).
#  4. Left truncation on any lookback crossing -> Piece C's trailing-12m floor (2025-08-01) sits
#     the Aug-2025 data floor                    exactly AT the data floor, not before it (12m
#                                                 ending ~Aug 2026 needs no data earlier than the
#                                                 floor). Piece B's cohort/leaver pull floors at
#                                                 2024-01-01 (repo hard floor) with its own
#                                                 MASTER_FLOOR margin (Cell [12]).
#  5. MASTER join not 1:1 (~11% inflation)     -> DISTINCT subquery in every MASTER join
#                                                 (Cells 2,3,4,12,13,14), copied verbatim from
#                                                 spotlight.py.
#  6. Bite predicate must sit inside the       -> MOD(ABS(CLNT_NO), N_BITES) lives inside the
#     MASTER subquery (spool 2646)                MASTER DISTINCT subquery in every pull, never
#                                                 in an outer WHERE. Same fix DFP/BHV inherit via
#                                                 the cohort CTE's own MASTER-scoped bite.
#  7. Event grain (client, treatment,          -> Every ek/cohort_ek CTE: GROUP BY 1,2,3(,4),
#     disposition, DAY) — evergreen id reuse      CAST(disposition_dt_tm AS DATE).
#  8. Non-dated ids = residue                  -> Every date anchored on disposition_dt_tm; the
#                                                 TREATMENT_ID is read only for its mnemonic.
#  9. Piece B anchor floating to "now"         -> T0_ANCHOR_B / P12_ANCHOR_B are literal dates
#     (spotlight2.py's defect #1)                 (2025-08-31 / 2026-08-31), never derived from
#                                                 datetime.date.today() anywhere in Cell [0].
#                                                 RUN_DATE (provenance stamp only) is the one place
#                                                 today() is used, and it never feeds an anchor.
#                                                 SAME FIX applied to Piece A's UCP snapshot
#                                                 (red-team BLOCKER, this review): UCP_MONTH_A
#                                                 (Cell [0]) is HARDCODED to Window A close,
#                                                 replacing Cell [5]'s prior date.today()-derived
#                                                 "last closed month" anchor.
# 10. Piece B leaver flag = whole-window unsub -> Cell [12]'s cohort/leaver pull has its OWN event
#     (spotlight2.py's defect #2,                 window (2024-01-01 -> anchor+1day), separate from
#     post-treatment contamination)               Piece A/C's windows. Nothing here reuses a flag
#                                                 computed over a different window.
# 11. Piece B pulls bank-wide, cards flag      -> DFP/BHV (Cells 13,14) INNER JOIN a cohort CTE
#     applied post-hoc (spotlight2.py defect #4)  (cards-mailed-before-anchor) BEFORE the DFP/BHV
#                                                 scan aggregates — cohort scoping happens inside
#                                                 the same Teradata statement, not after landing.
# 12. UCP float-id join trap                  -> clnt_no cast decimal(18,0)->long on BOTH sides
#                                                 before every join (Cell [5]); 5 sample ids printed
#                                                 from each side before joining.
# 13. NOT IN with a subquery (NULL trap)       -> not used anywhere in this file; cohort/leaver
#                                                 scoping is INNER JOIN / EXISTS-shaped, never NOT IN.
#
# ============================================================================================
# WHAT THIS FILE REUSES VERBATIM FROM spotlight.py / spotlight2.py (do not redesign these)
# ============================================================================================
# - The ek CTE dedup grain (client, treatment, disposition, DAY) — spotlight.py Cell [1] Pull A.
# - MASTER DISTINCT subquery with the bite predicate INSIDE it — same file, same cell.
# - MASTER_FLOOR margin logic (load_tm lags disposition_dt_tm; 3-month margin) — spotlight.py
#   Cell [0] comment on MASTER_FLOOR.
# - TACTIC_ID_SHAPE_ONLY / TACTIC_ID_SQL — spotlight.py Cell [0], verbatim.
# - CARDS_MNES (12, verbatim list) — spotlight.py Cell [0].
# - _landed / _write_chunks / _rowcount_marker_path / write_cube — spotlight.py Cell [1],
#   near-verbatim (paths point at this file's own BASE).
# - AGE_EDGES / TENURE_EDGES / _band() — spotlight.py Cell [0]/[4].
# - UCP join guard (decimal(18,0)->long cast both sides, 5-sample print, >=70% match floor,
#   dedup-before-left-join, row-count assert) — spotlight.py Cell [4].
# - DFP accumulator validation gate (net_prch_amt_mtd resets monthly, proven not trusted) —
#   spotlight2.py Cell [1], verbatim method, re-anchored to T0_ANCHOR_B.
# - DFP one-scan + ROW_NUMBER pivot pattern (never multi-scan DLY_FULL_PORTFOLIO) —
#   spotlight2.py Cell [3].
# - CR_CRD_RPTS_ACCT direct clnt_no (no DFP bridge needed) + SEG_PRECEDENCE / SEG_LABEL +
#   raw-value probe — spotlight2.py Cell [4].
# - Spend-tier tercile cut, held fixed at t0, zero-spend clients kept as zeros — spotlight2.py
#   Cell [7].
#
# ============================================================================================
# WHAT THIS FILE DOES NOT DO, AND WHY (stated up front, per house rule — never buried)
# ============================================================================================
# - NO UCP PULL FOR PIECE B. The brief's locked Piece B deliverable (Workstream-2 template:
#   spend tier x revolver/transactor, single before/after pair) carries no UCP field. The task
#   spec's general UCP hard-rule ("for Piece B the UCP snapshot month should be the anchor month
#   if available") is addressed here rather than silently ignored: IF a later ask wants
#   demographic context on Piece B's leavers, the anchor month 2025-08-31 is CONFIRMED inside the
#   live UCP partition range (2023-12-31 -> 2026-06-30, references/ucp/gotchas.md #1,
#   2026-07-27 probe) and Cell [5]'s UCP_MONTH pattern would apply unchanged with
#   MONTH_END_DATE=2025-08-31. Not built because not asked — adding it now would be an
#   unrequested column per house rule.
# - Piece B's t12 remeasure (2026-08-31) is a FUTURE month as of this file's build date
#   (2026-08-02). DFP/BHV bites for t12 will land thin-to-empty until that month closes — this is
#   arithmetic, not a bug, and is asserted/printed loudly in Cells [13]/[14]/[15], never silently
#   averaged away. Rerun this file after 2026-09-01 to pick up real t12 data.
# - REGULATORY_MNES tagging (spotlight.py carried this) is dropped — no ask in the unified brief
#   touches regulatory-campaign segmentation.
#
# ENGINE MAP: Cells [1]-[4], [10]-[14] are Teradata-direct (DTZV01.*, D3CV12A.*, no catalog
# prefix, teradatasql connector). Every other cell is PySpark (YARN, Lumina pre-initialized
# session) reading landed parquet off HDFS. Neither engine follows Trino/Starburst syntax rules
# (references/query_engine_guidelines.md) — that canon is for the federated engine, not used here.


# %% [0] CONFIG - every tunable lives here. No literal below this cell is hand-typed elsewhere.

import calendar
import datetime

SCRIPT_NAME = "unsub_unified.py"
RUN_DATE = datetime.date.today().isoformat()   # provenance stamp ONLY - never feeds an anchor.

# ---- Campaign id scope. Shape only - verbatim from spotlight.py Cell [0]. ----
TACTIC_ID_SHAPE_ONLY = True
TACTIC_ID_SQL = """
          AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
          AND SUBSTR(TRIM(TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'""" if TACTIC_ID_SHAPE_ONLY else ""

# ---- CARDS_MNES - verbatim from spotlight.py Cell [0] / spotlight2.py Cell [0]. ----
CARDS_MNES = frozenset({"PCQ", "PCL", "PCD", "AUH", "CLI", "CRV",
                        "VBA", "VBU", "CRO", "CEC", "VIF", "MET"})
assert len(CARDS_MNES) == 12, "CARDS_MNES should hold exactly 12 MNEs - recount before running"
CARDS_LIST_SQL = ", ".join("'%s'" % m for m in sorted(CARDS_MNES))


def _months_before(date_str, n):
    """Calendar-month subtraction from a FIXED date string, stdlib only. Used to derive
    MASTER_FLOOR margins from fixed window floors - never from today()."""
    d = datetime.date.fromisoformat(date_str)
    total = d.year * 12 + (d.month - 1) - n
    y, m = divmod(total, 12)
    m += 1
    day = min(d.day, calendar.monthrange(y, m)[1])
    return datetime.date(y, m, day).isoformat()


def _add_months(d, n):
    total = d.year * 12 + (d.month - 1) + n
    y, m = divmod(total, 12)
    m += 1
    return datetime.date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def _ym(d):
    return d.year * 100 + d.month


# ---- WINDOW A - Piece A. Locked 2026-08-02 (Andre verbal): Jan-Apr 2026, in-window. ----
WIN_A_FLOOR = "2026-01-01"
WIN_A_CEIL = "2026-05-01"   # half-open ceiling - excludes May.
MASTER_FLOOR_A = _months_before(WIN_A_FLOOR, 3)   # load_tm lags disposition_dt_tm; 3mo margin.

# ---- UCP SNAPSHOT - Piece A (Cell [5]). Red-team BLOCKER fix: this was date.today()-derived
# (drifted with run date) - HARDCODED now, same fix pattern as WINDOW B's anchors below. ----
UCP_MONTH_A = "2026-04-30"  # Window A close. HARDCODED - runs must not drift with run date. Confirmed inside live partition range (2023-12-31..2026-06-30, references/ucp/gotchas.md #1).

# ---- WINDOW C - Piece C. Trailing 12 months, its own axis, deliberately outside A and B.
# HARD ceiling (not "floor to today"), same reasoning as spotlight.py's WIN_CEIL: a run that
# resumes across sessions must not have its pulls drift apart in time. Move forward by hand each
# sprint. As of this build (2026-08-02) this floor lands exactly ON the Aug-2025 data floor - a
#12m-trailing window ending ~Aug 2026 needs nothing earlier than that, so trap #4 does not fire.
WIN_C_CEIL = "2026-08-01"
WIN_C_FLOOR = _months_before(WIN_C_CEIL, 12)   # "2025-08-01"
MASTER_FLOOR_C = _months_before(WIN_C_FLOOR, 3)   # "2025-05-01" - matches spotlight.py's literal.

# ---- WINDOW B - Piece B. Anchor HARDCODED - the fix for spotlight2.py's defect #1 (anchor
# floated to "now"). Never derive these two dates from datetime.date.today(). ----
T0_ANCHOR_B = datetime.date(2025, 8, 31)
P12_ANCHOR_B = datetime.date(2026, 8, 31)
T_OFFSETS_B = [0, 12]
T_ANCHOR_B = {0: T0_ANCHOR_B, 12: P12_ANCHOR_B}
T_TAG_B = {0: "t0", 12: "p12"}
T_YM_B = {o: _ym(T_ANCHOR_B[o]) for o in T_OFFSETS_B}
ANCHOR_DATES_SQL_B = ", ".join("DATE '%s'" % T_ANCHOR_B[o].isoformat() for o in T_OFFSETS_B)

# P12_CLOSE_DATE_B - first day of the month AFTER P12_ANCHOR_B's month. NOT a window anchor (does
# not feed any SQL floor/ceiling) - it is a RESUME-REGIME gate only: B_DFP/B_BHV bites pulled
# before this date carry a legitimately future-dated/thin t12 (annual_spend_p12 etc.), so their
# landed marker must not block a real re-pull once this date passes. Computed from the anchor, not
# hand-typed - round-2 review blocker fix (Sept-rerun no-op).
P12_CLOSE_DATE_B = _add_months(P12_ANCHOR_B.replace(day=1), 1)   # 2026-09-01

# Cohort membership window: "clients mailed by CARDS_MNES before the anchor" - own floor, the
# repo hard floor (2024_data_floor memory), not derived from WIN_A/WIN_C.
COHORT_B_FLOOR = "2024-01-01"
assert datetime.date.fromisoformat(COHORT_B_FLOOR) >= datetime.date(2024, 1, 1), \
    "repo floor is 2024-01-01 - do not go below"
ANCHOR_B_CEIL = (T0_ANCHOR_B + datetime.timedelta(days=1)).isoformat()   # "on or before anchor"
MASTER_FLOOR_B = _months_before(COHORT_B_FLOOR, 3)

# Annual-spend lookback windows (12 calendar months ending at each Piece-B offset), computed, not
# hand-typed - one mistake in a hand-typed 24-month IN-list is invisible until the totals are off.
ANNUAL_LOOKBACK_MONTHS = 12
ANNUAL_YMS_B = {
    o: [_ym(_add_months(T_ANCHOR_B[o].replace(day=1), k))
        for k in range(-(ANNUAL_LOOKBACK_MONTHS - 1), 1)]
    for o in T_OFFSETS_B
}
SPEND_YMS_B = sorted(set(sum(ANNUAL_YMS_B.values(), [])) | set(T_YM_B.values()))
SPEND_YMS_B_SQL = ", ".join(str(y) for y in SPEND_YMS_B)
_spend_floor_date = min(_add_months(T_ANCHOR_B[o].replace(day=1), -(ANNUAL_LOOKBACK_MONTHS - 1))
                        for o in T_OFFSETS_B)
if _spend_floor_date < datetime.date(2024, 1, 1):
    print("NOTE: computed DFP scan floor", _spend_floor_date, "is below the repo 2024-01-01 "
          "floor - clamping. This shortens the t0 annual-spend lookback.")
    _spend_floor_date = datetime.date(2024, 1, 1)
    SPEND_YMS_B = [y for y in SPEND_YMS_B if y >= 202401]
    SPEND_YMS_B_SQL = ", ".join(str(y) for y in SPEND_YMS_B)
SPEND_FLOOR_B = _spend_floor_date.isoformat()
SPEND_CEIL_B = (max(T_ANCHOR_B.values()) + datetime.timedelta(days=1)).isoformat()   # exclusive

# ---- Behaviour segment precedence - verbatim from spotlight2.py Cell [0] (UNVERIFIED U1/U3
# there; unresolved here too - carried forward, not re-litigated). ----
SEG_PRECEDENCE = [("Revolver", 1), ("Transactor", 2), ("Dormant", 3)]
SEG_LABEL = {1: "Revolver", 2: "Transactor", 3: "Dormant", 4: "other_or_none", 0: "no_data"}

# ---- Spend tiers - verbatim method from spotlight2.py Cell [7]: tercile of annual spend, cut
# ONCE at t0, held fixed across every offset. ----
SPEND_TIER_QUANTILES = [1.0 / 3.0, 2.0 / 3.0]
SPEND_TIER_REL_ERR = 0.01

# ---- Band edges - AGE/TENURE verbatim from spotlight.py Cell [0]. EMAILS edges are a NEW
# first-cut for a ~4-month window (Piece A has no house standard either - references/ucp/
# gotchas.md #7 applies the same way it did to spotlight.py's edges). Tune after SMOKE output. --
AGE_EDGES = [(None, 25, "<25"), (25, 34, "25-34"), (35, 49, "35-49"), (50, 64, "50-64"), (65, None, "65+")]
TENURE_EDGES = [(None, 1, "<1yr"), (1, 3, "1-3yr"), (4, 7, "4-7yr"), (8, 15, "8-15yr"), (16, None, "16yr+")]
WIN_EMAILS_ALL_EDGES = [(0, 0, "0"), (1, 4, "1-4"), (5, 10, "5-10"), (11, 25, "11-25"), (26, None, "26+")]
WIN_EMAILS_CARDS_EDGES = [(0, 0, "0"), (1, 2, "1-2"), (3, 5, "3-5"), (6, 10, "6-10"), (11, None, "11+")]

# ---- RUN SWITCHES - the only things to touch before hitting Run All ----
SMOKE = True   # True -> bite 0 only (~10% of clients). Flip False for the full population AFTER
               # checking bite-0 shapes against the coverage table (brief's pre-run gate).
N_BITES = 10   # MOD(ABS(CLNT_NO), N_BITES) - every Teradata-direct pull in this file.
LAND_CHUNK_ROWS = 1_500_000   # rows per createDataFrame call - stays under Spark's 128MB RPC cap.
RUN_PULLS = ["A1", "A2", "C", "B_COHORT", "B_DFP", "B_BHV"]   # restrict which pulls Cells run.
UCP_MATCH_FLOOR = 70.0   # catches a broken join key, not ordinary UCP attrition.

# ---- Paths - HDFS only. Own namespace, does not collide with spotlight.py/spotlight2.py. ----
BASE = "hdfs:///user/427966379/unsub_unified/"      # reference_andre_hdfs_user_path.md
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"           # references/ucp/README.md - personal only
SCHEMA_VERSION = 1

A1_DIR = BASE + "a1_client_v%d/" % SCHEMA_VERSION
A2_DIR = BASE + "a2_mne_v%d/" % SCHEMA_VERSION
C_DIR = BASE + "c_month_mne_v%d/" % SCHEMA_VERSION
BCOHORT_DIR = BASE + "b_cohort_v%d/" % SCHEMA_VERSION
BDFP_DIR = BASE + "b_dfp_v%d/" % SCHEMA_VERSION
BBHV_DIR = BASE + "b_bhv_v%d/" % SCHEMA_VERSION
UCPA_DIR = BASE + "ucp_enriched_a2_v%d/" % SCHEMA_VERSION  # bitten UCP-join output, Cell [5].
# (a2, 2026-08-03: name bumped from ucp_enriched_a - the v1 dirs mixed landings from two code
# vintages and carried a1 RAW accounting (10,439,806 incl 10 NULL-id rows) vs distinct base
# (10,439,797); re-landing every bite under one code version, with the explicit NULL policy
# below, removes all benign routes for that mismatch. Old v1 dirs are dead - ignore them.)

OUT_DIR = BASE + ("out_smoke/" if SMOKE else "out/")
PQ_DIR = OUT_DIR.rstrip("/") + "_parquet/"


def write_cube(df, name):
    """Every cube lands twice: CSV for Excel, parquet for duckdb. Verbatim from spotlight.py."""
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(OUT_DIR + name)
    df.coalesce(1).write.mode("overwrite").parquet(PQ_DIR + name)
    print("   wrote", OUT_DIR + name, "(csv) and", PQ_DIR + name, "(parquet)")


def _stamp(df, window_label, population_label):
    """Provenance columns on every output - script/run date/window/population, per house rule
    (no bare numbers without a citation)."""
    return (df.withColumn("script", F.lit(SCRIPT_NAME))
              .withColumn("run_date", F.lit(RUN_DATE))
              .withColumn("window_label", F.lit(window_label))
              .withColumn("population_label", F.lit(population_label))
              .withColumn("smoke_run", F.lit(1 if SMOKE else 0)))


# BUILD STAMP - bump the tag on EVERY code change that gets pushed. This prints first so any
# screenshot of any run is instantly attributable to the exact code version that produced it
# (2026-08-03: three debugging rounds were spent on outputs from older code than assumed).
PIPELINE_BUILD = "build 2026-08-03c | SMOKE-aware A4 guard; empty-B-read guards; zip path quoting - full static sweep applied"
print("=" * 88)
print("PIPELINE_BUILD:", PIPELINE_BUILD)
print("=" * 88)
print("CONFIG loaded | WIN_A:", WIN_A_FLOOR, "->", WIN_A_CEIL,
      "| WIN_C:", WIN_C_FLOOR, "->", WIN_C_CEIL,
      "| ANCHOR_B:", T0_ANCHOR_B.isoformat(), "-> +12m ->", P12_ANCHOR_B.isoformat())
print("SMOKE:", SMOKE, "| N_BITES:", N_BITES, "| CARDS_MNES:", len(CARDS_MNES), "| OUT_DIR:", OUT_DIR)


# %% [1] TERADATA CONNECTION + shared landing helpers - verbatim pattern from spotlight.py Cell [1].

import time
import getpass
import teradatasql
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, IntegerType

if not hasattr(pd.DataFrame, "iteritems"):
    pd.DataFrame.iteritems = pd.DataFrame.items

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.sparkContext.setLogLevel("ERROR")   # silence WARN chatter so OUR printed checks stay readable
# The atlas lineage harvester (the "Missing unknown leaf node: LogicalRDD/ReusedExchange..." red
# blocks) logs through its own logger and ignores setLogLevel - switch it OFF by name. This kills
# ONLY system log noise; every check/assert/WARN printed by THIS FILE is stdout and untouched.
try:
    _l4j = spark.sparkContext._jvm.org.apache.log4j
    _l4j.LogManager.getRootLogger().setLevel(_l4j.Level.ERROR)
    for _noisy in ("com.hortonworks.spark.atlas", "com.hortonworks", "org.apache.spark.scheduler",
                   "org.apache.spark.storage", "org.apache.spark.executor"):
        _l4j.LogManager.getLogger(_noisy).setLevel(_l4j.Level.OFF)
    print("log noise: root=ERROR, atlas lineage harvester=OFF - file's own checks unaffected.")
except Exception as _e:
    print("log-noise suppression skipped (%s) - cosmetic only, run is unaffected." % type(_e).__name__)

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")
EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com", user=username, password=password,
                          logmech="LDAP")

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


def _rowcount_marker_path(path):
    return path.rstrip("/") + "_ROWCOUNT"
    # NOTE: marker sits INSIDE the parent dir as a sibling of bite_K, so any data read
    # MUST use the single-char glob "bite_?" (matches bite_0..bite_9 ONLY, N_BITES=10),
    # NEVER "bite_*" - that wildcard also matches bite_K_ROWCOUNT/_REGIME sidecars and
    # silently adds one NULL-keyed row per bite to every union read. This exact bug
    # burned 2026-08-02/03 as phantom "9 duplicates" / NULL rows / assert mismatches.


def _landed(path):
    """A directory existing is NOT proof a bite fully landed. Verbatim from spotlight.py."""
    try:
        _n_actual = spark.read.parquet(BASE + path).count()
    except Exception as e:
        msg = str(e).lower()
        if any(s in msg for s in ("path does not exist", "path_not_found", "filenotfound",
                                  "unable to infer schema")):
            return False
        raise RuntimeError(path + ": cannot verify HDFS state, refusing to guess. " + str(e)[:300])
    try:
        _n_expected = spark.read.parquet(BASE + _rowcount_marker_path(path)).collect()[0]["expected_rows"]
    except Exception:
        print(path, ": parquet exists but no _ROWCOUNT marker - treating as NOT landed.")
        return False
    if _n_actual != _n_expected:
        print("WARNING:", path, "- marker expects", _n_expected, "actual", _n_actual,
              "- partial write. Re-pulling this bite.")
        return False
    return True


def _regime_flag_path(path):
    return path.rstrip("/") + "_REGIME"


_REGIME_SCHEMA = StructType([StructField("regime", StringType(), False)])


def _write_regime_flag(hdfs_path, regime):
    """Stamps whether a B_DFP/B_BHV bite was pulled pre- or post- P12_CLOSE_DATE_B. Consulted by
    _landed_b_offset() (round-2 review blocker fix) so a marker written PRE-close - when t12 is
    legitimately future-dated/thin - doesn't silently block a real re-pull once the P12 month has
    actually closed. hdfs_path is relative (no BASE prefix), matching _landed()'s convention."""
    _sdf = spark.createDataFrame([(regime,)], schema=_REGIME_SCHEMA)
    _sdf.coalesce(1).write.mode("overwrite").parquet(BASE + _regime_flag_path(hdfs_path))


def _current_regime():
    """post_close once P12_CLOSE_DATE_B has passed (t12 data can be real), pre_close before it
    (t12 is arithmetic thinness by construction). This IS a date.today() read, deliberately - it
    gates RESUME behaviour only, never a SQL window anchor (those stay hardcoded per Cell [0])."""
    return "post_close" if datetime.date.today() >= P12_CLOSE_DATE_B else "pre_close"


def _landed_b_offset(path):
    """_landed() variant for B_DFP/B_BHV bites ONLY (Cells [13]/[14]) - these carry a t12/p12
    value that is legitimately future-dated/thin before P12_CLOSE_DATE_B. A marker written
    pre-close does NOT mean the bite is done for a post-close rerun: t12 needs a real pull once
    the month has closed, so this forces a re-pull in exactly that transition. A pre-close rerun
    (nothing has changed yet) still skips normally, same as every other bite in this file.
    Round-2 review BLOCKER fix - without this, a September rerun silently keeps Sept t12 zeros."""
    if not _landed(path):
        return False
    _now_regime = _current_regime()
    try:
        _stored_regime = spark.read.parquet(BASE + _regime_flag_path(path)).collect()[0]["regime"]
    except Exception:
        print(path, ": landed but no regime flag found (pre-retrofit landing, or the flag write "
              "failed) - treating the stored regime as UNKNOWN and forcing a re-pull to be safe "
              "(current run regime:", _now_regime + ").")
        return False
    if _stored_regime == "pre_close" and _now_regime == "post_close":
        print(path, ": landed PRE-CLOSE (before", P12_CLOSE_DATE_B.isoformat(), ") but this run "
              "is POST-CLOSE - t12 was future-dated/thin when this bite was pulled. Forcing "
              "re-pull for a real t12 read.")
        return False
    print(path, ": already landed (stored regime:", _stored_regime, "| current run regime:",
          _now_regime, ") -", spark.read.parquet(BASE + path).count(), "rows - SKIP")
    return True


def _write_chunks(pdf, schema, hdfs_path):
    """Shared chunked-write helper. Verbatim from spotlight.py."""
    _first = True
    for _s in range(0, len(pdf), LAND_CHUNK_ROWS):
        _part = pdf.iloc[_s:_s + LAND_CHUNK_ROWS]
        _sdf = spark.createDataFrame(_part, schema=schema)
        _sdf.write.mode("overwrite" if _first else "append").parquet(BASE + hdfs_path)
        _first = False
        print("   ...", hdfs_path, "chunk", _s, "-", _s + len(_part), "written", flush=True)
    nback = spark.read.parquet(BASE + hdfs_path).count()
    _marker_schema = StructType([StructField("expected_rows", LongType(), False)])
    _marker_sdf = spark.createDataFrame([(int(nback),)], schema=_marker_schema)
    _marker_sdf.coalesce(1).write.mode("overwrite").parquet(BASE + _rowcount_marker_path(hdfs_path))
    return nback


def _write_spark_marker(hdfs_path, n):
    """Same _ROWCOUNT-marker pattern as _write_chunks' tail, for cells that write a Spark
    DataFrame directly (join output) instead of a pandas frame from Teradata. hdfs_path is
    relative (no BASE prefix), matching _landed()'s convention."""
    _marker_schema = StructType([StructField("expected_rows", LongType(), False)])
    _marker_sdf = spark.createDataFrame([(int(n),)], schema=_marker_schema)
    _marker_sdf.coalesce(1).write.mode("overwrite").parquet(BASE + _rowcount_marker_path(hdfs_path))


def _read_bite_or_empty(dir_with_base, bite, schema):
    """Read one landed bite subdir (dir_with_base already carries the BASE prefix, e.g.
    BCOHORT_DIR). A Teradata pull can legitimately land zero rows for a bite (B_COHORT/B_DFP/
    B_BHV all skip writing when a bite pulls empty) - return an empty typed frame instead of
    erroring, same exception-handling as _landed()."""
    _path = dir_with_base + "bite_%d" % bite
    try:
        return spark.read.parquet(_path)
    except Exception as e:
        msg = str(e).lower()
        if any(s in msg for s in ("path does not exist", "path_not_found", "filenotfound",
                                  "unable to infer schema")):
            print(_path, ": not landed (this bite pulled zero rows upstream) - using an empty frame.")
            return spark.createDataFrame([], schema=schema)
        raise RuntimeError(_path + ": cannot verify HDFS state, refusing to guess. " + str(e)[:300])


def _band(col, edges):
    """Verbatim from spotlight.py Cell [4]."""
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


print("Cell [1] done - EDW connection live, landing helpers ready.")


# %% [2] PULL A1 - client-grain, WINDOW A (Jan-Apr 2026), ALL mnes (unfiltered by mne - Piece A's
# unsub_flag_any must be enterprise-wide, per trap #3). Bitten, landed, resumable.
# ENGINE: Teradata-direct (DTZV01.*, no catalog prefix, teradatasql).
# Serves: A1 enterprise dedup (Cell 6), A3 contact load (Cell 8), A4 profile (Cell 9).
# NOTE: LOB (mne) rollups of these per-mne client counts double-count multi-list clients within an
# LOB - the enterprise total (Cell [6]) is the deduped truth; any per-LOB ratio built from mne sums
# is an UPPER BOUND, not exact. cards_unsub_flag landed here (client grain) is the exception that
# lets Cell [6] add an EXACT CARDS_TOTAL_UNIQUE_CLIENTS row alongside ENTERPRISE_TOTAL.

A1_SCHEMA = StructType([
    StructField("clnt_no", LongType(), True),
    StructField("unsub_flag_any", IntegerType(), True),
    StructField("cards_unsub_flag", IntegerType(), True),
    StructField("n_emails_all", LongType(), True),
    StructField("n_emails_cards", LongType(), True),
])


def _prep_a1(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    _n_null = pd.to_numeric(pdf["clnt_no"], errors="coerce").isna().sum()
    assert _n_null == 0, "clnt_no has %d nulls - CLNT_NO IS NOT NULL filter is not firing" % _n_null
    pdf["clnt_no"] = pd.to_numeric(pdf["clnt_no"], errors="coerce").astype("int64")
    for _c in ["unsub_flag_any", "cards_unsub_flag"]:
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce").fillna(0).astype("int32")
    for _c in ["n_emails_all", "n_emails_cards"]:
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce").fillna(0).astype("int64")
    return pdf[[f.name for f in A1_SCHEMA.fields]]


def land_a1_bite(bite):
    name = "a1_client_v%d/bite_%d" % (SCHEMA_VERSION, bite)
    if _landed(name):
        print(name, ": already landed,", spark.read.parquet(BASE + name).count(), "rows - SKIP")
        return
    sql = """
    WITH ek AS (
        SELECT consumer_id_hashed, TREATMENT_ID, disposition_cd,
               MIN(disposition_dt_tm) AS disposition_dt_tm
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd IN (1, 4)
          AND disposition_dt_tm >= DATE '%(floor)s'
          AND disposition_dt_tm <  DATE '%(ceil)s'%(tactic)s
        GROUP BY 1, 2, 3, CAST(disposition_dt_tm AS DATE)
    ),
    joined AS (
        SELECT m.CLNT_NO AS clnt_no,
               SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne,
               ek.disposition_cd AS disposition_cd
        FROM ek
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE load_tm >= DATE '%(mfloor)s'
                      AND CLNT_NO IS NOT NULL
                      AND MOD(ABS(CLNT_NO), %(n_bites)d) = %(bite)d) m
          ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
    )
    SELECT clnt_no,
           MAX(CASE WHEN disposition_cd = 4 THEN 1 ELSE 0 END) AS unsub_flag_any,
           MAX(CASE WHEN disposition_cd = 4 AND mne IN (%(cards)s) THEN 1 ELSE 0 END) AS cards_unsub_flag,
           SUM(CASE WHEN disposition_cd = 1 THEN 1 ELSE 0 END) AS n_emails_all,
           SUM(CASE WHEN disposition_cd = 1 AND mne IN (%(cards)s) THEN 1 ELSE 0 END) AS n_emails_cards
    FROM joined
    GROUP BY clnt_no
    """ % {"floor": WIN_A_FLOOR, "ceil": WIN_A_CEIL, "mfloor": MASTER_FLOOR_A, "tactic": TACTIC_ID_SQL,
           "n_bites": N_BITES, "bite": bite, "cards": CARDS_LIST_SQL}
    pdf = edw_pd(sql)
    assert len(pdf) > 0, name + " pulled zero rows for bite " + str(bite) + " - investigate"
    pdf = _prep_a1(pdf)
    nback = _write_chunks(pdf, A1_SCHEMA, name)
    assert nback == len(pdf), name + " HDFS readback mismatch: pulled %d, read back %d" % (len(pdf), nback)
    print(name, ": landed", len(pdf), "rows (client grain, WIN_A, enterprise-wide), readback", nback)


if "A1" in RUN_PULLS:
    for _b in (range(1) if SMOKE else range(N_BITES)):
        land_a1_bite(_b)
    print("PULL A1 done - landed at", A1_DIR + "*")

    # ---- MASTER-margin diagnostic (print-only, round-2 review ask). One cheap event-key-grain
    # aggregate (not client-grain, not bitten - same cost profile as Piece C's fan-out guard
    # further down): how many in-window cd=4 (unsub) events have NO match in the MASTER DISTINCT
    # slice this file actually joins against (load_tm >= MASTER_FLOOR_A)? Those events never reach
    # a1_client at all - they are unsubs silently lost to the load_tm margin, not a join bug. ----
    _master_margin_sql = """
    WITH ek4 AS (
        SELECT DISTINCT consumer_id_hashed, TREATMENT_ID
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd = 4
          AND disposition_dt_tm >= DATE '%(floor)s'
          AND disposition_dt_tm <  DATE '%(ceil)s'%(tactic)s
    ),
    master_keys AS (
        SELECT DISTINCT consumer_id_hashed, TREATMENT_ID
        FROM DTZV01.VENDOR_FEEDBACK_MASTER
        WHERE load_tm >= DATE '%(mfloor)s'
          AND CLNT_NO IS NOT NULL
    )
    SELECT COUNT(*) AS n_cd4_events,
           SUM(CASE WHEN mk.consumer_id_hashed IS NULL THEN 1 ELSE 0 END) AS n_unbridged
    FROM ek4
    LEFT JOIN master_keys mk
      ON mk.consumer_id_hashed = ek4.consumer_id_hashed AND mk.TREATMENT_ID = ek4.TREATMENT_ID
    """ % {"floor": WIN_A_FLOOR, "ceil": WIN_A_CEIL, "mfloor": MASTER_FLOOR_A, "tactic": TACTIC_ID_SQL}
    _master_margin_pdf = edw_pd(_master_margin_sql)
    _n_cd4_events = int(_master_margin_pdf.iloc[0]["n_cd4_events"]) if len(_master_margin_pdf) else 0
    _n_unbridged = int(_master_margin_pdf.iloc[0]["n_unbridged"] or 0) if len(_master_margin_pdf) else 0
    _unbridged_share = 100.0 * _n_unbridged / _n_cd4_events if _n_cd4_events else 0.0
    print("MASTER-MARGIN DIAGNOSTIC (WIN_A, print-only) | in-window cd=4 (unsub) events:", _n_cd4_events,
          "| unbridged (no MASTER match, load_tm >=", MASTER_FLOOR_A, "):", _n_unbridged,
          "| share: %.2f%%" % _unbridged_share, "- unbridged unsubs lost to the load_tm margin; "
          "if >2-3% consider widening MASTER_FLOOR_A.")
else:
    print("PULL A1 skipped - not in RUN_PULLS")


def read_a1():
    sdf = spark.read.parquet(A1_DIR + "bite_?")
    missing = [c.name for c in A1_SCHEMA.fields if c.name not in sdf.columns]
    if missing:
        raise RuntimeError("a1_client missing %s. Rerun Cell [2]." % missing)
    return sdf.withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))


# %% [3] PULL A2 - mne-grain, WINDOW A. senders / unsubs_attributed / leavers_exposed, three
# NAMED columns (trap #2 - never a bare "unsubs" column). leavers_exposed needs the ENTERPRISE
# any-list unsub flag joined onto each (client, mne) send row - computed once (client_any_unsub)
# and left-joined, never re-derived per mne. Bitten; per-bite output is mne-grain (tiny), summed
# across bites in Cell [7] exactly like spotlight.py's q_mne (bites partition clients disjointly,
# so summing COUNT-DISTINCT-per-bite is exact, not an overcount).
# ENGINE: Teradata-direct.

A2_SCHEMA = StructType([
    StructField("mne", StringType(), True),
    StructField("senders", LongType(), True),
    StructField("unsubs_attributed", LongType(), True),
    StructField("leavers_exposed", LongType(), True),
])


def _prep_a2(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    pdf["mne"] = pdf["mne"].astype(str)
    for _c in ["senders", "unsubs_attributed", "leavers_exposed"]:
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce").fillna(0).astype("int64")
    return pdf[[f.name for f in A2_SCHEMA.fields]]


def land_a2_bite(bite):
    name = "a2_mne_v%d/bite_%d" % (SCHEMA_VERSION, bite)
    if _landed(name):
        print(name, ": already landed,", spark.read.parquet(BASE + name).count(), "rows - SKIP")
        return
    sql = """
    WITH ek AS (
        SELECT consumer_id_hashed, TREATMENT_ID, disposition_cd,
               MIN(disposition_dt_tm) AS disposition_dt_tm
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd IN (1, 4)
          AND disposition_dt_tm >= DATE '%(floor)s'
          AND disposition_dt_tm <  DATE '%(ceil)s'%(tactic)s
        GROUP BY 1, 2, 3, CAST(disposition_dt_tm AS DATE)
    ),
    joined AS (
        SELECT m.CLNT_NO AS clnt_no,
               SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne,
               ek.disposition_cd AS disposition_cd
        FROM ek
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE load_tm >= DATE '%(mfloor)s'
                      AND CLNT_NO IS NOT NULL
                      AND MOD(ABS(CLNT_NO), %(n_bites)d) = %(bite)d) m
          ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
    ),
    client_any_unsub AS (
        SELECT clnt_no, MAX(CASE WHEN disposition_cd = 4 THEN 1 ELSE 0 END) AS any_unsub
        FROM joined
        GROUP BY clnt_no
    )
    SELECT j.mne,
           COUNT(DISTINCT CASE WHEN j.disposition_cd = 1 THEN j.clnt_no END) AS senders,
           COUNT(DISTINCT CASE WHEN j.disposition_cd = 4 THEN j.clnt_no END) AS unsubs_attributed,
           COUNT(DISTINCT CASE WHEN j.disposition_cd = 1 AND a.any_unsub = 1
                                THEN j.clnt_no END) AS leavers_exposed
    FROM joined j
    LEFT JOIN client_any_unsub a ON a.clnt_no = j.clnt_no
    GROUP BY j.mne
    """ % {"floor": WIN_A_FLOOR, "ceil": WIN_A_CEIL, "mfloor": MASTER_FLOOR_A, "tactic": TACTIC_ID_SQL,
           "n_bites": N_BITES, "bite": bite}
    pdf = edw_pd(sql)
    assert len(pdf) > 0, name + " pulled zero rows for bite " + str(bite) + " - investigate"
    pdf = _prep_a2(pdf)
    nback = _write_chunks(pdf, A2_SCHEMA, name)
    assert nback == len(pdf), name + " HDFS readback mismatch: pulled %d, read back %d" % (len(pdf), nback)
    print(name, ": landed", len(pdf), "rows (mne grain, WIN_A, PARTIAL - summed across bites "
          "in Cell [7]), readback", nback)


if "A2" in RUN_PULLS:
    for _b in (range(1) if SMOKE else range(N_BITES)):
        land_a2_bite(_b)
    print("PULL A2 done - landed at", A2_DIR + "*")
else:
    print("PULL A2 skipped - not in RUN_PULLS")


def read_a2_raw():
    sdf = spark.read.parquet(A2_DIR + "bite_?")
    missing = [c.name for c in A2_SCHEMA.fields if c.name not in sdf.columns]
    if missing:
        raise RuntimeError("a2_mne missing %s. Rerun Cell [3]." % missing)
    return sdf


# %% [4] PULL C - mne x CALENDAR MONTH of the event (not entry-cohort - the fix for q_trend's
# defect), trailing 12 months. Sends (cd=1) and unsubs_attributed (cd=4) counted in their own
# month. Server-side aggregate; per-bite output is mne x month (tiny) - bitten for spool safety
# on the MASTER join only, NOT because the output needs client grain. Summed across bites in
# Cell [10], same disjoint-bite-sum logic as Cell [7].
# ENGINE: Teradata-direct.

C_SCHEMA = StructType([
    StructField("mne", StringType(), True),
    StructField("ym", IntegerType(), True),
    StructField("sends", LongType(), True),
    StructField("unsubs_attributed", LongType(), True),
])


def _prep_c(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    pdf["mne"] = pdf["mne"].astype(str)
    pdf["ym"] = pd.to_numeric(pdf["ym"], errors="coerce").fillna(0).astype("int32")
    for _c in ["sends", "unsubs_attributed"]:
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce").fillna(0).astype("int64")
    return pdf[[f.name for f in C_SCHEMA.fields]]


def land_c_bite(bite):
    name = "c_month_mne_v%d/bite_%d" % (SCHEMA_VERSION, bite)
    if _landed(name):
        print(name, ": already landed,", spark.read.parquet(BASE + name).count(), "rows - SKIP")
        return
    sql = """
    WITH ek AS (
        SELECT consumer_id_hashed, TREATMENT_ID, disposition_cd,
               MIN(disposition_dt_tm) AS dt
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd IN (1, 4)
          AND disposition_dt_tm >= DATE '%(floor)s'
          AND disposition_dt_tm <  DATE '%(ceil)s'%(tactic)s
        GROUP BY 1, 2, 3, CAST(disposition_dt_tm AS DATE)
    ),
    joined AS (
        SELECT m.CLNT_NO AS clnt_no,
               SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne,
               ek.disposition_cd AS disposition_cd,
               (EXTRACT(YEAR FROM ek.dt) * 100 + EXTRACT(MONTH FROM ek.dt)) AS ym
        FROM ek
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE load_tm >= DATE '%(mfloor)s'
                      AND CLNT_NO IS NOT NULL
                      AND MOD(ABS(CLNT_NO), %(n_bites)d) = %(bite)d) m
          ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
    )
    SELECT mne, ym,
           SUM(CASE WHEN disposition_cd = 1 THEN 1 ELSE 0 END) AS sends,
           SUM(CASE WHEN disposition_cd = 4 THEN 1 ELSE 0 END) AS unsubs_attributed
    FROM joined
    GROUP BY mne, ym
    """ % {"floor": WIN_C_FLOOR, "ceil": WIN_C_CEIL, "mfloor": MASTER_FLOOR_C, "tactic": TACTIC_ID_SQL,
           "n_bites": N_BITES, "bite": bite}
    pdf = edw_pd(sql)
    assert len(pdf) > 0, name + " pulled zero rows for bite " + str(bite) + " - investigate"
    pdf = _prep_c(pdf)
    nback = _write_chunks(pdf, C_SCHEMA, name)
    assert nback == len(pdf), name + " HDFS readback mismatch: pulled %d, read back %d" % (len(pdf), nback)
    print(name, ": landed", len(pdf), "rows (mne x ym, PARTIAL - summed across bites), readback", nback)


if "C" in RUN_PULLS:
    # ---- Fan-out guard, print-only. Cheap diagnostic on the same connection: how many
    # (consumer_id_hashed, TREATMENT_ID) pairs in MASTER's DISTINCT scope map to >1 distinct
    # CLNT_NO. c_monthly_curve is a server-side COUNT(DISTINCT ...)-free SUM aggregate, so a
    # fanned-out pair inflates its sends/unsubs_attributed counts; A1/A2 are client-grain and
    # unaffected. Not fatal - informational only, same connection as the pull below.
    _c_fanout_sql = """
    SELECT COUNT(*) AS n_fanout_pairs
    FROM (
        SELECT consumer_id_hashed, TREATMENT_ID, COUNT(DISTINCT CLNT_NO) AS n_clnt
        FROM DTZV01.VENDOR_FEEDBACK_MASTER
        WHERE load_tm >= DATE '%(mfloor)s'
          AND CLNT_NO IS NOT NULL
        GROUP BY consumer_id_hashed, TREATMENT_ID
        HAVING COUNT(DISTINCT CLNT_NO) > 1
    ) x
    """ % {"mfloor": MASTER_FLOOR_C}
    _c_fanout_pdf = edw_pd(_c_fanout_sql)
    _c_fanout_n = int(_c_fanout_pdf.iloc[0, 0]) if len(_c_fanout_pdf) else 0
    print("PIECE C FAN-OUT GUARD | (consumer_id_hashed, TREATMENT_ID) pairs mapping to >1 distinct "
          "CLNT_NO in MASTER (load_tm >=", MASTER_FLOOR_C, "):", _c_fanout_n, "- if >0, "
          "c_monthly_curve unsub counts inflate by ~this share; A1/A2 unaffected (client-grain).")

    for _b in (range(1) if SMOKE else range(N_BITES)):
        land_c_bite(_b)
    print("PULL C done - landed at", C_DIR + "*")
else:
    print("PULL C skipped - not in RUN_PULLS")


def read_c_raw():
    sdf = spark.read.parquet(C_DIR + "bite_?")
    missing = [c.name for c in C_SCHEMA.fields if c.name not in sdf.columns]
    if missing:
        raise RuntimeError("c_month_mne missing %s. Rerun Cell [4]." % missing)
    return sdf


# %% [5] UCP JOIN - PIECE A. Single closed-month snapshot, HARDCODED to UCP_MONTH_A (Cell [0],
# Window A close) - red-team BLOCKER fix. Was date.today()-derived ("last closed month-end at run
# time"), which drifted with run date; same defect class as spotlight2.py's Piece-B anchor bug
# (trap #9), just not caught for Piece A until this review. UCP_MONTH_A is confirmed inside the
# live UCP partition range (references/ucp/gotchas.md #1). Casts clnt_no through decimal(18,0)->
# long on BOTH sides (trap #12), prints 5 sample ids per side, hard-errors below UCP_MATCH_FLOOR.
# T/I/B/C kept SEPARATE per the brief's locked decision ("TIBC = the four UCP fields kept
# SEPARATE... not collapsed to a count. Depth (count) stays too.") - unlike spotlight.py's Cube 1,
# which only kept the depth integer.
# PREREQ GATE: needs a1_client landed (Cell [2]) before it touches UCP - checked below BEFORE the
# UCP read, so a missing prerequisite fails fast and explained, not mid-read.
#
# BITE RETROFIT (OOM fix - this cell killed the 4GB local-mode kernel at full scale, Cell [5]
# joining ~10.4M base clients against a bank-wide Personal UCP snapshot in one shot). UCP is read
# ONCE, Personal-filtered, column-pruned, deduped and enriched (age/tenure bands, held_t/i/b/c,
# prod_cat_cnt) as a single pass - that part is a single-table transform, safe at full scale. The
# actual JOIN against base_ids_a is what blew up memory, so it is bitten: loop bite k, filter
# base_ids_a to MOD(ABS(clnt_no), N_BITES)=k (same predicate the Teradata pulls use), join that
# ~1-bite slice against the (cached) pruned UCP, land it to its own bite subdir with the usual
# _ROWCOUNT marker so a rerun skips already-landed bites. SMOKE=True runs bite 0 only, matching
# every other pull in this file. The UCP_MATCH_FLOOR check moves to AFTER the loop, computed off
# the accumulated landed total (not a separate full-scale pre-loop join) - same number, no second
# full-scale pass.
# ENGINE: PySpark (YARN) reading HDFS parquet - not Trino, not Teradata.

try:
    _a1_probe_n = spark.read.parquet(A1_DIR + "bite_?").limit(1).count()
except Exception:
    _a1_probe_n = 0
if _a1_probe_n == 0:
    raise RuntimeError(
        "Cell [5] needs a1_client landed data before it can join to UCP, and none was found at "
        + A1_DIR + " - run Cell [2] (Pull A1) first, or restore 'A1' in RUN_PULLS (Cell [0]) if it "
        "was removed, then rerun from Cell [2].")
print("Cell [5] pre-check: a1_client landed data found at", A1_DIR, "- proceeding to UCP read.")

_ucp_anchor = UCP_MONTH_A   # HARDCODED (Cell [0]) - never derived from date.today().
_ucp_path = UCP_BASE + "MONTH_END_DATE=" + _ucp_anchor
_TIBC_COLS = ["T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT"]
_UCP_COLS = ["CLNT_NO", "CLNT_TYP", "AGE", "TENURE_RBC_YEARS"] + _TIBC_COLS

_ucp_raw = spark.read.option("basePath", UCP_BASE).parquet(_ucp_path)
_missing = [c for c in _UCP_COLS if c not in _ucp_raw.columns]
assert not _missing, "UCP missing required columns at " + _ucp_anchor + ": " + str(_missing)
print("UCP SCHEMA PROBE at", _ucp_anchor, "- all required columns present:", _UCP_COLS)

# ---- Read UCP ONCE, filter Personal, select ONLY needed columns (column pruning - the biggest
# single lever on this table's memory footprint before any join happens). ----
ucp_sel = (_ucp_raw
           .filter(F.trim(F.col("CLNT_TYP")) == "Personal")
           .select(*_UCP_COLS)
           .withColumn("clnt_no_long", F.col("CLNT_NO").cast("decimal(18,0)").cast("long")))

_a1_for_base = read_a1().select("clnt_no")
_base_nulls = _a1_for_base.filter(F.col("clnt_no").isNull()).count()
if _base_nulls > 0:
    print("WARN: base_ids_a drops", _base_nulls, "NULL-clnt_no rows (unjoinable; landing "
          "artifact - same 10 rows Cell [6] guards). Distinct-count arithmetic: NULLs would "
          "collapse to 1 phantom row and desync the post-loop total assert.")
base_ids_a = (_a1_for_base.filter(F.col("clnt_no").isNotNull())
              .withColumnRenamed("clnt_no", "clnt_no_long").distinct())
_left_n = base_ids_a.count()   # single-table count, safe at full scale; NULL-free by construction.

print("Sample clnt_no (a1_client, 5):", [r.clnt_no_long for r in base_ids_a.limit(5).collect()])
print("Sample CLNT_NO (UCP, 5):", [r.clnt_no_long for r in ucp_sel.select("clnt_no_long").limit(5).collect()])

_ucp_dupes = ucp_sel.count() - ucp_sel.select("clnt_no_long").distinct().count()
print("UCP duplicate CLNT_NO rows:", _ucp_dupes, "- deduping before the join.")

_held = [(F.coalesce(F.col(c), F.lit(0)) > 0).cast("int") for c in _TIBC_COLS]
ucp_enriched = (ucp_sel
                 .dropDuplicates(["clnt_no_long"])
                 .withColumn("age_band", _band(F.col("AGE"), AGE_EDGES))
                 .withColumn("tenure_band", _band(F.col("TENURE_RBC_YEARS"), TENURE_EDGES))
                 .withColumn("held_t", _held[0])
                 .withColumn("held_i", _held[1])
                 .withColumn("held_b", _held[2])
                 .withColumn("held_c", _held[3])
                 .withColumn("prod_cat_cnt", (_held[0] + _held[1] + _held[2] + _held[3]))
                 .select("clnt_no_long", "age_band", "tenure_band",
                         "held_t", "held_i", "held_b", "held_c", "prod_cat_cnt")
                 .cache())
_n_ucp_enriched = ucp_enriched.count()   # materializes the cache once - single table, no join yet.
print("Pruned + enriched UCP (Personal, deduped, needed columns only) cached:", _n_ucp_enriched,
      "rows - this is what every bite below joins against.")

# ---- Bite loop: the actual client-grain x client-grain join, done ~1 bite (~1.1M rows) at a
# time. Resume-safe via _landed()/_write_spark_marker, same convention as every Teradata pull. ----
for _bite in (range(1) if SMOKE else range(N_BITES)):
    _bite_name = "ucp_enriched_a2_v%d/bite_%d" % (SCHEMA_VERSION, _bite)
    if _landed(_bite_name):
        print(_bite_name, ": already landed,", spark.read.parquet(BASE + _bite_name).count(),
              "rows - SKIP")
        continue
    _base_bite = base_ids_a.filter((F.abs(F.col("clnt_no_long")) % N_BITES) == _bite)
    _base_bite_n = _base_bite.count()
    _joined_bite = (_base_bite
                     .join(ucp_enriched, "clnt_no_long", "left")
                     .withColumn("age_band", F.coalesce(F.col("age_band"), F.lit("no_ucp_match")))
                     .withColumn("tenure_band", F.coalesce(F.col("tenure_band"), F.lit("no_ucp_match")))
                     .withColumn("held_t", F.coalesce(F.col("held_t"), F.lit(-1)))
                     .withColumn("held_i", F.coalesce(F.col("held_i"), F.lit(-1)))
                     .withColumn("held_b", F.coalesce(F.col("held_b"), F.lit(-1)))
                     .withColumn("held_c", F.coalesce(F.col("held_c"), F.lit(-1)))
                     .withColumn("prod_cat_cnt",
                                 F.coalesce(F.col("prod_cat_cnt").cast("string"), F.lit("no_ucp_match")))
                     .withColumnRenamed("clnt_no_long", "clnt_no"))
    _joined_bite.write.mode("overwrite").parquet(BASE + _bite_name)
    _n_back = spark.read.parquet(BASE + _bite_name).count()
    assert _n_back == _base_bite_n, (
        "ucp_enriched_a bite %d: wrote %d rows but the base bite has %d distinct clients - fan-out "
        "on a duplicate CLNT_NO within this bite. Every downstream A4 cube is unsafe until fixed."
        % (_bite, _n_back, _base_bite_n))
    _write_spark_marker(_bite_name, _n_back)
    print(_bite_name, ": landed", _n_back, "rows (bite", _bite, "of", N_BITES, "), fan-out check OK.")

ucp_enriched.unpersist()

ucp_enriched_a = spark.read.parquet(UCPA_DIR + "bite_?")
_n_ucp_a = ucp_enriched_a.count()
print("Cell [5] done - ucp_enriched_a landed (bitten) at", UCPA_DIR, "|", _n_ucp_a, "rows")
assert _n_ucp_a == _left_n, (
    "ucp_enriched_a has %d rows (summed across bites) but base_ids_a (distinct) has %d - a bite "
    "filter missed rows, or fan-out slipped past the per-bite guard. Every downstream A4 cube is "
    "unsafe until fixed." % (_n_ucp_a, _left_n))

# ---- Match-rate floor, computed on the ACCUMULATED total AFTER the loop (not a separate
# full-scale pre-loop join) - held_t == -1 is the no-match sentinel set by the per-bite left join
# above, so this is a single-table count over the already-landed result. ----
_matched_n = ucp_enriched_a.filter(F.col("held_t") != -1).count()
_match_pct = 100.0 * _matched_n / _left_n if _left_n else 0.0
print("UCP JOIN MATCH RATE (Piece A, accumulated across all bites) - a1 clients:", _left_n,
      "| matched:", _matched_n, "| match pct: %.1f%%" % _match_pct)
assert _match_pct >= UCP_MATCH_FLOOR, (
    "UCP join match rate %.1f%% (%d/%d) is below the %.0f%% floor - looks like a broken key, not "
    "attrition. Check clnt_no normalization and the UCP snapshot date (%s)."
    % (_match_pct, _matched_n, _left_n, UCP_MATCH_FLOOR, _ucp_anchor))
print("held_t/i/b/c == -1 means no_ucp_match (not '0 = does not hold'), so pivots must treat -1 "
      "as its own bucket, not fold it into 0.")


# %% [6] A1 OUTPUT - unique enterprise-wide unsub clients in-window + per-mne unsub counts.
# Enterprise total is a CLIENT-GRAIN dedup (from a1_client, one row per client already) - never a
# SUM of per-mne unsubs_attributed, which would double-count multi-list unsubscribers (trap #3).
# CARDS_TOTAL_UNIQUE_CLIENTS (round-2 review ask) is the SAME dedup pattern applied to
# cards_unsub_flag instead of unsub_flag_any - a1_client's bites partition clients disjointly
# (asserted below), so a plain COUNT DISTINCT over the full landed table is exact, not an
# approximation of a per-bite sum. This is the ONE exact Cards-vs-rest number in the file; every
# other per-LOB ratio (built from A2's mne sums) stays an upper bound.
# ENGINE: PySpark.

a1_client = read_a1().cache()
a2_raw = read_a2_raw()

# NULL clnt_no rows cannot join to anything downstream and collapse into one
# phantom "duplicate client" in the distinct-count (2026-08-02: 10 NULL rows
# landed, one per bite - conversion artifact at landing; the Teradata SQL itself
# excludes NULL ids). Count LOUDLY, drop, then assert uniqueness on the rest.
_a1_nulls = a1_client.filter(F.col("clnt_no").isNull()).count()
if _a1_nulls > 0:
    print("WARN: dropping %d NULL-clnt_no rows from a1_client (unjoinable; landing "
          "conversion artifact). If this number is ever more than a handful, investigate." % _a1_nulls)
    a1_client = a1_client.filter(F.col("clnt_no").isNotNull()).cache()

_a1_n = a1_client.count()
_a1_dupes = _a1_n - a1_client.select("clnt_no").distinct().count()
assert _a1_dupes == 0, "a1_client has %d duplicate clnt_no rows - bites did not partition disjointly" % _a1_dupes
print("a1_client uniqueness on clnt_no: confirmed, 0 duplicates,", _a1_n, "rows.")

_enterprise_unsubs = a1_client.filter(F.col("unsub_flag_any") == 1).select("clnt_no").distinct().count()
_enterprise_mailed = a1_client.select("clnt_no").distinct().count()
print("A1 ENTERPRISE - unique unsub clients in-window (Jan-Apr 2026):", _enterprise_unsubs,
      "of", _enterprise_mailed, "mailed clients (%.2f%% unsub rate)."
      % (100.0 * _enterprise_unsubs / _enterprise_mailed if _enterprise_mailed else 0.0))

_cards_unsubs = a1_client.filter(F.col("cards_unsub_flag") == 1).select("clnt_no").distinct().count()
print("A1 CARDS - unique CARDS-unsub clients in-window (Jan-Apr 2026):", _cards_unsubs,
      "- EXACT (cards_unsub_flag is a client-grain column in a1_client, not a per-mne sum;",
      "%.2f%% of the enterprise total)." % (100.0 * _cards_unsubs / _enterprise_unsubs if _enterprise_unsubs else 0.0))

a2_by_mne = (a2_raw.groupBy("mne")
             .agg(F.sum("senders").alias("senders"),
                  F.sum("unsubs_attributed").alias("unsubs_attributed"),
                  F.sum("leavers_exposed").alias("leavers_exposed"))
             .cache())

_per_mne_sum = a2_by_mne.agg(F.sum("unsubs_attributed").alias("s")).collect()[0]["s"] or 0
print("Sanity check (expected to DIFFER, not match): SUM of per-mne unsubs_attributed =", _per_mne_sum,
      "vs enterprise dedup =", _enterprise_unsubs, "- the gap is clients who unsubbed under more "
      "than one mne in-window (trap #3). If per-mne sum < enterprise dedup, investigate before "
      "shipping - that direction is not explainable by multi-list unsubbing.")
assert _per_mne_sum >= _enterprise_unsubs, (
    "per-mne SUM (%d) is LESS than the enterprise dedup (%d) - impossible if unsubs_attributed is "
    "correctly per-mne; investigate PULL A2 before trusting a1_mne_share." % (_per_mne_sum, _enterprise_unsubs))

_a1_mne_rows = a2_by_mne.select("mne", "unsubs_attributed")
_a1_enterprise_row = spark.createDataFrame(
    [("ENTERPRISE_TOTAL_UNIQUE_CLIENTS", int(_enterprise_unsubs))], ["mne", "unsubs_attributed"])
_a1_cards_row = spark.createDataFrame(
    [("CARDS_TOTAL_UNIQUE_CLIENTS", int(_cards_unsubs))], ["mne", "unsubs_attributed"])
_a1_summary_mnes = ["ENTERPRISE_TOTAL_UNIQUE_CLIENTS", "CARDS_TOTAL_UNIQUE_CLIENTS"]  # LIST, not
# tuple - pyspark Column.isin() unpacks list/set but wraps a tuple whole into lit() -> JVM
# "literal for ArrayList not supported" crash (hit 2026-08-03, first run to reach this line).
a1_mne_share = (_a1_mne_rows.unionByName(_a1_enterprise_row).unionByName(_a1_cards_row)
                 .orderBy(F.col("mne").isin(_a1_summary_mnes), F.desc("unsubs_attributed")))
a1_mne_share = _stamp(a1_mne_share, "WIN_A Jan-Apr 2026", "enterprise-wide, all mnes, shape-filtered")

a1_mne_share_pd = a1_mne_share.toPandas()
print("A1_MNE_SHARE | grain: one row per mne + ENTERPRISE_TOTAL row + CARDS_TOTAL row (both "
      "EXACT client-grain dedups, per-mne rows remain upper bounds) |", len(a1_mne_share_pd), "rows")
print(a1_mne_share_pd.to_string(index=False))
write_cube(a1_mne_share, "a1_mne_share")


# %% [7] A2 OUTPUT - mne x {senders, unsubs_attributed, leavers_exposed}, counts only, no rates
# (Andre derives rates in the Excel pivot). ENGINE: PySpark.

a2_mne_rates = _stamp(a2_by_mne.orderBy(F.desc("unsubs_attributed")), "WIN_A Jan-Apr 2026",
                       "enterprise-wide, all mnes, shape-filtered")
a2_mne_rates_pd = a2_mne_rates.toPandas()
print("A2_MNE_RATES | grain: one row per mne | senders/unsubs_attributed/leavers_exposed are "
      "separate NAMED columns (trap #2) |", len(a2_mne_rates_pd), "rows")
print(a2_mne_rates_pd.to_string(index=False))
write_cube(a2_mne_rates, "a2_mne_rates")


# %% [8] A3 OUTPUT - in-window contact load: n_emails_all / n_emails_cards, banded, x unsub_flag
# (any-list, primary bucket) x cards_unsub_flag (rides as an extra measure column, not a dim - it
# is a SUBSET of leavers, so it stays a count column beside stayers/leavers rather than
# multiplying the grain). No 12-month lookback per the brief's frequency rules (locked): 3-4 month
# in-window intensity is the metric; annualization happens at presentation layer only, if ever.
# ENGINE: PySpark.

client_roll_a3 = (a1_client
                   .withColumn("n_emails_all_bucket", _band(F.col("n_emails_all"), WIN_EMAILS_ALL_EDGES))
                   .withColumn("n_emails_cards_bucket", _band(F.col("n_emails_cards"), WIN_EMAILS_CARDS_EDGES)))

a3_contact_cube = (client_roll_a3
                    .groupBy("n_emails_all_bucket", "n_emails_cards_bucket")
                    .agg(F.sum(F.when(F.col("unsub_flag_any") == 0, 1).otherwise(0)).alias("stayers"),
                         F.sum(F.when(F.col("unsub_flag_any") == 1, 1).otherwise(0)).alias("leavers"),
                         F.sum(F.when((F.col("unsub_flag_any") == 1) & (F.col("cards_unsub_flag") == 1), 1)
                               .otherwise(0)).alias("leavers_cards_unsub_subset"),
                         F.count("*").alias("clients_total"))
                    .orderBy("n_emails_all_bucket", "n_emails_cards_bucket")
                    .cache())

a3_contact_cube_stamped = _stamp(a3_contact_cube, "WIN_A Jan-Apr 2026, in-window only, no lookback",
                                  "enterprise-wide clients mailed in WIN_A")
a3_pd = a3_contact_cube_stamped.toPandas()
_a3_total = int(a3_pd["clients_total"].sum())
print("A3_CONTACT_CUBE | grain: one row per (n_emails_all_bucket, n_emails_cards_bucket) | "
      "stayers/leavers COLUMNS, cards subset rides beside them | %d rows | %d clients total"
      % (len(a3_pd), _a3_total))
print(a3_pd.to_string(index=False))
assert _a3_total == _a1_n, (
    "a3 cube totals %d clients but a1_client has %d - a band expression dropped rows (check "
    "'unbucketed' fallout in WIN_EMAILS edges)." % (_a3_total, _a1_n))
write_cube(a3_contact_cube_stamped, "a3_contact_cube")


# %% [9] A4 OUTPUT - age_band x tenure_band x held_t x held_i x held_b x held_c (kept SEPARATE,
# per the brief's locked decision) x prod_cat_cnt (depth, kept too) x {stayers, leavers}.
# leavers_cards_unsub rides beside leavers as the cards-view subset (same pattern as A3's
# leavers_cards_unsub_subset) - lets the cards profile cut be derived without a second cube.
#
# BITE RETROFIT (OOM fix): a1_client x ucp_enriched_a is a client-grain x client-grain join at up
# to ~10.4M rows on each side - the same shape of join that killed Cell [5]. Both sides already
# share the SAME MOD(ABS(clnt_no), N_BITES) partitioning (a1_client via the Teradata pulls,
# ucp_enriched_a via Cell [5]'s bite loop), so this is bitten by reading ucp_enriched_a's own
# per-bite subdir and filtering a1_client to the matching bite - never materializing a full-scale
# join. The cube's measures (stayers/leavers/leavers_cards_unsub/clients_total) are additive
# counts, so each bite produces a PARTIAL cube; partials are unioned and summed into the final
# cube after the loop (never a full-scale union of client-grain rows, only of tiny partial cubes).
# ENGINE: PySpark.

_a4_partials = []
_a4_join_total = 0
_a4_expected = 0   # sum of a1-bite counts over the bites THIS RUN processes - the mode-correct
                   # comparison base (SMOKE processes bite 0 only; comparing vs full _a1_n under
                   # SMOKE was a mode-blind assert that crashed the 2026-08-03 run).
for _bite in (range(1) if SMOKE else range(N_BITES)):
    _a1_bite = a1_client.filter((F.abs(F.col("clnt_no")) % N_BITES) == _bite)
    _a1_bite_n = _a1_bite.count()
    _a4_expected += _a1_bite_n
    _ucp_bite_path = "ucp_enriched_a2_v%d/bite_%d" % (SCHEMA_VERSION, _bite)
    _ucp_bite = spark.read.parquet(BASE + _ucp_bite_path)
    _a4_src_bite = _a1_bite.join(_ucp_bite, "clnt_no", "left")
    _a4_bite_n = _a4_src_bite.count()
    assert _a4_bite_n == _a1_bite_n, (
        "a4 bite %d: %d rows after joining ucp_enriched_a's bite onto a1_client's matching bite "
        "(%d) - the UCP join fanned out within this bite. ucp_enriched_a was asserted unique on "
        "clnt_no per-bite in Cell [5]; re-check that assert." % (_bite, _a4_bite_n, _a1_bite_n))
    _a4_join_total += _a4_bite_n
    _a4_partials.append(
        _a4_src_bite
        .groupBy("age_band", "tenure_band", "held_t", "held_i", "held_b", "held_c", "prod_cat_cnt")
        .agg(F.sum(F.when(F.col("unsub_flag_any") == 0, 1).otherwise(0)).alias("stayers"),
             F.sum(F.when(F.col("unsub_flag_any") == 1, 1).otherwise(0)).alias("leavers"),
             F.sum(F.when((F.col("unsub_flag_any") == 1) & (F.col("cards_unsub_flag") == 1), 1)
                   .otherwise(0)).alias("leavers_cards_unsub"),
             F.count("*").alias("clients_total")))
    print("A4 bite", _bite, "of", N_BITES, ": joined", _a4_bite_n, "clients, no fan-out, partial cube built.")

assert _a4_join_total == _a4_expected, (
    "a4 join total across processed bites is %d but those bites' a1_client rows sum to %d - "
    "fan-out or dropped rows inside the bite loop." % (_a4_join_total, _a4_expected))
if SMOKE:
    print("A4 SMOKE mode: bite 0 only -", _a4_join_total, "of", _a1_n, "a1 clients processed; "
          "full-coverage check runs on the SMOKE=False pass.")
else:
    assert _a4_expected == _a1_n, (
        "a4 full run covered %d a1 rows but a1_client has %d - the MOD/ABS bite partition missed "
        "clients." % (_a4_expected, _a1_n))
    print("A4 join row-count guard: a1_client", _a1_n, "-> a4_src (summed across bites)",
          _a4_join_total, "- no fan-out, full coverage confirmed.")

_a4_union = _a4_partials[0]
for _p in _a4_partials[1:]:
    _a4_union = _a4_union.unionByName(_p)

a4_profile_cube = (_a4_union
                    .groupBy("age_band", "tenure_band", "held_t", "held_i", "held_b", "held_c", "prod_cat_cnt")
                    .agg(F.sum("stayers").alias("stayers"),
                         F.sum("leavers").alias("leavers"),
                         F.sum("leavers_cards_unsub").alias("leavers_cards_unsub"),
                         F.sum("clients_total").alias("clients_total"))
                    .orderBy("age_band", "tenure_band", "prod_cat_cnt"))

a4_profile_cube_stamped = _stamp(a4_profile_cube, "WIN_A Jan-Apr 2026",
                                  "enterprise-wide clients mailed in WIN_A, UCP-enriched")
a4_pd = a4_profile_cube_stamped.toPandas()
print("A4_PROFILE_CUBE | grain: one row per (age_band, tenure_band, held_t, held_i, held_b, "
      "held_c, prod_cat_cnt) | stayers/leavers/leavers_cards_unsub COLUMNS | %d rows | %d clients total"
      % (len(a4_pd), int(a4_pd["clients_total"].sum())))
print(a4_pd.head(30).to_string(index=False))
write_cube(a4_profile_cube_stamped, "a4_profile_cube")


# %% [10] C OUTPUT - month x mne, sends + unsubs_attributed, summed across bites (bites partition
# clients disjointly, so summing per-bite partial counts is exact - same logic as spotlight.py's
# q_mne/q_trend). ~300-1,000 rows expected, server-side aggregate only, no client-grain landing
# anywhere in this cell. ENGINE: PySpark.

c_raw = read_c_raw()
c_monthly_curve = (c_raw
                    .groupBy("mne", "ym")
                    .agg(F.sum("sends").alias("sends"),
                         F.sum("unsubs_attributed").alias("unsubs_attributed"))
                    .orderBy("mne", "ym"))

c_monthly_curve_stamped = _stamp(c_monthly_curve, "trailing 12m (%s -> %s)" % (WIN_C_FLOOR, WIN_C_CEIL),
                                  "enterprise-wide, all mnes, shape-filtered")
c_pd = c_monthly_curve_stamped.toPandas()
print("C_MONTHLY_CURVE | grain: one row per (mne, ym) | %d rows" % len(c_pd))
if not (100 <= len(c_pd) <= 3000):
    print("WARN: c_monthly_curve has %d rows - expected roughly 300-1,000 per the brief. Never "
          "fatal (SMOKE bites are legitimately sparse) - check WIN_C_FLOOR/CEIL and the mne count "
          "before shipping a non-SMOKE run." % len(c_pd))
print(c_pd.head(30).to_string(index=False))
write_cube(c_monthly_curve_stamped, "c_monthly_curve")


# %% [11] DFP ACCUMULATOR VALIDATION GATE - blocking, runs BEFORE Piece B's DFP pull depends on
# net_prch_amt_mtd. Verbatim method from spotlight2.py Cell [1], re-anchored to T0_ANCHOR_B (a
# fixed past date, not "now" - matches this file's anchor-hardcoding fix).
# ENGINE: Teradata-direct (D3CV12A.DLY_FULL_PORTFOLIO). Reuses the EDW connection from Cell [1].

_val_month_start = _add_months(T0_ANCHOR_B.replace(day=1), -1)
_val_month_end = T0_ANCHOR_B.replace(day=1)
VALIDATION_MONTH = _val_month_start.isoformat()

_val_sql = """
WITH month_rows AS (
    SELECT p.acct_no, p.dt_record_ext,
           CAST(p.net_prch_amt_dly AS FLOAT) AS dly,
           CAST(p.net_prch_amt_mtd AS FLOAT) AS mtd
    FROM D3CV12A.DLY_FULL_PORTFOLIO p
    WHERE p.dt_record_ext >= DATE '%(mstart)s'
      AND p.dt_record_ext <  DATE '%(mend)s'
      AND p.clnt_no IS NOT NULL
      AND MOD(ABS(p.acct_no), 100000) = 7
),
ranked AS (
    SELECT acct_no, dt_record_ext, dly, mtd,
           ROW_NUMBER() OVER (PARTITION BY acct_no ORDER BY dt_record_ext DESC) AS rn
    FROM month_rows
),
sums AS (
    SELECT acct_no, SUM(dly) AS sum_dly, COUNT(*) AS n_rows
    FROM month_rows
    GROUP BY acct_no
)
SELECT s.acct_no, s.n_rows, s.sum_dly, r.mtd AS last_mtd, r.dt_record_ext AS last_dt
FROM sums s
INNER JOIN ranked r ON r.acct_no = s.acct_no AND r.rn = 1
ORDER BY s.acct_no
""" % {"mstart": _val_month_start.isoformat(), "mend": _val_month_end.isoformat()}

_val_pdf = edw_pd(_val_sql)
assert len(_val_pdf) > 0, (
    "accumulator validation pulled zero accounts for %s - widen the sample or pick another month; "
    "do NOT proceed on an unproven accumulator." % VALIDATION_MONTH)

_val_pdf.columns = [c.lower() for c in _val_pdf.columns]
_val_pdf["sum_dly"] = pd.to_numeric(_val_pdf["sum_dly"], errors="coerce")
_val_pdf["last_mtd"] = pd.to_numeric(_val_pdf["last_mtd"], errors="coerce")
_val_pdf["abs_diff"] = (_val_pdf["last_mtd"] - _val_pdf["sum_dly"]).abs()

print("ACCUMULATOR CHECK | month", VALIDATION_MONTH, "| grain: one row per acct_no |",
      len(_val_pdf), "accounts sampled")
_material = _val_pdf[_val_pdf["sum_dly"].abs() > 1.0]
_bad = _material[_material["abs_diff"] > (0.01 * _material["sum_dly"].abs())]
print("Material accounts (abs(sum_dly) > 1):", len(_material), "| mismatching by >1pct:", len(_bad))

if len(_material) == 0:
    raise RuntimeError(
        "ACCUMULATOR CHECK INCONCLUSIVE for %s - widen the sample or pick another month." % VALIDATION_MONTH)
if len(_bad) > 0:
    raise RuntimeError(
        "ACCUMULATOR CHECK FAILED - net_prch_amt_mtd does not equal SUM(net_prch_amt_dly) for %d "
        "of %d accounts in %s. Fix the method before rerunning.\n%s"
        % (len(_bad), len(_material), VALIDATION_MONTH, _bad.head(20).to_string(index=False)))

ACCUM_VALIDATED = True
print("ACCUMULATOR CHECK PASSED -", len(_material), "materially-active accounts, last-of-month "
      "MTD equals SUM(daily deltas) within 1pct on every one.")


# %% [12] PULL B_COHORT - client-grain, cohort SCOPING pull. Defines the Piece-B population
# (clients mailed by CARDS_MNES on/before the anchor) AND the leaver flags at anchor, in the SAME
# pull that scopes it - this is the fix for spotlight2.py's defect #4 ("pulls bank-wide, cards
# flag applied post-hoc"). The WHERE mailed_cards = 1 at the very end IS the scoping: only cohort
# clients are landed, nothing bank-wide ever reaches HDFS.
#
# LEAVER FLAG has its OWN window (COHORT_B_FLOOR -> anchor+1day), separate from Piece A/C's
# windows - the fix for spotlight2.py's defect #2 ("do NOT reuse a flag computed over the full
# year"). any_unsub_by_anchor is enterprise-wide (any mne); cards_unsub_by_anchor is the cards
# subset, carried as a second column per the brief.
# ENGINE: Teradata-direct.

BCOHORT_SCHEMA = StructType([
    StructField("clnt_no", LongType(), True),
    StructField("any_unsub_by_anchor", IntegerType(), True),
    StructField("cards_unsub_by_anchor", IntegerType(), True),
])


def _prep_bcohort(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    _n_null = pd.to_numeric(pdf["clnt_no"], errors="coerce").isna().sum()
    assert _n_null == 0, "clnt_no has %d nulls - CLNT_NO IS NOT NULL filter is not firing" % _n_null
    pdf["clnt_no"] = pd.to_numeric(pdf["clnt_no"], errors="coerce").astype("int64")
    for _c in ["any_unsub_by_anchor", "cards_unsub_by_anchor"]:
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce").fillna(0).astype("int32")
    return pdf[[f.name for f in BCOHORT_SCHEMA.fields]]


def land_bcohort_bite(bite):
    name = "b_cohort_v%d/bite_%d" % (SCHEMA_VERSION, bite)
    if _landed(name):
        print(name, ": already landed,", spark.read.parquet(BASE + name).count(), "rows - SKIP")
        return
    sql = """
    WITH ek AS (
        SELECT consumer_id_hashed, TREATMENT_ID, disposition_cd,
               MIN(disposition_dt_tm) AS dt
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd IN (1, 4)
          AND disposition_dt_tm >= DATE '%(floor)s'
          AND disposition_dt_tm <  DATE '%(ceil)s'%(tactic)s
        GROUP BY 1, 2, 3, CAST(disposition_dt_tm AS DATE)
    ),
    joined AS (
        SELECT m.CLNT_NO AS clnt_no,
               SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne,
               ek.disposition_cd AS disposition_cd
        FROM ek
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE load_tm >= DATE '%(mfloor)s'
                      AND CLNT_NO IS NOT NULL
                      AND MOD(ABS(CLNT_NO), %(n_bites)d) = %(bite)d) m
          ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
    ),
    client_flags AS (
        SELECT clnt_no,
               MAX(CASE WHEN disposition_cd = 1 AND mne IN (%(cards)s) THEN 1 ELSE 0 END) AS mailed_cards,
               MAX(CASE WHEN disposition_cd = 4 THEN 1 ELSE 0 END) AS any_unsub_by_anchor,
               MAX(CASE WHEN disposition_cd = 4 AND mne IN (%(cards)s) THEN 1 ELSE 0 END) AS cards_unsub_by_anchor
        FROM joined
        GROUP BY clnt_no
    )
    SELECT clnt_no, any_unsub_by_anchor, cards_unsub_by_anchor
    FROM client_flags
    WHERE mailed_cards = 1
    """ % {"floor": COHORT_B_FLOOR, "ceil": ANCHOR_B_CEIL, "mfloor": MASTER_FLOOR_B, "tactic": TACTIC_ID_SQL,
           "n_bites": N_BITES, "bite": bite, "cards": CARDS_LIST_SQL}
    pdf = edw_pd(sql)
    if len(pdf) == 0:
        print(name, ": zero cohort clients in this bite - possible for a MOD-narrow bite this "
              "far back, not necessarily an error. Continuing.")
        return
    pdf = _prep_bcohort(pdf)
    nback = _write_chunks(pdf, BCOHORT_SCHEMA, name)
    assert nback == len(pdf), name + " HDFS readback mismatch: pulled %d, read back %d" % (len(pdf), nback)
    print(name, ": landed", len(pdf), "cohort clients (Cards-mailed on/before anchor), readback", nback)


if "B_COHORT" in RUN_PULLS:
    for _b in (range(1) if SMOKE else range(N_BITES)):
        land_bcohort_bite(_b)
    print("PULL B_COHORT done - landed at", BCOHORT_DIR + "*")
else:
    print("PULL B_COHORT skipped - not in RUN_PULLS")


def read_bcohort():
    try:
        sdf = spark.read.parquet(BCOHORT_DIR + "bite_?")
    except Exception as _e:   # zero bites landed (all-empty pulls) -> explained empty, not a crash
        print("read_bcohort: NO landed bites at", BCOHORT_DIR, "(%s)" % type(_e).__name__,
              "- returning EMPTY cohort. Downstream B cells will report no_data, not crash.")
        return spark.createDataFrame([], BCOHORT_SCHEMA)
    missing = [c.name for c in BCOHORT_SCHEMA.fields if c.name not in sdf.columns]
    if missing:
        raise RuntimeError("b_cohort missing %s. Rerun Cell [12]." % missing)
    return sdf.withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))


def read_bcohort_bite(bite):
    """Single-bite read (not the 'bite_*' glob) - used by Cell [15]'s bite-looped panel build so
    that cell never touches the full-scale cohort table at once."""
    return _read_bite_or_empty(BCOHORT_DIR, bite, BCOHORT_SCHEMA).withColumn(
        "clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))


cohort_b = read_bcohort().cache()
_cohort_nulls = cohort_b.filter(F.col("clnt_no").isNull()).count()
if _cohort_nulls > 0:
    print("WARN: dropping %d NULL-clnt_no rows from cohort_b (unjoinable; landing artifact)." % _cohort_nulls)
    cohort_b = cohort_b.filter(F.col("clnt_no").isNotNull()).cache()
COHORT_B_N = cohort_b.count()
_cohort_dupes = COHORT_B_N - cohort_b.select("clnt_no").distinct().count()
assert _cohort_dupes == 0, "cohort_b has %d duplicate clnt_no - bites did not partition disjointly" % _cohort_dupes
print("COHORT B - Cards-mailed on/before", T0_ANCHOR_B.isoformat(), "|", COHORT_B_N, "clients, "
      "0 duplicates confirmed.")
cohort_b.groupBy(F.when(F.col("any_unsub_by_anchor") == 1, "LEAVER").otherwise("STAYER").alias("bucket")).agg(
    F.count("*").alias("clients"), F.sum("cards_unsub_by_anchor").alias("cards_unsub_subset")
).show(truncate=False)


# %% [13] PULL B_DFP - annual spend at t0 and t12, cohort-scoped via an embedded cohort CTE that
# INNER JOINs DLY_FULL_PORTFOLIO BEFORE any aggregation - the scoping happens inside this ONE
# Teradata statement, never post-hoc in Spark (fix for defect #4). The cohort CTE here is a CHEAP
# cards-only send check (mirrors spotlight.py Pull C's cost profile), re-derived rather than
# handed off from Cell [12] - keeps this cell self-contained and independently resumable.
# ENGINE: Teradata-direct (D3CV12A.DLY_FULL_PORTFOLIO). One scan, ROW_NUMBER pivot, never
# multi-scan (table_catalog_notes.md:147-152) - same pattern as spotlight2.py Cell [3].

assert "ACCUM_VALIDATED" in globals() and ACCUM_VALIDATED, "Run Cell [11] first."

BDFP_SCHEMA = StructType([
    StructField("clnt_no", LongType(), True),
    StructField("n_accts_total", LongType(), True),
    StructField("annual_spend_t0", DoubleType(), True),
    StructField("annual_spend_p12", DoubleType(), True),
])


def _cohort_cte_sql(bite):
    """Cheap, cards-only cohort-membership CTE, shared text between the DFP and BHV pulls. Kept
    as a Python function (not a stored SQL macro) so both pulls stay self-contained files.
    Half-open on the anchor (< ANCHOR_B_CEIL), matching Cell [12]'s own cohort pull - anchor-day
    off-by-one fix, was <= T0_ANCHOR_B which double-counted anchor-day events across the boundary."""
    return """
    cohort_ek AS (
        SELECT consumer_id_hashed, TREATMENT_ID, MIN(disposition_dt_tm) AS dt
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd = 1
          AND disposition_dt_tm >= DATE '%(cfloor)s'
          AND disposition_dt_tm <  DATE '%(anchor_ceil)s'
          AND SUBSTR(TREATMENT_ID, 8, 3) IN (%(cards)s)%(tactic)s
        GROUP BY 1, 2, CAST(disposition_dt_tm AS DATE)
    ),
    cohort AS (
        SELECT DISTINCT m.CLNT_NO AS clnt_no
        FROM cohort_ek
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE load_tm >= DATE '%(mfloor)s'
                      AND CLNT_NO IS NOT NULL
                      AND MOD(ABS(CLNT_NO), %(n_bites)d) = %(bite)d) m
          ON m.consumer_id_hashed = cohort_ek.consumer_id_hashed AND m.TREATMENT_ID = cohort_ek.TREATMENT_ID
    )""" % {"cfloor": COHORT_B_FLOOR, "anchor_ceil": ANCHOR_B_CEIL, "cards": CARDS_LIST_SQL,
            "tactic": TACTIC_ID_SQL, "mfloor": MASTER_FLOOR_B, "n_bites": N_BITES, "bite": bite}


_annual_case_t0 = "SUM(CASE WHEN ym IN (%s) THEN acct_month_spend ELSE 0 END)" % (
    ", ".join(str(y) for y in ANNUAL_YMS_B[0]))
_annual_case_p12 = "SUM(CASE WHEN ym IN (%s) THEN acct_month_spend ELSE 0 END)" % (
    ", ".join(str(y) for y in ANNUAL_YMS_B[12]))
# n_p12_rows - counts ACTUAL p12-month DFP rows (not spend dollars) per account. Round-2 review
# BLOCKER fix: the account-level SUM(...ELSE 0) above cannot tell "no p12 data yet" apart from
# "real $0 spend" - both look like 0. This count, summed at the client grain below, can: if a
# client has ZERO p12-month rows across every account, annual_spend_p12 must come back NULL
# (pre-close t12 arithmetic thinness), never 0.0. t0 needs no such guard (t0 is always fully past).
_p12_row_count_case = "SUM(CASE WHEN ym IN (%s) THEN 1 ELSE 0 END)" % (
    ", ".join(str(y) for y in ANNUAL_YMS_B[12]))


def _dfp_sql(bite):
    return """
    WITH %(cohort_cte)s,
    dfp_scoped AS (
        SELECT p.clnt_no, p.acct_no, p.dt_record_ext,
               CAST(p.net_prch_amt_mtd AS FLOAT) AS net_prch_amt_mtd,
               EXTRACT(YEAR FROM p.dt_record_ext) * 100 + EXTRACT(MONTH FROM p.dt_record_ext) AS ym
        FROM D3CV12A.DLY_FULL_PORTFOLIO p
        INNER JOIN cohort c ON c.clnt_no = p.clnt_no
        WHERE p.dt_record_ext >= DATE '%(sfloor)s'
          AND p.dt_record_ext <  DATE '%(sceil)s'
          AND p.clnt_no IS NOT NULL
          AND EXTRACT(YEAR FROM p.dt_record_ext) * 100 + EXTRACT(MONTH FROM p.dt_record_ext) IN (%(yms)s)
    ),
    ranked AS (
        SELECT clnt_no, acct_no, ym, net_prch_amt_mtd,
               ROW_NUMBER() OVER (PARTITION BY acct_no, ym ORDER BY dt_record_ext DESC) AS rn
        FROM dfp_scoped
    ),
    acct_month AS (
        SELECT clnt_no, acct_no, ym, net_prch_amt_mtd AS acct_month_spend
        FROM ranked
        WHERE rn = 1
    ),
    acct_wide AS (
        SELECT clnt_no, acct_no,
               %(annual_t0)s AS annual_spend_t0,
               %(annual_p12)s AS annual_spend_p12,
               %(p12_rows)s AS n_p12_rows
        FROM acct_month
        GROUP BY clnt_no, acct_no
    )
    SELECT clnt_no,
       COUNT(*) AS n_accts_total,
       SUM(annual_spend_t0) AS annual_spend_t0,
       CASE WHEN SUM(n_p12_rows) = 0 THEN NULL ELSE SUM(annual_spend_p12) END AS annual_spend_p12
    FROM acct_wide
    GROUP BY clnt_no
    """ % {"cohort_cte": _cohort_cte_sql(bite), "sfloor": SPEND_FLOOR_B, "sceil": SPEND_CEIL_B,
           "yms": SPEND_YMS_B_SQL, "annual_t0": _annual_case_t0, "annual_p12": _annual_case_p12,
           "p12_rows": _p12_row_count_case}


def _prep_bdfp(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    _n_null = pd.to_numeric(pdf["clnt_no"], errors="coerce").isna().sum()
    assert _n_null == 0, "clnt_no has %d nulls" % _n_null
    pdf["clnt_no"] = pd.to_numeric(pdf["clnt_no"], errors="coerce").astype("int64")
    pdf["n_accts_total"] = pd.to_numeric(pdf["n_accts_total"], errors="coerce").fillna(0).astype("int64")
    pdf["annual_spend_t0"] = pd.to_numeric(pdf["annual_spend_t0"], errors="coerce").fillna(0.0).astype("float64")
    # annual_spend_p12 - NO fillna (round-2 review BLOCKER fix). The SQL now emits a real SQL NULL
    # when a client has zero p12-month DFP rows (pre-close t12 thinness) - fillna(0.0) here would
    # silently turn that back into a fake "$0 spend", which Cell [15] then bands as "Low" instead
    # of "untiered", poisoning the whole t12 slice. Preserve NaN as an explicit Python None (not a
    # float NaN) so PySpark's schema-based row conversion writes it as a genuine parquet NULL,
    # which F.col("spend_at_offset").isNull() in Cell [15] can actually catch - float NaN would
    # NOT be caught by isNull() and would silently slip through as a non-null value.
    pdf["annual_spend_p12"] = pd.to_numeric(pdf["annual_spend_p12"], errors="coerce")
    _p12_isna = pdf["annual_spend_p12"].isna()
    pdf["annual_spend_p12"] = pdf["annual_spend_p12"].astype(object)
    pdf.loc[_p12_isna, "annual_spend_p12"] = None
    return pdf[[f.name for f in BDFP_SCHEMA.fields]]


def land_bdfp_bite(bite):
    path = "b_dfp_v%d/bite_%d" % (SCHEMA_VERSION, bite)
    if _landed_b_offset(path):
        return
    pdf = edw_pd(_dfp_sql(bite))
    if len(pdf) == 0:
        print(path, ": zero cohort clients with DFP rows in this bite - continuing (t12 is "
              "future-dated for the whole cohort as of this build; t0 should not be thin).")
        return
    pdf = _prep_bdfp(pdf)
    nback = _write_chunks(pdf, BDFP_SCHEMA, path)
    assert nback == len(pdf), path + " HDFS readback mismatch: pulled %d, read back %d" % (len(pdf), nback)
    _regime = _current_regime()
    _write_regime_flag(path, _regime)
    print(path, ": landed", len(pdf), "rows (cohort-scoped, offsets t0/p12 pivoted, regime=",
          _regime, "), readback", nback)


if "B_DFP" in RUN_PULLS:
    for _b in (range(1) if SMOKE else range(N_BITES)):
        land_bdfp_bite(_b)
    print("PULL B_DFP done - landed at", BDFP_DIR + "*")
else:
    print("PULL B_DFP skipped - not in RUN_PULLS")


def read_bdfp():
    try:
        sdf = spark.read.parquet(BDFP_DIR + "bite_?")
    except Exception as _e:   # zero bites landed -> explained empty, not a crash
        print("read_bdfp: NO landed bites at", BDFP_DIR, "(%s)" % type(_e).__name__,
              "- returning EMPTY frame. Downstream B cells report no_data, not crash.")
        return spark.createDataFrame([], BDFP_SCHEMA)
    missing = [c.name for c in BDFP_SCHEMA.fields if c.name not in sdf.columns]
    if missing:
        raise RuntimeError("b_dfp missing %s. Rerun Cell [13]." % missing)
    return sdf.withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))


def read_bdfp_bite(bite):
    """Single-bite read - used by Cell [15]'s bite-looped panel build."""
    return _read_bite_or_empty(BDFP_DIR, bite, BDFP_SCHEMA).withColumn(
        "clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))


# %% [14] PULL B_BHV - revolver/transactor at t0 and t12 EXACT month-ends, cohort-scoped the same
# way as Cell [13]. CR_CRD_RPTS_ACCT carries clnt_no directly - no DFP bridge needed (verbatim
# finding from spotlight2.py Cell [4], D2). Raw-value probe runs first, same as spotlight2.py -
# the rank mapping rests on values nobody has printed for THIS pull.
# ENGINE: Teradata-direct (D3CV12A.CR_CRD_RPTS_ACCT).

BBHV_SCHEMA = StructType([
    StructField("clnt_no", LongType(), True),
    StructField("bhvr_rank_t0", IntegerType(), True),
    StructField("bhvr_rank_p12", IntegerType(), True),
])

_seg_case_sql = "\n                    ".join(
    "WHEN TRIM(r.usg_bhvr_seg_at_cyc_cd) = '%s' THEN %d" % (lbl, rk) for lbl, rk in SEG_PRECEDENCE)

_probe_sql = """
SELECT r.usg_bhvr_seg_at_cyc_cd AS raw_value, COUNT(*) AS accounts
FROM D3CV12A.CR_CRD_RPTS_ACCT r
WHERE r.ME_DT = DATE '%s'
GROUP BY r.usg_bhvr_seg_at_cyc_cd
ORDER BY accounts DESC
""" % T0_ANCHOR_B.isoformat()

if "B_BHV" in RUN_PULLS:
    _probe = edw_pd(_probe_sql)
    _probe.columns = [c.lower() for c in _probe.columns]
    print("BEHAVIOUR VALUE PROBE at ME_DT", T0_ANCHOR_B.isoformat(), "| grain: one row per raw "
          "value |", len(_probe), "rows")
    print(_probe.to_string(index=False))
    _expected = [lbl for lbl, _ in SEG_PRECEDENCE]
    _seen = [str(v).strip() for v in _probe["raw_value"].tolist()]
    _unmapped = [v for v in _seen if v not in _expected and v not in ("", "None", "nan")]
    if _unmapped:
        print("WARNING: raw values not in SEG_PRECEDENCE, will read other_or_none:", _unmapped)
    else:
        print("All raw values map cleanly onto SEG_PRECEDENCE", _expected)
else:
    print("Behaviour probe skipped - B_BHV not in RUN_PULLS")


def _bhv_sql(bite):
    return """
    WITH %(cohort_cte)s,
    seg AS (
        SELECT r.clnt_no, r.ME_DT AS me_dt,
               CASE
                    %(seg_case)s
                    ELSE 4
               END AS seg_rank
        FROM D3CV12A.CR_CRD_RPTS_ACCT r
        INNER JOIN cohort c ON c.clnt_no = r.clnt_no
        WHERE r.ME_DT IN (%(anchors)s)
          AND r.clnt_no IS NOT NULL
    )
    SELECT clnt_no,
       MIN(CASE WHEN me_dt = DATE '%(t0)s' THEN seg_rank END) AS bhvr_rank_t0,
       MIN(CASE WHEN me_dt = DATE '%(p12)s' THEN seg_rank END) AS bhvr_rank_p12
    FROM seg
    GROUP BY clnt_no
    """ % {"cohort_cte": _cohort_cte_sql(bite), "seg_case": _seg_case_sql, "anchors": ANCHOR_DATES_SQL_B,
           "t0": T0_ANCHOR_B.isoformat(), "p12": P12_ANCHOR_B.isoformat()}


def _prep_bbhv(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    _n_null = pd.to_numeric(pdf["clnt_no"], errors="coerce").isna().sum()
    assert _n_null == 0, "clnt_no has %d nulls" % _n_null
    pdf["clnt_no"] = pd.to_numeric(pdf["clnt_no"], errors="coerce").astype("int64")
    for _c in ["bhvr_rank_t0", "bhvr_rank_p12"]:
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce").fillna(0).astype("int32")
    return pdf[[f.name for f in BBHV_SCHEMA.fields]]


def land_bbhv_bite(bite):
    path = "b_bhv_v%d/bite_%d" % (SCHEMA_VERSION, bite)
    if _landed_b_offset(path):
        return
    pdf = edw_pd(_bhv_sql(bite))
    if len(pdf) == 0:
        print(path, ": zero cohort clients with a behaviour row in this bite - t0 should not be "
              "thin (it is real past data); investigate if this keeps happening across bites.")
        return
    pdf = _prep_bbhv(pdf)
    nback = _write_chunks(pdf, BBHV_SCHEMA, path)
    assert nback == len(pdf), path + " HDFS readback mismatch: pulled %d, read back %d" % (len(pdf), nback)
    _regime = _current_regime()
    _write_regime_flag(path, _regime)
    print(path, ": landed", len(pdf), "rows (cohort-scoped, offsets t0/p12 pivoted, regime=",
          _regime, "), readback", nback)


if "B_BHV" in RUN_PULLS:
    for _b in (range(1) if SMOKE else range(N_BITES)):
        land_bbhv_bite(_b)
    print("PULL B_BHV done - landed at", BBHV_DIR + "*")
else:
    print("PULL B_BHV skipped - not in RUN_PULLS")


def read_bbhv():
    try:
        sdf = spark.read.parquet(BBHV_DIR + "bite_?")
    except Exception as _e:   # zero bites landed -> explained empty, not a crash
        print("read_bbhv: NO landed bites at", BBHV_DIR, "(%s)" % type(_e).__name__,
              "- returning EMPTY frame. Downstream B cells report no_data, not crash.")
        return spark.createDataFrame([], BBHV_SCHEMA)
    missing = [c.name for c in BBHV_SCHEMA.fields if c.name not in sdf.columns]
    if missing:
        raise RuntimeError("b_bhv missing %s. Rerun Cell [14]." % missing)
    return sdf.withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))


def read_bbhv_bite(bite):
    """Single-bite read - used by Cell [15]'s bite-looped panel build."""
    return _read_bite_or_empty(BBHV_DIR, bite, BBHV_SCHEMA).withColumn(
        "clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))


# %% [15] PIECE B CUBE - spend_tier x spend_tier_at_offset x usg_bhvr_seg x {stayers, leavers} x
# {t0, t12}, unique client counts. spend_tier is cut ONCE on annual_spend_t0 and HELD FIXED across
# t0/t12 (re-cutting per period lets clients migrate tiers and the comparison stops meaning
# anything - verbatim reasoning from spotlight2.py Cell [7]) - unchanged by this fix.
# spend_tier_at_offset is NEW (red-team fix - annual_spend_p12 was landed in Cell [13] but never
# used, so spend/tier trajectory was unanswerable): the offset-appropriate spend value
# (annual_spend_t0 at t_offset=0, annual_spend_p12 at t_offset=12) banded against the SAME t0
# cutpoints (_q1/_q2) - so "did this client's spend tier move" is now readable from the cube.
# Counts only - no spend dollars in the CSV, same as spend_tier always was.
# leavers_cards_unsub_subset rides beside leavers as the second column the brief asks for
# (cards_unsub_by_anchor), not a new pivot dimension.
# t12 (2026-08-31) is a FUTURE month as of this build (2026-08-02) - its rows will read thin/
# no_data until rerun after that month closes. Printed loudly below, not hidden.
#
# usg_bhvr_seg_t0 (round-2 review ask) rides as an ADDITIONAL groupBy dim on every row - the
# client's t0 segment, fixed, alongside the existing offset-dependent usg_bhvr_seg. On t0 rows the
# two are identical by construction; on t12 rows (usg_bhvr_seg_t0, usg_bhvr_seg) is a Revolver ->
# Transactor (etc.) flow pair, pivotable without a second cube. Existing columns unchanged.
#
# BITE RETROFIT (OOM fix): the panel build joins cohort_b x spend_tier x bhv_seg x dfp_wide -
# three client-grain tables joined onto the cohort, x2 for the t0/t12 union - the same shape of
# join that killed Cell [5]. cohort_b/dfp_wide/bhv_wide were ALREADY landed per-bite by Cells
# [12]/[13]/[14] using the SAME MOD(ABS(CLNT_NO), N_BITES) predicate (same bite param threaded
# through _cohort_cte_sql), so this reads each bite's subdir directly instead of the 'bite_*' glob
# - no re-partitioning needed, the disjoint split already exists on HDFS. The spend-tier cutpoints
# (_q1/_q2) MUST stay global (a per-bite tercile would not be comparable across bites), so those
# are computed once from a single-table streaming read of the full dfp table (approxQuantile is a
# bounded-memory distributed aggregate, not a join - safe to leave unbitten, same class as Cell
# [10]'s c_raw). Each bite then produces a PARTIAL cube (additive counts); partials are unioned and
# summed into the final cube after the loop.
# ENGINE: PySpark (YARN).

dfp_wide_all = read_bdfp()   # single table, no join - safe at full scale, used ONLY for the
                             # global quantile cut below.
_tier_src = dfp_wide_all.select("clnt_no", "annual_spend_t0").distinct()
_q1, _q2 = _tier_src.approxQuantile("annual_spend_t0", SPEND_TIER_QUANTILES, SPEND_TIER_REL_ERR)
print("SPEND TIER CUT | annual spend trailing 12m ending", T0_ANCHOR_B.isoformat(),
      "| cut ONCE (global, across all bites), held fixed at t0 and t12 | Low <=", round(_q1, 2),
      "< Mid <=", round(_q2, 2), "< High")
if _q1 == _q2:
    print("WARNING: tercile cut points are identical - more than a third of the cohort shares the "
          "same annual spend (likely zero). Read the tier as zero-vs-something, not three terciles.")

# seg-label CASE expressions - built once (data-independent), applied per bite in the loop below.
_seg_label_expr = None
for _rk in sorted(SEG_LABEL):
    _t0c = F.col("bhvr_rank_t0") == _rk
    _seg_label_expr = (F.when(_t0c, F.lit(SEG_LABEL[_rk])) if _seg_label_expr is None
                       else _seg_label_expr.when(_t0c, F.lit(SEG_LABEL[_rk])))
_seg_t0_expr = _seg_label_expr.otherwise(F.lit("no_data"))

_seg_label_expr2 = None
for _rk in sorted(SEG_LABEL):
    _p12c = F.col("bhvr_rank_p12") == _rk
    _seg_label_expr2 = (F.when(_p12c, F.lit(SEG_LABEL[_rk])) if _seg_label_expr2 is None
                        else _seg_label_expr2.when(_p12c, F.lit(SEG_LABEL[_rk])))
_seg_p12_expr = _seg_label_expr2.otherwise(F.lit("no_data"))

_stay = F.col("any_unsub_by_anchor") == 0
_leave = F.col("any_unsub_by_anchor") == 1

_b_partials = []
_n_long_b_total = 0
for _bite in (range(1) if SMOKE else range(N_BITES)):
    _cohort_bite = read_bcohort_bite(_bite)
    _cohort_bite_n = _cohort_bite.count()
    if _cohort_bite_n == 0:
        print("PIECE B bite", _bite, "of", N_BITES, ": zero cohort clients - skipping (matches "
              "Cell [12]'s own zero-row skip for this bite).")
        continue

    _dfp_bite = read_bdfp_bite(_bite)
    _bhv_bite = read_bbhv_bite(_bite)

    _spend_tier_bite = (_dfp_bite.select("clnt_no", "annual_spend_t0").distinct()
                         .withColumn("spend_tier",
                                     F.when(F.col("annual_spend_t0") <= _q1, "Low")
                                      .when(F.col("annual_spend_t0") <= _q2, "Mid")
                                      .otherwise("High"))
                         .select("clnt_no", "spend_tier"))

    _bhv_seg_bite = (_bhv_bite
                      .withColumn("usg_bhvr_seg_t0", _seg_t0_expr)
                      .withColumn("usg_bhvr_seg_p12", _seg_p12_expr)
                      .select("clnt_no", "usg_bhvr_seg_t0", "usg_bhvr_seg_p12"))

    # usg_bhvr_seg_t0 - FIXED dim (round-2 review ask, R/T flows): the client's t0 segment, joined
    # onto every row regardless of offset - so t0 rows carry usg_bhvr_seg_t0 == usg_bhvr_seg (same
    # source value) and t12 rows carry the client's t0 seg ALONGSIDE their own (offset-dependent)
    # usg_bhvr_seg. Pivoting (usg_bhvr_seg_t0, usg_bhvr_seg) on t12 rows reads as a Revolver ->
    # Transactor (etc.) flow matrix; existing columns are unchanged.
    _bhv_seg_t0_fixed = _bhv_seg_bite.select("clnt_no", "usg_bhvr_seg_t0")

    _long_t0_bite = (_cohort_bite
                      .join(_spend_tier_bite, "clnt_no", "left")
                      .join(_bhv_seg_bite.select("clnt_no", F.col("usg_bhvr_seg_t0").alias("usg_bhvr_seg")),
                            "clnt_no", "left")
                      .join(_dfp_bite.select("clnt_no", F.col("annual_spend_t0").alias("spend_at_offset")),
                            "clnt_no", "left")
                      .join(_bhv_seg_t0_fixed, "clnt_no", "left")
                      .withColumn("t_offset", F.lit(0)))
    _long_p12_bite = (_cohort_bite
                       .join(_spend_tier_bite, "clnt_no", "left")   # tier HELD FIXED at t0
                       .join(_bhv_seg_bite.select("clnt_no", F.col("usg_bhvr_seg_p12").alias("usg_bhvr_seg")),
                             "clnt_no", "left")
                       .join(_dfp_bite.select("clnt_no", F.col("annual_spend_p12").alias("spend_at_offset")),
                             "clnt_no", "left")
                       .join(_bhv_seg_t0_fixed, "clnt_no", "left")
                       .withColumn("t_offset", F.lit(12)))

    _long_b_bite = (_long_t0_bite.unionByName(_long_p12_bite)
                     .withColumn("spend_tier", F.coalesce(F.col("spend_tier"), F.lit("untiered")))
                     .withColumn("usg_bhvr_seg", F.coalesce(F.col("usg_bhvr_seg"), F.lit("no_data")))
                     .withColumn("usg_bhvr_seg_t0", F.coalesce(F.col("usg_bhvr_seg_t0"), F.lit("no_data")))
                     .withColumn("spend_tier_at_offset",
                                 F.when(F.col("spend_at_offset").isNull(), F.lit("untiered"))
                                  .when(F.col("spend_at_offset") <= _q1, F.lit("Low"))
                                  .when(F.col("spend_at_offset") <= _q2, F.lit("Mid"))
                                  .otherwise(F.lit("High"))))

    _n_long_b_bite = _long_b_bite.count()
    _n_expected_bite = _cohort_bite_n * len(T_OFFSETS_B)
    assert _n_long_b_bite == _n_expected_bite, (
        "PIECE B bite %d: long table has %d rows, expected %d (%d cohort clients x %d offsets) - a "
        "join fanned out within this bite. dfp/bhv/cohort bites are all supposed to be unique on "
        "clnt_no; re-check those." % (_bite, _n_long_b_bite, _n_expected_bite, _cohort_bite_n, len(T_OFFSETS_B)))
    _n_long_b_total += _n_long_b_bite

    _b_partials.append(
        _long_b_bite
        .groupBy("t_offset", "spend_tier", "spend_tier_at_offset", "usg_bhvr_seg", "usg_bhvr_seg_t0")
        .agg(F.sum(F.when(_stay, 1).otherwise(0)).alias("stayers"),
             F.sum(F.when(_leave, 1).otherwise(0)).alias("leavers"),
             F.sum(F.when(_leave & (F.col("cards_unsub_by_anchor") == 1), 1)
                   .otherwise(0)).alias("leavers_cards_unsub_subset"),
             F.count("*").alias("clients_total")))
    print("PIECE B bite", _bite, "of", N_BITES, ":", _cohort_bite_n, "cohort clients,",
          _n_long_b_bite, "long rows, partial cube built.")

_n_expected_b = COHORT_B_N * len(T_OFFSETS_B)
print("PIECE B LONG TABLE | grain: one row per (clnt_no, t_offset) | summed across bites:",
      _n_long_b_total, "rows | expected", _n_expected_b, "(", COHORT_B_N, "cohort clients x",
      len(T_OFFSETS_B), "offsets )")
assert _n_long_b_total == _n_expected_b, (
    "long_b total row count mismatch: %d vs %d expected - a bite filter missed rows, or a join "
    "fanned out somewhere the per-bite assert didn't catch." % (_n_long_b_total, _n_expected_b))

_B_PARTIAL_SCHEMA = StructType([
    StructField("t_offset", IntegerType(), True),
    StructField("spend_tier", StringType(), True),
    StructField("spend_tier_at_offset", StringType(), True),
    StructField("usg_bhvr_seg", StringType(), True),
    StructField("usg_bhvr_seg_t0", StringType(), True),
    StructField("stayers", LongType(), True),
    StructField("leavers", LongType(), True),
    StructField("leavers_cards_unsub_subset", LongType(), True),
    StructField("clients_total", LongType(), True),
])

if _b_partials:
    _b_union = _b_partials[0]
    for _p in _b_partials[1:]:
        _b_union = _b_union.unionByName(_p)
else:
    print("WARNING: every bite had zero cohort clients (COHORT_B_N=%d) - shipping an EMPTY "
          "b_before_after_cube. Investigate before trusting this run." % COHORT_B_N)
    _b_union = spark.createDataFrame([], schema=_B_PARTIAL_SCHEMA)

b_before_after_cube = (_b_union
                        .groupBy("t_offset", "spend_tier", "spend_tier_at_offset", "usg_bhvr_seg",
                                 "usg_bhvr_seg_t0")
                        .agg(F.sum("stayers").alias("stayers"),
                             F.sum("leavers").alias("leavers"),
                             F.sum("leavers_cards_unsub_subset").alias("leavers_cards_unsub_subset"),
                             F.sum("clients_total").alias("clients_total"))
                        .orderBy("t_offset", "spend_tier", "spend_tier_at_offset", "usg_bhvr_seg",
                                 "usg_bhvr_seg_t0"))

b_before_after_cube_stamped = _stamp(
    b_before_after_cube,
    "anchor %s, remeasure +12m %s" % (T0_ANCHOR_B.isoformat(), P12_ANCHOR_B.isoformat()),
    "Cards-mailed cohort as of anchor (n=%d)" % COHORT_B_N)

b_pd = b_before_after_cube_stamped.toPandas()
print("B_BEFORE_AFTER_CUBE | grain: one row per (t_offset, spend_tier, spend_tier_at_offset, "
      "usg_bhvr_seg, usg_bhvr_seg_t0) | spend_tier = held fixed at t0 | spend_tier_at_offset = "
      "t0/p12 spend banded on t0 cutpoints (trajectory) | usg_bhvr_seg_t0 = fixed t0 seg (R/T flow "
      "pair with usg_bhvr_seg on t12 rows) | stayers/leavers COLUMNS, cards subset rides beside "
      "leavers | %d rows" % len(b_pd))
print(b_pd.to_string(index=False))

_p12_nodata_share = b_pd[b_pd["t_offset"] == 12]["clients_total"].sum()
_t0_total = b_pd[b_pd["t_offset"] == 0]["clients_total"].sum()
print("\nt12 (%s) coverage check: %d of %d cohort clients have a t12 row (spend_tier/usg_bhvr_seg "
      "default to untiered/no_data where DFP/BHV had no row for that future month)."
      % (P12_ANCHOR_B.isoformat(), _p12_nodata_share, _t0_total))
if T0_ANCHOR_B <= datetime.date.today() < P12_ANCHOR_B:
    print("t12 IS STILL IN THE FUTURE as of this run (today: %s) - t12 rows are arithmetic thinness, "
          "not a real trend. Rerun this file after %s to get a real t12 read."
          % (datetime.date.today().isoformat(), P12_ANCHOR_B.isoformat()))

write_cube(b_before_after_cube_stamped, "b_before_after_cube")


# %% [16] ONE FILE TO DOWNLOAD - bundle all six CSVs into a single xlsx, one sheet each. Verbatim
# pattern from spotlight.py Cell [7]: HDFS is the durable output, this is a delivery convenience.
# TOLERATES a missing/empty b_before_after_cube (pre-Sept-2026 runs, before t12 closes, or any
# other cube landing thin/empty): the filter below drops empty frames from the workbook and NAMES
# every dropped sheet below it, so the run never crashes on a thin cube - it ships what exists.
# ENGINE: PySpark driver + pandas. No new EDW connection.

import os
import subprocess as _sp

_leaf = "unsub_unified_out_smoke" if SMOKE else "unsub_unified_out"
LOCAL_OUT = None
for _cand in ("/home/jovyan", os.path.expanduser("~"), os.getcwd(), "/tmp"):
    try:
        _try = os.path.join(_cand, _leaf)
        os.makedirs(_try, exist_ok=True)
        _t = os.path.join(_try, ".writetest")
        open(_t, "w").write("x")
        os.remove(_t)
        LOCAL_OUT = _try
        break
    except Exception:
        continue

if LOCAL_OUT is None:
    print("No writable local directory found - everything is on HDFS at", OUT_DIR,
          "pull with:  !hdfs dfs -get -f", OUT_DIR + "* .")
else:
    print("Local output dir:", LOCAL_OUT)
    _sheets = {
        "a1_mne_share": a1_mne_share_pd,
        "a2_mne_rates": a2_mne_rates_pd,
        "a3_contact_cube": a3_pd,
        "a4_profile_cube": a4_pd,
        "b_before_after_cube": b_pd,
        "c_monthly_curve": c_pd,
    }
    _sheets_dropped = [k for k, v in _sheets.items() if v is None or len(v) == 0]
    _sheets = {k: v for k, v in _sheets.items() if v is not None and len(v) > 0}
    if _sheets_dropped:
        print("WARN: dropping", len(_sheets_dropped), "empty sheet(s) from the workbook, shipping "
              "the rest -", _sheets_dropped, "- expected for b_before_after_cube before",
              P12_ANCHOR_B.isoformat(), "closes; investigate any OTHER name on this list before "
              "shipping.")
    _xlsx = os.path.join(LOCAL_OUT, "unsub_unified_cubes.xlsx")
    _bundle_names = []          # whatever actually got written - xlsx or fallback CSVs
    try:
        try:
            import openpyxl  # noqa - fail fast with a clear message, engine check
            _engine = "openpyxl"
        except ImportError:
            import xlsxwriter  # noqa - second choice, often present when openpyxl is not
            _engine = "xlsxwriter"
        with pd.ExcelWriter(_xlsx, engine=_engine) as _xl:
            for _name, _df in _sheets.items():
                _df.to_excel(_xl, sheet_name=_name[:31], index=False)
        print("WROTE", _xlsx, "(engine=%s) |" % _engine, os.path.getsize(_xlsx), "bytes")
        for _name, _df in _sheets.items():
            print("   sheet %-24s %6d rows x %d cols" % (_name[:31], len(_df), len(_df.columns)))
        _bundle_names = ["unsub_unified_cubes.xlsx"]
    except Exception as e:
        print("Excel write failed (%s: %s) - falling back to named CSVs." % (type(e).__name__, str(e)[:200]))
        print("   (to get one xlsx next run: pip install openpyxl)")
        for _name, _df in _sheets.items():
            _p = os.path.join(LOCAL_OUT, _name + ".csv")
            _df.to_csv(_p, index=False)
            print("   wrote", _p, "|", len(_df), "rows")
            _bundle_names.append(_name + ".csv")

    # zip whatever exists - xlsx or the fallback CSVs - so ONE DOWNLOAD always prints.
    _zip = os.path.join(LOCAL_OUT, "unsub_unified_cubes.zip")
    _sp.run("cd '%s' && rm -f unsub_unified_cubes.zip && zip -rq unsub_unified_cubes.zip %s"
            % (LOCAL_OUT, " ".join("'%s'" % n for n in _bundle_names)), shell=True)
    if os.path.exists(_zip):
        print("ONE DOWNLOAD:", _zip, "|", round(os.path.getsize(_zip) / 1048576.0, 1), "MB",
              "| contains:", ", ".join(_bundle_names))
    else:
        print("WARN: zip not created - download the files above individually from", LOCAL_OUT)


# %% [17] COVERAGE / RUN SUMMARY - final self-check against the header's coverage table. Print
# only - proves every deliverable file exists with a non-zero row count before Andre trusts the
# run, per the brief's pre-run coverage gate (this is the post-run mirror of it).

print("=" * 90)
print("RUN SUMMARY -", SCRIPT_NAME, "| run_date:", RUN_DATE, "| SMOKE:", SMOKE)
print("=" * 90)
_summary = [
    ("A1", "a1_mne_share.csv", len(a1_mne_share_pd)),
    ("A2", "a2_mne_rates.csv", len(a2_mne_rates_pd)),
    ("A3", "a3_contact_cube.csv", len(a3_pd)),
    ("A4", "a4_profile_cube.csv", len(a4_pd)),
    ("B", "b_before_after_cube.csv", len(b_pd)),
    ("C", "c_monthly_curve.csv", len(c_pd)),
]
for _piece, _fname, _n in _summary:
    _status = "OK" if _n > 0 else "EMPTY - INVESTIGATE BEFORE SHIPPING"
    print("  %-4s %-26s %6d rows  [%s]" % (_piece, _fname, _n, _status))
print()
print("Enterprise unique unsub clients, WIN_A:", _enterprise_unsubs)
print("Cohort B size (Cards-mailed on/before", T0_ANCHOR_B.isoformat(), "):", COHORT_B_N)
print("If SMOKE is True, every count above is roughly a tenth of reality (bite 0 only, 10% of "
      "clients by MOD(ABS(clnt_no), 10)) - do not report these numbers. Flip SMOKE to False in "
      "Cell [0] and rerun once bite-0 shapes match the coverage table in the file header.")
print("All six deliverables land under:", OUT_DIR)
