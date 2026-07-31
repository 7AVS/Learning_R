# unsub_tracking/spotlight/spotlight2.py
#
# Workstream 2: Unsubscription Drivers by LOB and MNE. How unsubscribing relates to cards
# spend, revolver/transactor behavior, and account closure over time.
#
# Standalone file. Does NOT import spotlight.py - any shared fact (CARDS_MNES, the UCP read
# pattern, the accumulator trap) is re-verified and retyped here, cited to its source.
#
# ============================== SCOPE ========================================================
# Target: Spends (High/Mid/Low tercile) x [Revolver/Transactor/Dormant Subs|Unsubs], read at
# t = -3, 0, +3, +6, +9, +12 months relative to the cohort window end. -3 is load-bearing: it is
# what turns a level comparison ("leavers spend less") into a trend comparison ("leavers were
# ALREADY lower-spending before they left").
#
# ============================== VERIFIED FACTS (file:line evidence) ==========================
#
# UNSUB EVENT = DTZV01.VENDOR_FEEDBACK_EVENT.disposition_cd = 4. SEND = disposition_cd = 1.
#   Same table pair and join path as spotlight.py - unsub_tracking/UNSUB_TRACKING_KNOWLEDGE.md:130.
#
# JOIN: EVENT.(consumer_id_hashed, TREATMENT_ID) = MASTER.(consumer_id_hashed, TREATMENT_ID).
#   MASTER carries CLNT_NO - EVENT does not. MNE = SUBSTR(TREATMENT_ID, 8, 3).
#   UNSUB_TRACKING_KNOWLEDGE.md:145-165.
#
# ATTRIBUTION RULE = unsub attaches to the CLOSEST PRECEDING send (any MNE), within 30 days.
#   unsub_tracking/spotlight/RUN_2026-07-31_scope_test.sql (whole file). Verdict on line 5:
#   "the unsubscribe is PER-LIST, not a global fan-out. Per-campaign attribution is real."
#   ~97% of multi-treatment unsubscribers show distinct_ts == n_treatments (line 40), refuting
#   the global-opt-out hypothesis pack 20 had left open. Line 85: a 30-day window captures 71.5%
#   of logged unsub events - stated as coverage, not completeness, per that file's own note.
#
# DFP GRAIN: D3CV12A.DLY_FULL_PORTFOLIO, one row per acct_no x dt_record_ext, event-driven,
#   gaps allowed. clnt_no is DIRECT (decimal(13,0)) - no acct-to-client bridge table needed here,
#   unlike the CRV scripts that bridge through TACTIC keys. dt_record_ext is the real record
#   date - me_dt is a derived calendar tag, not the join/filter column (both confirmed present
#   in the same table per campaigns/PCQ/next_best_card/pcq_q1_26_monthly_balance.sql, which uses
#   dt_record_ext for filtering/ranking and me_dt only as the calendar-month group tag).
#
# ACCUMULATOR: net_prch_amt_mtd is a MONTH-TO-DATE running total that resets on the 1st -
#   NOT a running-forever total. Validated ONCE, already, in this exact repo:
#   pcq_q1_26_monthly_balance.sql CTE 3 (last_net_prch_amt_mtd via ROW_NUMBER rn=1 per
#   (acct_no, me_dt) ordered by dt_record_ext DESC) cross-checked against
#   SUM(net_prch_amt_dly) - see that file's "PAIN POINTS RESOLVED" note [A]. Cell [1] below
#   re-runs the same check as a live blocking gate rather than trusting the prior write-up.
#
# CR_CRD_RPTS_ACCT: D3CV12A.CR_CRD_RPTS_ACCT, column usg_bhvr_seg_at_cyc_cd
#   (Dormant/Transactor/Revolver/null). ACCOUNT-level, no clnt_no column seen in either grep hit
#   below - bridged through DFP's acct_no/clnt_no pairing instead (see UNVERIFIED #2 in the
#   delivery report).
#   Snapshot/date column CONFIRMED = ME_DT (month-end), grepped from two in-repo precedents:
#     - campaigns/CRV/suppression_experiment/c4_demand_shaping.sql:86-99 - joins on
#       "r.me_dt < k.offer_start_date AND r.me_dt >= k.offer_start_date - INTERVAL '70' DAY",
#       ranked by ME_DT DESC to get the most recent PRE-treatment snapshot.
#     - campaigns/CRV/bulletproof_analysis/26_behavior_mix_by_overlap_arm.sql:64-78 - reads
#       "ME_DT BETWEEN DATE '2024-09-30' AND DATE '2026-05-31'", joins on
#       "b.ME_DT = f.treatmt_strt_dt - EXTRACT(DAY FROM f.treatmt_strt_dt)" (month-end before).
#       Line 13's own sanity-check comment: "if higher, CR_CRD_RPTS_ACCT has >1 row per acct x
#       ME_DT and needs a dedup first" - i.e. the source ITSELF flags this as unverified, not
#       resolved. This file dedups defensively (see UNVERIFIED #1) rather than assuming clean.
#
# UCP: /prod/sz/tsz/00172/data/ucp4/, MONTH_END_DATE partition (path segment, not a column),
#   filter trim(CLNT_TYP)=='Personal', snapshot = last CLOSED month. PROF_TOT_ANNUAL, CLNT_NO,
#   T/I/B/C_TOT_CNT all CONFIRMED live 2026-07-27 (references/ucp/field_catalog_personal.md
#   RELIABILITY BANNER). PROF_TOT_ANNUAL has NO confirmed definition (references/ucp/gotchas.md
#   #8) and there is no cards-specific profit field anywhere - every use below is named/labeled
#   "_ucp_proxy" so nobody downstream mistakes it for a vetted LTV figure.
#
# CARDS_MNES (12) - retyped VERBATIM from spotlight.py (Andre, 2026-07-31, sourced from
#   unsub_tracking/museum/unsub_value_museum.py:119-120). Used ONLY to tag is_cards_exposed
#   client-side, never as a SQL filter. Kept in manual sync with spotlight.py - see UNVERIFIED #6.
#
# ============================== UNVERIFIED - see delivery report for the full list ===========
# 1. CR_CRD_RPTS_ACCT >1 row per (acct_no, ME_DT): flagged by its own prior users, never
#    resolved. Dedup here takes MAX(usg_bhvr_seg_at_cyc_cd) per (acct_no, ME_DT) - an arbitrary,
#    deterministic tiebreak, not a documented rule.
# 2. CR_CRD_RPTS_ACCT has no clnt_no in any grep hit found - bridged through DFP's acct_no set
#    for the same cohort/window. Not independently confirmed complete.
# 3. Client-level revolver/transactor rollup (a client can hold >1 account): precedence
#    Revolver > Transactor > Dormant > no_data, picked here with zero prior in-repo precedent
#    (references/ucp/gotchas.md #7 already flags "no house standard" for band cuts generally -
#    this extends that same gap to behavior-segment precedence).
# 4. Closure uses acct_cls_dt only (permanent once set, per RUN_THIS_dfp_only.py's "status is
#    one-way" comment) - does NOT cross-check the `status` code. If acct_cls_dt is ever null for
#    a WOFF/BKPT/COLL/FRD account, that account reads as still-open.
# 5. Closure has no account-OPEN date pulled, so an account opened after an early t_offset
#    (e.g. t=-3) cannot be distinguished from one that already existed and stayed open - both
#    read "open." Directional, not exact, at early offsets.
# 6. is_cards_exposed's CARDS_MNES list is a manual retype of spotlight.py's set, not a shared
#    import - a future edit to one and not the other silently diverges the two files.
# 7. UCP is ONE fixed month-end snapshot applied to every t_offset (same simplification as
#    spotlight.py) even though this file's whole point is trend-over-time; prod_cat_cnt and the
#    profit proxy do not actually move with the client the way spend/behavior/closure do here.
# 8. Attribution tie-break (two sends at the identical timestamp): ROW_NUMBER ORDER BY
#    send_ts DESC has no secondary sort key, so Teradata's own row order decides ties.
#
# ==============================================================================================


# %% [0] CONFIG - every tunable lives here.

import calendar
import datetime

# ---- Cohort window: all clients emailed (disposition_cd=1) here, bank-wide, all mnemonics ----
WIN_START = "2026-04-01"
WIN_END   = "2026-07-01"     # matches RUN_2026-07-31_scope_test.sql's validated window exactly
WIN_START_DATE = datetime.date(2026, 4, 1)
WIN_END_DATE   = datetime.date(2026, 7, 1)
assert WIN_START_DATE >= datetime.date(2024, 1, 1), "repo floor is 2024-01-01 - do not go below"

# ---- Attribution ----
RESPONSE_DAYS = 30   # unsub attaches to the closest PRECEDING send within this many days.
                     # RUN_2026-07-31_scope_test.sql: 30 days covers 71.5% of logged unsub
                     # events - state coverage, don't imply completeness.


def _add_months(d, n):
    total = d.year * 12 + (d.month - 1) + n
    y, m = divmod(total, 12)
    m += 1
    day = min(d.day, calendar.monthrange(y, m)[1])
    return datetime.date(y, m, day)


def _month_end(d):
    return datetime.date(d.year, d.month, calendar.monthrange(d.year, d.month)[1])


ATTR_SEND_START = (WIN_START_DATE - datetime.timedelta(days=RESPONSE_DAYS)).isoformat()

# ---- Time points, months relative to WIN_END. -3 is load-bearing (pre-period, not optional) ----
T_OFFSETS = [-3, 0, 3, 6, 9, 12]
T_OFFSET_ANCHOR_DATE = {o: _month_end(_add_months(WIN_END_DATE, o)) for o in T_OFFSETS}   # month-end dt
T_OFFSET_YM = {o: _add_months(WIN_END_DATE, o).strftime("%Y%m") for o in T_OFFSETS}        # for spend join

# ---- Spend tier: tercile of ANNUAL spend, cut ONCE on the whole cohort at t=0, held FIXED
# across every t_offset. "Annual" = trailing 12 calendar months ending at t=0 (months t=-11..0
# inclusive). This is an editable choice, not a house standard - no prior in-repo precedent for
# what "annual spend" means at a point in time; trailing-12-ending-at-t=0 was picked because it
# is the only window that doesn't require future data relative to the cut point itself.
ANNUAL_LOOKBACK_MONTHS = 12
ANNUAL_SPEND_YMS = [_add_months(WIN_END_DATE, o).strftime("%Y%m")
                    for o in range(-(ANNUAL_LOOKBACK_MONTHS - 1), 1)]   # t=-11 .. t=0, 12 months

# ---- DFP / CR_CRD_RPTS_ACCT scan window: must cover every t_offset PLUS the annual lookback ----
_month_range_start_offset = min(T_OFFSETS + [-(ANNUAL_LOOKBACK_MONTHS - 1)])
_month_range_end_offset = max(T_OFFSETS)
_spend_floor_date = _add_months(WIN_END_DATE.replace(day=1), _month_range_start_offset)
if _spend_floor_date < datetime.date(2024, 1, 1):
    print("NOTE: computed spend/behavior floor %s is below the 2024-01-01 repo floor - clamping. "
          "This shortens the annual-spend lookback and the t=-3 pre-period for this run."
          % _spend_floor_date.isoformat())
    _spend_floor_date = datetime.date(2024, 1, 1)
SPEND_FLOOR = _spend_floor_date.isoformat()
SPEND_CEILING = _add_months(_month_end(_add_months(WIN_END_DATE, _month_range_end_offset)), 1).replace(day=1).isoformat()
# ^ first day of the month AFTER the last t_offset's month-end - exclusive upper bound.
# NOTE: t=+12's anchor date is in the future relative to any recent WIN_END - that bite of the
# pull will legitimately come back thin (no rows past "today"), not broken. Expected, not a bug.

# ---- MNE tag set - CLIENT-SIDE tag only, retyped verbatim from spotlight.py (UNVERIFIED #6) ----
CARDS_MNES = frozenset({"PCQ", "PCL", "PCD", "AUH", "CLI", "CRV",
                        "VBA", "VBU", "CRO", "CEC", "VIF", "MET"})
assert len(CARDS_MNES) == 12, "CARDS_MNES should hold exactly 12 MNEs - recount vs spotlight.py"

# ---- Bite plan ----
N_BITES = 10   # MOD(CLNT_NO, N_BITES) on the VENDOR_FEEDBACK side; MOD(ABS(clnt_no), N_BITES) on
               # the DFP side (ABS matches RUN_THIS_dfp_only.py's defensive precedent).
SMOKE = True   # True -> bite 0 only. Flip after checking bite-0 row counts and the accumulator gate.

# ---- Run switches ----
DROP_CLIENT_GRAIN = False   # Cell [10]. Refuses to run unless the cubes are already landed.

# ---- Paths ----
BASE = "hdfs:///user/427966379/unsub_spotlight2/"
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"

# Schema version in every landed path - a column change lands in a fresh directory, never
# silently reuses stale parquet from a prior design (spotlight.py's own documented bug).
SCHEMA_VERSION = 1
EXPOSURE_DIR = BASE + "exposure_v%d/" % SCHEMA_VERSION
SPEND_DIR    = BASE + "spend_v%d/" % SCHEMA_VERSION
CLOSURE_DIR  = BASE + "closure_v%d/" % SCHEMA_VERSION
BEHAVIOR_DIR = BASE + "behavior_v%d/" % SCHEMA_VERSION
UCP_DIR      = BASE + "ucp_enriched_v%d/" % SCHEMA_VERSION
LONG_DIR     = BASE + "client_long_v%d/" % SCHEMA_VERSION
OUT_CUBE     = BASE + "out/cube_main_v%d" % SCHEMA_VERSION

EXPOSURE_COLS = ["clnt_no", "mne", "n_sends_in_window", "first_send_dt", "last_send_dt",
                 "n_campaigns_in_window", "unsub_flag", "unsub_dt", "trigger_mne"]
SPEND_COLS = ["clnt_no", "ym", "client_month_spend"]
CLOSURE_COLS = ["clnt_no", "acct_no", "acct_cls_dt"]
BEHAVIOR_COLS = ["acct_no", "me_dt", "usg_bhvr_seg"]

print("CONFIG loaded - window:", WIN_START, "to", WIN_END, "| attribution lookback from:",
      ATTR_SEND_START, "| t_offsets (months):", T_OFFSETS)
print("DFP/behavior scan range:", SPEND_FLOOR, "to", SPEND_CEILING, "| annual-spend YMs:",
      ANNUAL_SPEND_YMS)
print("bites:", N_BITES, "| SMOKE:", SMOKE, "| SCHEMA_VERSION:", SCHEMA_VERSION)


# %% [1] ACCUMULATOR VALIDATION GATE - blocking, runs BEFORE anything depends on net_prch_amt_mtd.
# ENGINE: Teradata-direct (D3CV12A.DLY_FULL_PORTFOLIO, no catalog prefix). Establishes the ONE
# EDW connection reused by Cells [2]-[5] in this same kernel session.
#
# Method: pick 20 accounts active on day 1 of a known-closed calendar month (the month before
# WIN_END - unambiguously in the past). For that month, compare SUM(net_prch_amt_dly) against
# the LAST record's net_prch_amt_mtd per account (same ranked pattern as
# pcq_q1_26_monthly_balance.sql CTE 3). If net_prch_amt_mtd resets on the 1st, the two match.
# If it is actually year-to-date, the MTD figure is 3-12x too large depending on the month -
# that is a hard error, not a warning, because every monthly spend figure downstream depends on it.

import getpass
import time
import pandas as pd
import teradatasql

if not hasattr(pd.DataFrame, "iteritems"):
    pd.DataFrame.iteritems = pd.DataFrame.items

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


def _landed(path):
    try:
        spark.read.parquet(path).limit(1).collect()
        return True
    except Exception as e:
        msg = str(e).lower()
        if any(s in msg for s in ("path does not exist", "path_not_found", "filenotfound",
                                  "unable to infer schema")):
            return False
        raise RuntimeError(path + ": cannot verify HDFS state, refusing to guess. " + str(e)[:300])


def land_bite(bite_dir, bite, sql):
    """Shared bite-and-land helper for Cells [2]-[5]. Skips if this bite already landed."""
    path = bite_dir + "bite_%d" % bite
    if _landed(path):
        n = spark.read.parquet(path).count()
        print(path, ": already landed,", n, "rows - SKIP")
        return
    pdf = edw_pd(sql)
    assert len(pdf) > 0, path + " pulled zero rows - investigate before proceeding (a thin " \
        "future-dated t_offset can shrink a bite but should not zero it entirely)"
    sdf = spark.createDataFrame(pdf)
    sdf.write.mode("overwrite").parquet(path)
    nback = spark.read.parquet(path).count()
    assert nback == len(pdf), path + " HDFS readback mismatch: pulled %d, read back %d" % (len(pdf), nback)
    print(path, ": landed", len(pdf), "rows, HDFS readback confirms", nback)


_val_month_start = _add_months(WIN_END_DATE, -1).replace(day=1).isoformat()
_val_month_end = WIN_END_DATE.replace(day=1).isoformat()

_val_sql = """
WITH day1_accts AS (
    SELECT TOP 20 acct_no
    FROM D3CV12A.DLY_FULL_PORTFOLIO
    WHERE dt_record_ext = DATE '%s'
    ORDER BY acct_no
),
month_rows AS (
    SELECT p.acct_no, p.dt_record_ext, p.net_prch_amt_dly, p.net_prch_amt_mtd
    FROM D3CV12A.DLY_FULL_PORTFOLIO p
    INNER JOIN day1_accts d ON d.acct_no = p.acct_no
    WHERE p.dt_record_ext >= DATE '%s'
      AND p.dt_record_ext <  DATE '%s'
)
SELECT acct_no,
       SUM(CAST(net_prch_amt_dly AS FLOAT)) AS sum_dly,
       MAX(CASE WHEN dt_record_ext = maxdt THEN net_prch_amt_mtd END) AS last_mtd
FROM (
    SELECT acct_no, dt_record_ext, net_prch_amt_dly, net_prch_amt_mtd,
           MAX(dt_record_ext) OVER (PARTITION BY acct_no) AS maxdt
    FROM month_rows
) x
GROUP BY acct_no
""" % (_val_month_start, _val_month_start, _val_month_end)

_val_pdf = edw_pd(_val_sql)
assert len(_val_pdf) > 0, "accumulator validation pulled zero accounts for %s - pick a different " \
    "validation month, this one may have no activity" % _val_month_start

_val_pdf["sum_dly"] = pd.to_numeric(_val_pdf["sum_dly"], errors="coerce")
_val_pdf["last_mtd"] = pd.to_numeric(_val_pdf["last_mtd"], errors="coerce")
_val_pdf["ratio"] = _val_pdf["last_mtd"] / _val_pdf["sum_dly"].replace(0, pd.NA)
print("ACCUMULATOR CHECK - %s sample accounts, month %s | grain: one row per acct_no | %d rows"
      % (len(_val_pdf), _val_month_start, len(_val_pdf)))
print(_val_pdf.to_string(index=False))

_bad = _val_pdf[(_val_pdf["sum_dly"].abs() > 1) & (_val_pdf["ratio"].abs() > 3)]
if len(_bad) > 0:
    raise RuntimeError(
        "ACCUMULATOR CHECK FAILED - net_prch_amt_mtd does not reset monthly for %d of %d sampled "
        "accounts (last_mtd is >3x sum_dly). This looks like a YEAR-to-date accumulator, not "
        "month-to-date - every monthly spend figure in Cells [3]/[7]/[9] would be wrong until "
        "this is fixed. Failing rows:\n%s"
        % (len(_bad), len(_val_pdf), _bad.to_string(index=False))
    )

ACCUM_VALIDATED = True
print("ACCUMULATOR CHECK PASSED for all %d sampled accounts - net_prch_amt_mtd resets monthly, "
      "last-of-month MTD matches SUM(daily deltas) within tolerance. Monthly spend method is sound."
      % len(_val_pdf))


# %% [2] PULL EXPOSURE + UNSUB ATTRIBUTION - one row per (clnt_no, mne), bank-wide, bitten.
# ENGINE: Teradata-direct, reuses EDW/edw_pd/land_bite from Cell [1].
#
# trigger_mne / unsub_flag / unsub_dt / n_campaigns_in_window are CLIENT-level facts, denormalized
# onto every mne row for that client (same "stamp cohort facts on every row" pattern as
# pcq_q1_26_monthly_balance.sql's cohort_accts). Collapsed to client grain in Cell [8].

assert "ACCUM_VALIDATED" in globals() and ACCUM_VALIDATED, "Run Cell [1] first - it validates " \
    "the accumulator this file depends on."


def _exposure_sql(bite):
    return """
    WITH sends_member AS (
        SELECT DISTINCT m.CLNT_NO AS clnt_no, ek.consumer_id_hashed,
               SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne,
               CAST(ek.disposition_dt_tm AS TIMESTAMP(0)) AS send_ts
        FROM DTZV01.VENDOR_FEEDBACK_EVENT ek
        INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
          ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
        WHERE ek.disposition_cd = 1
          AND ek.disposition_dt_tm >= DATE '%(win_start)s'
          AND ek.disposition_dt_tm <  DATE '%(win_end)s'
          AND MOD(m.CLNT_NO, %(n_bites)d) = %(bite)d
    ),
    sends_lookback AS (
        SELECT DISTINCT m.CLNT_NO AS clnt_no, ek.consumer_id_hashed,
               SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne,
               CAST(ek.disposition_dt_tm AS TIMESTAMP(0)) AS send_ts
        FROM DTZV01.VENDOR_FEEDBACK_EVENT ek
        INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
          ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
        WHERE ek.disposition_cd = 1
          AND ek.disposition_dt_tm >= DATE '%(attr_start)s'
          AND ek.disposition_dt_tm <  DATE '%(win_end)s'
          AND MOD(m.CLNT_NO, %(n_bites)d) = %(bite)d
    ),
    unsubs_first AS (
        SELECT clnt_no, consumer_id_hashed, unsub_ts
        FROM (
            SELECT m.CLNT_NO AS clnt_no, ek.consumer_id_hashed,
                   CAST(ek.disposition_dt_tm AS TIMESTAMP(0)) AS unsub_ts,
                   ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO ORDER BY ek.disposition_dt_tm ASC) AS rn
            FROM DTZV01.VENDOR_FEEDBACK_EVENT ek
            INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
              ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
            WHERE ek.disposition_cd = 4
              AND ek.disposition_dt_tm >= DATE '%(win_start)s'
              AND ek.disposition_dt_tm <  DATE '%(win_end)s'
              AND MOD(m.CLNT_NO, %(n_bites)d) = %(bite)d
        ) z
        WHERE rn = 1
    ),
    attributed AS (
        SELECT clnt_no, trigger_mne
        FROM (
            SELECT u.clnt_no, l.mne AS trigger_mne,
                   ROW_NUMBER() OVER (PARTITION BY u.clnt_no ORDER BY l.send_ts DESC) AS rn
            FROM unsubs_first u
            INNER JOIN sends_lookback l
              ON l.consumer_id_hashed = u.consumer_id_hashed
             AND l.send_ts <  u.unsub_ts
             AND l.send_ts >= u.unsub_ts - INTERVAL '%(response_days)d' DAY
        ) y
        WHERE rn = 1
    ),
    member_agg AS (
        SELECT clnt_no, mne, COUNT(*) AS n_sends_in_window,
               MIN(send_ts) AS first_send_dt, MAX(send_ts) AS last_send_dt
        FROM sends_member
        GROUP BY clnt_no, mne
    ),
    client_campaigns AS (
        SELECT clnt_no, COUNT(DISTINCT mne) AS n_campaigns_in_window
        FROM sends_member
        GROUP BY clnt_no
    )
    SELECT ma.clnt_no, ma.mne, ma.n_sends_in_window, ma.first_send_dt, ma.last_send_dt,
           cc.n_campaigns_in_window,
           CASE WHEN u.clnt_no IS NOT NULL THEN 1 ELSE 0 END AS unsub_flag,
           u.unsub_ts AS unsub_dt,
           a.trigger_mne
    FROM member_agg ma
    INNER JOIN client_campaigns cc ON cc.clnt_no = ma.clnt_no
    LEFT JOIN unsubs_first u ON u.clnt_no = ma.clnt_no
    LEFT JOIN attributed a ON a.clnt_no = ma.clnt_no
    """ % {"win_start": WIN_START, "win_end": WIN_END, "attr_start": ATTR_SEND_START,
           "n_bites": N_BITES, "bite": bite, "response_days": RESPONSE_DAYS}


for _b in (range(1) if SMOKE else range(N_BITES)):
    land_bite(EXPOSURE_DIR, _b, _exposure_sql(_b))

print("Cell [2] done - exposure grain landed at", EXPOSURE_DIR + "*",
      "| one row per (clnt_no, mne), client-level attribution facts denormalized on every row.")


# %% [3] PULL CLIENT MONTHLY SPEND - one row per (clnt_no, ym), bitten. ENGINE: Teradata-direct.
#
# One row per (acct_no, month) at MAX(dt_record_ext) within the month holds that month's true
# total (Cell [1] just proved this). Summed across a client's accounts -> client_month_spend.
# No row for a (clnt_no, ym) means zero spend that month (event-driven gaps) - NOT an exclusion,
# handled downstream via coalesce-to-zero, never dropped.

def _spend_sql(bite):
    return """
    WITH cohort_bite AS (
        SELECT DISTINCT m.CLNT_NO AS clnt_no
        FROM DTZV01.VENDOR_FEEDBACK_EVENT ek
        INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
          ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
        WHERE ek.disposition_cd = 1
          AND ek.disposition_dt_tm >= DATE '%(win_start)s'
          AND ek.disposition_dt_tm <  DATE '%(win_end)s'
          AND MOD(m.CLNT_NO, %(n_bites)d) = %(bite)d
    ),
    dfp_scoped AS (
        SELECT p.clnt_no, p.acct_no, p.dt_record_ext, p.net_prch_amt_mtd
        FROM D3CV12A.DLY_FULL_PORTFOLIO p
        INNER JOIN cohort_bite c ON c.clnt_no = p.clnt_no
        WHERE p.dt_record_ext >= DATE '%(spend_floor)s'
          AND p.dt_record_ext <  DATE '%(spend_ceiling)s'
          AND MOD(ABS(p.clnt_no), %(n_bites)d) = %(bite)d
    ),
    ranked AS (
        SELECT clnt_no, acct_no, net_prch_amt_mtd,
               EXTRACT(YEAR FROM dt_record_ext) * 100 + EXTRACT(MONTH FROM dt_record_ext) AS ym,
               ROW_NUMBER() OVER (
                   PARTITION BY acct_no,
                                EXTRACT(YEAR FROM dt_record_ext) * 100 + EXTRACT(MONTH FROM dt_record_ext)
                   ORDER BY dt_record_ext DESC) AS rn
        FROM dfp_scoped
    ),
    acct_month AS (
        SELECT clnt_no, acct_no, ym, net_prch_amt_mtd AS acct_month_spend
        FROM ranked
        WHERE rn = 1
    )
    SELECT clnt_no, CAST(ym AS VARCHAR(6)) AS ym,
           SUM(CAST(acct_month_spend AS FLOAT)) AS client_month_spend
    FROM acct_month
    GROUP BY clnt_no, ym
    """ % {"win_start": WIN_START, "win_end": WIN_END, "spend_floor": SPEND_FLOOR,
           "spend_ceiling": SPEND_CEILING, "n_bites": N_BITES, "bite": bite}


for _b in (range(1) if SMOKE else range(N_BITES)):
    land_bite(SPEND_DIR, _b, _spend_sql(_b))

print("Cell [3] done - client monthly spend landed at", SPEND_DIR + "*",
      "| one row per (clnt_no, ym) with activity - gaps mean zero spend, filled downstream.")


# %% [4] PULL ACCOUNT CLOSURE STATE - one row per (clnt_no, acct_no), bitten. ENGINE: Teradata-direct.
#
# acct_cls_dt is a PERMANENT flag once set (RUN_THIS_dfp_only.py: "status is one-way ... once an
# account goes non-OPEN it stays non-OPEN"). MAX() here is defensive collapse of the daily rows
# to one value per account, not a real MAX-over-time (the value should already be constant).
# See UNVERIFIED #4/#5 in the file header for what this does NOT capture.

def _closure_sql(bite):
    return """
    WITH cohort_bite AS (
        SELECT DISTINCT m.CLNT_NO AS clnt_no
        FROM DTZV01.VENDOR_FEEDBACK_EVENT ek
        INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
          ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
        WHERE ek.disposition_cd = 1
          AND ek.disposition_dt_tm >= DATE '%(win_start)s'
          AND ek.disposition_dt_tm <  DATE '%(win_end)s'
          AND MOD(m.CLNT_NO, %(n_bites)d) = %(bite)d
    ),
    dfp_scoped AS (
        SELECT p.clnt_no, p.acct_no, p.acct_cls_dt
        FROM D3CV12A.DLY_FULL_PORTFOLIO p
        INNER JOIN cohort_bite c ON c.clnt_no = p.clnt_no
        WHERE p.dt_record_ext >= DATE '%(spend_floor)s'
          AND p.dt_record_ext <  DATE '%(spend_ceiling)s'
          AND MOD(ABS(p.clnt_no), %(n_bites)d) = %(bite)d
    )
    SELECT clnt_no, acct_no, MAX(acct_cls_dt) AS acct_cls_dt
    FROM dfp_scoped
    GROUP BY clnt_no, acct_no
    """ % {"win_start": WIN_START, "win_end": WIN_END, "spend_floor": SPEND_FLOOR,
           "spend_ceiling": SPEND_CEILING, "n_bites": N_BITES, "bite": bite}


for _b in (range(1) if SMOKE else range(N_BITES)):
    land_bite(CLOSURE_DIR, _b, _closure_sql(_b))

print("Cell [4] done - account closure state landed at", CLOSURE_DIR + "*",
      "| one row per (clnt_no, acct_no). NULL acct_cls_dt = never closed in the scan window.")


# %% [5] PULL REVOLVER/TRANSACTOR BEHAVIOR - one row per (acct_no, me_dt), bitten. ENGINE: Teradata-direct.
#
# Bridged through DFP's acct_no set for this bite (UNVERIFIED #2 - CR_CRD_RPTS_ACCT itself has
# no clnt_no). MAX(usg_bhvr_seg_at_cyc_cd) collapses any duplicate (acct_no, ME_DT) rows -
# arbitrary tiebreak (UNVERIFIED #1), not a documented rule. Per-t_offset as-of matching happens
# in Spark (Cell [8]) - this cell lands the raw monthly segment history, undecided by time point.

def _behavior_sql(bite):
    return """
    WITH cohort_bite AS (
        SELECT DISTINCT m.CLNT_NO AS clnt_no
        FROM DTZV01.VENDOR_FEEDBACK_EVENT ek
        INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
          ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
        WHERE ek.disposition_cd = 1
          AND ek.disposition_dt_tm >= DATE '%(win_start)s'
          AND ek.disposition_dt_tm <  DATE '%(win_end)s'
          AND MOD(m.CLNT_NO, %(n_bites)d) = %(bite)d
    ),
    cohort_accts AS (
        SELECT DISTINCT p.acct_no
        FROM D3CV12A.DLY_FULL_PORTFOLIO p
        INNER JOIN cohort_bite c ON c.clnt_no = p.clnt_no
        WHERE p.dt_record_ext >= DATE '%(spend_floor)s'
          AND p.dt_record_ext <  DATE '%(spend_ceiling)s'
          AND MOD(ABS(p.clnt_no), %(n_bites)d) = %(bite)d
    )
    SELECT r.acct_no, r.ME_DT AS me_dt, MAX(r.usg_bhvr_seg_at_cyc_cd) AS usg_bhvr_seg
    FROM D3CV12A.CR_CRD_RPTS_ACCT r
    INNER JOIN cohort_accts a ON a.acct_no = r.acct_no
    WHERE r.ME_DT >= DATE '%(spend_floor)s'
      AND r.ME_DT <  DATE '%(spend_ceiling)s'
    GROUP BY r.acct_no, r.ME_DT
    """ % {"win_start": WIN_START, "win_end": WIN_END, "spend_floor": SPEND_FLOOR,
           "spend_ceiling": SPEND_CEILING, "n_bites": N_BITES, "bite": bite}


for _b in (range(1) if SMOKE else range(N_BITES)):
    land_bite(BEHAVIOR_DIR, _b, _behavior_sql(_b))

print("Cell [5] done - behavior history landed at", BEHAVIOR_DIR + "*",
      "| one row per (acct_no, me_dt). As-of matching to each t_offset happens in Cell [8].")


# %% [6] READ HELPERS - loud schema asserts, mirrors spotlight.py's read_base(). ENGINE: PySpark.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


def _read(dir_path, cols, label):
    sdf = spark.read.parquet(dir_path + "*")
    missing = [c for c in cols if c not in sdf.columns]
    if missing:
        raise RuntimeError(
            "%s at %s is missing %s. Found: %s. Stale/short schema - delete %s and rerun its "
            "pull cell." % (label, dir_path, missing, sdf.columns, dir_path))
    return sdf


def read_exposure():
    return (_read(EXPOSURE_DIR, EXPOSURE_COLS, "exposure")
            .withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long")))


def read_spend():
    return (_read(SPEND_DIR, SPEND_COLS, "spend")
            .withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))
            .withColumn("client_month_spend", F.col("client_month_spend").cast("double")))


def read_closure():
    return (_read(CLOSURE_DIR, CLOSURE_COLS, "closure")
            .withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long")))


def read_behavior():
    return _read(BEHAVIOR_DIR, BEHAVIOR_COLS, "behavior")


print("Cell [6] done - read_exposure/read_spend/read_closure/read_behavior ready.")


# %% [7] UCP PROFIT PROXY JOIN - snapshot = last CLOSED month, CLNT_TYP='Personal'.
# ENGINE: PySpark (YARN) reading HDFS parquet - not Trino, not Teradata.
# Pattern per references/ucp/README.md "Canonical read pattern" + gotchas.md #1/#3/#4.

import datetime as _dt

_ucp_anchor_date = _dt.date.today().replace(day=1) - _dt.timedelta(days=1)   # last closed month-end
_ucp_anchor = _ucp_anchor_date.strftime("%Y-%m-%d")
_ucp_path = UCP_BASE + "MONTH_END_DATE=" + _ucp_anchor
_TIBC_COLS = ["T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT"]
_UCP_COLS = ["CLNT_NO", "CLNT_TYP", "PROF_TOT_ANNUAL"] + _TIBC_COLS

_ucp_raw = spark.read.option("basePath", UCP_BASE).parquet(_ucp_path)
_missing = [c for c in _UCP_COLS if c not in _ucp_raw.columns]
assert not _missing, "UCP missing required columns at " + _ucp_anchor + ": " + str(_missing)
print("UCP SCHEMA PROBE at", _ucp_anchor, "- all required columns present:", _UCP_COLS)

_held = [(F.coalesce(F.col(c), F.lit(0)) > 0).cast("int") for c in _TIBC_COLS]

ucp_sel = (_ucp_raw
           .filter(F.trim(F.col("CLNT_TYP")) == "Personal")
           .select(*_UCP_COLS)
           .withColumn("clnt_no", F.col("CLNT_NO").cast("decimal(18,0)").cast("long"))
           .withColumn("prod_cat_cnt", (_held[0] + _held[1] + _held[2] + _held[3]).cast("string"))
           .withColumnRenamed("PROF_TOT_ANNUAL", "prof_annual_ucp_proxy")
           .select("clnt_no", "prod_cat_cnt", "prof_annual_ucp_proxy"))

_cohort_ids = read_exposure().select("clnt_no").distinct()
_left_n = _cohort_ids.count()
_matched_n = _cohort_ids.join(ucp_sel.select("clnt_no").distinct(), "clnt_no", "inner").count()
_match_pct = 100.0 * _matched_n / _left_n if _left_n else 0.0
print("UCP JOIN MATCH RATE - cohort clients:", _left_n, "| matched to UCP:", _matched_n,
      "| match pct: %.1f%%" % _match_pct)
print("Sample cohort clnt_no (5):", [r.clnt_no for r in _cohort_ids.limit(5).collect()])
print("Sample UCP clnt_no (5):", [r.clnt_no for r in ucp_sel.select("clnt_no").limit(5).collect()])
print("Below ~70%% would look like a broken join key rather than ordinary attrition - UCP is "
      "personal-only on one snapshot month, see spotlight.py Cell [4] for the same floor logic. "
      "No hard assert here (directional file, not the profit-critical one) - read the number.")

ucp_enriched = (_cohort_ids
                 .join(ucp_sel, "clnt_no", "left")
                 .withColumn("prod_cat_cnt", F.coalesce(F.col("prod_cat_cnt"), F.lit("no_ucp_match")))
                 .withColumn("prof_annual_ucp_proxy", F.col("prof_annual_ucp_proxy").cast("double")))

ucp_enriched.write.mode("overwrite").parquet(UCP_DIR)
_n_ucp = spark.read.parquet(UCP_DIR).count()
print("Cell [7] done - ucp_enriched landed at", UCP_DIR, "| one row per cohort clnt_no |",
      _n_ucp, "rows. prod_cat_cnt/prof_annual_ucp_proxy are a SINGLE fixed snapshot - see "
      "UNVERIFIED #7.")


# %% [8] BUILD CLIENT x T_OFFSET LONG TABLE - the join layer. Nothing here is the deliverable;
# Cell [9] aggregates this into the cube and this table gets dropped in Cell [10].
# ENGINE: PySpark (YARN).

exposure = read_exposure()
spend = read_spend()
closure = read_closure()
behavior = read_behavior()
ucp_enriched = spark.read.parquet(UCP_DIR)

# ---- client-level exposure rollup ----
_cards_u = F.upper(F.trim(F.col("mne")))
exposure_tagged = exposure.withColumn("is_cards_mne", F.when(_cards_u.isin(sorted(CARDS_MNES)), 1).otherwise(0))
client_exposure = (exposure_tagged.groupBy("clnt_no")
                    .agg(F.max("unsub_flag").alias("unsub_flag"),
                         F.max("n_campaigns_in_window").alias("n_campaigns_in_window"),
                         F.max("is_cards_mne").alias("is_cards_exposed"),
                         F.max("trigger_mne").alias("trigger_mne"))
                    .withColumn("bucket", F.when(F.col("unsub_flag") == 1, "LEAVER").otherwise("STAYER")))

full_clients = client_exposure.select("clnt_no")

# ---- t_offset anchor spine (t_offset, anchor_date, ym) - tiny, broadcast everywhere ----
_anchors_pd = [(o, T_OFFSET_ANCHOR_DATE[o].isoformat(), T_OFFSET_YM[o]) for o in T_OFFSETS]
anchors = spark.createDataFrame(_anchors_pd, ["t_offset", "anchor_dt", "ym"])
anchors = anchors.withColumn("anchor_dt", F.col("anchor_dt").cast("date"))

spine = full_clients.crossJoin(F.broadcast(anchors)).select("clnt_no", "t_offset", "ym", "anchor_dt")

# ---- spend per t_offset: exact ym match, gaps -> 0 ----
spend_by_offset = (spine.join(spend, ["clnt_no", "ym"], "left")
                   .withColumn("client_month_spend", F.coalesce(F.col("client_month_spend"), F.lit(0.0)))
                   .select("clnt_no", "t_offset", "client_month_spend"))

# ---- annual spend at t=0 for the tier cut (Cell [9] cuts on this) ----
annual_spend = (spend.filter(F.col("ym").isin(ANNUAL_SPEND_YMS))
                 .groupBy("clnt_no").agg(F.sum("client_month_spend").alias("annual_spend")))
annual_full = (full_clients.join(annual_spend, "clnt_no", "left")
               .withColumn("annual_spend", F.coalesce(F.col("annual_spend"), F.lit(0.0))))

# ---- closure per t_offset: client closed iff every one of its accounts is closed by then ----
n_accts_total = closure.groupBy("clnt_no").agg(F.countDistinct("acct_no").alias("n_accts_total"))
closure_anchor = (closure.crossJoin(F.broadcast(anchors.select("t_offset", "anchor_dt").distinct()))
                  .withColumn("acct_closed_by_t",
                              (F.col("acct_cls_dt").isNotNull() & (F.col("acct_cls_dt") <= F.col("anchor_dt"))).cast("int")))
closed_counts = closure_anchor.groupBy("clnt_no", "t_offset").agg(F.sum("acct_closed_by_t").alias("n_closed_by_t"))
closed_flag = (closed_counts.join(n_accts_total, "clnt_no")
               .withColumn("closed_flag", (F.col("n_closed_by_t") == F.col("n_accts_total")).cast("int"))
               .select("clnt_no", "t_offset", "closed_flag"))

# ---- behavior per t_offset: as-of match (most recent me_dt <= anchor), then account -> client
#      rollup by precedence Revolver > Transactor > Dormant > no_data (UNVERIFIED #3) ----
_acct_anchors = anchors.select("t_offset", "anchor_dt").distinct()
_behavior_asof = (behavior.crossJoin(F.broadcast(_acct_anchors))
                  .filter(F.col("me_dt") <= F.col("anchor_dt"))
                  .withColumn("rn", F.row_number().over(
                      Window.partitionBy("acct_no", "t_offset").orderBy(F.col("me_dt").desc())))
                  .filter(F.col("rn") == 1)
                  .select("acct_no", "t_offset", "usg_bhvr_seg"))

_acct_to_client = closure.select("acct_no", "clnt_no").distinct()
_seg_rank = (F.when(F.col("usg_bhvr_seg") == "Revolver", 1)
              .when(F.col("usg_bhvr_seg") == "Transactor", 2)
              .when(F.col("usg_bhvr_seg") == "Dormant", 3)
              .otherwise(4))
behavior_client = (_behavior_asof.join(_acct_to_client, "acct_no")
                   .withColumn("seg_rank", _seg_rank)
                   .withColumn("rn", F.row_number().over(
                       Window.partitionBy("clnt_no", "t_offset").orderBy("seg_rank")))
                   .filter(F.col("rn") == 1)
                   .select("clnt_no", "t_offset", "usg_bhvr_seg"))

# ---- assemble ----
long_tbl = (spine.select("clnt_no", "t_offset")
            .join(spend_by_offset, ["clnt_no", "t_offset"], "left")
            .join(closed_flag, ["clnt_no", "t_offset"], "left")
            .join(behavior_client, ["clnt_no", "t_offset"], "left")
            .join(annual_full.select("clnt_no", "annual_spend"), "clnt_no", "left")
            .join(ucp_enriched.select("clnt_no", "prod_cat_cnt", "prof_annual_ucp_proxy"), "clnt_no", "left")
            .join(client_exposure.select("clnt_no", "bucket", "is_cards_exposed"), "clnt_no", "left")
            .withColumn("client_month_spend", F.coalesce(F.col("client_month_spend"), F.lit(0.0)))
            .withColumn("closed_flag", F.coalesce(F.col("closed_flag"), F.lit(0)))
            .withColumn("usg_bhvr_seg", F.coalesce(F.col("usg_bhvr_seg"), F.lit("no_data"))))

long_tbl.write.mode("overwrite").parquet(LONG_DIR)
_n_long = spark.read.parquet(LONG_DIR).count()
_n_expected = full_clients.count() * len(T_OFFSETS)
print("Cell [8] done - client x t_offset long table landed at", LONG_DIR, "|", _n_long, "rows | "
      "expected", _n_expected, "(clients x", len(T_OFFSETS), "t_offsets)")
assert _n_long == _n_expected, "long table row count mismatch - spine crossJoin fanned out or " \
    "collapsed somewhere. Investigate before Cell [9]."


# %% [9] SPEND TIER CUT + MAIN CUBE - tercile of annual_spend cut ONCE on the whole cohort
# (including zero-spenders - dropping them would inflate the median), held FIXED across every
# t_offset row for that client. ENGINE: PySpark (YARN).

long_tbl = spark.read.parquet(LONG_DIR)

_q1, _q2 = annual_full.approxQuantile("annual_spend", [1.0 / 3.0, 2.0 / 3.0], 0.01)
print("Spend tercile cut points (annual_spend, trailing %d months ending at t=0): Low <= %.2f, "
      "Mid <= %.2f, High above." % (ANNUAL_LOOKBACK_MONTHS, _q1, _q2))

_tier_expr = F.when(F.col("annual_spend") <= _q1, "Low").when(F.col("annual_spend") <= _q2, "Mid").otherwise("High")
spend_tier = annual_full.withColumn("spend_tier", _tier_expr).select("clnt_no", "spend_tier")

cube_src = (long_tbl.join(spend_tier, "clnt_no", "left")
            .withColumn("spend_tier", F.coalesce(F.col("spend_tier"), F.lit("no_ucp_match"))))

_stay = F.col("bucket") == "STAYER"
_leave = F.col("bucket") == "LEAVER"
_has_spend = F.col("client_month_spend") > 0

cube_main = (cube_src
             .groupBy("t_offset", "spend_tier", "usg_bhvr_seg", "prod_cat_cnt", "is_cards_exposed")
             .agg(
                 F.sum(F.when(_stay, 1).otherwise(0)).alias("stayers"),
                 F.sum(F.when(_leave, 1).otherwise(0)).alias("leavers"),
                 F.count("*").alias("clients_total"),
                 F.sum(F.when(_stay & _has_spend, 1).otherwise(0)).alias("stayers_with_spend"),
                 F.sum(F.when(_leave & _has_spend, 1).otherwise(0)).alias("leavers_with_spend"),
                 F.expr("percentile_approx(CASE WHEN bucket = 'STAYER' THEN client_month_spend END, 0.5)")
                  .alias("median_spend_stayers"),
                 F.expr("percentile_approx(CASE WHEN bucket = 'LEAVER' THEN client_month_spend END, 0.5)")
                  .alias("median_spend_leavers"),
                 F.expr("percentile_approx(CASE WHEN bucket = 'STAYER' THEN prof_annual_ucp_proxy END, 0.5)")
                  .alias("median_prof_stayers_ucp_proxy"),
                 F.expr("percentile_approx(CASE WHEN bucket = 'LEAVER' THEN prof_annual_ucp_proxy END, 0.5)")
                  .alias("median_prof_leavers_ucp_proxy"),
                 F.sum(F.when(_stay & (F.col("closed_flag") == 1), 1).otherwise(0)).alias("closed_stayers"),
                 F.sum(F.when(_leave & (F.col("closed_flag") == 1), 1).otherwise(0)).alias("closed_leavers"),
             )
             .orderBy("t_offset", "spend_tier", "usg_bhvr_seg", "prod_cat_cnt", "is_cards_exposed"))

cube_main_pd = cube_main.toPandas()
print("CUBE MAIN | grain: one row per (t_offset, spend_tier, usg_bhvr_seg, prod_cat_cnt, "
      "is_cards_exposed) | stayers/leavers are COLUMNS, no rates | %d rows" % len(cube_main_pd))
print(cube_main_pd.to_string(index=False))

cube_main.coalesce(1).write.mode("overwrite").option("header", True).csv(OUT_CUBE)
print("Cell [9] done - written to HDFS:", OUT_CUBE, "|", len(cube_main_pd), "rows")
if SMOKE:
    print("SMOKE was True - this is bite 0 only, ~1/%d of the bank-wide cohort. Every absolute "
          "count is roughly a tenth of the full-run figure; flip SMOKE and rerun before reporting "
          "real numbers." % N_BITES)


# %% [10] CLEANUP - drop client/account-grain intermediates, keep only the cube.
# NOT automatic. Controlled by DROP_CLIENT_GRAIN in Cell [0]. Refuses to delete unless the cube
# is already landed and readable - same gate shape as spotlight.py Cell [9].

import subprocess

_targets = [EXPOSURE_DIR.rstrip("/"), SPEND_DIR.rstrip("/"), CLOSURE_DIR.rstrip("/"),
            BEHAVIOR_DIR.rstrip("/"), UCP_DIR.rstrip("/"), LONG_DIR.rstrip("/")]

if not DROP_CLIENT_GRAIN:
    print("DROP_CLIENT_GRAIN is False - nothing deleted. Would remove:")
    for _t in _targets:
        print("   ", _t)
    print("\nCheck", OUT_CUBE, "first. Once these are gone, re-cutting means re-pulling from Teradata.")
else:
    try:
        spark.read.option("header", True).csv(OUT_CUBE).limit(1).collect()
        _cube_ok = True
    except Exception:
        _cube_ok = False
    if not _cube_ok:
        raise RuntimeError("Refusing to delete client-grain data - %s is missing or unreadable. "
                           "Rerun Cell [9] first." % OUT_CUBE)

    print("Cube verified present. Deleting client/account-grain intermediates:")
    for _t in _targets:
        _rc = subprocess.run("hdfs dfs -rm -r -skipTrash %s" % _t,
                             shell=True, capture_output=True, text=True, timeout=600)
        print("  ", _t, "-> rc", _rc.returncode, (_rc.stdout or _rc.stderr).strip()[:200])
    print("\nDone. Only the cube remains on HDFS. Nothing at client or account grain persists.")

print("\nWhat stays:", OUT_CUBE)
