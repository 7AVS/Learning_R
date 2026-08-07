# unsub_tracking/spotlight/spotlight2.py
#
# Workstream 2: what an unsub is WORTH and what happens to the client afterwards. Spend,
# revolver/transactor behaviour, product depth, profitability proxy and account closure, read at
# t = -3, 0, +3, +6, +9, +12 months relative to the cohort window end, stayers vs leavers.
#
# The -3 point is load-bearing. Without a pre-period this is a level comparison and the first
# challenge in the room is "were leavers already lower-spending before they left".
#
# ============================================================================================
# 2026-07-31 REWRITE - what changed vs the morning version, and why
# ============================================================================================
# 1. IT NO LONGER RE-PULLS THE VENDOR TABLES. The morning version re-queried
#    DTZV01.VENDOR_FEEDBACK_EVENT + VENDOR_FEEDBACK_MASTER to rebuild its own cohort. That data
#    is ALREADY landed on HDFS by spotlight.py and is read here, not re-pulled:
#      hdfs:///user/427966379/unsub_spotlight/clientagg_v6/bite_*   (one row per client, ~12.3M)
#      hdfs:///user/427966379/unsub_spotlight/cards_pair_v6/bite_*  (one row per client x cards mne)
#    Roughly an hour of Teradata pulling is saved and, more important, the two files now agree by
#    construction instead of by luck - a second pull run minutes later drifts (~1K unsubs/day
#    bank-wide) and the cubes stop reconciling against spotlight.py's.
#
# 2. UCP IS READ AT SIX MONTH-ENDS, NOT ONE. The morning version applied a single UCP snapshot to
#    every t_offset, so profitability and product depth never moved across t = -3 -> +12. For a
#    file whose entire subject is change over time that is a hole. MONTH_END_DATE is a PATH
#    SEGMENT (references/ucp/README.md:56-64,88-97), so six snapshots is six partition reads in
#    ONE Spark job - not six expensive queries.
#
# 3. Every hardening spotlight.py earned the hard way is carried over verbatim: bite predicate
#    INSIDE the joined subquery, MOD(ABS(...)), chunked writes on an explicit StructType,
#    _ROWCOUNT resume markers written INSIDE the bite directory, pandas dtype prep before Spark,
#    UCP dedup + row-count assert, no stray percent signs in SQL, SMOKE default True with a
#    smoke_run stamp, every switch in Cell [0].
#
# 4. What still genuinely needs pulling is exactly TWO things: DFP (spend + closure) and
#    CR_CRD_RPTS_ACCT (revolver/transactor). Both are aggregated server-side to ONE ROW PER
#    CLIENT with the t_offsets pivoted into columns - not one row per client x month, which would
#    have been ~72M rows on the wire for nothing.
#
# ============================================================================================
# VERIFIED FACTS (file:line evidence)
# ============================================================================================
#
# COHORT SOURCE - spotlight.py's landed pulls. clientagg_v6 = one row per clnt_no bank-wide,
#   columns per spotlight.py:347-352 (CLIENTAGG_COLS). cards_pair_v6 = one row per
#   (clnt_no, cards mne), columns per spotlight.py:363-365 (CARDSPAIR_COLS). unsub_flag on both
#   is MAX(disposition_cd = 4) inside the window - spotlight.py:638, :860.
#
# UNSUB EVENT = DTZV01.VENDOR_FEEDBACK_EVENT.disposition_cd = 4; SEND = 1.
#   unsub_tracking/UNSUB_TRACKING_KNOWLEDGE.md:130. Inherited through the landed files - this
#   file never touches those tables itself.
#
# DFP - D3CV12A.DLY_FULL_PORTFOLIO. Every column used here is confirmed in
#   references/table_catalog_notes.md:143-176 (the repo's authoritative empirical inventory -
#   "No formal schema doc exists anywhere in the repo; this is the authoritative empirical
#   inventory", line 140):
#     - grain "acct_no x dt_record_ext (with gaps)", event-driven not truly daily (line 143-146)
#     - dt_record_ext = "actual record/extract date. THE daily date for filtering/joining/curves"
#       (line 155); me_dt = "month-end tag ... NOT the record date" (line 156). Filter and rank on
#       dt_record_ext, never me_dt.
#     - clnt_no decimal(13,0) present DIRECTLY on DFP (line 159) - no acct-to-client bridge needed
#     - acct_cls_dt (line 157), acct_open_dt (line 157)
#     - net_prch_amt_mtd = "cumulative month-to-date through that row's dt_record_ext (last row of
#       month = monthly total)" (line 164-165). net_prch_amt_dly = daily delta (line 169).
#     - status char(4) OPEN/VOL/WOFF/BKPT/COLL/FRD/INV, "Once non-OPEN, stays non-OPEN" (line 177-179)
#
# DFP READ PATTERN - table_catalog_notes.md:147-152 is explicit: "Query in ONE scan + window
#   functions - never multi-scan / multi-subquery on this table in one volatile (blows spool). For
#   per-acct + per-month aggregates use two ROW_NUMBER() windows ... then GROUP BY acct_no and
#   pivot via MAX(CASE...)". Cell [3] follows that pattern exactly: ONE scan, one ROW_NUMBER in a
#   derived table with WHERE rn = 1 (not QUALIFY - Teradata-direct, but the file's convention is
#   ranked-CTE anyway), then two levels of GROUP BY and a CASE pivot. See DEPARTURE note below.
#
# MONTHLY SPEND = the LAST record per (acct_no, month) at MAX(dt_record_ext), reading
#   net_prch_amt_mtd. NOT a SUM of ~30 daily rows. Cell [1] proves the accumulator resets monthly
#   before anything depends on it - if it were year-to-date instead, every monthly figure in this
#   file would be wrong, so that gate raises rather than warns.
#
# BEHAVIOUR - D3CV12A.CR_CRD_RPTS_ACCT, column usg_bhvr_seg_at_cyc_cd.
#   Snapshot/date column CONFIRMED = ME_DT (month-end), from two independent in-repo precedents:
#     - campaigns/CRV/bulletproof_analysis/26_behavior_mix_by_overlap_arm.sql:64-66 -
#       "SELECT acct_no, ME_DT, usg_bhvr_seg_at_cyc_cd FROM D3CV12A.CR_CRD_RPTS_ACCT
#        WHERE ME_DT BETWEEN DATE '2024-09-30' AND DATE '2026-05-31'"
#       and :78 - "AND b.ME_DT = f.treatmt_strt_dt - EXTRACT(DAY FROM f.treatmt_strt_dt)"
#       (i.e. the month-end BEFORE treatment), with line 6 naming the column outright:
#       "Behavior = usg_bhvr_seg_at_cyc_cd from D3CV12A.CR_CRD_RPTS_ACCT".
#     - campaigns/CRV/suppression_experiment/c4_demand_shaping.sql:92-99 -
#       "r.usg_bhvr_seg_at_cyc_cd AS usg_bhvr_seg ... ORDER BY r.me_dt DESC ...
#        AND r.me_dt < k.offer_start_date AND r.me_dt >= k.offer_start_date - INTERVAL '70' DAY".
#
# BEHAVIOUR CARRIES clnt_no - CORRECTION TO THE BRIEF, stated up front rather than buried.
#   The brief for this rewrite said CR_CRD_RPTS_ACCT is account-level with no clnt_no and must be
#   bridged through DFP's (clnt_no, acct_no). Two files in this repo disagree, and they are the
#   ones that actually ran:
#     - campaigns/CRV/installments_tag/crv_acct_to_clnt_bridge.sql:3-10 - its whole purpose is
#       "CRV table is acct-grain with NO clnt_no; CR_CRD_RPTS_ACCT is the acct->clnt lookup",
#       selecting "r.clnt_no ... FROM D3CV12A.CR_CRD_RPTS_ACCT r".
#     - campaigns/CRV/suppression_experiment/c4_demand_shaping.sql:73-77 -
#       "SELECT r.acct_no, r.clnt_no ... FROM d3cv12a.cr_crd_rpts_acct r".
#   So the bridge is unnecessary. Cell [4] bites CR_CRD_RPTS_ACCT on its OWN clnt_no, which (a)
#   removes an entire DFP re-scan, (b) lets the bite predicate sit inside the innermost derived
#   table exactly like every other pull here, and (c) means a client whose account never shows up
#   in the DFP scan window still gets a behaviour reading. If the column turns out to be absent at
#   run time the pull fails loudly on an unknown-column error, which is the right failure.
#
# UCP - /prod/sz/tsz/00172/data/ucp4/, partitioned by MONTH_END_DATE as a PATH SEGMENT, not a
#   column (references/ucp/README.md:56-64: "You cannot SELECT MONTH_END_DATE or filter on it as a
#   DataFrame column pre-read - it only exists as the .../MONTH_END_DATE=yyyy-MM-dd/ directory
#   segment"). Read via .option("basePath", UCP_BASE) + explicit partition paths (README.md:88-97).
#   Defensive filter trim(CLNT_TYP) == 'Personal' (README.md:97; gotchas.md:160-165 confirms
#   CLNT_TYP is live on personal ucp4 as of the 2026-07-27 probe).
#   Partition availability CONFIRMED 2023-12-31 through 2026-06-30 by a live probe
#   (gotchas.md #1, "CONFIRMED RANGE (2026-07-27, pack 28 v4)"), and the current open month never
#   has a partition (gotchas.md #1).
#   Fields used - all CONFIRMED live 2026-07-27 (references/ucp/field_catalog_personal.md:10-13,
#   :81, :96, :165): CLNT_NO, CLNT_TYP, AGE, TENURE_RBC_YEARS, PROF_TOT_ANNUAL,
#   T_TOT_CNT / I_TOT_CNT / B_TOT_CNT / C_TOT_CNT.
#
# PRODUCT DEPTH = COUNT of NONZERO TIBC columns (0-4), never their SUM. Summing the four counts
#   measures ACCOUNT VOLUME, not depth (spotlight.py:97-102, citing
#   unsub_value_museum.py:1126-1174 BAND_VERSION v4).
#
# PROFIT COLUMN IS NAMED prof_annual_ucp_proxy, deliberately. PROF_TOT_ANNUAL has NO documented
#   definition anywhere (references/ucp/gotchas.md #8, lines 186-208: "is not defined anywhere -
#   do NOT treat it as an LTV proxy without vetting"), and there is no cards-specific profit field
#   in UCP at all. The caveat travels with the column name so nobody downstream reads it as a
#   vetted LTV figure.
#
# CARDS_MNES (12) - retyped VERBATIM from spotlight.py:220-221 (which sources it from
#   unsub_value_museum.py:119-120). Used here only to sanity-check cards_pair_v6's mne set; the
#   is_cards_exposed flag comes from clientagg's own n_campaigns_cards, which spotlight.py's
#   Pull A computed server-side with the same list.
#
# ============================================================================================
# DEPARTURES FROM THE BRIEF - both stated at the top, per house rule, not buried
# ============================================================================================
# D1. The brief said "no ordered analytics inside subqueries" for Teradata-direct. Cell [3] uses
#     ONE ROW_NUMBER() in a derived table (WHERE rn = 1, never QUALIFY). The alternative -
#     MAX(dt_record_ext) GROUP BY (acct_no, ym) then a self-join back to DFP - reads the table
#     TWICE, which is precisely what table_catalog_notes.md:147-148 forbids for this table
#     ("never multi-scan / multi-subquery on this table ... blows spool") and prescribes the
#     window-function form instead. Canon beats the general rule here. Cell [4] uses no analytics
#     at all.
# D2. The brief said CR_CRD_RPTS_ACCT must be bridged through DFP for clnt_no. It carries clnt_no
#     directly - see the two citations above. Bridging was dropped.
#
# ============================================================================================
# UNVERIFIED - exhaustive, and repeated in the delivery report
# ============================================================================================
# U1. usg_bhvr_seg_at_cyc_cd's VALUE DOMAIN. Two repo files describe it as
#     "Dormant/Transactor/Revolver" in prose (26_behavior_mix_by_overlap_arm.sql:6 header,
#     c4_demand_shaping.sql:133 "Dormant/Transactor/Revolver, pre-wave") but neither prints the
#     raw codes, so it is unconfirmed whether the stored values are those words or codes.
#     Cell [4] runs a DISTINCT-value probe BEFORE the bites and prints what is actually in the
#     column; anything outside the three expected labels maps to rank 4 = "other_or_none", which
#     the probe makes visible rather than hiding.
# U2. CR_CRD_RPTS_ACCT may hold more than one row per (acct_no, ME_DT).
#     26_behavior_mix_by_overlap_arm.sql:13 flags this itself and never resolves it: "if higher,
#     CR_CRD_RPTS_ACCT has >1 row per acct x ME_DT and needs a dedup first". Cell [4] collapses
#     to CLIENT grain with MIN(seg_rank), so duplicates cannot fan out - but the precedence that
#     MIN implements is U3, not a documented rule.
# U3. Client-level behaviour precedence Revolver > Transactor > Dormant > other_or_none. A client
#     can hold several cards in different segments. No in-repo precedent exists for collapsing
#     them (references/ucp/gotchas.md #7 already records "no house standard" for band cuts; this
#     is the same gap). Editable in Cell [0] via SEG_PRECEDENCE.
# U4. Closure uses acct_cls_dt only. `status` is not cross-checked. If acct_cls_dt is ever null on
#     a WOFF/BKPT/COLL/FRD account, that account reads as still open. status IS confirmed one-way
#     (table_catalog_notes.md:179) so a status-based definition is possible later.
# U5. No account-OPEN gate at early offsets. acct_open_dt exists on DFP but is not pulled, so an
#     account opened after t=-3 cannot be told apart from one that already existed. Closure counts
#     at early offsets are directional, not exact.
# U6. An account is assumed to keep the same clnt_no across the scan window. If it moves, it is
#     counted once under each client in n_accts_total.
# U7. "Annual spend" = trailing 12 calendar months ending at t=0. Editable choice, no house
#     standard; picked because it is the only window that needs no data from after the cut point.
# U8. Behaviour is matched at the EXACT anchor month-end (an IN-list of six dates), not as-of the
#     most recent ME_DT at or before it. A client with no row at that exact month-end reads
#     no_data rather than carrying the previous month forward. Cheaper by a wide margin and
#     honest about gaps; flip BEHAVIOUR_EXACT_MONTH_END in Cell [0] only if you are prepared to
#     pay for the ranked as-of scan.
# U9. CARDS_MNES is a manual retype of spotlight.py's set, not a shared import - editing one and
#     not the other silently diverges the files.
# U10. spotlight.py's own open question rides along unchanged: whether disposition_cd = 4 is a
#     per-list or a global opt-out. cards_pair_v6's per-mne unsub_flag inherits that ambiguity, so
#     trigger_mne below is "the cards campaign the unsub was logged against", not proven causation.
# ============================================================================================


# %% [0] CONFIG - every tunable lives here. No literal below this cell is hand-typed elsewhere.

import calendar
import datetime

# ---- RUN SWITCHES - the only things to touch before hitting Run All -------------------------
SMOKE = True            # True -> bite 0 only (about a tenth of clients). Flip to False for the
                        # full population AFTER checking bite-0 counts and the Cell [1] gate.
                        # Outputs land under a _smoke-suffixed prefix and every cube carries a
                        # smoke_run column, so a smoke run can never be mistaken for a full one.
N_BITES = 10            # MOD(ABS(clnt_no), N_BITES). ABS matters: Teradata MOD(-7, 10) = -7 and
                        # matches no bite, so negative ids would silently vanish.
LAND_CHUNK_ROWS = 1_500_000   # rows per createDataFrame call. A full bite serialized in one shot
                        # exceeds spark.rpc.message.maxSize (128 MB). Lower if the limit is hit.
RUN_PULLS = ["DFP", "BHV"]    # restrict Cell [3] / Cell [4], e.g. ["BHV"] to resume only that one.
                        # Landed bites are skipped via their _ROWCOUNT marker regardless.
DROP_CLIENT_GRAIN = False     # Cell [8]. Refuses to delete unless the cubes exist AND read back.
                        # Never touches spotlight.py's landings - those are shared.
BEHAVIOUR_EXACT_MONTH_END = True   # see UNVERIFIED U8. False is not implemented; the flag exists
                        # so the assumption is visible in the config, not buried in SQL.

# ---- Cohort window. MUST match the window spotlight.py's landed pulls were built over, or the
# reused files describe a different population than the t_offsets are anchored on. ------------
WIN_START = "2025-08-01"    # spotlight.py:176 WIN_FLOOR
WIN_END   = "2026-08-01"    # spotlight.py:202 WIN_CEIL - a HARD ceiling, not "floor to now"
WIN_START_DATE = datetime.date(2025, 8, 1)
WIN_END_DATE   = datetime.date(2026, 8, 1)
assert WIN_START_DATE >= datetime.date(2024, 1, 1), \
    "repo floor is 2024-01-01 (memory: 2024_data_floor) - do not go below"

# ---- Time points, months relative to the cohort window end -----------------------------------
T_OFFSETS = [-3, 0, 3, 6, 9, 12]
assert -3 in T_OFFSETS and 0 in T_OFFSETS, \
    "t=-3 and t=0 are both load-bearing: without the pre-period this is a level comparison, and " \
    "the spend tier is cut at t=0. Removing either changes what the cube means."


def _add_months(d, n):
    total = d.year * 12 + (d.month - 1) + n
    y, m = divmod(total, 12)
    m += 1
    return datetime.date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def _month_end(d):
    return datetime.date(d.year, d.month, calendar.monthrange(d.year, d.month)[1])


def _tag(o):
    """Column-name-safe tag for an offset: -3 -> m3, 0 -> t0, +6 -> p6."""
    return "t0" if o == 0 else ("m%d" % abs(o) if o < 0 else "p%d" % o)


def _ym(d):
    return d.year * 100 + d.month


# t=0 anchor. The cohort window ends on WIN_END, so the natural anchor is the month-end inside it,
# but UCP has no partition for the current open month (references/ucp/gotchas.md #1) and neither
# DFP nor CR_CRD_RPTS_ACCT has a complete month for it either. Clamp to the last CLOSED month-end
# and say so out loud - a silent clamp would move every offset by a month without anyone noticing.
_T0_NATURAL = _month_end(WIN_END_DATE - datetime.timedelta(days=1))
_LAST_CLOSED = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
T0_ANCHOR = min(_T0_NATURAL, _LAST_CLOSED)
if T0_ANCHOR != _T0_NATURAL:
    print("NOTE: t=0 clamped from %s (month-end inside the cohort window) to %s (last CLOSED "
          "month-end). Current open months have no UCP partition and no complete DFP month. "
          "Every t_offset below is anchored on the clamped date."
          % (_T0_NATURAL.isoformat(), T0_ANCHOR.isoformat()))

T_ANCHOR = {o: _month_end(_add_months(T0_ANCHOR, o)) for o in T_OFFSETS}   # month-end date per offset
T_YM = {o: _ym(T_ANCHOR[o]) for o in T_OFFSETS}                            # YYYYMM int per offset
T_TAG = {o: _tag(o) for o in T_OFFSETS}

# ---- Annual spend for the tier cut: trailing 12 calendar months ending at t=0 (UNVERIFIED U7) --
ANNUAL_LOOKBACK_MONTHS = 12
ANNUAL_YMS = [_ym(_add_months(T0_ANCHOR, k)) for k in range(-(ANNUAL_LOOKBACK_MONTHS - 1), 1)]
assert len(ANNUAL_YMS) == ANNUAL_LOOKBACK_MONTHS

# ---- Every month DFP has to be scanned for = the six offsets plus the annual lookback ---------
SPEND_YMS = sorted(set(list(T_YM.values()) + ANNUAL_YMS))
_all_anchor_dates = sorted(set(list(T_ANCHOR.values()) +
                              [_month_end(_add_months(T0_ANCHOR, k))
                               for k in range(-(ANNUAL_LOOKBACK_MONTHS - 1), 1)]))
_scan_floor = _all_anchor_dates[0].replace(day=1)
if _scan_floor < datetime.date(2024, 1, 1):
    print("NOTE: computed DFP scan floor %s is below the 2024-01-01 repo floor - clamping. This "
          "shortens the annual-spend lookback for this run." % _scan_floor.isoformat())
    _scan_floor = datetime.date(2024, 1, 1)
    SPEND_YMS = [y for y in SPEND_YMS if y >= 202401]
SPEND_FLOOR = _scan_floor.isoformat()
SPEND_CEILING = (_all_anchor_dates[-1] + datetime.timedelta(days=1)).isoformat()   # exclusive
# t=+12 (and any offset past today) is in the future - those bites come back with no rows for
# those months. Thin, not broken. The cube reports them as zero-spend / no_data, which is the
# honest reading, and Cell [5] names every offset with no UCP partition explicitly.

SPEND_YMS_SQL = ", ".join(str(y) for y in SPEND_YMS)
ANNUAL_YMS_SQL = ", ".join(str(y) for y in ANNUAL_YMS)
ANCHOR_DATES_SQL = ", ".join("DATE '%s'" % T_ANCHOR[o].isoformat() for o in T_OFFSETS)

# ---- Behaviour segment precedence at client grain (UNVERIFIED U3) -----------------------------
# rank 1 wins. Anything not in this map lands on rank 4 and is labelled other_or_none, which the
# Cell [4] probe makes visible instead of silently folding it into a real segment.
SEG_PRECEDENCE = [("Revolver", 1), ("Transactor", 2), ("Dormant", 3)]
SEG_LABEL = {1: "Revolver", 2: "Transactor", 3: "Dormant", 4: "other_or_none", 0: "no_data"}

# ---- Spend tiers: tercile of annual spend, cut ONCE at t=0 and HELD FIXED across every offset --
# Re-cutting per period lets clients migrate between tiers and the comparison stops meaning
# anything - "High spenders fell" would then partly be "the ones who fell left the High tier".
SPEND_TIER_QUANTILES = [1.0 / 3.0, 2.0 / 3.0]
SPEND_TIER_REL_ERR = 0.01
# Zero-spend clients are ZEROS, not exclusions - dropping them would move every median. The cube
# reports stayers_with_spend / leavers_with_spend beside the counts so the denominator is visible.

# ---- MNE tag set - retyped verbatim from spotlight.py:220-221 (UNVERIFIED U9) ------------------
CARDS_MNES = frozenset({"PCQ", "PCL", "PCD", "AUH", "CLI", "CRV",
                        "VBA", "VBU", "CRO", "CEC", "VIF", "MET"})
assert len(CARDS_MNES) == 12, "CARDS_MNES should hold exactly 12 MNEs - recount vs spotlight.py:220"

# ---- Paths ------------------------------------------------------------------------------------
BASE = "hdfs:///user/427966379/unsub_spotlight2/"          # this file's own landings
SPOTLIGHT1_BASE = "hdfs:///user/427966379/unsub_spotlight/"  # spotlight.py's - READ ONLY, never deleted
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"                 # references/ucp/README.md - personal only
S1_SCHEMA_VERSION = 6   # spotlight.py:321 SCHEMA_VERSION - the version of ITS landed files

CLIENTAGG_DIR = SPOTLIGHT1_BASE + "clientagg_v%d/" % S1_SCHEMA_VERSION
CARDSPAIR_DIR = SPOTLIGHT1_BASE + "cards_pair_v%d/" % S1_SCHEMA_VERSION
MNEAGG_DIR    = SPOTLIGHT1_BASE + "mne_agg_v%d/" % S1_SCHEMA_VERSION   # read only by the reconcile print

# Landed-schema version for THIS file. The _landed() guard checks counts, not columns, so a schema
# change has to land in a fresh directory or a rerun silently reuses parquet from a prior design.
SCHEMA_VERSION = 1
DFP_DIR = BASE + "dfp_client_v%d/" % SCHEMA_VERSION      # Pull DFP  - one row per clnt_no, wide by offset
BHV_DIR = BASE + "bhvr_client_v%d/" % SCHEMA_VERSION     # Pull BHV  - one row per clnt_no, wide by offset
UCP_DIR = BASE + "ucp_offsets_v%d/" % SCHEMA_VERSION     # one row per (clnt_no, t_offset)
LONG_DIR = BASE + "client_long_v%d/" % SCHEMA_VERSION    # one row per (clnt_no, t_offset)

OUT_DIR = BASE + ("out_smoke/" if SMOKE else "out/")
PQ_DIR = OUT_DIR.rstrip("/") + "_parquet/"   # duckdb reads parquet; CSV loses every dtype

# ---- Landed schemas of the files this file REUSES (spotlight.py:347-365) -----------------------
CLIENTAGG_COLS = [
    "clnt_no", "n_campaigns_all", "n_campaigns_branded", "n_campaigns_cards",
    "n_campaigns_regulatory", "n_emails_all", "n_emails_3m", "n_emails_6m",
    "n_emails_promo_6m", "n_emails_regulatory_6m", "n_emails_cards", "n_emails_cards_6m",
    "unsub_flag", "unsub_dt", "first_send_dt", "last_send_dt",
]
CARDSPAIR_COLS = [
    "clnt_no", "mne", "n_sends", "unsub_flag", "unsub_dt", "first_send_dt", "last_send_dt",
]

# ---- Landed schemas of the two things this file pulls itself ----------------------------------
DFP_COLS = (["clnt_no", "n_accts_total", "annual_spend_t0"]
            + ["spend_" + T_TAG[o] for o in T_OFFSETS]
            + ["n_closed_" + T_TAG[o] for o in T_OFFSETS])
BHV_COLS = ["clnt_no"] + ["bhvr_rank_" + T_TAG[o] for o in T_OFFSETS]
UCP_OFFSET_COLS = ["clnt_no", "t_offset", "age", "tenure_years", "prod_cat_cnt",
                   "prof_annual_ucp_proxy"]

print("CONFIG loaded")
print("  cohort window (spotlight.py's):", WIN_START, "->", WIN_END,
      "| reusing", CLIENTAGG_DIR, "and", CARDSPAIR_DIR)
print("  t=0 anchor:", T0_ANCHOR.isoformat(), "| offsets:", T_OFFSETS)
print("  anchors:", {o: T_ANCHOR[o].isoformat() for o in T_OFFSETS})
print("  DFP scan:", SPEND_FLOOR, "->", SPEND_CEILING, "|", len(SPEND_YMS), "months in the IN-list")
print("  annual-spend months (t=-11..0):", ANNUAL_YMS)
print("  SMOKE:", SMOKE, "| N_BITES:", N_BITES, "| RUN_PULLS:", RUN_PULLS,
      "| SCHEMA_VERSION:", SCHEMA_VERSION)
print("  OUT_DIR:", OUT_DIR)


# %% [1] ACCUMULATOR VALIDATION GATE - blocking, runs BEFORE anything depends on net_prch_amt_mtd.
# ENGINE: Teradata-direct (D3CV12A.DLY_FULL_PORTFOLIO, no catalog prefix, Teradata syntax,
# teradatasql). Opens the ONE EDW connection reused by Cells [3] and [4] in this kernel session.
#
# Method: take the accounts active on the first record-date of a known-CLOSED calendar month (the
# month before t=0, unambiguously in the past). For that month, compare SUM(net_prch_amt_dly)
# against the LAST record's net_prch_amt_mtd per account. If net_prch_amt_mtd resets on the 1st,
# the two agree. If it is actually year-to-date, the MTD figure is several times too large and
# EVERY monthly spend number in this file is wrong - so this raises rather than warns.
#
# table_catalog_notes.md:164-165 states the reset ("cumulative month-to-date through that row's
# dt_record_ext, last row of month = monthly total"). This gate re-proves it live rather than
# trusting a write-up, per the house prove-then-proceed rule.

import getpass
import time

import pandas as pd
import teradatasql
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (StructType, StructField, LongType, StringType, DoubleType,
                               IntegerType, DateType)

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
        parts.append(c)
        n += len(c)
        print("  ...", n, "rows,", int(time.time() - t0), "s elapsed", flush=True)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


_val_month_start = _add_months(T0_ANCHOR.replace(day=1), -1)
_val_month_end = T0_ANCHOR.replace(day=1)
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
    "accumulator validation pulled zero accounts for %s - the 1-in-100000 acct_no sample found "
    "nothing. Widen the sample (change the MOD divisor in _val_sql) or pick a different month; do "
    "NOT proceed on an unproven accumulator." % VALIDATION_MONTH)

_val_pdf.columns = [c.lower() for c in _val_pdf.columns]
_val_pdf["sum_dly"] = pd.to_numeric(_val_pdf["sum_dly"], errors="coerce")
_val_pdf["last_mtd"] = pd.to_numeric(_val_pdf["last_mtd"], errors="coerce")
_val_pdf["abs_diff"] = (_val_pdf["last_mtd"] - _val_pdf["sum_dly"]).abs()
_val_pdf["ratio"] = _val_pdf["last_mtd"] / _val_pdf["sum_dly"].replace(0, pd.NA)

print("ACCUMULATOR CHECK | month %s | grain: one row per acct_no | %d accounts sampled"
      % (VALIDATION_MONTH, len(_val_pdf)))
print("Expected if net_prch_amt_mtd resets on the 1st: last_mtd == sum_dly for every row.")
print(_val_pdf.head(20).to_string(index=False))

# Only judge accounts with material activity - a near-zero sum_dly makes the ratio meaningless.
_material = _val_pdf[_val_pdf["sum_dly"].abs() > 1.0]
_bad = _material[_material["abs_diff"] > (0.01 * _material["sum_dly"].abs())]
print("Accounts with material activity (abs(sum_dly) > 1):", len(_material),
      "| mismatching by more than 1 pct:", len(_bad))

if len(_material) == 0:
    raise RuntimeError(
        "ACCUMULATOR CHECK INCONCLUSIVE - not one sampled account had material spend in %s, so "
        "nothing was actually proven. Widen the sample or pick another month. Refusing to proceed "
        "on an unproven accumulator: if net_prch_amt_mtd were year-to-date, every monthly figure "
        "in Cells [3]/[6]/[7] would be silently inflated." % VALIDATION_MONTH)

if len(_bad) > 0:
    raise RuntimeError(
        "ACCUMULATOR CHECK FAILED - last-of-month net_prch_amt_mtd does not equal SUM(net_prch_amt_dly) "
        "for %d of %d materially-active sampled accounts in %s. If the ratios below cluster well "
        "above 1 this is a YEAR-to-date accumulator, not month-to-date, and every monthly spend "
        "figure downstream is wrong. Fix the method before rerunning - do not relax this gate.\n%s"
        % (len(_bad), len(_material), VALIDATION_MONTH, _bad.head(20).to_string(index=False)))

ACCUM_VALIDATED = True
print("ACCUMULATOR CHECK PASSED - %d materially-active accounts, last-of-month MTD equals "
      "SUM(daily deltas) within 1 pct on every one. Taking the last record per (acct_no, month) "
      "and reading net_prch_amt_mtd is sound." % len(_material))


# %% [2] READERS - spotlight.py's landed files (REUSED, never re-pulled) and this file's own
# landings. Every reader asserts its full expected column list loudly, exactly like
# spotlight.py:368-399 read_clientagg(): a stale or short schema fails here rather than producing
# a quietly-wrong cube six cells later.
# ENGINE: PySpark (YARN). No EDW connection is opened or used in this cell.


def _read_dir(dir_path, cols, label, rerun_hint):
    sdf = spark.read.parquet(dir_path + "bite_*")
    missing = [c for c in cols if c not in sdf.columns]
    if missing:
        raise RuntimeError(
            "%s at %s is missing %s. Found: %s. Stale parquet from an older schema - %s"
            % (label, dir_path, missing, sdf.columns, rerun_hint))
    return sdf


def read_clientagg():
    """spotlight.py Pull A - one row per clnt_no, BANK-WIDE, about 12.3M rows. REUSED, not
    re-pulled: rebuilding this from VENDOR_FEEDBACK_* costs roughly an hour of wire time and, worse,
    a second pull drifts against the first (about 1K unsubs/day bank-wide) so the two files' cubes
    would no longer reconcile."""
    sdf = _read_dir(CLIENTAGG_DIR, CLIENTAGG_COLS, "clientagg (spotlight.py Pull A)",
                    "rerun spotlight.py Cell [1] Pull A - do NOT delete it from this file.")
    return sdf.withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))


def read_cardspair():
    """spotlight.py Pull C - one row per (clnt_no, cards mne), cards-only. REUSED, not re-pulled."""
    sdf = _read_dir(CARDSPAIR_DIR, CARDSPAIR_COLS, "cards_pair (spotlight.py Pull C)",
                    "rerun spotlight.py Cell [1] Pull C - do NOT delete it from this file.")
    return sdf.withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))


def read_dfp():
    sdf = _read_dir(DFP_DIR, DFP_COLS, "dfp_client",
                    "delete %s and rerun Cell [3]." % DFP_DIR)
    return sdf.withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))


def read_bhv():
    sdf = _read_dir(BHV_DIR, BHV_COLS, "bhvr_client",
                    "delete %s and rerun Cell [4]." % BHV_DIR)
    return sdf.withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))


def read_ucp_offsets():
    sdf = spark.read.parquet(UCP_DIR)
    missing = [c for c in UCP_OFFSET_COLS if c not in sdf.columns]
    if missing:
        raise RuntimeError("ucp_offsets at %s is missing %s. Found: %s. Delete it and rerun "
                           "Cell [5]." % (UCP_DIR, missing, sdf.columns))
    return sdf


def write_cube(df, name):
    """Every cube lands twice: CSV for the Excel pivot, parquet for duckdb in VS Code."""
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(OUT_DIR + name)
    df.coalesce(1).write.mode("overwrite").parquet(PQ_DIR + name)
    print("   wrote", OUT_DIR + name, "(csv) and", PQ_DIR + name, "(parquet)")


# ---- Cohort, straight off the reused clientagg. Leaver = unsub_flag 1, stayer = 0. -------------
_clientagg = read_clientagg()
cohort = (_clientagg
          .select("clnt_no", "unsub_flag", "n_campaigns_cards", "unsub_dt")
          .withColumn("bucket", F.when(F.col("unsub_flag") == 1, "LEAVER").otherwise("STAYER"))
          .withColumn("is_cards_exposed", (F.col("n_campaigns_cards") > 0).cast("int"))
          .cache())

COHORT_N = cohort.count()
print("COHORT read from spotlight.py's clientagg_v%d - grain: one row per clnt_no | %d clients"
      % (S1_SCHEMA_VERSION, COHORT_N))
cohort.groupBy("bucket").agg(F.count("*").alias("clients"),
                             F.sum("is_cards_exposed").alias("cards_exposed")).show(truncate=False)

_cohort_dupes = COHORT_N - cohort.select("clnt_no").distinct().count()
assert _cohort_dupes == 0, (
    "clientagg_v%d is not unique on clnt_no (%d duplicate rows). It is the left side of every join "
    "below, so a duplicate fans the client out through the whole cube. Investigate spotlight.py's "
    "Pull A before continuing." % (S1_SCHEMA_VERSION, _cohort_dupes))
print("Cohort uniqueness on clnt_no: confirmed, 0 duplicates.")

# ---- trigger_mne rides along from cards_pair (the cards campaign the unsub was logged against) --
# Client-level outcomes are NOT divisible across the campaigns that mailed a client - spend and
# profit belong to the client, not to a campaign - so this is a descriptive tag on the client, not
# an attribution of the outcome. See UNVERIFIED U10.
_cards_pair = read_cardspair()
_trigger = (_cards_pair
            .filter(F.col("unsub_flag") == 1)
            .withColumn("rn", F.row_number().over(
                Window.partitionBy("clnt_no").orderBy(F.col("unsub_dt").asc_nulls_last(),
                                                      F.col("mne").asc())))
            .filter(F.col("rn") == 1)
            .select("clnt_no", F.col("mne").alias("trigger_mne")))

_unexpected_mnes = (_cards_pair.select("mne").distinct()
                    .filter(~F.upper(F.trim(F.col("mne"))).isin(sorted(CARDS_MNES))))
_n_unexpected = _unexpected_mnes.count()
if _n_unexpected > 0:
    print("WARNING: cards_pair_v%d holds %d mne values outside this file's CARDS_MNES list "
          "(UNVERIFIED U9 - the two lists have diverged):" % (S1_SCHEMA_VERSION, _n_unexpected))
    _unexpected_mnes.show(20, truncate=False)
else:
    print("cards_pair mne set matches CARDS_MNES (12) - the retyped list is still in sync.")

print("Cell [2] done - readers ready, cohort cached (%d clients), trigger_mne derived from the "
      "REUSED cards_pair. Zero Teradata rows pulled in this cell." % COHORT_N)


# %% [3] PULL DFP - spend + closure, one row per CLIENT with the t_offsets pivoted into columns.
# ENGINE: Teradata-direct (D3CV12A.DLY_FULL_PORTFOLIO, no catalog prefix, teradatasql).
#
# WHY WIDE: one row per (clnt_no, month) would be about 12M clients x 18 months on the wire for
# nothing - every downstream cell immediately collapses it. Aggregating server-side to one row per
# client with six spend columns, six closed-account counts and the annual total puts about 1/18th
# of that on the wire.
#
# MONTHLY SPEND = the LAST record per (acct_no, month) at MAX(dt_record_ext), reading
# net_prch_amt_mtd (table_catalog_notes.md:164-165; proven live by Cell [1]). NOT a SUM of the
# daily rows.
#
# SPOOL HARDENING, carried from spotlight.py's v4 to v5 fix:
#   - the bite predicate MOD(ABS(clnt_no), N) lives INSIDE the innermost scanned subquery, never in
#     an outer WHERE. Outside, the full scan spools before the bite narrows anything - that is what
#     threw Teradata 2646 repeatedly.
#   - ABS() because Teradata MOD(-7, 10) = -7, which matches no bite; negative ids would vanish
#     from bitten pulls while unbitten ones still counted them.
#   - ONE scan of DFP, one ROW_NUMBER window, then two levels of GROUP BY - the pattern
#     table_catalog_notes.md:147-152 prescribes for this table by name. See DEPARTURE D1.
#   - the month IN-list narrows the scan to the 18 months actually needed on top of the
#     dt_record_ext range filter (the range prunes, the IN-list narrows).
# No percent sign appears anywhere in the SQL below - a comment containing a percent sign broke a
# pull earlier today, because these strings are percent-formatted.

assert "ACCUM_VALIDATED" in globals() and ACCUM_VALIDATED, \
    "Run Cell [1] first - it proves the accumulator this cell depends on."

DFP_SPARK_SCHEMA = StructType(
    [StructField("clnt_no", LongType(), True),
     StructField("n_accts_total", LongType(), True),
     StructField("annual_spend_t0", DoubleType(), True)]
    + [StructField("spend_" + T_TAG[o], DoubleType(), True) for o in T_OFFSETS]
    + [StructField("n_closed_" + T_TAG[o], LongType(), True) for o in T_OFFSETS])

BHV_SPARK_SCHEMA = StructType(
    [StructField("clnt_no", LongType(), True)]
    + [StructField("bhvr_rank_" + T_TAG[o], IntegerType(), True) for o in T_OFFSETS])


def _rowcount_marker_path(path):
    """INSIDE the bite directory, not a sibling. spotlight.py used a sibling <name>_ROWCOUNT and it
    doubled the entry count under the landing dir, which slowed every download. Spark's file index
    skips children whose name starts with an underscore, so a nested _ROWCOUNT is invisible to a
    parquet read of the bite itself."""
    return path.rstrip("/") + "/_ROWCOUNT"


def _landed(path):
    """A directory existing is NOT proof a bite fully landed - a session killed mid-write leaves a
    short directory that would otherwise be silently accepted as done, undercounting everything
    downstream. Landed means: the parquet reads AND the _ROWCOUNT marker exists AND matches."""
    try:
        _n_actual = spark.read.parquet(path).count()
    except Exception as e:
        msg = str(e).lower()
        if any(s in msg for s in ("path does not exist", "path_not_found", "filenotfound",
                                  "unable to infer schema")):
            return False
        raise RuntimeError(path + ": cannot verify HDFS state, refusing to guess. " + str(e)[:300])
    try:
        _n_expected = spark.read.parquet(_rowcount_marker_path(path)).collect()[0]["expected_rows"]
    except Exception:
        print(path, ": parquet exists but no _ROWCOUNT marker - partial write from a killed "
                    "session, treating as NOT landed and re-pulling.")
        return False
    if _n_actual != _n_expected:
        print("WARNING:", path, "- marker expects", _n_expected, "actual", _n_actual,
              "- partial write. Re-pulling this bite.")
        return False
    return True


def _write_chunks(pdf, schema, path):
    """Chunked write on an EXPLICIT StructType. Two reasons, both learned the hard way:
    a full bite in one createDataFrame call exceeds spark.rpc.message.maxSize (128 MB), and an
    INFERRED schema drifts between chunks whenever a column happens to be all-null in one of
    them, which makes the appended parquet unreadable as a single dataset."""
    _first = True
    for _s in range(0, len(pdf), LAND_CHUNK_ROWS):
        _part = pdf.iloc[_s:_s + LAND_CHUNK_ROWS]
        _sdf = spark.createDataFrame(_part, schema=schema)
        _sdf.write.mode("overwrite" if _first else "append").parquet(path)
        _first = False
        print("   ...", path, "chunk", _s, "-", _s + len(_part), "written", flush=True)
    nback = spark.read.parquet(path).count()
    # Marker written ONLY after every chunk landed, so a session killed mid-bite leaves no marker
    # and the rerun correctly re-pulls instead of trusting a short directory.
    _marker_schema = StructType([StructField("expected_rows", LongType(), False)])
    (spark.createDataFrame([(int(nback),)], schema=_marker_schema)
     .coalesce(1).write.mode("overwrite").parquet(_rowcount_marker_path(path)))
    return nback


def _prep_dfp(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    _n_null = pd.to_numeric(pdf["clnt_no"], errors="coerce").isna().sum()
    assert _n_null == 0, ("clnt_no has %d null/unparseable values - the SQL filters "
                          "clnt_no IS NOT NULL, so the filter is not firing" % _n_null)
    # int64, NEVER float: pandas renders large float64 ids in scientific notation on a string cast
    # and the join silently matches zero rows (memory: pandas_float_keys_scientific_notation).
    pdf["clnt_no"] = pd.to_numeric(pdf["clnt_no"], errors="coerce").astype("int64")
    # fillna BEFORE any int cast - casting a null to int throws "cannot convert non-finite values".
    for _c in ["n_accts_total"] + ["n_closed_" + T_TAG[o] for o in T_OFFSETS]:
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce").fillna(0).astype("int64")
    for _c in ["annual_spend_t0"] + ["spend_" + T_TAG[o] for o in T_OFFSETS]:
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce").fillna(0.0).astype("float64")
    return pdf[[f.name for f in DFP_SPARK_SCHEMA.fields]]


_dfp_spend_cols_sql = ",\n       ".join(
    "SUM(spend_%s) AS spend_%s" % (T_TAG[o], T_TAG[o]) for o in T_OFFSETS)
_dfp_closed_cols_sql = ",\n       ".join(
    "SUM(CASE WHEN acct_cls_dt IS NOT NULL AND acct_cls_dt <= DATE '%s' THEN 1 ELSE 0 END) "
    "AS n_closed_%s" % (T_ANCHOR[o].isoformat(), T_TAG[o]) for o in T_OFFSETS)
_dfp_acct_spend_sql = ",\n           ".join(
    "SUM(CASE WHEN ym = %d THEN acct_month_spend ELSE 0 END) AS spend_%s" % (T_YM[o], T_TAG[o])
    for o in T_OFFSETS)


def _dfp_sql(bite):
    return """
    WITH dfp_scoped AS (
        -- ONE scan of DLY_FULL_PORTFOLIO. The bite predicate is INSIDE this scan, not in an outer
        -- WHERE: outside, the full table spools before the bite narrows anything, which is the
        -- Teradata 2646 failure mode. ABS() because MOD of a negative id is negative in Teradata
        -- and would match no bite at all.
        -- dt_record_ext is the record date and the thing to filter on; me_dt is only a month-end
        -- tag shared by every row in a month (table_catalog_notes.md lines 155-156).
        SELECT p.clnt_no,
               p.acct_no,
               p.dt_record_ext,
               p.acct_cls_dt,
               CAST(p.net_prch_amt_mtd AS FLOAT) AS net_prch_amt_mtd,
               EXTRACT(YEAR FROM p.dt_record_ext) * 100
                 + EXTRACT(MONTH FROM p.dt_record_ext) AS ym
        FROM D3CV12A.DLY_FULL_PORTFOLIO p
        WHERE p.dt_record_ext >= DATE '%(floor)s'
          AND p.dt_record_ext <  DATE '%(ceil)s'
          AND p.clnt_no IS NOT NULL
          AND MOD(ABS(p.clnt_no), %(n_bites)d) = %(bite)d
          AND EXTRACT(YEAR FROM p.dt_record_ext) * 100
                + EXTRACT(MONTH FROM p.dt_record_ext) IN (%(yms)s)
    ),
    ranked AS (
        -- Last record per (acct_no, month). The month total lives on that row's
        -- net_prch_amt_mtd - a running month-to-date accumulator, proven to reset by Cell [1].
        -- Summing the daily rows instead would double-count nothing but would silently drop the
        -- spend on days with no row, since DFP is event-driven with gaps.
        SELECT clnt_no, acct_no, ym, acct_cls_dt, net_prch_amt_mtd,
               ROW_NUMBER() OVER (PARTITION BY acct_no, ym
                                  ORDER BY dt_record_ext DESC) AS rn
        FROM dfp_scoped
    ),
    acct_month AS (
        SELECT clnt_no, acct_no, ym, acct_cls_dt,
               net_prch_amt_mtd AS acct_month_spend
        FROM ranked
        WHERE rn = 1
    ),
    acct_wide AS (
        -- Collapse to ONE row per account, pivoting the months into columns. Doing the closure
        -- roll-up here rather than at client grain avoids needing several COUNT(DISTINCT) in one
        -- SELECT, which Teradata handles badly.
        SELECT clnt_no, acct_no,
           MAX(acct_cls_dt) AS acct_cls_dt,
           SUM(CASE WHEN ym IN (%(annual_yms)s) THEN acct_month_spend ELSE 0 END) AS annual_spend,
           %(acct_spend)s
        FROM acct_month
        GROUP BY clnt_no, acct_no
    )
    SELECT clnt_no,
       COUNT(*) AS n_accts_total,
       SUM(annual_spend) AS annual_spend_t0,
       %(spend_cols)s,
       %(closed_cols)s
    FROM acct_wide
    GROUP BY clnt_no
    """ % {"floor": SPEND_FLOOR, "ceil": SPEND_CEILING, "n_bites": N_BITES, "bite": bite,
           "yms": SPEND_YMS_SQL, "annual_yms": ANNUAL_YMS_SQL,
           "acct_spend": _dfp_acct_spend_sql, "spend_cols": _dfp_spend_cols_sql,
           "closed_cols": _dfp_closed_cols_sql}


def land_dfp_bite(bite):
    path = DFP_DIR + "bite_%d" % bite
    if _landed(path):
        print(path, ": already landed,", spark.read.parquet(path).count(), "rows - SKIP")
        return
    pdf = edw_pd(_dfp_sql(bite))
    assert len(pdf) > 0, (
        path + " pulled zero rows - investigate before proceeding. A future-dated offset makes a "
        "bite thin, it does not empty it: the annual-lookback months are all in the past.")
    pdf = _prep_dfp(pdf)
    nback = _write_chunks(pdf, DFP_SPARK_SCHEMA, path)
    assert nback == len(pdf), path + " HDFS readback mismatch: pulled %d, read back %d" % (
        len(pdf), nback)
    print(path, ": landed", len(pdf), "rows (one per clnt_no, offsets pivoted), readback", nback)


if "DFP" in RUN_PULLS:
    for _b in (range(1) if SMOKE else range(N_BITES)):
        land_dfp_bite(_b)
else:
    print("DFP pull skipped - not in RUN_PULLS")

print("Cell [3] done - DFP landed at", DFP_DIR + "bite_*",
      "| one row per clnt_no: n_accts_total, annual_spend_t0, spend and n_closed per t_offset.")


# %% [4] PULL BEHAVIOUR - revolver/transactor/dormant, one row per CLIENT, offsets pivoted.
# ENGINE: Teradata-direct (D3CV12A.CR_CRD_RPTS_ACCT, no catalog prefix, teradatasql).
#
# NO BRIDGE THROUGH DFP. CR_CRD_RPTS_ACCT carries clnt_no directly - crv_acct_to_clnt_bridge.sql:3-10
# exists solely to use it as the account-to-client lookup, and c4_demand_shaping.sql:73-77 selects
# r.acct_no, r.clnt_no off it. See DEPARTURE D2 in the header. That removes a whole DFP re-scan and
# lets the bite predicate sit inside the innermost derived table, same as every other pull here.
#
# ME_DT is the snapshot column (26_behavior_mix_by_overlap_arm.sql:64-66, c4_demand_shaping.sql:94-99).
# Matching is on the EXACT anchor month-ends, an IN-list of six dates - a client with no row at
# that exact month-end reads no_data rather than carrying the previous month forward (UNVERIFIED U8).
#
# The DISTINCT-value probe runs FIRST, before any bite, because the whole rank mapping below rests
# on values nobody in this repo has ever printed (UNVERIFIED U1). Anything outside the three
# expected labels lands on rank 4 = other_or_none and shows up in the probe output.

_seg_case_sql = "\n                    ".join(
    "WHEN TRIM(r.usg_bhvr_seg_at_cyc_cd) = '%s' THEN %d" % (lbl, rk) for lbl, rk in SEG_PRECEDENCE)
_bhv_rank_cols_sql = ",\n       ".join(
    "MIN(CASE WHEN me_dt = DATE '%s' THEN seg_rank END) AS bhvr_rank_%s"
    % (T_ANCHOR[o].isoformat(), T_TAG[o]) for o in T_OFFSETS)

_probe_sql = """
SELECT r.usg_bhvr_seg_at_cyc_cd AS raw_value, COUNT(*) AS accounts
FROM D3CV12A.CR_CRD_RPTS_ACCT r
WHERE r.ME_DT = DATE '%s'
GROUP BY r.usg_bhvr_seg_at_cyc_cd
ORDER BY accounts DESC
""" % T_ANCHOR[0].isoformat()

if "BHV" in RUN_PULLS:
    _probe = edw_pd(_probe_sql)
    _probe.columns = [c.lower() for c in _probe.columns]
    print("BEHAVIOUR VALUE PROBE - distinct usg_bhvr_seg_at_cyc_cd at ME_DT %s | grain: one row "
          "per raw value | %d rows" % (T_ANCHOR[0].isoformat(), len(_probe)))
    print(_probe.to_string(index=False))
    _expected = [lbl for lbl, _ in SEG_PRECEDENCE]
    _seen = [str(v).strip() for v in _probe["raw_value"].tolist()]
    _unmapped = [v for v in _seen if v not in _expected and v not in ("", "None", "nan")]
    if _unmapped:
        print("WARNING (UNVERIFIED U1): these raw values are NOT in SEG_PRECEDENCE and will be "
              "labelled other_or_none, not folded into a real segment:", _unmapped)
        print("If any of them is really a Revolver/Transactor/Dormant code, add it to "
              "SEG_PRECEDENCE in Cell [0] and rerun this pull before reading the cube.")
    else:
        print("All raw values map cleanly onto SEG_PRECEDENCE", _expected, "- U1 is now resolved "
              "for this snapshot. Record it in UNSUB_TRACKING_KNOWLEDGE.md.")
else:
    print("Behaviour probe skipped - BHV not in RUN_PULLS")


def _prep_bhv(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    _n_null = pd.to_numeric(pdf["clnt_no"], errors="coerce").isna().sum()
    assert _n_null == 0, ("clnt_no has %d null/unparseable values - the SQL filters "
                          "clnt_no IS NOT NULL, so the filter is not firing" % _n_null)
    pdf["clnt_no"] = pd.to_numeric(pdf["clnt_no"], errors="coerce").astype("int64")
    for _c in ["bhvr_rank_" + T_TAG[o] for o in T_OFFSETS]:
        # 0 is the sentinel for "no row at that exact month-end", mapped to no_data in Cell [6].
        # fillna before the int cast - a null cast to int throws on non-finite values.
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce").fillna(0).astype("int32")
    return pdf[[f.name for f in BHV_SPARK_SCHEMA.fields]]


def _bhv_sql(bite):
    return """
    WITH seg AS (
        -- Bite predicate INSIDE the scan, on CR_CRD_RPTS_ACCT's own clnt_no. ABS() because
        -- Teradata MOD of a negative id is negative and matches no bite.
        -- ME_DT restricted to the six anchor month-ends, so this reads six monthly snapshots
        -- rather than the whole history.
        SELECT r.clnt_no,
               r.acct_no,
               r.ME_DT AS me_dt,
               CASE
                    %(seg_case)s
                    ELSE 4
               END AS seg_rank
        FROM D3CV12A.CR_CRD_RPTS_ACCT r
        WHERE r.ME_DT IN (%(anchors)s)
          AND r.clnt_no IS NOT NULL
          AND MOD(ABS(r.clnt_no), %(n_bites)d) = %(bite)d
    )
    SELECT clnt_no,
       %(rank_cols)s
    FROM seg
    GROUP BY clnt_no
    """ % {"seg_case": _seg_case_sql, "anchors": ANCHOR_DATES_SQL, "n_bites": N_BITES,
           "bite": bite, "rank_cols": _bhv_rank_cols_sql}
    # MIN(seg_rank) per client implements the precedence in SEG_PRECEDENCE (rank 1 wins) and, as a
    # side effect, makes duplicate rows per (acct_no, ME_DT) harmless - see UNVERIFIED U2 and U3.


def land_bhv_bite(bite):
    path = BHV_DIR + "bite_%d" % bite
    if _landed(path):
        print(path, ": already landed,", spark.read.parquet(path).count(), "rows - SKIP")
        return
    pdf = edw_pd(_bhv_sql(bite))
    assert len(pdf) > 0, (
        path + " pulled zero rows - investigate. Future-dated offsets are expected to be empty, "
        "but t=-3 and t=0 are both in the past, so an empty bite means the ME_DT IN-list or the "
        "bite predicate is wrong.")
    pdf = _prep_bhv(pdf)
    nback = _write_chunks(pdf, BHV_SPARK_SCHEMA, path)
    assert nback == len(pdf), path + " HDFS readback mismatch: pulled %d, read back %d" % (
        len(pdf), nback)
    print(path, ": landed", len(pdf), "rows (one per clnt_no, offsets pivoted), readback", nback)


if "BHV" in RUN_PULLS:
    for _b in (range(1) if SMOKE else range(N_BITES)):
        land_bhv_bite(_b)
else:
    print("Behaviour pull skipped - not in RUN_PULLS")

print("Cell [4] done - behaviour landed at", BHV_DIR + "bite_*",
      "| one row per clnt_no, bhvr_rank per t_offset (1 Revolver, 2 Transactor, 3 Dormant, "
      "4 other_or_none, 0 no row at that month-end).")


# %% [5] UCP AT SIX MONTH-ENDS - one row per (clnt_no, t_offset). ENGINE: PySpark (YARN) reading
# HDFS parquet. Not Trino, not Teradata.
#
# THE FIX THIS REWRITE EXISTS FOR. The morning version applied ONE snapshot to every offset, so
# profitability and product depth were flat across t = -3 -> +12 by construction. In a file about
# change over time that is a hole, not a simplification.
#
# MONTH_END_DATE is a PATH SEGMENT, not a column (references/ucp/README.md:56-64), so six snapshots
# is six partition paths handed to ONE spark.read - one Spark job, not six queries. basePath makes
# MONTH_END_DATE reappear as a column so each row knows which snapshot it came from.
#
# Order of operations matters: filter Personal, select the seven columns, then SEMI-JOIN down to
# the cohort IMMEDIATELY - before the TIBC arithmetic, before dedup, before anything. Otherwise
# this carries 12.3M rows x 6 snapshots through every subsequent step.

_ucp_paths = {}
_ucp_missing = []
_ucp_ceiling = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)


def _hdfs_exists(path):
    _jvm = spark._jvm
    _p = _jvm.org.apache.hadoop.fs.Path(path)
    return _p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(_p)


for _o in T_OFFSETS:
    _d = T_ANCHOR[_o].isoformat()
    _p = UCP_BASE + "MONTH_END_DATE=" + _d
    if _hdfs_exists(_p):
        _ucp_paths[_o] = _p
    else:
        _ucp_missing.append((_o, _d, _p))

# A missing PAST partition is a real data gap and must stop the run - it would show up in the cube
# as a genuine-looking trend break. A missing FUTURE partition is arithmetic: UCP has no partition
# for a month that has not closed yet (references/ucp/gotchas.md #1).
_past_missing = [(o, d, p) for (o, d, p) in _ucp_missing
                 if datetime.date(*[int(x) for x in d.split("-")]) <= _ucp_ceiling]
_future_missing = [(o, d, p) for (o, d, p) in _ucp_missing if (o, d, p) not in _past_missing]

if _past_missing:
    raise RuntimeError(
        "UCP partition MISSING for a CLOSED month - refusing to run. A silently absent month "
        "reads as a real trend in the cube. Missing: %s. Confirmed-available range per "
        "references/ucp/gotchas.md #1 is 2023-12-31 through 2026-06-30; check the path and the "
        "t=0 anchor (%s)." % (_past_missing, T0_ANCHOR.isoformat()))

if _future_missing:
    print("UCP has NO partition for these offsets because their month-end has not closed yet "
          "(last closed month-end: %s):" % _ucp_ceiling.isoformat())
    for _o, _d, _p in _future_missing:
        print("   t = %+d  ->  %s  (no partition)" % (_o, _d))
    print("Those offsets carry prod_cat_cnt = no_ucp_match and a NULL profit proxy in the cube. "
          "They are NOT zeros and must not be read as a decline. Spend and behaviour at those "
          "offsets are likewise thin-to-empty. Move WIN_END forward and rerun once the months "
          "close.")

UCP_OFFSETS_AVAILABLE = sorted(_ucp_paths.keys())
assert 0 in UCP_OFFSETS_AVAILABLE and -3 in UCP_OFFSETS_AVAILABLE, (
    "UCP is missing t=0 and/or t=-3 (%s). Those two carry the whole pre/post comparison - there is "
    "no point running the cube without them." % UCP_OFFSETS_AVAILABLE)
print("UCP partitions to read:", {o: _ucp_paths[o] for o in UCP_OFFSETS_AVAILABLE})

_TIBC_COLS = ["T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT"]
_UCP_COLS = ["CLNT_NO", "CLNT_TYP", "AGE", "TENURE_RBC_YEARS", "PROF_TOT_ANNUAL"] + _TIBC_COLS

_ucp_raw = (spark.read.option("basePath", UCP_BASE)
            .parquet(*[_ucp_paths[o] for o in UCP_OFFSETS_AVAILABLE]))
_missing_cols = [c for c in _UCP_COLS if c not in _ucp_raw.columns]
assert not _missing_cols, (
    "UCP is missing required columns across the six partitions: %s. Found: %s. Every one of these "
    "is marked CONFIRMED live 2026-07-27 in references/ucp/field_catalog_personal.md - if one has "
    "gone, stop and re-probe the schema before trusting anything here."
    % (_missing_cols, _ucp_raw.columns))
assert "MONTH_END_DATE" in _ucp_raw.columns, (
    "MONTH_END_DATE did not materialise as a partition column - basePath was not honoured, so "
    "every row would be untagged and all six snapshots would collapse into one.")
print("UCP SCHEMA PROBE - all required columns present across", len(UCP_OFFSETS_AVAILABLE),
      "partitions:", _UCP_COLS)

# Path segment -> t_offset. Built from the same dict that produced the paths, so they cannot drift.
_offset_expr = None
for _o in UCP_OFFSETS_AVAILABLE:
    _cond = F.col("MONTH_END_DATE").cast("string") == T_ANCHOR[_o].isoformat()
    _offset_expr = F.when(_cond, F.lit(_o)) if _offset_expr is None else _offset_expr.when(_cond, F.lit(_o))

_cohort_ids = cohort.select("clnt_no").distinct()

ucp_sel = (_ucp_raw
           .filter(F.trim(F.col("CLNT_TYP")) == "Personal")
           .select(*(_UCP_COLS + ["MONTH_END_DATE"]))
           .withColumn("t_offset", _offset_expr)
           .withColumn("clnt_no", F.col("CLNT_NO").cast("decimal(18,0)").cast("long"))
           .drop("CLNT_NO", "CLNT_TYP")
           # SEMI-JOIN TO THE COHORT NOW - before the TIBC arithmetic, before dedup. Otherwise this
           # carries about 12.3M rows per snapshot x 6 through everything that follows.
           .join(_cohort_ids, "clnt_no", "left_semi"))

assert ucp_sel.filter(F.col("t_offset").isNull()).limit(1).count() == 0, \
    "some UCP rows did not map to a t_offset - MONTH_END_DATE format differs from the ISO date " \
    "used to build the partition paths."

# prod_cat_cnt = COUNT of NONZERO TIBC categories (0-4), NEVER their sum. Summing the four counts
# measures ACCOUNT VOLUME, not product depth (spotlight.py:97-102 / unsub_value_museum.py:1126-1174).
_held = [(F.coalesce(F.col(c), F.lit(0)) > 0).cast("int") for c in _TIBC_COLS]

ucp_off = (ucp_sel
           .withColumn("prod_cat_cnt", (_held[0] + _held[1] + _held[2] + _held[3]))
           .withColumnRenamed("AGE", "age")
           .withColumnRenamed("TENURE_RBC_YEARS", "tenure_years")
           # Named _ucp_proxy deliberately: PROF_TOT_ANNUAL is bank-wide and formally undefined
           # (references/ucp/gotchas.md #8), so the caveat travels with the column.
           .withColumnRenamed("PROF_TOT_ANNUAL", "prof_annual_ucp_proxy")
           .select("clnt_no", "t_offset", "age", "tenure_years",
                   F.col("prod_cat_cnt").cast("string").alias("prod_cat_cnt"),
                   F.col("prof_annual_ucp_proxy").cast("double").alias("prof_annual_ucp_proxy")))

# DEDUP before it becomes the right side of a left join. Measured on the single-snapshot version
# 2026-07-31: exactly ONE duplicate in 12.3M - tiny, but one duplicate fans that client out through
# every offset of every cube.
_n_pre = ucp_off.count()
ucp_off = ucp_off.dropDuplicates(["clnt_no", "t_offset"])
_n_post = ucp_off.count()
print("UCP rows after cohort semi-join:", _n_pre, "| after dedup on (clnt_no, t_offset):", _n_post,
      "| duplicates removed:", _n_pre - _n_post)

ucp_off.write.mode("overwrite").parquet(UCP_DIR)
_ucp_land = read_ucp_offsets().cache()

_per_off = (_ucp_land.groupBy("t_offset")
            .agg(F.count("*").alias("rows"),
                 F.countDistinct("clnt_no").alias("distinct_clients"))
            .orderBy("t_offset"))
print("UCP MATCH BY OFFSET | grain: one row per t_offset | cohort size", COHORT_N)
_per_off.show(truncate=False)

_per_off_pd = _per_off.toPandas()
for _r in _per_off_pd.itertuples(index=False):
    assert _r.rows == _r.distinct_clients, (
        "UCP offset %d has %d rows for %d distinct clients - the dedup did not hold and every "
        "left join at that offset fans out." % (_r.t_offset, _r.rows, _r.distinct_clients))
    assert _r.rows <= COHORT_N, (
        "UCP offset %d has %d rows against a cohort of %d - the semi-join did not narrow to the "
        "cohort." % (_r.t_offset, _r.rows, COHORT_N))
print("Cell [5] done - ucp_offsets landed at", UCP_DIR,
      "| one row per (clnt_no, t_offset) | rows == distinct clients at every offset, confirmed.")
print("Match rate is expected below 100 pct: UCP is personal-only, so business recipients and "
      "clients closed before a given snapshot never match. A LOW rate at one offset only is the "
      "signal worth chasing - a uniformly low rate is the usual personal-only attrition.")


# %% [6] CLIENT x T_OFFSET LONG TABLE - the join layer. Nothing here is the deliverable; Cell [7]
# aggregates it into the cubes and Cell [8] can delete it. ENGINE: PySpark (YARN).
#
# Outcomes are CLIENT-level throughout. Spend and profit are not divisible across the campaigns
# that mailed a client, so trigger_mne and n_campaigns_cards ride along as descriptive tags on the
# client - never as a way of splitting a dollar.

dfp_wide = read_dfp()
bhv_wide = read_bhv()
ucp_off = read_ucp_offsets()

# ---- pivot the wide pulls back to long, one struct per offset -------------------------------
_dfp_structs = [F.struct(F.lit(o).alias("t_offset"),
                         F.col("spend_" + T_TAG[o]).alias("month_spend"),
                         F.col("n_closed_" + T_TAG[o]).alias("n_closed"))
                for o in T_OFFSETS]
dfp_long = (dfp_wide
            .select("clnt_no", "n_accts_total",
                    F.explode(F.array(*_dfp_structs)).alias("s"))
            .select("clnt_no", "n_accts_total",
                    F.col("s.t_offset").alias("t_offset"),
                    F.col("s.month_spend").alias("month_spend"),
                    F.col("s.n_closed").alias("n_closed"))
            # A client is CLOSED at t only when every account it holds is closed by then. A client
            # with no DFP account at all is not "closed" - it never had a card (n_accts_total 0).
            .withColumn("closed_flag",
                        ((F.col("n_accts_total") > 0)
                         & (F.col("n_closed") == F.col("n_accts_total"))).cast("int"))
            .select("clnt_no", "t_offset", "month_spend", "closed_flag"))

_bhv_structs = [F.struct(F.lit(o).alias("t_offset"),
                         F.col("bhvr_rank_" + T_TAG[o]).alias("rank"))
                for o in T_OFFSETS]
_seg_label_expr = None
for _rk in sorted(SEG_LABEL):
    _c = F.col("rank") == _rk
    _seg_label_expr = (F.when(_c, F.lit(SEG_LABEL[_rk])) if _seg_label_expr is None
                       else _seg_label_expr.when(_c, F.lit(SEG_LABEL[_rk])))

bhv_long = (bhv_wide
            .select("clnt_no", F.explode(F.array(*_bhv_structs)).alias("s"))
            .select("clnt_no", F.col("s.t_offset").alias("t_offset"), F.col("s.rank").alias("rank"))
            .withColumn("usg_bhvr_seg", _seg_label_expr.otherwise(F.lit("no_data")))
            .select("clnt_no", "t_offset", "usg_bhvr_seg"))

# ---- spine: every cohort client at every offset. Zero-spend clients are ZEROS, not exclusions. --
_offsets_df = spark.createDataFrame([(o,) for o in T_OFFSETS], ["t_offset"])
spine = cohort.select("clnt_no").crossJoin(F.broadcast(_offsets_df))

annual = dfp_wide.select("clnt_no", "annual_spend_t0", "n_accts_total")

long_tbl = (spine
            .join(dfp_long, ["clnt_no", "t_offset"], "left")
            .join(bhv_long, ["clnt_no", "t_offset"], "left")
            .join(ucp_off.select("clnt_no", "t_offset", "prod_cat_cnt", "prof_annual_ucp_proxy",
                                 "age", "tenure_years"), ["clnt_no", "t_offset"], "left")
            .join(annual, "clnt_no", "left")
            .join(cohort.select("clnt_no", "bucket", "unsub_flag", "is_cards_exposed",
                                "n_campaigns_cards"), "clnt_no", "left")
            .join(_trigger, "clnt_no", "left")
            .withColumn("month_spend", F.coalesce(F.col("month_spend"), F.lit(0.0)))
            .withColumn("annual_spend_t0", F.coalesce(F.col("annual_spend_t0"), F.lit(0.0)))
            .withColumn("n_accts_total", F.coalesce(F.col("n_accts_total"), F.lit(0)))
            .withColumn("closed_flag", F.coalesce(F.col("closed_flag"), F.lit(0)))
            .withColumn("usg_bhvr_seg", F.coalesce(F.col("usg_bhvr_seg"), F.lit("no_data")))
            .withColumn("prod_cat_cnt", F.coalesce(F.col("prod_cat_cnt"), F.lit("no_ucp_match")))
            .withColumn("trigger_mne", F.coalesce(F.col("trigger_mne"), F.lit("none"))))

long_tbl.write.mode("overwrite").parquet(LONG_DIR)
long_tbl = spark.read.parquet(LONG_DIR).cache()

_n_long = long_tbl.count()
_n_expected = COHORT_N * len(T_OFFSETS)
print("LONG TABLE landed at", LONG_DIR, "| grain: one row per (clnt_no, t_offset) |", _n_long,
      "rows | expected", _n_expected, "(", COHORT_N, "clients x", len(T_OFFSETS), "offsets )")
assert _n_long == _n_expected, (
    "long table row count mismatch: %d vs %d expected. A left join fanned out - the usual cause is "
    "a duplicate key in dfp_wide, bhv_wide, ucp_offsets or the trigger_mne table. Every one of "
    "those is asserted unique upstream, so start by re-reading those asserts."
    % (_n_long, _n_expected))

print("\nCOVERAGE BY OFFSET | grain: one row per t_offset | clients with any spend, any behaviour "
      "reading, any UCP row. Future-dated offsets are expected to be thin - that is arithmetic, "
      "not a bug.")
(long_tbl.groupBy("t_offset")
 .agg(F.count("*").alias("clients"),
      F.sum((F.col("month_spend") != 0).cast("int")).alias("clients_with_spend"),
      F.sum((F.col("usg_bhvr_seg") != "no_data").cast("int")).alias("clients_with_behaviour"),
      F.sum((F.col("prod_cat_cnt") != "no_ucp_match").cast("int")).alias("clients_with_ucp"),
      F.sum("closed_flag").alias("clients_all_accts_closed"))
 .orderBy("t_offset").show(truncate=False))
print("Cell [6] done.")


# %% [7] SPEND TIER CUT + CUBES. ENGINE: PySpark (YARN).
#
# The tier is cut ONCE, on annual spend at t=0, and HELD FIXED across every offset. Re-cutting per
# period lets clients migrate between tiers, and then "High spenders fell" partly means "the ones
# who fell stopped being High" - the comparison stops meaning anything.
#
# Zero-spend clients are in the cut, not excluded - dropping them would move every tier boundary
# and every median. clients_with_spend rides beside the counts so the denominator is visible.
#
# WIDE, not long: stayers and leavers are COLUMNS. In long format a cell with leavers but no
# stayers has no stayer row at all, and a pivot then divides by the wrong denominator.
# COUNTS ONLY - no rates, no percentages, no lifts. Numerator and denominator are both present and
# Andre derives the rate in the pivot.

_tier_src = (long_tbl.filter(F.col("t_offset") == 0)
             .select("clnt_no", "annual_spend_t0").distinct())
_q1, _q2 = _tier_src.approxQuantile("annual_spend_t0", SPEND_TIER_QUANTILES, SPEND_TIER_REL_ERR)
print("SPEND TIER CUT | annual spend = trailing %d months ending at t=0 (%s) | cut ONCE, held "
      "fixed across every offset | Low <= %.2f < Mid <= %.2f < High"
      % (ANNUAL_LOOKBACK_MONTHS, T0_ANCHOR.isoformat(), _q1, _q2))
if _q1 == _q2:
    print("WARNING: the two tercile cut points are identical (%.2f). More than a third of the "
          "cohort has the same annual spend - almost certainly zero - so Low and Mid are not "
          "separable and every client at that value lands in Low. Read the tier as "
          "zero-vs-something, not as three real terciles." % _q1)

spend_tier = (_tier_src
              .withColumn("spend_tier",
                          F.when(F.col("annual_spend_t0") <= _q1, "Low")
                           .when(F.col("annual_spend_t0") <= _q2, "Mid")
                           .otherwise("High"))
              .select("clnt_no", "spend_tier"))

(spend_tier.groupBy("spend_tier").agg(F.count("*").alias("clients")).orderBy("spend_tier")
 .show(truncate=False))

cube_src = (long_tbl.join(spend_tier, "clnt_no", "left")
            .withColumn("spend_tier", F.coalesce(F.col("spend_tier"), F.lit("untiered")))
            .cache())

_stay = F.col("bucket") == "STAYER"
_leave = F.col("bucket") == "LEAVER"
_has_spend = F.col("month_spend") != 0

_MEASURES = [
    F.sum(F.when(_stay, 1).otherwise(0)).alias("stayers"),
    F.sum(F.when(_leave, 1).otherwise(0)).alias("leavers"),
    F.count("*").alias("clients_total"),
    F.sum(F.when(_stay & _has_spend, 1).otherwise(0)).alias("stayers_with_spend"),
    F.sum(F.when(_leave & _has_spend, 1).otherwise(0)).alias("leavers_with_spend"),
    F.expr("percentile_approx(CASE WHEN bucket = 'STAYER' THEN month_spend END, 0.5)")
     .alias("median_spend_stayers"),
    F.expr("percentile_approx(CASE WHEN bucket = 'LEAVER' THEN month_spend END, 0.5)")
     .alias("median_spend_leavers"),
    F.expr("percentile_approx(CASE WHEN bucket = 'STAYER' THEN prof_annual_ucp_proxy END, 0.5)")
     .alias("median_prof_stayers_ucp_proxy"),
    F.expr("percentile_approx(CASE WHEN bucket = 'LEAVER' THEN prof_annual_ucp_proxy END, 0.5)")
     .alias("median_prof_leavers_ucp_proxy"),
    F.sum(F.when(_stay & (F.col("closed_flag") == 1), 1).otherwise(0)).alias("closed_stayers"),
    F.sum(F.when(_leave & (F.col("closed_flag") == 1), 1).otherwise(0)).alias("closed_leavers"),
]

CUBE_DIMS = ["t_offset", "spend_tier", "usg_bhvr_seg", "prod_cat_cnt", "is_cards_exposed"]

cube_main = (cube_src.groupBy(*CUBE_DIMS).agg(*_MEASURES)
             .withColumn("smoke_run", F.lit(1 if SMOKE else 0))
             .orderBy(*CUBE_DIMS)
             .cache())   # cached: toPandas() then two writes = three actions on one DAG

cube_main_pd = cube_main.toPandas()
print("\nCUBE MAIN | grain: one row per (%s) | stayers and leavers are COLUMNS | counts only, no "
      "rates | %d rows | SMOKE=%s" % (", ".join(CUBE_DIMS), len(cube_main_pd), SMOKE))
print(cube_main_pd.head(40).to_string(index=False))
write_cube(cube_main, "cube_main")

# ---- the campaign dimension riding along. Same measures, grain (t_offset, trigger_mne). Outcomes
# are still CLIENT-level: this says "clients whose unsub was logged against PCL spend X", not
# "PCL cost X". See UNVERIFIED U10.
cube_trigger = (cube_src.groupBy("t_offset", "trigger_mne", "is_cards_exposed").agg(*_MEASURES)
                .withColumn("smoke_run", F.lit(1 if SMOKE else 0))
                .orderBy("t_offset", "trigger_mne")
                .cache())
cube_trigger_pd = cube_trigger.toPandas()
print("\nCUBE BY TRIGGER MNE | grain: one row per (t_offset, trigger_mne, is_cards_exposed) | "
      "trigger_mne = 'none' means no cards-campaign unsub was logged for that client | %d rows"
      % len(cube_trigger_pd))
print(cube_trigger_pd.head(30).to_string(index=False))
write_cube(cube_trigger, "cube_by_trigger_mne")

# ---- reconciliation against spotlight.py, so the two files cannot quietly disagree -------------
_recon = cube_main_pd[cube_main_pd["t_offset"] == 0]
_recon_stay = int(_recon["stayers"].sum())
_recon_leave = int(_recon["leavers"].sum())
_s1 = (cohort.agg(F.sum((F.col("unsub_flag") == 0).cast("int")).alias("stayers"),
                  F.sum((F.col("unsub_flag") == 1).cast("int")).alias("leavers")).collect()[0])
print("\nRECONCILIATION against spotlight.py's clientagg_v%d | grain: one row" % S1_SCHEMA_VERSION)
print("   cube_main at t=0 : stayers %d, leavers %d" % (_recon_stay, _recon_leave))
print("   clientagg        : stayers %d, leavers %d" % (_s1["stayers"], _s1["leavers"]))
assert _recon_stay == _s1["stayers"] and _recon_leave == _s1["leavers"], (
    "cube_main at t=0 does not reconcile against the clientagg it was built from. Every client "
    "must appear exactly once per offset - a mismatch means a join fanned out or dropped rows "
    "between Cell [6]'s assert and here.")
print("   RECONCILED - every cohort client appears exactly once at t=0.")

if SMOKE:
    print("\nSMOKE was True. The COHORT is the full bank-wide clientagg, but the DFP and behaviour "
          "pulls only ran bite 0, so spend, closure and behaviour exist for about a tenth of "
          "clients and the rest read as zero-spend / no_data. Do NOT report these numbers. Flip "
          "SMOKE to False in Cell [0] and rerun Cells [3]-[7].")

print("\nCell [7] done -", OUT_DIR, "holds cube_main and cube_by_trigger_mne, as CSV and parquet.")
print("Pull them down with:  hdfs dfs -get -f " + OUT_DIR + "* .")


# %% [8] CLEANUP - drop this file's client-grain intermediates, keep the cubes.
# NOT automatic. Controlled by DROP_CLIENT_GRAIN in Cell [0]. Refuses to delete unless both cubes
# exist AND read back.
#
# spotlight.py's landings (clientagg_v6, cards_pair_v6, mne_agg_v6, ucp_enriched) are NEVER touched
# by this cell. They are shared, they cost roughly an hour to rebuild, and spotlight.py has its own
# cleanup cell that owns them.

import subprocess

_targets = [DFP_DIR.rstrip("/"), BHV_DIR.rstrip("/"), UCP_DIR.rstrip("/"), LONG_DIR.rstrip("/")]
for _t in _targets:
    assert _t.startswith(BASE.rstrip("/")), (
        "refusing to delete %s - it is outside this file's own BASE (%s). spotlight.py's landings "
        "are shared and must never be deleted from here." % (_t, BASE))

_required = [OUT_DIR + "cube_main", OUT_DIR + "cube_by_trigger_mne"]

if not DROP_CLIENT_GRAIN:
    print("DROP_CLIENT_GRAIN is False - nothing deleted. Would remove:")
    for _t in _targets:
        print("   ", _t)
    print("\nCheck the cubes first:", _required)
    print("Once these are gone, re-cutting means re-pulling DFP and CR_CRD_RPTS_ACCT from Teradata.")
else:
    _missing_out = []
    for _r in _required:
        try:
            spark.read.option("header", True).csv(_r).limit(1).collect()
        except Exception:
            _missing_out.append(_r)
    if _missing_out:
        raise RuntimeError("Refusing to delete client-grain data - these cubes are missing or "
                           "unreadable: %s. Rerun Cell [7] first." % _missing_out)
    print("Cubes verified present and readable. Deleting this file's client-grain intermediates:")
    for _t in _targets:
        _rc = subprocess.run("hdfs dfs -rm -r -skipTrash %s" % _t,
                             shell=True, capture_output=True, text=True, timeout=600)
        print("  ", _t, "-> rc", _rc.returncode, (_rc.stdout or _rc.stderr).strip()[:200])
    print("\nDone. Only the cubes remain. Nothing at client grain persists under", BASE)

print("\nWhat stays here:", OUT_DIR + "*  (cube_main, cube_by_trigger_mne; csv and parquet)")
print("What this file NEVER deletes:", CLIENTAGG_DIR, "|", CARDSPAIR_DIR, "|", MNEAGG_DIR)
