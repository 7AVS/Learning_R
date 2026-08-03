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
#    bound. CARDS_MNES (Cell [0]) is now ALL 32 catalog codes, INCLUDING regulatory/operational/
#    fulfillment (Andre 2026-08-03, scope change: those must stay IN and be ISOLATABLE, not
#    dropped) - cards_unsub_flag/n_emails_cards reflect that full 32. Two twin pairs ride on
#    a1_client to make the subsets isolatable without re-querying: n_emails_cards_ex_fwc /
#    cards_ex_fwc_unsub_flag (flag basis minus FWC, 31 mnes) and n_emails_cards_nonmkt /
#    cards_nonmkt_unsub_flag (the 10 regulatory/operational/fulfillment mnes). Marketing-only view
#    is derivable by subtraction (cards minus nonmkt) - no third twin materialized. Not surfaced in
#    a1_mne_share; available to A3/A4.
#  A1b unique unsub clients x 5 LOB groupings (ENTERPRISE/    -> Cell [6b] -> a1_lob_dedup.csv
#    CARDS_LOB_ALL[32]/CARDS_MKT[22]/CARDS_EX_FWC[21]/CARDS_NONMKT[10]), each an exact client-grain
#    COUNT DISTINCT (not a per-mne sum) - CARDS_LOB_ALL cross-checked against A1's
#    ENTERPRISE_TOTAL_UNIQUE_CLIENTS-vs-CARDS_TOTAL_UNIQUE_CLIENTS split, same window.
#  A2 mne x {senders, unsubs_attributed, leavers_exposed}  -> Cell [7]  -> a2_mne_rates.csv
#  A3 in-window contact load, banded, x unsub x cards_unsub-> Cell [8]  -> a3_contact_cube.csv
#    + sum_emails_all / sum_emails_cards (2026-08-03c, per-bucket email volume alongside client counts)
#  A4 age x tenure x T/I/B/C(separate) x depth x stay/leave-> Cell [9]  -> a4_profile_cube.csv
#    x leavers_cards_unsub (cards-view subset, rides beside leavers) x leavers_cards_ex_fwc x
#    leavers_cards_nonmkt (2026-08-03c, ex-FWC and non-marketing subsets) + sum_emails_all /
#    sum_emails_cards (per-profile email volume)
#
#  PIECE B (build 2026-08-03e REDESIGN - "then vs now vs delta". Anchors 2025-06-30 ("then") and
#  2026-06-30 ("now"), BOTH CLOSED as of this build's run date - the whole point: the comparison is
#  computable NOW, no future-dated thinness, no regime/resume machinery (that machinery from the
#  Aug-anchor build is retired, see Cell [18]). Cards-mailed cohort only, MARKETING-ONLY scope -
#  CARDS_MKT_MNES/22, not the 32-mne CARDS_MNES flag - see Cell [12].)
#  ---------------------------------------------------------------------------------------------
#  cohort + leaver flags + cards_unsub_mne campaign dim    -> Cell [12]  -> b_cohort_v3/ (landed)
#  scoped BEFORE pulling DFP/BHV/UCP, not post-hoc
#  spend (3-month window around then/now), cohort-scoped    -> Cell [13]  -> b_dfp_v3/ (landed)
#  revolver/transactor at then/now month-ends, cohort-scoped -> Cell [14]  -> b_bhv_v3/ (landed)
#  UCP profitability + T/I/B/C holding at then/now snapshots -> Cell [14b] -> b_ucp_v3/ (landed)
#  spend tier x tier_now x seg_then x seg_now x              -> Cell [15]  -> b_before_after_cube.csv
#  {stayers, leavers(cards), leavers_ex_fwc}  (spend tier = held fixed at THEN terciles; tier_now =
#  the SAME then-cutpoints applied to the now spend value - answers the spend/tier trajectory
#  question spend tier alone cannot. KEEPS ITS NAME - Maya's heatmap source.)
#  group x metric x period(then/now/delta) -> value          -> Cell [15b] -> b_delta_summary.csv
#  long/pivot-ready: STAYERS, LEAVERS_ALL, one row-group per cards_unsub_mne with >=500 leavers,
#  smaller ones pooled LEAVERS_OTHER - n_clients, spend_3mo avg+median, prof_annual avg+median,
#  prod_cnt avg, pct_held T/I/B/C, pct_revolver/transactor/dormant, pct_no_ucp_match.
#
#  PIECE C (trailing 12 months, monthly, its own time axis — outside Windows A and B)
#  ---------------------------------------------------------------------------------------------
#  sends + unsubs_attributed by CALENDAR month of the event -> Cell [10] -> c_monthly_curve.csv
#  x mne, server-side aggregate, no client-grain landing
#
#  DELIVERY
#  ---------------------------------------------------------------------------------------------
#  one xlsx bundling all eight CSVs above                   -> Cell [16] -> spotlight_unified.xlsx
#  coverage / row-count self-check                          -> Cell [17] -> printed only
#  auto-retirement of superseded HDFS dirs (v2/v1 Piece B)  -> Cell [18] -> hdfs dirs deleted
#
# ============================================================================================
# TRAP LEDGER — which trap, guarded where (brief TRAPS section + AUDIT_2026-08-02.md)
# ============================================================================================
#
#  1. Mnemonic/shape filter everywhere        -> TACTIC_ID_SQL appended to every ek/cohort_ek
#                                                 CTE's WHERE (Cells 2,3,4,6b,12,13,14).
#  2. Attribution vs exposure conflation       -> A2 (Cell 7) carries senders / unsubs_attributed /
#                                                 leavers_exposed as three NAMED columns, never a
#                                                 bare "unsubs" column.
#  3. Per-list unsub (~97% evidence)           -> Piece A's unsub_flag_any is ANY-list (all mnes,
#                                                 unfiltered by mne in the ek CTE) -> Cell [2]/[3].
#                                                 A1's enterprise total (Cell 6) is a CLIENT-GRAIN
#                                                 dedup, not a sum of per-mne counts (which would
#                                                 double-count multi-list unsubscribers). Cell [6b]
#                                                 (2026-08-03d) re-derives the same ENTERPRISE dedup
#                                                 independently and asserts it against Cell [6]'s
#                                                 number - a second, differently-shaped proof of the
#                                                 same client-grain-dedup discipline.
#  4. Left truncation on any lookback crossing -> Piece C's trailing-12m floor (2025-08-01) sits
#     the Aug-2025 data floor                    exactly AT the data floor, not before it (12m
#                                                 ending ~Aug 2026 needs no data earlier than the
#                                                 floor). Piece B's cohort/leaver pull floors at
#                                                 2024-01-01 (repo hard floor) with its own
#                                                 MASTER_FLOOR margin (Cell [12]).
#  5. MASTER join not 1:1 (~11% inflation)     -> DISTINCT subquery in every MASTER join
#                                                 (Cells 2,3,4,6b,12,13,14), copied verbatim from
#                                                 spotlight.py.
#  6. Bite predicate must sit inside the       -> MOD(ABS(CLNT_NO), N_BITES) lives inside the
#     MASTER subquery (spool 2646)                MASTER DISTINCT subquery in every pull, never
#                                                 in an outer WHERE. Same fix DFP/BHV inherit via
#                                                 the cohort CTE's own MASTER-scoped bite.
#  7. Event grain (client, treatment,          -> Every ek/cohort_ek CTE: GROUP BY 1,2,3(,4),
#     disposition, DAY) — evergreen id reuse      CAST(disposition_dt_tm AS DATE).
#  8. Non-dated ids = residue                  -> Every date anchored on disposition_dt_tm; the
#                                                 TREATMENT_ID is read only for its mnemonic.
#  9. Piece B anchor floating to "now"         -> T0_ANCHOR_B / T1_ANCHOR_B are literal dates
#     (spotlight2.py's defect #1)                 (2025-06-30 / 2026-06-30), never derived from
#                                                 datetime.date.today() anywhere in Cell [0]. BOTH
#                                                 are CLOSED months as of this build (2026-08-03e) -
#                                                 the regime/resume machinery that guarded a
#                                                 future-dated t12 (P12_CLOSE_DATE_B, _write_regime_
#                                                 flag/_landed_b_offset) is DEAD CODE now that
#                                                 neither anchor can be future-dated, and was
#                                                 deleted this build (Cell [1]) - plain _landed()
#                                                 markers suffice, same as every other pull in this
#                                                 file. RUN_DATE (provenance stamp only) is the one
#                                                 place today() is used, and it never feeds an
#                                                 anchor. SAME FIX applied to Piece A's UCP snapshot
#                                                 (red-team BLOCKER, prior review): UCP_MONTH_A
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
# 14. Two ex-FWC sets, easy to swap            -> CARDS_EX_FWC (CARDS_MNES/32 minus FWC, flag basis)
#     (2026-08-03d, new this build)               feeds ONLY A1 (Cell [2]) / A4 (Cell [9]).
#                                                 CARDS_MKT_EX_FWC (CARDS_MKT_MNES/22 minus FWC,
#                                                 marketing basis) feeds ONLY B_COHORT (Cell [12])
#                                                 and the LOB-dedup pull's CARDS_EX_FWC row (Cell
#                                                 [6b]) - both marketing-scoped by construction.
#                                                 Named distinctly on purpose so a copy-paste never
#                                                 silently reintroduces the 10 non-marketing mnes
#                                                 into a marketing-scoped pull, or vice versa.
#
# ============================================================================================
# WHAT THIS FILE REUSES VERBATIM FROM spotlight.py / spotlight2.py (do not redesign these)
# ============================================================================================
# - The ek CTE dedup grain (client, treatment, disposition, DAY) — spotlight.py Cell [1] Pull A.
# - MASTER DISTINCT subquery with the bite predicate INSIDE it — same file, same cell.
# - MASTER_FLOOR margin logic (load_tm lags disposition_dt_tm; 3-month margin) — spotlight.py
#   Cell [0] comment on MASTER_FLOOR.
# - TACTIC_ID_SHAPE_ONLY / TACTIC_ID_SQL — spotlight.py Cell [0], verbatim.
# - CARDS_MNES origin — spotlight.py Cell [0] (was 12, verbatim). SUPERSEDED 2026-08-03d: now the
#   32-code catalog Andre transcribed (MNE_CATALOG, includes regulatory/operational/fulfillment -
#   those must stay seeable/isolatable, not dropped), plus CARDS_MKT_MNES (22, marketing-only,
#   Piece B's cohort scope) and two ex-FWC twins on different bases. See Cell [0] comments.
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
# - Spend-tier tercile cut, held fixed at the earlier period (t0 -> THEN, same method), zero-spend
#   clients kept as zeros — spotlight2.py Cell [7].
#
# ============================================================================================
# WHAT THIS FILE DOES NOT DO, AND WHY (stated up front, per house rule — never buried)
# ============================================================================================
# - REGULATORY_MNES tagging (spotlight.py carried this) is dropped — no ask in the unified brief
#   touches regulatory-campaign segmentation.
# - RETIRED THIS BUILD (2026-08-03e, Piece B then->now->delta redesign — see Cell [18] for the
#   mechanical cleanup):
#   * The Aug-anchor design (T0_ANCHOR_B=2025-08-31, remeasure +12m=2026-08-31) is gone. That
#     anchor pair always had a future-dated t12 as of any build before Sept 2026 — this build
#     replaces it with two anchors that are BOTH already closed (2025-06-30 / 2026-06-30), so the
#     comparison is real data now, not arithmetic thinness.
#   * P12_CLOSE_DATE_B / _write_regime_flag / _current_regime / _landed_b_offset (the regime-aware
#     resume machinery that let a September rerun detect "t12 just became real") are deleted —
#     with both anchors closed there is nothing left for that machinery to guard against.
#   * "NO UCP PULL FOR PIECE B" is retired too — this build adds one (Cell [14b], b_ucp_v3/):
#     PROF_TOT_ANNUAL (an ANNUAL PROFITABILITY ESTIMATE, NOT a validated LTV figure — canon
#     reference_ucp_canon.md) and T/I/B/C holding at both closed snapshots (UCP_MONTH_B0/B1, Cell
#     [0]), confirmed inside the live UCP partition range (2023-12-31 -> 2026-06-30, references/
#     ucp/gotchas.md #1).
#
# ENGINE MAP: Cells [1]-[4], [6b], [10]-[14] are Teradata-direct (DTZV01.*, D3CV12A.*, no catalog
# prefix, teradatasql connector). Cell [14b] and Cell [15]/[15b] are PySpark reading HDFS parquet
# (no new Teradata connection). Every other cell is PySpark (YARN, Lumina pre-initialized session)
# reading landed parquet off HDFS. Neither engine follows Trino/Starburst syntax rules
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

# ---- MNE_CATALOG - ALL mnemonics in Andre's 2026-08-03 catalog transcription, (description,
# action_type) per mne. Transcribed EXACTLY - do not paraphrase/reorder. This is the full universe
# used to build CARDS_MNES/CARDS_LOB_ALL below and the LOB-dedup grouping (Cell [6b]).
# COUNT FLAG RESOLVED (2026-08-03): the earlier 31-vs-32 mismatch was a dictation drop - VIF was
# missing from the catalog relayed to Claude, even though it is in the source photo AND was in the
# ORIGINAL 12-code cards flag this file started from. VIF added below; catalog is now verified at
# 32 = 31 photo codes (including CLI, kept per Andre - no recent deployments) + VIF.
MNE_CATALOG = {
    "FWC": ("RBC X VISA FIFA Campaign", "Pre_Attract"),
    "VIF": ("Info Protector", "Attract"),
    "PCQ": ("Credit Card Opportunity", "Attract"),
    "PCL": ("Credit Card Limit Increase Opportunity", "Deepen"),
    "PCD": ("Credit Card Best Fit Check", "Deepen"),
    "COB": ("Personal Cards Onboarding", "Onboard"),
    "CRV": ("Credit card installment plan offer", "Deepen"),
    "VBA": ("Business Credit Card Opportunity", "Attract"),
    "WJR": ("WestJet Retention Trigger Monthly Campaign", "Retain"),
    "MWA": ("Mobile Wallet Activity", "Onboard"),
    "VBU": ("Business Cards Upgrade", "Deepen"),
    "CEC": ("CRO Reminder Emails on Retail PreApproved Credit", "Attract"),
    "AUH": ("Card Authorized User Acquisition", "Deepen"),
    "CLL": ("Credit Card limit increase nurture", "Attract"),
    "MVP": ("MVP Tests for Next Best Action", "Attract"),
    "BCO": ("Business Cards Onboarding", "Onboard"),
    "VLI": ("Business Credit Card Limit Increase", "Deepen"),
    "WJF": ("WestJet Engagement Trigger Fulfillment", "Fulfillment"),
    "WJA": ("WestJet Acquisition Offer Fulfillment", "Fulfillment"),
    "POT": ("Pontiac Life Cycle Trigger Onboarding Emails", "Onboard"),
    "MET": ("moi RBC Visa Onboarding", "Deepen"),
    "WNH": ("Remediation ENOVA WATERLOO NORTH HYDRO", "Operational"),
    "OTC": ("Regulatory notification on changes to T and Cs", "Regulatory"),
    "RPF": ("PBA Retention Fulfillment", "Fulfillment"),
    "HCD": ("Medical and Dental Student Offers", "Fulfillment"),
    "VCL": ("Credit Card Limit Increase Opportunity", "Deepen"),
    "BAF": ("British Airways Fulfillment Process", "Fulfillment"),
    "CRO": ("Retail PreApproved Credit for New Clients", "Attract"),
    "AML": ("AML Client ID not compliant", "Regulatory"),
    "MEF": ("Metro Employee Application Bonus Fulfillment", "Fulfillment"),
    "PON": ("Project Pontiac Bill 96 Client Monitoring", "Regulatory"),
    "CLI": ("Credit Limit Increase", "Deepen"),   # kept per Andre 2026-08-03 - no recent deployments.
}
assert len(MNE_CATALOG) == 32, "MNE_CATALOG should hold exactly 32 mnes - recount before running"
CARDS_LOB_ALL = frozenset(MNE_CATALOG)

# ---- CARDS_MNES - the "cards" flag scope. SCOPE CHANGE (Andre 2026-08-03, supersedes an earlier
# 12-mne verbatim list AND a since-reverted 22-mne marketing-only draft): CARDS_MNES is now ALL 32
# catalog codes - regulatory/operational/fulfillment mnes STAY IN the cards_unsub_flag/n_emails_cards
# population, because they must be SEEABLE, not silently dropped. Isolating the marketing-only or
# non-marketing-only view happens via the twin columns below (Cell [2]), not via shrinking this set.
CARDS_MNES = CARDS_LOB_ALL
assert len(CARDS_MNES) == 32, "CARDS_MNES should hold exactly 32 MNEs (== MNE_CATALOG) - recount before running"
CARDS_LIST_SQL = ", ".join("'%s'" % m for m in sorted(CARDS_MNES))

# ---- CARDS_NONMKT - the 10 non-marketing codes (Regulatory/Operational/Fulfillment action types)
# inside CARDS_MNES. Powers the nonmkt twin columns (Cell [2]/[9]) that isolate this slice without
# a second query. ----
CARDS_NONMKT = frozenset({"OTC", "AML", "PON", "WNH", "WJF", "WJA", "RPF", "HCD", "BAF", "MEF"})
assert len(CARDS_NONMKT) == 10, "CARDS_NONMKT should hold exactly 10 MNEs - recount before running"
CARDS_NONMKT_LIST_SQL = ", ".join("'%s'" % m for m in sorted(CARDS_NONMKT))

# ---- CARDS_MKT_MNES - marketing-only subset (CARDS_MNES minus CARDS_NONMKT). Used ONLY for Piece
# B's cohort definition (Cell [12]) - "Cards-mailed" membership there means CAMPAIGN CONTACT, and a
# regulatory T&C notice or an operational remediation email is not campaign contact; including it
# would silently redefine the Piece-B cohort as "all cardholders touched by any cards-coded mail",
# not "clients marketed to". ASSUMPTION logged 2026-08-03, Andre-overridable. ----
CARDS_MKT_MNES = CARDS_MNES - CARDS_NONMKT
assert len(CARDS_MKT_MNES) == 22, "CARDS_MKT_MNES should hold exactly 22 MNEs - recount before running"
CARDS_MKT_LIST_SQL = ", ".join("'%s'" % m for m in sorted(CARDS_MKT_MNES))

# ---- Two DISTINCT ex-FWC sets - do not collapse into one. FWC (FIFA) is enterprise-wide, not a
# steady-state cards campaign, so isolating it matters on BOTH bases, but the "population minus
# FWC" question means something different depending which population is being asked about:
#   CARDS_EX_FWC     = CARDS_MNES (all 32, the A1/a4 flag population) minus FWC - used by A1's own
#                       ex-FWC twin (Cell [2]) and a4's leavers_cards_ex_fwc (Cell [9]).
#   CARDS_MKT_EX_FWC = CARDS_MKT_MNES (the 22-mne marketing/Piece-B population) minus FWC - used
#                       ONLY by B_COHORT's cards_ex_fwc_unsub_by_anchor (Cell [12]) and the LOB-dedup
#                       pull's CARDS_EX_FWC row (Cell [6b]), both of which are marketing-scoped by
#                       construction. Using the flag-basis set there would silently reintroduce the
#                       10 non-marketing mnes B is deliberately excluding.
CARDS_EX_FWC = CARDS_MNES - {"FWC"}
assert len(CARDS_EX_FWC) == 31, "CARDS_EX_FWC should hold exactly 31 MNEs - recount before running"
CARDS_EX_FWC_LIST_SQL = ", ".join("'%s'" % m for m in sorted(CARDS_EX_FWC))

CARDS_MKT_EX_FWC = CARDS_MKT_MNES - {"FWC"}
assert len(CARDS_MKT_EX_FWC) == 21, "CARDS_MKT_EX_FWC should hold exactly 21 MNEs - recount before running"
CARDS_MKT_EX_FWC_LIST_SQL = ", ".join("'%s'" % m for m in sorted(CARDS_MKT_EX_FWC))


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

# ---- WINDOW B - Piece B, REDESIGNED 2026-08-03e ("then vs now vs delta"). Both anchors HARDCODED
# and BOTH CLOSED as of this build - the fix for spotlight2.py's defect #1 (anchor floated to
# "now") plus the retirement of the Aug-anchor design's regime/resume machinery (Cell [1]): with
# neither anchor able to be future-dated, plain _landed() markers are enough, same as every other
# pull in this file. Never derive these two dates from datetime.date.today(). ----
T0_ANCHOR_B = datetime.date(2025, 6, 30)   # "then" - closed.
T1_ANCHOR_B = datetime.date(2026, 6, 30)   # "now" - closed as of this build's run date (2026-08-03).
PERIODS_B = ["then", "now"]
ANCHOR_B = {"then": T0_ANCHOR_B, "now": T1_ANCHOR_B}
ANCHOR_DATES_SQL_B = ", ".join("DATE '%s'" % ANCHOR_B[p].isoformat() for p in PERIODS_B)

# Cohort membership window: "clients mailed by CARDS_MKT_MNES (marketing-only) before the anchor" -
# own floor, the repo hard floor (2024_data_floor memory), not derived from WIN_A/WIN_C.
COHORT_B_FLOOR = "2024-01-01"
assert datetime.date.fromisoformat(COHORT_B_FLOOR) >= datetime.date(2024, 1, 1), \
    "repo floor is 2024-01-01 - do not go below"
ANCHOR_B_CEIL = (T0_ANCHOR_B + datetime.timedelta(days=1)).isoformat()   # "on or before anchor" (half-open, "2025-07-01")
MASTER_FLOOR_B = _months_before(COHORT_B_FLOOR, 3)

# Spend windows - Andre's order (2026-08-03e ask): a light 3-month window around EACH anchor, not a
# trailing-12m lookback (the Aug-anchor design's ANNUAL_YMS_B/SPEND_YMS_B machinery is retired -
# lighter windows need no lookback-months constant or repo-floor clamp). Hand-typed per Andre's
# explicit spec, not derived, because these are the ask itself, not a computed consequence of it.
SPEND_YMS_THEN = [202504, 202505, 202506]   # Apr-Jun 2025, around T0_ANCHOR_B.
SPEND_YMS_NOW = [202604, 202605, 202606]    # Apr-Jun 2026, around T1_ANCHOR_B.
SPEND_YMS_THEN_SQL = ", ".join(str(y) for y in SPEND_YMS_THEN)
SPEND_YMS_NOW_SQL = ", ".join(str(y) for y in SPEND_YMS_NOW)
_SPEND_YMS_B_ALL = sorted(set(SPEND_YMS_THEN) | set(SPEND_YMS_NOW))
SPEND_YMS_B_SQL = ", ".join(str(y) for y in _SPEND_YMS_B_ALL)


def _ym_to_month_start(ym):
    return datetime.date(ym // 100, ym % 100, 1)


SPEND_FLOOR_B = _ym_to_month_start(_SPEND_YMS_B_ALL[0]).isoformat()                       # "2025-04-01"
SPEND_CEIL_B = _add_months(_ym_to_month_start(_SPEND_YMS_B_ALL[-1]), 1).isoformat()        # "2026-07-01" (exclusive)
assert datetime.date.fromisoformat(SPEND_FLOOR_B) >= datetime.date(2024, 1, 1), \
    "SPEND_FLOOR_B is below the repo 2024-01-01 floor - recheck SPEND_YMS_THEN/NOW"

# UCP snapshot months - PIECE B (Cell [14b]). Same then/now anchors as B_COHORT/B_DFP/B_BHV.
# Confirmed inside the live UCP partition range (2023-12-31 -> 2026-06-30, references/ucp/
# gotchas.md #1, 2026-07-27 probe) - UCP_MONTH_B1 sits exactly at that range's upper edge.
# UCP_MONTH_A (Piece A, Cell [0] above) is UNTOUCHED by this Piece-B redesign.
UCP_MONTH_B0 = "2025-06-30"
UCP_MONTH_B1 = "2026-06-30"
UCP_MONTH_B = {"then": UCP_MONTH_B0, "now": UCP_MONTH_B1}

# ---- Behaviour segment precedence - verbatim from spotlight2.py Cell [0] (UNVERIFIED U1/U3
# there; unresolved here too - carried forward, not re-litigated). ----
SEG_PRECEDENCE = [("Revolver", 1), ("Transactor", 2), ("Dormant", 3)]
SEG_LABEL = {1: "Revolver", 2: "Transactor", 3: "Dormant", 4: "other_or_none", 0: "no_data"}

# ---- Spend tiers - verbatim method from spotlight2.py Cell [7]: tercile of spend, cut ONCE at
# the THEN anchor (spend_3mo_then), held fixed for the NOW-side comparison (tier_now). ----
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
RUN_PULLS = ["A1", "A2", "C", "B_COHORT", "B_DFP", "B_BHV", "B_UCP"]   # restrict which pulls Cells run.
UCP_MATCH_FLOOR = 70.0   # catches a broken join key, not ordinary UCP attrition.

# ---- Paths - HDFS only. Own namespace, does not collide with spotlight.py/spotlight2.py. ----
BASE = "hdfs:///user/427966379/unsub_unified/"      # reference_andre_hdfs_user_path.md
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"           # references/ucp/README.md - personal only
SCHEMA_VERSION = 1        # A2/C ONLY - their SQL has no CARDS_MNES dependency (verified 2026-08-03d
                           # while making this change), so a cards-list update never re-lands them.
CARDS_SCHEMA_VERSION = 2  # A1 ONLY (2026-08-03e: SPLIT from Piece B's version below). A-side is
                           # UNTOUCHED by the Piece-B then/now/delta redesign - a1_client_v2 stays
                           # exactly as landed, no re-pull, so CARDS_SCHEMA_VERSION must NOT move
                           # in lockstep with Piece B's schema changes anymore.
B_SCHEMA_VERSION = 3      # B_COHORT/B_DFP/B_BHV/B_UCP ONLY - bumped 2026-08-03e for the then/now/
                           # delta redesign (new anchors, new columns, new UCP pull). Was unified
                           # with CARDS_SCHEMA_VERSION (both =2) before this build; split so a
                           # future Piece-B-only change never forces an A1 re-pull, and vice versa.
                           # The old v2 B dirs (b_cohort_v2/b_dfp_v2/b_bhv_v2, Aug-anchor era) are
                           # superseded by this split and auto-deleted by Cell [18].

A1_DIR = BASE + "a1_client_v%d/" % CARDS_SCHEMA_VERSION
A2_DIR = BASE + "a2_mne_v%d/" % SCHEMA_VERSION
C_DIR = BASE + "c_month_mne_v%d/" % SCHEMA_VERSION
BCOHORT_DIR = BASE + "b_cohort_v%d/" % B_SCHEMA_VERSION
BDFP_DIR = BASE + "b_dfp_v%d/" % B_SCHEMA_VERSION
BBHV_DIR = BASE + "b_bhv_v%d/" % B_SCHEMA_VERSION
BUCP_DIR = BASE + "b_ucp_v%d/" % B_SCHEMA_VERSION   # NEW 2026-08-03e - dual UCP snapshot, Cell [14b].
UCPA_DIR = BASE + "ucp_enriched_a3_v%d/" % SCHEMA_VERSION  # bitten UCP-join output, Cell [5].
# (a3, 2026-08-03d: name bumped from ucp_enriched_a2 - upstream a1_client moved to
# CARDS_SCHEMA_VERSION=2 for the cards-list scope change; new namespace avoids any ambiguity about
# which a1_client vintage a given ucp_enriched_a bite was joined against, same reasoning as the
# prior a -> a2 bump below. Only the join's LEFT side gained columns - id set/join logic unchanged.)
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
PIPELINE_BUILD = ("build 2026-08-03e | Piece B then->now->delta (anchors 2025-06-30 -> 2026-06-30, "
                   "both closed), campaign dim, dual UCP snapshots, auto-retirement")
print("=" * 88)
print("PIPELINE_BUILD:", PIPELINE_BUILD)
print("=" * 88)
print("CONFIG loaded | WIN_A:", WIN_A_FLOOR, "->", WIN_A_CEIL,
      "| WIN_C:", WIN_C_FLOOR, "->", WIN_C_CEIL,
      "| ANCHOR_B: then", T0_ANCHOR_B.isoformat(), "-> now", T1_ANCHOR_B.isoformat(), "(both closed)")
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
    # NEVER "bite_*" - that wildcard also matches bite_K_ROWCOUNT sidecars (and, on any
    # pre-2026-08-03e Piece-B dir still on HDFS, the now-retired _REGIME sidecar) and
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
# cards_unsub_flag/n_emails_cards are now scoped by CARDS_MNES = ALL 32 catalog mnes (2026-08-03d
# scope change - regulatory/operational/fulfillment stay IN, isolatable via two twin pairs added
# this build: n_emails_cards_ex_fwc/cards_ex_fwc_unsub_flag (flag basis minus FWC, CARDS_EX_FWC)
# and n_emails_cards_nonmkt/cards_nonmkt_unsub_flag (the 10 non-marketing mnes, CARDS_NONMKT).

A1_SCHEMA = StructType([
    StructField("clnt_no", LongType(), True),
    StructField("unsub_flag_any", IntegerType(), True),
    StructField("cards_unsub_flag", IntegerType(), True),
    StructField("n_emails_all", LongType(), True),
    StructField("n_emails_cards", LongType(), True),
    StructField("n_emails_cards_ex_fwc", LongType(), True),        # NEW 2026-08-03d - CARDS_EX_FWC twin
    StructField("cards_ex_fwc_unsub_flag", IntegerType(), True),   # NEW 2026-08-03d - CARDS_EX_FWC twin
    StructField("n_emails_cards_nonmkt", LongType(), True),        # NEW 2026-08-03d - CARDS_NONMKT twin
    StructField("cards_nonmkt_unsub_flag", IntegerType(), True),   # NEW 2026-08-03d - CARDS_NONMKT twin
])


def _prep_a1(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    _n_null = pd.to_numeric(pdf["clnt_no"], errors="coerce").isna().sum()
    assert _n_null == 0, "clnt_no has %d nulls - CLNT_NO IS NOT NULL filter is not firing" % _n_null
    pdf["clnt_no"] = pd.to_numeric(pdf["clnt_no"], errors="coerce").astype("int64")
    for _c in ["unsub_flag_any", "cards_unsub_flag", "cards_ex_fwc_unsub_flag", "cards_nonmkt_unsub_flag"]:
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce").fillna(0).astype("int32")
    for _c in ["n_emails_all", "n_emails_cards", "n_emails_cards_ex_fwc", "n_emails_cards_nonmkt"]:
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce").fillna(0).astype("int64")
    return pdf[[f.name for f in A1_SCHEMA.fields]]


def land_a1_bite(bite):
    name = "a1_client_v%d/bite_%d" % (CARDS_SCHEMA_VERSION, bite)
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
           SUM(CASE WHEN disposition_cd = 1 AND mne IN (%(cards)s) THEN 1 ELSE 0 END) AS n_emails_cards,
           SUM(CASE WHEN disposition_cd = 1 AND mne IN (%(cards_ex_fwc)s) THEN 1 ELSE 0 END) AS n_emails_cards_ex_fwc,
           MAX(CASE WHEN disposition_cd = 4 AND mne IN (%(cards_ex_fwc)s) THEN 1 ELSE 0 END) AS cards_ex_fwc_unsub_flag,
           SUM(CASE WHEN disposition_cd = 1 AND mne IN (%(cards_nonmkt)s) THEN 1 ELSE 0 END) AS n_emails_cards_nonmkt,
           MAX(CASE WHEN disposition_cd = 4 AND mne IN (%(cards_nonmkt)s) THEN 1 ELSE 0 END) AS cards_nonmkt_unsub_flag
    FROM joined
    GROUP BY clnt_no
    """ % {"floor": WIN_A_FLOOR, "ceil": WIN_A_CEIL, "mfloor": MASTER_FLOOR_A, "tactic": TACTIC_ID_SQL,
           "n_bites": N_BITES, "bite": bite, "cards": CARDS_LIST_SQL,
           "cards_ex_fwc": CARDS_EX_FWC_LIST_SQL, "cards_nonmkt": CARDS_NONMKT_LIST_SQL}
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
    _bite_name = "ucp_enriched_a3_v%d/bite_%d" % (SCHEMA_VERSION, _bite)
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


# %% [6b] LOB DEDUP PULL - one Teradata aggregated query, WINDOW A, unique unsub clients (cd=4)
# under FIVE labeled groupings - each an exact client-grain COUNT(DISTINCT clnt_no), same dedup
# guarantee as A1's ENTERPRISE_TOTAL/CARDS_TOTAL rows (Cell [6]), never a per-mne sum. Reuses the
# ek/MASTER DISTINCT join pattern (trap #1 shape filter via TACTIC_ID_SQL, trap #5 DISTINCT
# subquery) - NOT bitten (tiny wire cost, five COUNT(DISTINCT) numbers back, same cost class as
# Pull C's fan-out guard diagnostic, Cell [4]/[10]).
#   ENTERPRISE       - no mne filter (still shape-filtered) - cross-checks A1's
#                       ENTERPRISE_TOTAL_UNIQUE_CLIENTS row, same window/population.
#   CARDS_LOB_ALL(32) - CARDS_LIST_SQL, the full flag population (Cell [0]).
#   CARDS_MKT(22)     - CARDS_MKT_LIST_SQL, marketing-only (same set Piece B's cohort uses).
#   CARDS_EX_FWC(21)  - CARDS_MKT_EX_FWC_LIST_SQL - MARKETING basis minus FWC (matches B_COHORT's
#                       cards_ex_fwc_unsub_by_anchor twin, NOT A1's flag-basis CARDS_EX_FWC twin -
#                       see the two-ex-FWC-sets comment in Cell [0]).
#   CARDS_NONMKT(10)  - CARDS_NONMKT_LIST_SQL, the 10 regulatory/operational/fulfillment mnes.
# The CARDS_LOB_ALL/OTHER split is built via CASE on SUBSTR(TREATMENT_ID,8,3), per spec; the other
# three rows are simple mne-IN filters over the same `joined` CTE (no need to re-tag). Label column
# CAST to VARCHAR(40) in every branch (Teradata UNION ALL truncation rule - canon
# teradata_pushdown notes - the first SELECT sets the width for every branch that follows).
# ENGINE: Teradata-direct.

_lob_dedup_sql = """
WITH ek AS (
    SELECT consumer_id_hashed, TREATMENT_ID,
           MIN(disposition_dt_tm) AS disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '%(floor)s'
      AND disposition_dt_tm <  DATE '%(ceil)s'%(tactic)s
    GROUP BY 1, 2, CAST(disposition_dt_tm AS DATE)
),
joined AS (
    SELECT m.CLNT_NO AS clnt_no,
           SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne
    FROM ek
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE load_tm >= DATE '%(mfloor)s'
                  AND CLNT_NO IS NOT NULL) m
      ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
),
lob_tagged AS (
    SELECT clnt_no,
           CASE WHEN mne IN (%(lob_all)s) THEN 'CARDS_LOB' ELSE 'OTHER' END AS lob_group
    FROM joined
)
SELECT CAST('ENTERPRISE' AS VARCHAR(40)) AS label, COUNT(DISTINCT clnt_no) AS unique_unsub_clients
FROM joined
UNION ALL
SELECT CAST('CARDS_LOB_ALL' AS VARCHAR(40)), COUNT(DISTINCT clnt_no)
FROM lob_tagged WHERE lob_group = 'CARDS_LOB'
UNION ALL
SELECT CAST('CARDS_MKT' AS VARCHAR(40)), COUNT(DISTINCT clnt_no)
FROM joined WHERE mne IN (%(mkt)s)
UNION ALL
SELECT CAST('CARDS_EX_FWC' AS VARCHAR(40)), COUNT(DISTINCT clnt_no)
FROM joined WHERE mne IN (%(mkt_ex_fwc)s)
UNION ALL
SELECT CAST('CARDS_NONMKT' AS VARCHAR(40)), COUNT(DISTINCT clnt_no)
FROM joined WHERE mne IN (%(nonmkt)s)
""" % {"floor": WIN_A_FLOOR, "ceil": WIN_A_CEIL, "mfloor": MASTER_FLOOR_A, "tactic": TACTIC_ID_SQL,
       "lob_all": CARDS_LIST_SQL, "mkt": CARDS_MKT_LIST_SQL, "mkt_ex_fwc": CARDS_MKT_EX_FWC_LIST_SQL,
       "nonmkt": CARDS_NONMKT_LIST_SQL}

_lob_dedup_pdf = edw_pd(_lob_dedup_sql)
_lob_dedup_pdf.columns = [c.lower() for c in _lob_dedup_pdf.columns]
assert len(_lob_dedup_pdf) == 5, (
    "a1_lob_dedup pulled %d rows, expected exactly 5 (ENTERPRISE/CARDS_LOB_ALL/CARDS_MKT/"
    "CARDS_EX_FWC/CARDS_NONMKT) - check the UNION ALL." % len(_lob_dedup_pdf))
_lob_dedup_pdf["unique_unsub_clients"] = pd.to_numeric(
    _lob_dedup_pdf["unique_unsub_clients"], errors="coerce").fillna(0).astype("int64")
print("A1_LOB_DEDUP | grain: one row per label | WIN_A Jan-Apr 2026 | 5 rows")
print(_lob_dedup_pdf.to_string(index=False))

_ent_lob_row = _lob_dedup_pdf.loc[_lob_dedup_pdf["label"] == "ENTERPRISE", "unique_unsub_clients"]
if len(_ent_lob_row) and "_enterprise_unsubs" in globals():
    _ent_lob = int(_ent_lob_row.iloc[0])
    if _ent_lob != _enterprise_unsubs:
        print("NOTE: a1_lob_dedup ENTERPRISE (%d) vs a1_mne_share's ENTERPRISE_TOTAL_UNIQUE_CLIENTS "
              "(%d) differ - both are supposed to be the same client-grain dedup over the same WIN_A "
              "window; investigate before trusting either if this drifts." % (_ent_lob, _enterprise_unsubs))
    else:
        print("Cross-check OK: a1_lob_dedup ENTERPRISE matches a1_mne_share's "
              "ENTERPRISE_TOTAL_UNIQUE_CLIENTS (%d)." % _ent_lob)

_LOB_DEDUP_SCHEMA = StructType([
    StructField("label", StringType(), True),
    StructField("unique_unsub_clients", LongType(), True),
])
a1_lob_dedup = spark.createDataFrame(_lob_dedup_pdf[["label", "unique_unsub_clients"]],
                                      schema=_LOB_DEDUP_SCHEMA)
a1_lob_dedup_stamped = _stamp(a1_lob_dedup, "WIN_A Jan-Apr 2026",
                               "enterprise-wide, shape-filtered, five LOB groupings")
a1_lob_dedup_pd = a1_lob_dedup_stamped.toPandas()
write_cube(a1_lob_dedup_stamped, "a1_lob_dedup")


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
                         F.count("*").alias("clients_total"),
                         F.sum("n_emails_all").alias("sum_emails_all"),        # NEW 2026-08-03d
                         F.sum("n_emails_cards").alias("sum_emails_cards"))    # NEW 2026-08-03d
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
# 2026-08-03d ADDITIONS (appended at the end of the agg list, existing columns untouched):
# leavers_cards_ex_fwc (flag-basis CARDS_EX_FWC subset), leavers_cards_nonmkt (CARDS_NONMKT
# subset), sum_emails_all / sum_emails_cards (per-profile email volume, mirrors A3's additions).
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
    _ucp_bite_path = "ucp_enriched_a3_v%d/bite_%d" % (SCHEMA_VERSION, _bite)
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
             F.count("*").alias("clients_total"),
             F.sum(F.when((F.col("unsub_flag_any") == 1) & (F.col("cards_ex_fwc_unsub_flag") == 1), 1)
                   .otherwise(0)).alias("leavers_cards_ex_fwc"),          # NEW 2026-08-03d
             F.sum("n_emails_all").alias("sum_emails_all"),               # NEW 2026-08-03d
             F.sum("n_emails_cards").alias("sum_emails_cards"),           # NEW 2026-08-03d
             F.sum(F.when((F.col("unsub_flag_any") == 1) & (F.col("cards_nonmkt_unsub_flag") == 1), 1)
                   .otherwise(0)).alias("leavers_cards_nonmkt")))         # NEW 2026-08-03d
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
                         F.sum("clients_total").alias("clients_total"),
                         F.sum("leavers_cards_ex_fwc").alias("leavers_cards_ex_fwc"),   # NEW 2026-08-03d
                         F.sum("sum_emails_all").alias("sum_emails_all"),               # NEW 2026-08-03d
                         F.sum("sum_emails_cards").alias("sum_emails_cards"),           # NEW 2026-08-03d
                         F.sum("leavers_cards_nonmkt").alias("leavers_cards_nonmkt"))   # NEW 2026-08-03d
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


# %% [12] PULL B_COHORT (v3, 2026-08-03e redesign) - client-grain, cohort SCOPING pull. Defines
# the Piece-B population (clients mailed by CARDS_MKT_MNES on/before T0_ANCHOR_B) AND the leaver
# flags at anchor, in the SAME pull that scopes it - this is the fix for spotlight2.py's defect #4
# ("pulls bank-wide, cards flag applied post-hoc"). The WHERE mailed_cards = 1 at the very end IS
# the scoping: only cohort clients are landed, nothing bank-wide ever reaches HDFS.
# MARKETING-ONLY SCOPE (2026-08-03d, Andre-overridable assumption, Cell [0]): mailed_cards /
# cards_unsub_by_anchor use CARDS_MKT_MNES (22 mnes), NOT the 32-mne CARDS_MNES flag A1 uses - a
# regulatory T&C notice or an operational remediation email is not campaign contact, and folding
# those in would silently redefine "Cards-mailed" as "any cards-coded mail, including non-marketing".
#
# LEAVER FLAG has its OWN window (COHORT_B_FLOOR -> anchor+1day), separate from Piece A/C's
# windows - the fix for spotlight2.py's defect #2 ("do NOT reuse a flag computed over the full
# year"). any_unsub_by_anchor is enterprise-wide (any mne); cards_unsub_by_anchor is the
# marketing-cards subset; cards_ex_fwc_unsub_by_anchor is the same subset minus FWC (CARDS_MKT_EX_
# FWC, marketing basis - see the two-ex-FWC-sets comment in Cell [0]).
#
# cards_unsub_mne (NEW 2026-08-03e) - the campaign dimension for Cell [15b]'s per-mne breakdown: the
# MNE of the client's EARLIEST cards-marketing (cd=4, mne IN CARDS_MKT_MNES) unsub event on/before
# the anchor, via ROW_NUMBER() PARTITION BY clnt_no ORDER BY dt ASC (same ranked-CTE-then-filter
# pattern Cell [13]'s DFP pivot already uses, not QUALIFY - this file never uses QUALIFY, matching
# the rest of its Teradata-direct SQL). NULL for anyone with cards_unsub_by_anchor = 0 (stayers, by
# this artifact's definition - see Cell [15]/[15b] header for the STAYERS/LEAVERS_ALL scoping
# assumption, Andre-overridable). ENGINE: Teradata-direct.

BCOHORT_SCHEMA = StructType([
    StructField("clnt_no", LongType(), True),
    StructField("any_unsub_by_anchor", IntegerType(), True),
    StructField("cards_unsub_by_anchor", IntegerType(), True),
    StructField("cards_ex_fwc_unsub_by_anchor", IntegerType(), True),
    StructField("cards_unsub_mne", StringType(), True),   # NEW 2026-08-03e - campaign dim, NULL for stayers
])


def _prep_bcohort(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    _n_null = pd.to_numeric(pdf["clnt_no"], errors="coerce").isna().sum()
    assert _n_null == 0, "clnt_no has %d nulls - CLNT_NO IS NOT NULL filter is not firing" % _n_null
    pdf["clnt_no"] = pd.to_numeric(pdf["clnt_no"], errors="coerce").astype("int64")
    for _c in ["any_unsub_by_anchor", "cards_unsub_by_anchor", "cards_ex_fwc_unsub_by_anchor"]:
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce").fillna(0).astype("int32")
    # cards_unsub_mne - genuine NULL for stayers, never a sentinel string. Same None-not-NaN
    # discipline as B_DFP's annual/spend NULLs (Cell [13]) so F.col(...).isNull() actually catches it.
    pdf["cards_unsub_mne"] = pdf["cards_unsub_mne"].astype(object)
    pdf.loc[pdf["cards_unsub_mne"].isna(), "cards_unsub_mne"] = None
    return pdf[[f.name for f in BCOHORT_SCHEMA.fields]]


def land_bcohort_bite(bite):
    name = "b_cohort_v%d/bite_%d" % (B_SCHEMA_VERSION, bite)
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
               ek.dt AS dt,
               ek.TREATMENT_ID AS treatment_id
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
               MAX(CASE WHEN disposition_cd = 4 AND mne IN (%(cards)s) THEN 1 ELSE 0 END) AS cards_unsub_by_anchor,
               MAX(CASE WHEN disposition_cd = 4 AND mne IN (%(cards_ex_fwc)s) THEN 1 ELSE 0 END) AS cards_ex_fwc_unsub_by_anchor
        FROM joined
        GROUP BY clnt_no
    ),
    cards_unsub_ranked AS (
        -- Deterministic tie-break (red-team Blocker 2): a client with two cards-marketing unsubs
        -- on the SAME dt (calendar day, per the ek CTE's own dedup grain) needs a stable pick, not
        -- whatever order Teradata happens to return - ORDER BY dt, then mne, then TREATMENT_ID
        -- fully orders every row, so cards_unsub_mne is reproducible run-to-run.
        SELECT clnt_no, mne,
               ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY dt ASC, mne ASC, treatment_id ASC) AS rn
        FROM joined
        WHERE disposition_cd = 4 AND mne IN (%(cards)s)
    ),
    earliest_cards_unsub AS (
        SELECT clnt_no, mne AS cards_unsub_mne
        FROM cards_unsub_ranked
        WHERE rn = 1
    )
    SELECT f.clnt_no, f.any_unsub_by_anchor, f.cards_unsub_by_anchor, f.cards_ex_fwc_unsub_by_anchor,
           e.cards_unsub_mne
    FROM client_flags f
    LEFT JOIN earliest_cards_unsub e ON e.clnt_no = f.clnt_no
    WHERE f.mailed_cards = 1
    """ % {"floor": COHORT_B_FLOOR, "ceil": ANCHOR_B_CEIL, "mfloor": MASTER_FLOOR_B, "tactic": TACTIC_ID_SQL,
           "n_bites": N_BITES, "bite": bite, "cards": CARDS_MKT_LIST_SQL,
           "cards_ex_fwc": CARDS_MKT_EX_FWC_LIST_SQL}
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
    # ---- PRE-PULL PROBE (red-team guard, downgraded from Blocker to belt-and-braces check) - one
    # cheap Teradata aggregate BEFORE the bite loop touches anything: do pre-anchor cards-marketing
    # send events exist at all in this window? The 2026-08-03d run already proved they do (5.12M-
    # client cohort from 2024+ sends), so this is expected to PASS and print the count + earliest
    # date - it exists to catch a future regression (table access change, a broken shape filter, an
    # empty mne list), not because this run doubts the population. ----
    _precheck_sql = """
    SELECT COUNT(*) AS n_sends, MIN(disposition_dt_tm) AS min_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '%(floor)s'
      AND disposition_dt_tm <  DATE '%(ceil)s'
      AND SUBSTR(TREATMENT_ID, 8, 3) IN (%(cards)s)%(tactic)s
    """ % {"floor": COHORT_B_FLOOR, "ceil": ANCHOR_B_CEIL, "cards": CARDS_MKT_LIST_SQL, "tactic": TACTIC_ID_SQL}
    _precheck_pdf = edw_pd(_precheck_sql)
    _precheck_pdf.columns = [c.lower() for c in _precheck_pdf.columns]
    _n_precheck = int(_precheck_pdf.iloc[0]["n_sends"]) if len(_precheck_pdf) else 0
    _min_dt_precheck = _precheck_pdf.iloc[0]["min_dt"] if len(_precheck_pdf) else None
    if _n_precheck == 0:
        raise RuntimeError(
            "PRE-PULL PROBE FAILED: zero pre-anchor cards-marketing send events (disposition_cd=1, "
            "mne IN CARDS_MKT_MNES, shape-filtered) in %s -> %s - no pre-anchor cards send history, "
            "check window vs data availability before running the bite loop. The 2026-08-03d run "
            "proved these events exist (5.12M-client cohort from 2024+ sends), so a zero here means "
            "something upstream changed (table access, TACTIC_ID_SQL, CARDS_MKT_MNES), not that the "
            "cohort is legitimately empty." % (COHORT_B_FLOOR, ANCHOR_B_CEIL))
    print("PRE-PULL PROBE OK | cards-marketing sends", COHORT_B_FLOOR, "->", ANCHOR_B_CEIL, "|",
          _n_precheck, "events | earliest send:", _min_dt_precheck, "- expected to pass (2026-08-03d "
          "already proved this population exists); this is a regression guard, not a discovery.")

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
    """Single-bite read (not the 'bite_*' glob) - used by Cells [14b]/[15]'s bite-looped builds so
    those cells never touch the full-scale cohort table at once."""
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
cohort_b.groupBy(F.when(F.col("cards_unsub_by_anchor") == 1, "LEAVER").otherwise("STAYER").alias("bucket")).agg(
    F.count("*").alias("clients"), F.sum("any_unsub_by_anchor").alias("any_list_unsub_subset")
).show(truncate=False)
_bcohort_unmapped = cohort_b.filter((F.col("cards_unsub_by_anchor") == 1) & F.col("cards_unsub_mne").isNull()).count()
if _bcohort_unmapped > 0:
    print("WARNING:", _bcohort_unmapped, "clients have cards_unsub_by_anchor=1 but cards_unsub_mne "
          "IS NULL - the earliest-cards-unsub join in Cell [12] missed rows. Investigate before "
          "trusting Cell [15b]'s per-mne breakdown.")


# %% [13] PULL B_DFP (v3, 2026-08-03e redesign) - 3-month spend around T0_ANCHOR_B ("then",
# SPEND_YMS_THEN) and around T1_ANCHOR_B ("now", SPEND_YMS_NOW), cohort-scoped via an embedded
# cohort CTE that INNER JOINs DLY_FULL_PORTFOLIO BEFORE any aggregation - the scoping happens
# inside this ONE Teradata statement, never post-hoc in Spark (fix for defect #4). The cohort CTE
# here is a CHEAP cards-only send check (mirrors spotlight.py Pull C's cost profile), re-derived
# rather than handed off from Cell [12] - keeps this cell self-contained and independently
# resumable. Both windows are NULL (never 0) when a client has zero DFP rows in that window - see
# n_then_rows/n_now_rows below; a closed window can still be legitimately thin for a given client
# (e.g. a closed account), and that must never silently read as "$0 spend".
# ENGINE: Teradata-direct (D3CV12A.DLY_FULL_PORTFOLIO). One scan, ROW_NUMBER pivot, never
# multi-scan (table_catalog_notes.md:147-152) - same pattern as spotlight2.py Cell [3].

assert "ACCUM_VALIDATED" in globals() and ACCUM_VALIDATED, "Run Cell [11] first."

BDFP_SCHEMA = StructType([
    StructField("clnt_no", LongType(), True),
    StructField("n_accts_total", LongType(), True),
    StructField("spend_3mo_then", DoubleType(), True),
    StructField("spend_3mo_now", DoubleType(), True),
])


def _cohort_cte_sql(bite):
    """Cheap, cards-only cohort-membership CTE, shared text between the DFP/BHV/UCP pulls. Kept
    as a Python function (not a stored SQL macro) so all pulls stay self-contained files.
    Half-open on the anchor (< ANCHOR_B_CEIL), matching Cell [12]'s own cohort pull - anchor-day
    off-by-one fix, was <= T0_ANCHOR_B which double-counted anchor-day events across the boundary.
    MARKETING-ONLY (2026-08-03d): uses CARDS_MKT_LIST_SQL, matching Cell [12]'s B_COHORT population
    exactly - this is a self-contained RE-DERIVATION of the same cohort, not a read from Cell [12]'s
    landed table, so the two definitions must stay identical or Piece B's population drifts between
    cells. Using the broader 32-mne flag list here would scan/pull extra accounts DFP/BHV never
    needed (Cell [15] joins onto Cell [12]'s cohort_bite regardless), so this is also a compute fix."""
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
    )""" % {"cfloor": COHORT_B_FLOOR, "anchor_ceil": ANCHOR_B_CEIL, "cards": CARDS_MKT_LIST_SQL,
            "tactic": TACTIC_ID_SQL, "mfloor": MASTER_FLOOR_B, "n_bites": N_BITES, "bite": bite}


_spend_case_then = "SUM(CASE WHEN ym IN (%s) THEN acct_month_spend ELSE 0 END)" % SPEND_YMS_THEN_SQL
_spend_case_now = "SUM(CASE WHEN ym IN (%s) THEN acct_month_spend ELSE 0 END)" % SPEND_YMS_NOW_SQL
# n_then_rows/n_now_rows - count ACTUAL then/now-window DFP rows (not spend dollars) per account.
# The account-level SUM(...ELSE 0) above cannot tell "no DFP data this window" apart from "real $0
# spend" - both look like 0. This count, summed at the client grain below, can: if a client has
# ZERO then (or now) window rows across every account, spend_3mo_then (or _now) must come back
# NULL, never 0.0 - both windows get this guard now (round-2 review BLOCKER fix originally applied
# to t12 only; the then/now redesign needs it symmetrically since neither window is guaranteed to
# be populated for every cohort client, e.g. a closed account).
_then_row_count_case = "SUM(CASE WHEN ym IN (%s) THEN 1 ELSE 0 END)" % SPEND_YMS_THEN_SQL
_now_row_count_case = "SUM(CASE WHEN ym IN (%s) THEN 1 ELSE 0 END)" % SPEND_YMS_NOW_SQL


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
               %(spend_then)s AS spend_3mo_then,
               %(spend_now)s AS spend_3mo_now,
               %(then_rows)s AS n_then_rows,
               %(now_rows)s AS n_now_rows
        FROM acct_month
        GROUP BY clnt_no, acct_no
    )
    SELECT clnt_no,
       COUNT(*) AS n_accts_total,
       CASE WHEN SUM(n_then_rows) = 0 THEN NULL ELSE SUM(spend_3mo_then) END AS spend_3mo_then,
       CASE WHEN SUM(n_now_rows) = 0 THEN NULL ELSE SUM(spend_3mo_now) END AS spend_3mo_now
    FROM acct_wide
    GROUP BY clnt_no
    """ % {"cohort_cte": _cohort_cte_sql(bite), "sfloor": SPEND_FLOOR_B, "sceil": SPEND_CEIL_B,
           "yms": SPEND_YMS_B_SQL, "spend_then": _spend_case_then, "spend_now": _spend_case_now,
           "then_rows": _then_row_count_case, "now_rows": _now_row_count_case}


def _prep_bdfp(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    _n_null = pd.to_numeric(pdf["clnt_no"], errors="coerce").isna().sum()
    assert _n_null == 0, "clnt_no has %d nulls" % _n_null
    pdf["clnt_no"] = pd.to_numeric(pdf["clnt_no"], errors="coerce").astype("int64")
    pdf["n_accts_total"] = pd.to_numeric(pdf["n_accts_total"], errors="coerce").fillna(0).astype("int64")
    # spend_3mo_then/spend_3mo_now - NO fillna on either (both windows get the NULL-preserving
    # guard now, not just the offset-dependent one). The SQL emits a real SQL NULL when a client
    # has zero rows in that window - fillna(0.0) here would silently turn that back into a fake
    # "$0 spend", which Cell [15] then bands as "Low"/"untiered" incorrectly. Preserve as an
    # explicit Python None (not float NaN) so PySpark's schema-based row conversion writes a
    # genuine parquet NULL that F.col(...).isNull() in Cell [15] can actually catch - float NaN
    # would NOT be caught by isNull() and would silently slip through as a non-null value.
    for _c in ["spend_3mo_then", "spend_3mo_now"]:
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce")
        _isna = pdf[_c].isna()
        pdf[_c] = pdf[_c].astype(object)
        pdf.loc[_isna, _c] = None
    return pdf[[f.name for f in BDFP_SCHEMA.fields]]


def land_bdfp_bite(bite):
    path = "b_dfp_v%d/bite_%d" % (B_SCHEMA_VERSION, bite)
    if _landed(path):
        print(path, ": already landed,", spark.read.parquet(BASE + path).count(), "rows - SKIP")
        return
    pdf = edw_pd(_dfp_sql(bite))
    if len(pdf) == 0:
        print(path, ": zero cohort clients with DFP rows in this bite - continuing.")
        return
    pdf = _prep_bdfp(pdf)
    nback = _write_chunks(pdf, BDFP_SCHEMA, path)
    assert nback == len(pdf), path + " HDFS readback mismatch: pulled %d, read back %d" % (len(pdf), nback)
    print(path, ": landed", len(pdf), "rows (cohort-scoped, then/now spend pivoted), readback", nback)


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


# %% [14] PULL B_BHV (v3, 2026-08-03e redesign) - revolver/transactor at T0_ANCHOR_B ("then") and
# T1_ANCHOR_B ("now") EXACT month-ends, cohort-scoped the same way as Cell [13]. CR_CRD_RPTS_ACCT
# carries clnt_no directly - no DFP bridge needed (verbatim finding from spotlight2.py Cell [4],
# D2). Raw-value probe runs first, same as spotlight2.py - the rank mapping rests on values nobody
# has printed for THIS pull. Probed at BOTH anchors now (both are closed months with real data, not
# just THEN) so a "now"-specific raw-value drift would be caught before it silently becomes
# other_or_none downstream.
# ENGINE: Teradata-direct (D3CV12A.CR_CRD_RPTS_ACCT).

BBHV_SCHEMA = StructType([
    StructField("clnt_no", LongType(), True),
    StructField("bhvr_rank_then", IntegerType(), True),
    StructField("bhvr_rank_now", IntegerType(), True),
])

_seg_case_sql = "\n                    ".join(
    "WHEN TRIM(r.usg_bhvr_seg_at_cyc_cd) = '%s' THEN %d" % (lbl, rk) for lbl, rk in SEG_PRECEDENCE)

_probe_sql = """
SELECT r.ME_DT AS me_dt, r.usg_bhvr_seg_at_cyc_cd AS raw_value, COUNT(*) AS accounts
FROM D3CV12A.CR_CRD_RPTS_ACCT r
WHERE r.ME_DT IN (%s)
GROUP BY r.ME_DT, r.usg_bhvr_seg_at_cyc_cd
ORDER BY r.ME_DT, accounts DESC
""" % ANCHOR_DATES_SQL_B

if "B_BHV" in RUN_PULLS:
    _probe = edw_pd(_probe_sql)
    _probe.columns = [c.lower() for c in _probe.columns]
    print("BEHAVIOUR VALUE PROBE at ME_DT IN (then=", T0_ANCHOR_B.isoformat(), ", now=",
          T1_ANCHOR_B.isoformat(), ") | grain: one row per (me_dt, raw value) |", len(_probe), "rows")
    print(_probe.to_string(index=False))
    _expected = [lbl for lbl, _ in SEG_PRECEDENCE]
    _seen = [str(v).strip() for v in _probe["raw_value"].tolist()]
    _unmapped = sorted(set(v for v in _seen if v not in _expected and v not in ("", "None", "nan")))
    if _unmapped:
        print("WARNING: raw values not in SEG_PRECEDENCE, will read other_or_none:", _unmapped)
    else:
        print("All raw values (both anchors) map cleanly onto SEG_PRECEDENCE", _expected)
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
       MIN(CASE WHEN me_dt = DATE '%(t_then)s' THEN seg_rank END) AS bhvr_rank_then,
       MIN(CASE WHEN me_dt = DATE '%(t_now)s' THEN seg_rank END) AS bhvr_rank_now
    FROM seg
    GROUP BY clnt_no
    """ % {"cohort_cte": _cohort_cte_sql(bite), "seg_case": _seg_case_sql, "anchors": ANCHOR_DATES_SQL_B,
           "t_then": T0_ANCHOR_B.isoformat(), "t_now": T1_ANCHOR_B.isoformat()}


def _prep_bbhv(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    _n_null = pd.to_numeric(pdf["clnt_no"], errors="coerce").isna().sum()
    assert _n_null == 0, "clnt_no has %d nulls" % _n_null
    pdf["clnt_no"] = pd.to_numeric(pdf["clnt_no"], errors="coerce").astype("int64")
    for _c in ["bhvr_rank_then", "bhvr_rank_now"]:
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce").fillna(0).astype("int32")
    return pdf[[f.name for f in BBHV_SCHEMA.fields]]


def land_bbhv_bite(bite):
    path = "b_bhv_v%d/bite_%d" % (B_SCHEMA_VERSION, bite)
    if _landed(path):
        print(path, ": already landed,", spark.read.parquet(BASE + path).count(), "rows - SKIP")
        return
    pdf = edw_pd(_bhv_sql(bite))
    if len(pdf) == 0:
        print(path, ": zero cohort clients with a behaviour row in this bite - both anchors are "
              "real past data now; investigate if this keeps happening across bites.")
        return
    pdf = _prep_bbhv(pdf)
    nback = _write_chunks(pdf, BBHV_SCHEMA, path)
    assert nback == len(pdf), path + " HDFS readback mismatch: pulled %d, read back %d" % (len(pdf), nback)
    print(path, ": landed", len(pdf), "rows (cohort-scoped, then/now behaviour pivoted), readback", nback)


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


# %% [14b] PULL B_UCP (NEW 2026-08-03e) - dual UCP snapshot join, PIECE B. Same OOM-safe discipline
# as Cell [5]'s Piece-A UCP join: read + Personal-filter + column-prune + cache EACH snapshot month
# ONCE (single-table transform, safe at full scale), then join to the cohort ~1 bite at a time (the
# join is what blows up memory, not the single-table read). UCP_MONTH_B0/B1 (Cell [0]) are the SAME
# then/now anchors as B_COHORT/B_DFP/B_BHV - 2025-06-30 and 2026-06-30, both confirmed inside the
# live UCP partition range.
# prof_then/prof_now = UCP's PROF_TOT_ANNUAL - an ANNUAL PROFITABILITY ESTIMATE, NOT a validated
# LTV figure (canon: reference_ucp_canon.md) - left NULL (never coalesced to 0) on no-match, since
# it is a continuous dollar estimate, not a count.
# held_t/i/b/c_then/_now and prod_cnt_then/_now: 0/1 (T/I/B/C) or a 0-4 count, -1 = no UCP match
# that snapshot (NEVER fold -1 into 0 downstream - same hard rule as Cell [5]'s Piece-A held_*).
# ENGINE: PySpark (YARN) reading HDFS parquet - not Trino, not Teradata.

BUCP_SCHEMA = StructType([
    StructField("clnt_no", LongType(), True),
    StructField("prof_then", DoubleType(), True),
    StructField("prof_now", DoubleType(), True),
    StructField("held_t_then", IntegerType(), True),
    StructField("held_i_then", IntegerType(), True),
    StructField("held_b_then", IntegerType(), True),
    StructField("held_c_then", IntegerType(), True),
    StructField("prod_cnt_then", IntegerType(), True),
    StructField("held_t_now", IntegerType(), True),
    StructField("held_i_now", IntegerType(), True),
    StructField("held_b_now", IntegerType(), True),
    StructField("held_c_now", IntegerType(), True),
    StructField("prod_cnt_now", IntegerType(), True),
])

_TIBC_COLS_B = ["T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT"]
_UCP_COLS_B = ["CLNT_NO", "CLNT_TYP", "PROF_TOT_ANNUAL"] + _TIBC_COLS_B


def _read_ucp_snapshot_b(month_str, tag):
    """Single-table read+prune+enrich for one UCP snapshot month - safe at full scale (no join
    yet), same pattern as Cell [5]'s ucp_sel/ucp_enriched. tag is 'then' or 'now', print-only."""
    _path = UCP_BASE + "MONTH_END_DATE=" + month_str
    _raw = spark.read.option("basePath", UCP_BASE).parquet(_path)
    _missing = [c for c in _UCP_COLS_B if c not in _raw.columns]
    assert not _missing, "UCP missing required columns at " + month_str + " (" + tag + "): " + str(_missing)
    _held = [(F.coalesce(F.col(c), F.lit(0)) > 0).cast("int") for c in _TIBC_COLS_B]
    _sel = (_raw
            .filter(F.trim(F.col("CLNT_TYP")) == "Personal")
            .select(*_UCP_COLS_B)
            .withColumn("clnt_no_long", F.col("CLNT_NO").cast("decimal(18,0)").cast("long"))
            .withColumn("prof", F.col("PROF_TOT_ANNUAL").cast("double"))
            .withColumn("held_t", _held[0])
            .withColumn("held_i", _held[1])
            .withColumn("held_b", _held[2])
            .withColumn("held_c", _held[3])
            .withColumn("prod_cnt", (_held[0] + _held[1] + _held[2] + _held[3]))
            .select("clnt_no_long", "prof", "held_t", "held_i", "held_b", "held_c", "prod_cnt")
            .dropDuplicates(["clnt_no_long"])
            .cache())
    _n = _sel.count()
    print("UCP snapshot (%s, %s) pruned + enriched + cached:" % (tag, month_str), _n, "rows.")
    return _sel


if "B_UCP" in RUN_PULLS:
    _ucp_then_b = _read_ucp_snapshot_b(UCP_MONTH_B0, "then")
    _ucp_now_b = _read_ucp_snapshot_b(UCP_MONTH_B1, "now")

    for _bite in (range(1) if SMOKE else range(N_BITES)):
        _bucp_name = "b_ucp_v%d/bite_%d" % (B_SCHEMA_VERSION, _bite)
        if _landed(_bucp_name):
            print(_bucp_name, ": already landed,", spark.read.parquet(BASE + _bucp_name).count(),
                  "rows - SKIP")
            continue
        _cohort_bite_ucp = read_bcohort_bite(_bite).select("clnt_no")
        _cohort_bite_n_ucp = _cohort_bite_ucp.count()
        if _cohort_bite_n_ucp == 0:
            print(_bucp_name, ": zero cohort clients in this bite - skipping (matches Cell [12]'s "
                  "own zero-row skip for this bite).")
            continue
        _joined_then = (_cohort_bite_ucp
                         .join(_ucp_then_b, F.col("clnt_no") == F.col("clnt_no_long"), "left")
                         .select(F.col("clnt_no"),
                                 F.col("prof").alias("prof_then"),
                                 F.coalesce(F.col("held_t"), F.lit(-1)).alias("held_t_then"),
                                 F.coalesce(F.col("held_i"), F.lit(-1)).alias("held_i_then"),
                                 F.coalesce(F.col("held_b"), F.lit(-1)).alias("held_b_then"),
                                 F.coalesce(F.col("held_c"), F.lit(-1)).alias("held_c_then"),
                                 F.coalesce(F.col("prod_cnt"), F.lit(-1)).alias("prod_cnt_then")))
        _joined_now = (_cohort_bite_ucp
                       .join(_ucp_now_b, F.col("clnt_no") == F.col("clnt_no_long"), "left")
                       .select(F.col("clnt_no"),
                               F.col("prof").alias("prof_now"),
                               F.coalesce(F.col("held_t"), F.lit(-1)).alias("held_t_now"),
                               F.coalesce(F.col("held_i"), F.lit(-1)).alias("held_i_now"),
                               F.coalesce(F.col("held_b"), F.lit(-1)).alias("held_b_now"),
                               F.coalesce(F.col("held_c"), F.lit(-1)).alias("held_c_now"),
                               F.coalesce(F.col("prod_cnt"), F.lit(-1)).alias("prod_cnt_now")))
        _bucp_bite = _joined_then.join(_joined_now, "clnt_no", "inner")
        _n_bucp_bite = _bucp_bite.count()
        assert _n_bucp_bite == _cohort_bite_n_ucp, (
            "b_ucp bite %d: %d rows after joining then+now UCP onto the cohort bite (%d clients) - "
            "fan-out on a duplicate clnt_no_long within a UCP snapshot. Both snapshots were "
            "dropDuplicates-deduped before this join; re-check that." % (_bite, _n_bucp_bite, _cohort_bite_n_ucp))
        _bucp_bite.write.mode("overwrite").parquet(BASE + _bucp_name)
        _n_back = spark.read.parquet(BASE + _bucp_name).count()
        assert _n_back == _n_bucp_bite, "%s: wrote %d rows, read back %d" % (_bucp_name, _n_bucp_bite, _n_back)
        _write_spark_marker(_bucp_name, _n_back)
        print(_bucp_name, ": landed", _n_back, "rows (bite", _bite, "of", N_BITES, "), dual UCP snapshot joined.")

    _ucp_then_b.unpersist()
    _ucp_now_b.unpersist()
    print("PULL B_UCP done - landed at", BUCP_DIR + "*")
else:
    print("PULL B_UCP skipped - not in RUN_PULLS")


def read_bucp():
    try:
        sdf = spark.read.parquet(BUCP_DIR + "bite_?")
    except Exception as _e:   # zero bites landed -> explained empty, not a crash
        print("read_bucp: NO landed bites at", BUCP_DIR, "(%s)" % type(_e).__name__,
              "- returning EMPTY frame. Downstream B cells report no_data, not crash.")
        return spark.createDataFrame([], BUCP_SCHEMA)
    missing = [c.name for c in BUCP_SCHEMA.fields if c.name not in sdf.columns]
    if missing:
        raise RuntimeError("b_ucp missing %s. Rerun Cell [14b]." % missing)
    return sdf.withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))


def read_bucp_bite(bite):
    """Single-bite read - used by Cell [15]'s bite-looped panel build."""
    return _read_bite_or_empty(BUCP_DIR, bite, BUCP_SCHEMA).withColumn(
        "clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))


# %% [15] PIECE B PANEL BUILD + b_before_after_cube (REDESIGNED 2026-08-03e, "then vs now vs
# delta"). Replaces the old t_offset=0/12 long-table design: BOTH T0_ANCHOR_B ("then") and
# T1_ANCHOR_B ("now") are already closed, so then/now sit as columns on the SAME per-client row
# instead of stacked rows - no more thin/no_data future-month rows to explain away.
#
# GROUP BASIS (Andre-overridable assumption, logged here per house rule): this artifact's
# stayers/leavers split is CARDS_UNSUB_BY_ANCHOR (marketing-cards unsub), not the enterprise-wide
# any_unsub_by_anchor the Aug-anchor build used - cards_unsub_mne (Cell [12]) is only meaningful as
# a campaign dimension for CARDS leavers, and the brief's "Anatomy of an Unsub" ask is about Cards.
# A cohort client who unsubbed from a non-cards list only (any_unsub_by_anchor=1, cards_unsub_by_
# anchor=0) reads as a STAYER in this specific pair of outputs - flagged loudly here, not buried.
#
# BITE DISCIPLINE (same as Cells [5]/[9]/[14b]): cohort_bite x dfp_bite x bhv_bite x ucp_bite are
# ALREADY landed under the SAME MOD(ABS(CLNT_NO), N_BITES) partition (Cells [12]/[13]/[14]/[14b] all
# thread the same bite param through _cohort_cte_sql / the UCP join loop), so this reads each
# bite's subdir directly - no re-partitioning, no full-scale client-grain join at once. The ONE
# deviation from "never hold a full-scale client-grain frame": Cell [15b]'s exact median needs the
# WHOLE cohort's row-level values, not just per-bite partial sums (sums compose across bites; a
# median does not) - so this cell also unions a NARROW per-client panel (b_panel, ~20 small columns,
# cached once) for Cell [15b] to aggregate in one pass. b_before_after_cube itself is still built
# from additive PARTIAL cube sums, same as every other bitten cube in this file.
# ENGINE: PySpark (YARN).

_seg_label_expr_then = None
for _rk in sorted(SEG_LABEL):
    _c = F.col("bhvr_rank_then") == _rk
    _seg_label_expr_then = (F.when(_c, F.lit(SEG_LABEL[_rk])) if _seg_label_expr_then is None
                             else _seg_label_expr_then.when(_c, F.lit(SEG_LABEL[_rk])))
_seg_then_expr = _seg_label_expr_then.otherwise(F.lit("no_data"))

_seg_label_expr_now = None
for _rk in sorted(SEG_LABEL):
    _c = F.col("bhvr_rank_now") == _rk
    _seg_label_expr_now = (F.when(_c, F.lit(SEG_LABEL[_rk])) if _seg_label_expr_now is None
                            else _seg_label_expr_now.when(_c, F.lit(SEG_LABEL[_rk])))
_seg_now_expr = _seg_label_expr_now.otherwise(F.lit("no_data"))

dfp_wide_all = read_bdfp()   # single table, no join - safe at full scale, used ONLY for the
                             # global THEN-side quantile cut below.
_tier_src = dfp_wide_all.select("clnt_no", "spend_3mo_then").distinct()
# Guarded quantile cut (red-team Blocker 3): approxQuantile can return fewer than 2 values (empty
# input, or every value null) and a bare `_q1, _q2 = ...` unpack on that is an uncaught ValueError.
# Pre-check the non-null count, then check the result shape - on EITHER failure, route to the
# explained-empty path (_q1/_q2 stay None, _tier_expr below returns 'untiered' for every client)
# instead of crashing the run.
_q1, _q2 = None, None
_tier_nonnull_n = _tier_src.filter(F.col("spend_3mo_then").isNotNull()).count()
if _tier_nonnull_n > 0:
    _q_result = _tier_src.approxQuantile("spend_3mo_then", SPEND_TIER_QUANTILES, SPEND_TIER_REL_ERR)
    if len(_q_result) == 2:
        _q1, _q2 = _q_result

if _q1 is None or _q2 is None:
    print("WARNING: spend tier cut could not be computed (%d non-null spend_3mo_then values, "
          "approxQuantile returned %s) - EVERY client reads as 'untiered' this run for both tier "
          "and tier_now. Investigate B_DFP (Cell [13]) before trusting the tier dims in "
          "b_before_after_cube." % (_tier_nonnull_n, "no result" if _tier_nonnull_n == 0 else "an unexpected shape"))
else:
    print("SPEND TIER CUT | 3-month spend around", T0_ANCHOR_B.isoformat(), "(THEN window",
          SPEND_YMS_THEN, ") | cut ONCE (global, cohort-wide, across all bites), held fixed for "
          "tier_now | Low <=", round(_q1, 2), "< Mid <=", round(_q2, 2), "< High | NULL "
          "spend_3mo_then -> 'untiered'.")
    if _q1 == _q2:
        print("WARNING: tercile cut points are identical - more than a third of the cohort shares "
              "the same THEN spend (likely zero). Read the tier as zero-vs-something, not three terciles.")


def _tier_expr(spend_col):
    if _q1 is None or _q2 is None:
        return F.lit("untiered")
    return (F.when(F.col(spend_col).isNull(), F.lit("untiered"))
             .when(F.col(spend_col) <= _q1, F.lit("Low"))
             .when(F.col(spend_col) <= _q2, F.lit("Mid"))
             .otherwise(F.lit("High")))


_PANEL_COLS = ["clnt_no", "group_tag", "spend_3mo_then", "spend_3mo_now", "tier", "tier_now",
               "seg_then", "seg_now", "prof_then", "prof_now",
               "held_t_then", "held_i_then", "held_b_then", "held_c_then",
               "held_t_now", "held_i_now", "held_b_now", "held_c_now",
               "prod_cnt_then", "prod_cnt_now",
               "cards_unsub_by_anchor", "cards_ex_fwc_unsub_by_anchor"]

_B_PANEL_SCHEMA = StructType([
    StructField("clnt_no", LongType(), True),
    StructField("group_tag", StringType(), True),
    StructField("spend_3mo_then", DoubleType(), True),
    StructField("spend_3mo_now", DoubleType(), True),
    StructField("tier", StringType(), True),
    StructField("tier_now", StringType(), True),
    StructField("seg_then", StringType(), True),
    StructField("seg_now", StringType(), True),
    StructField("prof_then", DoubleType(), True),
    StructField("prof_now", DoubleType(), True),
    StructField("held_t_then", IntegerType(), True),
    StructField("held_i_then", IntegerType(), True),
    StructField("held_b_then", IntegerType(), True),
    StructField("held_c_then", IntegerType(), True),
    StructField("held_t_now", IntegerType(), True),
    StructField("held_i_now", IntegerType(), True),
    StructField("held_b_now", IntegerType(), True),
    StructField("held_c_now", IntegerType(), True),
    StructField("prod_cnt_then", IntegerType(), True),
    StructField("prod_cnt_now", IntegerType(), True),
    StructField("cards_unsub_by_anchor", IntegerType(), True),
    StructField("cards_ex_fwc_unsub_by_anchor", IntegerType(), True),
])   # empty-frame fallback schema (matches _PANEL_COLS order/types) - guards the "SMOKE mode, bite
     # 0 pulled zero cohort clients" edge case so Cell [15]/[15b] print an explained empty panel
     # instead of an IndexError on _b_panel_bites[0].

_b_cube_partials = []
_b_panel_bites = []
_n_panel_total = 0
_n_panel_expected = 0   # accumulated only over bites THIS RUN processes - mode-safe (SMOKE
                        # processes bite 0 only; comparing vs full COHORT_B_N under SMOKE would be
                        # the exact mode-blind assert that crashed the 2026-08-03 A4 run before it
                        # was fixed - same fix applied here).
for _bite in (range(1) if SMOKE else range(N_BITES)):
    _cohort_bite = read_bcohort_bite(_bite)
    _cohort_bite_n = _cohort_bite.count()
    if _cohort_bite_n == 0:
        print("PIECE B bite", _bite, "of", N_BITES, ": zero cohort clients - skipping (matches "
              "Cell [12]'s own zero-row skip for this bite).")
        continue
    _n_panel_expected += _cohort_bite_n

    _dfp_bite = read_bdfp_bite(_bite)
    _bhv_bite = (read_bbhv_bite(_bite)
                 .withColumn("seg_then", _seg_then_expr)
                 .withColumn("seg_now", _seg_now_expr))
    _ucp_bite = read_bucp_bite(_bite)

    _panel_bite = (_cohort_bite
                   .join(_dfp_bite.select("clnt_no", "spend_3mo_then", "spend_3mo_now"), "clnt_no", "left")
                   .join(_bhv_bite.select("clnt_no", "seg_then", "seg_now"), "clnt_no", "left")
                   .join(_ucp_bite, "clnt_no", "left")
                   .withColumn("tier", _tier_expr("spend_3mo_then"))
                   .withColumn("tier_now", _tier_expr("spend_3mo_now"))
                   .withColumn("seg_then", F.coalesce(F.col("seg_then"), F.lit("no_data")))
                   .withColumn("seg_now", F.coalesce(F.col("seg_now"), F.lit("no_data")))
                   .withColumn("held_t_then", F.coalesce(F.col("held_t_then"), F.lit(-1)))
                   .withColumn("held_i_then", F.coalesce(F.col("held_i_then"), F.lit(-1)))
                   .withColumn("held_b_then", F.coalesce(F.col("held_b_then"), F.lit(-1)))
                   .withColumn("held_c_then", F.coalesce(F.col("held_c_then"), F.lit(-1)))
                   .withColumn("held_t_now", F.coalesce(F.col("held_t_now"), F.lit(-1)))
                   .withColumn("held_i_now", F.coalesce(F.col("held_i_now"), F.lit(-1)))
                   .withColumn("held_b_now", F.coalesce(F.col("held_b_now"), F.lit(-1)))
                   .withColumn("held_c_now", F.coalesce(F.col("held_c_now"), F.lit(-1)))
                   .withColumn("prod_cnt_then", F.coalesce(F.col("prod_cnt_then"), F.lit(-1)))
                   .withColumn("prod_cnt_now", F.coalesce(F.col("prod_cnt_now"), F.lit(-1)))
                   .withColumn("group_tag",
                               F.when(F.col("cards_unsub_by_anchor") == 0, F.lit("STAYERS"))
                                .otherwise(F.coalesce(F.col("cards_unsub_mne"), F.lit("LEAVERS_UNMAPPED"))))
                   .select(*_PANEL_COLS))

    _n_panel_bite = _panel_bite.count()
    assert _n_panel_bite == _cohort_bite_n, (
        "PIECE B bite %d: panel has %d rows, expected %d cohort clients - a join fanned out. "
        "dfp/bhv/ucp bites are all supposed to be unique on clnt_no; re-check those."
        % (_bite, _n_panel_bite, _cohort_bite_n))
    _n_panel_total += _n_panel_bite
    _b_panel_bites.append(_panel_bite)

    _b_cube_partials.append(
        _panel_bite
        .groupBy("tier", "tier_now", "seg_then", "seg_now")
        .agg(F.sum(F.when(F.col("cards_unsub_by_anchor") == 0, 1).otherwise(0)).alias("stayers"),
             F.sum(F.when(F.col("cards_unsub_by_anchor") == 1, 1).otherwise(0)).alias("leavers"),
             F.sum(F.when(F.col("cards_ex_fwc_unsub_by_anchor") == 1, 1).otherwise(0)).alias("leavers_ex_fwc"),
             F.count("*").alias("clients_total")))
    print("PIECE B bite", _bite, "of", N_BITES, ":", _cohort_bite_n, "cohort clients, panel + "
          "partial cube built.")

if SMOKE:
    print("PIECE B PANEL SMOKE mode: bite 0 only -", _n_panel_total, "of", COHORT_B_N,
          "cohort clients processed; full-coverage check runs on the SMOKE=False pass.")
else:
    assert _n_panel_total == COHORT_B_N, (
        "PIECE B panel total (%d) != cohort_b (%d) - a bite filter missed rows, or a join fanned "
        "out somewhere the per-bite assert didn't catch." % (_n_panel_total, COHORT_B_N))
    print("PIECE B panel row-count guard: cohort_b", COHORT_B_N, "-> panel (summed across bites)",
          _n_panel_total, "- no fan-out, full coverage confirmed.")
assert _n_panel_total == _n_panel_expected, (
    "PIECE B panel total (%d) != sum of processed-bite cohort counts (%d) - internal accounting "
    "bug in the loop above." % (_n_panel_total, _n_panel_expected))

if _b_panel_bites:
    b_panel = _b_panel_bites[0]
    for _p in _b_panel_bites[1:]:
        b_panel = b_panel.unionByName(_p)
else:
    print("WARNING: every processed bite had zero cohort clients (COHORT_B_N=%d) - shipping an "
          "EMPTY b_panel. Cell [15b]'s b_delta_summary will read as all-zero groups; investigate "
          "before trusting this run." % COHORT_B_N)
    b_panel = spark.createDataFrame([], schema=_B_PANEL_SCHEMA)
b_panel = b_panel.cache()
_n_b_panel_cached = b_panel.count()
print("PIECE B PANEL | grain: one row per clnt_no |", _n_b_panel_cached, "rows cached (the ONE "
      "full-scale row-level cache in Piece B - narrow columns only, needed for Cell [15b]'s exact "
      "medians, which cannot be reassembled from partial-bite sums the way COUNT/SUM can).")

_b_unmapped_n = b_panel.filter(F.col("group_tag") == "LEAVERS_UNMAPPED").count()
if _b_unmapped_n > 0:
    print("WARNING:", _b_unmapped_n, "cards leavers landed in the panel with cards_unsub_by_anchor"
          "=1 but no cards_unsub_mne - Cell [12]'s earliest-unsub join missed them. They fall into "
          "LEAVERS_UNMAPPED here rather than silently vanishing; investigate before trusting the "
          "per-mne rows in Cell [15b].")

_B_CUBE_PARTIAL_SCHEMA = StructType([
    StructField("tier", StringType(), True),
    StructField("tier_now", StringType(), True),
    StructField("seg_then", StringType(), True),
    StructField("seg_now", StringType(), True),
    StructField("stayers", LongType(), True),
    StructField("leavers", LongType(), True),
    StructField("leavers_ex_fwc", LongType(), True),
    StructField("clients_total", LongType(), True),
])

if _b_cube_partials:
    _b_cube_union = _b_cube_partials[0]
    for _p in _b_cube_partials[1:]:
        _b_cube_union = _b_cube_union.unionByName(_p)
else:
    print("WARNING: every bite had zero cohort clients (COHORT_B_N=%d) - shipping an EMPTY "
          "b_before_after_cube. Investigate before trusting this run." % COHORT_B_N)
    _b_cube_union = spark.createDataFrame([], schema=_B_CUBE_PARTIAL_SCHEMA)

# b_before_after_cube KEEPS ITS NAME (Maya's heatmap source). Schema note per house rule (additions
# vs a meaning change must be flagged, never silently swapped): "spend_tier" -> "tier" and the old
# "leavers"/"leavers_cards_unsub_subset" pairing are RENAMED/REPURPOSED this build, not additive -
# the old design had one "leavers" (any-list) with a cards SUBSET column; this design's ONLY
# leavers basis is cards_unsub_by_anchor (see the GROUP BASIS note above), so "leavers" here means
# CARDS unsub, not any-list. tier_now/seg_now are genuinely NEW (additive) columns.
b_before_after_cube = (_b_cube_union
                        .groupBy("tier", "tier_now", "seg_then", "seg_now")
                        .agg(F.sum("stayers").alias("stayers"),
                             F.sum("leavers").alias("leavers"),
                             F.sum("leavers_ex_fwc").alias("leavers_ex_fwc"),
                             F.sum("clients_total").alias("clients_total"))
                        .orderBy("tier", "tier_now", "seg_then", "seg_now"))

b_before_after_cube_stamped = _stamp(
    b_before_after_cube,
    "then %s -> now %s (both closed)" % (T0_ANCHOR_B.isoformat(), T1_ANCHOR_B.isoformat()),
    "Cards-marketing-mailed cohort as of anchor (n=%d)" % COHORT_B_N)

b_pd = b_before_after_cube_stamped.toPandas()
print("B_BEFORE_AFTER_CUBE | grain: one row per (tier, tier_now, seg_then, seg_now) | tier = held "
      "fixed at THEN terciles | tier_now = NOW spend banded on the SAME then-cutpoints (trajectory) "
      "| stayers/leavers(cards)/leavers_ex_fwc COLUMNS (cards_unsub_by_anchor basis - see GROUP "
      "BASIS note above) | %d rows" % len(b_pd))
print(b_pd.to_string(index=False))

write_cube(b_before_after_cube_stamped, "b_before_after_cube")


# %% [15b] b_delta_summary (NEW 2026-08-03e, red-team-hardened) - Andre's real deliverable:
# STAYERS vs LEAVERS_ALL vs per-campaign leaver breakdown, then/now/delta, long/pivot-ready.
# Two-pass grouping: cards_unsub_mne counts can only be evaluated for the >=500-leavers threshold
# AFTER the whole cohort is known (never per-bite) - Cell [15] already built the full b_panel cache
# for exactly this reason.
#
# GROUPS: STAYERS, LEAVERS_ALL, one row-group per cards_unsub_mne with >=500 leavers, LEAVERS_OTHER
# (everything under the floor, pooled). LEAVERS_UNMAPPED (Cell [15]'s fallback group_tag for a cards
# leaver whose cards_unsub_mne join came back NULL - see Cell [12]'s guard) is DELIBERATELY NEVER
# folded into LEAVERS_OTHER: it stays its OWN visible row-group if any such clients exist, small or
# not, because pooling a data-quality gap into "small campaigns" would hide it (red-team W3 -
# transparency about the mapping gap matters more here than a clean small-group list).
#
# METRICS: n_clients (period='n/a'); spend_monthly avg+median (see the /3 note below); prof_annual
# avg+median; prod_cnt avg (no median, per spec); pct_held_t/i/b/c (denominator = UCP-MATCHED
# clients only, held_*!=-1 - folding the -1 no-match sentinel into the denominator would understate
# holding, same hard rule as Cell [5]); pct_revolver/transactor/dormant (denominator = clients with
# a real seg, excludes 'no_data'); pct_no_ucp_match (denominator = ALL clients in the group, numer =
# held_t_then/now == -1); pct_no_dfp_match (red-team W2 - separate from the UCP match rate: share of
# the group with spend_3mo_then/now IS NULL, i.e. zero DFP rows for that whole window, denominator =
# ALL clients in the group). delta = now-value minus then-value, computed on the aggregate (not a
# per-client delta distribution) - "delta where meaningful" means n_clients gets no delta row.
#
# spend_monthly_avg/median (red-team W5, Andre's wording governs): the panel's spend_3mo_then/now
# columns (Cell [15], unchanged) are a 3-CALENDAR-MONTH SUM, not a monthly figure - this summary
# divides by 3 to report AVG MONTHLY card spend, so it reads next to prof_annual without a mental
# unit conversion. avg(X)/3 == avg(X/3) and, because dividing by a positive constant is a strictly
# increasing transform, median(X)/3 == median(X/3) too - so dividing the already-computed
# then/now/delta aggregate is mathematically identical to dividing every per-client value first.
# CAVEAT, stated not hidden: a client with a DFP row in only 1-2 of the 3 months (account opened/
# closed mid-window) still gets spend_3mo_then = SUM of whichever months had data (Cell [13]'s SQL
# only emits NULL when ALL THREE months are absent), so dividing that partial sum by a flat 3
# UNDERSTATES their true average-monthly spend. This is a documented approximation, not a bug.
#
# Medians use percentile_approx (Spark's standard approximate percentile - NOT exact) computed in
# ONE pass over the FULL b_panel (Cell [15]'s bite-looped union, cached once) - bite-safe by
# construction: every bite's rows are already inside b_panel before this cell runs, so the
# aggregate sees the whole cohort regardless of N_BITES, it just isn't a mathematically exact
# percentile (red-team W4 - "exact medians" in the prior build's comment overstated what
# percentile_approx guarantees; corrected here).
# ENGINE: PySpark (YARN), reusing b_panel from Cell [15] - no new EDW connection, no new HDFS pull.

_mne_counts_rows = (b_panel.filter(F.col("group_tag") != "STAYERS")
                    .groupBy("group_tag").agg(F.count("*").alias("n")).collect())
_mne_counts = {r["group_tag"]: r["n"] for r in _mne_counts_rows}
_MNE_LEAVER_FLOOR = 500
_unmapped_present = "LEAVERS_UNMAPPED" in _mne_counts
_mne_counts_real = {m: n for m, n in _mne_counts.items() if m != "LEAVERS_UNMAPPED"}
_big_mnes = sorted([m for m, n in _mne_counts_real.items() if n >= _MNE_LEAVER_FLOOR])
_small_mnes = sorted([m for m, n in _mne_counts_real.items() if n < _MNE_LEAVER_FLOOR])
print("PIECE B DELTA SUMMARY GROUPING | leaver cards_unsub_mne counts:", _mne_counts)
print("  >=", _MNE_LEAVER_FLOOR, "leavers (own row-group):", _big_mnes)
print("  <", _MNE_LEAVER_FLOOR, "leavers (pooled LEAVERS_OTHER):", _small_mnes)
if _unmapped_present:
    print("  LEAVERS_UNMAPPED present (%d clients) - kept as its OWN row-group, NOT pooled into "
          "LEAVERS_OTHER (transparency about the Cell [12] mapping gap)." % _mne_counts["LEAVERS_UNMAPPED"])


def _pct(numer, denom):
    return None if not denom else 100.0 * numer / denom


def _group_metrics_rows(df_scoped, group_label):
    """One Spark aggregation pass over df_scoped (already filtered to this group). Returns a list
    of (group, metric, period, value) tuples in long format."""
    _agg = df_scoped.agg(
        F.count("*").alias("n_clients"),
        F.avg("spend_3mo_then").alias("spend_avg_then"), F.avg("spend_3mo_now").alias("spend_avg_now"),
        F.expr("percentile_approx(spend_3mo_then, 0.5)").alias("spend_med_then"),
        F.expr("percentile_approx(spend_3mo_now, 0.5)").alias("spend_med_now"),
        F.avg("prof_then").alias("prof_avg_then"), F.avg("prof_now").alias("prof_avg_now"),
        F.expr("percentile_approx(prof_then, 0.5)").alias("prof_med_then"),
        F.expr("percentile_approx(prof_now, 0.5)").alias("prof_med_now"),
        F.avg(F.when(F.col("prod_cnt_then") != -1, F.col("prod_cnt_then"))).alias("prodcnt_avg_then"),
        F.avg(F.when(F.col("prod_cnt_now") != -1, F.col("prod_cnt_now"))).alias("prodcnt_avg_now"),
        F.sum(F.when(F.col("held_t_then") == 1, 1).otherwise(0)).alias("t_then_yes"),
        F.sum(F.when(F.col("held_t_then") != -1, 1).otherwise(0)).alias("t_then_matched"),
        F.sum(F.when(F.col("held_t_now") == 1, 1).otherwise(0)).alias("t_now_yes"),
        F.sum(F.when(F.col("held_t_now") != -1, 1).otherwise(0)).alias("t_now_matched"),
        F.sum(F.when(F.col("held_i_then") == 1, 1).otherwise(0)).alias("i_then_yes"),
        F.sum(F.when(F.col("held_i_then") != -1, 1).otherwise(0)).alias("i_then_matched"),
        F.sum(F.when(F.col("held_i_now") == 1, 1).otherwise(0)).alias("i_now_yes"),
        F.sum(F.when(F.col("held_i_now") != -1, 1).otherwise(0)).alias("i_now_matched"),
        F.sum(F.when(F.col("held_b_then") == 1, 1).otherwise(0)).alias("b_then_yes"),
        F.sum(F.when(F.col("held_b_then") != -1, 1).otherwise(0)).alias("b_then_matched"),
        F.sum(F.when(F.col("held_b_now") == 1, 1).otherwise(0)).alias("b_now_yes"),
        F.sum(F.when(F.col("held_b_now") != -1, 1).otherwise(0)).alias("b_now_matched"),
        F.sum(F.when(F.col("held_c_then") == 1, 1).otherwise(0)).alias("c_then_yes"),
        F.sum(F.when(F.col("held_c_then") != -1, 1).otherwise(0)).alias("c_then_matched"),
        F.sum(F.when(F.col("held_c_now") == 1, 1).otherwise(0)).alias("c_now_yes"),
        F.sum(F.when(F.col("held_c_now") != -1, 1).otherwise(0)).alias("c_now_matched"),
        F.sum(F.when(F.col("seg_then") == "Revolver", 1).otherwise(0)).alias("seg_then_revolver"),
        F.sum(F.when(F.col("seg_then") == "Transactor", 1).otherwise(0)).alias("seg_then_transactor"),
        F.sum(F.when(F.col("seg_then") == "Dormant", 1).otherwise(0)).alias("seg_then_dormant"),
        F.sum(F.when(F.col("seg_then") != "no_data", 1).otherwise(0)).alias("seg_then_known"),
        F.sum(F.when(F.col("seg_now") == "Revolver", 1).otherwise(0)).alias("seg_now_revolver"),
        F.sum(F.when(F.col("seg_now") == "Transactor", 1).otherwise(0)).alias("seg_now_transactor"),
        F.sum(F.when(F.col("seg_now") == "Dormant", 1).otherwise(0)).alias("seg_now_dormant"),
        F.sum(F.when(F.col("seg_now") != "no_data", 1).otherwise(0)).alias("seg_now_known"),
        F.sum(F.when(F.col("held_t_then") == -1, 1).otherwise(0)).alias("no_ucp_match_then"),
        F.sum(F.when(F.col("held_t_now") == -1, 1).otherwise(0)).alias("no_ucp_match_now"),
        F.sum(F.when(F.col("spend_3mo_then").isNull(), 1).otherwise(0)).alias("no_dfp_match_then"),
        F.sum(F.when(F.col("spend_3mo_now").isNull(), 1).otherwise(0)).alias("no_dfp_match_now"),
    ).collect()[0].asDict()

    _n = _agg["n_clients"]
    rows = [(group_label, "n_clients", "n/a", float(_n))]

    def _emit(metric, then_v, now_v):
        rows.append((group_label, metric, "then", then_v))
        rows.append((group_label, metric, "now", now_v))
        if then_v is not None and now_v is not None:
            rows.append((group_label, metric, "delta", now_v - then_v))

    # spend_monthly_avg/median - 3-month SUM (panel, unchanged) / 3. See header CAVEAT: partial-
    # window clients (DFP row in only 1-2 of 3 months) understate here, documented not hidden.
    _spend_avg_then_m = _agg["spend_avg_then"] / 3.0 if _agg["spend_avg_then"] is not None else None
    _spend_avg_now_m = _agg["spend_avg_now"] / 3.0 if _agg["spend_avg_now"] is not None else None
    _spend_med_then_m = _agg["spend_med_then"] / 3.0 if _agg["spend_med_then"] is not None else None
    _spend_med_now_m = _agg["spend_med_now"] / 3.0 if _agg["spend_med_now"] is not None else None
    _emit("spend_monthly_avg", _spend_avg_then_m, _spend_avg_now_m)
    _emit("spend_monthly_median", _spend_med_then_m, _spend_med_now_m)
    _emit("prof_annual_avg", _agg["prof_avg_then"], _agg["prof_avg_now"])
    _emit("prof_annual_median", _agg["prof_med_then"], _agg["prof_med_now"])
    _emit("prod_cnt_avg", _agg["prodcnt_avg_then"], _agg["prodcnt_avg_now"])
    _emit("pct_held_t", _pct(_agg["t_then_yes"], _agg["t_then_matched"]), _pct(_agg["t_now_yes"], _agg["t_now_matched"]))
    _emit("pct_held_i", _pct(_agg["i_then_yes"], _agg["i_then_matched"]), _pct(_agg["i_now_yes"], _agg["i_now_matched"]))
    _emit("pct_held_b", _pct(_agg["b_then_yes"], _agg["b_then_matched"]), _pct(_agg["b_now_yes"], _agg["b_now_matched"]))
    _emit("pct_held_c", _pct(_agg["c_then_yes"], _agg["c_then_matched"]), _pct(_agg["c_now_yes"], _agg["c_now_matched"]))
    _emit("pct_revolver", _pct(_agg["seg_then_revolver"], _agg["seg_then_known"]), _pct(_agg["seg_now_revolver"], _agg["seg_now_known"]))
    _emit("pct_transactor", _pct(_agg["seg_then_transactor"], _agg["seg_then_known"]), _pct(_agg["seg_now_transactor"], _agg["seg_now_known"]))
    _emit("pct_dormant", _pct(_agg["seg_then_dormant"], _agg["seg_then_known"]), _pct(_agg["seg_now_dormant"], _agg["seg_now_known"]))
    _emit("pct_no_ucp_match", _pct(_agg["no_ucp_match_then"], _n), _pct(_agg["no_ucp_match_now"], _n))
    _emit("pct_no_dfp_match", _pct(_agg["no_dfp_match_then"], _n), _pct(_agg["no_dfp_match_now"], _n))
    return rows


_delta_rows = []
_stayers_scope = b_panel.filter(F.col("group_tag") == "STAYERS")
_leavers_scope = b_panel.filter(F.col("group_tag") != "STAYERS")
_delta_rows += _group_metrics_rows(_stayers_scope, "STAYERS")
_delta_rows += _group_metrics_rows(_leavers_scope, "LEAVERS_ALL")
for _mne in _big_mnes:
    _delta_rows += _group_metrics_rows(_leavers_scope.filter(F.col("group_tag") == _mne), _mne)
if _small_mnes:
    _delta_rows += _group_metrics_rows(
        _leavers_scope.filter(F.col("group_tag").isin(_small_mnes)), "LEAVERS_OTHER")
if _unmapped_present:
    _delta_rows += _group_metrics_rows(
        _leavers_scope.filter(F.col("group_tag") == "LEAVERS_UNMAPPED"), "LEAVERS_UNMAPPED")

_stayers_n = next(v for g, m, p, v in _delta_rows if g == "STAYERS" and m == "n_clients")
_leavers_all_n = next(v for g, m, p, v in _delta_rows if g == "LEAVERS_ALL" and m == "n_clients")
assert int(_stayers_n + _leavers_all_n) == _n_b_panel_cached, (
    "STAYERS (%d) + LEAVERS_ALL (%d) != panel total (%d) - group_tag assignment missed rows."
    % (_stayers_n, _leavers_all_n, _n_b_panel_cached))
_per_mne_n = sum(v for g, m, p, v in _delta_rows if g in _big_mnes and m == "n_clients")
_other_n = sum(v for g, m, p, v in _delta_rows if g == "LEAVERS_OTHER" and m == "n_clients")
_unmapped_n = sum(v for g, m, p, v in _delta_rows if g == "LEAVERS_UNMAPPED" and m == "n_clients")
assert int(_per_mne_n + _other_n + _unmapped_n) == int(_leavers_all_n), (
    "per-mne rows (%d) + LEAVERS_OTHER (%d) + LEAVERS_UNMAPPED (%d) != LEAVERS_ALL (%d) - the "
    ">=500 threshold split (or the unmapped carve-out) lost or double-counted clients."
    % (_per_mne_n, _other_n, _unmapped_n, _leavers_all_n))
print("b_delta_summary group-total cross-check OK: STAYERS + LEAVERS_ALL == panel total (%d); "
      "per-mne + LEAVERS_OTHER + LEAVERS_UNMAPPED == LEAVERS_ALL (%d)."
      % (_n_b_panel_cached, int(_leavers_all_n)))

_delta_pdf = pd.DataFrame(_delta_rows, columns=["group", "metric", "period", "value"])
_delta_pdf["value"] = _delta_pdf["value"].astype(object)
_delta_pdf.loc[_delta_pdf["value"].isna(), "value"] = None

_B_DELTA_SCHEMA = StructType([
    StructField("group", StringType(), True),
    StructField("metric", StringType(), True),
    StructField("period", StringType(), True),
    StructField("value", DoubleType(), True),
])
b_delta_summary = spark.createDataFrame(_delta_pdf, schema=_B_DELTA_SCHEMA)
b_delta_summary_stamped = _stamp(
    b_delta_summary,
    "then %s -> now %s (both closed)" % (T0_ANCHOR_B.isoformat(), T1_ANCHOR_B.isoformat()),
    "Cards-marketing-mailed cohort: STAYERS + LEAVERS_ALL + per-cards_unsub_mne (>=%d) + "
    "LEAVERS_OTHER + LEAVERS_UNMAPPED (visible, not pooled)" % _MNE_LEAVER_FLOOR)

b_delta_pd = b_delta_summary_stamped.toPandas()
print("B_DELTA_SUMMARY | grain: one row per (group, metric, period) | long/pivot-ready | groups:",
      sorted(set(r[0] for r in _delta_rows)), "|", len(b_delta_pd), "rows")
print(b_delta_pd.to_string(index=False))

write_cube(b_delta_summary_stamped, "b_delta_summary")


# %% [16] ONE FILE TO DOWNLOAD - bundle all eight CSVs into a single xlsx, one sheet each. Verbatim
# pattern from spotlight.py Cell [7]: HDFS is the durable output, this is a delivery convenience.
# TOLERATES a missing/empty cube (e.g. a SMOKE-only bite that pulled zero cohort clients): the
# filter below drops empty frames from the workbook and NAMES every dropped sheet below it, so the
# run never crashes on a thin cube - it ships what exists. Both Piece-B anchors are closed as of
# this build, so b_before_after_cube/b_delta_summary are no longer expected to be thin on a normal
# non-SMOKE run the way the old t12-future-dated design was.
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
        "a1_lob_dedup": a1_lob_dedup_pd,
        "a2_mne_rates": a2_mne_rates_pd,
        "a3_contact_cube": a3_pd,
        "a4_profile_cube": a4_pd,
        "b_before_after_cube": b_pd,
        "b_delta_summary": b_delta_pd,
        "c_monthly_curve": c_pd,
    }
    _sheets_dropped = [k for k, v in _sheets.items() if v is None or len(v) == 0]
    _sheets = {k: v for k, v in _sheets.items() if v is not None and len(v) > 0}
    if _sheets_dropped:
        print("WARN: dropping", len(_sheets_dropped), "empty sheet(s) from the workbook, shipping "
              "the rest -", _sheets_dropped, "- both Piece-B anchors are closed this build, so an "
              "empty b_before_after_cube/b_delta_summary here means a real problem (empty cohort, "
              "join fan-out caught upstream), not future-month thinness - investigate before shipping.")
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
    ("A1b", "a1_lob_dedup.csv", len(a1_lob_dedup_pd)),
    ("A2", "a2_mne_rates.csv", len(a2_mne_rates_pd)),
    ("A3", "a3_contact_cube.csv", len(a3_pd)),
    ("A4", "a4_profile_cube.csv", len(a4_pd)),
    ("B", "b_before_after_cube.csv", len(b_pd)),
    ("B2", "b_delta_summary.csv", len(b_delta_pd)),
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
print("All eight deliverables land under:", OUT_DIR)


# %% [18] AUTO-RETIREMENT (NEW 2026-08-03e) - deletes HDFS dirs superseded by this build's Piece-B
# then/now/delta redesign (b_cohort_v2/b_dfp_v2/b_bhv_v2, the Aug-anchor/regime-era Piece B) and any
# leftover *_v1 dirs from earlier landings (a1_client_v1, b_cohort_v1, b_dfp_v1, b_bhv_v1,
# ucp_enriched_a2_v1, ucp_enriched_a_v1, ucp_enriched_a). ALWAYS runs (not gated by RUN_PULLS or
# SMOKE) - this is Andre's "your code doesn't know how to replace" fix: versioning now retires its
# own predecessors instead of leaving dead HDFS dirs for the next person to trip over.
#
# REFUSE-TO-DELETE GUARD: before deleting ANYTHING, re-probe THIS build's own CURRENT outputs
# (.limit(1) on each landed dir) for EVERY family that has a predecessor in _RETIRE_LIST below -
# not just the three Piece-B v3 dirs. a1_client_v1 and the ucp_enriched_a* v1 dirs are also on the
# deletion list, so their current replacements (A1_DIR = a1_client_v2, UCPA_DIR =
# ucp_enriched_a3_v1) are probed too (red-team W1: the guard must protect every family the deletion
# list touches, never rely on Run-All cell ordering having already landed them this session). If
# any probe fails, this run's replacement is not actually in place, and deleting the predecessor
# would destroy the only good copy - deletion is skipped (loudly) in that case, not forced through.
# Under SMOKE=True the probe only confirms bite 0 landed (that is all a SMOKE run pulls) - printed
# explicitly below so a SMOKE run's retirement isn't mistaken for a full-population confirmation.

_RETIRE_GUARD_DIRS = [
    ("b_cohort_v%d" % B_SCHEMA_VERSION, BCOHORT_DIR),
    ("b_dfp_v%d" % B_SCHEMA_VERSION, BDFP_DIR),
    ("b_bhv_v%d" % B_SCHEMA_VERSION, BBHV_DIR),
    ("b_ucp_v%d" % B_SCHEMA_VERSION, BUCP_DIR),
    ("a1_client_v%d" % CARDS_SCHEMA_VERSION, A1_DIR),          # protects a1_client_v1 deletion below
    ("ucp_enriched_a3_v%d" % SCHEMA_VERSION, UCPA_DIR),        # protects ucp_enriched_a2_v1/a_v1/a deletion below
]
_retire_guard_ok = True
for _name, _dir in _RETIRE_GUARD_DIRS:
    try:
        _n_probe = spark.read.parquet(_dir + "bite_?").limit(1).count()
        if _n_probe == 0:
            print("RETIREMENT GUARD: %s has no readable rows at %s - refusing to delete anything "
                  "this run until the v3 replacement is confirmed landed." % (_name, _dir))
            _retire_guard_ok = False
        else:
            print("RETIREMENT GUARD: %s readable at %s - OK." % (_name, _dir))
    except Exception as _e:
        print("RETIREMENT GUARD: %s NOT readable at %s (%s) - refusing to delete anything this "
              "run." % (_name, _dir, type(_e).__name__))
        _retire_guard_ok = False
if _retire_guard_ok and SMOKE:
    print("RETIREMENT GUARD NOTE: SMOKE=True - the probes above only confirm bite 0 landed, not "
          "the full population. Deletion will proceed anyway (Andre's spec: readability, not "
          "population coverage, is the guard) - flip SMOKE=False and rerun before trusting a full "
          "non-SMOKE report off these outputs.")

_RETIRE_LIST = [
    "b_cohort_v2", "b_dfp_v2", "b_bhv_v2",              # Aug-anchor/regime-era Piece B, superseded
    "a1_client_v1", "b_cohort_v1", "b_dfp_v1", "b_bhv_v1",
    "ucp_enriched_a2_v1", "ucp_enriched_a_v1", "ucp_enriched_a",
]

if not _retire_guard_ok:
    print("AUTO-RETIREMENT SKIPPED ENTIRELY - this run's own v3 outputs did not pass the "
          "readability probe above. Nothing was deleted. Re-run Cells [12]-[15b] to land v3 "
          "properly, then re-run this cell.")
else:
    print("AUTO-RETIREMENT - deleting superseded HDFS dirs (v3 outputs confirmed readable above):")
    for _old in _RETIRE_LIST:
        _old_path = BASE + _old + "/"
        _exists = True
        try:
            spark.read.parquet(_old_path + "bite_?").limit(1).count()
        except Exception:
            _exists = False
        if not _exists:
            print("  SKIP (not present):", _old_path)
            continue
        _rc = _sp.run(["hdfs", "dfs", "-rm", "-r", "-skipTrash", _old_path],
                       capture_output=True, text=True)
        if _rc.returncode == 0:
            print("  DELETED:", _old_path)
        else:
            print("  FAILED to delete", _old_path, "-", _rc.stderr.strip()[:300])
