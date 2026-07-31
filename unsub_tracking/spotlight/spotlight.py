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
#    stayer vs leaver.
# 5. The UCP join now casts CLNT_NO through decimal(18,0)->long on BOTH sides before any
#    comparison, prints 5 sample ids from each side before joining, and HARD-ERRORS if match
#    rate < UCP_MATCH_FLOOR (70%, see Cell [0]). See memory note pandas_float_keys_scientific_notation:
#    pandas float64 client ids
#    render in scientific notation on a straight string cast, which silently zero-matches a join.
#
# ============================== 2026-07-31 WIRE-COST REDESIGN (schema v2 -> v3) ==============
# PROBLEM: v2's Cell [1] pulled one row per (clnt_no, mne) BANK-WIDE across ~250 mnemonics over
# 12 months = ~94M rows through teradatasql at ~22k rows/sec = ~70 minutes of wire time, most of
# it moving rows that every downstream cell immediately GROUP BY'd back down. Teradata could
# aggregate almost all of that server-side before it ever left the wire.
#
# FIX: v2's single pull is replaced by THREE pulls, each aggregated server-side to the grain the
# downstream cells actually need:
#   PULL A (clientagg_v3) - GROUP BY m.CLNT_NO, bank-wide. ~15M rows. Client-level counts, with
#     is_cards/is_regulatory/is_branded resolved via SQL IN-list tags INSIDE the aggregation
#     (this pull has no mne column to tag client-side afterward - the tagging has to happen
#     before the GROUP BY collapses mne away).
#   PULL B (mne_agg_v3) - GROUP BY mne, cohort_month, bank-wide. ~3,000 rows. NOT bitten (too
#     small to need it). Cadence (median days between sends) is a nested aggregate: per
#     (client, mne) gap, then PERCENTILE_CONT(0.5) per mne, via a derived table (no QUALIFY).
#   PULL C (cards_pair_v3) - GROUP BY clnt_no, mne, CARDS MNES ONLY (SUBSTR(...) IN cards list).
#     ~10M rows. The ONLY pull that filters by mne - it exists because it is the only output that
#     needs a campaign and a client attribute on the same row (cube1_profiling).
#
# CONSEQUENCE - cube1_profiling narrows: it is now cards-only (12 mnes), because Pull C (its only
# source with client x mne on one row) is cards-only. cube1_allclients is added alongside it to
# keep the age/tenure/product-depth cut at FULL bank-wide coverage (Pull A + UCP, no mne dim).
#
# SECOND CONSEQUENCE (flagged in the delivery report, not silently dropped): the old bank-wide
# "which mnes look like business/commercial audiences" UCP-match shape test (250 mnes) needed
# client x mne rows for EVERY mne, which no longer exist post-optimization - only cards mnes still
# have that grain (Pull C). ucp_match_by_mne is therefore cards-only (12 mnes) now, not 250. The
# bank-wide match-rate and stayer/leaver bias guards (which only need client-level unsub_flag, not
# mne) still run at full coverage in the UCP join cell.
#
# base_v2/ (the old grain) is NOT reused - v3's schema is entirely different (three files, not
# one). SCHEMA_VERSION bumped 2 -> 3; each pull gets its own landed path.
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
# ENGINE: Cell [1a] and Cell [1] (all three pulls) are Teradata-direct (DTZV01.* tables, no
#   catalog prefix, Teradata syntax, teradatasql connector). Every cell after them is PySpark
#   (YARN, Lumina pre-initialized session) reading landed parquet off HDFS - no further EDW
#   connection is opened. references/query_engine_guidelines.md "PySpark (YARN) - Known Gotchas".
#
# CARDS_MNES v2 (12 MNEs) - carried VERBATIM from unsub_value_museum.py:119-120 (Andre,
#   2026-07-31). PCQ, PCL, PCD, AUH, CLI, CRV, VBA, VBU, CRO, CEC, VIF, MET. Used to tag
#   is_cards (Fix 1) AND, in Pull C only, as the deliberate SQL filter (the one pull allowed to
#   filter by mne - see WIRE-COST REDESIGN above).
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
# TERADATA MEDIAN/PERCENTILE_CONT - per this redesign's own brief (Andre, 2026-07-31): Teradata
#   Vantage supports PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY col) OVER (PARTITION BY ...) as an
#   ordered analytical (OLAP) function. Used in Pull B's cadence measure via a derived table
#   (wrapped + DISTINCT to collapse back to one row per mne), NOT QUALIFY, per instruction.
#
# ============================== OPEN QUESTIONS - see delivery report for the full list =========
# - Whether disposition_cd=4 is a per-list (per-MNE) unsub or a global/ESP-level opt-out is NOT
#   settled anywhere in UNSUB_TRACKING_KNOWLEDGE.md. Cube 1's "bucket" (STAYER/LEAVER) is read at
#   (clnt_no, mne) grain using that row's own unsub_flag - if disposition_cd=4 is actually a
#   global opt-out that gets logged against whichever campaign's send triggered it, a LEAVER tag
#   on one mne does not necessarily mean the client unsubscribed FROM that mne specifically.
# - Which table holds ACTION_TYPE and whether it joins to TACTIC_ID/MNE (see REGULATORY_MNES
#   note above) - would let REGULATORY_MNES be derived instead of hardcoded.
# - Bank-wide (250-mne) UCP-match shape test is gone post-redesign (see WIRE-COST REDESIGN,
#   second consequence) - only the cards-only (12-mne) version remains.
#
# ============================== 2026-07-31 SPOOL/CORRECTNESS FIX (schema v4 -> v5) ============
# Audit found the bite filter (MOD on CLNT_NO) sitting in the outer WHERE of Pulls A/C, AFTER the
# join - every bite still spooled the full ek aggregate and full MASTER DISTINCT before narrowing,
# so biting cut wire traffic but not spool (this is what threw Teradata 2646). FIX: MOD moved
# INSIDE the MASTER DISTINCT subquery on both pulls, so MASTER is filtered to its slice before the
# join ever runs.
#
# Pull B was the single biggest spool exposure: one statement materialized the full ek aggregate,
# full MASTER DISTINCT, a ~94M-row client_mne, AND a PERCENTILE_CONT gap set - four large
# concurrent spools, unbitten, on every run regardless of SMOKE. FIX: cadence (median days between
# sends) is REMOVED from Pull B's SQL entirely. Pull B is now bitten exactly like A/C (MOD inside
# the MASTER subquery, landed per-bite to mne_agg_v5/bite_N) - its per-bite output is mne x month
# rows only, so it stays tiny. Cadence is instead computed in Spark from Pull C (already client x
# mne grain, cards-only), so cadence exists for the 12 cards MNEs only - the brief only asks
# cadence for cards campaigns anyway.
#
# Pull B's WHERE n_sends >= 1 filter silently dropped (client, mne) rows that carried an unsub but
# no in-window send - Pulls A and C keep those rows, so unsub_clients in q_mne/q_trend undercounted
# against leavers in cube1 and the cubes did not reconcile. That filter is removed.
#
# ucp_enriched_full is now asserted row-count-equal to base_ids right after it is built - a
# duplicate-CLNT_NO UCP snapshot would otherwise fan out every left join downstream silently.
#
# SMOKE runs now write to a _smoke-suffixed HDFS out/ prefix and local dir, and every cube carries
# a smoke_run (1/0) column - a full Run All under SMOKE used to be indistinguishable from a real
# one.
#
# _landed() used to trust a directory's mere existence - a session killed mid-bite left a short
# directory that a rerun would silently accept as "already landed". Every landed path now gets a
# sibling _ROWCOUNT marker recording the expected count; _landed() only returns True if the marker
# exists AND the actual parquet count matches it.
#
# WASTE removed: Pull C no longer lands is_cards/is_regulatory/is_branded (constant on every row -
# it is cards-filtered in SQL already); cube1_profiling drops those two as groupBy dims. Cube
# DataFrames are cached before their first action so toPandas()+write does not re-run the DAG
# twice. Dead code removed: _WIN_FLOOR_ORIG, the duplicate _band def, the duplicate
# autoBroadcastJoinThreshold set, the unconsumed "bucket" column in Cell [6].
#
# SCHEMA_VERSION bumped 4 -> 5: Pull B's landed shape (no more median/clients_used_for_gap columns)
# and Pull C's landed shape (no more is_cards/is_regulatory/is_branded columns) both change.


# %% [0] CONFIG - every tunable lives here. No literal below this cell is hand-typed elsewhere.

import calendar
import datetime

# ---- Analysis window ----
WIN_FLOOR = "2025-08-01"   # event window opens here
# ---- Campaign id scope. Shape only, no vintage test, no tactic-table whitelist. ----
# Decided 2026-07-31 after preflight5. Three things settled it:
#   1. DTZV01.TACTIC_EVNT_IP_AR_H60M is NOT a bank-wide whitelist. 2026084QCF and 2026085QCF are
#      correctly formed, dated inside the window, carry 7,315,801 sends between them, and have no
#      row in it. Same for 2025270ERI and 2021342KFI. Likely ODS-deployed campaigns. Whitelisting
#      against that table would delete real programs.
#   2. A YEAR RANGE is the wrong instrument in both directions - it discarded live ids and kept
#      residue - and it is unnecessary once the anchor changes.
#   3. The Julian day in an id records when the ID WAS MINTED, not when the email went out.
#      2018319KVM was still sending in 2026, eight years apart. Only the send date is a fact about
#      the client, so EVERY date in this pipeline is anchored on disposition_dt_tm and the id is
#      read for its mnemonic alone.
#
# What remains is a pure shape test: 10 chars, first 7 numeric, last 3 the mnemonic. That excludes
# DEFAULT, CABVRSN1, COI, ESPTVER2 and the rest of the 29 non-conforming ids (~6% of send volume,
# 18.4M of it DEFAULT and CABVRSN1 alone) and keeps every real campaign regardless of vintage.
#
# Cost, stated: two waves of the same mnemonic in one month collapse into one row. The brief asks
# for unsubs by campaign, not by wave, so nothing is lost here - but per-wave questions would need
# the tactic table back.
TACTIC_ID_SHAPE_ONLY = True
TACTIC_ID_SQL = """
          AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
          AND SUBSTR(TRIM(TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'""" if TACTIC_ID_SHAPE_ONLY else ""

WIN_CEIL  = "2026-08-01"   # HARD ceiling. Without it each statement runs floor-to-its-own-clock,
                           # and the pulls run minutes or days apart because resume is a feature -
                           # at ~35K unsubs/month bank-wide that is ~1K/day of drift BETWEEN pulls,
                           # so the cubes would not reconcile against each other.
MASTER_FLOOR = "2025-05-01"  # MASTER is filtered on load_tm, which is a RECORD-LOAD timestamp, not
                           # a send date (archaeology/18_vendor_retention_probe.sql:5). With no
                           # margin, an unsub inside the window whose MASTER row loaded earlier is
                           # deleted by the INNER JOIN. 28.5%% of unsubs lag their send by >30 days
                           # (RUN_2026-07-31_scope_test.sql), so a zero-margin floor systematically
                           # undercuts leavers in exactly the months q_trend reports. 3-month
                           # lookback matches pack 19, pack 20 and museum/20_lookback_cards.sql.
# WIN_CEIL above is a HARD ceiling, not "floor to now" - see its own comment: without a fixed end
# date the pulls (which resume across sessions) drift apart in time and the cubes stop reconciling.
# Move WIN_CEIL forward by hand each sprint to pick up new sends/unsubs.

# ---- MNE tag sets ----
# CARDS_MNES: client-side tag everywhere EXCEPT Pull C, where it is the one deliberate SQL filter.
# Never used as a SQL filter in Pull A or Pull B.
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

# SQL IN-list literals, built once here so no pull hand-types the MNE codes.
CARDS_LIST_SQL = ", ".join("'%s'" % m for m in sorted(CARDS_MNES))
REGULATORY_LIST_SQL = ", ".join("'%s'" % m for m in sorted(REGULATORY_MNES))

# ---- Bite plan for the two expensive pulls (Pull A, Pull C in Cell [1]) ----
# ---- RUN SWITCHES - the only things to touch before hitting Run All ----
WRITE_CLIENT_EXTRACT = False  # Cell [8]. Client-grain file for local ad-hoc work. Off by
                              # default: nothing at client grain is meant to persist.
DROP_CLIENT_GRAIN = False     # Cell [9]. Delete clientagg_v5 + cards_pair_v5 + ucp_enriched once
                              # the cubes land. Leave False until the cubes are checked.

LAND_CHUNK_ROWS = 1_500_000   # rows per createDataFrame call. A full bite of the old bank-wide
                              # client x mne pull serialized to just over Spark's 128 MB RPC
                              # ceiling; this keeps each payload well under it. Lower it if the
                              # limit is still hit on Pull A, B or C.

N_BITES = 10   # MOD(CLNT_NO, N_BITES) - applies to Pull A, Pull B and Pull C (all three pulls, as
               # of the v4->v5 fix - Pull B used to skip biting but was the single biggest spool
               # exposure unbitten, see the SPOOL/CORRECTNESS FIX header note). Each bite is
               # landed and checked before running, so a killed run resumes at the next un-landed
               # bite.

SMOKE = True   # True  -> pull bite 0 only, for Pull A, Pull B and Pull C.
               # False -> all 10 bites, full population, for all three pulls.

# ---- Trailing send-frequency windows - Pull A hardcodes 3m/6m columns by name
# (n_emails_3m, n_emails_6m, ..._promo_6m, ..._regulatory_6m, ..._cards_6m); FREQ_WINDOWS must
# keep 3 and 6 in it (asserted below) since those are the two windows baked into Pull A's SQL.
FREQ_WINDOWS = [3, 6]
assert set(FREQ_WINDOWS) >= {3, 6}, (
    "Pull A's SQL hardcodes n_emails_3m/n_emails_6m (and the 6m promo/regulatory/cards variants) "
    "by name - FREQ_WINDOWS must include 3 and 6 or those columns have no cutoff to use.")


def _months_ago(n_months, anchor=None):
    """Calendar-month subtraction, stdlib only (no dateutil dependency)."""
    anchor = anchor or datetime.date.today()
    total = anchor.year * 12 + (anchor.month - 1) - n_months
    y, m = divmod(total, 12)
    m += 1
    day = min(anchor.day, calendar.monthrange(y, m)[1])
    return datetime.date(y, m, day)


# Computed ONCE, at Cell [0] run time, and baked into Pull A's SQL in Cell [1] - all bites in one
# run share the same cutoff dates, which is what "trailing N months as of today" should mean.
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

# ---- Paths - HDFS only. No local disk path anywhere in this file except Cell [7]'s driver-side
# download convenience (explicitly the one place that is allowed, see that cell's docstring). ----
BASE = "hdfs:///user/427966379/unsub_spotlight/"   # reference_andre_hdfs_user_path.md
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"          # references/ucp/README.md - personal only

# Landed-schema version. The _landed() cache guard checks path existence, NOT columns - so a
# schema change silently reuses stale parquet from a prior design. Bump this whenever a pull's
# output columns change; the new schema lands in a fresh directory and cannot collide.
# v1 = cards-filtered, untagged, one pull. v2 = bank-wide one pull, client x mne grain, ~94M rows.
# v3 = THREE pulls, each aggregated server-side to the grain downstream actually needs (see
# WIRE-COST REDESIGN above). base_v2/ is NOT reused - v3 is a different shape entirely.
# v4 = MASTER deduped + null clients dropped + EVENT retries collapsed.
SCHEMA_VERSION = 6   # v6 = tactic-id-only scope   # v5 = Pull B bitten (MOD moved inside the MASTER subquery like A/C; cadence
                     # columns dropped from its SQL - computed in Spark from Pull C instead) +
                     # Pull C drops the constant is_cards/is_regulatory/is_branded columns.

# Floor for the UCP join guard. Catches a broken join key (which shows near-zero), NOT ordinary
# non-match: UCP is personal-only on one closed-month snapshot, so business/commercial recipients
# and clients who closed after their last send never match. Observed 2026-07-31: 87%.
UCP_MATCH_FLOOR = 70.0

CLIENTAGG_DIR = BASE + "clientagg_v%d/" % SCHEMA_VERSION   # Pull A - client grain, bank-wide
MNEAGG_DIR = BASE + "mne_agg_v%d/" % SCHEMA_VERSION        # Pull B - mne x cohort_month, bank-wide
CARDSPAIR_DIR = BASE + "cards_pair_v%d/" % SCHEMA_VERSION  # Pull C - client x mne, cards only

# Every deliverable output (cubes, xlsx, zip) lands under a _smoke-suffixed prefix when SMOKE is
# True, so a smoke run's artifacts can never be mistaken for a full one - see the
# SPOOL/CORRECTNESS FIX header note. smoke_run (1/0) is also stamped onto every cube DataFrame.
OUT_DIR = BASE + ("out_smoke/" if SMOKE else "out/")

CLIENTAGG_COLS = [
    "clnt_no", "n_campaigns_all", "n_campaigns_branded", "n_campaigns_cards",
    "n_campaigns_regulatory", "n_emails_all", "n_emails_3m", "n_emails_6m",
    "n_emails_promo_6m", "n_emails_regulatory_6m", "n_emails_cards", "n_emails_cards_6m",
    "unsub_flag", "unsub_dt", "first_send_dt", "last_send_dt",
]
# median_days_between_sends / clients_used_for_gap are GONE from Pull B's landed schema (v5) -
# cadence is now computed in Spark from Pull C (cards-only), not in Pull B's SQL. See Cell [1]
# Pull B and Cell [2].
MNEAGG_COLS = [
    "mne", "cohort_month_num", "clients_mailed", "sends", "unsub_clients",
    "is_cards", "is_regulatory", "is_branded",
]
# is_cards / is_regulatory / is_branded are GONE from Pull C's landed schema (v5) - Pull C is
# cards-filtered in SQL already, so all three were constants (1, 0, 1) on every one of its ~10M
# rows. cube1_profiling now sets them literally instead of carrying them as columns.
CARDSPAIR_COLS = [
    "clnt_no", "mne", "n_sends", "unsub_flag", "unsub_dt", "first_send_dt", "last_send_dt",
]


def read_clientagg():
    """Pull A reader. Every downstream cell reads through here so a stale/short schema fails
    loudly instead of silently running on a partial column set."""
    sdf = spark.read.parquet(CLIENTAGG_DIR + "*")
    missing = [c for c in CLIENTAGG_COLS if c not in sdf.columns]
    if missing:
        raise RuntimeError(
            "clientagg at %s is missing %s. Found: %s. Stale parquet from an older schema - "
            "delete %s and rerun Cell [1]'s Pull A." % (CLIENTAGG_DIR, missing, sdf.columns, CLIENTAGG_DIR))
    return sdf.withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))


def read_mneagg():
    """Pull B reader."""
    sdf = spark.read.parquet(MNEAGG_DIR + "*")
    missing = [c for c in MNEAGG_COLS if c not in sdf.columns]
    if missing:
        raise RuntimeError(
            "mne_agg at %s is missing %s. Found: %s. Stale parquet from an older schema - "
            "delete %s and rerun Cell [1]'s Pull B." % (MNEAGG_DIR, missing, sdf.columns, MNEAGG_DIR))
    return sdf


def read_cardspair():
    """Pull C reader."""
    sdf = spark.read.parquet(CARDSPAIR_DIR + "*")
    missing = [c for c in CARDSPAIR_COLS if c not in sdf.columns]
    if missing:
        raise RuntimeError(
            "cards_pair at %s is missing %s. Found: %s. Stale parquet from an older schema - "
            "delete %s and rerun Cell [1]'s Pull C." % (CARDSPAIR_DIR, missing, sdf.columns, CARDSPAIR_DIR))
    return sdf.withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))


print("CONFIG loaded - floor:", WIN_FLOOR, "| cards MNEs:", len(CARDS_MNES),
      "| regulatory MNEs:", len(REGULATORY_MNES), "| bites (Pull A/B/C):", N_BITES, "| SMOKE:", SMOKE)
print("FREQ_WINDOWS:", FREQ_WINDOWS, "-> cutoffs:", FREQ_CUTOFFS)
print("SCHEMA_VERSION:", SCHEMA_VERSION, "| clientagg:", CLIENTAGG_DIR,
      "| mne_agg:", MNEAGG_DIR, "| cards_pair:", CARDSPAIR_DIR, "| OUT_DIR:", OUT_DIR)


# %% [1a] REMOVED - the "cheap probe" was not cheap. COUNT(DISTINCT consumer_id_hashed) over the
# full 400M+ row event window blew spool (Teradata 2646) before the pulls even started, and the
# numbers it wanted are free downstream anyway: Pull A lands one row per client, so its row count
# IS the distinct-client count, and Pull B returns clients_mailed and sends per mne. Enough
# probing - preflight.sql and preflight2.sql already settled the questions that mattered.


# %% [1] PULLS - THREE Teradata-direct pulls replacing the old single ~94M-row pull (see
# WIRE-COST REDESIGN at the top of this file). Each is aggregated server-side to the grain
# downstream cells need, so the network only carries rows that could not be collapsed further.
# ENGINE: Teradata-direct (DTZV01.* tables, no catalog prefix, Teradata syntax, teradatasql).
#
#   PULL A -> clientagg_v5 - GROUP BY CLNT_NO, bank-wide. Bitten, landed, resumable.
#   PULL B -> mne_agg_v5   - GROUP BY mne, cohort_month, bank-wide. Bitten (v5), landed, resumable
#     - no cadence in its SQL any more, see Cell [2].
#   PULL C -> cards_pair_v5 - GROUP BY CLNT_NO, mne, CARDS MNES ONLY. Bitten, landed, resumable.

import time
import getpass
import teradatasql
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, IntegerType

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


def _rowcount_marker_path(path):
    # Sibling to the landed directory, not nested inside it - a stray non-chunk file inside the
    # parquet dir risks confusing schema inference on reload. Written by _write_chunks() after
    # every chunk has landed, holding the count _landed() must match to trust the directory.
    return path.rstrip("/") + "_ROWCOUNT"


def _landed(path):
    """A directory existing is NOT proof a bite fully landed - a session killed mid-write leaves a
    short directory that used to be silently accepted as done, undercounting downstream. A landed
    path is now only trusted if its sibling _ROWCOUNT marker exists AND matches the actual count."""
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
        print(path, ": parquet exists but no _ROWCOUNT marker - pre-fix or partial write, "
                     "treating as NOT landed and re-pulling.")
        return False

    if _n_actual != _n_expected:
        print("WARNING:", path, "- _ROWCOUNT marker expects", _n_expected, "actual rows are",
              _n_actual, "- partial write from a killed session. Re-pulling this bite.")
        return False
    return True


def _tag_mne_client_side(sdf, mne_col="mne"):
    """is_cards / is_regulatory / is_branded computed in Spark from the mne column, never as a
    SQL WHERE clause (Pull C's cards filter is the sole deliberate exception, and even there
    these three columns are still derived the same way so cube1_profiling's dims are consistent
    with q_mne/q_trend's)."""
    mne_u = F.upper(F.trim(F.col(mne_col)))
    unbranded = mne_u.isNull() | (mne_u == "") | (mne_u == "DEFAULT")
    return (sdf
            .withColumn("is_branded", F.when(unbranded, F.lit(0)).otherwise(F.lit(1)))
            .withColumn("is_cards", F.when(mne_u.isin(sorted(CARDS_MNES)), F.lit(1)).otherwise(F.lit(0)))
            .withColumn("is_regulatory", F.when(mne_u.isin(sorted(REGULATORY_MNES)), F.lit(1)).otherwise(F.lit(0))))


def _write_chunks(pdf, schema, hdfs_path, tag_fn=None, tag_col=None):
    """Shared chunked-write helper for all three pulls - a full-size payload exceeds Spark's
    128 MB RPC ceiling, so every pull writes in LAND_CHUNK_ROWS-row slices, never one shot."""
    _first = True
    for _s in range(0, len(pdf), LAND_CHUNK_ROWS):
        _part = pdf.iloc[_s:_s + LAND_CHUNK_ROWS]
        _sdf = spark.createDataFrame(_part, schema=schema)
        if tag_fn is not None:
            _sdf = tag_fn(_sdf, tag_col) if tag_col else tag_fn(_sdf)
        _sdf.write.mode("overwrite" if _first else "append").parquet(BASE + hdfs_path)
        _first = False
        print("   ...", hdfs_path, "chunk", _s, "-", _s + len(_part), "written", flush=True)
    nback = spark.read.parquet(BASE + hdfs_path).count()

    # _ROWCOUNT marker: written ONLY after every chunk has landed, so a session killed mid-bite
    # never leaves a marker behind - a rerun's _landed() check then correctly sees "no marker,
    # not landed" instead of trusting a short directory.
    _marker_schema = StructType([StructField("expected_rows", LongType(), False)])
    _marker_sdf = spark.createDataFrame([(int(nback),)], schema=_marker_schema)
    _marker_sdf.coalesce(1).write.mode("overwrite").parquet(BASE + _rowcount_marker_path(hdfs_path))

    return nback


# ---------------------------------------------------------------------------------------------
# PULL A - clientagg_v5. GROUP BY CLNT_NO, bank-wide. Tags resolved via SQL IN-lists INSIDE the
# aggregation (this grain has no mne column left to tag client-side).
# ---------------------------------------------------------------------------------------------

CLIENTAGG_SPARK_SCHEMA = StructType([
    StructField("clnt_no", LongType(), True),
    StructField("n_campaigns_all", LongType(), True),
    StructField("n_campaigns_branded", LongType(), True),
    StructField("n_campaigns_cards", LongType(), True),
    StructField("n_campaigns_regulatory", LongType(), True),
    StructField("n_emails_all", LongType(), True),
    StructField("n_emails_3m", LongType(), True),
    StructField("n_emails_6m", LongType(), True),
    StructField("n_emails_promo_6m", LongType(), True),
    StructField("n_emails_regulatory_6m", LongType(), True),
    StructField("n_emails_cards", LongType(), True),
    StructField("n_emails_cards_6m", LongType(), True),
    StructField("unsub_flag", IntegerType(), True),
    StructField("unsub_dt", DateType(), True),
    StructField("first_send_dt", DateType(), True),
    StructField("last_send_dt", DateType(), True),
])


def _prep_clientagg(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    pdf["clnt_no"] = pd.to_numeric(pdf["clnt_no"], errors="coerce").astype("int64")
    _int_cols = ["n_campaigns_all", "n_campaigns_branded", "n_campaigns_cards",
                 "n_campaigns_regulatory", "n_emails_all", "n_emails_3m", "n_emails_6m",
                 "n_emails_promo_6m", "n_emails_regulatory_6m", "n_emails_cards", "n_emails_cards_6m"]
    for _c in _int_cols:
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce").fillna(0).astype("int64")
    pdf["unsub_flag"] = pd.to_numeric(pdf["unsub_flag"], errors="coerce").fillna(0).astype("int32")
    for _c in ["unsub_dt", "first_send_dt", "last_send_dt"]:
        pdf[_c] = pd.to_datetime(pdf[_c], errors="coerce").dt.date
        pdf[_c] = pdf[_c].where(pdf[_c].notna(), None)
    return pdf[[f.name for f in CLIENTAGG_SPARK_SCHEMA.fields]]


def land_pullA_bite(bite):
    name = "clientagg_v%d/bite_%d" % (SCHEMA_VERSION, bite)
    if _landed(name):
        n = spark.read.parquet(BASE + name).count()
        print(name, ": already landed,", n, "rows - SKIP")
        return

    sql = """
    WITH ek AS (
        -- One row per (client, treatment, disposition, DAY).
        -- P3 found 8,084,229 pairs with more than one cd=1 timestamp and I assumed retries. Q12
        -- disproved that: barely a quarter are same-day and over a third are more than 30
        -- deleted roughly 3M emails. Collapsing to the DAY keeps genuine re-sends and removes
        -- only same-day retry rows.
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
               ek.disposition_cd AS disposition_cd,
               CAST(ek.disposition_dt_tm AS DATE) AS evt_dt
        FROM ek
        -- MASTER is NOT one row per (consumer_id_hashed, TREATMENT_ID): preflight P1 measured
        -- 1.123 rows per key, max 95,014. P6 showed 376,129,145 of 385,665,736 keys map to
        -- exactly one client, so it is duplication, not ambiguity, and DISTINCT resolves it.
        -- P7 showed the worst keys carry a NULL CLNT_NO under junk TREATMENT_IDs ('DEFAULT',
        -- 'CABVRSN1'); those are dropped here explicitly rather than lost inside MOD(NULL).
        -- Bite filter lives INSIDE this DISTINCT subquery, not in the outer WHERE - MASTER is
        -- ~385M keys; filtering it to its bite slice BEFORE the join is what actually shrinks the
        -- join's spool. A WHERE on the join output still spools the full MASTER DISTINCT first
        -- (this is what threw Teradata 2646 - biting cut wire traffic but not spool).
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE load_tm >= DATE '%(mfloor)s'
                      AND CLNT_NO IS NOT NULL
                      AND MOD(ABS(CLNT_NO), %(n_bites)d) = %(bite)d) m
          ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
    )
    SELECT
        clnt_no,
        COUNT(DISTINCT mne) AS n_campaigns_all,
        COUNT(DISTINCT CASE WHEN mne IS NOT NULL AND UPPER(TRIM(mne)) NOT IN ('', 'DEFAULT')
                             THEN mne END) AS n_campaigns_branded,
        COUNT(DISTINCT CASE WHEN mne IN (%(cards)s) THEN mne END) AS n_campaigns_cards,
        COUNT(DISTINCT CASE WHEN mne IN (%(reg)s) THEN mne END) AS n_campaigns_regulatory,
        SUM(CASE WHEN disposition_cd = 1 THEN 1 ELSE 0 END) AS n_emails_all,
        SUM(CASE WHEN disposition_cd = 1 AND evt_dt >= DATE '%(cut3)s' THEN 1 ELSE 0 END) AS n_emails_3m,
        SUM(CASE WHEN disposition_cd = 1 AND evt_dt >= DATE '%(cut6)s' THEN 1 ELSE 0 END) AS n_emails_6m,
        SUM(CASE WHEN disposition_cd = 1 AND evt_dt >= DATE '%(cut6)s'
                  AND mne IS NOT NULL AND UPPER(TRIM(mne)) NOT IN ('', 'DEFAULT')
                  AND mne NOT IN (%(reg)s)
                 THEN 1 ELSE 0 END) AS n_emails_promo_6m,
        SUM(CASE WHEN disposition_cd = 1 AND evt_dt >= DATE '%(cut6)s' AND mne IN (%(reg)s)
                 THEN 1 ELSE 0 END) AS n_emails_regulatory_6m,
        SUM(CASE WHEN disposition_cd = 1 AND mne IN (%(cards)s) THEN 1 ELSE 0 END) AS n_emails_cards,
        SUM(CASE WHEN disposition_cd = 1 AND evt_dt >= DATE '%(cut6)s' AND mne IN (%(cards)s)
                 THEN 1 ELSE 0 END) AS n_emails_cards_6m,
        MAX(CASE WHEN disposition_cd = 4 THEN 1 ELSE 0 END) AS unsub_flag,
        MIN(CASE WHEN disposition_cd = 4 THEN evt_dt END) AS unsub_dt,
        MIN(CASE WHEN disposition_cd = 1 THEN evt_dt END) AS first_send_dt,
        MAX(CASE WHEN disposition_cd = 1 THEN evt_dt END) AS last_send_dt
    FROM joined
    GROUP BY clnt_no
    """ % {"floor": WIN_FLOOR, "mfloor": MASTER_FLOOR, "ceil": WIN_CEIL, "tactic": TACTIC_ID_SQL,
           "n_bites": N_BITES, "bite": bite, "cards": CARDS_LIST_SQL,
           "reg": REGULATORY_LIST_SQL, "cut3": FREQ_CUTOFFS[3], "cut6": FREQ_CUTOFFS[6]}

    pdf = edw_pd(sql)
    assert len(pdf) > 0, name + " pulled zero rows for bite " + str(bite) + " - investigate before proceeding"
    pdf = _prep_clientagg(pdf)
    nback = _write_chunks(pdf, CLIENTAGG_SPARK_SCHEMA, name)
    assert nback == len(pdf), name + " HDFS readback mismatch: pulled %d, read back %d" % (len(pdf), nback)
    print(name, ": landed", len(pdf), "rows (bank-wide, client grain), HDFS readback confirms", nback)


for _b in (range(1) if SMOKE else range(N_BITES)):
    land_pullA_bite(_b)

print("PULL A done - landed at", CLIENTAGG_DIR + "*", "| one row per clnt_no, bank-wide.")


# ---------------------------------------------------------------------------------------------
# PULL B - mne_agg_v5. GROUP BY mne, cohort_month, bank-wide. Bitten exactly like A/C (v4->v5 fix -
# unbitten, this was the single biggest spool exposure: one statement materialized the full ek
# aggregate, full MASTER DISTINCT, a ~94M-row client_mne, AND a PERCENTILE_CONT gap set on every
# run regardless of SMOKE). Cadence is GONE from this SQL entirely - computed in Spark from Pull C
# instead (see Cell [2]), because a per-bite gap set can't be percentiled correctly bite-by-bite.
# Per-bite output is just mne x cohort_month rows, so it stays tiny even though it now runs once
# per bite.
# ---------------------------------------------------------------------------------------------

MNEAGG_SPARK_SCHEMA = StructType([
    StructField("mne", StringType(), True),
    StructField("cohort_month_num", LongType(), True),
    StructField("clients_mailed", LongType(), True),
    StructField("sends", LongType(), True),
    StructField("unsub_clients", LongType(), True),
])


def _prep_mneagg(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    pdf["mne"] = pdf["mne"].astype(str)
    pdf["cohort_month_num"] = pd.to_numeric(pdf["cohort_month_num"], errors="coerce").astype("int64")
    for _c in ["clients_mailed", "sends", "unsub_clients"]:
        pdf[_c] = pd.to_numeric(pdf[_c], errors="coerce").fillna(0).astype("int64")
    return pdf[[f.name for f in MNEAGG_SPARK_SCHEMA.fields]]


def land_pullB_bite(bite):
    name = "mne_agg_v%d/bite_%d" % (SCHEMA_VERSION, bite)
    if _landed(name):
        n = spark.read.parquet(BASE + name).count()
        print(name, ": already landed,", n, "rows - SKIP")
        return

    sql = """
    WITH ek AS (
        -- One row per (client, treatment, disposition, DAY).
        -- P3 found 8,084,229 pairs with more than one cd=1 timestamp and I assumed retries. Q12
        -- disproved that: barely a quarter are same-day and over a third are more than 30
        -- deleted roughly 3M emails. Collapsing to the DAY keeps genuine re-sends and removes
        -- only same-day retry rows.
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
               ek.disposition_cd AS disposition_cd,
               CAST(ek.disposition_dt_tm AS DATE) AS evt_dt
        FROM ek
        -- MASTER is NOT one row per (consumer_id_hashed, TREATMENT_ID): preflight P1 measured
        -- 1.123 rows per key, max 95,014. P6 showed 376,129,145 of 385,665,736 keys map to
        -- exactly one client, so it is duplication, not ambiguity, and DISTINCT resolves it.
        -- P7 showed the worst keys carry a NULL CLNT_NO under junk TREATMENT_IDs ('DEFAULT',
        -- 'CABVRSN1'); those are dropped here explicitly rather than lost inside MOD(NULL).
        -- Bite filter lives INSIDE this DISTINCT subquery (same fix as Pull A/C) - filtering
        -- MASTER to its slice before the join is what shrinks spool, not just wire traffic.
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE load_tm >= DATE '%(mfloor)s'
                      AND CLNT_NO IS NOT NULL
                      AND MOD(ABS(CLNT_NO), %(n_bites)d) = %(bite)d) m
          ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
    ),
    client_mne AS (
        -- No "WHERE n_sends >= 1" here (v4 had it and it was WRONG) - it dropped (client, mne)
        -- rows that carried an unsub but no in-window send, undercounting unsub_clients here
        -- against leavers in cube1/clientagg, which keep those rows. Every (clnt_no, mne) pair
        -- that appears in `joined` at all - sent, unsubbed, or both - is kept.
        SELECT clnt_no, mne,
               SUM(CASE WHEN disposition_cd = 1 THEN 1 ELSE 0 END) AS n_sends,
               MIN(CASE WHEN disposition_cd = 1 THEN evt_dt END) AS first_send_dt,
               MAX(CASE WHEN disposition_cd = 4 THEN 1 ELSE 0 END) AS unsub_flag
        FROM joined
        GROUP BY clnt_no, mne
    )
    SELECT mne,
           (EXTRACT(YEAR FROM first_send_dt) * 100 + EXTRACT(MONTH FROM first_send_dt))
               AS cohort_month_num,
           COUNT(DISTINCT clnt_no) AS clients_mailed,
           SUM(n_sends) AS sends,
           SUM(unsub_flag) AS unsub_clients
    FROM client_mne
    GROUP BY mne, cohort_month_num
    ORDER BY mne, cohort_month_num
    """ % {"floor": WIN_FLOOR, "mfloor": MASTER_FLOOR, "ceil": WIN_CEIL, "tactic": TACTIC_ID_SQL,
           "n_bites": N_BITES, "bite": bite}

    pdf = edw_pd(sql)
    assert len(pdf) > 0, name + " pulled zero rows for bite " + str(bite) + " - investigate before proceeding"
    pdf = _prep_mneagg(pdf)
    nback = _write_chunks(pdf, MNEAGG_SPARK_SCHEMA, name, tag_fn=_tag_mne_client_side)
    assert nback == len(pdf), name + " HDFS readback mismatch: pulled %d, read back %d" % (len(pdf), nback)
    print(name, ": landed", len(pdf), "rows (mne x cohort_month, bank-wide), HDFS readback confirms", nback)


for _b in (range(1) if SMOKE else range(N_BITES)):
    land_pullB_bite(_b)

print("PULL B done - landed at", MNEAGG_DIR + "*", "| one row per (mne, cohort_month) PER BITE, "
      "bank-wide - Cell [2] sums partial aggregates across bites to recover the true totals.")


# ---------------------------------------------------------------------------------------------
# PULL C - cards_pair_v5. GROUP BY CLNT_NO, mne, CARDS MNES ONLY - the one deliberate mne filter.
# is_cards/is_regulatory/is_branded are NOT landed here (v5) - they were CONSTANT on every row
# (1, 0, 1) because this pull is already cards-filtered in SQL, so landing them on ~10M rows was
# pure waste. cube1_profiling (Cell [5]) documents the constant values instead of carrying columns.
# ---------------------------------------------------------------------------------------------

CARDSPAIR_SPARK_SCHEMA = StructType([
    StructField("clnt_no", LongType(), True),
    StructField("mne", StringType(), True),
    StructField("n_sends", LongType(), True),
    StructField("unsub_flag", IntegerType(), True),
    StructField("unsub_dt", DateType(), True),
    StructField("first_send_dt", DateType(), True),
    StructField("last_send_dt", DateType(), True),
])


def _prep_cardspair(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    pdf["clnt_no"] = pd.to_numeric(pdf["clnt_no"], errors="coerce").astype("int64")
    pdf["mne"] = pdf["mne"].astype(str)
    pdf["n_sends"] = pd.to_numeric(pdf["n_sends"], errors="coerce").fillna(0).astype("int64")
    pdf["unsub_flag"] = pd.to_numeric(pdf["unsub_flag"], errors="coerce").fillna(0).astype("int32")
    for _c in ["unsub_dt", "first_send_dt", "last_send_dt"]:
        pdf[_c] = pd.to_datetime(pdf[_c], errors="coerce").dt.date
        pdf[_c] = pdf[_c].where(pdf[_c].notna(), None)
    return pdf[[f.name for f in CARDSPAIR_SPARK_SCHEMA.fields]]


def land_pullC_bite(bite):
    name = "cards_pair_v%d/bite_%d" % (SCHEMA_VERSION, bite)
    if _landed(name):
        n = spark.read.parquet(BASE + name).count()
        print(name, ": already landed,", n, "rows - SKIP")
        return

    sql = """
    WITH ek AS (
        -- One row per (client, treatment, disposition, DAY).
        -- P3 found 8,084,229 pairs with more than one cd=1 timestamp and I assumed retries. Q12
        -- disproved that: barely a quarter are same-day and over a third are more than 30
        -- deleted roughly 3M emails. Collapsing to the DAY keeps genuine re-sends and removes
        -- only same-day retry rows.
        SELECT consumer_id_hashed, TREATMENT_ID, disposition_cd,
               MIN(disposition_dt_tm) AS disposition_dt_tm
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd IN (1, 4)
          AND disposition_dt_tm >= DATE '%(floor)s'
          AND disposition_dt_tm <  DATE '%(ceil)s'%(tactic)s
          AND SUBSTR(TREATMENT_ID, 8, 3) IN (%(cards)s)
        GROUP BY 1, 2, 3, CAST(disposition_dt_tm AS DATE)
    ),
    joined AS (
        SELECT m.CLNT_NO AS clnt_no,
               SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne,
               ek.disposition_cd AS disposition_cd,
               CAST(ek.disposition_dt_tm AS DATE) AS evt_dt
        FROM ek
        -- MASTER is NOT one row per (consumer_id_hashed, TREATMENT_ID): preflight P1 measured
        -- 1.123 rows per key, max 95,014. P6 showed 376,129,145 of 385,665,736 keys map to
        -- exactly one client, so it is duplication, not ambiguity, and DISTINCT resolves it.
        -- P7 showed the worst keys carry a NULL CLNT_NO under junk TREATMENT_IDs ('DEFAULT',
        -- 'CABVRSN1'); those are dropped here explicitly rather than lost inside MOD(NULL).
        -- Bite filter lives INSIDE this DISTINCT subquery, not in the outer WHERE - see Pull A's
        -- comment on why (spool, not just wire traffic).
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE load_tm >= DATE '%(mfloor)s'
                      AND CLNT_NO IS NOT NULL
                      AND MOD(ABS(CLNT_NO), %(n_bites)d) = %(bite)d) m
          ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
    )
    SELECT clnt_no, mne,
           SUM(CASE WHEN disposition_cd = 1 THEN 1 ELSE 0 END) AS n_sends,
           MAX(CASE WHEN disposition_cd = 4 THEN 1 ELSE 0 END) AS unsub_flag,
           MIN(CASE WHEN disposition_cd = 4 THEN evt_dt END) AS unsub_dt,
           MIN(CASE WHEN disposition_cd = 1 THEN evt_dt END) AS first_send_dt,
           MAX(CASE WHEN disposition_cd = 1 THEN evt_dt END) AS last_send_dt
    FROM joined
    GROUP BY clnt_no, mne
    """ % {"floor": WIN_FLOOR, "mfloor": MASTER_FLOOR, "ceil": WIN_CEIL, "tactic": TACTIC_ID_SQL,
           "cards": CARDS_LIST_SQL, "n_bites": N_BITES, "bite": bite}

    pdf = edw_pd(sql)
    assert len(pdf) > 0, name + " pulled zero rows for bite " + str(bite) + " - investigate before proceeding"
    pdf = _prep_cardspair(pdf)
    # No tag_fn here (v5) - is_cards/is_regulatory/is_branded would be constant on every row of a
    # cards-filtered pull, so they are not landed at all. See Pull C's header note.
    nback = _write_chunks(pdf, CARDSPAIR_SPARK_SCHEMA, name)
    assert nback == len(pdf), name + " HDFS readback mismatch: pulled %d, read back %d" % (len(pdf), nback)
    print(name, ": landed", len(pdf), "rows (cards-only, client x mne), HDFS readback confirms", nback)


for _b in (range(1) if SMOKE else range(N_BITES)):
    land_pullC_bite(_b)

print("PULL C done - landed at", CARDSPAIR_DIR + "*", "| one row per (clnt_no, mne), cards-only.")

print("\nCell [1] done - all three pulls landed.",
      "clientagg:", CLIENTAGG_DIR, "| mne_agg:", MNEAGG_DIR, "| cards_pair:", CARDSPAIR_DIR)


# %% [2] Q_MNE - unsubs by campaign, absolute counts, BANK-WIDE (Andre filters to cards in the
# Excel pivot using the is_cards column). Print only, no CSV - row count is however many distinct
# mnemonics sent mail bank-wide in the window, still small enough to print in full.
# SOURCE: Pull B (mne_agg), rolled up across cohort_month AND across bites back to one row per mne.
# ENGINE: PySpark (YARN), reads landed mne_agg - no new EDW connection.
#
# Pull B is bitten (v5) - read_mneagg() reads mne_agg_v5/bite_0..N as one union, so a given
# (mne, cohort_month) combination has up to N_BITES partial rows, one contributed by each bite.
# Summing clients_mailed/sends/unsub_clients across those partial rows recovers the true total:
# each bite's SQL filtered MASTER by MOD(ABS(CLNT_NO), N_BITES) = bite BEFORE the join, so the
# bites partition CLIENTS disjointly - a given clnt_no's sends/unsub row can only ever land in
# ONE bite file. Summing COUNT(DISTINCT clnt_no)-per-bite is therefore exact, not an overcount.

mne_agg = read_mneagg()

q_mne = (mne_agg
         .groupBy("mne", "is_cards", "is_regulatory", "is_branded")
         .agg(F.sum("clients_mailed").alias("clients_mailed"),
              F.sum("sends").alias("sends"),
              F.sum("unsub_clients").alias("unsub_clients"))
         .orderBy("mne"))

# Cadence (median days between sends) - REMOVED from Pull B's SQL (v5, see Cell [1] header) and
# computed HERE in Spark instead, from Pull C (cards_pair - already client x mne grain). Pull C is
# CARDS-ONLY (12 mnes), so cadence exists for cards mnes only - non-cards mnes get a NULL cadence
# below, not a zero. The brief only asks cadence for cards campaigns, so this is not a narrowing.
cards_pair_for_cadence = read_cardspair()
_cadence_src = (cards_pair_for_cadence
                .filter(F.col("n_sends") >= 2)
                .withColumn("approx_gap_days",
                            F.datediff(F.col("last_send_dt"), F.col("first_send_dt")).cast("double") /
                            (F.col("n_sends") - 1)))
cadence_by_mne = (_cadence_src
                  .groupBy("mne")
                  .agg(F.expr("percentile_approx(approx_gap_days, 0.5)").alias("median_days_between_sends"),
                       F.count("*").alias("clients_used_for_gap")))
print("CADENCE - computed from Pull C (client x mne, CARDS-ONLY, 12 mnes), not Pull B - non-cards "
      "mnes get NULL median_days_between_sends below, not zero.")

q_mne = q_mne.join(cadence_by_mne, "mne", "left").withColumn("smoke_run", F.lit(1 if SMOKE else 0))

q_mne_pd = q_mne.toPandas()
print("Q_MNE - unsubs by campaign, bank-wide | grain: one row per mne | %d rows | SMOKE=%s" % (
      len(q_mne_pd), SMOKE))
print(q_mne_pd.to_string(index=False))


# %% [3] Q_TREND - mne x cohort_month, BANK-WIDE. Print .head(30) and land CSV to HDFS.
# SOURCE: Pull B (mne_agg), summed across bites - same reasoning as Cell [2]'s q_mne: bites
# partition clients disjointly by MOD(ABS(CLNT_NO), N_BITES), so a given (mne, cohort_month) can
# have up to N_BITES partial rows (one per bite) that must be SUMMED, not just selected - the old
# unbitten Pull B needed no groupBy here because it was already one row per (mne, cohort_month);
# that is no longer true post v5.
# ENGINE: PySpark (YARN).
#
# COHORT_MONTH HERE = the month a client FIRST appears as sent-to for that mne (month of
# first_send_dt) - an ENTRY-COHORT trend, not a per-calendar-month send-volume trend, computed
# server-side in Pull B's SQL (EXTRACT(YEAR/MONTH FROM first_send_dt)).

mne_agg = read_mneagg()

q_trend = (mne_agg
           .groupBy("mne", "is_cards", "is_regulatory", "cohort_month_num")
           .agg(F.sum("clients_mailed").alias("clients_entered"),
                F.sum("sends").alias("sends"),
                F.sum("unsub_clients").alias("unsub_clients"))
           .withColumn("cohort_month",
                       F.concat(F.substring(F.col("cohort_month_num").cast("string"), 1, 4),
                                F.lit("-"),
                                F.substring(F.col("cohort_month_num").cast("string"), 5, 2)))
           .select("mne", "is_cards", "is_regulatory", "cohort_month",
                   "clients_entered", "sends", "unsub_clients")
           .withColumn("smoke_run", F.lit(1 if SMOKE else 0))
           .orderBy("mne", "cohort_month")
           .cache())

q_trend_pd = q_trend.toPandas()
print("Q_TREND - mne x cohort_month (entry-cohort), bank-wide | grain: one row per mne x "
      "cohort_month | %d rows | SMOKE=%s" % (len(q_trend_pd), SMOKE))
print(q_trend_pd.head(30).to_string(index=False))

_trend_path = OUT_DIR + "q_trend"
q_trend.coalesce(1).write.mode("overwrite").option("header", True).csv(_trend_path)
print("written to HDFS:", _trend_path, "|", len(q_trend_pd), "rows")


# %% [4] UCP JOIN - the single most important guard in this file. Casts CLNT_NO through
# decimal(18,0)->long on BOTH sides (never str(float) - that renders scientific notation and
# silently zero-matches, see memory note pandas_float_keys_scientific_notation). Prints 5 sample
# ids per side BEFORE joining, then HARD-ERRORS if match rate < UCP_MATCH_FLOOR (70%, Cell [0]).
#
# Client universe for this join = clientagg (Pull A), bank-wide, ~15M clients - NOT a client x
# mne grain (that per-mne UCP shape test is gone post-redesign, see the file header; it still
# runs, cards-only, inside Cube 1's cell using Pull C).
#
# Snapshot: last CLOSED month-end (references/ucp/README.md:86-89, gotchas.md #1) - current month
# never has a partition. CLNT_TYP == 'Personal' filter applied per README.md:97 ("defensive
# filter, see gotchas.md #6"); gotchas.md:160-165 confirms CLNT_TYP is live on personal ucp4 as of
# the 2026-07-27 printSchema probe. Fields used: CLNT_NO, AGE, TENURE_RBC_YEARS, PROF_TOT_ANNUAL,
# T_TOT_CNT/I_TOT_CNT/B_TOT_CNT/C_TOT_CNT - all CONFIRMED live (references/ucp/field_catalog_personal.md
# RELIABILITY BANNER, lines 5-16).
# ENGINE: PySpark (YARN) reading HDFS parquet - not Trino, not Teradata.

import datetime as _dt

# autoBroadcastJoinThreshold already set once, in Cell [1] - not repeated here (duplicate set
# removed).

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

base_ids = (read_clientagg()
            .withColumn("clnt_no_long", F.col("clnt_no").cast("decimal(18,0)").cast("long"))
            .select("clnt_no_long").distinct())

# Sample BEFORE joining, so a format mismatch is visible immediately.
print("Sample clnt_no (clientagg, 5):", [r.clnt_no_long for r in base_ids.limit(5).collect()])
print("Sample CLNT_NO (UCP, 5):", [r.clnt_no_long for r in ucp_sel.select("clnt_no_long").limit(5).collect()])

_ucp_ids = ucp_sel.select("clnt_no_long").distinct()

_left_n = base_ids.count()
_matched_n = base_ids.join(_ucp_ids, "clnt_no_long", "inner").count()
_match_pct = 100.0 * _matched_n / _left_n if _left_n else 0.0
print("UCP JOIN MATCH RATE - clientagg clients:", _left_n, "| matched to UCP:", _matched_n,
      "| match pct: %.1f%%" % _match_pct)

# The guard catches a BROKEN KEY (format mismatch -> near-zero match), not ordinary attrition.
assert _match_pct >= UCP_MATCH_FLOOR, (
    "UCP join match rate %.1f%% (%d/%d) is below the %.0f%% floor - this looks like a broken key, "
    "not attrition. Check clnt_no normalization on both sides (decimal(18,0)->long) and the UCP "
    "snapshot date (%s)." % (_match_pct, _matched_n, _left_n, UCP_MATCH_FLOOR, _ucp_anchor)
)

# The rate itself is not the risk - BIAS in who fails to match is. This bias check only needs
# CLIENT-level unsub_flag (clientagg already carries it - no mne needed), so it still runs at
# full bank-wide coverage post-redesign.
_bucket_ids = (read_clientagg()
               .select("clnt_no", "unsub_flag")
               .withColumn("clnt_no_long", F.col("clnt_no").cast("decimal(18,0)").cast("long"))
               .withColumn("bucket", F.when(F.col("unsub_flag") == 1, "LEAVER").otherwise("STAYER")))
_bias = (_bucket_ids
         .join(_ucp_ids.withColumn("in_ucp", F.lit(1)), "clnt_no_long", "left")
         .groupBy("bucket")
         .agg(F.count("*").alias("clients"),
              F.sum(F.coalesce(F.col("in_ucp"), F.lit(0))).alias("matched")))
print("UCP MATCH BIAS CHECK - match rate by bucket (grain: one row per STAYER/LEAVER, bank-wide):")
_bias.withColumn("match_pct", F.round(100.0 * F.col("matched") / F.col("clients"), 1)).show(truncate=False)
print("If STAYER and LEAVER match rates differ by more than a few points, the profiling cuts "
      "are biased and the 'no_ucp_match' rows must be reported, not filtered out.")

# NOTE: the old bank-wide (250-mne) "business vs consumer shape test" per campaign is GONE -
# it needed client x mne rows for every mne, which no longer exist post-redesign (see file
# header, WIRE-COST REDESIGN, second consequence). A cards-only (12-mne) version of the per-mne
# UCP match check runs in Cell [5] using Pull C, which still has client x mne rows for cards.

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

# Left-join onto the FULL clientagg client universe (bank-wide, ~15M) now, so downstream cube
# cells just read this table and get "no_ucp_match" for anyone who fell out of the >=UCP_MATCH_FLOOR
# (70%) match - no repeated join logic downstream.
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
      "(full clientagg universe, bank-wide, UCP misses labeled no_ucp_match) |", _n_ucp, "rows")

# DEDUP GUARD - ucp_enriched_full is the right side of a left join in cells [4]/[5]/[6]/[8]. If
# the ucp4 snapshot holds duplicate CLNT_NO rows, EVERY one of those joins fans out silently.
# base_ids is already DISTINCT clnt_no_long (see its .distinct() above), so ucp_enriched_full - a
# left join of base_ids onto ucp_enriched - must have exactly one row per base_ids row.
assert _n_ucp == _left_n, (
    "ucp_enriched_full has %d rows but base_ids (the distinct client universe it was left-joined "
    "onto) has %d - the UCP snapshot is fanning out on a duplicate CLNT_NO. Every downstream cube "
    "join is unsafe until this is fixed." % (_n_ucp, _left_n))


# %% [5] CUBE 1 - profiling cube. EXACT spec, minus two constants (see below):
# Dims:    mne, age_band, tenure_band, prod_cat_cnt (bare integer 0-4 as string, or no_ucp_match)
# Measures: stayers, leavers, clients_total, mean_tenure_stayers/leavers,
#          median_prof_stayers/leavers (percentile_approx .5, NOT quintile)
#
# is_cards and is_regulatory are DROPPED as dims (v5) - Pull C is cards-filtered in SQL, so both
# were constant across every row of this cube: is_cards=1 for all 12 mnes, is_regulatory=0 for all
# 12 (none of CARDS_MNES are in REGULATORY_MNES - the two sets are disjoint). Carrying a constant
# as a groupBy dim is waste, not information; documented here instead of stored as a column.
#
# SOURCE: Pull C (cards_pair) + ucp_enriched. cube1_profiling is CARDS-ONLY (12 mnes) post-
# redesign - Pull C is the only pull with client x mne on one row, and Pull C is cards-only (see
# file header). cube1_allclients below restores full bank-wide coverage of the SAME demographic
# dims (age/tenure/product-depth), just without the mne breakdown, using Pull A instead.
#
# WIDE, not long. stayers and leavers are COLUMNS, not values of a "bucket" dimension - a cell
# with leavers but zero stayers still gets both columns, so a pivot never divides by the wrong
# denominator. Rate stays out of the file - Andre derives leavers/(stayers+leavers).
# ENGINE: PySpark (YARN).

cards_pair = read_cardspair()
ucp_enriched_full = spark.read.parquet(BASE + "ucp_enriched")

# ---- Cards-only (12 mnes) UCP match-by-mne check - the surviving half of the old 250-mne shape
# test (see Cell [4] note). Uses Pull C's client x mne rows, the only ones left with that grain.
_ucp_ids_for_match = ucp_enriched_full.filter(F.col("age_band") != "no_ucp_match").select("clnt_no").distinct().withColumn("in_ucp", F.lit(1))
_cardspair_ids = cards_pair.select("clnt_no", "mne").distinct()
_mne_match = (_cardspair_ids
              .join(_ucp_ids_for_match, "clnt_no", "left")
              .groupBy("mne")
              .agg(F.countDistinct("clnt_no").alias("clients"),
                   F.sum(F.coalesce(F.col("in_ucp"), F.lit(0))).alias("matched"))
              .withColumn("match_pct", F.round(100.0 * F.col("matched") / F.col("clients"), 1))
              .withColumn("smoke_run", F.lit(1 if SMOKE else 0))
              .orderBy("match_pct")
              .cache())  # cached: used by .show(), .write(), and .toPandas() below (3 actions).
print("UCP MATCH BY MNE - CARDS ONLY (12 mnes). This replaces the old bank-wide 250-mne shape "
      "test, which is no longer computable post wire-cost redesign (see file header) - only "
      "cards mnes still carry client x mne rows (Pull C).")
_mne_match.show(20, truncate=False)
_mne_match.coalesce(1).write.mode("overwrite").option("header", True).csv(OUT_DIR + "ucp_match_by_mne")
_mne_match_pd = _mne_match.toPandas()
print("Landed:", OUT_DIR + "ucp_match_by_mne", "| grain: one row per mne (cards-only) |", len(_mne_match_pd), "rows")

# ---- cube1_profiling: cards-only, per spec dims/measures ----
cube1_src = cards_pair.join(ucp_enriched_full, "clnt_no", "left")

_stay = F.col("unsub_flag") == 0
_leave = F.col("unsub_flag") == 1

cube1_profiling = (cube1_src
         .groupBy("mne", "age_band", "tenure_band", "prod_cat_cnt")
         .agg(F.sum(F.when(_stay, 1).otherwise(0)).alias("stayers"),
              F.sum(F.when(_leave, 1).otherwise(0)).alias("leavers"),
              F.count("*").alias("clients_total"),
              F.avg(F.when(_stay, F.col("tenure_years"))).alias("mean_tenure_stayers"),
              F.avg(F.when(_leave, F.col("tenure_years"))).alias("mean_tenure_leavers"),
              F.expr("percentile_approx(CASE WHEN unsub_flag = 0 THEN prof_tot_annual END, 0.5)")
               .alias("median_prof_stayers"),
              F.expr("percentile_approx(CASE WHEN unsub_flag = 1 THEN prof_tot_annual END, 0.5)")
               .alias("median_prof_leavers"))
         .withColumn("is_cards", F.lit(1))       # constant - Pull C is cards-filtered in SQL
         .withColumn("is_regulatory", F.lit(0))  # constant - CARDS_MNES and REGULATORY_MNES are disjoint
         .withColumn("smoke_run", F.lit(1 if SMOKE else 0))
         .orderBy("mne", "age_band", "tenure_band", "prod_cat_cnt")
         .cache())  # cached: used by .toPandas() then .write() below (2 actions on the same DAG).

cube1_profiling_pd = cube1_profiling.toPandas()
print("\nCUBE 1 PROFILING (CARDS-ONLY, 12 mnes; is_cards=1/is_regulatory=0 constant, see header) | "
      "grain: one row per (mne, age_band, tenure_band, prod_cat_cnt) | stayers/leavers are "
      "COLUMNS | %d rows" % len(cube1_profiling_pd))
print(cube1_profiling_pd.head(30).to_string(index=False))

_cube1_path = OUT_DIR + "cube1_profiling"
cube1_profiling.coalesce(1).write.mode("overwrite").option("header", True).csv(_cube1_path)
print("written to HDFS:", _cube1_path, "|", len(cube1_profiling_pd), "rows")

# ---- cube1_allclients: SAME demographic dims, FULL bank-wide coverage, no mne dimension -----
# Uses Pull A (clientagg) + ucp_enriched, every client bank-wide, not just cards. This is the
# compensating output for cube1_profiling's cards-only narrowing (see file header).
clientagg = read_clientagg()
cube1_src_all = clientagg.select("clnt_no", "unsub_flag").join(ucp_enriched_full, "clnt_no", "left")

cube1_allclients = (cube1_src_all
         .groupBy("age_band", "tenure_band", "prod_cat_cnt")
         .agg(F.sum(F.when(_stay, 1).otherwise(0)).alias("stayers"),
              F.sum(F.when(_leave, 1).otherwise(0)).alias("leavers"),
              F.count("*").alias("clients_total"),
              F.avg(F.when(_stay, F.col("tenure_years"))).alias("mean_tenure_stayers"),
              F.avg(F.when(_leave, F.col("tenure_years"))).alias("mean_tenure_leavers"),
              F.expr("percentile_approx(CASE WHEN unsub_flag = 0 THEN prof_tot_annual END, 0.5)")
               .alias("median_prof_stayers"),
              F.expr("percentile_approx(CASE WHEN unsub_flag = 1 THEN prof_tot_annual END, 0.5)")
               .alias("median_prof_leavers"))
         .withColumn("smoke_run", F.lit(1 if SMOKE else 0))
         .orderBy("age_band", "tenure_band", "prod_cat_cnt")
         .cache())  # cached: used by .toPandas() then .write() below (2 actions on the same DAG).

cube1_allclients_pd = cube1_allclients.toPandas()
print("\nCUBE 1 ALLCLIENTS (bank-wide, no mne dim) | grain: one row per (age_band, tenure_band, "
      "prod_cat_cnt) | stayers/leavers are COLUMNS | %d rows" % len(cube1_allclients_pd))
print(cube1_allclients_pd.to_string(index=False))

_cube1_all_path = OUT_DIR + "cube1_allclients"
cube1_allclients.coalesce(1).write.mode("overwrite").option("header", True).csv(_cube1_all_path)
print("written to HDFS:", _cube1_all_path, "|", len(cube1_allclients_pd), "rows")


# %% [6] CUBE 2 - contact-frequency cube. Andre: "why do I need pairs" - the real question is how
# many campaigns and how many emails a client got, and whether that differs between stayers and
# leavers.
# Dims:    n_branded_campaigns_bucket, n_emails_3m_bucket, n_emails_6m_bucket,
#          n_promo_emails_6m_bucket, n_regulatory_emails_6m_bucket, prod_cat_cnt
# Measures: stayers, leavers, clients_total
#
# SOURCE: Pull A (clientagg) + ucp_enriched, BANK-WIDE, all mnemonics. clientagg is ALREADY at
# client grain with n_campaigns_branded / n_emails_3m/6m / n_emails_promo_6m /
# n_emails_regulatory_6m computed server-side in Pull A - no client-side re-aggregation needed
# (the old design's client_roll groupBy is gone; clientagg IS the client roll-up).
# ENGINE: PySpark (YARN).

clientagg = read_clientagg()
ucp_enriched_full = spark.read.parquet(BASE + "ucp_enriched")

# _band() already defined in Cell [4] (duplicate definition removed here - same function, no
# reason for two copies).

client_roll = (clientagg
               .withColumn("n_branded_campaigns_bucket", _band(F.col("n_campaigns_branded"), BRANDED_CAMP_EDGES))
               .withColumn("n_emails_3m_bucket", _band(F.col("n_emails_3m"), EMAILS_3M_EDGES))
               .withColumn("n_emails_6m_bucket", _band(F.col("n_emails_6m"), EMAILS_6M_EDGES))
               .withColumn("n_promo_emails_6m_bucket", _band(F.col("n_emails_promo_6m"), PROMO_EMAILS_6M_EDGES))
               .withColumn("n_regulatory_emails_6m_bucket", _band(F.col("n_emails_regulatory_6m"), REG_EMAILS_6M_EDGES))
               .join(ucp_enriched_full.select("clnt_no", "prod_cat_cnt"), "clnt_no", "left")
               .withColumn("prod_cat_cnt", F.coalesce(F.col("prod_cat_cnt"), F.lit("no_ucp_match"))))
# NOTE: no "bucket" (STAYER/LEAVER) column here - it was never consumed downstream (cube2 groups
# by the *_bucket dims below, not by "bucket"); unconsumed column removed.

cube2 = (client_roll
         .groupBy("n_branded_campaigns_bucket", "n_emails_3m_bucket", "n_emails_6m_bucket",
                   "n_promo_emails_6m_bucket", "n_regulatory_emails_6m_bucket", "prod_cat_cnt")
         .agg(F.sum(F.when(F.col("unsub_flag") == 0, 1).otherwise(0)).alias("stayers"),
              F.sum(F.when(F.col("unsub_flag") == 1, 1).otherwise(0)).alias("leavers"),
              F.count("*").alias("clients_total"))
         .withColumn("smoke_run", F.lit(1 if SMOKE else 0))
         .orderBy("n_branded_campaigns_bucket", "n_emails_6m_bucket")
         .cache())  # cached: used by .toPandas() then .write() below (2 actions on the same DAG).

cube2_pd = cube2.toPandas()
print("CUBE 2 - contact frequency, bank-wide | grain: one row per (n_branded_campaigns_bucket, "
      "n_emails_3m_bucket, n_emails_6m_bucket, n_promo_emails_6m_bucket, "
      "n_regulatory_emails_6m_bucket, prod_cat_cnt) | stayers and leavers are COLUMNS "
      "| %d rows" % len(cube2_pd))
print(cube2_pd.head(30).to_string(index=False))

_cube2_path = OUT_DIR + "cube2_frequency"
cube2.coalesce(1).write.mode("overwrite").option("header", True).csv(_cube2_path)
print("written to HDFS:", _cube2_path, "|", len(cube2_pd), "rows")

# n_emails_all summary - stayer vs leaver full-window volume compare, straight off clientagg.
_s = F.col("unsub_flag") == 0
_l = F.col("unsub_flag") == 1
q_emails_all = (clientagg.agg(
    F.sum(F.when(_s, 1).otherwise(0)).alias("stayers"),
    F.sum(F.when(_l, 1).otherwise(0)).alias("leavers"),
    F.sum(F.when(_s, F.col("n_emails_all")).otherwise(0)).alias("total_emails_stayers"),
    F.sum(F.when(_l, F.col("n_emails_all")).otherwise(0)).alias("total_emails_leavers"),
    F.expr("percentile_approx(CASE WHEN unsub_flag = 0 THEN n_emails_all END, 0.5)")
     .alias("median_emails_stayers"),
    F.expr("percentile_approx(CASE WHEN unsub_flag = 1 THEN n_emails_all END, 0.5)")
     .alias("median_emails_leavers"))
    .withColumn("smoke_run", F.lit(1 if SMOKE else 0))
    .cache())  # cached: used by .toPandas() then .write() below (2 actions on the same DAG).
q_emails_all_pd = q_emails_all.toPandas()
print("\nn_emails_all summary, stayers vs leavers side by side, bank-wide | 1 row | %d rows" % len(q_emails_all_pd))
print(q_emails_all_pd.to_string(index=False))

_emails_all_path = OUT_DIR + "q_emails_all_summary"
q_emails_all.coalesce(1).write.mode("overwrite").option("header", True).csv(_emails_all_path)
print("written to HDFS:", _emails_all_path, "|", len(q_emails_all_pd), "rows")


# %% [7] ONE FILE TO DOWNLOAD - collect every cube into a single Excel workbook on the POD's
# local disk, so the manual HDFS -> shared-drive hop is one download instead of several.
#
# Why this works from the Spark kernel: df.write.csv("file:///...") writes on the EXECUTORS, which
# are other machines, so the file lands nowhere visible. toPandas() pulls to the DRIVER, which IS
# this pod. Every cube here is a few hundred rows, so collecting is safe.
#
# Nothing on HDFS is replaced - the parquet and CSV outputs above still stand. This is purely a
# delivery convenience: one .xlsx, one sheet per cube, already in the format the pivot needs.
# ENGINE: PySpark driver + pandas. No new EDW connection.

import os

# _smoke suffix when SMOKE is True - a full Run All under SMOKE must never be mistaken for a real
# one (see SPOOL/CORRECTNESS FIX header note). Every sheet below also carries a smoke_run column.
LOCAL_OUT = os.path.join(os.path.expanduser("~"), "spotlight_out_smoke" if SMOKE else "spotlight_out")
os.makedirs(LOCAL_OUT, exist_ok=True)

# Sheet names are capped at 31 chars by Excel; keep them short and stable.
_sheets = {
    "cube1_profiling": cube1_profiling_pd,
    "cube1_allclients": cube1_allclients_pd,
    "cube2_frequency": cube2_pd,
    "q_mne": q_mne_pd,
    "q_trend": q_trend_pd,
    "q_emails_all": q_emails_all_pd,
    "ucp_match_by_mne": _mne_match_pd,
}
_sheets = {k: v for k, v in _sheets.items() if v is not None and len(v) > 0}

_xlsx = os.path.join(LOCAL_OUT, "spotlight_cubes.xlsx")
try:
    with pd.ExcelWriter(_xlsx, engine="openpyxl") as _xl:
        for _name, _df in _sheets.items():
            _df.to_excel(_xl, sheet_name=_name[:31], index=False)
    print("WROTE", _xlsx, "|", os.path.getsize(_xlsx), "bytes")
    for _name, _df in _sheets.items():
        print("   sheet %-20s %6d rows x %d cols" % (_name[:31], len(_df), len(_df.columns)))
    print("\nDownload THIS ONE FILE from the pod, not the HDFS part-files.")
except Exception as e:
    # openpyxl missing or Excel write blocked - fall back to named CSVs in one folder, still a
    # single directory to grab rather than several HDFS part-file hunts.
    print("Excel write failed (%s: %s) - falling back to named CSVs." % (type(e).__name__, str(e)[:200]))
    for _name, _df in _sheets.items():
        _p = os.path.join(LOCAL_OUT, _name + ".csv")
        _df.to_csv(_p, index=False)
        print("   wrote", _p, "|", len(_df), "rows")

import subprocess as _sp
_zip = os.path.join(LOCAL_OUT, "spotlight_cubes.zip")
_sp.run("cd %s && rm -f spotlight_cubes.zip && "
        "zip -rq spotlight_cubes.zip spotlight_cubes.xlsx" % LOCAL_OUT, shell=True)
if os.path.exists(_zip):
    print("ONE DOWNLOAD:", _zip, "|", round(os.path.getsize(_zip) / 1048576.0, 1), "MB")
    print("xlsx = pivot in Excel.")

print("\nLocal output dir:", LOCAL_OUT)
print("If env_probe.py cell [P8] showed this path on overlay/tmpfs, it does NOT survive a pod "
      "restart - the HDFS copies above are the durable ones.")


# %% [8] CLIENT-LEVEL EXTRACT - the thing worth downloading
# clientagg_v5 IS already one row per CLIENT (Pull A's grain), so no groupBy is needed here - this
# cell mostly just joins UCP on and adds the cards_mnes / touched_by_cards fields, which need
# Pull C (the only client x mne source, cards-only).
#
# What it drops vs the pre-redesign version: n_emails_promo (full-window, non-6m) is not in Pull
# A's column spec and was NOT added (no unrequested columns) - only n_emails_promo_6m exists.
# Per-campaign detail for NON-cards mnes is not recoverable client-side any more (Pull A collapses
# mne away server-side) - Pull B (q_mne/q_trend) is where non-cards per-campaign detail lives.
# ENGINE: PySpark (YARN).

clientagg = read_clientagg()
cards_pair = read_cardspair()
ucp_enriched_full = spark.read.parquet(BASE + "ucp_enriched")

_cards_roll = (cards_pair.groupBy("clnt_no")
               .agg(F.max(F.lit(1)).alias("touched_by_cards"),
                    F.concat_ws("|", F.sort_array(F.collect_set("mne"))).alias("cards_mnes")))

client_extract = (clientagg
                  .join(_cards_roll, "clnt_no", "left")
                  .withColumn("touched_by_cards", F.coalesce(F.col("touched_by_cards"), F.lit(0)))
                  .withColumn("cards_mnes", F.coalesce(F.col("cards_mnes"), F.lit("")))
                  .join(ucp_enriched_full, "clnt_no", "left")
                  .withColumn("age_band", F.coalesce(F.col("age_band"), F.lit("no_ucp_match")))
                  .withColumn("tenure_band", F.coalesce(F.col("tenure_band"), F.lit("no_ucp_match")))
                  .withColumn("prod_cat_cnt", F.coalesce(F.col("prod_cat_cnt"), F.lit("no_ucp_match")))
                  .withColumn("smoke_run", F.lit(1 if SMOKE else 0)))

_ext_path = OUT_DIR + "client_extract"

if not WRITE_CLIENT_EXTRACT:
    print("Cell [8] SKIPPED - WRITE_CLIENT_EXTRACT is False.")
    print("The cubes are the deliverable. This file exists only for local ad-hoc re-cutting at")
    print("client grain; flip the switch in Cell [0] if you want it.")
else:
    client_extract.write.mode("overwrite").parquet(_ext_path)
    _ext = spark.read.parquet(_ext_path)
    print("CLIENT EXTRACT | grain: one row per clnt_no |", _ext.count(), "rows")
    print("landed:", _ext_path)
    print("columns:", _ext.columns)
    print(_ext.limit(5).toPandas().to_string(index=False))
    print("If SMOKE was True for Pulls A/B/C, this is 10%% of clients and every absolute count is")
    print("a tenth of reality (see smoke_run column) - rates are unaffected.")
    print("Pull it down with:  !hdfs dfs -get -f %s /home/jovyan/spotlight_out/" % _ext_path)


# %% [9] CLEANUP - drop the client-grain intermediates, keep only aggregates
# Nothing at client grain is meant to persist. clientagg_v5, cards_pair_v5 and ucp_enriched exist
# for two reasons and neither survives this cell:
#   1. Cross-system join buffer. Email events live in Teradata, UCP lives in HDFS parquet. Teradata
#      cannot see HDFS and the join cannot be pushed down, so the two only meet after both sides
#      land. There is no query-time alternative.
#   2. Crash resumability. The bites are what make a killed Pull A/B/C run resume instead of restart.
# mne_agg_v5 is NOT client-grain (it is already an mne x cohort_month aggregate with no client
# ids) and is left alone - there is nothing sensitive or heavy about it to clean up.
#
# Once the cubes are written both jobs are done and the client-grain copies are dead weight.
#
# NOT automatic. Controlled by DROP_CLIENT_GRAIN in Cell [0]. Deleting means any new cut needs
# a fresh Teradata pull - hours. Leave it False until the cubes are checked.

import subprocess

_targets = [CLIENTAGG_DIR.rstrip("/"), CARDSPAIR_DIR.rstrip("/"), BASE + "ucp_enriched"]

if not DROP_CLIENT_GRAIN:
    print("DROP_CLIENT_GRAIN is False - nothing deleted. Would remove:")
    for _t in _targets:
        print("   ", _t)
    print("\nCheck the cubes first. Once these are gone, re-cutting means re-pulling from Teradata.")
else:
    # Refuse to delete the join buffer before the thing it was built for exists. OUT_DIR (not a
    # hardcoded "out/") so this checks the SMOKE-suffixed path when SMOKE is True, matching what
    # the cube cells actually wrote.
    _required = [OUT_DIR + "cube1_profiling", OUT_DIR + "cube1_allclients",
                 OUT_DIR + "cube2_frequency", OUT_DIR + "q_trend"]
    _missing = []
    for _r in _required:
        try:
            spark.read.option("header", True).csv(_r).limit(1).collect()
        except Exception:
            _missing.append(_r)
    if _missing:
        raise RuntimeError("Refusing to delete client-grain data - these aggregates are missing or "
                           "unreadable: %s. Rerun the cube cells first." % _missing)

    print("Aggregates verified present. Deleting client-grain intermediates:")
    for _t in _targets:
        _rc = subprocess.run("hdfs dfs -rm -r -skipTrash %s" % _t,
                             shell=True, capture_output=True, text=True, timeout=600)
        print("  ", _t, "-> rc", _rc.returncode, (_rc.stdout or _rc.stderr).strip()[:200])
    print("\nDone. Only aggregates remain on HDFS. Nothing at client grain persists.")

print("\nWhat stays:", OUT_DIR + "*  (cube1_profiling, cube1_allclients, cube2, q_mne, q_trend, "
      "q_emails_all, ucp_match_by_mne)")
print(("mne_agg_v%d/ also stays (it is an aggregate, not client-grain) in case q_mne/q_trend need "
       "a re-cut without a fresh Teradata pull.") % SCHEMA_VERSION)
