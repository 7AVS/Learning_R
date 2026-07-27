# UNSUB VALUE / UCP v4 - not all unsubscriptions have the same value, AND a stayer's value should
# not be counted once per campaign that mailed them. 2026-07-26.
#
# WHAT CHANGED FROM v3 (Andre's direction, 2026-07-26): v3 built one spine at CLIENT x TREATMENT
# grain across the full 12-month bank-wide send base, so every V-table was a TRUE RATE (unsubbed
# sends / total sends). Andre's objection: a stayer hit by 6 cards campaigns in Q2 carries their
# PROF_TOT_ANNUAL into that spine 6 times - fine for a rate denominator, wrong for a value profile,
# where it silently overweights whoever gets mailed most. v4 fixes the STAYER side and narrows scope:
#   - stayers are now CARDS-ONLY, Q2-2026-ONLY (Apr/May/Jun), and collapsed to ONE ROW PER CLIENT
#     at their LAST deployment in the quarter - multi-campaign participation collapses to the most
#     recent, so value is counted once per client, never once per campaign.
#   - leavers get a matching cards-Q2 definition (trigger mne in CARDS_MNES, last unsub per client,
#     unsub_tm in Q2) so V2-V6 compare two client-grain populations built the same way.
#   - the full-12m, all-program, bank-wide view survives as its own thing (V1's WHERE feed +
#     unsub_profile_12m save) but is now explicitly a VOLUME/VALUE-MIX view, NOT a rate table - it
#     has no send denominator (the bank-wide send pull was never built at 12m grain - see below).
#   - INPUT CONTRACT CHANGE (Andre, mid-build, 2026-07-26): the send extract itself was re-cut.
#     v3's OPEN QUESTION about sends_12m being bank-wide vs cards-only is now moot - sends_12m was
#     RETIRED before it ever landed (git history 61c0519; a partial size-probe may have left orphan
#     files under sends_12m/ in HDFS, this script does not read them). Its replacement,
#     sends_cards_q2/, is CARDS-ONLY BY SERVER-SIDE CONSTRUCTION (SUBSTR(TREATMENT_ID,8,3) IN
#     CARDS_MNES applied in the extract SQL itself, cpc_reservoir_extract.py cell [18]-[21]) and
#     Q2-2026-ONLY (3 monthly subpaths). This means the cards-MNE filter this script applies below
#     is now a DEFENSIVE ASSERT, not the primary filter - see cell [5].
#
# HYPOTHESIS: unsub is not a uniform event, AND the reachable-cards value pool should be measured
# at client grain, not send grain. Feeds a MAX-2-SLIDE deck:
#   Slide 1 - WHERE we lose clients (by program/MNE, bank-wide, cards in perspective) and who's
#             losing the valuable ones. Volume + value mix, NOT a rate (no bank-wide send pull).
#   Slide 2 - WHO leaves vs who stays, cards Q2 only, client-grain concentration + door-closing.
#
# EXACTLY FOUR client variables drive every output: tenure, age, TIBC product counts,
# PROF_TOT_ANNUAL. No others. AGE_RNG / TENURE_RBC_RNG / PROF_SEG_CD are pulled defensively in
# the schema probe (cheap, already there) but no output below uses them - do not let them creep
# into a cut.
#
# Engine: Spark/YARN only. `spark` is pre-initialized in the kernel - no SparkSession.builder,
# no .stop(). NO Teradata, NO teradatasql, NO credentials, NO EDW pull - every input below is
# already landed to HDFS: unsub_base by cpc_reservoir_extract.py, sends_cards_q2/* by the same
# file's cells [18]-[22] (landed 2026-07-26, replaces the retired sends_12m design).
#
# Inputs (BASE = hdfs:///user/427966379/unsub_cpc/):
#   unsub_base/*             CLNT_NO, unsub_tm, TREATMENT_ID - ALL unsub events, bank-wide, FULL
#                             12-MONTH window (unchanged from v3) - "where do we lose clients"
#                             still needs the whole bank as the frame, and the full-12m leaver
#                             exclusion for stayers needs the whole bank's unsub history, not just
#                             cards or just Q2.
#   sends_cards_q2/m2026_04, m2026_05, m2026_06   CLNT_NO, TREATMENT_ID - disposition_cd=1 (sent),
#                             distinct client x treatment PER MONTH, CARDS MNEs ONLY (server-side
#                             filter, cpc_reservoir_extract.py cell [18]: SUBSTR(TREATMENT_ID,8,3)
#                             IN CARDS_MNES applied in the extract SQL). Exactly 3 monthly
#                             subpaths, Apr through Jun 2026. Replaces v3's 12-month bank-wide
#                             sends_12m/*, which never landed and was retired before use.
#   UCP personal parquet at /prod/sz/tsz/00172/data/ucp4/MONTH_END_DATE=<date>
#
# OUTPUT MODEL: TWO saved tables, both client grain, both small - cards_q2_clients (leavers +
# stayers, one row per client, role flag) and unsub_profile_12m (bank-wide leaver spine, one row
# per client). Every V-table result is ALSO a SMALL PRINTED LABELED TABLE (museum evidence style,
# see cpc_evidence_hdfs.py's E1..E12), numbered V1..V6, each stating its own window/grain in the
# printed header. Transcribe to RESULTS_CATALOG.md by hand after the run - these prints are the
# record.

# %% [0] Header (see module docstring above for the full brief - this cell just states OUT)
BASE = "hdfs:///user/427966379/unsub_cpc/"
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"
OUT = BASE + "unsub_value/"
print("v4 | two persisted outputs: OUT + 'cards_q2_clients' (leaver+stayer, client grain, cards")
print("     Q2 only) and OUT + 'unsub_profile_12m' (bank-wide leaver spine, client grain, full 12m)")
print("     everything else prints, nothing else saves")

# %% [1] Setup
import pandas as pd
from IPython.display import display, Markdown
from pyspark.sql import functions as F, Window

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.max_rows", 60)

# every join below is send-to-unsub, cohort-to-UCP, or cohort-to-itself for banding; none of
# these are small-broadcast-safe by default on this cluster (house convention, see 15_*.py / v2/v3)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


def land_df(name, df):
    """Write a Spark DataFrame to OUT+name, read it back, assert the count holds. Idempotent:
    overwrites every call. Called exactly TWICE in this script (cell [13]): cards_q2_clients
    (leaver+stayer, one row per client) and unsub_profile_12m (bank-wide leaver spine, one row
    per client). Both are client-grain and small - everything downstream reads these landings
    back rather than re-deriving from UCP."""
    n_before = df.count()
    df.write.mode("overwrite").parquet(OUT + name)
    n_after = spark.read.parquet(OUT + name).count()
    assert n_after == n_before, (
        name + " HDFS readback mismatch: wrote " + str(n_before) + " read back " + str(n_after))
    print(name, ": landed", n_before, "rows, readback confirms", n_after)


def norm_clnt(col):
    return F.regexp_replace(F.trim(col.cast("string")), "^0+", "")


def T(label, df):
    """Render a titled table AND return it. A bare name renders only as a cell's last expression;
    this always renders, and still hands back the object to export or plot.
    Timestamp columns are stringified first - pandas 2.x rejects Spark's unit-less datetime64.
    Ported verbatim from cpc_evidence_hdfs.py's T() (museum evidence-table convention) - this
    file's V1..V6 output is meant to read exactly like that file's E-tables."""
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


print("helpers defined | BASE =", BASE, "| UCP_BASE =", UCP_BASE, "| OUT =", OUT)

# %% [2] SCHEMA PROBE - hard gate, run first. Nothing downstream may assume a column exists.
# Unchanged from v3. Partition discovery via a distinct-partition read is fine at this table's
# size; if it ever times out, the HDFS-listing fallback is:
#   hadoop fs -ls /prod/sz/tsz/00172/data/ucp4/ | awk -F'MONTH_END_DATE=' '{print $2}'
_parts = (spark.read.option("basePath", UCP_BASE).parquet(UCP_BASE)
          .select("MONTH_END_DATE").distinct().toPandas())
_avail = sorted(_parts["MONTH_END_DATE"].astype(str).tolist())
assert len(_avail) > 0, "no UCP partitions visible under " + UCP_BASE + " - check the path before proceeding"
UCP_MIN, UCP_MAX = _avail[0], _avail[-1]
print("UCP partitions available:", len(_avail), "| min:", UCP_MIN, "| max:", UCP_MAX)
print("full sorted list:", _avail)

_latest = spark.read.parquet(UCP_BASE + "MONTH_END_DATE=" + UCP_MAX)
print("\nschema of latest partition (" + UCP_MAX + "):")
_latest.printSchema()

# Exactly the four output variables (AGE, TENURE_RBC_YEARS, T/I/B/C_TOT_CNT, PROF_TOT_ANNUAL)
# plus three defensive extras (AGE_RNG, TENURE_RBC_RNG, PROF_SEG_CD) that earlier scripts pulled
# and that cost nothing to probe for - none of the three feed any V-table below.
REQUESTED_COLS = ["CLNT_NO", "AGE", "AGE_RNG", "TENURE_RBC_YEARS", "TENURE_RBC_RNG",
                   "PROF_TOT_ANNUAL", "PROF_SEG_CD", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT"]

_actual = set(_latest.columns)
_present_tbl = pd.DataFrame(
    {"column": REQUESTED_COLS,
     "status": ["PRESENT" if c in _actual else "MISSING" for c in REQUESTED_COLS]})
T("PROBE1 - UCP requested column presence check (latest partition " + UCP_MAX + ")", _present_tbl)

UCP_COLS = [c for c in REQUESTED_COLS if c in _actual]
_dropped = [c for c in REQUESTED_COLS if c not in _actual]
print("\nUCP_COLS (usable, intersection with actual schema):", UCP_COLS)
if _dropped:
    print("DROPPED (requested but not in schema):", _dropped)
else:
    print("nothing dropped - all requested columns present")

# CLNT_TYP is NOT in the documented 53-field list but earlier cards scripts filter on it - probe
# for it explicitly rather than assuming either way.
HAS_CLNT_TYP = "CLNT_TYP" in _actual
print("\nHAS_CLNT_TYP =", HAS_CLNT_TYP,
      "(controls whether the UCP read loop in cell [10] applies the Personal-client filter)")

# CRITICAL MINIMUM - promoted from v1/v2, unchanged in v3/v4: PROF_TOT_ANNUAL is REQUIRED, not
# optional. This whole script exists to answer "which leavers were worth more" - without
# PROF_TOT_ANNUAL there is no value axis and the analysis is pointless. Fail loud here rather than
# silently degrading to a volume-only report several cells from now.
_critical = ["CLNT_NO", "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT",
             "C_TOT_CNT", "PROF_TOT_ANNUAL"]
_missing_critical = [c for c in _critical if c not in _actual]
assert not _missing_critical, (
    "CRITICAL UCP COLUMNS MISSING from schema: " + str(_missing_critical) + ". "
    "If PROF_TOT_ANNUAL is on this list: STOP. The entire premise of this script is 'not all "
    "unsubs are worth the same' - without a value field there is nothing to measure and every "
    "cell below is unwriteable. Check the UCP field catalog before touching anything else.")

# v4's anchor is per-TREATMENT (month before launch) for BOTH sides. Stayers' launches are all in
# Q2-2026 (Apr/May/Jun), so the latest anchor they can need is May-2026 (month before a June
# launch). Leavers' TRIGGER treatment can have launched any time in the 12-month unsub_base
# history, so their anchor floor is not newly introduced here - only the upper bound matters, and
# it is the same floor v3 used. This is a floor, not the full needed range - cell [7] re-checks the
# ACTUAL computed anchor months against [UCP_MIN, UCP_MAX] once launch dates are decoded, and flags
# any that got clamped.
assert UCP_MAX >= "2026-05-31", (
    "max available UCP partition (" + UCP_MAX + ") does not cover the latest anchor this window "
    "needs (2026-05-31, for June-2026 launches). Anchors past UCP_MAX will silently clamp to "
    "UCP_MAX in cell [7] - fix by re-running once fresher UCP partitions land, or accept the "
    "clamp and note it in the caveats.")

print("\nSCHEMA PROBE PASSED - UCP_COLS and HAS_CLNT_TYP are now fixed for the rest of this run.")

# %% [3] SEND BASE - cards Q2 only. INPUT CONTRACT CHANGE (Andre, 2026-07-26, mid-build): reads
# sends_cards_q2/, NOT sends_12m/* - the bank-wide 12-month send pull was retired before it ever
# landed (git history 61c0519; see cpc_reservoir_extract.py). sends_cards_q2/ is CARDS-ONLY BY
# SERVER-SIDE CONSTRUCTION (SUBSTR(TREATMENT_ID,8,3) IN CARDS_MNES applied in the extract SQL
# itself) and Q2-2026-ONLY: exactly 3 monthly subpaths, Apr/May/Jun 2026, disposition_cd=1 already
# applied upstream. DEFAULT/blank-stream sends are excluded UPSTREAM by the server-side filter -
# they could never be attributed to a cards MNE anyway, so "stayer" in this script means "client
# with an IDENTIFIABLE cards send in Q2", not "client with any send in Q2".
_SEND_MONTHS_Q2 = ["m2026_04", "m2026_05", "m2026_06"]
assert len(_SEND_MONTHS_Q2) == 3, "expected exactly 3 monthly subpaths (Apr/May/Jun 2026)"


def read_sends_cards_q2():
    frames = []
    counts = []
    for sub in _SEND_MONTHS_Q2:
        f = (spark.read.parquet(BASE + "sends_cards_q2/" + sub)
             .select("CLNT_NO", "TREATMENT_ID")
             .withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO"))))
        n = f.count()
        assert n > 0, ("sends_cards_q2/" + sub + " landed zero rows - a subpath failed to land "
                        "silently, fix before trusting any total in this run")
        counts.append({"month": sub, "client_treatment_rows": n})
        frames.append(f)
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out, counts


print("reading 3 monthly cards-Q2 send subpaths (Apr/May/Jun 2026)...")
_sends_raw, _send_month_counts = read_sends_cards_q2()
T("SEND1 - per-month send subpath row counts (sends_cards_q2/, Apr-Jun 2026, cards MNEs only)",
  pd.DataFrame(_send_month_counts))
_n_raw = _sends_raw.count()

# Dedupe AFTER the union - the same (CLNT_NO, TREATMENT_ID) pair can legitimately appear in two
# adjacent monthly files if a send straddles a month boundary. Within-month dedup is already the
# extract's job (grain stated as distinct client x treatment per month, DISTINCT applied in the
# extract SQL); this dedup only removes cross-month duplicates of the exact same pair.
sends_q2 = _sends_raw.dropDuplicates(["CLNT_NO", "TREATMENT_ID"])
_n_dedup = sends_q2.count()
T("SEND2 - cards-Q2 send base after cross-month dedup (client x TREATMENT_ID grain)",
  pd.DataFrame([{
      "rows_before_cross_month_dedup": _n_raw,
      "rows_after_dedup": _n_dedup,
      "cross_month_duplicate_pairs_removed": _n_raw - _n_dedup,
  }]))

assert _n_dedup > 0, "sends_q2 after dedup is zero - something is badly wrong upstream"
sends_q2.cache()

# %% [4] UNSUB BASE READ - bank-wide, ALL unsub events, FULL 12-month window. Unchanged from v3.
# This is the single source for: (a) the full-12m all-program leaver profile (V1 / unsub_profile_12m),
# (b) the cards-Q2 leaver definition, and (c) the "any unsub, any program, any time" exclusion set
# for stayers - so it is read once here and reused by every downstream cell that needs it.
_unsub_raw = (spark.read.parquet(BASE + "unsub_base/*")
              .select("CLNT_NO", "unsub_tm", "TREATMENT_ID")
              .withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO"))))
_n_unsub_raw = _unsub_raw.count()
_n_unsub_raw_clients = _unsub_raw.select("CLNT_NO").distinct().count()
assert _n_unsub_raw > 0, "unsub_base/* landed zero rows - check the path before proceeding"
T("UNSUB1 - unsub_base raw (bank-wide, full 12-month window)",
  pd.DataFrame([{"raw_event_rows": _n_unsub_raw, "distinct_clients": _n_unsub_raw_clients}]))
# NOTE: no pair-level dedup here (v3 deduped to one row per (CLNT_NO, TREATMENT_ID) because it
# joined this table against sends on that exact key). v4 never joins unsub_base to sends directly -
# every downstream use is a per-CLIENT window function (row_number over CLNT_NO ordered by
# unsub_tm/launch_dt), which collapses duplicate raw rows for the same client regardless of how
# many events they logged, so a pair-level dedup here would be redundant, not protective.

# %% [5] MNE/PROGRAM MAPPING - CARDS_MNES sourced from the repo's own MNE catalog, not invented
# here. Source 1: UNSUB_TRACKING_KNOWLEDGE.md section 4 "MNE tracking scope" - the ONLY table in
# the repo that groups MNEs by business line. Its "Cards" rows: PCQ, PCL, PCD, AUH, CLI, MVP, CRV.
# Source 2: email_active_mnes.md - a volume-ranked top-30 subset (not exhaustive), confirms PCQ,
# PCL, PCD as "Cards?" checked; does not contradict source 1, just doesn't cover the smaller ones.
#
# RULING (Andre, 2026-07-26, final - replaces v2's open question, carried into v3 and v4): CTU
# and O2P are NOT cards - they are programs that involve cards, reported inside async, but out of
# the cards package. They stay OUT of CARDS_MNES and remain visible under their own MNEs in the
# enterprise-wide view (they surface in V1's top-15 MNE table under program='OTHER_BANK' via the
# raw mne column - nothing is hidden, just not pre-labeled Cards).
#
# THIS LIST MUST STAY IN SYNC WITH cpc_reservoir_extract.py's copy (cell [18]) - that file applies
# the identical set SERVER-SIDE (SUBSTR(TREATMENT_ID,8,3) IN CARDS_MNES) to build sends_cards_q2/.
# If either copy changes, update both.
CARDS_MNES = frozenset({"PCQ", "PCL", "PCD", "AUH", "CLI", "MVP", "CRV"})

# DEFAULT stream (per UNSUB_TRACKING_KNOWLEDGE.md): TREATMENT_ID = 'DEFAULT' literally, or a blank
# MNE substring, both mean "mail outside campaign taxonomy" (service + broken-template + untagged
# marketing) - invisible to MNE-based suppression, a governance finding in its own right. Keep it
# labeled, never collapse it into OTHER_BANK. sends_cards_q2 cannot contain DEFAULT rows (the
# server-side filter only keeps cards MNEs) - this branch exists for unsub_base, which is
# bank-wide and DOES carry DEFAULT-stream unsub events.
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
         .otherwise(F.lit("OTHER_BANK")))
    return df


sends_q2 = add_mne_program(sends_q2)
unsub_base = add_mne_program(_unsub_raw)

# DEFENSIVE ASSERT (new in v4, replaces the primary-filter role CARDS_MNES played in v3): the
# cards-MNE filter is now applied SERVER-SIDE in the extract, not client-side here. This assert
# proves the two copies of CARDS_MNES (this file's cell [5] and cpc_reservoir_extract.py's cell
# [18]) have not drifted apart. A tiny tolerance (<=0.5%) is allowed for edge-case TREATMENT_IDs
# that decode oddly; anything above that means the lists disagree and must be reconciled before
# trusting the stayer pool.
_n_sendq2_total = sends_q2.count()
_offenders = sends_q2.filter(~F.col("mne").isin(list(CARDS_MNES))).cache()
_n_offenders = _offenders.count()
_pct_offenders = round(100.0 * _n_offenders / _n_sendq2_total, 3)
T("MNE0 - defensive check: sends_cards_q2 rows whose decoded MNE is NOT in CARDS_MNES "
  "(should be ~0 - the extract already filtered server-side; a non-zero count means pack 28's "
  "CARDS_MNES and cpc_reservoir_extract.py's copy have drifted apart)",
  pd.DataFrame([{"total_send_rows": _n_sendq2_total, "offending_rows": _n_offenders,
                 "pct_offending": _pct_offenders}]))
if _n_offenders:
    T("MNE0b - offending MNEs present (sample)",
      _offenders.groupBy("mne").agg(F.count("*").alias("rows")).orderBy(F.desc("rows")))
assert _pct_offenders <= 0.5, (
    "sends_cards_q2 has " + str(_pct_offenders) + "% of rows decoding to a non-cards MNE (need "
    "<=0.5%). The server-side filter in cpc_reservoir_extract.py and this file's CARDS_MNES have "
    "drifted apart - reconcile both copies before trusting the stayer pool built below.")
_offenders.unpersist()
print("\nMNE0 PASSED - sends_cards_q2's server-side cards filter and pack 28's CARDS_MNES agree "
      "within tolerance.")

T("MNE1 - unsub_base (bank-wide, full 12m) program totals, distinct clients per program",
  unsub_base.groupBy("program").agg(F.countDistinct("CLNT_NO").alias("distinct_clients_any_event"),
                                     F.count("*").alias("unsub_events"))
            .orderBy(F.desc("unsub_events")))

# %% [6] TREATMENT_ID DECODE - launch date. DOCUMENTED: MNE = SUBSTR(TREATMENT_ID, 8, 3)
# (UNSUB_TRACKING_KNOWLEDGE.md:145, and used identically across 02_campaign_unsub_tracker.py,
# 01_vendor_feedback_eda.sql, cpc_evidence.sql, cpc_reservoir_extract.py). The julian-date piece
# is NAMED but never given exact positions anywhere in the repo: "the ID encodes MNE + julian
# date" (UNSUB_TRACKING_KNOWLEDGE.md:163, 05_email_journey_by_mne_cohort.sql:14,
# 17_em_decision_vendor_coverage.sql:19) - no file states which characters carry it. WORKING
# ASSUMPTION (Andre's brief, unverified in repo): positions 1-7 = julian launch date YYYYDDD
# (4-digit year + 3-digit day-of-year). Validated empirically below, not trusted blind. Same
# decode used for BOTH sides now: sends_q2 (stayers' last-deployment launch) and unsub_base
# (leavers'/profile's TRIGGER treatment launch, for the UCP anchor).
# FALLBACK if validation fails: decode launch date from DTZV01.TACTIC_EVNT_IP_AR_H60M instead
# (has real deployment dates per TACTIC_ID) - that table is NOT in the reservoir and would need
# a fresh EDW pull, out of scope for this script.
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
        (F.col("_launch_dt_raw") >= F.lit("2024-01-01")) &
        (F.col("_launch_dt_raw") < F.lit("2027-01-01")))
    df = df.withColumn("launch_dt", F.when(F.col("_valid"), F.col("_launch_dt_raw")))
    return df.drop("_yr", "_doy", "_launch_dt_raw")


sends_q2 = add_launch_decode(sends_q2)
unsub_base = add_launch_decode(unsub_base)

# Validate over the UNION of distinct TREATMENT_IDs actually used by this script - sends_q2's and
# unsub_base's - not just one side, since both sides' launch_dt/cohort_month/ucp_anchor depend on
# this decode holding.
_decode_check = (sends_q2.select("TREATMENT_ID", "_valid")
                  .unionByName(unsub_base.select("TREATMENT_ID", "_valid"))
                  .dropDuplicates(["TREATMENT_ID"]))
_n_ids = _decode_check.count()
_n_valid = _decode_check.filter(F.col("_valid")).count()
_pct_valid = round(100.0 * _n_valid / _n_ids, 2)
T("DECODE1 - TREATMENT_ID launch-date decode validation (distinct TREATMENT_IDs across sends_q2 "
  "+ unsub_base, positions 1-7 = YYYYDDD)",
  pd.DataFrame([{"distinct_treatment_ids": _n_ids, "decoded_valid": _n_valid, "pct_valid": _pct_valid}]))

if _n_valid < _n_ids:
    _fail_reason = (
        F.when(F.trim(F.col("TREATMENT_ID")) == "DEFAULT", F.lit("DEFAULT literal"))
         .otherwise(F.lit("non-numeric prefix or decoded outside [2024-01-01, 2027-01-01)")))
    T("DECODE2 - decode failures by reason (distinct TREATMENT_IDs)",
      _decode_check.filter(~F.col("_valid")).withColumn("_fail_reason", _fail_reason)
                    .groupBy("_fail_reason").agg(F.count("*").alias("distinct_treatment_ids")))

assert _pct_valid >= 99.0, (
    "TREATMENT_ID launch-date decode only validated for " + str(_pct_valid) + "% of distinct "
    "TREATMENT_IDs (need >= 99%). The positions-1-7=YYYYDDD assumption is WRONG or needs "
    "adjustment - do not proceed with cohort_month/ucp_anchor built on this decode. See the "
    "fallback note above this cell (TACTIC_EVNT_IP_AR_H60M).")
print("\nDECODE VALIDATED >= 99% - launch_dt trusted for the rest of this run.")

sends_q2 = sends_q2.drop("_valid")
unsub_base = unsub_base.drop("_valid")

# %% [7] cohort_month (= launch month, NOT unsub month) + UCP anchor - ONE rule for everyone:
# last completed month-end strictly before the treatment's LAUNCH date. Applied identically to
# sends_q2 (stayers' candidate rows, pre-collapse) and unsub_base (leavers'/profile's candidate
# rows, pre-collapse) so every population downstream gets the same pre-treatment UCP snapshot rule.
def add_ucp_anchor(df):
    df = df.withColumn(
        "cohort_month",
        F.when(F.col("launch_dt").isNotNull(), F.date_format(F.col("launch_dt"), "yyyy-MM"))
         .otherwise(F.lit("UNKNOWN")))
    # last_day(add_months(d, -1)) always lands on the previous calendar month's last day, which is
    # always < d regardless of where in its own month d falls - a single closed-form expression,
    # no day-of-month edge case (same formula as v3 cell [6]).
    df = df.withColumn(
        "_ucp_anchor_raw",
        F.when(F.col("launch_dt").isNotNull(), F.last_day(F.add_months(F.col("launch_dt"), -1))))
    df = df.withColumn(
        "ucp_anchor",
        F.when(F.col("_ucp_anchor_raw").isNotNull(),
               F.greatest(F.least(F.col("_ucp_anchor_raw"), F.lit(UCP_MAX).cast("date")),
                          F.lit(UCP_MIN).cast("date"))))
    return df.drop("_ucp_anchor_raw")


sends_q2 = add_ucp_anchor(sends_q2)
unsub_base = add_ucp_anchor(unsub_base)
sends_q2.cache()
unsub_base.cache()

_n_clamped_sends = sends_q2.filter(
    F.col("launch_dt").isNotNull() &
    (F.last_day(F.add_months(F.col("launch_dt"), -1)) != F.col("ucp_anchor"))).count()
_n_clamped_unsub = unsub_base.filter(
    F.col("launch_dt").isNotNull() &
    (F.last_day(F.add_months(F.col("launch_dt"), -1)) != F.col("ucp_anchor"))).count()
T("COHORT1 - anchor clamp check (rows where the raw anchor formula got clamped to UCP_MIN/UCP_MAX "
  "- their UCP snapshot is NOT the true month-before-launch)",
  pd.DataFrame([{"population": "sends_q2 (stayer candidates)", "clamped_rows": _n_clamped_sends},
                {"population": "unsub_base (leaver/profile candidates)", "clamped_rows": _n_clamped_unsub}]))

# %% [8] BUILD LEAVERS (cards Q2) + FULL-12M PROFILE (all programs) - both from unsub_base.
# FULL-12M PROFILE: all programs, last unsub event per client, distinct-client grain. This is the
# WHERE/value-mix view for V1 - no send denominator (the bank-wide 12-month send pull was retired,
# see header), so V1 is volume + value mix only, never a rate.
_w_last_any = Window.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").desc(), F.col("TREATMENT_ID").desc())
profile = (unsub_base.withColumn("rn", F.row_number().over(_w_last_any)).filter("rn = 1").drop("rn"))
_n_profile = profile.count()
T("PROFILE1 - full-12m all-program leaver profile (bank-wide, distinct client, last unsub event)",
  pd.DataFrame([{"distinct_clients": _n_profile}]))

# LEAVERS (cards Q2, the comparison side for slide 2): trigger mne in CARDS_MNES FIRST, then last
# unsub per client AMONG THOSE filtered events, then keep only clients whose resulting last-cards-
# unsub falls inside the Q2 window. FLAG: Q2 + cards window chosen for symmetry with the stayer
# pool (Claude's interpretation of Andre's brief, not independently confirmed - widen if Andre
# wants leavers whose most recent CARDS unsub predates Q2 but who also unsubbed from something
# else inside Q2; that reading is NOT what is built here).
_cards_unsub_events = unsub_base.filter(F.col("mne").isin(list(CARDS_MNES)))
_w_last_cards = Window.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").desc(), F.col("TREATMENT_ID").desc())
_last_cards_unsub = (_cards_unsub_events.withColumn("rn", F.row_number().over(_w_last_cards))
                      .filter("rn = 1").drop("rn"))
_n_last_cards = _last_cards_unsub.count()

leavers = _last_cards_unsub.filter(
    (F.col("unsub_tm") >= F.lit("2026-04-01")) & (F.col("unsub_tm") < F.lit("2026-07-01")))
_n_leavers = leavers.count()
T("LEAVERS1 - cards-Q2 leaver build (trigger mne in CARDS_MNES -> last unsub per client among "
  "those -> filtered to unsub_tm in [2026-04-01, 2026-07-01))",
  pd.DataFrame([{"clients_with_a_cards_unsub_ever": _n_last_cards,
                "leavers_last_cards_unsub_in_q2_window": _n_leavers,
                "dropped_last_cards_unsub_outside_q2": _n_last_cards - _n_leavers}]))
assert _n_leavers > 0, "zero leavers after the cards-Q2 filter - check CARDS_MNES and the window bounds"

# EXCLUSION SET for stayers: distinct CLNT_NO present in unsub_base AT ALL - any program, any
# time in the full 12-month window. A Nov-2025 unsubscriber who still got cards mail via the
# proven leakage is not a stayer.
_unsub_any_clients = unsub_base.select("CLNT_NO").distinct()

# %% [9] BUILD STAYERS - cards Q2, one row per client at their LAST deployment. Multi-campaign
# participation collapses to the most recent (Andre's decision, 2026-07-26): a stayer hit by 6
# cards campaigns in Q2 carries their PROF_TOT_ANNUAL exactly once, not 6 times.
# mne is already restricted to cards MNEs by construction (server-side filter in the extract,
# reconfirmed by the MNE0 defensive assert in cell [5]) - no client-side CARDS_MNES filter is
# needed here, unlike the leaver side which reads from the bank-wide unsub_base.
_w_last_dep = Window.partitionBy("CLNT_NO").orderBy(F.col("launch_dt").desc(), F.col("TREATMENT_ID").desc())
_stayer_candidates = (sends_q2.withColumn("rn", F.row_number().over(_w_last_dep))
                       .filter("rn = 1").drop("rn"))
_n_stayer_candidates = _stayer_candidates.count()

stayers = _stayer_candidates.join(_unsub_any_clients, "CLNT_NO", "left_anti")
_n_stayers = stayers.count()
_n_excluded_prior_unsub = _n_stayer_candidates - _n_stayers
T("STAYERS1 - cards-Q2 stayer build (last deployment per client -> excluded if the client has ANY "
  "unsub event, any program, any time in the full 12-month unsub_base)",
  pd.DataFrame([{"distinct_clients_sent_q2_cards": _n_stayer_candidates,
                "excluded_for_prior_unsub_any_program": _n_excluded_prior_unsub,
                "stayers_after_exclusion": _n_stayers}]))
print("\nexclusion removed", _n_excluded_prior_unsub, "clients (",
      round(100.0 * _n_excluded_prior_unsub / _n_stayer_candidates, 1),
      "% of Q2 cards recipients) - these clients received cards mail in Q2 despite an unsub "
      "event somewhere in the bank in the trailing 12 months.")
assert _n_stayers > 0, "zero stayers after exclusion - check the unsub_base exclusion join"

# %% [10] UCP READ - one partition per distinct non-null ucp_anchor needed across ALL THREE
# populations (leavers, stayers, profile), deduped to (CLNT_NO, ucp_anchor) pairs before reading
# so a client needed by more than one population at the same anchor is only read once. Semi-join
# to the needed clients BEFORE selecting columns (same optimization as v3 cell [8]): the needed-
# client list per anchor month is a small fraction of the full UCP partition, so filtering rows
# first means the final union carries far fewer rows through the shuffle than projecting the
# whole partition first.
def read_ucp_for_requests(requests_df, cols):
    _rows = (requests_df.filter(F.col("ucp_anchor").isNotNull())
             .select("ucp_anchor").distinct().collect())
    anchor_months = sorted(str(r["ucp_anchor"]) for r in _rows)
    print("reading UCP for", len(anchor_months), "distinct anchor months:", anchor_months)
    frames = []
    read_summary = []
    for m in anchor_months:
        needed = requests_df.filter(F.col("ucp_anchor") == F.lit(m)).select("CLNT_NO").distinct()
        n_needed = needed.count()
        raw = spark.read.option("basePath", UCP_BASE).parquet(UCP_BASE + "MONTH_END_DATE=" + m)
        raw = raw.withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO")))
        if HAS_CLNT_TYP:
            raw = raw.filter(F.trim(F.col("CLNT_TYP")) == "Personal")
        raw = raw.join(needed, "CLNT_NO", "leftsemi")
        sel = raw.select(*cols).withColumn("ucp_month_end", F.lit(m))
        n = sel.count()
        read_summary.append({"ucp_anchor": m, "needed_distinct_clients": n_needed,
                              "matched_rows": n, "personal_filter_applied": HAS_CLNT_TYP})
        frames.append(sel)
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out, read_summary


_anchor_requests = (leavers.select("CLNT_NO", "ucp_anchor")
                     .unionByName(stayers.select("CLNT_NO", "ucp_anchor"))
                     .unionByName(profile.select("CLNT_NO", "ucp_anchor"))
                     .dropDuplicates(["CLNT_NO", "ucp_anchor"]))

ucp_spine, _ucp_read_summary = read_ucp_for_requests(_anchor_requests, UCP_COLS)
T("UCP1 - per-anchor-month UCP read summary (leavers + stayers + profile combined requests)",
  pd.DataFrame(_ucp_read_summary))
assert ucp_spine.count() > 0, "UCP anchor-month read returned zero rows - investigate before proceeding"

# %% [11] ATTACH UCP - one join + fan-out guard per population, same shared ucp_spine.
def attach_ucp(label, df, ucp_spine, cols):
    _before = df.count()
    _joined = df.join(
        ucp_spine, (df.CLNT_NO == ucp_spine.CLNT_NO) & (df.ucp_anchor == ucp_spine.ucp_month_end),
        how="left"
    ).select(df["*"], *[ucp_spine[c].alias(c) for c in cols if c != "CLNT_NO"], ucp_spine["ucp_month_end"])
    _after = _joined.count()
    # If this assert trips, UCP is not unique per (CLNT_NO, MONTH_END_DATE) - dedup fallback:
    # w = Window.partitionBy("CLNT_NO", "ucp_month_end").orderBy(F.lit(1))
    # ucp_spine = ucp_spine.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
    assert _before == _after, (
        "FAN-OUT on " + label + ": " + str(_before) + " rows -> " + str(_after) +
        " rows after UCP join. UCP is not unique per (CLNT_NO, MONTH_END_DATE) - apply the "
        "commented dedup fallback above and re-run this cell.")
    _joined = _joined.withColumn("ucp_matched", F.col("AGE").isNotNull())
    _matched = _joined.filter(F.col("ucp_matched")).count()
    T("MATCH - " + label + " UCP match rate",
      pd.DataFrame([{"clients": _before, "matched": _matched, "unmatched": _before - _matched,
                    "match_pct": round(100.0 * _matched / _before, 1)}]))
    return _joined


leavers = attach_ucp("leavers (cards Q2)", leavers, ucp_spine, UCP_COLS)
stayers = attach_ucp("stayers (cards Q2)", stayers, ucp_spine, UCP_COLS)
profile = attach_ucp("profile (full 12m, all programs)", profile, ucp_spine, UCP_COLS)

# %% [12] ASSEMBLE + SAVE (client grain, both small)
_UCP_ATTR_COLS = ["AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT",
                   "PROF_TOT_ANNUAL"]
_SAVE_COLS = ["CLNT_NO", "TREATMENT_ID", "mne", "program", "launch_dt", "cohort_month",
              "ucp_month_end", "unsub_tm"] + _UCP_ATTR_COLS + ["ucp_matched"]

cards_q2_clients = (
    leavers.withColumn("role", F.lit("leaver")).select(["role"] + _SAVE_COLS)
    .unionByName(stayers.withColumn("role", F.lit("stayer"))
                 .withColumn("unsub_tm", F.lit(None).cast("timestamp")).select(["role"] + _SAVE_COLS)))
print("cards_q2_clients row count before write:", cards_q2_clients.count(),
      "(one row per client, leaver + stayer, cards Q2 only)")
land_df("cards_q2_clients", cards_q2_clients)

unsub_profile_12m = profile.select(_SAVE_COLS)
print("unsub_profile_12m row count before write:", unsub_profile_12m.count(),
      "(one row per client, bank-wide, full 12-month leaver spine)")
land_df("unsub_profile_12m", unsub_profile_12m)

# %% [13] BANDING - one function, applied to the landed tables (re-read - cheap, this is the point
# of persisting them). PROF_CUTS computed on the CARDS Q2 POOL (stayers + leavers combined,
# matched clients only) - not the bank-wide profile - because the reachable-cards base is the
# reference population now: "valuable" means valuable relative to who cards campaigns can
# actually reach, and the cards Q2 pool is the only base built at that grain. The same cuts are
# then reused for V1's bank-wide profile too (see V1's printed label - it is explicit about
# borrowing cards-Q2 cut points, not deriving its own).
TENURE_EDGES = [2, 5, 10, 20]     # years; bins: 0-2, 3-5, 6-10, 11-20, 20+ - our choice, not documented
AGE_EDGES = [24, 34, 44, 54, 64]  # bins: <25, 25-34, 35-44, 45-54, 55-64, 65+ - our choice, not documented


def apply_bands(df, prof_cuts):
    """prof_cuts = 4 cut points (20/40/60/80th pct) computed ONCE on the cards-Q2 reachable pool
    (leavers + stayers, matched, cell [13] main body) and reused everywhere."""
    df = df.withColumn(
        "prod_cnt",
        F.coalesce(F.col("T_TOT_CNT"), F.lit(0)) + F.coalesce(F.col("I_TOT_CNT"), F.lit(0)) +
        F.coalesce(F.col("B_TOT_CNT"), F.lit(0)) + F.coalesce(F.col("C_TOT_CNT"), F.lit(0)))
    df = df.withColumn(
        "prod_band",
        F.when(F.col("prod_cnt") == 0, "0").when(F.col("prod_cnt") == 1, "1")
         .when(F.col("prod_cnt") == 2, "2").when(F.col("prod_cnt") <= 4, "3-4").otherwise("5+"))

    has_t = F.coalesce(F.col("T_TOT_CNT"), F.lit(0)) > 0
    has_i = F.coalesce(F.col("I_TOT_CNT"), F.lit(0)) > 0
    has_b = F.coalesce(F.col("B_TOT_CNT"), F.lit(0)) > 0
    has_c = F.coalesce(F.col("C_TOT_CNT"), F.lit(0)) > 0
    n_held = (has_t.cast("int") + has_i.cast("int") + has_b.cast("int") + has_c.cast("int"))
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
         .when(F.col("PROF_TOT_ANNUAL") <= prof_cuts[0], "1")
         .when(F.col("PROF_TOT_ANNUAL") <= prof_cuts[1], "2")
         .when(F.col("PROF_TOT_ANNUAL") <= prof_cuts[2], "3")
         .when(F.col("PROF_TOT_ANNUAL") <= prof_cuts[3], "4")
         .otherwise("5"))
    return df


cards_q2_read = spark.read.parquet(OUT + "cards_q2_clients")
profile_read = spark.read.parquet(OUT + "unsub_profile_12m")

_prof_cut_base = cards_q2_read.filter(F.col("ucp_matched") & F.col("PROF_TOT_ANNUAL").isNotNull())
PROF_CUTS = _prof_cut_base.approxQuantile("PROF_TOT_ANNUAL", [0.2, 0.4, 0.6, 0.8], 0.01)
T("PROF1 - PROF_TOT_ANNUAL quintile cut points (cards-Q2 pool, leavers + stayers combined, "
  "matched clients only, n = " + str(_prof_cut_base.count()) + ")",
  pd.DataFrame([{"p20": PROF_CUTS[0], "p40": PROF_CUTS[1], "p60": PROF_CUTS[2], "p80": PROF_CUTS[3]}]))

cards_q2_banded = apply_bands(cards_q2_read, PROF_CUTS)
cards_q2_banded.cache()
profile_banded = apply_bands(profile_read, PROF_CUTS)
profile_banded.cache()
print("\ncards_q2_banded (leaver+stayer, cards Q2, client grain):", cards_q2_banded.count(), "rows")
print("profile_banded (bank-wide, full 12m, client grain):", profile_banded.count(), "rows")

leavers_banded = cards_q2_banded.filter(F.col("role") == "leaver")
stayers_banded = cards_q2_banded.filter(F.col("role") == "stayer")
_n_leavers_total = leavers_banded.count()
_n_stayers_total = stayers_banded.count()
_n_pool_total = _n_leavers_total + _n_stayers_total

# %% [14] V1 - WHERE we lose clients (Slide 1 feed). GRAIN: distinct CLIENT, last unsub event,
# bank-wide, full 12-month window, ALL programs. NO RATE - there is no bank-wide send pull at
# this grain (see header), so this is volume + value mix only. Quintile cuts BORROWED from the
# cards-Q2 reachable pool (cell [13]) - the only base built at that grain - stated honestly in
# the label, not silently reused.
_profile_total = profile_banded.count()


def _v1_metrics(df, keys):
    total = df.groupBy(*keys).agg(F.count("*").alias("unsub_clients"))
    total = total.withColumn("share_of_all_leavers_pct", F.round(100.0 * F.col("unsub_clients") / _profile_total, 2))
    matched = (df.filter(F.col("ucp_matched")).groupBy(*keys)
               .agg(F.expr("percentile_approx(PROF_TOT_ANNUAL, 0.5)").alias("median_prof_annual"),
                    F.expr("percentile_approx(TENURE_RBC_YEARS, 0.5)").alias("median_tenure_years"),
                    F.round(100.0 * F.sum(F.when(F.col("prod_band") == "1", 1).otherwise(0)) / F.count("*"), 1)
                    .alias("pct_single_product"),
                    F.round(100.0 * F.sum(F.when(F.col("prof_quintile") == "5", 1).otherwise(0)) / F.count("*"), 1)
                    .alias("pct_top_prof_quintile")))
    return total.join(matched, list(keys), "left")


v1_program = _v1_metrics(profile_banded, ["program"]).orderBy(F.desc("unsub_clients"))
T("V1 - WHERE, by program | window: full 12-month unsub_base (bank-wide, ALL programs) | GRAIN: "
  "distinct client, last unsub event | VOLUME + VALUE MIX ONLY - no rate (no bank-wide send "
  "denominator, see header) | prof_quintile cuts BORROWED from the cards-Q2 reachable pool (cell "
  "[13]) - the only base we have at this grain", v1_program)

v1_mne = _v1_metrics(profile_banded, ["mne", "program"]).orderBy(F.desc("unsub_clients")).limit(15)
T("V1 - WHERE, top-15 MNEs by unsub-client volume | window: full 12-month unsub_base (bank-wide) "
  "| GRAIN: distinct client | quintile cuts from cards Q2 reachable pool | program flag attached, "
  "CARDS rows are the ones this deck is about", v1_mne)

# %% [15] V2 - WHO, four-variable mix, LEAVERS vs STAYERS (cards Q2, Slide 2 feed). Both sides are
# now client grain, built the same way (last deployment / last cards unsub, one row per client).
_v2_dims = ["prod_band", "tenure_band", "age_band", "prof_quintile"]
_v2_frames = []
for dim in _v2_dims:
    lb = (leavers_banded.groupBy(dim).count()
          .withColumn("pct_leavers", F.round(100.0 * F.col("count") / _n_leavers_total, 2)).drop("count"))
    sb = (stayers_banded.groupBy(dim).count()
          .withColumn("pct_stayers", F.round(100.0 * F.col("count") / _n_stayers_total, 2)).drop("count"))
    m = (lb.join(sb, dim, "outer")
         .withColumn("pct_leavers", F.coalesce(F.col("pct_leavers"), F.lit(0.0)))
         .withColumn("pct_stayers", F.coalesce(F.col("pct_stayers"), F.lit(0.0)))
         .withColumn("ratio", F.round(F.col("pct_leavers") / F.col("pct_stayers"), 2))
         .withColumn("segment_dim", F.lit(dim))
         .withColumnRenamed(dim, "segment_value")
         .select("segment_dim", "segment_value", "pct_stayers", "pct_leavers", "ratio"))
    _v2_frames.append(m)

v2 = _v2_frames[0]
for f in _v2_frames[1:]:
    v2 = v2.unionByName(f)
T("V2 - WHO, four-variable mix | GRAIN: distinct client, cards Q2 only | leavers (n = " +
  str(_n_leavers_total) + ") vs stayers (n = " + str(_n_stayers_total) + ") | ratio = pct_leavers "
  "/ pct_stayers; ratio > 1 means that segment is OVER-represented among leavers relative to stayers",
  v2.orderBy("segment_dim", F.desc("ratio")))

# %% [16] V3 - CONCENTRATION (the headline, cards Q2). Clean client-grain rate now: leaver share
# per prof_quintile = leavers / (leavers + stayers) IN that quintile - no send denominator needed,
# because the comparison population (stayers) already IS the reachable-but-didn't-leave set.
_v3_tbl = (cards_q2_banded.groupBy("prof_quintile")
           .agg(F.sum(F.when(F.col("role") == "leaver", 1).otherwise(0)).alias("leavers"),
                F.sum(F.when(F.col("role") == "stayer", 1).otherwise(0)).alias("stayers"))
           .withColumn("pool", F.col("leavers") + F.col("stayers"))
           .withColumn("leaver_share_pct", F.round(100.0 * F.col("leavers") / F.col("pool"), 2))
           .withColumn("share_of_all_leavers_pct", F.round(100.0 * F.col("leavers") / _n_leavers_total, 1))
           .withColumn("share_of_pool_pct", F.round(100.0 * F.col("pool") / _n_pool_total, 1))
           .orderBy("prof_quintile"))
_v3_pd = T("V3 - CONCENTRATION | GRAIN: distinct client, cards Q2 only | top quintile = "
           "prof_quintile '5' (cut points from cell [13], cards-Q2 reachable pool) | "
           "leaver_share_pct = leavers / (leavers+stayers) WITHIN that quintile | share_of_* "
           "columns compare that quintile's slice of all leavers vs its slice of the whole pool",
           _v3_tbl)

_top_row = _v3_pd[_v3_pd["prof_quintile"] == "5"]
_bottom_row = _v3_pd[_v3_pd["prof_quintile"] == "1"]
if len(_top_row) and len(_bottom_row):
    display(Markdown("**headline:** of reachable cards clients in the **top** value quintile, **" +
                      str(_top_row["leaver_share_pct"].iloc[0]) + "%** left in Q2, vs **" +
                      str(_bottom_row["leaver_share_pct"].iloc[0]) + "%** in the **bottom** quintile"))

# %% [17] V4 - DOOR-CLOSING. GRAIN: distinct CLIENT (cards-Q2 leavers only, already client grain -
# no further collapse needed) - a client who leaves while holding one product closes the email
# door on every OTHER product cards might have cross-sold them.
_single_overall = leavers_banded.filter(F.col("prod_band") == "1").count()
display(Markdown("**V4 - DOOR-CLOSING** · GRAIN: distinct cards-Q2 leaver CLIENTS · single-product "
                  "leavers as % of ALL cards-Q2 leavers: **" +
                  str(round(100.0 * _single_overall / _n_leavers_total, 1)) + "%** (" +
                  str(_single_overall) + " of " + str(_n_leavers_total) + ")"))

v4_by_mne = (leavers_banded.groupBy("mne")
             .agg(F.count("*").alias("leaver_clients"),
                  F.sum(F.when(F.col("prod_band") == "1", 1).otherwise(0)).alias("single_product_clients"))
             .withColumn("pct_single_product", F.round(100.0 * F.col("single_product_clients") / F.col("leaver_clients"), 1))
             .orderBy(F.desc("leaver_clients")))
T("V4 - DOOR-CLOSING, by mne (cards Q2 leavers)", v4_by_mne)

v4_tibc = (leavers_banded.filter(F.col("prod_band") == "1").groupBy("tibc_mix")
           .agg(F.count("*").alias("single_product_clients")).orderBy(F.desc("single_product_clients")))
T("V4 - DOOR-CLOSING, single-product leavers by category held (cards Q2)", v4_tibc)

# %% [18] V5 - STABILITY. V3's leaver-share-by-quintile split by cohort_month, restricted to
# 2026-04/05/06 launches only - unlike V3 (no month restriction, since leavers' trigger campaign
# can have launched any time in the 12-month unsub_base), V5 is specifically about whether the
# concentration pattern holds across the three Q2 LAUNCH months.
_q2_launch_months = ["2026-04", "2026-05", "2026-06"]
_v5_pool = cards_q2_banded.filter(F.col("cohort_month").isin(_q2_launch_months))
_n_v5_dropped = cards_q2_banded.count() - _v5_pool.count()
_v5_tbl = (_v5_pool.groupBy("cohort_month", "prof_quintile")
           .agg(F.sum(F.when(F.col("role") == "leaver", 1).otherwise(0)).alias("leavers"),
                F.sum(F.when(F.col("role") == "stayer", 1).otherwise(0)).alias("stayers"))
           .withColumn("pool", F.col("leavers") + F.col("stayers"))
           .withColumn("leaver_share_pct", F.round(100.0 * F.col("leavers") / F.col("pool"), 2))
           .orderBy("cohort_month", "prof_quintile"))
T("V5 - STABILITY | GRAIN: distinct client, cards Q2 | V3's leaver-share-by-quintile, split by "
  "cohort_month (= launch month of the client's trigger/last treatment) | restricted to the 3 "
  "Q2-2026 LAUNCH months (leavers can have a trigger treatment launched outside Q2 even though "
  "their unsub_tm is inside Q2 - " + str(_n_v5_dropped) + " cards-Q2-pool client rows excluded here "
  "for that reason, still counted in V3/V4)", _v5_tbl)

# %% [19] V6 - PROF_TOT_ANNUAL vetting. GRAIN: distinct client, cards-Q2 pool (leavers + stayers),
# matched rows only - investigation, not reporting. Checking whether the field behaves like
# current-year contribution (should RISE with tenure) or something else. If it rises with tenure:
# label it "annual profitability" on slides, NEVER "LTV" or "lifetime value", and flag that it
# will UNDERSTATE young/new clients relative to their future trajectory - a young high-potential
# leaver looks cheap here by construction.
PCTS = [0.10, 0.25, 0.50, 0.75, 0.90]
_matched_pool = cards_q2_banded.filter(F.col("ucp_matched"))
_v6_rows = []
for band in ["0-2", "3-5", "6-10", "11-20", "20+", "unknown"]:
    sub = _matched_pool.filter(F.col("tenure_band") == band)
    n = sub.count()
    if n == 0:
        continue
    q = sub.approxQuantile("PROF_TOT_ANNUAL", PCTS, 0.01)
    _v6_rows.append({"tenure_band": band, "n": n, "p10": q[0], "p25": q[1], "p50": q[2], "p75": q[3], "p90": q[4]})
T("V6 - PROF_TOT_ANNUAL VETTING | GRAIN: distinct client, cards-Q2 pool (leavers+stayers), matched "
  "rows only | percentiles of PROF_TOT_ANNUAL by tenure_band - rising with tenure supports "
  "'current-year contribution'; flat or non-monotonic means the definition needs confirming "
  "before it goes anywhere near a slide", pd.DataFrame(_v6_rows))

# %% [20] ONE-SCREEN SUMMARY
display(Markdown("## UNSUB VALUE / UCP v4 SUMMARY"))

_summary_rows = [
    ("Cards-Q2 send base, distinct clients (pre-exclusion)", _n_stayer_candidates),
    ("Stayers (cards Q2, excl. any bank-wide unsub in trailing 12m)", _n_stayers),
    ("Excluded from stayer pool for prior unsub (any program, any time)", _n_excluded_prior_unsub),
    ("Leavers (cards Q2, trigger mne cards, unsub_tm in window)", _n_leavers),
    ("Cards-Q2 pool (leavers + stayers)", _n_pool_total),
    ("Full-12m all-program leaver profile (bank-wide, V1 feed)", _n_profile),
]
T("SUMMARY - headline sizes", pd.DataFrame(_summary_rows, columns=["figure", "value"]))

T("SUMMARY - V1 top-3 programs by leaver volume (full 12m, bank-wide, no rate)",
  v1_program.orderBy(F.desc("unsub_clients")).limit(3))

if len(_top_row) and len(_bottom_row):
    display(Markdown("**V3 concentration:** top-quintile leaver share **" +
                      str(_top_row["leaver_share_pct"].iloc[0]) + "%** vs bottom-quintile **" +
                      str(_bottom_row["leaver_share_pct"].iloc[0]) + "%** (cards Q2, client grain)"))

display(Markdown("**V4 door-closing:** **" + str(round(100.0 * _single_overall / _n_leavers_total, 1)) +
                  "%** of cards-Q2 leaver clients hold exactly one product"))

print("\nCAVEATS:")
print("- LEAVER WINDOW SYMMETRY IS AN INTERPRETATION, NOT STATED IN THE BRIEF: leavers = trigger")
print("  mne in CARDS_MNES, last unsub per client among those, THEN filtered to unsub_tm in Q2 -")
print("  chosen to mirror the stayer pool's Q2 window; Andre can widen if he wants a different rule")
print("- mne/cohort attribution for leavers/profile is TRIGGER-based, not last-touch-overall: a")
print("  client's leaver row uses THEIR LAST CARDS unsub event, which may not be their most recent")
print("  unsub bank-wide if their latest unsub was on a non-cards program")
print("- launch_dt decode: positions 1-7 of TREATMENT_ID = YYYYDDD is Andre's WORKING ASSUMPTION,")
print("  not documented anywhere in the repo (only positions 8-10 = MNE is documented) - validated")
print("  empirically in cell [6] at", _pct_valid, "% of distinct TREATMENT_IDs (threshold: 99%)")
print("- V2-V6 are ALL cards-Q2, CLIENT-GRAIN comparisons (leavers vs stayers, one row per client")
print("  each) - this is the load-bearing change from v3, which was client-TREATMENT grain and")
print("  double/multi-counted stayers hit by more than one campaign")
print("- V1 has NO RATE - the bank-wide 12-month send pull (v3's sends_12m) was retired before it")
print("  ever landed (Andre, 2026-07-26, git history 61c0519); V1 is volume + value mix only,")
print("  revivable as a rate if the full-12m bank-wide pull is ever rebuilt")
print("- PROF_TOT_ANNUAL is UNVETTED as a value proxy - see V6; do not call it LTV on a slide")
print("- prof_quintile cut points come from the cards-Q2 reachable pool ONLY (cell [13]) - the")
print("  only base built at client grain; V1 borrows these cuts for its bank-wide, all-program")
print("  view rather than deriving its own (stated in V1's printed label)")
print("- CARDS_MNES excludes CTU and O2P per Andre's 2026-07-26 ruling (see cell [5]) - both are")
print("  visible under OTHER_BANK via the raw mne column in V1, not hidden")
print("- DEFAULT-stream unsubs appear in V1 only (bank-wide unsub_base carries them); they cannot")
print("  appear in V2-V6 because sends_cards_q2 is cards-MNE-only by server-side construction, so")
print("  a DEFAULT-stream client can never enter the stayer pool, and CARDS_MNES excludes DEFAULT")
print("  from the leaver trigger filter too")
print("- HAS_CLNT_TYP =", HAS_CLNT_TYP, "- if False, the Personal-client filter was NOT applied anywhere")
print("- UCP uniqueness per (CLNT_NO, MONTH_END_DATE) is asserted by the cell [11] fan-out guards, not")
print("  independently verified beyond that")
print("- rows where ucp_anchor got clamped to UCP_MIN/UCP_MAX (see cell [7]): sends_q2 side",
      _n_clamped_sends, ", unsub_base side", _n_clamped_unsub,
      "- their UCP snapshot is not the true month-before-launch")

# OPEN QUESTIONS (unverified, flag before this ships anywhere):
# - JULIAN LAYOUT: positions 1-7 = YYYYDDD is an assumption, not documented (see cell [6] header
#   comment for the full citation trail). Validated empirically at runtime; if the % ever drops
#   below 99%, do not trust cohort_month/ucp_anchor - fall back to TACTIC_EVNT_IP_AR_H60M.
# - PROF_TOT_ANNUAL definition: current-year contribution vs lifetime/projected value - V6 is the
#   vetting pass; if it doesn't rise cleanly with tenure, do not caption it "profitability" either
#   without a footnote.
# - UCP PARTITION AVAILABILITY: the schema-probe floor (cell [2]) only guarantees coverage through
#   2026-05-31; cell [7] flags (not blocks on) any anchor that got clamped for lack of a true
#   month-before-launch partition - re-check the clamp counts in the summary before trusting the
#   affected rows.
# - LEAVER WINDOW SYMMETRY: Claude's interpretation (see cell [8] and the caveats above), not
#   independently confirmed with Andre - the Q2 unsub_tm filter on top of "last cards unsub" is a
#   design choice made to mirror the stayer pool, not something the brief stated explicitly.
# - FULL-12M PER-PROGRAM RATES: dropped along with the bank-wide 12-month send denominator (v3's
#   sends_12m, retired before landing, git history 61c0519) - V1 is volume+mix only now. Revivable
#   if the full-12m bank-wide send pull is ever rebuilt; until then do not imply a rate exists.
# - CARDS_MNES vs CTU/O2P: resolved 2026-07-26 (see cell [5]) - no longer open.
# - CARDS_MNES SYNC: this file's copy must stay identical to cpc_reservoir_extract.py's copy (cell
#   [18]) - the MNE0 defensive assert in cell [5] catches drift at runtime but does not prevent it;
#   if MNE0 ever fails, reconcile both copies before re-running.
# - CLNT_TYP presence: probed at runtime (cell [2]); if absent, HAS_CLNT_TYP=False and NO
#   client-type filter is applied anywhere downstream - the cohort may include non-Personal clients.
# - ucp4 uniqueness per (CLNT_NO, MONTH_END_DATE): asserted via the fan-out guards in cell [11]; if
#   one trips, the commented dedup fallback in attach_ucp() needs to be uncommented and re-run.
# - tibc_mix / prod_band / tenure_band / age_band cut points are this script's own choice, not
#   documented anywhere else in the repo - editable at the top of cell [13] (TENURE_EDGES, AGE_EDGES).
# - prof_quintile cut points are derived from the cards-Q2 reachable pool (cell [13]) - re-running
#   this script over a different quarter will shift the cut points; that is intentional (quintiles
#   are always relative to who cards can currently reach), but it means V3's numbers are NOT
#   comparable run-to-run without re-stating the cut points.

# %% [21] SYNTAX CHECK - parse this file's own source to catch any indentation/syntax error before
# handing it off; cheap, deterministic, and catches the class of mistake a partial copy-paste
# rewrite is most likely to introduce.
import ast

with open(__file__ if "__file__" in dir() else "28_unsub_value_ucp.py", "r", encoding="utf-8") as _f:
    _source = _f.read()
ast.parse(_source)
print("ast.parse PASSED - script is syntactically valid.")
