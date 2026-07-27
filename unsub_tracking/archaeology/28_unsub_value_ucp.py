# UNSUB VALUE / UCP v3 - not all unsubscriptions have the same value. Some leavers cost more
# than others. 2026-07-26.
#
# WHAT CHANGED FROM v2 (Andre's direction, 2026-07-26): v2 compared Q2-only unsubs to a Q2
# emailed base at one arbitrary COMMON snapshot (RATE_ANCHOR) - a spine built from the UNSUB
# side, profiled against a separately-built BASE side, joined only by a shared prof_quintile cut.
# v3 rebuilds the spine and denominator from scratch:
#   - denominator = the vendor-feedback SEND base, FULL 12-month window (Jul2025-Jun2026), at
#     CLIENT x TREATMENT grain - not a distinct-client snapshot.
#   - attribution = TREATMENT-COHORT, not last-unsub-event. The cohort IS the TREATMENT_ID: a
#     client's outcome on a given send attributes to THAT treatment's launch date, not to
#     whichever treatment happened to carry their most recent unsub click bank-wide.
#   - the numerator (unsub yes/no) is now a column ON the send grain (exact-key join), not a
#     separately-built cohort compared against a base - so V2/V3 are TRUE RATES with no
#     circularity, for the whole window, not just Q2.
#   - UCP anchor is ONE rule for everyone: last completed month-end before that treatment's
#     LAUNCH date. v2's PROFILE_ANCHOR/RATE_ANCHOR duality is gone entirely - there is no
#     separate "common snapshot" anymore because the denominator no longer needs one.
#
# HYPOTHESIS: unsub is not a uniform event. Feeds a MAX-2-SLIDE deck:
#   Slide 1 - WHERE we lose clients (by program/MNE, bank-wide, cards in perspective) and who's
#             losing the valuable ones.
#   Slide 2 - WHO leaves vs the sent base + concentration + door-closing.
#
# EXACTLY FOUR client variables drive every output: tenure, age, TIBC product counts,
# PROF_TOT_ANNUAL. No others. AGE_RNG / TENURE_RBC_RNG / PROF_SEG_CD are pulled defensively in
# the schema probe (cheap, already there) but no output below uses them - do not let them creep
# into a cut.
#
# Engine: Spark/YARN only. `spark` is pre-initialized in the kernel - no SparkSession.builder,
# no .stop(). NO Teradata, NO teradatasql, NO credentials, NO EDW pull - every input below is
# already landed to HDFS: unsub_base by cpc_reservoir_extract.py, sends_12m/* by the companion
# extract (pack 29, not in this repo yet as of 2026-07-26 - if sends_12m/* is missing, that
# extract has not landed and this script cannot run past cell [3]).
#
# Inputs (BASE = hdfs:///user/427966379/unsub_cpc/):
#   unsub_base/*        CLNT_NO, unsub_tm, TREATMENT_ID - ALL unsub events, bank-wide (NOT
#                        filtered to cards - "where do we lose clients" needs the whole bank as
#                        the frame, cards is a slice of that, not the universe)
#   sends_12m/m2025_07 .. sends_12m/m2026_06   CLNT_NO, TREATMENT_ID - disposition_cd=1 (sent),
#                        distinct client x treatment PER MONTH. 12 monthly subpaths, Jul2025
#                        through Jun2026. OPEN QUESTION (see bottom): whether this is bank-wide
#                        (all campaigns, matching v2's q2_recipients convention) or cards-only -
#                        not stated in the brief; treated as bank-wide here for consistency with
#                        every other "the sent base" definition in this repo. Flag if wrong.
#   UCP personal parquet at /prod/sz/tsz/00172/data/ucp4/MONTH_END_DATE=<date>
#
# OUTPUT MODEL - unchanged from v2: NO big saved tables beyond the one spine, NO CSV. Every
# result is a SMALL PRINTED LABELED TABLE (museum evidence style, see cpc_evidence_hdfs.py's
# E1..E12), numbered V1..V6, each stating its own window/numerator/denominator/GRAIN in the
# printed header. Transcribe to RESULTS_CATALOG.md by hand after the run - these prints are the
# record. The ONLY persisted artifact is cohort_spine (cell [9]) - it is client x TREATMENT x
# (effectively) month, so it is the "big one": UCP reads are expensive, every re-cut after that
# is a cheap groupBy over a landed parquet.

# %% [0] Header (see module docstring above for the full brief - this cell just states OUT)
BASE = "hdfs:///user/427966379/unsub_cpc/"
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"
OUT = BASE + "unsub_value/"
print("v3 | single persisted output: OUT + 'cohort_spine' (client x TREATMENT_ID grain, full")
print("     12-month window) | everything else prints, nothing else saves")

# %% [1] Setup
import pandas as pd
from IPython.display import display, Markdown
from pyspark.sql import functions as F, Window

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.max_rows", 60)

# every join below is send-to-unsub, cohort-to-UCP, or cohort-to-itself for banding; none of
# these are small-broadcast-safe by default on this cluster (house convention, see 15_*.py / v2)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


def land_df(name, df):
    """Write a Spark DataFrame to OUT+name, read it back, assert the count holds. Idempotent:
    overwrites every call. Called exactly ONCE in this script (cell [9], the spine) - everything
    downstream reads that landing back rather than re-deriving from UCP."""
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
# Partition discovery via a distinct-partition read is fine at this table's size; if it ever
# times out, the HDFS-listing fallback is:
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
      "(controls whether the UCP read loop in cell [8] applies the Personal-client filter)")

# CRITICAL MINIMUM - promoted from v1/v2: PROF_TOT_ANNUAL is now REQUIRED, not optional. This
# whole script exists to answer "which leavers were worth more" - without PROF_TOT_ANNUAL there
# is no value axis and the analysis is pointless. Fail loud here rather than silently degrading
# to a volume-only report several cells from now.
_critical = ["CLNT_NO", "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT",
             "C_TOT_CNT", "PROF_TOT_ANNUAL"]
_missing_critical = [c for c in _critical if c not in _actual]
assert not _missing_critical, (
    "CRITICAL UCP COLUMNS MISSING from schema: " + str(_missing_critical) + ". "
    "If PROF_TOT_ANNUAL is on this list: STOP. The entire premise of this script is 'not all "
    "unsubs are worth the same' - without a value field there is nothing to measure and every "
    "cell below is unwriteable. Check the UCP field catalog before touching anything else.")

# v3's anchor is per-TREATMENT (month before launch), not a single common snapshot. The latest
# possible anchor in the 12-month window is for a June-2026 launch -> last day of May 2026. This
# is a floor, not the full needed range - cell [6] re-checks the ACTUAL computed anchor months
# against [UCP_MIN, UCP_MAX] once launch dates are decoded, and flags any that got clamped.
assert UCP_MAX >= "2026-05-31", (
    "max available UCP partition (" + UCP_MAX + ") does not cover the latest anchor this window "
    "needs (2026-05-31, for June-2026 launches). Anchors past UCP_MAX will silently clamp to "
    "UCP_MAX in cell [6] - fix by re-running once fresher UCP partitions land, or accept the "
    "clamp and note it in the caveats.")

print("\nSCHEMA PROBE PASSED - UCP_COLS and HAS_CLNT_TYP are now fixed for the rest of this run.")

# %% [3] SEND BASE - the new denominator. 12 monthly subpaths, client x TREATMENT_ID grain,
# disposition_cd=1 already applied upstream by the reservoir extract. Read each subpath
# individually and assert non-empty (same discipline as v2's m04/m05/m06 reads) - a silently
# empty subpath would understate the denominator for that whole cohort month.
_SEND_MONTHS = ["m2025_07", "m2025_08", "m2025_09", "m2025_10", "m2025_11", "m2025_12",
                "m2026_01", "m2026_02", "m2026_03", "m2026_04", "m2026_05", "m2026_06"]
assert len(_SEND_MONTHS) == 12, "expected exactly 12 monthly subpaths (Jul2025-Jun2026)"


def read_sends_12m():
    frames = []
    counts = []
    for sub in _SEND_MONTHS:
        f = (spark.read.parquet(BASE + "sends_12m/" + sub)
             .select("CLNT_NO", "TREATMENT_ID")
             .withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO"))))
        n = f.count()
        assert n > 0, ("sends_12m/" + sub + " landed zero rows - a subpath failed to land "
                        "silently, fix before trusting any total in this run")
        counts.append({"month": sub, "client_treatment_rows": n})
        frames.append(f)
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out, counts


print("reading 12 monthly send subpaths (Jul2025-Jun2026)...")
_sends_raw, _send_month_counts = read_sends_12m()
T("SEND1 - per-month send subpath row counts (sends_12m/, Jul2025-Jun2026)",
  pd.DataFrame(_send_month_counts))
_n_raw = _sends_raw.count()

# Dedupe AFTER the union - the same (CLNT_NO, TREATMENT_ID) pair can legitimately appear in two
# adjacent monthly files if a send straddles a month boundary (per Andre's brief). Within-month
# dedup is already the extract's job (grain stated as distinct client x treatment per month);
# this dedup only removes cross-month duplicates of the exact same pair.
sends = _sends_raw.dropDuplicates(["CLNT_NO", "TREATMENT_ID"])
_n_dedup = sends.count()
T("SEND2 - send base after cross-month dedup (client x TREATMENT_ID grain, full 12-month window)",
  pd.DataFrame([{
      "rows_before_cross_month_dedup": _n_raw,
      "rows_after_dedup": _n_dedup,
      "cross_month_duplicate_pairs_removed": _n_raw - _n_dedup,
  }]))

assert _n_dedup > 0, "sends after dedup is zero - something is badly wrong upstream"
sends.cache()

# %% [4] Program mapping - CARDS_MNES sourced from the repo's own MNE catalog, not invented here.
# Source 1: UNSUB_TRACKING_KNOWLEDGE.md section 4 "MNE tracking scope" - the ONLY table in the
#   repo that groups MNEs by business line. Its "Cards" rows: PCQ, PCL, PCD, AUH, CLI, MVP, CRV.
# Source 2: email_active_mnes.md - a volume-ranked top-30 subset (not exhaustive), confirms PCQ,
#   PCL, PCD as "Cards?" checked; does not contradict source 1, just doesn't cover the smaller ones.
#
# RULING (Andre, 2026-07-26, final - replaces v2's open question): CTU and O2P are NOT cards -
# they are programs that involve cards, reported inside async, but out of the cards package.
# They stay OUT of CARDS_MNES and remain visible under their own MNEs in the enterprise-wide
# view (they surface in V1's top-15 MNE table under program='OTHER_BANK' via the raw mne column
# - nothing is hidden, just not pre-labeled Cards).
CARDS_MNES = frozenset({"PCQ", "PCL", "PCD", "AUH", "CLI", "MVP", "CRV"})

# DEFAULT stream (per UNSUB_TRACKING_KNOWLEDGE.md): TREATMENT_ID = 'DEFAULT' literally, or a blank
# MNE substring, both mean "mail outside campaign taxonomy" (service + broken-template + untagged
# marketing) - invisible to MNE-based suppression, a governance finding in its own right. Keep it
# labeled, never collapse it into OTHER_BANK.
sends = sends.withColumn(
    "mne",
    F.when((F.trim(F.col("TREATMENT_ID")) == "DEFAULT") |
           (F.trim(F.substring(F.col("TREATMENT_ID"), 8, 3)) == ""), F.lit("DEFAULT"))
     .otherwise(F.trim(F.substring(F.col("TREATMENT_ID"), 8, 3))))

sends = sends.withColumn(
    "program",
    F.when(F.col("mne") == "DEFAULT", F.lit("DEFAULT"))
     .when(F.col("mne").isin(list(CARDS_MNES)), F.lit("CARDS"))
     .otherwise(F.lit("OTHER_BANK")))

_mne_top20 = (sends.groupBy("mne", "program").agg(F.countDistinct("TREATMENT_ID").alias("treatments"),
                                                    F.count("*").alias("client_treatment_sends"))
              .orderBy(F.desc("client_treatment_sends")).limit(20))
T("MNE1 - top-20 MNEs by client-treatment sends, program label attached", _mne_top20)

T("MNE2 - program totals (bank-wide, full window)",
  sends.groupBy("program").agg(F.countDistinct("TREATMENT_ID").alias("treatments"),
                                F.count("*").alias("client_treatment_sends"))
       .orderBy(F.desc("client_treatment_sends")))

# %% [5] TREATMENT_ID DECODE - launch date. DOCUMENTED: MNE = SUBSTR(TREATMENT_ID, 8, 3)
# (UNSUB_TRACKING_KNOWLEDGE.md:145, and used identically across 02_campaign_unsub_tracker.py,
# 01_vendor_feedback_eda.sql, cpc_evidence.sql, cpc_reservoir_extract.py). The julian-date piece
# is NAMED but never given exact positions anywhere in the repo: "the ID encodes MNE + julian
# date" (UNSUB_TRACKING_KNOWLEDGE.md:163, 05_email_journey_by_mne_cohort.sql:14,
# 17_em_decision_vendor_coverage.sql:19) - no file states which characters carry it. WORKING
# ASSUMPTION (Andre's brief, unverified in repo): positions 1-7 = julian launch date YYYYDDD
# (4-digit year + 3-digit day-of-year). Validated empirically below, not trusted blind.
# FALLBACK if validation fails: decode launch date from DTZV01.TACTIC_EVNT_IP_AR_H60M instead
# (has real deployment dates per TACTIC_ID) - that table is NOT in the reservoir and would need
# a fresh EDW pull, out of scope for this script.
cohort = sends.withColumn("_yr", F.substring(F.col("TREATMENT_ID"), 1, 4).cast("int"))
cohort = cohort.withColumn("_doy", F.substring(F.col("TREATMENT_ID"), 5, 3).cast("int"))
cohort = cohort.withColumn(
    "_launch_dt_raw",
    F.when(F.col("_yr").isNotNull() & F.col("_doy").between(1, 366),
           F.expr("date_add(to_date(concat(_yr, '-01-01')), _doy - 1)")))
cohort = cohort.withColumn(
    "_valid",
    F.col("_launch_dt_raw").isNotNull() &
    (F.col("_launch_dt_raw") >= F.lit("2024-01-01")) &
    (F.col("_launch_dt_raw") < F.lit("2027-01-01")))
cohort = cohort.withColumn(
    "_fail_reason",
    F.when(F.col("_valid"), F.lit(None))
     .when(F.trim(F.col("TREATMENT_ID")) == "DEFAULT", F.lit("DEFAULT literal"))
     .when(F.col("_yr").isNull() | F.col("_doy").isNull(), F.lit("non-numeric prefix"))
     .otherwise(F.lit("decoded but outside [2024-01-01, 2027-01-01)")))

_decode_check = cohort.select("TREATMENT_ID", "_valid", "_fail_reason").dropDuplicates(["TREATMENT_ID"])
_n_ids = _decode_check.count()
_n_valid = _decode_check.filter(F.col("_valid")).count()
_pct_valid = round(100.0 * _n_valid / _n_ids, 2)
T("DECODE1 - TREATMENT_ID launch-date decode validation (distinct TREATMENT_IDs, positions 1-7 = YYYYDDD)",
  pd.DataFrame([{"distinct_treatment_ids": _n_ids, "decoded_valid": _n_valid, "pct_valid": _pct_valid}]))

if _n_valid < _n_ids:
    T("DECODE2 - decode failures by reason (distinct TREATMENT_IDs)",
      _decode_check.filter(~F.col("_valid")).groupBy("_fail_reason")
                    .agg(F.count("*").alias("distinct_treatment_ids")))

assert _pct_valid >= 99.0, (
    "TREATMENT_ID launch-date decode only validated for " + str(_pct_valid) + "% of distinct "
    "TREATMENT_IDs (need >= 99%). The positions-1-7=YYYYDDD assumption is WRONG or needs "
    "adjustment - do not proceed with cohort_month/ucp_anchor built on this decode. See the "
    "fallback note above this cell (TACTIC_EVNT_IP_AR_H60M).")
print("\nDECODE VALIDATED >= 99% - launch_dt trusted for the rest of this run.")

cohort = cohort.withColumn("launch_dt", F.when(F.col("_valid"), F.col("_launch_dt_raw")))
cohort = cohort.drop("_yr", "_doy", "_launch_dt_raw", "_valid", "_fail_reason")

# %% [6] cohort_month (= launch month, NOT unsub month) + UCP anchor - ONE rule for everyone:
# last completed month-end strictly before the treatment's LAUNCH date. Leavers and stayers in
# the same cohort get the same snapshot; every UCP attribute is pre-treatment by construction.
# This replaces v2's PROFILE_ANCHOR/RATE_ANCHOR duality entirely.
cohort = cohort.withColumn(
    "cohort_month",
    F.when(F.col("launch_dt").isNotNull(), F.date_format(F.col("launch_dt"), "yyyy-MM"))
     .otherwise(F.lit("UNKNOWN")))

# last_day(add_months(d, -1)) always lands on the previous calendar month's last day, which is
# always < d regardless of where in its own month d falls - a single closed-form expression, no
# day-of-month edge case (same formula as v2 cell [5], now keyed to launch_dt not unsub_tm).
cohort = cohort.withColumn(
    "_ucp_anchor_raw",
    F.when(F.col("launch_dt").isNotNull(), F.last_day(F.add_months(F.col("launch_dt"), -1))))
cohort = cohort.withColumn(
    "ucp_anchor",
    F.when(F.col("_ucp_anchor_raw").isNotNull(),
           F.greatest(F.least(F.col("_ucp_anchor_raw"), F.lit(UCP_MAX).cast("date")),
                      F.lit(UCP_MIN).cast("date"))))
cohort = cohort.drop("_ucp_anchor_raw")
cohort.cache()

_cohort_month_dist = (cohort.groupBy("cohort_month")
                       .agg(F.countDistinct("TREATMENT_ID").alias("treatments"),
                            F.count("*").alias("client_treatment_sends"))
                       .orderBy("cohort_month"))
T("COHORT1 - cohort_month distribution (launch month, decoded from TREATMENT_ID)", _cohort_month_dist)

_anchor_dist = (cohort.filter(F.col("ucp_anchor").isNotNull())
                 .groupBy("ucp_anchor").agg(F.count("*").alias("client_treatment_sends"))
                 .orderBy("ucp_anchor"))
T("COHORT2 - UCP anchor distribution (month before launch, clamped to [" + UCP_MIN + ", " + UCP_MAX + "])",
  _anchor_dist)

# Flag (not hard-assert - the clamp already keeps this from crashing) any anchor month that got
# clamped away from what the raw formula wanted, i.e. a launch close enough to UCP_MIN/UCP_MAX
# that the true "month before launch" partition doesn't exist.
_n_clamped = cohort.filter(
    F.col("launch_dt").isNotNull() &
    (F.last_day(F.add_months(F.col("launch_dt"), -1)) != F.col("ucp_anchor"))).count()
print("\nrows where the raw anchor formula got clamped to UCP_MIN/UCP_MAX:", _n_clamped,
      "(their UCP snapshot is NOT the true month-before-launch - small distortion if >0, note in caveats)")

# %% [7] UNSUB NUMERATOR - unsub_base (bank-wide, ALL unsub events) joined to the send base on
# EXACT keys (TREATMENT_ID, CLNT_NO). No time-window join conditions, ever - TACTIC_ID/TREATMENT_ID
# is unique per deployment and (TREATMENT_ID, CLNT_NO) is unique, so the exact key alone pins the
# wave (reference_tactic_id_unique_per_deployment.md; UNSUB_TRACKING_KNOWLEDGE.md:163).
_unsub_raw = (spark.read.parquet(BASE + "unsub_base/*")
              .withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO"))))
_n_unsub_raw = _unsub_raw.count()

# Defensive dedup to one row per (CLNT_NO, TREATMENT_ID) - uniqueness is asserted for SENDS, not
# independently verified for unsub EVENT rows; take the earliest unsub_tm per pair if duplicates
# exist (should not, but the fan-out guard below is the real proof, not this dedup).
_w_pair = Window.partitionBy("CLNT_NO", "TREATMENT_ID").orderBy(F.col("unsub_tm").asc())
unsub_events = (_unsub_raw.withColumn("rn", F.row_number().over(_w_pair)).filter("rn = 1")
                 .select("CLNT_NO", "TREATMENT_ID", F.col("unsub_tm").alias("unsub_tm")))
_n_unsub_pairs = unsub_events.count()
print("UNSUB EVENTS (bank-wide unsub_base/*): raw rows", _n_unsub_raw,
      "| distinct (CLNT_NO, TREATMENT_ID) pairs", _n_unsub_pairs,
      "| collapsed as duplicates:", _n_unsub_raw - _n_unsub_pairs)

_before_numerator = cohort.count()
cohort = cohort.join(unsub_events, ["CLNT_NO", "TREATMENT_ID"], "left")
_after_numerator = cohort.count()
assert _before_numerator == _after_numerator, (
    "FAN-OUT on unsub numerator join: " + str(_before_numerator) + " -> " + str(_after_numerator) +
    " - (CLNT_NO, TREATMENT_ID) is not unique on one side of this join. Check the dedup above "
    "before trusting any unsub_flag/unsub_tm downstream.")
cohort = cohort.withColumn("unsub_flag", F.when(F.col("unsub_tm").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
cohort.cache()

_n_unsub_matched = cohort.filter(F.col("unsub_flag") == 1).count()
T("NUM1 - numerator attached to send base (exact-key join, no time window)",
  pd.DataFrame([{
      "client_treatment_sends": _before_numerator, "unsubbed": _n_unsub_matched,
      "unsub_rate_per_1000_sends": round(1000.0 * _n_unsub_matched / _before_numerator, 2),
  }]))

# %% [8] UCP READ - one partition per distinct non-null ucp_anchor (~12-13 months of launches ->
# ~12-13 partitions, per Andre's estimate). Semi-join to the clients needing that anchor BEFORE
# selecting columns: the needed-client list per anchor month is a small fraction of the full UCP
# partition (only clients whose launch fell in that anchor's prior month), so filtering rows
# first means select()/the final union carry far fewer rows through the shuffle than projecting
# the whole partition first. This is the same tradeoff v2 didn't have to make (it read the whole
# partition every time) - worth it here because the client list, not the column list, is what's
# expensive at this grain.
def read_ucp_for_cohort(cohort_df, cols):
    _rows = (cohort_df.filter(F.col("ucp_anchor").isNotNull())
             .select("ucp_anchor").distinct().collect())
    anchor_months = sorted(str(r["ucp_anchor"]) for r in _rows)
    print("reading UCP for", len(anchor_months), "distinct anchor months:", anchor_months)
    frames = []
    read_summary = []
    for m in anchor_months:
        needed = cohort_df.filter(F.col("ucp_anchor") == F.lit(m)).select("CLNT_NO").distinct()
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


ucp_spine, _ucp_read_summary = read_ucp_for_cohort(cohort, UCP_COLS)
T("UCP1 - per-anchor-month UCP read summary", pd.DataFrame(_ucp_read_summary))
assert ucp_spine.count() > 0, "UCP anchor-month read returned zero rows - investigate before proceeding"

# %% [9] JOIN + fan-out guard + THE ONLY SAVE IN THIS SCRIPT
# Client-level UCP attributes join on (CLNT_NO, ucp_anchor == ucp_month_end) - anchor varies per
# treatment, so this is NOT a plain CLNT_NO join; a client with treatments launched in different
# months legitimately gets different UCP snapshots on different rows.
_before_join = cohort.count()
_joined = cohort.join(
    ucp_spine, (cohort.CLNT_NO == ucp_spine.CLNT_NO) & (cohort.ucp_anchor == ucp_spine.ucp_month_end),
    how="left"
).select(cohort["*"], *[ucp_spine[c].alias(c) for c in UCP_COLS if c != "CLNT_NO"], ucp_spine["ucp_month_end"])
_after_join = _joined.count()

# If this assert trips, UCP is not unique per (CLNT_NO, MONTH_END_DATE) - dedup fallback:
# w = Window.partitionBy("CLNT_NO", "ucp_month_end").orderBy(F.lit(1))
# ucp_spine = ucp_spine.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
assert _before_join == _after_join, (
    "FAN-OUT: cohort " + str(_before_join) + " rows -> joined " + str(_after_join) +
    " rows after UCP join. UCP is not unique per (CLNT_NO, MONTH_END_DATE) - apply the commented "
    "dedup fallback above and re-run this cell.")
print("fan-out guard OK: cohort rows preserved through UCP join (", _before_join, "==", _after_join, ")")

_joined = _joined.withColumn("ucp_matched", F.col("AGE").isNotNull())
_matched = _joined.filter(F.col("ucp_matched")).count()
T("MATCH1 - UCP match rate (spine, client x treatment grain)",
  pd.DataFrame([{
      "client_treatment_sends": _before_join, "matched": _matched, "unmatched": _before_join - _matched,
      "match_pct": round(100.0 * _matched / _before_join, 1),
  }]))

# BIG TABLE WARNING: this is client x treatment x (effectively) month - materially larger than
# v2's client-grain spine. Persisting is still preferred over recomputing UCP on every re-cut
# below, but repartition first if the write stalls or produces too many tiny files
# (e.g. .repartition(200) before .write below) - left as a comment, not applied, since the
# right partition count depends on the actual row count this run produces.
_spine_out = _joined.select(
    "CLNT_NO", "TREATMENT_ID", "mne", "program", "launch_dt", "cohort_month", "ucp_month_end",
    "unsub_flag", "unsub_tm",
    "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT",
    "PROF_TOT_ANNUAL", "ucp_matched")
print("cohort_spine row count before write:", _spine_out.count(),
      "(client x treatment x month - the big one; repartition before write if it stalls)")
land_df("cohort_spine", _spine_out)

# %% [10] BANDING - one function, applied to the landed spine (re-read - cheap, this is the
# point of persisting it). PROF_CUTS computed on the full SEND base, distinct CLIENTS, each at
# their EARLIEST anchor month (avoids double-weighting clients who received many treatments).
TENURE_EDGES = [2, 5, 10, 20]     # years; bins: 0-2, 3-5, 6-10, 11-20, 20+ - our choice, not documented
AGE_EDGES = [24, 34, 44, 54, 64]  # bins: <25, 25-34, 35-44, 45-54, 55-64, 65+ - our choice, not documented


def apply_bands(df, prof_cuts):
    """prof_cuts = 4 cut points (20/40/60/80th pct) computed ONCE on the earliest-anchor
    distinct-client send base and reused everywhere - see cell [10] main body for why."""
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


spine = spark.read.parquet(OUT + "cohort_spine")

# "valuable" means valuable RELATIVE TO WHO WE CAN REACH - cut points computed on distinct
# clients at their EARLIEST anchor month only, so a client sent 40 treatments doesn't get
# weighted 40x into the quantile computation. Cutting quintiles on the unsub subset itself would
# also be circular (unsubs would always split 20/20/20/20/20 by construction); this avoids both.
_w_earliest = Window.partitionBy("CLNT_NO").orderBy(F.col("ucp_month_end").asc())
_earliest_client_rows = (spine.filter(F.col("ucp_matched") & F.col("PROF_TOT_ANNUAL").isNotNull())
                          .withColumn("rn", F.row_number().over(_w_earliest)).filter("rn = 1"))
PROF_CUTS = _earliest_client_rows.approxQuantile("PROF_TOT_ANNUAL", [0.2, 0.4, 0.6, 0.8], 0.01)
T("PROF1 - PROF_TOT_ANNUAL quintile cut points (earliest-anchor distinct clients, n = " +
  str(_earliest_client_rows.count()) + ")",
  pd.DataFrame([{"p20": PROF_CUTS[0], "p40": PROF_CUTS[1], "p60": PROF_CUTS[2], "p80": PROF_CUTS[3]}]))

spine_banded = apply_bands(spine, PROF_CUTS)
spine_banded.cache()
print("\nspine_banded (full window, bank-wide, client x treatment grain):", spine_banded.count(), "rows")

# %% [11] V1 - WHERE we lose clients (Slide 1 feed). Grain: client x treatment sends.
_total_sends = spine_banded.count()
_total_unsubs = spine_banded.filter(F.col("unsub_flag") == 1).count()


def _v1_metrics(df, keys):
    total = (df.groupBy(*keys)
             .agg(F.countDistinct("TREATMENT_ID").alias("cohorts_distinct_treatments"),
                  F.count("*").alias("client_treatment_sends"),
                  F.sum("unsub_flag").alias("unsubs")))
    total = total.withColumn(
        "unsub_rate_per_1000_sends", F.round(1000.0 * F.col("unsubs") / F.col("client_treatment_sends"), 2))
    unsub_only = (df.filter(F.col("unsub_flag") == 1).groupBy(*keys)
                  .agg(F.expr("percentile_approx(PROF_TOT_ANNUAL, 0.5)").alias("median_prof_annual_of_unsubbed"),
                       F.round(100.0 * F.sum(F.when(F.col("prod_band") == "1", 1).otherwise(0)) / F.count("*"), 1)
                       .alias("pct_single_product_of_unsubbed"),
                       F.round(100.0 * F.sum(F.when(F.col("prof_quintile") == "5", 1).otherwise(0)) / F.count("*"), 1)
                       .alias("pct_top_quintile_of_unsubbed")))
    return total.join(unsub_only, list(keys), "left")


v1_program = _v1_metrics(spine_banded, ["program"]).orderBy(F.desc("unsubs"))
T("V1 - WHERE, by program | window: full 12-month send window (bank-wide) | GRAIN: client-treatment "
  "sends | unsub_rate_per_1000 is a TRUE rate (unsubbed sends / total sends) | "
  "median_prof/pct_single_product/pct_top_quintile computed on the UNSUBBED subset only (matched "
  "clients)", v1_program)

v1_mne = _v1_metrics(spine_banded, ["mne", "program"]).orderBy(F.desc("unsubs")).limit(15)
T("V1 - WHERE, top-15 MNEs by unsub volume | window: full 12-month send window (bank-wide) | "
  "GRAIN: client-treatment sends | program flag attached, CARDS rows are the ones this deck is about",
  v1_mne)

# %% [12] V2 - WHO, four-variable mix, unsubbed client-treatments vs ALL client-treatment sends
# (Slide 2 feed). Both sides come from the SAME spine now - no separate base to reconcile.
_v2_dims = ["prod_band", "tenure_band", "age_band", "prof_quintile"]
_v2_frames = []
_unsub_rows = spine_banded.filter(F.col("unsub_flag") == 1)
for dim in _v2_dims:
    ub = (_unsub_rows.groupBy(dim).count()
          .withColumn("pct_unsubs", F.round(100.0 * F.col("count") / _total_unsubs, 2)).drop("count"))
    ab = (spine_banded.groupBy(dim).count()
          .withColumn("pct_all_sends", F.round(100.0 * F.col("count") / _total_sends, 2)).drop("count"))
    m = (ub.join(ab, dim, "outer")
         .withColumn("pct_unsubs", F.coalesce(F.col("pct_unsubs"), F.lit(0.0)))
         .withColumn("pct_all_sends", F.coalesce(F.col("pct_all_sends"), F.lit(0.0)))
         .withColumn("ratio", F.round(F.col("pct_unsubs") / F.col("pct_all_sends"), 2))
         .withColumn("segment_dim", F.lit(dim))
         .withColumnRenamed(dim, "segment_value")
         .select("segment_dim", "segment_value", "pct_all_sends", "pct_unsubs", "ratio"))
    _v2_frames.append(m)

v2 = _v2_frames[0]
for f in _v2_frames[1:]:
    v2 = v2.unionByName(f)
T("V2 - WHO, four-variable mix | GRAIN: client-treatment | full 12-month window | unsubbed sends "
  "(n = " + str(_total_unsubs) + ") vs ALL sends (n = " + str(_total_sends) + ") | ratio = "
  "pct_unsubs / pct_all_sends; ratio > 1 means that segment is OVER-represented among unsubs",
  v2.orderBy("segment_dim", F.desc("ratio")))

# %% [13] V3 - CONCENTRATION (the headline). TRUE rate per 1,000 sends by prof_quintile - no
# circularity: the denominator is real sends at that quintile, not a separately-built base.
_v3_tbl = (spine_banded.groupBy("prof_quintile")
           .agg(F.count("*").alias("sends"), F.sum("unsub_flag").alias("unsubs"))
           .withColumn("unsub_rate_per_1000_sends", F.round(1000.0 * F.col("unsubs") / F.col("sends"), 2))
           .withColumn("pct_of_all_sends", F.round(100.0 * F.col("sends") / _total_sends, 1))
           .withColumn("pct_of_all_unsubs", F.round(100.0 * F.col("unsubs") / _total_unsubs, 1))
           .orderBy("prof_quintile"))
_v3_pd = T("V3 - CONCENTRATION | GRAIN: client-treatment | full 12-month window | top quintile = "
           "prof_quintile '5' (cut points from cell [10], earliest-anchor distinct clients) | rate = "
           "unsubbed sends / all sends x 1000, BY quintile | share columns = that quintile's share of "
           "all unsubs vs its share of all sends", _v3_tbl)

_top_row = _v3_pd[_v3_pd["prof_quintile"] == "5"]
if len(_top_row):
    display(Markdown("**top quintile:** unsub rate **" + str(_top_row["unsub_rate_per_1000_sends"].iloc[0]) +
                      "** per 1,000 sends, vs bank-wide rate **" +
                      str(round(1000.0 * _total_unsubs / _total_sends, 2)) + "** per 1,000 - it is **" +
                      str(round(_top_row["unsub_rate_per_1000_sends"].iloc[0] /
                                (1000.0 * _total_unsubs / _total_sends), 2)) + "x** the overall rate"))

# %% [14] V4 - DOOR-CLOSING. GRAIN: distinct CLIENTS (not client-treatments) - a client who
# unsubs while holding one product closes the email door on every OTHER product the bank might
# have cross-sold them, counted once per client, not once per treatment they unsubbed from.
_w_last_unsub = Window.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").desc())
unsub_clients = (spine_banded.filter(F.col("unsub_flag") == 1)
                  .withColumn("rn", F.row_number().over(_w_last_unsub)).filter("rn = 1"))
_n_unsub_clients = unsub_clients.count()
_single_overall = unsub_clients.filter(F.col("prod_band") == "1").count()
display(Markdown("**V4 - DOOR-CLOSING** · GRAIN: distinct unsubbed CLIENTS (full window, bank-wide) · "
                  "one row per client, taken at their LATEST unsub_tm if they unsubbed from more than "
                  "one treatment · single-product clients as % of ALL unsubbed clients: **" +
                  str(round(100.0 * _single_overall / _n_unsub_clients, 1)) + "%** (" +
                  str(_single_overall) + " of " + str(_n_unsub_clients) + ")"))

v4_by_program = (unsub_clients.groupBy("program")
                  .agg(F.count("*").alias("unsub_clients"),
                       F.sum(F.when(F.col("prod_band") == "1", 1).otherwise(0)).alias("single_product_clients"))
                  .withColumn("pct_single_product", F.round(100.0 * F.col("single_product_clients") / F.col("unsub_clients"), 1))
                  .orderBy(F.desc("unsub_clients")))
T("V4 - DOOR-CLOSING, by program", v4_by_program)

v4_tibc = (unsub_clients.filter(F.col("prod_band") == "1").groupBy("tibc_mix")
           .agg(F.count("*").alias("single_product_clients")).orderBy(F.desc("single_product_clients")))
T("V4 - DOOR-CLOSING, single-product unsubs by category held", v4_tibc)

# %% [15] V5 - STABILITY. V3's rate-by-quintile split by cohort_month (launch month) - does the
# concentration pattern hold every month of the year or is V3 a few months' noise.
_v5_months = [m for m in sorted(spine_banded.select("cohort_month").distinct().toPandas()["cohort_month"])
              if m != "UNKNOWN"]
_v5_tbl = (spine_banded.filter(F.col("cohort_month").isin(_v5_months))
           .groupBy("cohort_month", "prof_quintile")
           .agg(F.count("*").alias("sends"), F.sum("unsub_flag").alias("unsubs"))
           .withColumn("unsub_rate_per_1000_sends", F.round(1000.0 * F.col("unsubs") / F.col("sends"), 2))
           .orderBy("cohort_month", "prof_quintile"))
T("V5 - STABILITY | GRAIN: client-treatment | V3's rate-by-quintile, split by cohort_month (= launch "
  "month, not unsub month) | 12 real cohort months x 5 quintiles; UNKNOWN cohort_month (undecodable "
  "TREATMENT_ID) shown as its own row, excluded from the 12x5 grid", _v5_tbl)

_unknown_n = spine_banded.filter(F.col("cohort_month") == "UNKNOWN").count()
if _unknown_n:
    display(Markdown("cohort_month = UNKNOWN (decode failed, see cell [5]): **" + str(_unknown_n) +
                      "** client-treatment sends, **" +
                      str(spine_banded.filter((F.col("cohort_month") == "UNKNOWN") &
                                               (F.col("unsub_flag") == 1)).count()) + "** unsubbed"))

# %% [16] V6 - PROF_TOT_ANNUAL vetting. GRAIN: client-treatment, matched rows only -
# investigation, not reporting. Checking whether the field behaves like current-year contribution
# (should RISE with tenure) or something else. If it rises with tenure: label it "annual
# profitability" on slides, NEVER "LTV" or "lifetime value", and flag that it will UNDERSTATE
# young/new clients relative to their future trajectory - a young high-potential unsub looks
# cheap here by construction.
PCTS = [0.10, 0.25, 0.50, 0.75, 0.90]
_matched_spine = spine_banded.filter(F.col("ucp_matched"))
_v6_rows = []
for band in ["0-2", "3-5", "6-10", "11-20", "20+", "unknown"]:
    sub = _matched_spine.filter(F.col("tenure_band") == band)
    n = sub.count()
    if n == 0:
        continue
    q = sub.approxQuantile("PROF_TOT_ANNUAL", PCTS, 0.01)
    _v6_rows.append({"tenure_band": band, "n": n, "p10": q[0], "p25": q[1], "p50": q[2], "p75": q[3], "p90": q[4]})
T("V6 - PROF_TOT_ANNUAL VETTING | GRAIN: client-treatment, matched rows only, full window | "
  "percentiles of PROF_TOT_ANNUAL by tenure_band - rising with tenure supports 'current-year "
  "contribution'; flat or non-monotonic means the definition needs confirming before it goes "
  "anywhere near a slide", pd.DataFrame(_v6_rows))

# %% [17] ONE-SCREEN SUMMARY
display(Markdown("## UNSUB VALUE / UCP v3 SUMMARY"))

_summary_rows = [
    ("Send base (client-treatment sends, full 12-month window Jul2025-Jun2026, bank-wide)", _total_sends),
    ("Unsubs", _total_unsubs),
    ("Unsub rate per 1,000 sends", round(1000.0 * _total_unsubs / _total_sends, 2)),
    ("Match rate, spine, UCP at per-treatment anchor (%)", round(100.0 * _matched / _before_join, 1)),
    ("Distinct unsubbed clients (V4 grain)", _n_unsub_clients),
]
T("SUMMARY - headline figures", pd.DataFrame(_summary_rows, columns=["figure", "value"]))

T("SUMMARY - V1 top-3 programs by unsub VOLUME", v1_program.orderBy(F.desc("unsubs")).limit(3))
T("SUMMARY - V1 top-3 programs by unsub RATE (min 1000 sends to avoid small-n noise)",
  v1_program.filter(F.col("client_treatment_sends") >= 1000)
            .orderBy(F.desc("unsub_rate_per_1000_sends")).limit(3))

if len(_top_row):
    display(Markdown("**V3 concentration:** top-quintile unsub rate is **" +
                      str(_top_row["unsub_rate_per_1000_sends"].iloc[0]) + "** per 1,000 sends vs "
                      "bank-wide **" + str(round(1000.0 * _total_unsubs / _total_sends, 2)) + "**"))

display(Markdown("**V4 door-closing:** **" + str(round(100.0 * _single_overall / _n_unsub_clients, 1)) +
                  "%** of distinct unsubbed clients (full window) hold exactly one product"))

print("\nCAVEATS:")
print("- mne/cohort attribution is TREATMENT-based, not last-touch: a client's outcome on a given")
print("  send attributes to THAT treatment's own launch date, independent of any other treatment")
print("  the same client may have unsubbed from elsewhere in the window")
print("- launch_dt decode: positions 1-7 of TREATMENT_ID = YYYYDDD is Andre's WORKING ASSUMPTION,")
print("  not documented anywhere in the repo (only positions 8-10 = MNE is documented) - validated")
print("  empirically in cell [5] at", _pct_valid, "% of distinct TREATMENT_IDs (threshold: 99%)")
print("- V1-V6 are ALL full-window (bank-wide, 12 months) and are TRUE RATES now (real send")
print("  denominator on every row) - this is the load-bearing change from v2, which only had true")
print("  rates for a single Q2 quarter compared against a separately-built base")
print("- PROF_TOT_ANNUAL is UNVETTED as a value proxy - see V6; do not call it LTV on a slide")
print("- CARDS_MNES excludes CTU and O2P per Andre's 2026-07-26 ruling (see cell [4]) - both are")
print("  visible under OTHER_BANK via the raw mne column, not hidden")
print("- DEFAULT stream is shown, not hidden - it is real client-facing mail outside MNE suppression")
print("- HAS_CLNT_TYP =", HAS_CLNT_TYP, "- if False, the Personal-client filter was NOT applied anywhere")
print("- UCP uniqueness per (CLNT_NO, MONTH_END_DATE) is asserted by the cell [9] fan-out guard, not")
print("  independently verified beyond that")
print("- rows where ucp_anchor got clamped to UCP_MIN/UCP_MAX (see cell [6]):", _n_clamped,
      "- their UCP snapshot is not the true month-before-launch")

# OPEN QUESTIONS (unverified, flag before this ships anywhere):
# - JULIAN LAYOUT: positions 1-7 = YYYYDDD is an assumption, not documented (see cell [5] header
#   comment for the full citation trail). Validated empirically at runtime; if the % ever drops
#   below 99%, do not trust cohort_month/ucp_anchor - fall back to TACTIC_EVNT_IP_AR_H60M.
# - PROF_TOT_ANNUAL definition: current-year contribution vs lifetime/projected value - V6 is the
#   vetting pass; if it doesn't rise cleanly with tenure, do not caption it "profitability" either
#   without a footnote.
# - UCP PARTITION AVAILABILITY: the schema-probe floor (cell [2]) only guarantees coverage through
#   2026-05-31; cell [6] flags (not blocks on) any anchor that got clamped for lack of a true
#   month-before-launch partition - re-check _n_clamped before trusting the affected rows.
# - SENDS_12M GRAIN: assumed bank-wide (all campaigns), matching v2's q2_recipients convention -
#   not explicitly stated in the brief for this dataset; if the companion extract (pack 29) is
#   actually cards-only or program-scoped, V1's "cards in perspective" framing breaks and needs
#   re-deriving.
# - CARDS_MNES vs CTU/O2P: resolved 2026-07-26 (see cell [4]) - no longer open.
# - CLNT_TYP presence: probed at runtime (cell [2]); if absent, HAS_CLNT_TYP=False and NO
#   client-type filter is applied anywhere downstream - the cohort may include non-Personal clients.
# - ucp4 uniqueness per (CLNT_NO, MONTH_END_DATE): asserted via the fan-out guard in cell [9]; if
#   it trips, the commented dedup fallback there needs to be uncommented and re-run.
# - tibc_mix / prod_band / tenure_band / age_band cut points are this script's own choice, not
#   documented anywhere else in the repo - editable at the top of cell [10] (TENURE_EDGES, AGE_EDGES).
# - prof_quintile cut points are derived from earliest-anchor distinct clients in THIS 12-month
#   window (cell [10]) - re-running this script over a different window will shift the cut
#   points; that is intentional (quintiles are always relative to who we can currently reach),
#   but it means V3's numbers are NOT comparable run-to-run without re-stating the cut points.
