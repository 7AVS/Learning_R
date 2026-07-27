# UNSUB VALUE / UCP v2 - not all unsubscriptions have the same value. Some leavers cost more
# than others. 2026-07-26.
#
# HYPOTHESIS: unsub is not a uniform event. Feeds a MAX-2-SLIDE deck:
#   Slide 1 - WHERE we lose clients (by program/MNE, bank-wide, cards in perspective) and who's
#             losing the valuable ones.
#   Slide 2 - WHO leaves vs the emailed base + concentration + door-closing.
#
# EXACTLY FOUR client variables drive every output: tenure, age, TIBC product counts,
# PROF_TOT_ANNUAL. No others. AGE_RNG / TENURE_RBC_RNG / PROF_SEG_CD are pulled defensively in
# the schema probe (cheap, already there) but no output below uses them - do not let them creep
# into a cut.
#
# Engine: Spark/YARN only. `spark` is pre-initialized in the kernel - no SparkSession.builder,
# no .stop(). NO Teradata, NO teradatasql, NO credentials, NO EDW pull - every input below is
# already landed to HDFS by cpc_reservoir_extract.py (unsub_base, q2_recipients).
#
# Inputs (BASE = hdfs:///user/427966379/unsub_cpc/):
#   unsub_base/*       CLNT_NO, unsub_tm, TREATMENT_ID - ALL unsub events, bank-wide (NOT
#                       filtered to cards - "where do we lose clients" needs the whole bank as
#                       the frame, cards is a slice of that, not the universe)
#   q2_recipients/m04, m05, m06     CLNT_NO only - anyone emailed that month, all campaigns
#   UCP personal parquet at /prod/sz/tsz/00172/data/ucp4/MONTH_END_DATE=<date>
#
# OUTPUT MODEL - this is the load-bearing change from v1: NO big saved tables, NO CSV. Every
# result is a SMALL PRINTED LABELED TABLE (museum evidence style, see cpc_evidence_hdfs.py's
# E1..E12), numbered V1..V6, each stating its own window/numerator/denominator in the printed
# header. Transcribe to RESULTS_CATALOG.md by hand after the run - these prints are the record.
# The ONLY persisted artifact is the enriched spine (cell [6]) - UCP reads are expensive, every
# re-cut after that is a cheap groupBy over a landed parquet. rate_matrix_q2 and
# profile_full_window (v1's saves) are retired - if they still exist under OUT from a prior run,
# they are orphaned; clean up manually (hadoop fs -rm -r) if disk matters, this script no longer
# writes them.

# %% [0] Header (see module docstring above for the full brief - this cell just states OUT)
BASE = "hdfs:///user/427966379/unsub_cpc/"
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"
OUT = BASE + "unsub_value/"
print("v2 | single persisted output: OUT + 'enriched_spine' | everything else prints, nothing else saves")

# %% [1] Setup
import pandas as pd
from pyspark.sql import functions as F, Window

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.max_rows", 60)

# every join below is cohort-to-UCP or cohort-to-recipients; none of these are small-broadcast-safe
# by default on this cluster (house convention, see 15_*.py / v1 of this script)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


def land_df(name, df):
    """Write a Spark DataFrame to OUT+name, read it back, assert the count holds. Idempotent:
    overwrites every call. This is called exactly ONCE in this script (cell [6], the spine) -
    everything downstream reads that landing back rather than re-deriving from UCP."""
    n_before = df.count()
    df.write.mode("overwrite").parquet(OUT + name)
    n_after = spark.read.parquet(OUT + name).count()
    assert n_after == n_before, (
        name + " HDFS readback mismatch: wrote " + str(n_before) + " read back " + str(n_after))
    print(name, ": landed", n_before, "rows, readback confirms", n_after)


def norm_clnt(col):
    return F.regexp_replace(F.trim(col.cast("string")), "^0+", "")


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
print("\nrequested column presence check:")
_present_tbl = pd.DataFrame(
    {"column": REQUESTED_COLS,
     "status": ["PRESENT" if c in _actual else "MISSING" for c in REQUESTED_COLS]})
print(_present_tbl.to_string(index=False))

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
      "(controls whether the UCP read loop in cell [5] applies the Personal-client filter)")

# CRITICAL MINIMUM - promoted from v1: PROF_TOT_ANNUAL is now REQUIRED, not optional. This whole
# script exists to answer "which leavers were worth more" - without PROF_TOT_ANNUAL there is no
# value axis and the analysis is pointless. Fail loud here rather than silently degrading to a
# volume-only report three cells from now.
_critical = ["CLNT_NO", "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT",
             "C_TOT_CNT", "PROF_TOT_ANNUAL"]
_missing_critical = [c for c in _critical if c not in _actual]
assert not _missing_critical, (
    "CRITICAL UCP COLUMNS MISSING from schema: " + str(_missing_critical) + ". "
    "If PROF_TOT_ANNUAL is on this list: STOP. The entire premise of this script is 'not all "
    "unsubs are worth the same' - without a value field there is nothing to measure and every "
    "cell below is unwriteable. Check the UCP field catalog before touching anything else.")

assert UCP_MAX >= "2026-06-30", (
    "max available UCP partition (" + UCP_MAX + ") does not cover the analysis window (need "
    "through at least 2026-06-30 for the Q2 base comparison). Adjust RATE_ANCHOR / the per-client "
    "anchor clamp in cell [5] to whatever UCP_MAX actually is.")

print("\nSCHEMA PROBE PASSED - UCP_COLS and HAS_CLNT_TYP are now fixed for the rest of this run.")

# %% [3] Unsub cohort - bank-wide, NOT filtered to cards. "Where do we lose clients" needs the
# whole bank as the frame; cards is a slice of that, not the universe, and V1 below needs the
# other-bank volume to show cards in perspective.
_raw = spark.read.parquet(BASE + "unsub_base/*")
_raw = _raw.withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO")))

# Andre's rule: per client, take the LAST unsub event - that position is what anchors the UCP
# month (cell [5]), and it is the client's most recent, most decision-relevant exit.
_wlast = Window.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").desc())
cohort = (_raw.withColumn("rn", F.row_number().over(_wlast)).filter("rn = 1")
          .select("CLNT_NO",
                  F.col("unsub_tm").alias("last_unsub_tm"),
                  F.col("TREATMENT_ID").alias("last_treatment_id")))
cohort = cohort.withColumn("cohort_month", F.date_format("last_unsub_tm", "yyyy-MM"))
cohort.cache()

_n_rows = _raw.count()
_n_clients = cohort.count()
_multi = _raw.groupBy("CLNT_NO").count().filter("count > 1").count()
_span = _raw.agg(F.min("unsub_tm").alias("mn"), F.max("unsub_tm").alias("mx")).collect()[0]

print("UNSUB COHORT (unsub_base/*, bank-wide, ALL events, last event per client):")
print(pd.DataFrame([{
    "total_event_rows": _n_rows,
    "distinct_clients": _n_clients,
    "clients_with_gt1_event": _multi,
    "min_unsub_tm": str(_span["mn"]),
    "max_unsub_tm": str(_span["mx"]),
}]).to_string(index=False))

assert _n_clients > 300000, (
    "distinct unsub clients (" + str(_n_clients) + ") is far below the expected ~319,733 from "
    "the verified reservoir - check unsub_base/* landed correctly before proceeding.")

# %% [4] Program mapping - CARDS_MNES sourced from the repo's own MNE catalog, not invented here.
# Source 1: UNSUB_TRACKING_KNOWLEDGE.md section 4 "MNE tracking scope" - the ONLY table in the
#   repo that groups MNEs by business line. Its "Cards" rows: PCQ, PCL, PCD, AUH, CLI, MVP, CRV.
# Source 2: email_active_mnes.md - a volume-ranked top-30 subset (not exhaustive), confirms PCQ,
#   PCL, PCD as "Cards?" checked; does not contradict source 1, just doesn't cover the smaller ones.
#
# JUDGMENT CALL FLAGGED FOR ANDRE: the v2 spec text named CTU and O2P as cards MNEs "at minimum".
# The repo's own group table classifies CTU as PBA ("Chequing Account Right Fit") and O2P as
# Personal Lending ("Pre-approved Overdraft Opportunity") - NOT Cards. Per feedback rule
# "/schemas/ and reference docs are truth, don't hedge documented fields", and because this is a
# documentation conflict rather than an operational fact Andre stated about a live switch, CTU and
# O2P are LEFT OUT of CARDS_MNES here pending confirmation. They still surface in V1's top-15 MNE
# table under program='OTHER_BANK' via the raw mne column - nothing is hidden, just not pre-labeled
# Cards. Flip this list if that's wrong.
CARDS_MNES = frozenset({"PCQ", "PCL", "PCD", "AUH", "CLI", "MVP", "CRV"})

# DEFAULT stream (per UNSUB_TRACKING_KNOWLEDGE.md): TREATMENT_ID = 'DEFAULT' literally, or a blank
# MNE substring, both mean "mail outside campaign taxonomy" (service + broken-template + untagged
# marketing) - invisible to MNE-based suppression, a governance finding in its own right. Keep it
# labeled, never collapse it into OTHER_BANK.
cohort = cohort.withColumn(
    "mne",
    F.when((F.trim(F.col("last_treatment_id")) == "DEFAULT") |
           (F.trim(F.substring(F.col("last_treatment_id"), 8, 3)) == ""), F.lit("DEFAULT"))
     .otherwise(F.trim(F.substring(F.col("last_treatment_id"), 8, 3))))

cohort = cohort.withColumn(
    "program",
    F.when(F.col("mne") == "DEFAULT", F.lit("DEFAULT"))
     .when(F.col("mne").isin(list(CARDS_MNES)), F.lit("CARDS"))
     .otherwise(F.lit("OTHER_BANK")))

_mne_top20 = (cohort.groupBy("mne", "program").agg(F.countDistinct("CLNT_NO").alias("unsub_clients"))
              .orderBy(F.desc("unsub_clients")).limit(20).toPandas())
print("TOP 20 MNEs by unsub count, program label attached:")
print(_mne_top20.to_string(index=False))
print("\nprogram totals (bank-wide):")
print(cohort.groupBy("program").agg(F.countDistinct("CLNT_NO").alias("unsub_clients"))
      .orderBy(F.desc("unsub_clients")).toPandas().to_string(index=False))

# %% [5] UCP anchors + read
# Per-client anchor: the last COMPLETED month-end strictly before that client's last_unsub_tm.
# last_day(add_months(d, -1)) always lands on the previous calendar month's last day, which is
# always < d regardless of where in its own month d falls - a single closed-form expression, no
# day-of-month edge case. Clamped both directions to partitions that actually exist.
cohort = cohort.withColumn(
    "ucp_anchor_raw", F.last_day(F.add_months(F.col("last_unsub_tm"), -1)))
cohort = cohort.withColumn(
    "ucp_anchor",
    F.greatest(F.least(F.col("ucp_anchor_raw"), F.lit(UCP_MAX).cast("date")),
               F.lit(UCP_MIN).cast("date")))

# RATE_ANCHOR: one common snapshot for the Q2 base comparison (cell [7]) - has to be common or
# the base-vs-unsub profile comparison isn't a comparison. Pre-Q2 (Q2 events start 2026-04-01),
# so every attribute pulled at this anchor is pre-treatment relative to the events being measured.
RATE_ANCHOR = min("2026-03-31", UCP_MAX)

_anchor_dist = (cohort.groupBy("ucp_anchor").agg(F.countDistinct("CLNT_NO").alias("clients"))
                 .orderBy("ucp_anchor").toPandas())
print("UCP_ANCHOR distribution (per-client, last-unsub month, clamped to [", UCP_MIN, ",", UCP_MAX, "]):")
print(_anchor_dist.to_string(index=False))
print("\nRATE_ANCHOR (single common snapshot, spine anchors are per-client - see cell [7] for its use):",
      RATE_ANCHOR)


def read_ucp(month_ends, cols):
    frames = []
    for m in month_ends:
        path = UCP_BASE + "MONTH_END_DATE=" + m
        raw = spark.read.option("basePath", UCP_BASE).parquet(path)
        # filter on CLNT_TYP BEFORE selecting cols - UCP_COLS never includes CLNT_TYP (it isn't
        # in the requested field list), so filtering after select() would hit an unresolved column
        if HAS_CLNT_TYP:
            raw = raw.filter(F.trim(F.col("CLNT_TYP")) == "Personal")
        sel = raw.select(*cols)
        sel = sel.withColumn("ucp_month_end", F.lit(m))
        sel = sel.withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO")))
        n = sel.count()
        print("  UCP partition", m, ":", n, "rows selected (Personal filter applied =", HAS_CLNT_TYP, ")")
        frames.append(sel)
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out


_anchor_months = sorted(str(m) for m in
                         [r["ucp_anchor"] for r in cohort.select("ucp_anchor").distinct().collect()])
print("reading UCP for", len(_anchor_months), "per-client anchor month-ends...")
ucp_spine = read_ucp(_anchor_months, UCP_COLS)
assert ucp_spine.count() > 0, "UCP spine-anchor read returned zero rows - investigate before proceeding"

print("\nreading UCP for RATE_ANCHOR (" + RATE_ANCHOR + ")...")
ucp_rate = read_ucp([RATE_ANCHOR], UCP_COLS)
assert ucp_rate.count() > 0, "UCP rate-anchor read returned zero rows - investigate before proceeding"

# %% [6] Join + fan-out guard + THE ONLY SAVE IN THIS SCRIPT
_before = cohort.count()
_joined = cohort.join(
    ucp_spine, (cohort.CLNT_NO == ucp_spine.CLNT_NO) & (cohort.ucp_anchor == ucp_spine.ucp_month_end),
    how="left"
).select(cohort["*"], *[ucp_spine[c].alias(c) for c in UCP_COLS if c != "CLNT_NO"], ucp_spine["ucp_month_end"])
_after = _joined.count()

# If this assert trips, UCP is not unique per (CLNT_NO, MONTH_END_DATE) - dedup fallback:
# w = Window.partitionBy("CLNT_NO", "ucp_month_end").orderBy(F.lit(1))
# ucp_spine = ucp_spine.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
assert _before == _after, (
    "FAN-OUT: cohort " + str(_before) + " rows -> joined " + str(_after) +
    " rows after UCP join. UCP is not unique per (CLNT_NO, MONTH_END_DATE) - apply the commented "
    "dedup fallback above and re-run this cell.")
print("fan-out guard OK: cohort rows preserved through UCP join (", _before, "==", _after, ")")

_joined = _joined.withColumn("ucp_matched", F.col("AGE").isNotNull())
_matched = _joined.filter(F.col("ucp_matched")).count()
print("MATCH RATE (spine):")
print(pd.DataFrame([{
    "cohort_clients": _before, "matched": _matched, "unmatched": _before - _matched,
    "match_pct": round(100.0 * _matched / _before, 1),
}]).to_string(index=False))

_spine_out = _joined.select(
    "CLNT_NO", "last_unsub_tm", "cohort_month", "mne", "program", "ucp_month_end",
    "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT",
    "PROF_TOT_ANNUAL", "ucp_matched")
land_df("enriched_spine", _spine_out)

# %% [7] Q2 emailed base - per-month reads (client-month grain), then collapsed to distinct
# clients for the profile comparison (V2/V3/V5 compare PROFILES, not per-month rates - one row
# per client across the quarter, banded at the single common RATE_ANCHOR).
_Q2_MONTH_SUBPATHS = [("m04", "2026-04"), ("m05", "2026-05"), ("m06", "2026-06")]


def read_q2_recipients():
    frames = []
    for sub, cm in _Q2_MONTH_SUBPATHS:
        f = (spark.read.parquet(BASE + "q2_recipients/" + sub)
             .withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO")))
             .withColumn("cohort_month", F.lit(cm))
             .dropDuplicates(["CLNT_NO"]))
        n = f.count()
        assert n > 0, ("q2_recipients/" + sub + " landed zero rows - a subpath failed to land "
                        "silently, fix before trusting any base profile for " + cm)
        frames.append(f)
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out


q2_client_months = read_q2_recipients()
print("Q2 EMAILED BASE - per-month client counts (client-month grain, a client emailed in 2+")
print("months legitimately appears 2+ times here):")
print(q2_client_months.groupBy("cohort_month")
      .agg(F.countDistinct("CLNT_NO").alias("emailed_clients"))
      .orderBy("cohort_month").toPandas().to_string(index=False))

# collapse to ONE ROW PER CLIENT across the quarter - the profile-comparison grain
q2_base_clients = q2_client_months.select("CLNT_NO").distinct()
_ucp_rate_narrow = ucp_rate.drop("ucp_month_end")

_before_base = q2_base_clients.count()
q2_base = q2_base_clients.join(_ucp_rate_narrow, "CLNT_NO", "left")
_after_base = q2_base.count()
assert _before_base == _after_base, (
    "FAN-OUT on Q2 base join: " + str(_before_base) + " -> " + str(_after_base) +
    " - UCP not unique at RATE_ANCHOR, re-check cell [6]'s fan-out guard assumption.")
q2_base = q2_base.withColumn("ucp_matched", F.col("AGE").isNotNull())

_n_base = q2_base.count()
_n_base_matched = q2_base.filter(F.col("ucp_matched")).count()
print("\nQ2 BASE distinct-client total:", _n_base, "| matched to UCP at RATE_ANCHOR (" + RATE_ANCHOR + "):",
      _n_base_matched, "(", round(100.0 * _n_base_matched / _n_base, 1), "%)")

# %% [8] BANDING - one function, applied identically to spine (re-read from the landed parquet -
# cheap, this is the point of persisting it) and to the Q2 base built in cell [7].
TENURE_EDGES = [2, 5, 10, 20]     # years; bins: 0-2, 3-5, 6-10, 11-20, 20+ - our choice, not documented
AGE_EDGES = [24, 34, 44, 54, 64]  # bins: <25, 25-34, 35-44, 45-54, 55-64, 65+ - our choice, not documented


def apply_bands(df, prof_cuts):
    """prof_cuts = 4 cut points (20/40/60/80th pct) computed ONCE on the Q2 base and reused here
    for both arms - see cell [8] main body for why base-derived cuts, not self-derived."""
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


# "valuable" means valuable RELATIVE TO WHO WE CAN REACH - cut points are computed on the Q2
# EMAILED BASE distribution (matched clients only), then applied unchanged to the unsub spine.
# Cutting quintiles on the unsub population itself would be circular: unsubs would always split
# 20/20/20/20/20 by construction, and V3's concentration finding would be manufactured, not found.
_base_matched_prof = q2_base.filter(F.col("ucp_matched") & F.col("PROF_TOT_ANNUAL").isNotNull())
PROF_CUTS = _base_matched_prof.approxQuantile("PROF_TOT_ANNUAL", [0.2, 0.4, 0.6, 0.8], 0.01)
print("PROF_TOT_ANNUAL quintile cut points (computed on Q2 base, matched clients, n =",
      _base_matched_prof.count(), "):")
print(pd.DataFrame([{"p20": PROF_CUTS[0], "p40": PROF_CUTS[1], "p60": PROF_CUTS[2], "p80": PROF_CUTS[3]}])
      .to_string(index=False))

base_banded = apply_bands(q2_base, PROF_CUTS)
spine_banded = apply_bands(spark.read.parquet(OUT + "enriched_spine"), PROF_CUTS)
q2_unsub_banded = spine_banded.filter(F.col("cohort_month").isin("2026-04", "2026-05", "2026-06"))

print("\nspine_banded (full window, bank-wide):", spine_banded.count(), "rows")
print("q2_unsub_banded (spine subset, Apr-Jun 2026 last-unsub):", q2_unsub_banded.count(), "rows")
print("base_banded (Q2 emailed, distinct clients):", base_banded.count(), "rows")

# %% [9] V1 - WHERE we lose clients (Slide 1 feed). FULL WINDOW, bank-wide spine.
print("=" * 100)
print("V1 - WHERE | window: full unsub cohort (bank-wide, all history in unsub_base) | by program,")
print("     then top-15 MNEs | median_prof_annual and pct_top_quintile use matched clients only")
print("=" * 100)
_total_unsubs = spine_banded.count()


def _v1_metrics(df, keys):
    return (df.groupBy(*keys)
            .agg(F.count("*").alias("unsub_clients"),
                 F.expr("percentile_approx(PROF_TOT_ANNUAL, 0.5)").alias("median_prof_annual"),
                 F.expr("percentile_approx(TENURE_RBC_YEARS, 0.5)").alias("median_tenure_years"),
                 F.round(100.0 * F.sum(F.when(F.col("prod_band") == "1", 1).otherwise(0)) / F.count("*"), 1)
                 .alias("pct_single_product"),
                 F.round(100.0 * F.sum(F.when(F.col("prof_quintile") == "5", 1).otherwise(0)) / F.count("*"), 1)
                 .alias("pct_top_quintile"))
            .withColumn("share_of_all_unsubs_pct", F.round(100.0 * F.col("unsub_clients") / _total_unsubs, 1)))


v1_program = _v1_metrics(spine_banded, ["program"]).orderBy(F.desc("unsub_clients"))
print("by program:")
print(v1_program.toPandas().to_string(index=False))

v1_mne = _v1_metrics(spine_banded, ["mne", "program"]).orderBy(F.desc("unsub_clients")).limit(15)
print("\ntop-15 MNEs (program flag attached, CARDS rows are the ones this deck is about):")
print(v1_mne.toPandas().to_string(index=False))

# %% [10] V2 - WHO, four-variable mix vs the Q2 emailed base (Slide 2 feed). Both arms profiled
# at RATE_ANCHOR for comparability; long format so the four dims sit in one printed table.
print("=" * 100)
print("V2 - WHO | Q2 unsubs (last_unsub_tm Apr-Jun 2026, n =", q2_unsub_banded.count(),
      ") vs Q2 emailed base (n =", base_banded.count(), ") | both profiled at RATE_ANCHOR =", RATE_ANCHOR)
print("     | ratio = pct_unsubs / pct_base; ratio > 1 means that segment is OVER-represented in unsubs")
print("=" * 100)

_v2_dims = ["prod_band", "tenure_band", "age_band", "prof_quintile"]
_n_unsub_q2 = q2_unsub_banded.count()
_n_base_q2 = base_banded.count()
_v2_frames = []
for dim in _v2_dims:
    ub = (q2_unsub_banded.groupBy(dim).count()
          .withColumn("pct_unsubs", F.round(100.0 * F.col("count") / _n_unsub_q2, 2)).drop("count"))
    bb = (base_banded.groupBy(dim).count()
          .withColumn("pct_base", F.round(100.0 * F.col("count") / _n_base_q2, 2)).drop("count"))
    m = (ub.join(bb, dim, "outer")
         .withColumn("pct_unsubs", F.coalesce(F.col("pct_unsubs"), F.lit(0.0)))
         .withColumn("pct_base", F.coalesce(F.col("pct_base"), F.lit(0.0)))
         .withColumn("ratio", F.round(F.col("pct_unsubs") / F.col("pct_base"), 2))
         .withColumn("segment_dim", F.lit(dim))
         .withColumnRenamed(dim, "segment_value")
         .select("segment_dim", "segment_value", "pct_base", "pct_unsubs", "ratio"))
    _v2_frames.append(m)

v2 = _v2_frames[0]
for f in _v2_frames[1:]:
    v2 = v2.unionByName(f)
print(v2.orderBy("segment_dim", F.desc("ratio")).toPandas().to_string(index=False))

# %% [11] V3 - CONCENTRATION (the headline). Q2 unsubs vs Q2 base, top prof quintile.
print("=" * 100)
print("V3 - CONCENTRATION | same Q2 unsub cohort / Q2 base as V2 | top quintile = prof_quintile '5'")
print("     (computed on the base, see cell [8]) | dollar share = sum(PROF_TOT_ANNUAL) among Q2 unsubs")
print("     in that quintile / sum(PROF_TOT_ANNUAL) across ALL Q2 unsubs - PROF_TOT_ANNUAL unvetted,")
print("     see V6")
print("=" * 100)

_base_top_n = base_banded.filter(F.col("prof_quintile") == "5").count()
_unsub_top_n = q2_unsub_banded.filter(F.col("prof_quintile") == "5").count()
_pct_base_top = round(100.0 * _base_top_n / _n_base_q2, 1)
_pct_unsub_top = round(100.0 * _unsub_top_n / _n_unsub_q2, 1)

_prof_by_q = (q2_unsub_banded.groupBy("prof_quintile")
              .agg(F.sum(F.coalesce(F.col("PROF_TOT_ANNUAL"), F.lit(0.0))).alias("prof_dollars"))
              .toPandas())
_prof_total = _prof_by_q["prof_dollars"].sum()
_prof_top_dollars = _prof_by_q.loc[_prof_by_q["prof_quintile"] == "5", "prof_dollars"].sum()
_pct_prof_share = round(100.0 * _prof_top_dollars / _prof_total, 1) if _prof_total else None

print("top-quintile clients are", _pct_base_top, "% of the emailed base (sanity row - should print",
      "~20% by construction) and", _pct_unsub_top, "% of Q2 unsubs; unsub clients in that quintile",
      "carry", _pct_prof_share, "% of all annual profitability attached to Q2 unsub clients.")
print("\nfull quintile breakdown:")
_v3_tbl = (base_banded.groupBy("prof_quintile").count().withColumnRenamed("count", "base_clients")
           .join(q2_unsub_banded.groupBy("prof_quintile").count().withColumnRenamed("count", "unsub_clients"),
                 "prof_quintile", "outer"))
_v3_pd = _v3_tbl.toPandas().merge(_prof_by_q, on="prof_quintile", how="left").sort_values("prof_quintile")
_v3_pd["pct_of_base"] = round(100.0 * _v3_pd["base_clients"].fillna(0) / _n_base_q2, 1)
_v3_pd["pct_of_unsubs"] = round(100.0 * _v3_pd["unsub_clients"].fillna(0) / _n_unsub_q2, 1)
_v3_pd["pct_of_unsub_prof_dollars"] = round(100.0 * _v3_pd["prof_dollars"].fillna(0) / _prof_total, 1) if _prof_total else None
print(_v3_pd.to_string(index=False))

# %% [12] V4 - DOOR-CLOSING. FULL WINDOW, bank-wide spine (matches V1's window, not V2/V3's Q2 slice).
print("=" * 100)
print("V4 - DOOR-CLOSING | window: full unsub cohort (bank-wide) | a single-product client who")
print("     unsubs closes the email door on every OTHER product the bank might have cross-sold")
print("     them by email - this is not just a cards loss, it's every future email channel for")
print("     whatever else that client could have held")
print("=" * 100)

_single_overall = spine_banded.filter(F.col("prod_band") == "1").count()
print("single-product clients as % of ALL unsubs:", round(100.0 * _single_overall / _total_unsubs, 1), "%",
      "(", _single_overall, "of", _total_unsubs, ")")

v4_by_program = (spine_banded.groupBy("program")
                  .agg(F.count("*").alias("unsub_clients"),
                       F.sum(F.when(F.col("prod_band") == "1", 1).otherwise(0)).alias("single_product_clients"))
                  .withColumn("pct_single_product", F.round(100.0 * F.col("single_product_clients") / F.col("unsub_clients"), 1))
                  .orderBy(F.desc("unsub_clients")))
print("\nby program:")
print(v4_by_program.toPandas().to_string(index=False))

v4_tibc = (spine_banded.filter(F.col("prod_band") == "1").groupBy("tibc_mix")
           .agg(F.count("*").alias("single_product_clients")).orderBy(F.desc("single_product_clients")))
print("\nsingle-product unsubs: which one category they hold:")
print(v4_tibc.toPandas().to_string(index=False))

# %% [13] V5 - STABILITY. V3's concentration numbers split by cohort_month - does the pattern
# hold every month of Q2 or is V3 one month's noise dressed up as a trend.
print("=" * 100)
print("V5 - STABILITY | V3's concentration line, split by cohort_month (last-unsub month) | base")
print("     pct_top is the SAME pooled-quarter number every row (base isn't split by month) -")
print("     included as the fixed reference line the monthly unsub numbers are compared against")
print("=" * 100)

_v5_rows = []
for cm in ["2026-04", "2026-05", "2026-06"]:
    sub = q2_unsub_banded.filter(F.col("cohort_month") == cm)
    n = sub.count()
    if n == 0:
        continue
    top_n = sub.filter(F.col("prof_quintile") == "5").count()
    dollars = sub.groupBy("prof_quintile").agg(F.sum(F.coalesce(F.col("PROF_TOT_ANNUAL"), F.lit(0.0))).alias("d")).toPandas()
    tot = dollars["d"].sum()
    top_d = dollars.loc[dollars["prof_quintile"] == "5", "d"].sum()
    _v5_rows.append({
        "cohort_month": cm, "unsub_clients": n, "pct_top_quintile": round(100.0 * top_n / n, 1),
        "pct_base_top_quintile_ref": _pct_base_top,
        "pct_of_month_prof_dollars_in_top_quintile": round(100.0 * top_d / tot, 1) if tot else None,
    })
print(pd.DataFrame(_v5_rows).to_string(index=False))

# %% [14] V6 - PROF_TOT_ANNUAL vetting. FULL WINDOW, matched clients, bank-wide spine - investigation,
# not reporting. Checking whether the field behaves like current-year contribution (should RISE with
# tenure) or something else. If it rises with tenure: label it "annual profitability" on slides,
# NEVER "LTV" or "lifetime value", and flag that it will UNDERSTATE young/new clients relative to
# their future trajectory - a young high-potential unsub looks cheap here by construction.
print("=" * 100)
print("V6 - PROF_TOT_ANNUAL VETTING | window: full unsub cohort, matched clients only | percentiles")
print("     of PROF_TOT_ANNUAL by tenure_band - rising with tenure supports 'current-year")
print("     contribution'; flat or non-monotonic means the definition needs to be confirmed before")
print("     it goes anywhere near a slide")
print("=" * 100)

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
print(pd.DataFrame(_v6_rows).to_string(index=False))

# %% [15] ONE-SCREEN SUMMARY
print("=" * 100)
print("UNSUB VALUE / UCP v2 SUMMARY")
print("=" * 100)
print("cohort: ", _n_clients, "distinct clients bank-wide, full window |", _n_unsub_q2, "distinct clients, Q2 window")
print("match rate (spine, UCP at per-client anchor):", round(100.0 * _matched / _before, 1), "%")
print("Q2 emailed base (distinct clients, all campaigns):", _n_base, "| matched to UCP:", _n_base_matched)

print("\nV1 top-3 programs by unsub volume:")
print(v1_program.orderBy(F.desc("unsub_clients")).limit(3).toPandas().to_string(index=False))

print("\nV3 concentration: top-quintile clients are", _pct_base_top, "% of the base and", _pct_unsub_top,
      "% of Q2 unsubs; they carry", _pct_prof_share, "% of Q2 unsubs' total PROF_TOT_ANNUAL")

print("\nV4 door-closing:", round(100.0 * _single_overall / _total_unsubs, 1),
      "% of all unsubs (full window) hold exactly one product when they leave")

print("\nCAVEATS:")
print("- mne is LAST-TOUCH attribution: the MNE on the unsub is the email that carried the unsub")
print("  click, not necessarily the sole or true cause of the client's decision to leave")
print("- V2/V3/V5 are Q2-only (Apr-Jun 2026) - the only window with a matched emailed-base denominator")
print("- V1/V4/V6 are full-window (bank-wide unsub_base) and are VOLUME cuts, no denominator - they")
print("  describe the shape of who leaves, they do not support a 'leaves at a higher RATE' claim")
print("- PROF_TOT_ANNUAL is UNVETTED as a value proxy - see V6; do not call it LTV on a slide")
print("- CARDS_MNES excludes CTU and O2P pending Andre's confirmation (see cell [4] comment) - both")
print("  currently fall under OTHER_BANK, visible via the raw mne column, not hidden")
print("- DEFAULT stream is shown, not hidden - it is real client-facing mail outside MNE suppression")
print("- HAS_CLNT_TYP =", HAS_CLNT_TYP, "- if False, the Personal-client filter was NOT applied anywhere")
print("- UCP uniqueness per (CLNT_NO, MONTH_END_DATE) is asserted by the cell [6] fan-out guard, not")
print("  independently verified beyond that")

# OPEN QUESTIONS (unverified, flag before this ships anywhere):
# - PROF_TOT_ANNUAL definition: current-year contribution vs lifetime/projected value - V6 is the
#   vetting pass; if it doesn't rise cleanly with tenure, do not caption it "profitability" either
#   without a footnote.
# - CARDS_MNES vs CTU/O2P: the v2 spec text and the repo's own MNE group table disagree (see cell
#   [4]) - resolve with Andre before this constant is trusted for a Cards-vs-bank framing.
# - CLNT_TYP presence: probed at runtime (cell [2]); if absent, HAS_CLNT_TYP=False and NO
#   client-type filter is applied anywhere downstream - the cohort may include non-Personal clients.
# - ucp4 uniqueness per (CLNT_NO, MONTH_END_DATE): asserted via the fan-out guards in cells [6] and
#   [7]; if either trips, the commented dedup fallback in [6] needs to be uncommented and re-run.
# - tibc_mix / prod_band / tenure_band / age_band cut points are this script's own choice, not
#   documented anywhere else in the repo - editable at the top of cell [8] (TENURE_EDGES, AGE_EDGES).
# - prof_quintile cut points are derived from the Q2 base ONLY (cell [8]) - re-running this script
#   in a later quarter with a different base population will shift the cut points; that is
#   intentional (quintiles are always relative to who we can currently reach), but it means V3's
#   numbers are NOT comparable run-to-run without re-stating the cut points each time.
