# WHO unsubscribes vs WHO DOESN'T - unsub reservoir x personal UCP. 2026-07-26.
#
# Proves: (a) a Q2 2026 UNSUB RATE matrix (emailed denominator vs unsub numerator,
# same UCP snapshot for both arms) by tibc_band x tenure_band x age_band, and
# (b) a Jul2025-Jun2026 volume profile of who shows up in the unsub cohort (no
# denominator - describes the cohort, does NOT support a "who unsubscribes more"
# claim). Also vets whether PROF_TOT_ANNUAL behaves like a value field at all.
#
# Engine: Spark/YARN only. `spark` is pre-initialized in the kernel - no
# SparkSession.builder, no .stop(). NO Teradata, NO teradatasql, NO credentials,
# NO EDW pull - every input below is already landed to HDFS by
# cpc_reservoir_extract.py (unsub_base, q2_recipients, q2_recipients_named).
#
# Supersedes 15_unsub_value_enrichment.py: that script had no emailed-base
# denominator, so its tenure x age x tibc matrix was volume only and could not
# say whether any segment unsubscribes at a higher RATE. This script adds the
# Q2 recipient reservoir (already landed) as the denominator and keeps the old
# volume cut as a separate, explicitly-labeled output.
#
# Inputs (BASE = hdfs:///user/427966379/unsub_cpc/):
#   unsub_base/*            CLNT_NO, unsub_tm, TREATMENT_ID - ALL unsub events,
#                            Jul2025-Jun2026, not deduped (319,733 distinct clients)
#   q2_recipients/*          CLNT_NO only - anyone emailed Apr/May/Jun 2026 (all campaigns)
#   q2_recipients_named/*    CLNT_NO only - same, named campaigns only (blank-MNE excluded)
#   UCP personal parquet at /prod/sz/tsz/00172/data/ucp4/MONTH_END_DATE=<date>
#
# Outputs (OUT = BASE + "unsub_value/"):
#   rate_matrix_q2        counts only (emailed_clients, unsub_clients) by band x cohort_month
#   profile_full_window   counts only, full-window volume profile by band x cohort_month

# %% [1] Setup
import pandas as pd
from pyspark.sql import functions as F, Window

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.max_rows", 60)

BASE = "hdfs:///user/427966379/unsub_cpc/"
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"
OUT = BASE + "unsub_value/"

# every join below is cohort-to-UCP or cohort-to-recipients; none of these are
# small-broadcast-safe by default on this cluster (house convention, see 15_*.py)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


def land_df(name, df):
    """Write a Spark DataFrame to OUT+name, read it back, assert the count holds.
    Idempotent: overwrites every call - this is a derived-output landing, not a
    once-only EDW pull, so there is no SQL-manifest skip logic (cf. land() in
    cpc_reservoir_extract.py, which guards an expensive Teradata round-trip)."""
    n_before = df.count()
    df.write.mode("overwrite").parquet(OUT + name)
    n_after = spark.read.parquet(OUT + name).count()
    assert n_after == n_before, (
        name + " HDFS readback mismatch: wrote " + str(n_before) + " read back " + str(n_after))
    print(name, ": landed", n_before, "rows, readback confirms", n_after)


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

REQUESTED_COLS = ["CLNT_NO", "AGE", "AGE_RNG", "GENERATION", "TENURE_RBC_YEARS", "TENURE_RBC_RNG",
                   "PROF_TOT_ANNUAL", "PROF_TOT_MONTHLY", "PROF_SEG_CD", "INCOME_AFTER_TAX_RNG",
                   "CREDIT_SCORE_RNG", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT",
                   "ACTV_PROD_CNT", "OLB_ENROLLED_IND", "MOBILE_AUTH_CNT"]

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

# CLNT_TYP is NOT in the documented 53-field list but earlier cards scripts (15_*.py) filter
# on it - probe for it explicitly rather than assuming either way.
HAS_CLNT_TYP = "CLNT_TYP" in _actual
print("\nHAS_CLNT_TYP =", HAS_CLNT_TYP,
      "(controls whether cell [5] applies the Personal-client filter)")

_critical = ["CLNT_NO", "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT"]
_missing_critical = [c for c in _critical if c not in _actual]
assert not _missing_critical, (
    "CRITICAL UCP COLUMNS MISSING from schema: " + str(_missing_critical) +
    " - this script cannot band tibc/tenure without them. Stop and check the UCP field catalog.")

assert UCP_MAX >= "2026-06-30", (
    "max available UCP partition (" + UCP_MAX + ") does not cover the analysis window "
    "(need through at least 2026-06-30 for the Q2 rate anchor and the full-window profile). "
    "Adjust RATE_ANCHOR / PROFILE_ANCHOR clamping in cell [4] to whatever UCP_MAX actually is.")

print("\nSCHEMA PROBE PASSED - UCP_COLS and HAS_CLNT_TYP are now fixed for the rest of this run.")

# %% [3] Unsub cohort from the reservoir - ALL events, not deduped to first
_raw = spark.read.parquet(BASE + "unsub_base/*")


def norm_clnt(col):
    return F.regexp_replace(F.trim(col.cast("string")), "^0+", "")


_raw = _raw.withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO")))
_raw = _raw.withColumn("mne", F.trim(F.substring(F.col("TREATMENT_ID"), 8, 3)))

wfirst = Window.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").asc())
wlast = Window.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").desc())

_first = (_raw.withColumn("rn", F.row_number().over(wfirst)).filter("rn = 1")
          .select("CLNT_NO",
                  F.col("unsub_tm").alias("first_unsub_tm"),
                  F.col("TREATMENT_ID").alias("first_treatment_id"),
                  F.col("mne").alias("first_mne")))
_last = (_raw.withColumn("rn", F.row_number().over(wlast)).filter("rn = 1")
         .select("CLNT_NO",
                 F.col("unsub_tm").alias("last_unsub_tm"),
                 F.col("TREATMENT_ID").alias("last_treatment_id"),
                 F.col("mne").alias("last_mne")))

cohort = _first.join(_last, "CLNT_NO", "inner")
cohort.cache()

_n_rows = _raw.count()
_n_clients = cohort.count()
_multi = (_raw.groupBy("CLNT_NO").count().filter("count > 1").count())
_span = _raw.agg(F.min("unsub_tm").alias("mn"), F.max("unsub_tm").alias("mx")).collect()[0]

print("UNSUB COHORT (unsub_base/*, all events, Jul2025-Jun2026):")
print(pd.DataFrame([{
    "total_event_rows": _n_rows,
    "distinct_clients": _n_clients,
    "clients_with_gt1_event": _multi,
    "min_unsub_tm": str(_span["mn"]),
    "max_unsub_tm": str(_span["mx"]),
}]).to_string(index=False))

assert _n_clients > 300000, (
    "distinct unsub clients (" + str(_n_clients) + ") is far below the expected ~319,733 "
    "from the verified reservoir - check unsub_base/* landed correctly before proceeding.")

# %% [4] Anchor dates - two anchors, different purposes, both explicit
# PROFILE_ANCHOR: per-client, tied to that client's OWN last unsub - Andre's stated rule is
# "last unsub position matched to the UCP month". Clamp to UCP_MAX so we never request a
# partition that doesn't exist yet.
cohort = cohort.withColumn(
    "profile_anchor",
    F.least(F.last_day(F.col("last_unsub_tm")), F.lit(UCP_MAX).cast("date")))

# RATE_ANCHOR: ONE common date for every client in the Q2 rate comparison, unsubscribers and
# stayers alike. It has to be common, or the comparison isn't a comparison - two clients profiled
# at different UCP snapshots could differ on tenure/age/tibc for reasons that have nothing to do
# with unsubscribing. 2026-03-31 is also PRE-Q2 (Q2 unsub events start 2026-04-01), so every
# attribute pulled at this anchor is pre-treatment relative to the events being measured.
RATE_ANCHOR = min("2026-03-31", UCP_MAX)

_prof_dist = (cohort.groupBy("profile_anchor").agg(F.countDistinct("CLNT_NO").alias("clients"))
              .orderBy("profile_anchor").toPandas())
print("PROFILE_ANCHOR distribution (per-client, last-unsub month, clamped to", UCP_MAX, "):")
print(_prof_dist.to_string(index=False))
print("\nRATE_ANCHOR (single common snapshot for numerator + denominator):", RATE_ANCHOR)

# %% [5] UCP read loop
def read_ucp(month_ends, cols):
    frames = []
    for m in month_ends:
        path = UCP_BASE + "MONTH_END_DATE=" + m
        raw = spark.read.option("basePath", UCP_BASE).parquet(path)
        # filter on CLNT_TYP BEFORE selecting cols - UCP_COLS never includes CLNT_TYP (it isn't
        # in the requested 53-field list), so filtering after select() would hit an unresolved column
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


_profile_months = [r["profile_anchor"] for r in
                    cohort.select("profile_anchor").distinct().collect()]
_profile_months = sorted(str(m) for m in _profile_months)
print("reading UCP for", len(_profile_months), "PROFILE_ANCHOR month-ends...")
ucp_profile = read_ucp(_profile_months, UCP_COLS)
assert ucp_profile.count() > 0, "UCP profile-anchor read returned zero rows - investigate before proceeding"

print("\nreading UCP for RATE_ANCHOR (" + RATE_ANCHOR + ")...")
ucp_rate = read_ucp([RATE_ANCHOR], UCP_COLS)
assert ucp_rate.count() > 0, "UCP rate-anchor read returned zero rows - investigate before proceeding"

# %% [6] Join + fan-out guard (profile arm)
_before = cohort.count()
joined = cohort.join(
    ucp_profile, (cohort.CLNT_NO == ucp_profile.CLNT_NO) &
    (cohort.profile_anchor == ucp_profile.ucp_month_end),
    how="left"
).select(cohort["*"], *[ucp_profile[c].alias(c) for c in UCP_COLS if c != "CLNT_NO"],
          ucp_profile["ucp_month_end"])
_after = joined.count()

# If this assert trips, UCP is not unique per (CLNT_NO, MONTH_END_DATE) - uncomment the
# dedup fallback below (row_number over CLNT_NO+ucp_month_end, keep rn=1) and re-join.
# w = Window.partitionBy("CLNT_NO", "ucp_month_end").orderBy(F.lit(1))
# ucp_profile = (ucp_profile.withColumn("rn", F.row_number().over(w))
#                .filter("rn = 1").drop("rn"))
assert _before == _after, (
    "FAN-OUT: cohort " + str(_before) + " rows -> joined " + str(_after) +
    " rows after UCP join. UCP is not unique per (CLNT_NO, MONTH_END_DATE) - "
    "apply the commented dedup fallback above and re-run this cell.")
print("fan-out guard OK: cohort rows preserved through UCP join (", _before, "==", _after, ")")

joined = joined.withColumn("ucp_matched", F.col("AGE").isNotNull() if "AGE" in UCP_COLS
                            else F.col("ucp_month_end").isNotNull())
_matched = joined.filter(F.col("ucp_matched")).count()
print("MATCH RATE (profile arm):")
print(pd.DataFrame([{
    "cohort_clients": _before, "matched": _matched, "unmatched": _before - _matched,
    "match_pct": round(100.0 * _matched / _before, 1),
}]).to_string(index=False))

# %% [7] Banding - editable cut points in one place, applied via a shared function (cell [8] reuses it)
TIBC_BANDS = {0: "0", 1: "1", 2: "2", 3: "3"}   # anything above 3 falls to "4+" below
# NOTE: no standard TIBC/tenure/age band cut points are documented anywhere in this repo
# (same note as 15_unsub_value_enrichment.py cell [4]) - these are chosen here, adjustable.


def apply_bands(df):
    df = df.withColumn(
        "tibc_total",
        F.coalesce(F.col("T_TOT_CNT"), F.lit(0)) + F.coalesce(F.col("I_TOT_CNT"), F.lit(0)) +
        F.coalesce(F.col("B_TOT_CNT"), F.lit(0)) + F.coalesce(F.col("C_TOT_CNT"), F.lit(0)))
    df = df.withColumn(
        "tibc_band",
        F.when(F.col("tibc_total") == 0, TIBC_BANDS[0])
         .when(F.col("tibc_total") == 1, TIBC_BANDS[1])
         .when(F.col("tibc_total") == 2, TIBC_BANDS[2])
         .when(F.col("tibc_total") == 3, TIBC_BANDS[3])
         .otherwise("4+"))
    df = df.withColumn(
        "tenure_band",
        F.when(F.col("TENURE_RBC_YEARS").isNull(), "unknown")
         .when(F.col("TENURE_RBC_YEARS") < 1, "<1yr")
         .when(F.col("TENURE_RBC_YEARS") <= 3, "1-3yr")
         .when(F.col("TENURE_RBC_YEARS") <= 7, "4-7yr")
         .when(F.col("TENURE_RBC_YEARS") <= 15, "8-15yr")
         .otherwise("16yr+"))
    df = df.withColumn(
        "age_band",
        F.when(F.col("AGE").isNull(), "unknown")
         .when(F.col("AGE") < 25, "<25")
         .when(F.col("AGE") <= 34, "25-34")
         .when(F.col("AGE") <= 49, "35-49")
         .when(F.col("AGE") <= 64, "50-64")
         .otherwise("65+"))
    return df


joined = apply_bands(joined)
# house rule: cohort_month on every output, never pooled at extraction - pooling is a downstream pivot
joined = joined.withColumn("cohort_month", F.date_format("last_unsub_tm", "yyyy-MM"))

print("tibc_band distribution (profile arm):")
print(joined.groupBy("tibc_band").count().orderBy("tibc_band").toPandas().to_string(index=False))

# %% [8] DENOMINATOR - Q2 email base, both arms, banded identically to cell [7]
# q2_recipients is chunked as subpaths m04/m05/m06 - globbing "q2_recipients/*" loses which month
# a client was on, and the rate matrix carries cohort_month as a dimension, so the denominator has
# to carry it too. Read each subpath individually and stamp its own month explicitly.
_Q2_MONTH_SUBPATHS = [("m04", "2026-04"), ("m05", "2026-05"), ("m06", "2026-06")]


def read_q2_recipients(subdir):
    frames = []
    for sub, cohort_month in _Q2_MONTH_SUBPATHS:
        f = (spark.read.parquet(BASE + subdir + "/" + sub)
             .withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO")))
             .withColumn("cohort_month", F.lit(cohort_month))
             .dropDuplicates(["CLNT_NO"]))
        n = f.count()
        assert n > 0, subdir + "/" + sub + " landed zero rows - a subpath failed to land silently, fix before trusting any rate for " + cohort_month
        frames.append(f)
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out


# denominator grain is now CLIENT-MONTHS, not distinct clients: a client emailed in all three
# months legitimately appears three times, once per month. That is intended, not a fan-out bug -
# each cohort_month row in the rate matrix needs ITS OWN month's recipient base as the denominator.
q2_all = read_q2_recipients("q2_recipients")
q2_named = read_q2_recipients("q2_recipients_named")

_ucp_rate_narrow = ucp_rate.drop("ucp_month_end")

denom_all = apply_bands(
    q2_all.join(_ucp_rate_narrow, "CLNT_NO", "left")
).withColumn("rate_anchor", F.lit(RATE_ANCHOR))
denom_named = apply_bands(
    q2_named.join(_ucp_rate_narrow, "CLNT_NO", "left")
).withColumn("rate_anchor", F.lit(RATE_ANCHOR))

print("Q2 DENOMINATOR by month (emailed clients, RATE_ANCHOR =", RATE_ANCHOR, "):")
_denom_by_month = (q2_all.groupBy("cohort_month").agg(F.countDistinct("CLNT_NO").alias("emailed_clients_all"))
                    .join(q2_named.groupBy("cohort_month").agg(F.countDistinct("CLNT_NO").alias("emailed_clients_named")),
                          "cohort_month", "outer")
                    .orderBy("cohort_month"))
print(_denom_by_month.toPandas().to_string(index=False))
for _sub, _cm in _Q2_MONTH_SUBPATHS:
    assert q2_all.filter(F.col("cohort_month") == _cm).count() > 0, (
        "q2_recipients/" + _sub + " produced zero rows for " + _cm + " after banding - investigate before trusting that month's rate")

_n_all, _n_named = q2_all.count(), q2_named.count()
print("\nQ2 DENOMINATOR totals (client-month rows, NOT distinct clients - see comment above):")
print(pd.DataFrame([{"arm": "all_campaigns", "emailed_client_months": _n_all},
                     {"arm": "named_campaigns_only", "emailed_client_months": _n_named}]).to_string(index=False))

# %% [9] RATE MATRIX - the headline output. Q2 2026 unsub events only (Apr-Jun), matched MONTH FOR
# MONTH to the Q2 recipient denominator - each cohort_month row divides by THAT month's recipient
# base, never the pooled quarter (pooling three months into one denominator understates every
# monthly rate ~3x and triple-counts repeat recipients - see cell [8]). Uses q2_all (all campaigns)
# as the base; swap in denom_named if the named-campaigns-only question is the one being asked.
q2_unsub_events = (_raw.filter("unsub_tm >= DATE'2026-04-01' AND unsub_tm < DATE'2026-07-01'")
                    .withColumn("cohort_month", F.date_format("unsub_tm", "yyyy-MM")))
# cohort_month here = the month of the Q2 unsub EVENT, so it lines up with the denominator's
# per-month cohort_month (the month the client was emailed) - same key, same meaning, both sides.
q2_unsub_clients = (q2_unsub_events.groupBy("CLNT_NO", "cohort_month")
                     .agg(F.max("unsub_tm").alias("unsub_tm")).select("CLNT_NO", "cohort_month"))

_num_banded = apply_bands(
    q2_unsub_clients.join(_ucp_rate_narrow, "CLNT_NO", "left")
)

_denom_g = (denom_all.groupBy("tibc_band", "tenure_band", "age_band", "cohort_month")
            .agg(F.countDistinct("CLNT_NO").alias("emailed_clients")))
_num_g = (_num_banded.groupBy("tibc_band", "tenure_band", "age_band", "cohort_month")
          .agg(F.countDistinct("CLNT_NO").alias("unsub_clients")))

# full cross of every band combo x every Q2 month, THEN left-join denominator and numerator onto
# it - a plain join would silently drop band/month cells with zero emails or zero unsubs, which
# biases the "safest segment" read (cell [12] bottom-5) toward segments that merely had an event
_band_combos = _denom_g.select("tibc_band", "tenure_band", "age_band").distinct()
_q2_months = spark.createDataFrame([("2026-04",), ("2026-05",), ("2026-06",)], ["cohort_month"])
_full_grid = _band_combos.crossJoin(_q2_months)

rate_matrix = (_full_grid
               .join(_denom_g, ["tibc_band", "tenure_band", "age_band", "cohort_month"], "left")
               .join(_num_g, ["tibc_band", "tenure_band", "age_band", "cohort_month"], "left")
               .select("tibc_band", "tenure_band", "age_band", "cohort_month",
                        F.coalesce(F.col("emailed_clients"), F.lit(0)).alias("emailed_clients"),
                        F.coalesce(F.col("unsub_clients"), F.lit(0)).alias("unsub_clients")))

print("=" * 100)
print("RATE MATRIX Q2 2026 | numerator = distinct clients with >=1 unsub event IN THAT cohort_month")
print("                     | denominator = distinct clients SENT EMAIL IN THAT SAME cohort_month")
print("                       (any campaign), banded at RATE_ANCHOR =", RATE_ANCHOR, "(pre-Q2, common")
print("                       snapshot both arms) | grain = band x cohort_month (client-month, NOT")
print("                       distinct client across months - a client emailed in 2+ months of Q2")
print("                       contributes to 2+ denominator rows, one per month)")
print("                     | one row per (tibc_band, tenure_band, age_band, cohort_month)")
print("=" * 100)
_print_matrix = rate_matrix.withColumn(
    "unsub_rate_pct", F.round(100.0 * F.col("unsub_clients") / F.col("emailed_clients"), 3)
).orderBy(F.desc("unsub_rate_pct"))
print(_print_matrix.toPandas().to_string(index=False))
print("NOTE: unsub_rate_pct is printed for reading only - the saved table below is COUNTS ONLY.")

land_df("rate_matrix_q2", rate_matrix)

# %% [10] FULL-WINDOW PROFILE - Jul2025-Jun2026 volume at PROFILE_ANCHOR. NO denominator.
profile_main = (joined.groupBy("tibc_band", "tenure_band", "age_band", "cohort_month")
                 .agg(F.countDistinct("CLNT_NO").alias("unsub_clients")))

_mne_counts = joined.groupBy("last_mne").agg(F.countDistinct("CLNT_NO").alias("n")).toPandas()
_mne_keep = set(_mne_counts[_mne_counts["n"] >= 1000]["last_mne"].tolist())
print("MNEs with >= 1,000 first-unsubs (full window):", sorted(_mne_keep))

profile_mne = (joined.filter(F.col("last_mne").isin(list(_mne_keep)))
               .groupBy(F.col("last_mne").alias("mne"), "tibc_band", "cohort_month")
               .agg(F.countDistinct("CLNT_NO").alias("unsub_clients")))

print("=" * 100)
print("FULL-WINDOW PROFILE Jul2025-Jun2026 | VOLUME ONLY - this cut has NO emailed-base denominator.")
print("It describes the shape of the unsub cohort. It CANNOT support any 'who unsubscribes MORE'")
print("claim - for that, use the rate_matrix_q2 output from cell [9].")
print("=" * 100)
print("main cut (tibc_band x tenure_band x age_band x cohort_month), top 20 rows by volume:")
print(profile_main.orderBy(F.desc("unsub_clients")).limit(20).toPandas().to_string(index=False))
print("\nMNE cut (mne x tibc_band x cohort_month, MNEs with >=1,000 first-unsubs only), top 20 rows:")
print(profile_mne.orderBy(F.desc("unsub_clients")).limit(20).toPandas().to_string(index=False))

# union needs a common schema - main cut has no mne, mne cut has no tenure/age_band; fill both with
# NULL rather than dropping either, so the mne identity survives into the saved table.
_profile_main_out = (profile_main.withColumn("mne", F.lit(None).cast("string"))
                      .withColumn("cut", F.lit("main"))
                      .select("cut", "mne", "tibc_band", "tenure_band", "age_band", "cohort_month", "unsub_clients"))
_profile_mne_out = (profile_mne.withColumn("tenure_band", F.lit(None).cast("string"))
                    .withColumn("age_band", F.lit(None).cast("string"))
                    .withColumn("cut", F.lit("by_mne"))
                    .select("cut", "mne", "tibc_band", "tenure_band", "age_band", "cohort_month", "unsub_clients"))

land_df("profile_full_window", _profile_main_out.unionByName(_profile_mne_out))

# %% [11] PROF_TOT_ANNUAL vetting - investigation, not reporting
if "PROF_TOT_ANNUAL" in UCP_COLS:
    print("=" * 100)
    print("PROF_TOT_ANNUAL VETTING (matched clients only, profile arm) - this is checking whether the")
    print("field behaves like current-year contribution or a lifetime-value figure. It is NOT a")
    print("confirmed LTV proxy and must not be used to value an unsub until its definition is confirmed.")
    print("=" * 100)
    PCTS = [0.10, 0.25, 0.50, 0.75, 0.90]
    matched = joined.filter(F.col("ucp_matched"))
    rows = []
    for band in ["<1yr", "1-3yr", "4-7yr", "8-15yr", "16yr+", "unknown"]:
        sub = matched.filter(F.col("tenure_band") == band)
        n = sub.count()
        if n == 0:
            continue
        q = sub.approxQuantile("PROF_TOT_ANNUAL", PCTS, 0.01)
        rows.append({"tenure_band": band, "n": n, "p10": q[0], "p25": q[1], "p50": q[2], "p75": q[3], "p90": q[4]})
    print(pd.DataFrame(rows).to_string(index=False))
else:
    print("PROF_TOT_ANNUAL not present in schema (see cell [2]) - vetting skipped.")

# %% [12] ONE-SCREEN SUMMARY
print("=" * 100)
print("UNSUB VALUE / UCP SUMMARY")
print("=" * 100)
print("window: unsub events Jul 2025 - Jun 2026 (full-window profile); Apr-Jun 2026 (rate matrix)")
print("PROFILE_ANCHOR: per-client last-unsub month-end, clamped to UCP_MAX =", UCP_MAX)
print("RATE_ANCHOR: single common snapshot =", RATE_ANCHOR, "(pre-Q2, same for unsubscribers and stayers)")
print("cohort: ", _n_clients, "distinct clients, full window |", q2_unsub_clients.select("CLNT_NO").distinct().count(),
      "distinct clients, Q2 window")
print("match rate (profile arm, UCP at PROFILE_ANCHOR):", round(100.0 * _matched / _before, 1), "%")
print("Q2 emailed base (denominator, all campaigns):", _n_all, "| named campaigns only:", _n_named)

_top5 = _print_matrix.orderBy(F.desc("unsub_rate_pct")).limit(5).toPandas()
_bot5 = _print_matrix.filter(F.col("emailed_clients") > 0).orderBy(F.asc("unsub_rate_pct")).limit(5).toPandas()
print("\nTOP 5 cells by unsub rate (rate matrix, Q2):")
print(_top5.to_string(index=False))
print("\nBOTTOM 5 cells by unsub rate (rate matrix, Q2, emailed_clients > 0):")
print(_bot5.to_string(index=False))

print("\nCAVEATS:")
print("- profile_full_window has NO denominator - volume only, cannot support a rate claim")
print("- rate_matrix_q2 denominator is ALL Q2 email sends, not named-campaigns-only (see denom_named for that cut)")
print("- band '0' on tibc mixes genuinely-zero-holdings clients with unmatched (no UCP row) clients")
print("- PROF_TOT_ANNUAL is UNVETTED as a value proxy - see cell [11]")
print("- HAS_CLNT_TYP =", HAS_CLNT_TYP, "- if False, the Personal-client filter was NOT applied anywhere in this run")
print("- UCP uniqueness per (CLNT_NO, MONTH_END_DATE) is asserted by the cell [6] fan-out guard, not independently verified")

# OPEN QUESTIONS (unverified, flag before this ships anywhere):
# - PROF_TOT_ANNUAL definition: current-year contribution vs lifetime/projected value - unconfirmed (cell [11]).
# - CLNT_TYP presence: probed at runtime (cell [2]); if absent, HAS_CLNT_TYP=False and NO client-type
#   filter is applied anywhere downstream - the cohort may include non-Personal clients.
# - ucp4 uniqueness per (CLNT_NO, MONTH_END_DATE): asserted via the fan-out guard in cell [6];
#   if it trips, the commented dedup fallback needs to be uncommented and re-run.
# - The 53-field UCP spellings (AGE_RNG, GENERATION, TENURE_RBC_RNG, INCOME_AFTER_TAX_RNG,
#   CREDIT_SCORE_RNG, ACTV_PROD_CNT, OLB_ENROLLED_IND, MOBILE_AUTH_CNT) are taken from the field
#   catalog as given - cell [2]'s probe is the actual verification; trust its PRESENT/MISSING table.
# - No balance or average-$ field is confirmed to exist anywhere in UCP for this cohort - if a
#   dollar-value-of-an-unsub question comes up, that field does not yet exist in this pipeline.
