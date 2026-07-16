# %% [0] Config — paths, tracked-MNE list, EDW helper, env patches
# Enriches the unsub value spine (13_unsub_value_spine.sql, S1) with UCP attributes
# at the month-end BEFORE each client's first unsub, then builds the TIBC x age
# segment matrix by triggering MNE. Env: Lumina/AI Farm YARN-Spark — `spark` and
# `EDW` are pre-initialized (no builder/stop, no teradatasql import).

import pandas as pd
import time
from pyspark.sql import functions as F, Window

# pandas 2.0 removed DataFrame.iteritems(); spark.createDataFrame(pandas_df) still
# calls it internally on this Spark version — alias it back or the conversion fails.
if not hasattr(pd.DataFrame, "iteritems"):
    pd.DataFrame.iteritems = pd.DataFrame.items

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"
HDFS_OUT = "/user/427966379/unsub_value/enriched_spine"

UCP_COLS = ["CLNT_NO", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT",
            "AGE", "AGE_RNG", "TENURE_RBC_YEARS", "PROF_TOT_ANNUAL",
            "PROF_SEG_CD", "CLNT_TYP"]

TRACKED_MNES = ['PCQ', 'PCL', 'PCD', 'AUH', 'CLI', 'MVP', 'CRV', 'CTU', 'O2P',
                'VDT', 'VUI', 'VUT', 'VDA', 'VAW', 'VCN', 'RCU', 'RCL']

MIN_MNE_ROWS = 1000   # matrix printed per-MNE only above this row count

# local FS is NOT writable from the Spark kernel on AI Farm — every join disables
# auto-broadcast by default; cell [3] re-enables it explicitly per-partition when
# the client subset is small enough to be safe.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


def edw_query(sql, desc=""):
    """Run SQL via the pre-initialized EDW cursor, return a pandas DataFrame."""
    t0 = time.time()
    if desc:
        print(f"  [{desc}] executing...", end=" ", flush=True)
    cur = EDW.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    cur.close()
    print(f"{len(rows):,} rows in {time.time() - t0:.0f}s")
    return pd.DataFrame(rows, columns=cols)


print(f"Config loaded. UCP_BASE={UCP_BASE}  tracked MNEs={len(TRACKED_MNES)}")


# %% [1] Pull the unsub value spine (S1, embedded verbatim from 13_unsub_value_spine.sql)

SPINE_SQL = """
WITH first_unsub AS (
    SELECT
        m.CLNT_NO,
        e.consumer_id_hashed,
        e.TREATMENT_ID,
        e.disposition_dt_tm,
        ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO
                           ORDER BY e.disposition_dt_tm ASC) AS rn
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'
)
SELECT
    CLNT_NO,
    consumer_id_hashed,
    TREATMENT_ID               AS trigger_treatment_id,
    SUBSTR(TREATMENT_ID, 8, 3) AS trigger_mne,
    disposition_dt_tm          AS first_unsub_tm,
    EXTRACT(YEAR FROM disposition_dt_tm) * 100
      + EXTRACT(MONTH FROM disposition_dt_tm) AS unsub_month_yyyymm
FROM first_unsub
WHERE rn = 1
"""

spine_pd = edw_query(SPINE_SQL, "unsub spine S1")
spine_pd.columns = [c.lower() for c in spine_pd.columns]

# EDW returns clnt_no as INTEGER; HDFS/UCP key is a leading-zero-stripped string.
# int -> str gives the stripped form directly (no leading zeros to begin with).
spine_pd["clnt_no"] = spine_pd["clnt_no"].astype("Int64").astype(str)

n_rows = len(spine_pd)
n_clients = spine_pd["clnt_no"].nunique()
mo_min, mo_max = spine_pd["unsub_month_yyyymm"].min(), spine_pd["unsub_month_yyyymm"].max()
print(f"Spine: {n_rows:,} rows, {n_clients:,} distinct clients "
      f"(expected 1:1 — one row per client x first unsub)")
print(f"Unsub months span: {mo_min} to {mo_max}")
if n_rows != n_clients:
    print(f"WARNING: {n_rows - n_clients:,} rows are not 1-per-client — check rn=1 filter upstream.")

spine_spark = spark.createDataFrame(spine_pd)


# %% [2] Derive the UCP snapshot month per client (last month-end BEFORE first_unsub_tm)

# month_end_before = last day of the month PRIOR to the unsub month.
# ucp_ceiling = last day of the most recent CLOSED month (today's month has no UCP partition yet).
# Clamp to whichever is earlier so we never request a partition that doesn't exist.
month_end_before = F.date_sub(F.trunc(F.col("first_unsub_tm"), "month"), 1)
ucp_ceiling = F.last_day(F.add_months(F.current_date(), -1))

spine_spark = spine_spark.withColumn("ucp_month_end", F.least(month_end_before, ucp_ceiling))

partitions_pd = (spine_spark.groupBy("ucp_month_end")
                  .agg(F.countDistinct("clnt_no").alias("n_clients"))
                  .orderBy(F.desc("n_clients"))
                  .toPandas())

print(f"Distinct UCP partitions needed: {len(partitions_pd)}")
print(sorted(partitions_pd["ucp_month_end"].astype(str).tolist()))
print("\nTop 10 partitions by client count:")
print(partitions_pd.head(10).to_string(index=False))


# %% [3] Loop UCP partitions, join to spine, dedup, LEFT join, fan-out guard

before_count = spine_spark.count()

ucp_frames = []
for _, prow in partitions_pd.iterrows():
    me = pd.Timestamp(prow["ucp_month_end"]).strftime("%Y-%m-%d")
    n_month_clients = int(prow["n_clients"])
    clients_this_month = (spine_spark
                           .filter(F.col("ucp_month_end") == me)
                           .select("clnt_no").distinct())

    ucp_path = f"{UCP_BASE}MONTH_END_DATE={me}"
    try:
        ucp_raw = (spark.read.option("basePath", UCP_BASE).parquet(ucp_path)
                   .filter(F.trim(F.col("CLNT_TYP")) == "Personal"))
    except Exception as e:
        print(f"  WARNING: partition {me} unreadable ({e}) — {n_month_clients:,} clients skipped")
        continue

    available = [c for c in UCP_COLS if c in ucp_raw.columns]
    missing = [c for c in UCP_COLS if c not in ucp_raw.columns]
    if missing:
        print(f"  WARNING: partition {me} missing columns {missing}")

    ucp_sel = ucp_raw.select(*available)
    ucp_sel = ucp_sel.toDF(*[c.lower() for c in ucp_sel.columns])
    # align join key: strip leading zeros / trim, same normalization as the EDW side
    ucp_sel = ucp_sel.withColumn("clnt_no", F.regexp_replace(F.trim(F.col("clnt_no").cast("string")), "^0+", ""))
    ucp_sel = ucp_sel.withColumn("month_end", F.lit(me).cast("date"))

    if n_month_clients < 10000:
        joined_month = ucp_sel.join(F.broadcast(clients_this_month), on="clnt_no", how="inner")
    else:
        joined_month = ucp_sel.join(clients_this_month, on="clnt_no", how="inner")

    ucp_frames.append(joined_month)

if not ucp_frames:
    raise RuntimeError("No UCP partitions were readable — cannot proceed with enrichment.")

ucp_all = ucp_frames[0]
for f in ucp_frames[1:]:
    ucp_all = ucp_all.unionByName(f)

# defensive dedup: one row per (clnt_no, month_end) even though UCP should already be unique
w = Window.partitionBy("clnt_no", "month_end").orderBy(F.lit(1))
ucp_dedup = (ucp_all.withColumn("rn", F.row_number().over(w))
             .filter(F.col("rn") == 1).drop("rn"))

s = spine_spark.alias("s")
u = ucp_dedup.alias("u")
ucp_value_cols = [c for c in ucp_dedup.columns if c not in ("clnt_no", "month_end")]

enriched = (s.join(u, (F.col("s.clnt_no") == F.col("u.clnt_no")) &
                       (F.col("s.ucp_month_end") == F.col("u.month_end")), how="left")
             .select("s.*", *[F.col(f"u.{c}").alias(c) for c in ucp_value_cols]))

after_count = enriched.count()
assert before_count == after_count, (
    f"FAN-OUT: spine {before_count:,} rows -> enriched {after_count:,} rows after UCP join. "
    f"UCP dedup did not fully collapse duplicate keys — investigate before proceeding.")
print(f"Fan-out guard OK: spine rows preserved through UCP join ({before_count:,} == {after_count:,})")

enriched = enriched.withColumn("ucp_matched", F.col("clnt_typ").isNotNull())
matched = enriched.filter(F.col("ucp_matched")).count()
match_pct = matched / before_count * 100
print(f"{matched:,} of {before_count:,} spine clients matched UCP = {match_pct:.1f}%")
if match_pct < 90:
    print(f"WARNING: match rate {match_pct:.1f}% is below the 90% threshold — investigate before trusting the matrix.")


# %% [4] Segment bands — tibc_band and age_band

# NOTE: nulls -> 0 per spec means an unmatched client (no UCP row at all) and a
# matched client with genuinely zero TIBC products both land in tibc_band '0'.
# Cell [3]'s match rate is the caveat for how much of band '0' is "no data" vs "zero holdings".
enriched = enriched.withColumn(
    "tibc_total",
    F.coalesce(F.col("t_tot_cnt"), F.lit(0)) + F.coalesce(F.col("i_tot_cnt"), F.lit(0)) +
    F.coalesce(F.col("b_tot_cnt"), F.lit(0)) + F.coalesce(F.col("c_tot_cnt"), F.lit(0))
)

enriched = enriched.withColumn(
    "tibc_band",
    F.when(F.col("tibc_total") == 0, "0")
     .when(F.col("tibc_total") == 1, "1")
     .when(F.col("tibc_total") == 2, "2")
     .when(F.col("tibc_total") == 3, "3")
     .otherwise("4+")
)

# age is NULL for unmatched clients too -> falls into 'unknown', consistent with spec.
enriched = enriched.withColumn(
    "age_band",
    F.when(F.col("age").isNull(), "unknown")
     .when(F.col("age") < 25, "<25")
     .when(F.col("age") <= 34, "25-34")
     .when(F.col("age") <= 49, "35-49")
     .when(F.col("age") <= 64, "50-64")
     .otherwise("65+")
)

print("tibc_band distribution:")
print(enriched.groupBy("tibc_band").count().orderBy("tibc_band").toPandas().to_string(index=False))
print("\nage_band distribution:")
print(enriched.groupBy("age_band").count().orderBy("age_band").toPandas().to_string(index=False))


# %% [5] Save enriched spine to HDFS

enriched.write.mode("overwrite").parquet(HDFS_OUT)
n_saved = enriched.count()
print(f"Saved: {HDFS_OUT}")
print(f"Rows: {n_saved:,}")


# %% [6] THE SEGMENT MATRIX — age_band x tibc_band, overall and per tracked MNE

AGE_ORDER = ["<25", "25-34", "35-49", "50-64", "65+", "unknown"]

overall_pd = enriched.groupBy("age_band", "tibc_band").count().toPandas()
overall_piv = overall_pd.pivot_table(index="age_band", columns="tibc_band",
                                      values="count", fill_value=0, aggfunc="sum")
overall_piv = overall_piv.reindex(AGE_ORDER)
print(f"OVERALL — age_band x tibc_band ({n_saved:,} first-unsubs, counts only):")
print(overall_piv.to_string())

mne_counts = (enriched.groupBy("trigger_mne").count().toPandas()
              .set_index("trigger_mne")["count"].to_dict())

print(f"\nPer tracked MNE (matrix shown only for >= {MIN_MNE_ROWS:,} first-unsubs):")
for mne in TRACKED_MNES:
    n = mne_counts.get(mne, 0)
    if n >= MIN_MNE_ROWS:
        sub_pd = (enriched.filter(F.col("trigger_mne") == mne)
                  .groupBy("age_band", "tibc_band").count().toPandas())
        piv = sub_pd.pivot_table(index="age_band", columns="tibc_band",
                                  values="count", fill_value=0, aggfunc="sum").reindex(AGE_ORDER)
        print(f"\n{mne} — age_band x tibc_band ({n:,} first-unsubs):")
        print(piv.to_string())
    else:
        print(f"{mne}: {n:,} first-unsubs (below {MIN_MNE_ROWS:,} threshold — not shown as matrix)")


# %% [7] Value-fields preview — PROF_TOT_ANNUAL and TENURE_RBC_YEARS by age_band

# Vetting the profitability field, not reporting it: PROF_TOT_ANNUAL, if it is a
# current-year contribution figure (not lifetime/projected), will understate young
# clients who are early in their RBC relationship. Confirm the field's definition
# before using it downstream as an LTV proxy.
PCTS = [0.10, 0.25, 0.50, 0.75, 0.90]
matched_df = enriched.filter(F.col("ucp_matched"))

for field in ["prof_tot_annual", "tenure_rbc_years"]:
    rows = []
    for band in AGE_ORDER:
        sub = matched_df.filter(F.col("age_band") == band)
        n = sub.count()
        if n == 0:
            continue
        q = sub.approxQuantile(field, PCTS, 0.01)
        rows.append({"age_band": band, "n": n, "p10": q[0], "p25": q[1],
                     "p50": q[2], "p75": q[3], "p90": q[4]})
    print(f"\n{field} percentiles by age_band (matched clients only):")
    print(pd.DataFrame(rows).to_string(index=False))
