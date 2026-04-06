# %% [markdown]
# # VBA Vintage Dev Notebook
#
# Dev-mode version of vba_vintage_notebook.py.
# Queries the 3 source tables ONCE, caches to parquet, then all downstream
# logic runs in PySpark so you can iterate without re-querying.
#
# Cells 2-4: EDW queries (run once, ~40 min total — skipped on cache hit)
# Cells 5-6: Pure PySpark — reruns in seconds
# Cell 7:    Export to CSV

# %%
# Cell 0 — Imports and config

from pyspark.sql import functions as F, Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, DateType, ShortType
)
import os
import time

CACHE_DIR = './cache'
os.makedirs(CACHE_DIR, exist_ok=True)
print(f"Cache dir: {os.path.abspath(CACHE_DIR)}")

# %%
# Cell 1 — Helper functions


def edw_query(sql, spark, desc=""):
    """Run SQL via global EDW connection, return PySpark DataFrame."""
    t0 = time.time()
    if desc:
        print(f"  [{desc}] executing...", end=" ", flush=True)
    cursor = EDW.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()

    # Build schema from cursor.description (StringType fallback — safe for caching)
    fields = []
    for col_desc in cursor.description:
        col_name = col_desc[0]
        fields.append(StructField(col_name, StringType(), True))

    cursor.close()
    elapsed = time.time() - t0
    print(f"{len(rows):,} rows in {elapsed:.0f}s")

    schema = StructType(fields)
    return spark.createDataFrame(rows, schema=schema)


def load_or_query(name, sql, spark, desc=""):
    """Load from parquet cache if available, otherwise query EDW and cache."""
    path = f"{CACHE_DIR}/{name}.parquet"
    if os.path.exists(path):
        print(f"  [{desc or name}] Loaded from cache: {path}")
        return spark.read.parquet(path)
    df = edw_query(sql, spark, desc=desc)
    df.write.mode("overwrite").parquet(path)
    print(f"  [{desc or name}] Cached to: {path}")
    return df

# %%
# Cell 2 — Query: Population (VBA tactic events)

sql_population = """
SELECT
    CAST(tactic_id AS VARCHAR(50))           AS tactic_id,
    clnt_no,
    CAST(treatmt_strt_dt AS DATE)            AS Treat_Start_DT,
    CAST(
        COALESCE(treatmt_end_dt, treatmt_strt_dt) AS DATE
    )                                        AS Treat_End_DT,
    tst_grp_cd
FROM DG6V01.tactic_evnt_ip_ar_hist
WHERE treatmt_strt_dt >= DATE '2025-11-01'
  AND SUBSTR(tactic_id, 8, 3) = 'VBA'
  AND SUBSTR(tactic_id, 8, 1) <> 'J'
"""

df_pop = load_or_query('vba_population', sql_population, spark, 'Population')
print(f"\nPopulation rows: {df_pop.count():,}")
df_pop.show(5, truncate=False)

# %%
# Cell 3 — Query: Casper responses
#
# Full WITH clause included so the server-side join to population happens
# in the EDW. The vba CTE mirrors Cell 2 exactly.

sql_casper = """
WITH vba AS (
    SELECT DISTINCT
        CAST(E.tactic_id AS VARCHAR(50))              AS tactic_id,
        E.clnt_no,
        CAST(E.treatmt_strt_dt AS DATE)               AS Treat_Start_DT,
        CAST(
            COALESCE(E.treatmt_end_dt, E.treatmt_strt_dt) AS DATE
        )                                             AS Treat_End_DT,
        E.tst_grp_cd
    FROM DG6V01.tactic_evnt_ip_ar_hist E
    WHERE E.treatmt_strt_dt >= DATE '2025-11-01'
      AND SUBSTR(E.tactic_id, 8, 3) = 'VBA'
      AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
),
casper_events AS (
    SELECT
        v.tactic_id,
        v.clnt_no,
        v.Treat_Start_DT,
        v.Treat_End_DT,
        v.tst_grp_cd,
        CASE WHEN p.Status = 'A' THEN p.acct_no END      AS visa_acct_no,
        CAST(p.app_rcv_dt AS DATE)                        AS visa_response_dt,
        CASE WHEN p.Status = 'A' THEN 1 ELSE 0 END       AS visa_app_approved,
        'Casper'                                          AS response_source,
        ROW_NUMBER() OVER (
            PARTITION BY v.clnt_no, p.acct_no
            ORDER BY
                p.Cell_Code DESC NULLS LAST,
                CASE WHEN p.Status = 'A' THEN p.cr_lmt_off ELSE NULL END DESC NULLS LAST,
                p.app_rcv_dt
        ) AS row_num
    FROM vba v
    INNER JOIN D3CV12A.appl_fact_dly p
        ON v.clnt_no = p.bus_clnt_no
    WHERE p.app_rcv_dt BETWEEN v.Treat_Start_DT AND v.Treat_End_DT
      AND p.status = 'A'
      AND p.SRC_TSYS_ACCT_IND = 'Y'
      AND p.PROD_APPRVD IN ('B', 'E')
      AND (p.Cell_Code IS NULL OR p.Cell_Code <> 'PATACT')
      AND p.CR_LMT_CHG_IND = 'N'
      AND p.visa_prod_cd NOT IN ('CCL', 'BXX')
      AND p.Cell_Code NOT IN ('GV0320')
)
SELECT tactic_id, clnt_no, Treat_Start_DT, Treat_End_DT, tst_grp_cd,
       visa_acct_no, visa_response_dt, visa_app_approved, response_source
FROM casper_events
WHERE row_num = 1
"""

df_casper = load_or_query('casper_responses', sql_casper, spark, 'Casper')
print(f"\nCasper rows: {df_casper.count():,}")
df_casper.show(5, truncate=False)

# %%
# Cell 4 — Query: SCOT responses
#
# Full WITH clause included. vba CTE mirrors Cell 2 exactly.
# GROUP BY happens server-side to match production query exactly.

# Read SCOT from HDFS (parquet) — only the 4 columns we need from the 971-column table
SCOT_HDFS = '/prod/sz/tsz/00222/data/CREDIT_APPLICATION_SNAPSHOT'
scot_cache = f"{CACHE_DIR}/scot_responses.parquet"

if os.path.exists(scot_cache):
    print(f"  [SCOT] Loaded from cache: {scot_cache}")
    df_scot = spark.read.parquet(scot_cache)
else:
    print(f"  [SCOT] Reading from HDFS: {SCOT_HDFS}")
    t0 = time.time()

    # Read only the columns we need
    tsys_col = 'creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid'

    df_scot_raw = (
        spark.read.parquet(SCOT_HDFS)
        .select(
            F.col('creditapplication_borrowers_borrowersrfnumber').cast('int').alias('clnt_no'),
            F.col(tsys_col).alias('tsys_acct_id_raw'),
            F.col('creditapplication_createddatetime').cast('date').alias('visa_response_dt'),
            F.col('creditapplication_creditapplicationstatuscode').alias('statuscode'),
            F.col('creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory').alias('productcategory')
        )
        .filter(F.col('productcategory') == 'CREDIT_CARD')
        .filter(F.col('statuscode') == 'FULFILLED')
        .drop('productcategory', 'statuscode')
    )

    # Aggregate by clnt_no ONLY (matching reference: GROUP BY 1, pre-filtered to FULFILLED)
    # visa_acct_no: MAX with NULL handling (CASE WHEN IS NULL THEN NULL ELSE CAST AS INT)
    df_scot_agg = (
        df_scot_raw
        .groupBy('clnt_no')
        .agg(
            F.max(
                F.when(F.col('tsys_acct_id_raw').isNotNull(), F.col('tsys_acct_id_raw').cast('int'))
            ).alias('visa_acct_no'),
            F.min('visa_response_dt').alias('visa_response_dt')
        )
        .withColumn('visa_app_approved', F.lit(1))
    )

    # Join to population AFTER aggregation (matching reference: scot_apps_raw → scot_apps)
    df_scot = (
        df_scot_agg
        .join(df_pop, on='clnt_no', how='inner')
        .filter(
            (F.col('visa_response_dt') >= F.col('Treat_Start_DT'))
            & (F.col('visa_response_dt') <= F.col('Treat_End_DT'))
        )
        .withColumn('response_source', F.lit('Scott'))
    )

    df_scot.write.mode('overwrite').parquet(scot_cache)
    elapsed = time.time() - t0
    print(f"  [SCOT] {df_scot.count():,} rows in {elapsed:.0f}s — cached to: {scot_cache}")

print(f"\nSCOT rows: {df_scot.count():,}")
df_scot.show(5, truncate=False)

# %%
# Cell 5 — VBA Summary (pure PySpark)
#
# Union casper + scot, deduplicate to one approved row per (tactic_id, clnt_no),
# left join to population, aggregate.

# --- 5a: Union responses ---
df_responses = df_casper.unionByName(df_scot)

# Sanity check: confirm both sources are present
print("Response source counts:")
df_responses.groupBy('response_source').count().show()

# --- 5b: Deduplicate — for approved clients, keep earliest response ---
w_dedup = Window.partitionBy('tactic_id', 'clnt_no').orderBy(F.col('visa_response_dt').asc())

df_approved = (
    df_responses
    .filter(F.col('visa_app_approved') == 1)
    .withColumn('rn', F.row_number().over(w_dedup))
    .filter(F.col('rn') == 1)
    .drop('rn')
)

print(f"\nDeduped approved rows: {df_approved.count():,}")
df_approved.show(5, truncate=False)

# --- 5c: Left join population to deduped successes (matches production SQL) ---
#   Production joins vba → success (earliest approved per client, one row max).
#   This ensures a client approved in BOTH sources is attributed to the earliest only.
df_summary = (
    df_pop
    .join(df_approved.select('tactic_id', 'clnt_no', 'visa_app_approved', 'response_source'),
          on=['tactic_id', 'clnt_no'], how='left')
    .groupBy('tactic_id', 'tst_grp_cd')
    .agg(
        F.countDistinct('clnt_no').alias('leads'),
        F.countDistinct(
            F.when(F.col('visa_app_approved') == 1, F.col('clnt_no'))
        ).alias('successes_any'),
        F.countDistinct(
            F.when((F.col('visa_app_approved') == 1) & (F.col('response_source') == 'Casper'), F.col('clnt_no'))
        ).alias('successes_casper'),
        F.countDistinct(
            F.when((F.col('visa_app_approved') == 1) & (F.col('response_source') == 'Scott'), F.col('clnt_no'))
        ).alias('successes_scott'),
    )
    .withColumn('rate_any',    F.round(F.col('successes_any')    * 100.0 / F.col('leads'), 2))
    .withColumn('rate_casper', F.round(F.col('successes_casper') * 100.0 / F.col('leads'), 2))
    .withColumn('rate_scott',  F.round(F.col('successes_scott')  * 100.0 / F.col('leads'), 2))
    .orderBy('tactic_id', 'tst_grp_cd')
)

print(f"\nSummary rows: {df_summary.count():,}")
df_summary.show(50, truncate=False)

# %%
# Cell 6 — Vintage Curves 0-90 days (pure PySpark)

# --- 6a: Casper — earliest approved per (clnt_no, tactic_id) ---
df_casper_approved = (
    df_casper
    .filter(F.col('visa_app_approved') == 1)
    .groupBy('clnt_no', 'tactic_id', 'Treat_Start_DT', 'Treat_End_DT', 'tst_grp_cd')
    .agg(F.min('visa_response_dt').alias('first_response_dt'))
    .withColumn('mne', F.substring('tactic_id', 8, 3))
    .withColumn(
        'vintage',
        F.greatest(F.lit(0), F.datediff(F.col('first_response_dt'), F.col('Treat_Start_DT')))
    )
    .filter((F.col('vintage') >= 0) & (F.col('vintage') <= 90))
)

print(f"Casper approved vintage rows (0-90d): {df_casper_approved.count():,}")

# --- 6b: SCOT — earliest approved per (clnt_no, tactic_id) ---
df_scot_approved = (
    df_scot
    .filter(F.col('visa_app_approved') == 1)
    .groupBy('clnt_no', 'tactic_id', 'Treat_Start_DT', 'Treat_End_DT', 'tst_grp_cd')
    .agg(F.min('visa_response_dt').alias('first_response_dt'))
    .withColumn('mne', F.substring('tactic_id', 8, 3))
    .withColumn(
        'vintage',
        F.greatest(F.lit(0), F.datediff(F.col('first_response_dt'), F.col('Treat_Start_DT')))
    )
    .filter((F.col('vintage') >= 0) & (F.col('vintage') <= 90))
)

print(f"SCOT approved vintage rows (0-90d): {df_scot_approved.count():,}")

# --- 6b2: Any — deduped across sources, earliest approved per client ---
df_any_approved = (
    df_casper_approved.select('clnt_no', 'tactic_id', 'Treat_Start_DT', 'Treat_End_DT', 'tst_grp_cd', 'first_response_dt', 'mne', 'vintage')
    .unionByName(
        df_scot_approved.select('clnt_no', 'tactic_id', 'Treat_Start_DT', 'Treat_End_DT', 'tst_grp_cd', 'first_response_dt', 'mne', 'vintage')
    )
)

# Keep earliest per client — recompute vintage from the earliest across both sources
w_any = Window.partitionBy('tactic_id', 'clnt_no').orderBy('first_response_dt')
df_any_approved = (
    df_any_approved
    .withColumn('rn', F.row_number().over(w_any))
    .filter(F.col('rn') == 1)
    .drop('rn')
    .withColumn(
        'vintage',
        F.greatest(F.lit(0), F.datediff(F.col('first_response_dt'), F.col('Treat_Start_DT')))
    )
    .filter((F.col('vintage') >= 0) & (F.col('vintage') <= 90))
)

print(f"Any (deduped) approved vintage rows (0-90d): {df_any_approved.count():,}")

# --- 6c: Cohort — leads per (mne, tst_grp_cd, Treat_Start_DT, Treat_End_DT) ---
df_cohort = (
    df_pop
    .withColumn('mne', F.substring('tactic_id', 8, 3))
    .groupBy('mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT')
    .agg(F.countDistinct('clnt_no').alias('leads'))
)

print(f"\nCohort rows: {df_cohort.count():,}")
df_cohort.show(20, truncate=False)

# --- 6d: Scaffold — cross join cohort with vintage days 0-90 ---
df_days = spark.range(0, 91).withColumnRenamed('id', 'vintage')

df_scaffold = df_cohort.crossJoin(df_days)
print(f"\nScaffold rows (should be cohort rows * 91): {df_scaffold.count():,}")

# --- 6e: Daily success counts — per source + deduped any ---
df_success_any = (
    df_any_approved
    .groupBy('mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT', 'vintage')
    .agg(F.countDistinct('clnt_no').alias('success_daily_any'))
)

df_success_primary = (
    df_casper_approved
    .groupBy('mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT', 'vintage')
    .agg(F.countDistinct('clnt_no').alias('success_daily_primary'))
)

df_success_secondary = (
    df_scot_approved
    .groupBy('mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT', 'vintage')
    .agg(F.countDistinct('clnt_no').alias('success_daily_secondary'))
)

# --- 6f: Join daily successes onto scaffold ---
join_keys = ['mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT', 'vintage']

df_scaffold_joined = (
    df_scaffold
    .join(df_success_any,       on=join_keys, how='left')
    .join(df_success_primary,   on=join_keys, how='left')
    .join(df_success_secondary, on=join_keys, how='left')
    .fillna({'success_daily_any': 0, 'success_daily_primary': 0, 'success_daily_secondary': 0})
)

# --- 6g: Cumulative sums via Window ---
w_vintage = (
    Window
    .partitionBy('mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT')
    .orderBy('vintage')
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

df_vintage = (
    df_scaffold_joined
    .withColumn('success_cum_any',       F.sum('success_daily_any').over(w_vintage))
    .withColumn('success_cum_primary',   F.sum('success_daily_primary').over(w_vintage))
    .withColumn('success_cum_secondary', F.sum('success_daily_secondary').over(w_vintage))
    .orderBy('mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT', 'vintage')
)

print(f"\nVintage curve rows: {df_vintage.count():,}")
df_vintage.show(20, truncate=False)

# %%
# Cell 7 — Export to CSV

df_summary.toPandas().to_csv('vba_vintage_summary.csv', index=False)
print(f"Summary exported: vba_vintage_summary.csv ({df_summary.count()} rows)")

df_vintage.toPandas().to_csv('vba_vintage_curves.csv', index=False)
print(f"Vintage curves exported: vba_vintage_curves.csv ({df_vintage.count()} rows)")
