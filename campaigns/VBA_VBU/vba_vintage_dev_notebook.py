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
)
SELECT
    v.tactic_id,
    v.clnt_no,
    v.Treat_Start_DT,
    v.Treat_End_DT,
    v.tst_grp_cd,
    CASE WHEN p.Status = 'A' THEN p.acct_no END      AS visa_acct_no,
    CAST(p.app_rcv_dt AS DATE)                        AS visa_response_dt,
    CASE WHEN p.Status = 'A' THEN 1 ELSE 0 END        AS visa_app_approved,
    'Casper'                                           AS response_source
FROM vba v
INNER JOIN D3CV12A.appl_fact_dly p
    ON v.clnt_no = p.bus_clnt_no
WHERE p.app_rcv_dt BETWEEN v.Treat_Start_DT AND v.Treat_End_DT
  AND p.Status IN ('A','D','O')
  AND p.PROD_APPRVD IN ('B','E')
  AND (p.Cell_Code IS NULL OR p.Cell_Code NOT IN ('PATACT','GV0320'))
  AND p.CR_LMT_CHG_IND = 'N'
  AND p.visa_prod_cd NOT IN ('CCL','BXX')
"""

df_casper = load_or_query('casper_responses', sql_casper, spark, 'Casper')
print(f"\nCasper rows: {df_casper.count():,}")
df_casper.show(5, truncate=False)

# %%
# Cell 4 — Query: SCOT responses
#
# Full WITH clause included. vba CTE mirrors Cell 2 exactly.
# GROUP BY happens server-side to match production query exactly.

sql_scot = """
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
)
SELECT
    v.tactic_id,
    v.clnt_no,
    v.Treat_Start_DT,
    v.Treat_End_DT,
    v.tst_grp_cd,
    TRY_CAST(
        s.creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid
        AS BIGINT
    )                                                  AS visa_acct_no,
    MAX(CASE
        WHEN s.creditapplication_creditapplicationstatuscode = 'FULFILLED' THEN 1 ELSE 0
    END)                                               AS visa_app_approved,
    CAST(MIN(s.creditapplication_createddatetime) AS DATE) AS visa_response_dt,
    'Scott'                                            AS response_source
FROM vba v
INNER JOIN edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot s
    ON v.clnt_no = CAST(s.creditapplication_borrowers_borrowersrfnumber AS INTEGER)
   AND CAST(s.creditapplication_createddatetime AS DATE) BETWEEN v.Treat_Start_DT AND v.Treat_End_DT
WHERE s.creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory = 'CREDIT_CARD'
GROUP BY
    v.tactic_id,
    v.clnt_no,
    v.Treat_Start_DT,
    v.Treat_End_DT,
    v.tst_grp_cd,
    TRY_CAST(
        s.creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid
        AS BIGINT
    )
"""

df_scot = load_or_query('scot_responses', sql_scot, spark, 'SCOT')
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

# --- 6a: Union Casper + SCOT, deduplicate to earliest approved per client ---
#   Same logic as the summary: one client = one source = earliest approved response.
df_all_approved = (
    df_responses
    .filter(F.col('visa_app_approved') == 1)
)

w_dedup_vintage = Window.partitionBy('tactic_id', 'clnt_no').orderBy(F.col('visa_response_dt').asc())

df_earliest = (
    df_all_approved
    .withColumn('rn', F.row_number().over(w_dedup_vintage))
    .filter(F.col('rn') == 1)
    .drop('rn')
)

# --- 6b: Compute vintage day per client ---
df_vintages = (
    df_earliest
    .withColumn('mne', F.substring('tactic_id', 8, 3))
    .withColumn(
        'vintage',
        F.greatest(
            F.lit(0),
            F.datediff(F.col('visa_response_dt'), F.col('Treat_Start_DT'))
        )
    )
    .filter((F.col('vintage') >= 0) & (F.col('vintage') <= 90))
)

print(f"Deduped approved vintage rows (0-90d): {df_vintages.count():,}")

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

# --- 6e: Daily success counts ---
df_success_daily = (
    df_vintages
    .groupBy('mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT', 'vintage')
    .agg(F.countDistinct('clnt_no').alias('success_daily'))
)

# --- 6f: Join daily successes onto scaffold ---
join_keys = ['mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT', 'vintage']

df_scaffold_joined = (
    df_scaffold
    .join(df_success_daily, on=join_keys, how='left')
    .fillna({'success_daily': 0})
)

# --- 6g: Cumulative sum via Window ---
w_vintage = (
    Window
    .partitionBy('mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT')
    .orderBy('vintage')
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

df_vintage = (
    df_scaffold_joined
    .withColumn('success_cum', F.sum('success_daily').over(w_vintage))
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
