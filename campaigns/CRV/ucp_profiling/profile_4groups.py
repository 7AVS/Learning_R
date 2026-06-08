# CRV × PLI — UCP 4-group profiling
# Compares four PLI mobile-banner lead segments on UCP demographic/holding fields.
# Runs on Lumina YARN-Spark kernel. spark + EDW are pre-initialized.

import pandas as pd
import time
from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim

# pandas 2.0 removed DataFrame.iteritems(); the AI Farm PySpark still calls it inside
# spark.createDataFrame(pandas_df). Alias it back so pandas->Spark conversions work.
if not hasattr(pd.DataFrame, "iteritems"):
    pd.DataFrame.iteritems = pd.DataFrame.items

# OUTPUT NOTE (AI Farm / YARN-Spark): the local Jupyter FS is NOT writable from the
# Spark kernel, so pandas .to_csv() to a local path FAILS. ALL saves go through Spark
# to HDFS. HDFS_BASE = Andre's HDFS user folder — CONFIRM the ID matches your config
# (it's the "/user/<id>" part of the HDFS path used in the existing UCP/EDW notebook).
HDFS_BASE = "/user/427966379/ucp_profiling"

UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"

# ── HDFS WRITE TEST — RUN THIS FIRST ─────────────────────────────────────────
# Confirms Spark can write to + read back from HDFS before running the full job.
# If it ERRORS, HDFS save isn't available -> we switch the final output to an
# in-notebook download link instead (no disk save at all).
def test_hdfs_write():
    test_path = f"{HDFS_BASE}/_write_test"
    (spark.createDataFrame([(1, "ok")], ["id", "status"])
        .coalesce(1).write.mode("overwrite").option("header", True).csv(test_path))
    back = spark.read.option("header", True).csv(test_path).collect()
    print(f"HDFS write/read OK at {test_path} -> {back}")

test_hdfs_write()

# ── EDITABLE FIELD LISTS ─────────────────────────────────────────────────────
# Confirmed against UCP reference documentation and memory notes.
# Fields marked [UNCONFIRMED] exist in the UCP4 personal schema by convention
# but were not explicitly enumerated in the cards-project schema docs —
# verify on first run with a SHOW COLUMNS / df.columns check.

NUMERIC_FIELDS = [
    "AGE",               # confirmed (UCP enrichment reference)
    "TENURE_RBC_YEARS",  # confirmed (VBA UCP field list)
    "PROF_TOT_ANNUAL",   # [UNCONFIRMED] — profession/income total annual; seen in VVD context but not in cards UCP doc
    "ACTV_PROD_CNT",     # confirmed (PCL segmentation overlay, UCP reference)
    "T_TOT_CNT",         # confirmed (UCP product-category reference: Transactional products)
    "I_TOT_CNT",         # confirmed (UCP product-category reference: Investment products)
    "B_TOT_CNT",         # confirmed (UCP product-category reference: Borrower products)
    "C_TOT_CNT",         # confirmed (UCP product-category reference: Cards products)
    "MOBILE_AUTH_CNT",   # confirmed (VBA UCP field list)
    "OFI_T_PROD_CNT",    # confirmed (UCP OFI reference: Transactional at OFI)
    "OFI_C_PROD_CNT",    # confirmed (UCP OFI reference: Cards at OFI)
    "OFI_L_PROD_CNT",    # confirmed (UCP OFI reference: Lending at OFI)
    "OFI_I_PROD_CNT",    # confirmed (UCP OFI reference: Investment at OFI)
    "OFI_M_PROD_CNT",    # confirmed (UCP OFI reference: Mutual fund at OFI)
]

CATEGORICAL_FIELDS = [
    "AGE_RNG",              # [UNCONFIRMED] — age range band; plausible UCP4 field, not in cards docs
    "GENERATION",           # [UNCONFIRMED] — generational cohort; plausible UCP4 field, not in cards docs
    "TENURE_RBC_RNG",       # confirmed (VBA UCP field list)
    "CREDIT_SCORE_RNG",     # [UNCONFIRMED] — credit score range; plausible UCP4 field, not in cards docs
    "DLQY_IND",             # confirmed (VBA UCP field list)
    "INCOME_AFTER_TAX_RNG", # [UNCONFIRMED] — income-after-tax range; plausible UCP4 field, not in cards docs
    "PROF_SEG_CD",          # [UNCONFIRMED] — profession segment code; plausible, not in cards docs
    "OLB_ENROLLED_IND",     # confirmed (VBA UCP eligibility list)
]

ALL_UCP_FIELDS = NUMERIC_FIELDS + CATEGORICAL_FIELDS

# ── COHORTS ──────────────────────────────────────────────────────────────────
# (cohort_label, cohort_month_start, cohort_month_end, ucp_partition)
COHORTS = [
    ("2026-04", "2026-04-01", "2026-04-30", "2026-03-31"),
    ("2026-03", "2026-03-01", "2026-03-31", "2026-02-28"),
    ("2026-02", "2026-02-01", "2026-02-28", "2026-01-31"),
]

# ── EDW HELPER ───────────────────────────────────────────────────────────────
def edw_query(sql, desc=""):
    t0 = time.time()
    if desc:
        print(f"  [{desc}] executing...", end=" ", flush=True)
    cursor = EDW.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    cursor.close()
    print(f"{len(rows):,} rows in {time.time() - t0:.0f}s")
    return pd.DataFrame(rows, columns=cols)


# ── STEP 1: GROUP ASSIGNMENT QUERY (Teradata) ─────────────────────────────
def build_group_sql(mo_start, mo_end):
    """
    Assigns each PLI mobile-banner lead in the cohort month to ONE of four groups.
    Priority: crv_action > crv_control > no_overlap_ever_crv > never_crv.

    Teradata constraints observed:
    - NO QUALIFY (Starburst/Trino syntax only) — use ranked CTE + WHERE rn=1
    - DATE - DATE for day arithmetic
    - No NULLIFZERO
    - No EXISTS inside CASE WHEN
    - LEFT JOIN to flag sets, not subqueries in CASE
    """
    return f"""
WITH pli_universe AS (
    SELECT
        acct_no,
        clnt_no,
        treatmt_strt_dt,
        treatmt_end_dt
    FROM DL_MR_PROD.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '{mo_start}'
      AND treatmt_strt_dt <= DATE '{mo_end}'
      AND channel LIKE '%MB%'
),

crv_ever_set AS (
    SELECT DISTINCT acct_no
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-01-01'
),

-- date-window overlap on acct_no: crv offer window intersects pli treatment window
crv_action_overlap AS (
    SELECT DISTINCT p.acct_no
    FROM pli_universe p
    INNER JOIN DL_MR_PROD.cards_crv_install_decis_resp c
        ON c.acct_no = p.acct_no
       AND c.channels_deployed LIKE '%IM%'
       AND c.action_control = 'Action'
       AND c.offer_start_date <= p.treatmt_end_dt
       AND c.offer_end_date   >= p.treatmt_strt_dt
),

crv_control_overlap AS (
    SELECT DISTINCT p.acct_no
    FROM pli_universe p
    INNER JOIN DL_MR_PROD.cards_crv_install_decis_resp c
        ON c.acct_no = p.acct_no
       AND c.action_control = 'Control'
       AND c.offer_start_date <= p.treatmt_end_dt
       AND c.offer_end_date   >= p.treatmt_strt_dt
),

ranked AS (
    SELECT
        p.clnt_no,
        CASE
            WHEN cao.acct_no IS NOT NULL THEN 'crv_action'
            WHEN cco.acct_no IS NOT NULL THEN 'crv_control'
            WHEN ce.acct_no  IS NOT NULL THEN 'no_overlap_ever_crv'
            ELSE                              'never_crv'
        END AS grp,
        ROW_NUMBER() OVER (PARTITION BY p.clnt_no ORDER BY p.treatmt_strt_dt) AS rn
    FROM pli_universe p
    LEFT JOIN crv_action_overlap  cao ON cao.acct_no = p.acct_no
    LEFT JOIN crv_control_overlap cco ON cco.acct_no = p.acct_no
    LEFT JOIN crv_ever_set        ce  ON ce.acct_no  = p.acct_no
)

SELECT clnt_no, grp
FROM ranked
WHERE rn = 1
"""


# ── PROFILING HELPER ──────────────────────────────────────────────────────
GROUPS_ORDER = ["crv_action", "crv_control", "no_overlap_ever_crv", "never_crv"]

def build_profile_table(ucp_joined):
    """
    One-hot encodes categoricals, then computes group means + overall mean.
    Returns a wide DataFrame: rows=features, cols=groups + 'overall'.
    """
    # One-hot encode categoricals
    cat_lower = [f.lower() for f in CATEGORICAL_FIELDS]
    num_lower  = [f.lower() for f in NUMERIC_FIELDS]

    # Collect distinct values per categorical to build dummies
    dummy_cols = []
    for cat in cat_lower:
        if cat not in ucp_joined.columns:
            continue
        vals = ucp_joined[cat].dropna().unique()
        for v in sorted(vals):
            col_name = f"{cat}__{v}"
            ucp_joined[col_name] = (ucp_joined[cat] == v).astype(int)
            dummy_cols.append(col_name)

    feature_cols = [c for c in num_lower if c in ucp_joined.columns] + dummy_cols

    rows = {}
    for grp in GROUPS_ORDER:
        sub = ucp_joined[ucp_joined["grp"] == grp]
        rows[grp] = sub[feature_cols].mean()

    rows["overall"] = ucp_joined[feature_cols].mean()

    profile = pd.DataFrame(rows)
    profile.index.name = "feature"
    return profile


# ── MAIN LOOP ────────────────────────────────────────────────────────────────
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

for cohort_label, mo_start, mo_end, ucp_partition in COHORTS:
    print(f"\n{'='*60}")
    print(f"COHORT: {cohort_label}  (PLI {mo_start} to {mo_end}, UCP {ucp_partition})")

    # STEP 1 — group assignment from EDW
    sql = build_group_sql(mo_start, mo_end)
    grp_pd = edw_query(sql, f"group assignment {cohort_label}")

    print(f"  Group counts:\n{grp_pd['grp'].value_counts().to_string()}")
    print(f"  Total leads: {len(grp_pd):,}")

    # STEP 2 — normalise clnt_no to string (int has no leading zeros, str(int) matches UCP key)
    grp_pd["clnt_no"] = grp_pd["clnt_no"].astype("Int64").astype(str)

    grp_spark = spark.createDataFrame(grp_pd[["clnt_no", "grp"]])

    # STEP 3 — read UCP for the prior-month-end partition only
    ucp_path = f"{UCP_BASE}MONTH_END_DATE={ucp_partition}"
    ucp_raw = (spark.read
               .option("basePath", UCP_BASE)
               .parquet(ucp_path)
               .filter(trim(col("CLNT_TYP")) == "Personal"))

    # Select only what we need; lower-case all column names
    available_ucp = [c for c in ALL_UCP_FIELDS if c in ucp_raw.columns]
    missing_ucp   = [c for c in ALL_UCP_FIELDS if c not in ucp_raw.columns]
    if missing_ucp:
        print(f"  WARNING: UCP fields not found in partition (skipped): {missing_ucp}")

    ucp_sel = ucp_raw.select("CLNT_NO", *available_ucp)
    ucp_sel = ucp_sel.toDF(*[c.lower() for c in ucp_sel.columns])
    # align join key types: UCP clnt_no -> trimmed string, leading zeros stripped
    # (the EDW side is int->str; without this the join can silently return 0 rows)
    ucp_sel = ucp_sel.withColumn("clnt_no", F.regexp_replace(F.trim(col("clnt_no").cast("string")), "^0+", ""))

    # Inner join to leads
    joined = ucp_sel.join(grp_spark, on="clnt_no", how="inner")
    joined_pd = joined.toPandas()
    print(f"  UCP matched: {len(joined_pd):,} of {len(grp_pd):,} leads")

    # sanity: % missing per selected field in the matched rows (lean, in-cell only)
    miss_cols = [f.lower() for f in ALL_UCP_FIELDS if f.lower() in joined_pd.columns]
    miss = joined_pd[miss_cols].isnull().mean().mul(100).round(1)
    print(f"  Missing % per field:\n{miss.to_string()}")

    # STEP 4 — profile table
    profile = build_profile_table(joined_pd)

    print(f"\n--- Profile table ({cohort_label}) ---")
    with pd.option_context("display.max_rows", 200, "display.width", 140, "display.float_format", "{:.4f}".format):
        print(profile.to_string())

    # Save final table to HDFS via Spark (pandas .to_csv writes to the local FS, which
    # AI Farm can't reach). reset_index() turns the feature names into a column.
    # NB: Spark writes a FOLDER of part-files at this path, not a single .csv.
    out_path = f"{HDFS_BASE}/ucp_profile_{cohort_label}"
    (spark.createDataFrame(profile.reset_index())
        .coalesce(1).write.mode("overwrite").option("header", True).csv(out_path))
    print(f"\n  Saved to HDFS: {out_path}")
