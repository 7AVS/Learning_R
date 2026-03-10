# ===================================================================
# Cards Decision & Response — Exploratory Data Analysis
# ===================================================================
#
# Three tables from `dl_mr_prod` accessed via PySpark (YARN):
# - PCD Ongoing (`cards_pcd_ongoing_decis_resp`) — 92 columns
# - PLI (`cards_pli_decision_resp`) — 125 columns
# - TPA PCQ (`cards_tpa_pcq_decision_resp`) — 79 columns

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("Cards EDA") \
    .master("yarn") \
    .enableHiveSupport() \
    .getOrCreate()

# Table references
DB = "dl_mr_prod"
TABLES = {
    "pcd": f"{DB}.cards_pcd_ongoing_decis_resp",
    "pli": f"{DB}.cards_pli_decision_resp",
    "tpa": f"{DB}.cards_tpa_pcq_decision_resp"
}

print("Spark session ready.")

# ===================================================================
# Helper Functions
# ===================================================================

def row_count(table):
    """Get row count for a table."""
    cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}").collect()[0]["cnt"]
    print(f"{table}: {cnt:,} rows")
    return cnt

def null_pct(table):
    """Return null % for every column in a table."""
    df = spark.sql(f"SELECT * FROM {table} LIMIT 0")
    cols = df.columns
    exprs = [f"ROUND(100.0 * SUM(CASE WHEN `{c}` IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS `{c}`" for c in cols]
    query = f"SELECT {', '.join(exprs)} FROM {table}"
    result = spark.sql(query).toPandas().T
    result.columns = ["null_pct"]
    result = result.sort_values("null_pct", ascending=False)
    print(f"\n--- Null % for {table} ---")
    print(result.to_string())
    return result

def top_values(table, col, n=20):
    """Show top N values for a column with count and percentage."""
    query = f"""
    SELECT `{col}`, COUNT(*) AS cnt,
           ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM {table}), 2) AS pct
    FROM {table}
    GROUP BY `{col}`
    ORDER BY cnt DESC
    LIMIT {n}
    """
    result = spark.sql(query).toPandas()
    print(f"\n--- {table}.{col} (top {n}) ---")
    print(result.to_string(index=False))
    return result

def numeric_stats(table, cols):
    """Summary stats for numeric columns."""
    exprs = []
    for c in cols:
        exprs.extend([
            f"MIN(`{c}`) AS `{c}_min`",
            f"MAX(`{c}`) AS `{c}_max`",
            f"ROUND(AVG(`{c}`), 2) AS `{c}_avg`",
            f"ROUND(PERCENTILE_APPROX(`{c}`, 0.5), 2) AS `{c}_median`",
            f"ROUND(STDDEV(`{c}`), 2) AS `{c}_stddev`",
            f"SUM(CASE WHEN `{c}` IS NULL THEN 1 ELSE 0 END) AS `{c}_nulls`"
        ])
    query = f"SELECT {', '.join(exprs)} FROM {table}"
    result = spark.sql(query).toPandas()
    # Reshape for readability
    import pandas as pd
    records = []
    for c in cols:
        records.append({
            "column": c,
            "min": result[f"{c}_min"].iloc[0],
            "max": result[f"{c}_max"].iloc[0],
            "avg": result[f"{c}_avg"].iloc[0],
            "median": result[f"{c}_median"].iloc[0],
            "stddev": result[f"{c}_stddev"].iloc[0],
            "nulls": result[f"{c}_nulls"].iloc[0]
        })
    out = pd.DataFrame(records)
    print(f"\n--- Numeric stats for {table} ---")
    print(out.to_string(index=False))
    return out

def date_range(table, cols):
    """Show min/max for date columns."""
    exprs = []
    for c in cols:
        exprs.extend([f"MIN(`{c}`) AS `{c}_min`", f"MAX(`{c}`) AS `{c}_max`"])
    query = f"SELECT {', '.join(exprs)} FROM {table}"
    result = spark.sql(query).toPandas()
    for c in cols:
        print(f"  {c}: {result[f'{c}_min'].iloc[0]}  to  {result[f'{c}_max'].iloc[0]}")
    return result

def flag_rates(table, cols, label=""):
    """For binary flag columns (0/1), show the rate of 1s."""
    exprs = [f"ROUND(100.0 * SUM(CAST(`{c}` AS INT)) / COUNT(*), 2) AS `{c}`" for c in cols]
    query = f"SELECT {', '.join(exprs)} FROM {table}"
    result = spark.sql(query).toPandas().T
    result.columns = ["rate_pct"]
    print(f"\n--- Flag rates: {label} ({table}) ---")
    print(result.to_string())
    return result

# ===================================================================
# Section 1: PCD Ongoing Decision Response
# ===================================================================
# `dl_mr_prod.cards_pcd_ongoing_decis_resp` — 92 columns
#
# Primary Index: acct_no, clnt_no, tactic_id_parent, response_start, response_end

tbl = TABLES["pcd"]
pcd_count = row_count(tbl)
spark.sql(f"SELECT * FROM {tbl} LIMIT 0").printSchema()

print(f"Date ranges for {tbl}:")
date_range(tbl, ["response_start", "response_end", "dt_prod_change", "success_dt_1", "success_dt_2"])

pcd_nulls = null_pct(tbl)

# -------------------------------------------------------------------
# PCD — Key Categorical Distributions
# -------------------------------------------------------------------

for col in ["mnemonic", "product_at_decision", "product_grouping_at_decision",
            "relationship_mgmt", "responder", "channels", "credit_phase",
            "life_stage", "age_band", "wallet_band", "bi_clnt_seg",
            "report_groups_period", "strategy_seg_cd", "act_ctl_seg",
            "new_comer", "ngen", "ias", "hsbc_ind"]:
    top_values(tbl, col)

# -------------------------------------------------------------------
# PCD — Numeric Summaries
# -------------------------------------------------------------------

numeric_stats(tbl, [
    "nlbt_expected_value", "nlbt_expec_value_upgradepath",
    "channelcost", "offer_bonus_cash", "offer_bonus_points",
    "opn_prod_cnt", "actv_prod_cnt", "actv_prod_srvc_cnt", "avg_yrs_rbc"
])

# -------------------------------------------------------------------
# PCD — Channel Deployment Flags
# -------------------------------------------------------------------

channel_cols = ["channel_deploy_cc", "channel_deploy_dm", "channel_deploy_do",
                "channel_deploy_im", "channel_deploy_em", "channel_deploy_rd",
                "channel_deploy_iv", "channel_em_reminder"]
# These are char(1) flags — count distinct values
for col in channel_cols:
    top_values(tbl, col)

# -------------------------------------------------------------------
# PCD — Response Analysis
# -------------------------------------------------------------------

flag_rates(tbl, ["responder_anyproduct", "responder_targetproduct", "responder_upgrade_path"],
           label="Response Rates")

# -------------------------------------------------------------------
# PCD — OandO Funnel
# -------------------------------------------------------------------

flag_rates(tbl, ["oando", "oando_actioned", "oando_pending", "oando_declined", "oando_approved"],
           label="OandO Funnel")

# -------------------------------------------------------------------
# PCD — Email & Digital
# -------------------------------------------------------------------

flag_rates(tbl, ["tactic_email", "email_status"], label="Email")
top_values(tbl, "email_disposition")
print()
flag_rates(tbl, ["olb", "mb", "dor", "ss_act_ind", "ss_opn_ind"], label="Digital & Self-Serve")
numeric_stats(tbl, ["impression_olb", "clicked_olb"])

# -------------------------------------------------------------------
# PCD — Offer & Upgrade Analysis
# -------------------------------------------------------------------

top_values(tbl, "invitation_to_upgrade")
top_values(tbl, "target_product")
top_values(tbl, "target_product_name")
top_values(tbl, "target_product_grouping")
top_values(tbl, "fulfillment_channel")
top_values(tbl, "test_description")
top_values(tbl, "test_value")

# ===================================================================
# Section 2: PLI Decision Response
# ===================================================================
# `dl_mr_prod.cards_pli_decision_resp` — 125 columns
#
# Primary Index: parent_tactic_id, acct_no, clnt_no

tbl = TABLES["pli"]
pli_count = row_count(tbl)
spark.sql(f"SELECT * FROM {tbl} LIMIT 0").printSchema()

print(f"Date ranges for {tbl}:")
date_range(tbl, ["decision_dt", "actual_strt_dt", "parent_actual_strt_dt",
                 "treatmt_strt_dt", "treatmt_end_dt", "dt_cl_change", "spid_proc_dt", "dt_acct_open"])

pli_nulls = null_pct(tbl)

# -------------------------------------------------------------------
# PLI — Key Categorical Distributions
# -------------------------------------------------------------------

for col in ["increase_decrease", "product_current", "product_name_current",
            "product_grouping_current", "wave", "wave2", "test",
            "like_for_like", "like_for_like_label", "owner", "cpc_dni",
            "usage_behaviour", "credit_phase", "life_stage", "age_band",
            "wallet_band", "bi_clnt_seg", "new_comer", "ngen", "ias",
            "mnemonic", "action_code", "report_groups_period",
            "test_groups_period", "parent_test_group", "new_to_campaign",
            "spid_label", "hsbc_ind", "low_grow_ind", "low_revenue_ind",
            "multi_card_ind", "olb_active_90", "mobile_active_at_decision"]:
    top_values(tbl, col)

# -------------------------------------------------------------------
# PLI — Numeric Summaries
# -------------------------------------------------------------------

numeric_stats(tbl, [
    "limit_increase_amt", "limit_decrease_amt", "cli_offer",
    "model_score", "cv_score", "decile", "new_decile",
    "opn_prod_cnt", "actv_prod_cnt", "actv_prod_srvc_cnt", "avg_yrs_rbc",
    "csr_interactions"
])

# -------------------------------------------------------------------
# PLI — Channel Analysis
# -------------------------------------------------------------------

flag_rates(tbl, ["channel_cc", "channel_dm", "channel_do", "channel_ec",
                 "channel_em", "channel_im", "channel_in", "channel_iu",
                 "channel_iv", "channel_mb", "channel_rd"],
           label="Channel Flags")
top_values(tbl, "channel")
top_values(tbl, "response_channel")
top_values(tbl, "response_source")

# -------------------------------------------------------------------
# PLI — Response, OandO, Email & Digital
# -------------------------------------------------------------------

flag_rates(tbl, ["responder_cli", "decisioned_acct", "student_indicator"], label="Key Flags")
print()
flag_rates(tbl, ["oando", "oando_actioned", "oando_pending", "oando_declined", "oando_approved"],
           label="OandO Funnel")
print()
flag_rates(tbl, ["tactic_email", "email_status"], label="Email")
top_values(tbl, "email_disposition")
print()
flag_rates(tbl, ["olb", "mb"], label="Digital")
numeric_stats(tbl, ["impression_olb", "clicked_olb", "impression_mb", "clicked_mb",
                     "mobile_banner", "mobile_offer_hub"])

# -------------------------------------------------------------------
# PLI — SPID & Model Analysis
# -------------------------------------------------------------------

top_values(tbl, "spid")
top_values(tbl, "spid_label")
top_values(tbl, "decile")
top_values(tbl, "new_decile")
flag_rates(tbl, ["pb_client", "premier_client", "tsne_ind"], label="Client Type Flags")

# ===================================================================
# Section 3: TPA PCQ Decision Response
# ===================================================================
# `dl_mr_prod.cards_tpa_pcq_decision_resp` — 79 columns
#
# Primary Index: clnt_no, tactic_id, target_seg, strtgy_seg_cd, treatmt_start_dt

tbl = TABLES["tpa"]
tpa_count = row_count(tbl)
spark.sql(f"SELECT * FROM {tbl} LIMIT 0").printSchema()

print(f"Date ranges for {tbl}:")
date_range(tbl, ["report_dt", "treatmt_start_dt", "treatmt_end_dt", "response_dt"])

tpa_nulls = null_pct(tbl)

# -------------------------------------------------------------------
# TPA — Key Categorical Distributions
# -------------------------------------------------------------------

for col in ["mnemonic", "target_seg", "tpa_ita", "like_for_like_group",
            "strtgy_seg_typ", "act_ctl_seg", "cmpgn_seg", "strtgy_seg_cd",
            "offer_prod_latest_group", "offer_prod_latest", "offer_prod_latest_name",
            "offer_fee_waiver_latest", "test_group_latest",
            "response_channel", "response_channel_grp",
            "product_applied", "product_applied_name",
            "asc_on_app", "asc_on_app_source", "hsbc_ind"]:
    top_values(tbl, col)

# -------------------------------------------------------------------
# TPA — Numeric Summaries
# -------------------------------------------------------------------

numeric_stats(tbl, [
    "offer_rate_latest", "offer_rate_months_latest", "offer_fee_waiver_months_latest",
    "offer_bonus_points_latest", "offer_cr_lmt_latest", "cr_lmt_approved",
    "days_to_respond", "model_score", "expected_value",
    "model_score_decile", "expected_value_decile",
    "times_targeted", "num_coapps", "num_auth_users"
])

# -------------------------------------------------------------------
# TPA — Application Funnel
# -------------------------------------------------------------------

flag_rates(tbl, ["app_completed", "app_approved"], label="Application Funnel")

# -------------------------------------------------------------------
# TPA — Channel Analysis
# -------------------------------------------------------------------

flag_rates(tbl, ["chnl_dm", "chnl_do", "chnl_ec", "chnl_em", "chnl_im",
                 "chnl_in", "chnl_iu", "chnl_iv", "chnl_mb", "chnl_md",
                 "chnl_rd", "chnl_em_reminder"],
           label="Channel Flags")
top_values(tbl, "channel")

# -------------------------------------------------------------------
# TPA — OandO, Email & Digital
# -------------------------------------------------------------------

flag_rates(tbl, ["oando", "oando_actioned", "oando_pending", "oando_declined", "oando_approved"],
           label="OandO Funnel")
print()
flag_rates(tbl, ["tactic_email", "email_status"], label="Email")
top_values(tbl, "email_disposition")
print()
numeric_stats(tbl, ["impression_olb", "clicked_olb", "impression_mb", "clicked_mb", "mobile_banner"])

# -------------------------------------------------------------------
# TPA — Call Center (Genesis)
# -------------------------------------------------------------------

flag_rates(tbl, ["tactic_call", "cntct_atmpt_gnsis", "call_ans_gnsis", "agt_gnsis", "rpc_gnsis"],
           label="Call Center (Genesis)")

# -------------------------------------------------------------------
# TPA — Time Analysis
# -------------------------------------------------------------------

top_values(tbl, "decsn_year")
top_values(tbl, "decsn_month")

# ===================================================================
# Section 4: Cross-Table Comparison
# ===================================================================

print("=== Row Count Summary ===")
print(f"  PCD Ongoing: {pcd_count:>12,}")
print(f"  PLI:         {pli_count:>12,}")
print(f"  TPA PCQ:     {tpa_count:>12,}")
print(f"  Total:       {pcd_count + pli_count + tpa_count:>12,}")

# -------------------------------------------------------------------
# Cross-Table — Client Overlap
# -------------------------------------------------------------------

overlap_query = f"""
SELECT
    COUNT(DISTINCT p.clnt_no) AS pcd_clients,
    COUNT(DISTINCT l.clnt_no) AS pli_clients,
    COUNT(DISTINCT t.clnt_no) AS tpa_clients
FROM (SELECT DISTINCT clnt_no FROM {TABLES['pcd']}) p
FULL OUTER JOIN (SELECT DISTINCT clnt_no FROM {TABLES['pli']}) l ON p.clnt_no = l.clnt_no
FULL OUTER JOIN (SELECT DISTINCT clnt_no FROM {TABLES['tpa']}) t ON COALESCE(p.clnt_no, l.clnt_no) = t.clnt_no
"""
# Note: This full outer join may be expensive. Alternative approach below.

# Simpler approach — distinct client counts per table
for name, tbl in TABLES.items():
    clnt_col = "clnt_no"
    cnt = spark.sql(f"SELECT COUNT(DISTINCT {clnt_col}) AS cnt FROM {tbl}").collect()[0]["cnt"]
    print(f"  {name}: {cnt:,} distinct clients")

# Pairwise overlap
pcd_pli = spark.sql(f"""
    SELECT COUNT(DISTINCT a.clnt_no) AS overlap
    FROM (SELECT DISTINCT clnt_no FROM {TABLES['pcd']}) a
    INNER JOIN (SELECT DISTINCT clnt_no FROM {TABLES['pli']}) b ON a.clnt_no = b.clnt_no
""").collect()[0]["overlap"]
print(f"\n  PCD-PLI overlap: {pcd_pli:,} clients")

pcd_tpa = spark.sql(f"""
    SELECT COUNT(DISTINCT a.clnt_no) AS overlap
    FROM (SELECT DISTINCT clnt_no FROM {TABLES['pcd']}) a
    INNER JOIN (SELECT DISTINCT CAST(clnt_no AS DECIMAL(14,0)) AS clnt_no FROM {TABLES['tpa']}) b ON a.clnt_no = b.clnt_no
""").collect()[0]["overlap"]
print(f"  PCD-TPA overlap: {pcd_tpa:,} clients")

pli_tpa = spark.sql(f"""
    SELECT COUNT(DISTINCT a.clnt_no) AS overlap
    FROM (SELECT DISTINCT clnt_no FROM {TABLES['pli']}) a
    INNER JOIN (SELECT DISTINCT CAST(clnt_no AS DECIMAL(14,0)) AS clnt_no FROM {TABLES['tpa']}) b ON a.clnt_no = b.clnt_no
""").collect()[0]["overlap"]
print(f"  PLI-TPA overlap: {pli_tpa:,} clients")

# -------------------------------------------------------------------
# Cross-Table — Common Dimension Comparison
# -------------------------------------------------------------------
# Comparing distributions of shared columns across tables.

# Compare mnemonic across tables that have it
print("=== Mnemonic comparison ===")
for name, tbl in TABLES.items():
    print(f"\n{name}:")
    top_values(tbl, "mnemonic", n=10)

# Compare hsbc_ind
print("\n\n=== HSBC Indicator ===")
for name, tbl in TABLES.items():
    print(f"\n{name}:")
    top_values(tbl, "hsbc_ind", n=5)

# -------------------------------------------------------------------
# Cross-Table — OandO Comparison
# -------------------------------------------------------------------

print("=== OandO rates across tables ===\n")
oando_cols = ["oando", "oando_actioned", "oando_pending", "oando_declined", "oando_approved"]
for name, tbl in TABLES.items():
    print(f"\n{name}:")
    flag_rates(tbl, oando_cols, label=name)

# ===================================================================
# Summary & Next Steps
# ===================================================================
# Key findings from EDA will populate after execution. Review:
# 1. Data volumes — row counts and date ranges per table
# 2. Data quality — null percentages, unexpected values
# 3. Distributions — categorical skew, numeric outliers
# 4. Overlap — client/account coverage across tables
# 5. Channel usage — deployment patterns
# 6. Response rates — conversion funnels
# 7. Segmentation — demographic and behavioral segments
