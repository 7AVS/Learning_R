# ===================================================================
# Cards Decision & Response — Exploratory Data Analysis
# ===================================================================
# Three tables from dl_mr_prod accessed via EDW (Teradata):
# - PCD Ongoing (cards_pcd_ongoing_decis_resp) — 92 columns
# - PLI (cards_pli_decision_resp) — 125 columns
# - TPA PCQ (cards_tpa_pcq_decision_resp) — 79 columns
#
# EDW connection is pre-established. Queries use Teradata SQL syntax.

from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder \
    .appName("Cards EDA") \
    .master("yarn") \
    .enableHiveSupport() \
    .getOrCreate()

DB = "dl_mr_prod"
PCD = f"{DB}.cards_pcd_ongoing_decis_resp"
PLI = f"{DB}.cards_pli_decision_resp"
TPA = f"{DB}.cards_tpa_pcq_decision_resp"

print("Spark session ready. EDW connection assumed available.")


# ===================================================================
# Section 1: PCD Ongoing Decision Response (92 columns)
# Primary Index: acct_no, clnt_no, tactic_id_parent, response_start, response_end
# ===================================================================

# --- PCD: Row count ---
cursor = EDW.cursor()
cursor.execute(f"SELECT COUNT(*) FROM {PCD}")
pcd_count = cursor.fetchall()[0][0]
cursor.close()
print(f"PCD Ongoing: {pcd_count:,} rows")

# --- PCD: Schema / column listing ---
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT ColumnName, ColumnType, Nullable
    FROM DBC.ColumnsV
    WHERE DatabaseName = 'dl_mr_prod'
      AND TableName = 'cards_pcd_ongoing_decis_resp'
    ORDER BY ColumnId
""")
pcd_schema = cursor.fetchall()
cursor.close()
pcd_schema_df = pd.DataFrame(pcd_schema, columns=["column", "type", "nullable"])
print("\nPCD Schema:")
print(pcd_schema_df.to_string(index=False))

# --- PCD: Date ranges ---
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT MIN(response_start) AS response_start_min, MAX(response_start) AS response_start_max,
           MIN(response_end) AS response_end_min, MAX(response_end) AS response_end_max,
           MIN(dt_prod_change) AS dt_prod_change_min, MAX(dt_prod_change) AS dt_prod_change_max,
           MIN(success_dt_1) AS success_dt_1_min, MAX(success_dt_1) AS success_dt_1_max,
           MIN(success_dt_2) AS success_dt_2_min, MAX(success_dt_2) AS success_dt_2_max
    FROM {PCD}
""")
pcd_dates = cursor.fetchall()
cursor.close()
pcd_dates_df = pd.DataFrame(pcd_dates, columns=[
    "response_start_min", "response_start_max", "response_end_min", "response_end_max",
    "dt_prod_change_min", "dt_prod_change_max", "success_dt_1_min", "success_dt_1_max",
    "success_dt_2_min", "success_dt_2_max"
])
print("\nPCD Date Ranges:")
print(pcd_dates_df.T.to_string(header=False))

# --- PCD: Null % per column ---
cursor = EDW.cursor()
pcd_col_names = pcd_schema_df["column"].str.strip().tolist()
pcd_null_exprs = ", ".join([
    f"CAST(100.0 * SUM(CASE WHEN \"{c}\" IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS \"{c}\""
    for c in pcd_col_names
])
cursor.execute(f"SELECT {pcd_null_exprs} FROM {PCD}")
pcd_null_results = cursor.fetchall()
cursor.close()
pcd_nulls = pd.DataFrame([pcd_null_results[0]], columns=pcd_col_names).T
pcd_nulls.columns = ["null_pct"]
pcd_nulls = pcd_nulls.sort_values("null_pct", ascending=False)
print("\nPCD Null %:")
print(pcd_nulls.to_string())

# --- PCD: Key categorical distributions ---
print("\n--- PCD Categorical Distributions ---")
for col in ["mnemonic", "product_at_decision", "product_grouping_at_decision",
            "relationship_mgmt", "responder", "channels", "credit_phase",
            "life_stage", "age_band", "wallet_band", "bi_clnt_seg",
            "report_groups_period", "strategy_seg_cd", "act_ctl_seg",
            "new_comer", "ngen", "ias", "hsbc_ind"]:
    cursor = EDW.cursor()
    cursor.execute(f"""
        SELECT TOP 20 "{col}", COUNT(*) AS cnt,
               CAST(100.0 * COUNT(*) / {pcd_count} AS DECIMAL(5,2)) AS pct
        FROM {PCD}
        GROUP BY "{col}"
        ORDER BY cnt DESC
    """)
    rows = cursor.fetchall()
    cursor.close()
    df = pd.DataFrame(rows, columns=[col, "cnt", "pct"])
    print(f"\n  >> {col}:")
    print(df.to_string(index=False))

# --- PCD: Numeric summaries ---
print("\n--- PCD Numeric Stats ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        MIN(nlbt_expected_value), MAX(nlbt_expected_value), CAST(AVG(nlbt_expected_value) AS DECIMAL(12,2)),
        MIN(nlbt_expec_value_upgradepath), MAX(nlbt_expec_value_upgradepath), CAST(AVG(nlbt_expec_value_upgradepath) AS DECIMAL(12,2)),
        MIN(channelcost), MAX(channelcost), CAST(AVG(channelcost) AS DECIMAL(10,2)),
        MIN(offer_bonus_cash), MAX(offer_bonus_cash), CAST(AVG(offer_bonus_cash) AS DECIMAL(10,2)),
        MIN(offer_bonus_points), MAX(offer_bonus_points), CAST(AVG(offer_bonus_points) AS DECIMAL(10,2)),
        MIN(opn_prod_cnt), MAX(opn_prod_cnt), CAST(AVG(opn_prod_cnt) AS DECIMAL(6,2)),
        MIN(actv_prod_cnt), MAX(actv_prod_cnt), CAST(AVG(actv_prod_cnt) AS DECIMAL(6,2)),
        MIN(actv_prod_srvc_cnt), MAX(actv_prod_srvc_cnt), CAST(AVG(actv_prod_srvc_cnt) AS DECIMAL(6,2)),
        MIN(avg_yrs_rbc), MAX(avg_yrs_rbc), CAST(AVG(avg_yrs_rbc) AS DECIMAL(6,2))
    FROM {PCD}
""")
pcd_nums = cursor.fetchall()
cursor.close()
num_cols = ["nlbt_expected_value", "nlbt_expec_value_upgradepath", "channelcost",
            "offer_bonus_cash", "offer_bonus_points", "opn_prod_cnt",
            "actv_prod_cnt", "actv_prod_srvc_cnt", "avg_yrs_rbc"]
pcd_num_df = pd.DataFrame([{
    "column": num_cols[i],
    "min": pcd_nums[0][i*3], "max": pcd_nums[0][i*3+1], "avg": pcd_nums[0][i*3+2]
} for i in range(len(num_cols))])
print(pcd_num_df.to_string(index=False))

# --- PCD: Channel deployment flags ---
print("\n--- PCD Channel Deployment ---")
for col in ["channel_deploy_cc", "channel_deploy_dm", "channel_deploy_do",
            "channel_deploy_im", "channel_deploy_em", "channel_deploy_rd",
            "channel_deploy_iv", "channel_em_reminder"]:
    cursor = EDW.cursor()
    cursor.execute(f"""
        SELECT "{col}", COUNT(*) AS cnt,
               CAST(100.0 * COUNT(*) / {pcd_count} AS DECIMAL(5,2)) AS pct
        FROM {PCD}
        GROUP BY "{col}"
        ORDER BY cnt DESC
    """)
    rows = cursor.fetchall()
    cursor.close()
    df = pd.DataFrame(rows, columns=[col, "cnt", "pct"])
    print(f"\n  >> {col}:")
    print(df.to_string(index=False))

# --- PCD: Response rates ---
print("\n--- PCD Response Rates ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(responder_anyproduct AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS resp_any_pct,
        CAST(100.0 * SUM(CAST(responder_targetproduct AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS resp_target_pct,
        CAST(100.0 * SUM(CAST(responder_upgrade_path AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS resp_upgrade_pct
    FROM {PCD}
""")
rows = cursor.fetchall()
cursor.close()
print(pd.DataFrame(rows, columns=["resp_any_pct", "resp_target_pct", "resp_upgrade_pct"]).to_string(index=False))

# --- PCD: OandO funnel ---
print("\n--- PCD OandO Funnel ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(oando AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS oando_pct,
        CAST(100.0 * SUM(CAST(oando_actioned AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS actioned_pct,
        CAST(100.0 * SUM(CAST(oando_pending AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS pending_pct,
        CAST(100.0 * SUM(CAST(oando_declined AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS declined_pct,
        CAST(100.0 * SUM(CAST(oando_approved AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS approved_pct
    FROM {PCD}
""")
rows = cursor.fetchall()
cursor.close()
print(pd.DataFrame(rows, columns=["oando_pct", "actioned_pct", "pending_pct", "declined_pct", "approved_pct"]).to_string(index=False))

# --- PCD: Email ---
print("\n--- PCD Email ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(tactic_email AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS tactic_email_pct,
        CAST(100.0 * SUM(CAST(email_status AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS email_status_pct
    FROM {PCD}
""")
rows = cursor.fetchall()
cursor.close()
print(pd.DataFrame(rows, columns=["tactic_email_pct", "email_status_pct"]).to_string(index=False))

cursor = EDW.cursor()
cursor.execute(f"""
    SELECT TOP 20 email_disposition, COUNT(*) AS cnt,
           CAST(100.0 * COUNT(*) / {pcd_count} AS DECIMAL(5,2)) AS pct
    FROM {PCD} GROUP BY email_disposition ORDER BY cnt DESC
""")
rows = cursor.fetchall()
cursor.close()
print("\n  >> email_disposition:")
print(pd.DataFrame(rows, columns=["email_disposition", "cnt", "pct"]).to_string(index=False))

# --- PCD: Digital & Self-Serve ---
print("\n--- PCD Digital & Self-Serve ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(olb AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS olb_pct,
        CAST(100.0 * SUM(CAST(mb AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS mb_pct,
        CAST(100.0 * SUM(CAST(dor AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS dor_pct,
        CAST(100.0 * SUM(CAST(ss_act_ind AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS ss_act_pct,
        CAST(100.0 * SUM(CAST(ss_opn_ind AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS ss_opn_pct
    FROM {PCD}
""")
rows = cursor.fetchall()
cursor.close()
print(pd.DataFrame(rows, columns=["olb_pct", "mb_pct", "dor_pct", "ss_act_pct", "ss_opn_pct"]).to_string(index=False))

cursor = EDW.cursor()
cursor.execute(f"""
    SELECT MIN(impression_olb), MAX(impression_olb), CAST(AVG(impression_olb) AS DECIMAL(10,2)),
           MIN(clicked_olb), MAX(clicked_olb), CAST(AVG(clicked_olb) AS DECIMAL(10,2))
    FROM {PCD}
""")
rows = cursor.fetchall()
cursor.close()
print("\n  OLB impressions/clicks:")
print(pd.DataFrame([{
    "imp_olb_min": rows[0][0], "imp_olb_max": rows[0][1], "imp_olb_avg": rows[0][2],
    "clk_olb_min": rows[0][3], "clk_olb_max": rows[0][4], "clk_olb_avg": rows[0][5]
}]).to_string(index=False))

# --- PCD: Offer & Upgrade ---
print("\n--- PCD Offers & Upgrades ---")
for col in ["invitation_to_upgrade", "target_product", "target_product_name",
            "target_product_grouping", "fulfillment_channel", "test_description", "test_value"]:
    cursor = EDW.cursor()
    cursor.execute(f"""
        SELECT TOP 20 "{col}", COUNT(*) AS cnt,
               CAST(100.0 * COUNT(*) / {pcd_count} AS DECIMAL(5,2)) AS pct
        FROM {PCD} GROUP BY "{col}" ORDER BY cnt DESC
    """)
    rows = cursor.fetchall()
    cursor.close()
    print(f"\n  >> {col}:")
    print(pd.DataFrame(rows, columns=[col, "cnt", "pct"]).to_string(index=False))


# ===================================================================
# Section 2: PLI Decision Response (125 columns)
# Primary Index: parent_tactic_id, acct_no, clnt_no
# ===================================================================

# --- PLI: Row count ---
cursor = EDW.cursor()
cursor.execute(f"SELECT COUNT(*) FROM {PLI}")
pli_count = cursor.fetchall()[0][0]
cursor.close()
print(f"\nPLI: {pli_count:,} rows")

# --- PLI: Schema ---
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT ColumnName, ColumnType, Nullable
    FROM DBC.ColumnsV
    WHERE DatabaseName = 'dl_mr_prod'
      AND TableName = 'cards_pli_decision_resp'
    ORDER BY ColumnId
""")
pli_schema = cursor.fetchall()
cursor.close()
pli_schema_df = pd.DataFrame(pli_schema, columns=["column", "type", "nullable"])
print("\nPLI Schema:")
print(pli_schema_df.to_string(index=False))

# --- PLI: Date ranges ---
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT MIN(decision_dt), MAX(decision_dt),
           MIN(actual_strt_dt), MAX(actual_strt_dt),
           MIN(parent_actual_strt_dt), MAX(parent_actual_strt_dt),
           MIN(treatmt_strt_dt), MAX(treatmt_strt_dt),
           MIN(treatmt_end_dt), MAX(treatmt_end_dt),
           MIN(dt_cl_change), MAX(dt_cl_change),
           MIN(spid_proc_dt), MAX(spid_proc_dt),
           MIN(dt_acct_open), MAX(dt_acct_open)
    FROM {PLI}
""")
pli_dates = cursor.fetchall()
cursor.close()
pli_date_cols = ["decision_dt_min", "decision_dt_max", "actual_strt_min", "actual_strt_max",
                 "parent_strt_min", "parent_strt_max", "treatmt_strt_min", "treatmt_strt_max",
                 "treatmt_end_min", "treatmt_end_max", "cl_change_min", "cl_change_max",
                 "spid_min", "spid_max", "acct_open_min", "acct_open_max"]
print("\nPLI Date Ranges:")
print(pd.DataFrame([pli_dates[0]], columns=pli_date_cols).T.to_string(header=False))

# --- PLI: Null % per column ---
pli_col_names = pli_schema_df["column"].str.strip().tolist()
pli_null_exprs = ", ".join([
    f"CAST(100.0 * SUM(CASE WHEN \"{c}\" IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS \"{c}\""
    for c in pli_col_names
])
cursor = EDW.cursor()
cursor.execute(f"SELECT {pli_null_exprs} FROM {PLI}")
pli_null_results = cursor.fetchall()
cursor.close()
pli_nulls = pd.DataFrame([pli_null_results[0]], columns=pli_col_names).T
pli_nulls.columns = ["null_pct"]
pli_nulls = pli_nulls.sort_values("null_pct", ascending=False)
print("\nPLI Null %:")
print(pli_nulls.to_string())

# --- PLI: Key categorical distributions ---
print("\n--- PLI Categorical Distributions ---")
for col in ["increase_decrease", "product_current", "product_name_current",
            "product_grouping_current", "wave", "wave2", "test",
            "like_for_like", "like_for_like_label", "owner", "cpc_dni",
            "usage_behaviour", "credit_phase", "life_stage", "age_band",
            "wallet_band", "bi_clnt_seg", "new_comer", "ngen", "ias",
            "mnemonic", "action_code", "report_groups_period",
            "test_groups_period", "parent_test_group", "new_to_campaign",
            "spid_label", "hsbc_ind", "low_grow_ind", "low_revenue_ind",
            "multi_card_ind", "olb_active_90", "mobile_active_at_decision"]:
    cursor = EDW.cursor()
    cursor.execute(f"""
        SELECT TOP 20 "{col}", COUNT(*) AS cnt,
               CAST(100.0 * COUNT(*) / {pli_count} AS DECIMAL(5,2)) AS pct
        FROM {PLI}
        GROUP BY "{col}"
        ORDER BY cnt DESC
    """)
    rows = cursor.fetchall()
    cursor.close()
    print(f"\n  >> {col}:")
    print(pd.DataFrame(rows, columns=[col, "cnt", "pct"]).to_string(index=False))

# --- PLI: Numeric summaries ---
print("\n--- PLI Numeric Stats ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        MIN(limit_increase_amt), MAX(limit_increase_amt), CAST(AVG(limit_increase_amt) AS DECIMAL(18,2)),
        MIN(limit_decrease_amt), MAX(limit_decrease_amt), CAST(AVG(limit_decrease_amt) AS DECIMAL(18,2)),
        MIN(cli_offer), MAX(cli_offer), CAST(AVG(cli_offer) AS DECIMAL(10,2)),
        MIN(model_score), MAX(model_score), CAST(AVG(model_score) AS DECIMAL(10,2)),
        MIN(cv_score), MAX(cv_score), CAST(AVG(cv_score) AS DECIMAL(10,2)),
        MIN(decile), MAX(decile), CAST(AVG(decile) AS DECIMAL(6,2)),
        MIN(new_decile), MAX(new_decile), CAST(AVG(new_decile) AS DECIMAL(6,2)),
        MIN(opn_prod_cnt), MAX(opn_prod_cnt), CAST(AVG(opn_prod_cnt) AS DECIMAL(6,2)),
        MIN(actv_prod_cnt), MAX(actv_prod_cnt), CAST(AVG(actv_prod_cnt) AS DECIMAL(6,2)),
        MIN(avg_yrs_rbc), MAX(avg_yrs_rbc), CAST(AVG(avg_yrs_rbc) AS DECIMAL(6,2)),
        MIN(csr_interactions), MAX(csr_interactions), CAST(AVG(csr_interactions) AS DECIMAL(6,2))
    FROM {PLI}
""")
pli_nums = cursor.fetchall()
cursor.close()
pli_num_cols = ["limit_increase_amt", "limit_decrease_amt", "cli_offer", "model_score",
                "cv_score", "decile", "new_decile", "opn_prod_cnt", "actv_prod_cnt",
                "avg_yrs_rbc", "csr_interactions"]
pli_num_df = pd.DataFrame([{
    "column": pli_num_cols[i],
    "min": pli_nums[0][i*3], "max": pli_nums[0][i*3+1], "avg": pli_nums[0][i*3+2]
} for i in range(len(pli_num_cols))])
print(pli_num_df.to_string(index=False))

# --- PLI: Channel flags ---
print("\n--- PLI Channel Flags ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(channel_cc AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(channel_dm AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(channel_do AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(channel_ec AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(channel_em AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(channel_im AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(channel_in AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(channel_iu AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(channel_iv AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(channel_mb AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(channel_rd AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {PLI}
""")
rows = cursor.fetchall()
cursor.close()
ch_labels = ["cc", "dm", "do", "ec", "em", "im", "in", "iu", "iv", "mb", "rd"]
print(pd.DataFrame([{"channel": ch_labels[i], "rate_pct": rows[0][i]} for i in range(len(ch_labels))]).to_string(index=False))

for col in ["channel", "response_channel", "response_source"]:
    cursor = EDW.cursor()
    cursor.execute(f"""
        SELECT TOP 20 "{col}", COUNT(*) AS cnt,
               CAST(100.0 * COUNT(*) / {pli_count} AS DECIMAL(5,2)) AS pct
        FROM {PLI} GROUP BY "{col}" ORDER BY cnt DESC
    """)
    rows = cursor.fetchall()
    cursor.close()
    print(f"\n  >> {col}:")
    print(pd.DataFrame(rows, columns=[col, "cnt", "pct"]).to_string(index=False))

# --- PLI: Key flags ---
print("\n--- PLI Key Flags ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(responder_cli AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(decisioned_acct AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(student_indicator AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {PLI}
""")
rows = cursor.fetchall()
cursor.close()
print(pd.DataFrame([rows[0]], columns=["responder_cli_pct", "decisioned_pct", "student_pct"]).to_string(index=False))

# --- PLI: OandO funnel ---
print("\n--- PLI OandO Funnel ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(oando AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(oando_actioned AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(oando_pending AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(oando_declined AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(oando_approved AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {PLI}
""")
rows = cursor.fetchall()
cursor.close()
print(pd.DataFrame([rows[0]], columns=["oando_pct", "actioned_pct", "pending_pct", "declined_pct", "approved_pct"]).to_string(index=False))

# --- PLI: Email ---
print("\n--- PLI Email ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(tactic_email AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(email_status AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {PLI}
""")
rows = cursor.fetchall()
cursor.close()
print(pd.DataFrame([rows[0]], columns=["tactic_email_pct", "email_status_pct"]).to_string(index=False))

cursor = EDW.cursor()
cursor.execute(f"""
    SELECT TOP 20 email_disposition, COUNT(*) AS cnt,
           CAST(100.0 * COUNT(*) / {pli_count} AS DECIMAL(5,2)) AS pct
    FROM {PLI} GROUP BY email_disposition ORDER BY cnt DESC
""")
rows = cursor.fetchall()
cursor.close()
print("\n  >> email_disposition:")
print(pd.DataFrame(rows, columns=["email_disposition", "cnt", "pct"]).to_string(index=False))

# --- PLI: Digital ---
print("\n--- PLI Digital ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(olb AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(mb AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {PLI}
""")
rows = cursor.fetchall()
cursor.close()
print(pd.DataFrame([rows[0]], columns=["olb_pct", "mb_pct"]).to_string(index=False))

cursor = EDW.cursor()
cursor.execute(f"""
    SELECT MIN(impression_olb), MAX(impression_olb), CAST(AVG(impression_olb) AS DECIMAL(10,2)),
           MIN(clicked_olb), MAX(clicked_olb), CAST(AVG(clicked_olb) AS DECIMAL(10,2)),
           MIN(impression_mb), MAX(impression_mb), CAST(AVG(impression_mb) AS DECIMAL(10,2)),
           MIN(clicked_mb), MAX(clicked_mb), CAST(AVG(clicked_mb) AS DECIMAL(10,2)),
           MIN(mobile_banner), MAX(mobile_banner), CAST(AVG(mobile_banner) AS DECIMAL(10,2)),
           MIN(mobile_offer_hub), MAX(mobile_offer_hub), CAST(AVG(mobile_offer_hub) AS DECIMAL(10,2))
    FROM {PLI}
""")
rows = cursor.fetchall()
cursor.close()
dig_cols = ["impression_olb", "clicked_olb", "impression_mb", "clicked_mb", "mobile_banner", "mobile_offer_hub"]
print(pd.DataFrame([{
    "column": dig_cols[i],
    "min": rows[0][i*3], "max": rows[0][i*3+1], "avg": rows[0][i*3+2]
} for i in range(len(dig_cols))]).to_string(index=False))

# --- PLI: SPID & Model ---
print("\n--- PLI SPID & Model ---")
for col in ["spid", "spid_label", "decile", "new_decile"]:
    cursor = EDW.cursor()
    cursor.execute(f"""
        SELECT TOP 20 "{col}", COUNT(*) AS cnt,
               CAST(100.0 * COUNT(*) / {pli_count} AS DECIMAL(5,2)) AS pct
        FROM {PLI} GROUP BY "{col}" ORDER BY cnt DESC
    """)
    rows = cursor.fetchall()
    cursor.close()
    print(f"\n  >> {col}:")
    print(pd.DataFrame(rows, columns=[col, "cnt", "pct"]).to_string(index=False))

cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(pb_client AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(premier_client AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(tsne_ind AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {PLI}
""")
rows = cursor.fetchall()
cursor.close()
print("\n  Client type flags:")
print(pd.DataFrame([rows[0]], columns=["pb_client_pct", "premier_pct", "tsne_pct"]).to_string(index=False))


# ===================================================================
# Section 3: TPA PCQ Decision Response (79 columns)
# Primary Index: clnt_no, tactic_id, target_seg, strtgy_seg_cd, treatmt_start_dt
# ===================================================================

# --- TPA: Row count ---
cursor = EDW.cursor()
cursor.execute(f"SELECT COUNT(*) FROM {TPA}")
tpa_count = cursor.fetchall()[0][0]
cursor.close()
print(f"\nTPA PCQ: {tpa_count:,} rows")

# --- TPA: Schema ---
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT ColumnName, ColumnType, Nullable
    FROM DBC.ColumnsV
    WHERE DatabaseName = 'dl_mr_prod'
      AND TableName = 'cards_tpa_pcq_decision_resp'
    ORDER BY ColumnId
""")
tpa_schema = cursor.fetchall()
cursor.close()
tpa_schema_df = pd.DataFrame(tpa_schema, columns=["column", "type", "nullable"])
print("\nTPA Schema:")
print(tpa_schema_df.to_string(index=False))

# --- TPA: Date ranges ---
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT MIN(report_dt), MAX(report_dt),
           MIN(treatmt_start_dt), MAX(treatmt_start_dt),
           MIN(treatmt_end_dt), MAX(treatmt_end_dt),
           MIN(response_dt), MAX(response_dt)
    FROM {TPA}
""")
tpa_dates = cursor.fetchall()
cursor.close()
print("\nTPA Date Ranges:")
print(pd.DataFrame([tpa_dates[0]], columns=[
    "report_min", "report_max", "treatmt_strt_min", "treatmt_strt_max",
    "treatmt_end_min", "treatmt_end_max", "response_min", "response_max"
]).T.to_string(header=False))

# --- TPA: Null % per column ---
tpa_col_names = tpa_schema_df["column"].str.strip().tolist()
tpa_null_exprs = ", ".join([
    f"CAST(100.0 * SUM(CASE WHEN \"{c}\" IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS \"{c}\""
    for c in tpa_col_names
])
cursor = EDW.cursor()
cursor.execute(f"SELECT {tpa_null_exprs} FROM {TPA}")
tpa_null_results = cursor.fetchall()
cursor.close()
tpa_nulls = pd.DataFrame([tpa_null_results[0]], columns=tpa_col_names).T
tpa_nulls.columns = ["null_pct"]
tpa_nulls = tpa_nulls.sort_values("null_pct", ascending=False)
print("\nTPA Null %:")
print(tpa_nulls.to_string())

# --- TPA: Key categorical distributions ---
print("\n--- TPA Categorical Distributions ---")
for col in ["mnemonic", "target_seg", "tpa_ita", "like_for_like_group",
            "strtgy_seg_typ", "act_ctl_seg", "cmpgn_seg", "strtgy_seg_cd",
            "offer_prod_latest_group", "offer_prod_latest", "offer_prod_latest_name",
            "offer_fee_waiver_latest", "test_group_latest",
            "response_channel", "response_channel_grp",
            "product_applied", "product_applied_name",
            "asc_on_app", "asc_on_app_source", "hsbc_ind"]:
    cursor = EDW.cursor()
    cursor.execute(f"""
        SELECT TOP 20 "{col}", COUNT(*) AS cnt,
               CAST(100.0 * COUNT(*) / {tpa_count} AS DECIMAL(5,2)) AS pct
        FROM {TPA}
        GROUP BY "{col}"
        ORDER BY cnt DESC
    """)
    rows = cursor.fetchall()
    cursor.close()
    print(f"\n  >> {col}:")
    print(pd.DataFrame(rows, columns=[col, "cnt", "pct"]).to_string(index=False))

# --- TPA: Numeric summaries ---
print("\n--- TPA Numeric Stats ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        MIN(offer_rate_latest), MAX(offer_rate_latest), CAST(AVG(offer_rate_latest) AS DECIMAL(10,2)),
        MIN(offer_rate_months_latest), MAX(offer_rate_months_latest), CAST(AVG(offer_rate_months_latest) AS DECIMAL(10,2)),
        MIN(offer_fee_waiver_months_latest), MAX(offer_fee_waiver_months_latest), CAST(AVG(offer_fee_waiver_months_latest) AS DECIMAL(10,2)),
        MIN(offer_bonus_points_latest), MAX(offer_bonus_points_latest), CAST(AVG(offer_bonus_points_latest) AS DECIMAL(10,2)),
        MIN(offer_cr_lmt_latest), MAX(offer_cr_lmt_latest), CAST(AVG(offer_cr_lmt_latest) AS DECIMAL(10,2)),
        MIN(cr_lmt_approved), MAX(cr_lmt_approved), CAST(AVG(cr_lmt_approved) AS DECIMAL(10,2)),
        MIN(days_to_respond), MAX(days_to_respond), CAST(AVG(days_to_respond) AS DECIMAL(10,2)),
        MIN(model_score), MAX(model_score), CAST(AVG(model_score) AS DECIMAL(10,2)),
        MIN(expected_value), MAX(expected_value), CAST(AVG(expected_value) AS DECIMAL(10,2)),
        MIN(times_targeted), MAX(times_targeted), CAST(AVG(times_targeted) AS DECIMAL(10,2)),
        MIN(num_coapps), MAX(num_coapps), CAST(AVG(num_coapps) AS DECIMAL(10,2)),
        MIN(num_auth_users), MAX(num_auth_users), CAST(AVG(num_auth_users) AS DECIMAL(10,2))
    FROM {TPA}
""")
tpa_nums = cursor.fetchall()
cursor.close()
tpa_num_cols = ["offer_rate_latest", "offer_rate_months_latest", "offer_fee_waiver_months_latest",
                "offer_bonus_points_latest", "offer_cr_lmt_latest", "cr_lmt_approved",
                "days_to_respond", "model_score", "expected_value",
                "times_targeted", "num_coapps", "num_auth_users"]
print(pd.DataFrame([{
    "column": tpa_num_cols[i],
    "min": tpa_nums[0][i*3], "max": tpa_nums[0][i*3+1], "avg": tpa_nums[0][i*3+2]
} for i in range(len(tpa_num_cols))]).to_string(index=False))

# --- TPA: Application funnel ---
print("\n--- TPA Application Funnel ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(app_completed AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(app_approved AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {TPA}
""")
rows = cursor.fetchall()
cursor.close()
print(pd.DataFrame([rows[0]], columns=["app_completed_pct", "app_approved_pct"]).to_string(index=False))

# --- TPA: Channel flags ---
print("\n--- TPA Channel Flags ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(chnl_dm AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(chnl_do AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(chnl_ec AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(chnl_em AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(chnl_im AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(chnl_in AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(chnl_iu AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(chnl_iv AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(chnl_mb AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(chnl_md AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(chnl_rd AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(chnl_em_reminder AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {TPA}
""")
rows = cursor.fetchall()
cursor.close()
tpa_ch = ["dm", "do", "ec", "em", "im", "in", "iu", "iv", "mb", "md", "rd", "em_reminder"]
print(pd.DataFrame([{"channel": tpa_ch[i], "rate_pct": rows[0][i]} for i in range(len(tpa_ch))]).to_string(index=False))

cursor = EDW.cursor()
cursor.execute(f"""
    SELECT TOP 20 channel, COUNT(*) AS cnt,
           CAST(100.0 * COUNT(*) / {tpa_count} AS DECIMAL(5,2)) AS pct
    FROM {TPA} GROUP BY channel ORDER BY cnt DESC
""")
rows = cursor.fetchall()
cursor.close()
print("\n  >> channel (text):")
print(pd.DataFrame(rows, columns=["channel", "cnt", "pct"]).to_string(index=False))

# --- TPA: OandO funnel ---
print("\n--- TPA OandO Funnel ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(oando AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(oando_actioned AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(oando_pending AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(oando_declined AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(oando_approved AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {TPA}
""")
rows = cursor.fetchall()
cursor.close()
print(pd.DataFrame([rows[0]], columns=["oando_pct", "actioned_pct", "pending_pct", "declined_pct", "approved_pct"]).to_string(index=False))

# --- TPA: Email ---
print("\n--- TPA Email ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(tactic_email AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(email_status AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {TPA}
""")
rows = cursor.fetchall()
cursor.close()
print(pd.DataFrame([rows[0]], columns=["tactic_email_pct", "email_status_pct"]).to_string(index=False))

cursor = EDW.cursor()
cursor.execute(f"""
    SELECT TOP 20 email_disposition, COUNT(*) AS cnt,
           CAST(100.0 * COUNT(*) / {tpa_count} AS DECIMAL(5,2)) AS pct
    FROM {TPA} GROUP BY email_disposition ORDER BY cnt DESC
""")
rows = cursor.fetchall()
cursor.close()
print("\n  >> email_disposition:")
print(pd.DataFrame(rows, columns=["email_disposition", "cnt", "pct"]).to_string(index=False))

# --- TPA: Digital ---
print("\n--- TPA Digital ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT MIN(impression_olb), MAX(impression_olb), CAST(AVG(impression_olb) AS DECIMAL(10,2)),
           MIN(clicked_olb), MAX(clicked_olb), CAST(AVG(clicked_olb) AS DECIMAL(10,2)),
           MIN(impression_mb), MAX(impression_mb), CAST(AVG(impression_mb) AS DECIMAL(10,2)),
           MIN(clicked_mb), MAX(clicked_mb), CAST(AVG(clicked_mb) AS DECIMAL(10,2)),
           MIN(mobile_banner), MAX(mobile_banner), CAST(AVG(mobile_banner) AS DECIMAL(10,2))
    FROM {TPA}
""")
rows = cursor.fetchall()
cursor.close()
tpa_dig = ["impression_olb", "clicked_olb", "impression_mb", "clicked_mb", "mobile_banner"]
print(pd.DataFrame([{
    "column": tpa_dig[i],
    "min": rows[0][i*3], "max": rows[0][i*3+1], "avg": rows[0][i*3+2]
} for i in range(len(tpa_dig))]).to_string(index=False))

# --- TPA: Call Center (Genesis) ---
print("\n--- TPA Call Center (Genesis) ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(tactic_call AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(cntct_atmpt_gnsis AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(call_ans_gnsis AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(agt_gnsis AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(rpc_gnsis AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {TPA}
""")
rows = cursor.fetchall()
cursor.close()
print(pd.DataFrame([rows[0]], columns=["tactic_call_pct", "contact_attempt_pct", "call_answered_pct", "agent_pct", "rpc_pct"]).to_string(index=False))

# --- TPA: Time analysis ---
print("\n--- TPA Decision Timing ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT decsn_year, COUNT(*) AS cnt,
           CAST(100.0 * COUNT(*) / {tpa_count} AS DECIMAL(5,2)) AS pct
    FROM {TPA} GROUP BY decsn_year ORDER BY decsn_year
""")
rows = cursor.fetchall()
cursor.close()
print("  >> decsn_year:")
print(pd.DataFrame(rows, columns=["decsn_year", "cnt", "pct"]).to_string(index=False))

cursor = EDW.cursor()
cursor.execute(f"""
    SELECT decsn_month, COUNT(*) AS cnt,
           CAST(100.0 * COUNT(*) / {tpa_count} AS DECIMAL(5,2)) AS pct
    FROM {TPA} GROUP BY decsn_month ORDER BY decsn_month
""")
rows = cursor.fetchall()
cursor.close()
print("\n  >> decsn_month:")
print(pd.DataFrame(rows, columns=["decsn_month", "cnt", "pct"]).to_string(index=False))


# ===================================================================
# Section 4: Cross-Table Comparison
# ===================================================================

print("\n=== Row Count Summary ===")
print(f"  PCD Ongoing: {pcd_count:>12,}")
print(f"  PLI:         {pli_count:>12,}")
print(f"  TPA PCQ:     {tpa_count:>12,}")
print(f"  Total:       {pcd_count + pli_count + tpa_count:>12,}")

# --- Distinct clients per table ---
print("\n--- Distinct Clients per Table ---")
for label, tbl in [("PCD", PCD), ("PLI", PLI), ("TPA", TPA)]:
    cursor = EDW.cursor()
    cursor.execute(f"SELECT COUNT(DISTINCT clnt_no) FROM {tbl}")
    cnt = cursor.fetchall()[0][0]
    cursor.close()
    print(f"  {label}: {cnt:,} distinct clients")

# --- Pairwise client overlap ---
print("\n--- Client Overlap ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT COUNT(DISTINCT a.clnt_no)
    FROM (SELECT DISTINCT clnt_no FROM {PCD}) a
    INNER JOIN (SELECT DISTINCT clnt_no FROM {PLI}) b ON a.clnt_no = b.clnt_no
""")
pcd_pli = cursor.fetchall()[0][0]
cursor.close()
print(f"  PCD-PLI overlap: {pcd_pli:,} clients")

cursor = EDW.cursor()
cursor.execute(f"""
    SELECT COUNT(DISTINCT a.clnt_no)
    FROM (SELECT DISTINCT clnt_no FROM {PCD}) a
    INNER JOIN (SELECT DISTINCT CAST(clnt_no AS DECIMAL(14,0)) AS clnt_no FROM {TPA}) b ON a.clnt_no = b.clnt_no
""")
pcd_tpa = cursor.fetchall()[0][0]
cursor.close()
print(f"  PCD-TPA overlap: {pcd_tpa:,} clients")

cursor = EDW.cursor()
cursor.execute(f"""
    SELECT COUNT(DISTINCT a.clnt_no)
    FROM (SELECT DISTINCT clnt_no FROM {PLI}) a
    INNER JOIN (SELECT DISTINCT CAST(clnt_no AS DECIMAL(14,0)) AS clnt_no FROM {TPA}) b ON a.clnt_no = b.clnt_no
""")
pli_tpa = cursor.fetchall()[0][0]
cursor.close()
print(f"  PLI-TPA overlap: {pli_tpa:,} clients")

# --- Mnemonic comparison ---
print("\n=== Mnemonic Comparison ===")
for label, tbl in [("PCD", PCD), ("PLI", PLI), ("TPA", TPA)]:
    cursor = EDW.cursor()
    cursor.execute(f"""
        SELECT TOP 10 mnemonic, COUNT(*) AS cnt
        FROM {tbl} GROUP BY mnemonic ORDER BY cnt DESC
    """)
    rows = cursor.fetchall()
    cursor.close()
    print(f"\n  {label}:")
    print(pd.DataFrame(rows, columns=["mnemonic", "cnt"]).to_string(index=False))

# --- HSBC indicator comparison ---
print("\n=== HSBC Indicator ===")
for label, tbl in [("PCD", PCD), ("PLI", PLI), ("TPA", TPA)]:
    cursor = EDW.cursor()
    cursor.execute(f"""
        SELECT TOP 5 hsbc_ind, COUNT(*) AS cnt
        FROM {tbl} GROUP BY hsbc_ind ORDER BY cnt DESC
    """)
    rows = cursor.fetchall()
    cursor.close()
    print(f"\n  {label}:")
    print(pd.DataFrame(rows, columns=["hsbc_ind", "cnt"]).to_string(index=False))

# --- OandO comparison ---
print("\n=== OandO Rates Across Tables ===")
for label, tbl in [("PCD", PCD), ("PLI", PLI), ("TPA", TPA)]:
    cursor = EDW.cursor()
    cursor.execute(f"""
        SELECT
            CAST(100.0 * SUM(CAST(oando AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
            CAST(100.0 * SUM(CAST(oando_actioned AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
            CAST(100.0 * SUM(CAST(oando_pending AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
            CAST(100.0 * SUM(CAST(oando_declined AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
            CAST(100.0 * SUM(CAST(oando_approved AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
        FROM {tbl}
    """)
    rows = cursor.fetchall()
    cursor.close()
    print(f"\n  {label}:")
    print(pd.DataFrame([rows[0]], columns=["oando", "actioned", "pending", "declined", "approved"]).to_string(index=False))


# ===================================================================
# Summary & Next Steps
# ===================================================================
# Review after execution:
# 1. Data volumes — row counts and date ranges per table
# 2. Data quality — null percentages, unexpected values
# 3. Distributions — categorical skew, numeric outliers
# 4. Overlap — client/account coverage across tables
# 5. Channel usage — deployment patterns
# 6. Response rates — conversion funnels
# 7. Segmentation — demographic and behavioral segments
print("\n=== EDA Complete ===")
