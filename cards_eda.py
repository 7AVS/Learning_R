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

# --- PCD: Date ranges ---
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT MIN(response_start), MAX(response_start),
           MIN(response_end), MAX(response_end),
           MIN(dt_prod_change), MAX(dt_prod_change),
           MIN(success_dt_1), MAX(success_dt_1),
           MIN(success_dt_2), MAX(success_dt_2)
    FROM {PCD}
""")
r = cursor.fetchall()[0]
cursor.close()
print("\nPCD Date Ranges:")
for label, mn, mx in [("response_start", r[0], r[1]), ("response_end", r[2], r[3]),
                       ("dt_prod_change", r[4], r[5]), ("success_dt_1", r[6], r[7]),
                       ("success_dt_2", r[8], r[9])]:
    print(f"  {label}: {mn}  to  {mx}")

# --- PCD: Null % per column (one query, positional mapping) ---
pcd_col_names = [
    "acct_no", "clnt_no", "tactic_id_parent", "response_start", "response_end",
    "mnemonic", "fy_start", "treatmt_mn", "product_at_decision", "product_grouping_at_decision",
    "product_name_at_decision", "relationship_mgmt", "offer_bonus_cash", "offer_bonus_points",
    "offer_description", "invitation_to_upgrade", "target_product", "target_product_name",
    "target_product_grouping", "channel_deploy_cc", "channel_deploy_dm", "channel_deploy_do",
    "channel_deploy_im", "channel_deploy_em", "channel_deploy_rd", "channel_deploy_iv",
    "channel_em_reminder", "channelcost", "channels", "dt_prod_change", "fy_prod_change",
    "month_prod_change", "new_product", "nibt_expected_value", "nibt_expec_value_upgradepath",
    "report_groups_period", "test_groups_period", "responder", "responder_anyproduct",
    "responder_targetproduct", "responder_upgrade_path", "strategy_seg_cd", "cmpgn_seg",
    "strtgy_seg_desc", "act_ctl_seg", "student_indicator", "success_cd_1", "success_cd_2",
    "success_dt_1", "success_dt_2", "weeknum_response", "csr_interactions", "test_description",
    "test_value", "tactic_email", "email_disposition", "email_status", "oando", "oando_actioned",
    "oando_pending", "oando_declined", "oando_approved", "fulfillment_channel", "gu", "active",
    "opn_prod_cnt", "actv_prod_cnt", "actv_prod_srvc_cnt", "ss_act_ind", "ss_opn_ind",
    "avg_yrs_rbc", "rbc_tenure", "life_stage", "value_for_money", "bi_clnt_seg", "vulnrblty_cd",
    "mny_in_potntl_cd", "mny_out_potntl_cd", "lifetm_val_5yr_clnt_cd", "credit_phase",
    "wallet_band", "new_comer", "ngen", "ias", "age_band", "tibc", "dor", "mb", "olb",
    "impression_olb", "clicked_olb", "hsbc_ind"
]
pcd_null_exprs = ", ".join([
    f"CAST(100.0 * SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2))"
    for c in pcd_col_names
])
cursor = EDW.cursor()
cursor.execute(f"SELECT {pcd_null_exprs} FROM {PCD}")
pcd_null_row = cursor.fetchall()[0]
cursor.close()
pcd_nulls = pd.DataFrame({"column": pcd_col_names, "null_pct": list(pcd_null_row)})
pcd_nulls = pcd_nulls.sort_values("null_pct", ascending=False)
print("\nPCD Null %:")
print(pcd_nulls.to_string(index=False))

# --- PCD: Key categorical distributions ---
print("\n--- PCD Categorical Distributions ---")
for col in ["mnemonic", "product_at_decision", "product_grouping_at_decision",
            "relationship_mgmt", "responder", "channels", "credit_phase",
            "life_stage", "age_band", "wallet_band", "bi_clnt_seg",
            "report_groups_period", "strategy_seg_cd", "act_ctl_seg",
            "new_comer", "ngen", "ias", "hsbc_ind"]:
    cursor = EDW.cursor()
    cursor.execute(f"SELECT {col}, COUNT(*) AS cnt FROM {PCD} GROUP BY {col} ORDER BY cnt DESC")
    rows = cursor.fetchall()
    cursor.close()
    df = pd.DataFrame(rows, columns=[col, "cnt"])
    df["pct"] = (100.0 * df["cnt"] / pcd_count).round(2)
    print(f"\n  >> {col}:")
    print(df.head(20).to_string(index=False))

# --- PCD: Numeric summaries ---
print("\n--- PCD Numeric Stats ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        MIN(nibt_expected_value), MAX(nibt_expected_value), CAST(AVG(nibt_expected_value) AS DECIMAL(12,2)),
        MIN(nibt_expec_value_upgradepath), MAX(nibt_expec_value_upgradepath), CAST(AVG(nibt_expec_value_upgradepath) AS DECIMAL(12,2)),
        MIN(channelcost), MAX(channelcost), CAST(AVG(channelcost) AS DECIMAL(10,2)),
        MIN(offer_bonus_cash), MAX(offer_bonus_cash), CAST(AVG(offer_bonus_cash) AS DECIMAL(10,2)),
        MIN(offer_bonus_points), MAX(offer_bonus_points), CAST(AVG(offer_bonus_points) AS DECIMAL(10,2)),
        MIN(opn_prod_cnt), MAX(opn_prod_cnt), CAST(AVG(opn_prod_cnt) AS DECIMAL(6,2)),
        MIN(actv_prod_cnt), MAX(actv_prod_cnt), CAST(AVG(actv_prod_cnt) AS DECIMAL(6,2)),
        MIN(actv_prod_srvc_cnt), MAX(actv_prod_srvc_cnt), CAST(AVG(actv_prod_srvc_cnt) AS DECIMAL(6,2)),
        MIN(avg_yrs_rbc), MAX(avg_yrs_rbc), CAST(AVG(avg_yrs_rbc) AS DECIMAL(6,2))
    FROM {PCD}
""")
r = cursor.fetchall()[0]
cursor.close()
num_cols = ["nibt_expected_value", "nibt_expec_value_upgradepath", "channelcost",
            "offer_bonus_cash", "offer_bonus_points", "opn_prod_cnt",
            "actv_prod_cnt", "actv_prod_srvc_cnt", "avg_yrs_rbc"]
print(pd.DataFrame([{"column": num_cols[i], "min": r[i*3], "max": r[i*3+1], "avg": r[i*3+2]} for i in range(len(num_cols))]).to_string(index=False))

# --- PCD: Channel deployment flags ---
print("\n--- PCD Channel Deployment ---")
for col in ["channel_deploy_cc", "channel_deploy_dm", "channel_deploy_do",
            "channel_deploy_im", "channel_deploy_em", "channel_deploy_rd",
            "channel_deploy_iv", "channel_em_reminder"]:
    cursor = EDW.cursor()
    cursor.execute(f"SELECT {col}, COUNT(*) AS cnt FROM {PCD} GROUP BY {col} ORDER BY cnt DESC")
    rows = cursor.fetchall()
    cursor.close()
    df = pd.DataFrame(rows, columns=[col, "cnt"])
    df["pct"] = (100.0 * df["cnt"] / pcd_count).round(2)
    print(f"\n  >> {col}:")
    print(df.head(20).to_string(index=False))

# --- PCD: Response rates ---
print("\n--- PCD Response Rates ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(responder_anyproduct AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(responder_targetproduct AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(responder_upgrade_path AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {PCD}
""")
r = cursor.fetchall()[0]
cursor.close()
print(pd.DataFrame([r], columns=["resp_any_pct", "resp_target_pct", "resp_upgrade_pct"]).to_string(index=False))

# --- PCD: OandO funnel ---
print("\n--- PCD OandO Funnel ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(oando AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(oando_actioned AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(oando_pending AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(oando_declined AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(oando_approved AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {PCD}
""")
r = cursor.fetchall()[0]
cursor.close()
print(pd.DataFrame([r], columns=["oando_pct", "actioned_pct", "pending_pct", "declined_pct", "approved_pct"]).to_string(index=False))

# --- PCD: Email ---
print("\n--- PCD Email ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(tactic_email AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(email_status AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {PCD}
""")
r = cursor.fetchall()[0]
cursor.close()
print(pd.DataFrame([r], columns=["tactic_email_pct", "email_status_pct"]).to_string(index=False))

cursor = EDW.cursor()
cursor.execute(f"SELECT email_disposition, COUNT(*) AS cnt FROM {PCD} GROUP BY email_disposition ORDER BY cnt DESC")
rows = cursor.fetchall()
cursor.close()
df = pd.DataFrame(rows, columns=["email_disposition", "cnt"])
df["pct"] = (100.0 * df["cnt"] / pcd_count).round(2)
print("\n  >> email_disposition:")
print(df.to_string(index=False))

# --- PCD: Digital & Self-Serve ---
print("\n--- PCD Digital & Self-Serve ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(olb AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(mb AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(dor AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(ss_act_ind AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(ss_opn_ind AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {PCD}
""")
r = cursor.fetchall()[0]
cursor.close()
print(pd.DataFrame([r], columns=["olb_pct", "mb_pct", "dor_pct", "ss_act_pct", "ss_opn_pct"]).to_string(index=False))

cursor = EDW.cursor()
cursor.execute(f"""
    SELECT MIN(impression_olb), MAX(impression_olb), CAST(AVG(impression_olb) AS DECIMAL(10,2)),
           MIN(clicked_olb), MAX(clicked_olb), CAST(AVG(clicked_olb) AS DECIMAL(10,2))
    FROM {PCD}
""")
r = cursor.fetchall()[0]
cursor.close()
print("\n  OLB impressions/clicks:")
print(pd.DataFrame([{"imp_olb_min": r[0], "imp_olb_max": r[1], "imp_olb_avg": r[2],
                      "clk_olb_min": r[3], "clk_olb_max": r[4], "clk_olb_avg": r[5]}]).to_string(index=False))

# --- PCD: Offer & Upgrade ---
print("\n--- PCD Offers & Upgrades ---")
for col in ["invitation_to_upgrade", "target_product", "target_product_name",
            "target_product_grouping", "fulfillment_channel", "test_description", "test_value"]:
    cursor = EDW.cursor()
    cursor.execute(f"SELECT {col}, COUNT(*) AS cnt FROM {PCD} GROUP BY {col} ORDER BY cnt DESC")
    rows = cursor.fetchall()
    cursor.close()
    df = pd.DataFrame(rows, columns=[col, "cnt"])
    df["pct"] = (100.0 * df["cnt"] / pcd_count).round(2)
    print(f"\n  >> {col}:")
    print(df.head(20).to_string(index=False))


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
r = cursor.fetchall()[0]
cursor.close()
print("\nPLI Date Ranges:")
for label, mn, mx in [("decision_dt", r[0], r[1]), ("actual_strt_dt", r[2], r[3]),
                       ("parent_actual_strt_dt", r[4], r[5]), ("treatmt_strt_dt", r[6], r[7]),
                       ("treatmt_end_dt", r[8], r[9]), ("dt_cl_change", r[10], r[11]),
                       ("spid_proc_dt", r[12], r[13]), ("dt_acct_open", r[14], r[15])]:
    print(f"  {label}: {mn}  to  {mx}")

# --- PLI: Null % per column ---
pli_col_names = [
    "parent_tactic_id", "acct_no", "clnt_no", "cellcode", "tst_grp_cd", "strategy_id",
    "rpt_grp_cd", "newimm_seg", "dt_cl_change", "limit_increase_amt", "responder_cli",
    "offer_description", "decision_dt", "increase_decrease", "actual_strt_dt",
    "parent_actual_strt_dt", "treatmt_strt_dt", "treatmt_end_dt", "action_code", "mnemonic",
    "channel", "channel_cc", "channel_dm", "channel_do", "channel_ec", "channel_em",
    "channel_im", "channel_in", "channel_iu", "channel_iv", "channel_mb", "channel_rd",
    "product_current", "product_name_current", "product_grouping_current", "wave",
    "test_groups_period", "parent_test_group", "dm_redeploy_test_grp", "em_redeploy_test_grp",
    "wave2", "limit_decrease_amt", "report_groups_period", "action_code_period",
    "parent_tactic_id_period", "report_date", "fy_cmpgn_start", "month_cmpgn_start",
    "fy_cl_change", "month_cl_change", "like_for_like", "like_for_like_label", "test",
    "decisioned_acct", "student_indicator", "cli_offer", "response_channel", "response_source",
    "channel_period_em_remind", "em_reminder_control", "pcl_expansion_pop", "spid", "spid_label",
    "spid_proc_dt", "model_score", "decile", "new_to_campaign", "owner", "cpc_dni",
    "mobile_active_at_decision", "dm_creative_id", "em_creative_id", "low_grow_ind",
    "low_revenue_ind", "multi_card_ind", "olb_active_90", "gu", "active", "opn_prod_cnt",
    "actv_prod_cnt", "actv_prod_srvc_cnt", "ss_act_ind", "ss_opn_ind", "avg_yrs_rbc",
    "rbc_tenure", "life_stage", "value_for_money", "bi_clnt_seg", "vulnrblty_cd",
    "mny_in_potntl_cd", "mny_out_potntl_cd", "lifetm_val_5yr_clnt_cd", "credit_phase",
    "wallet_band", "new_comer", "ngen", "ias", "age_band", "tibc", "dor", "mb", "olb",
    "csr_interactions", "oando", "oando_actioned", "oando_pending", "oando_declined",
    "oando_approved", "tactic_email", "email_disposition", "email_status", "impression_olb",
    "clicked_olb", "hsbc_ind", "usage_behaviour", "cv_score", "new_decile", "mobile_banner",
    "mobile_offer_hub", "impression_mb", "pb_client", "premier_client", "dt_acct_open",
    "clicked_mb", "tsne_ind"
]
pli_null_exprs = ", ".join([
    f"CAST(100.0 * SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2))"
    for c in pli_col_names
])
cursor = EDW.cursor()
cursor.execute(f"SELECT {pli_null_exprs} FROM {PLI}")
pli_null_row = cursor.fetchall()[0]
cursor.close()
pli_nulls = pd.DataFrame({"column": pli_col_names, "null_pct": list(pli_null_row)})
pli_nulls = pli_nulls.sort_values("null_pct", ascending=False)
print("\nPLI Null %:")
print(pli_nulls.to_string(index=False))

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
    cursor.execute(f"SELECT {col}, COUNT(*) AS cnt FROM {PLI} GROUP BY {col} ORDER BY cnt DESC")
    rows = cursor.fetchall()
    cursor.close()
    df = pd.DataFrame(rows, columns=[col, "cnt"])
    df["pct"] = (100.0 * df["cnt"] / pli_count).round(2)
    print(f"\n  >> {col}:")
    print(df.head(20).to_string(index=False))

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
r = cursor.fetchall()[0]
cursor.close()
pli_num_cols = ["limit_increase_amt", "limit_decrease_amt", "cli_offer", "model_score",
                "cv_score", "decile", "new_decile", "opn_prod_cnt", "actv_prod_cnt",
                "avg_yrs_rbc", "csr_interactions"]
print(pd.DataFrame([{"column": pli_num_cols[i], "min": r[i*3], "max": r[i*3+1], "avg": r[i*3+2]} for i in range(len(pli_num_cols))]).to_string(index=False))

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
r = cursor.fetchall()[0]
cursor.close()
ch_labels = ["cc", "dm", "do", "ec", "em", "im", "in", "iu", "iv", "mb", "rd"]
print(pd.DataFrame([{"channel": ch_labels[i], "rate_pct": r[i]} for i in range(len(ch_labels))]).to_string(index=False))

for col in ["channel", "response_channel", "response_source"]:
    cursor = EDW.cursor()
    cursor.execute(f"SELECT {col}, COUNT(*) AS cnt FROM {PLI} GROUP BY {col} ORDER BY cnt DESC")
    rows = cursor.fetchall()
    cursor.close()
    df = pd.DataFrame(rows, columns=[col, "cnt"])
    df["pct"] = (100.0 * df["cnt"] / pli_count).round(2)
    print(f"\n  >> {col}:")
    print(df.head(20).to_string(index=False))

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
r = cursor.fetchall()[0]
cursor.close()
print(pd.DataFrame([r], columns=["responder_cli_pct", "decisioned_pct", "student_pct"]).to_string(index=False))

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
r = cursor.fetchall()[0]
cursor.close()
print(pd.DataFrame([r], columns=["oando_pct", "actioned_pct", "pending_pct", "declined_pct", "approved_pct"]).to_string(index=False))

# --- PLI: Email ---
print("\n--- PLI Email ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(tactic_email AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(email_status AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {PLI}
""")
r = cursor.fetchall()[0]
cursor.close()
print(pd.DataFrame([r], columns=["tactic_email_pct", "email_status_pct"]).to_string(index=False))

cursor = EDW.cursor()
cursor.execute(f"SELECT email_disposition, COUNT(*) AS cnt FROM {PLI} GROUP BY email_disposition ORDER BY cnt DESC")
rows = cursor.fetchall()
cursor.close()
df = pd.DataFrame(rows, columns=["email_disposition", "cnt"])
df["pct"] = (100.0 * df["cnt"] / pli_count).round(2)
print("\n  >> email_disposition:")
print(df.to_string(index=False))

# --- PLI: Digital ---
print("\n--- PLI Digital ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(olb AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(mb AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {PLI}
""")
r = cursor.fetchall()[0]
cursor.close()
print(pd.DataFrame([r], columns=["olb_pct", "mb_pct"]).to_string(index=False))

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
r = cursor.fetchall()[0]
cursor.close()
dig_cols = ["impression_olb", "clicked_olb", "impression_mb", "clicked_mb", "mobile_banner", "mobile_offer_hub"]
print(pd.DataFrame([{"column": dig_cols[i], "min": r[i*3], "max": r[i*3+1], "avg": r[i*3+2]} for i in range(len(dig_cols))]).to_string(index=False))

# --- PLI: SPID & Model ---
print("\n--- PLI SPID & Model ---")
for col in ["spid", "spid_label", "decile", "new_decile"]:
    cursor = EDW.cursor()
    cursor.execute(f"SELECT {col}, COUNT(*) AS cnt FROM {PLI} GROUP BY {col} ORDER BY cnt DESC")
    rows = cursor.fetchall()
    cursor.close()
    df = pd.DataFrame(rows, columns=[col, "cnt"])
    df["pct"] = (100.0 * df["cnt"] / pli_count).round(2)
    print(f"\n  >> {col}:")
    print(df.head(20).to_string(index=False))

cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(pb_client AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(premier_client AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(tsne_ind AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {PLI}
""")
r = cursor.fetchall()[0]
cursor.close()
print("\n  Client type flags:")
print(pd.DataFrame([r], columns=["pb_client_pct", "premier_pct", "tsne_pct"]).to_string(index=False))


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

# --- TPA: Date ranges ---
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT MIN(report_dt), MAX(report_dt),
           MIN(treatmt_start_dt), MAX(treatmt_start_dt),
           MIN(treatmt_end_dt), MAX(treatmt_end_dt),
           MIN(response_dt), MAX(response_dt)
    FROM {TPA}
""")
r = cursor.fetchall()[0]
cursor.close()
print("\nTPA Date Ranges:")
for label, mn, mx in [("report_dt", r[0], r[1]), ("treatmt_start_dt", r[2], r[3]),
                       ("treatmt_end_dt", r[4], r[5]), ("response_dt", r[6], r[7])]:
    print(f"  {label}: {mn}  to  {mx}")

# --- TPA: Null % per column ---
tpa_col_names = [
    "report_dt", "mnemonic", "clnt_no", "like_for_like_group", "tactic_id", "decsn_year",
    "decsn_month", "target_seg", "cmpgn_seg", "strtgy_seg_typ", "act_ctl_seg", "strtgy_seg_cd",
    "tpa_ita", "channel", "chnl_dm", "chnl_do", "chnl_ec", "chnl_em", "chnl_im", "chnl_in",
    "chnl_iu", "chnl_iv", "chnl_mb", "chnl_md", "chnl_rd", "chnl_em_reminder",
    "offer_prod_latest_group", "offer_prod_latest", "offer_prod_latest_name",
    "offer_rate_latest", "offer_rate_months_latest", "offer_fee_waiver_months_latest",
    "offer_fee_waiver_latest", "offer_bonus_points_latest", "offer_description_latest",
    "offer_cr_lmt_latest", "test_group_latest", "treatmt_start_dt", "treatmt_end_dt",
    "response_dt", "acct_no", "days_to_respond", "app_approved", "app_completed",
    "response_channel", "response_channel_grp", "product_applied", "product_applied_name",
    "num_coapps", "num_auth_users", "cr_lmt_approved", "asc_on_app", "asc_on_app_source",
    "times_targeted", "model_score", "expected_value", "model_score_decile",
    "expected_value_decile", "csr_interactions", "oando", "oando_actioned", "oando_pending",
    "oando_declined", "oando_approved", "tactic_email", "email_disposition", "email_status",
    "impression_olb", "clicked_olb", "cv_score", "impression_mb", "clicked_mb", "mobile_banner",
    "tactic_call", "cntct_atmpt_gnsis", "call_ans_gnsis", "agt_gnsis", "hsbc_ind", "rpc_gnsis"
]
tpa_null_exprs = ", ".join([
    f"CAST(100.0 * SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2))"
    for c in tpa_col_names
])
cursor = EDW.cursor()
cursor.execute(f"SELECT {tpa_null_exprs} FROM {TPA}")
tpa_null_row = cursor.fetchall()[0]
cursor.close()
tpa_nulls = pd.DataFrame({"column": tpa_col_names, "null_pct": list(tpa_null_row)})
tpa_nulls = tpa_nulls.sort_values("null_pct", ascending=False)
print("\nTPA Null %:")
print(tpa_nulls.to_string(index=False))

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
    cursor.execute(f"SELECT {col}, COUNT(*) AS cnt FROM {TPA} GROUP BY {col} ORDER BY cnt DESC")
    rows = cursor.fetchall()
    cursor.close()
    df = pd.DataFrame(rows, columns=[col, "cnt"])
    df["pct"] = (100.0 * df["cnt"] / tpa_count).round(2)
    print(f"\n  >> {col}:")
    print(df.head(20).to_string(index=False))

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
r = cursor.fetchall()[0]
cursor.close()
tpa_num_cols = ["offer_rate_latest", "offer_rate_months_latest", "offer_fee_waiver_months_latest",
                "offer_bonus_points_latest", "offer_cr_lmt_latest", "cr_lmt_approved",
                "days_to_respond", "model_score", "expected_value",
                "times_targeted", "num_coapps", "num_auth_users"]
print(pd.DataFrame([{"column": tpa_num_cols[i], "min": r[i*3], "max": r[i*3+1], "avg": r[i*3+2]} for i in range(len(tpa_num_cols))]).to_string(index=False))

# --- TPA: Application funnel ---
print("\n--- TPA Application Funnel ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(app_completed AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(app_approved AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {TPA}
""")
r = cursor.fetchall()[0]
cursor.close()
print(pd.DataFrame([r], columns=["app_completed_pct", "app_approved_pct"]).to_string(index=False))

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
r = cursor.fetchall()[0]
cursor.close()
tpa_ch = ["dm", "do", "ec", "em", "im", "in", "iu", "iv", "mb", "md", "rd", "em_reminder"]
print(pd.DataFrame([{"channel": tpa_ch[i], "rate_pct": r[i]} for i in range(len(tpa_ch))]).to_string(index=False))

cursor = EDW.cursor()
cursor.execute(f"SELECT channel, COUNT(*) AS cnt FROM {TPA} GROUP BY channel ORDER BY cnt DESC")
rows = cursor.fetchall()
cursor.close()
df = pd.DataFrame(rows, columns=["channel", "cnt"])
df["pct"] = (100.0 * df["cnt"] / tpa_count).round(2)
print("\n  >> channel (text):")
print(df.to_string(index=False))

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
r = cursor.fetchall()[0]
cursor.close()
print(pd.DataFrame([r], columns=["oando_pct", "actioned_pct", "pending_pct", "declined_pct", "approved_pct"]).to_string(index=False))

# --- TPA: Email ---
print("\n--- TPA Email ---")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CAST(100.0 * SUM(CAST(tactic_email AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)),
        CAST(100.0 * SUM(CAST(email_status AS INTEGER)) / COUNT(*) AS DECIMAL(5,2))
    FROM {TPA}
""")
r = cursor.fetchall()[0]
cursor.close()
print(pd.DataFrame([r], columns=["tactic_email_pct", "email_status_pct"]).to_string(index=False))

cursor = EDW.cursor()
cursor.execute(f"SELECT email_disposition, COUNT(*) AS cnt FROM {TPA} GROUP BY email_disposition ORDER BY cnt DESC")
rows = cursor.fetchall()
cursor.close()
df = pd.DataFrame(rows, columns=["email_disposition", "cnt"])
df["pct"] = (100.0 * df["cnt"] / tpa_count).round(2)
print("\n  >> email_disposition:")
print(df.to_string(index=False))

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
r = cursor.fetchall()[0]
cursor.close()
tpa_dig = ["impression_olb", "clicked_olb", "impression_mb", "clicked_mb", "mobile_banner"]
print(pd.DataFrame([{"column": tpa_dig[i], "min": r[i*3], "max": r[i*3+1], "avg": r[i*3+2]} for i in range(len(tpa_dig))]).to_string(index=False))

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
r = cursor.fetchall()[0]
cursor.close()
print(pd.DataFrame([r], columns=["tactic_call_pct", "contact_attempt_pct", "call_answered_pct", "agent_pct", "rpc_pct"]).to_string(index=False))

# --- TPA: Time analysis ---
print("\n--- TPA Decision Timing ---")
cursor = EDW.cursor()
cursor.execute(f"SELECT decsn_year, COUNT(*) AS cnt FROM {TPA} GROUP BY decsn_year ORDER BY decsn_year")
rows = cursor.fetchall()
cursor.close()
df = pd.DataFrame(rows, columns=["decsn_year", "cnt"])
df["pct"] = (100.0 * df["cnt"] / tpa_count).round(2)
print("  >> decsn_year:")
print(df.to_string(index=False))

cursor = EDW.cursor()
cursor.execute(f"SELECT decsn_month, COUNT(*) AS cnt FROM {TPA} GROUP BY decsn_month ORDER BY decsn_month")
rows = cursor.fetchall()
cursor.close()
df = pd.DataFrame(rows, columns=["decsn_month", "cnt"])
df["pct"] = (100.0 * df["cnt"] / tpa_count).round(2)
print("\n  >> decsn_month:")
print(df.to_string(index=False))


# ===================================================================
# Section 4: Sample Rows, Duplicate Checks & Data Recency
# ===================================================================

# --- Sample rows ---
print("\n=== Sample Rows ===")
for label, tbl in [("PCD", PCD), ("PLI", PLI), ("TPA", TPA)]:
    cursor = EDW.cursor()
    cursor.execute(f"SELECT * FROM {tbl} SAMPLE 10")
    rows = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]
    cursor.close()
    print(f"\n--- {label}: 10 sample rows ---")
    print(pd.DataFrame(rows, columns=col_names).to_string(index=False))

# --- PCD: Duplicate check ---
print("\n=== Duplicate Check: PCD (acct_no, clnt_no, tactic_id_parent, response_start, response_end) ===")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT cnt_per_key, COUNT(*) AS num_groups
    FROM (
        SELECT acct_no, clnt_no, tactic_id_parent, response_start, response_end,
               COUNT(*) AS cnt_per_key
        FROM {PCD}
        GROUP BY acct_no, clnt_no, tactic_id_parent, response_start, response_end
    ) t
    GROUP BY cnt_per_key
    ORDER BY cnt_per_key DESC
""")
rows = cursor.fetchall()
cursor.close()
pcd_dup = pd.DataFrame(rows, columns=["rows_per_key", "num_groups"])
print(pcd_dup.to_string(index=False))
if len(pcd_dup) == 1 and int(pcd_dup.iloc[0]["rows_per_key"]) == 1:
    print("  => Primary index is UNIQUE — no duplicates.")
else:
    print("  => DUPLICATES FOUND on primary index. Investigate.")

# --- PLI: Duplicate check ---
print("\n=== Duplicate Check: PLI (parent_tactic_id, acct_no, clnt_no) ===")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT cnt_per_key, COUNT(*) AS num_groups
    FROM (
        SELECT parent_tactic_id, acct_no, clnt_no,
               COUNT(*) AS cnt_per_key
        FROM {PLI}
        GROUP BY parent_tactic_id, acct_no, clnt_no
    ) t
    GROUP BY cnt_per_key
    ORDER BY cnt_per_key DESC
""")
rows = cursor.fetchall()
cursor.close()
pli_dup = pd.DataFrame(rows, columns=["rows_per_key", "num_groups"])
print(pli_dup.to_string(index=False))
if len(pli_dup) == 1 and int(pli_dup.iloc[0]["rows_per_key"]) == 1:
    print("  => Primary index is UNIQUE — no duplicates.")
else:
    print("  => DUPLICATES FOUND on primary index. Investigate.")

# --- TPA: Duplicate check ---
print("\n=== Duplicate Check: TPA (clnt_no, tactic_id, target_seg, strtgy_seg_cd, treatmt_start_dt) ===")
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT cnt_per_key, COUNT(*) AS num_groups
    FROM (
        SELECT clnt_no, tactic_id, target_seg, strtgy_seg_cd, treatmt_start_dt,
               COUNT(*) AS cnt_per_key
        FROM {TPA}
        GROUP BY clnt_no, tactic_id, target_seg, strtgy_seg_cd, treatmt_start_dt
    ) t
    GROUP BY cnt_per_key
    ORDER BY cnt_per_key DESC
""")
rows = cursor.fetchall()
cursor.close()
tpa_dup = pd.DataFrame(rows, columns=["rows_per_key", "num_groups"])
print(tpa_dup.to_string(index=False))
if len(tpa_dup) == 1 and int(tpa_dup.iloc[0]["rows_per_key"]) == 1:
    print("  => Primary index is UNIQUE — no duplicates.")
else:
    print("  => DUPLICATES FOUND on primary index. Investigate.")

# --- Data recency ---
print("\n=== Data Recency ===")
cursor = EDW.cursor()
cursor.execute(f"SELECT MAX(response_start), MAX(response_end) FROM {PCD}")
r = cursor.fetchall()[0]
cursor.close()
print(f"  PCD: latest response_start = {r[0]}, latest response_end = {r[1]}")

cursor = EDW.cursor()
cursor.execute(f"SELECT MAX(decision_dt), MAX(actual_strt_dt) FROM {PLI}")
r = cursor.fetchall()[0]
cursor.close()
print(f"  PLI: latest decision_dt = {r[0]}, latest actual_strt_dt = {r[1]}")

cursor = EDW.cursor()
cursor.execute(f"SELECT MAX(report_dt), MAX(treatmt_start_dt), MAX(response_dt) FROM {TPA}")
r = cursor.fetchall()[0]
cursor.close()
print(f"  TPA: latest report_dt = {r[0]}, latest treatmt_start_dt = {r[1]}, latest response_dt = {r[2]}")

# --- Row count summary ---
print("\n=== Row Count Summary ===")
print(f"  PCD Ongoing: {pcd_count:>12,}")
print(f"  PLI:         {pli_count:>12,}")
print(f"  TPA PCQ:     {tpa_count:>12,}")
print(f"  Total:       {pcd_count + pli_count + tpa_count:>12,}")

print("\n=== EDA Complete ===")
