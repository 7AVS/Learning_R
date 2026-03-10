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
# Section 0: Teradata Built-in EDA Functions
# TD_UnivariateStatistics (numeric), TD_ColumnSummary (all types),
# TD_CategoricalSummary (categorical/varchar)
# ===================================================================

# --- Numeric columns per table (for TD_UnivariateStatistics) ---
pcd_numeric_cols = [
    'nibt_expected_value', 'nibt_expec_value_upgradepath', 'channelcost',
    'offer_bonus_cash', 'offer_bonus_points', 'fy_start', 'fy_prod_change',
    'responder_anyproduct', 'responder_targetproduct', 'responder_upgrade_path',
    'student_indicator', 'csr_interactions', 'tactic_email', 'email_status',
    'oando', 'oando_actioned', 'oando_pending', 'oando_declined', 'oando_approved',
    'opn_prod_cnt', 'actv_prod_cnt', 'actv_prod_srvc_cnt', 'ss_act_ind',
    'ss_opn_ind', 'avg_yrs_rbc', 'dor', 'mb', 'olb',
    'impression_olb', 'clicked_olb'
]

pli_numeric_cols = [
    'limit_increase_amt', 'limit_decrease_amt', 'cli_offer',
    'responder_cli', 'decisioned_acct', 'student_indicator',
    'channel_cc', 'channel_dm', 'channel_do', 'channel_ec', 'channel_em',
    'channel_im', 'channel_in', 'channel_iu', 'channel_iv', 'channel_mb', 'channel_rd',
    'channel_period_em_remind', 'em_reminder_control',
    'fy_cmpgn_start', 'fy_cl_change', 'spid', 'model_score', 'decile',
    'opn_prod_cnt', 'actv_prod_cnt', 'actv_prod_srvc_cnt', 'ss_act_ind',
    'ss_opn_ind', 'avg_yrs_rbc', 'dor', 'mb', 'olb',
    'csr_interactions', 'oando', 'oando_actioned', 'oando_pending',
    'oando_declined', 'oando_approved', 'tactic_email', 'email_status',
    'impression_olb', 'clicked_olb', 'mobile_banner', 'mobile_offer_hub',
    'impression_mb', 'clicked_mb', 'pb_client', 'premier_client',
    'cv_score', 'new_decile', 'tsne_ind'
]

tpa_numeric_cols = [
    'decsn_year', 'decsn_month',
    'chnl_dm', 'chnl_do', 'chnl_ec', 'chnl_em', 'chnl_im',
    'chnl_in', 'chnl_iu', 'chnl_iv', 'chnl_mb', 'chnl_md', 'chnl_rd',
    'chnl_em_reminder',
    'offer_rate_latest', 'offer_rate_months_latest',
    'offer_fee_waiver_months_latest', 'offer_bonus_points_latest',
    'offer_cr_lmt_latest',
    'days_to_respond', 'app_approved', 'app_completed',
    'num_coapps', 'num_auth_users', 'cr_lmt_approved',
    'times_targeted', 'model_score', 'expected_value',
    'model_score_decile', 'expected_value_decile',
    'csr_interactions', 'oando', 'oando_actioned', 'oando_pending',
    'oando_declined', 'oando_approved', 'tactic_email', 'email_status',
    'impression_olb', 'clicked_olb', 'cv_score',
    'impression_mb', 'clicked_mb', 'mobile_banner',
    'tactic_call', 'cntct_atmpt_gnsis', 'call_ans_gnsis',
    'agt_gnsis', 'rpc_gnsis'
]

# --- Categorical columns per table (for TD_CategoricalSummary) ---
pcd_cat_cols = [
    'mnemonic', 'product_at_decision', 'product_grouping_at_decision',
    'product_name_at_decision', 'relationship_mgmt', 'channels',
    'responder', 'strategy_seg_cd', 'cmpgn_seg', 'act_ctl_seg',
    'fulfillment_channel', 'credit_phase', 'wallet_band',
    'life_stage', 'bi_clnt_seg', 'tibc', 'age_band',
    'new_comer', 'ngen', 'ias', 'rbc_tenure', 'value_for_money',
    'hsbc_ind', 'invitation_to_upgrade'
]

pli_cat_cols = [
    'mnemonic', 'cellcode', 'tst_grp_cd', 'strategy_id', 'rpt_grp_cd',
    'offer_description', 'increase_decrease', 'action_code', 'channel',
    'product_current', 'product_name_current', 'product_grouping_current',
    'wave', 'wave2', 'test', 'response_channel', 'response_source',
    'credit_phase', 'wallet_band', 'life_stage', 'bi_clnt_seg',
    'tibc', 'age_band', 'new_comer', 'ngen', 'ias',
    'rbc_tenure', 'value_for_money', 'hsbc_ind', 'usage_behaviour',
    'low_grow_ind', 'low_revenue_ind', 'multi_card_ind', 'olb_active_90',
    'active', 'cpc_dni', 'owner', 'new_to_campaign',
    'spid_label', 'like_for_like', 'like_for_like_label',
    'pcl_expansion_pop', 'mobile_active_at_decision', 'newimm_seg'
]

tpa_cat_cols = [
    'mnemonic', 'like_for_like_group', 'tactic_id',
    'target_seg', 'cmpgn_seg', 'strtgy_seg_typ',
    'act_ctl_seg', 'strtgy_seg_cd', 'tpa_ita', 'channel',
    'offer_prod_latest_group', 'offer_prod_latest',
    'offer_prod_latest_name', 'offer_fee_waiver_latest',
    'offer_description_latest', 'test_group_latest',
    'response_channel', 'response_channel_grp',
    'product_applied', 'product_applied_name',
    'asc_on_app', 'asc_on_app_source',
    'email_disposition', 'hsbc_ind'
]

# --- All columns per table (for TD_ColumnSummary) ---
pcd_all_cols = [
    'acct_no', 'clnt_no', 'tactic_id_parent', 'response_start', 'response_end',
    'mnemonic', 'fy_start', 'treatmt_mn', 'product_at_decision',
    'product_grouping_at_decision', 'product_name_at_decision', 'relationship_mgmt',
    'offer_bonus_cash', 'offer_bonus_points', 'offer_description',
    'invitation_to_upgrade', 'target_product', 'target_product_name',
    'target_product_grouping', 'channel_deploy_cc', 'channel_deploy_dm',
    'channel_deploy_do', 'channel_deploy_im', 'channel_deploy_em',
    'channel_deploy_rd', 'channel_deploy_iv', 'channel_em_reminder',
    'channelcost', 'channels', 'dt_prod_change', 'fy_prod_change',
    'month_prod_change', 'new_product', 'nibt_expected_value',
    'nibt_expec_value_upgradepath', 'report_groups_period', 'test_groups_period',
    'responder', 'responder_anyproduct', 'responder_targetproduct',
    'responder_upgrade_path', 'strategy_seg_cd', 'cmpgn_seg', 'strtgy_seg_desc',
    'act_ctl_seg', 'student_indicator', 'success_cd_1', 'success_cd_2',
    'success_dt_1', 'success_dt_2', 'weeknum_response', 'csr_interactions',
    'test_description', 'test_value', 'tactic_email', 'email_disposition',
    'email_status', 'oando', 'oando_actioned', 'oando_pending', 'oando_declined',
    'oando_approved', 'fulfillment_channel', 'gu', 'active', 'opn_prod_cnt',
    'actv_prod_cnt', 'actv_prod_srvc_cnt', 'ss_act_ind', 'ss_opn_ind',
    'avg_yrs_rbc', 'rbc_tenure', 'life_stage', 'value_for_money', 'bi_clnt_seg',
    'vulnrblty_cd', 'mny_in_potntl_cd', 'mny_out_potntl_cd',
    'lifetm_val_5yr_clnt_cd', 'credit_phase', 'wallet_band', 'new_comer',
    'ngen', 'ias', 'age_band', 'tibc', 'dor', 'mb', 'olb',
    'impression_olb', 'clicked_olb', 'hsbc_ind'
]

pli_all_cols = [
    'parent_tactic_id', 'acct_no', 'clnt_no', 'cellcode', 'tst_grp_cd',
    'strategy_id', 'rpt_grp_cd', 'newimm_seg', 'dt_cl_change',
    'limit_increase_amt', 'responder_cli', 'offer_description', 'decision_dt',
    'increase_decrease', 'actual_strt_dt', 'parent_actual_strt_dt',
    'treatmt_strt_dt', 'treatmt_end_dt', 'action_code', 'mnemonic', 'channel',
    'channel_cc', 'channel_dm', 'channel_do', 'channel_ec', 'channel_em',
    'channel_im', 'channel_in', 'channel_iu', 'channel_iv', 'channel_mb',
    'channel_rd', 'product_current', 'product_name_current',
    'product_grouping_current', 'wave', 'test_groups_period', 'parent_test_group',
    'dm_redeploy_test_grp', 'em_redeploy_test_grp', 'wave2', 'limit_decrease_amt',
    'report_groups_period', 'action_code_period', 'parent_tactic_id_period',
    'report_date', 'fy_cmpgn_start', 'month_cmpgn_start', 'fy_cl_change',
    'month_cl_change', 'like_for_like', 'like_for_like_label', 'test',
    'decisioned_acct', 'student_indicator', 'cli_offer', 'response_channel',
    'response_source', 'channel_period_em_remind', 'em_reminder_control',
    'pcl_expansion_pop', 'spid', 'spid_label', 'spid_proc_dt', 'model_score',
    'decile', 'new_to_campaign', 'owner', 'cpc_dni',
    'mobile_active_at_decision', 'dm_creative_id', 'em_creative_id',
    'low_grow_ind', 'low_revenue_ind', 'multi_card_ind', 'olb_active_90',
    'gu', 'active', 'opn_prod_cnt', 'actv_prod_cnt', 'actv_prod_srvc_cnt',
    'ss_act_ind', 'ss_opn_ind', 'avg_yrs_rbc', 'rbc_tenure', 'life_stage',
    'value_for_money', 'bi_clnt_seg', 'vulnrblty_cd', 'mny_in_potntl_cd',
    'mny_out_potntl_cd', 'lifetm_val_5yr_clnt_cd', 'credit_phase', 'wallet_band',
    'new_comer', 'ngen', 'ias', 'age_band', 'tibc', 'dor', 'mb', 'olb',
    'csr_interactions', 'oando', 'oando_actioned', 'oando_pending',
    'oando_declined', 'oando_approved', 'tactic_email', 'email_disposition',
    'email_status', 'impression_olb', 'clicked_olb', 'hsbc_ind',
    'usage_behaviour', 'cv_score', 'new_decile', 'mobile_banner',
    'mobile_offer_hub', 'impression_mb', 'pb_client', 'premier_client',
    'dt_acct_open', 'clicked_mb', 'tsne_ind'
]

tpa_all_cols = [
    'report_dt', 'mnemonic', 'clnt_no', 'like_for_like_group', 'tactic_id',
    'decsn_year', 'decsn_month', 'target_seg', 'cmpgn_seg', 'strtgy_seg_typ',
    'act_ctl_seg', 'strtgy_seg_cd', 'tpa_ita', 'channel', 'chnl_dm', 'chnl_do',
    'chnl_ec', 'chnl_em', 'chnl_im', 'chnl_in', 'chnl_iu', 'chnl_iv',
    'chnl_mb', 'chnl_md', 'chnl_rd', 'chnl_em_reminder',
    'offer_prod_latest_group', 'offer_prod_latest', 'offer_prod_latest_name',
    'offer_rate_latest', 'offer_rate_months_latest',
    'offer_fee_waiver_months_latest', 'offer_fee_waiver_latest',
    'offer_bonus_points_latest', 'offer_description_latest',
    'offer_cr_lmt_latest', 'test_group_latest', 'treatmt_start_dt',
    'treatmt_end_dt', 'response_dt', 'acct_no', 'days_to_respond',
    'app_approved', 'app_completed', 'response_channel', 'response_channel_grp',
    'product_applied', 'product_applied_name', 'num_coapps', 'num_auth_users',
    'cr_lmt_approved', 'asc_on_app', 'asc_on_app_source', 'times_targeted',
    'model_score', 'expected_value', 'model_score_decile',
    'expected_value_decile', 'csr_interactions', 'oando', 'oando_actioned',
    'oando_pending', 'oando_declined', 'oando_approved', 'tactic_email',
    'email_disposition', 'email_status', 'impression_olb', 'clicked_olb',
    'cv_score', 'impression_mb', 'clicked_mb', 'mobile_banner', 'tactic_call',
    'cntct_atmpt_gnsis', 'call_ans_gnsis', 'agt_gnsis', 'hsbc_ind', 'rpc_gnsis'
]

# --- 0a: TD_ColumnSummary — metadata & null profiling (all column types) ---
print("\n" + "=" * 70)
print("SECTION 0a: TD_ColumnSummary — Column profiling (all types)")
print("=" * 70)

for label, tbl, all_cols in [
    ("PCD", PCD, pcd_all_cols),
    ("PLI", PLI, pli_all_cols),
    ("TPA", TPA, tpa_all_cols)
]:
    print(f"\n--- {label}: TD_ColumnSummary ---")
    batch_size = 10
    for i in range(0, len(all_cols), batch_size):
        batch = all_cols[i:i + batch_size]
        target = ", ".join([f"'{c}'" for c in batch])
        print(f"  Batch {i // batch_size + 1}: {', '.join(batch)}")
        cursor = EDW.cursor()
        sql = f"SELECT * FROM TD_ColumnSummary (ON {tbl} AS InputTable USING TargetColumns({target})) AS dt"
        cursor.execute(sql)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        cursor.close()
        df = pd.DataFrame(rows, columns=cols)
        print(df.to_string(index=False))
        print()

# --- 0b: TD_UnivariateStatistics — numeric summary stats ---
print("\n" + "=" * 70)
print("SECTION 0b: TD_UnivariateStatistics — Numeric summary stats")
print("=" * 70)

stats_list = (
    "'MEAN', 'MEDIAN', 'MINIMUM', 'MAXIMUM', "
    "'STANDARD DEVIATION', 'VARIANCE', 'NULL COUNT', "
    "'SKEWNESS', 'KURTOSIS', 'RANGE', "
    "'PERCENTILES', 'UNIQUE ENTITY COUNT'"
)

for label, tbl, num_cols in [
    ("PCD", PCD, pcd_numeric_cols),
    ("PLI", PLI, pli_numeric_cols),
    ("TPA", TPA, tpa_numeric_cols)
]:
    print(f"\n--- {label}: TD_UnivariateStatistics ---")
    # Process in batches of 10 to avoid overloading
    batch_size = 10
    for i in range(0, len(num_cols), batch_size):
        batch = num_cols[i:i + batch_size]
        target = ", ".join([f"'{c}'" for c in batch])
        print(f"  Batch {i // batch_size + 1}: {', '.join(batch)}")
        cursor = EDW.cursor()
        sql = f"SELECT * FROM TD_UnivariateStatistics (ON {tbl} AS InputTable USING TargetColumns({target}) Stats({stats_list})) AS dt"
        cursor.execute(sql)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        cursor.close()
        df = pd.DataFrame(rows, columns=cols)
        print(df.to_string(index=False))
        print()

# --- 0c: TD_CategoricalSummary — categorical value distributions ---
print("\n" + "=" * 70)
print("SECTION 0c: TD_CategoricalSummary — Categorical distributions")
print("=" * 70)

for label, tbl, cat_cols in [
    ("PCD", PCD, pcd_cat_cols),
    ("PLI", PLI, pli_cat_cols),
    ("TPA", TPA, tpa_cat_cols)
]:
    print(f"\n--- {label}: TD_CategoricalSummary ---")
    batch_size = 10
    for i in range(0, len(cat_cols), batch_size):
        batch = cat_cols[i:i + batch_size]
        target = ", ".join([f"'{c}'" for c in batch])
        print(f"  Batch {i // batch_size + 1}: {', '.join(batch)}")
        cursor = EDW.cursor()
        sql = f"SELECT * FROM TD_CategoricalSummary (ON {tbl} AS InputTable USING TargetColumns({target})) AS dt"
        cursor.execute(sql)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        cursor.close()
        df = pd.DataFrame(rows, columns=cols)
        print(df.to_string(index=False))
        print()

print("\n" + "=" * 70)
print("Section 0 complete — Teradata built-in EDA done.")
print("=" * 70)
