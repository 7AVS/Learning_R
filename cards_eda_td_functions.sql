-- ===================================================================
-- Cards Decision & Response -- Teradata Built-in EDA
-- Run in Teradata SQL Assistant / Studio
-- ===================================================================

-- -------------------------------------------------------------------
-- SECTION 1: TD_ColumnSummary -- Column profiling (all column types)
-- -------------------------------------------------------------------

-- PCD: TD_ColumnSummary Batch 1 (cols 1-10: acct_no, clnt_no, tactic_id_parent, response_start, response_end, mnemonic, fy_start, treatmt_mn, product_at_decision, product_grouping_at_decision)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('acct_no', 'clnt_no', 'tactic_id_parent', 'response_start', 'response_end', 'mnemonic', 'fy_start', 'treatmt_mn', 'product_at_decision', 'product_grouping_at_decision')) AS dt;

-- PCD: TD_ColumnSummary Batch 2 (cols 11-20: product_name_at_decision, relationship_mgmt, offer_bonus_cash, offer_bonus_points, offer_description, invitation_to_upgrade, target_product, target_product_name, target_product_grouping, channel_deploy_cc)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('product_name_at_decision', 'relationship_mgmt', 'offer_bonus_cash', 'offer_bonus_points', 'offer_description', 'invitation_to_upgrade', 'target_product', 'target_product_name', 'target_product_grouping', 'channel_deploy_cc')) AS dt;

-- PCD: TD_ColumnSummary Batch 3 (cols 21-30: channel_deploy_dm, channel_deploy_do, channel_deploy_im, channel_deploy_em, channel_deploy_rd, channel_deploy_iv, channel_em_reminder, channelcost, channels, dt_prod_change)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('channel_deploy_dm', 'channel_deploy_do', 'channel_deploy_im', 'channel_deploy_em', 'channel_deploy_rd', 'channel_deploy_iv', 'channel_em_reminder', 'channelcost', 'channels', 'dt_prod_change')) AS dt;

-- PCD: TD_ColumnSummary Batch 4 (cols 31-40: fy_prod_change, month_prod_change, new_product, nibt_expected_value, nibt_expec_value_upgradepath, report_groups_period, test_groups_period, responder, responder_anyproduct, responder_targetproduct)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('fy_prod_change', 'month_prod_change', 'new_product', 'nibt_expected_value', 'nibt_expec_value_upgradepath', 'report_groups_period', 'test_groups_period', 'responder', 'responder_anyproduct', 'responder_targetproduct')) AS dt;

-- PCD: TD_ColumnSummary Batch 5 (cols 41-50: responder_upgrade_path, strategy_seg_cd, cmpgn_seg, strtgy_seg_desc, act_ctl_seg, student_indicator, success_cd_1, success_cd_2, success_dt_1, success_dt_2)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('responder_upgrade_path', 'strategy_seg_cd', 'cmpgn_seg', 'strtgy_seg_desc', 'act_ctl_seg', 'student_indicator', 'success_cd_1', 'success_cd_2', 'success_dt_1', 'success_dt_2')) AS dt;

-- PCD: TD_ColumnSummary Batch 6 (cols 51-60: weeknum_response, csr_interactions, test_description, test_value, tactic_email, email_disposition, email_status, oando, oando_actioned, oando_pending)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('weeknum_response', 'csr_interactions', 'test_description', 'test_value', 'tactic_email', 'email_disposition', 'email_status', 'oando', 'oando_actioned', 'oando_pending')) AS dt;

-- PCD: TD_ColumnSummary Batch 7 (cols 61-70: oando_declined, oando_approved, fulfillment_channel, gu, active, opn_prod_cnt, actv_prod_cnt, actv_prod_srvc_cnt, ss_act_ind, ss_opn_ind)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('oando_declined', 'oando_approved', 'fulfillment_channel', 'gu', 'active', 'opn_prod_cnt', 'actv_prod_cnt', 'actv_prod_srvc_cnt', 'ss_act_ind', 'ss_opn_ind')) AS dt;

-- PCD: TD_ColumnSummary Batch 8 (cols 71-80: avg_yrs_rbc, rbc_tenure, life_stage, value_for_money, bi_clnt_seg, vulnrblty_cd, mny_in_potntl_cd, mny_out_potntl_cd, lifetm_val_5yr_clnt_cd, credit_phase)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('avg_yrs_rbc', 'rbc_tenure', 'life_stage', 'value_for_money', 'bi_clnt_seg', 'vulnrblty_cd', 'mny_in_potntl_cd', 'mny_out_potntl_cd', 'lifetm_val_5yr_clnt_cd', 'credit_phase')) AS dt;

-- PCD: TD_ColumnSummary Batch 9 (cols 81-90: wallet_band, new_comer, ngen, ias, age_band, tibc, dor, mb, olb, impression_olb)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('wallet_band', 'new_comer', 'ngen', 'ias', 'age_band', 'tibc', 'dor', 'mb', 'olb', 'impression_olb')) AS dt;

-- PCD: TD_ColumnSummary Batch 10 (cols 91-92: clicked_olb, hsbc_ind)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('clicked_olb', 'hsbc_ind')) AS dt;

-- PLI: TD_ColumnSummary Batch 1 (cols 1-10: parent_tactic_id, acct_no, clnt_no, cellcode, tst_grp_cd, strategy_id, rpt_grp_cd, newimm_seg, dt_cl_change, limit_increase_amt)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('parent_tactic_id', 'acct_no', 'clnt_no', 'cellcode', 'tst_grp_cd', 'strategy_id', 'rpt_grp_cd', 'newimm_seg', 'dt_cl_change', 'limit_increase_amt')) AS dt;

-- PLI: TD_ColumnSummary Batch 2 (cols 11-20: responder_cli, offer_description, decision_dt, increase_decrease, actual_strt_dt, parent_actual_strt_dt, treatmt_strt_dt, treatmt_end_dt, action_code, mnemonic)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('responder_cli', 'offer_description', 'decision_dt', 'increase_decrease', 'actual_strt_dt', 'parent_actual_strt_dt', 'treatmt_strt_dt', 'treatmt_end_dt', 'action_code', 'mnemonic')) AS dt;

-- PLI: TD_ColumnSummary Batch 3 (cols 21-30: channel, channel_cc, channel_dm, channel_do, channel_ec, channel_em, channel_im, channel_in, channel_iu, channel_iv)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('channel', 'channel_cc', 'channel_dm', 'channel_do', 'channel_ec', 'channel_em', 'channel_im', 'channel_in', 'channel_iu', 'channel_iv')) AS dt;

-- PLI: TD_ColumnSummary Batch 4 (cols 31-40: channel_mb, channel_rd, product_current, product_name_current, product_grouping_current, wave, test_groups_period, parent_test_group, dm_redeploy_test_grp, em_redeploy_test_grp)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('channel_mb', 'channel_rd', 'product_current', 'product_name_current', 'product_grouping_current', 'wave', 'test_groups_period', 'parent_test_group', 'dm_redeploy_test_grp', 'em_redeploy_test_grp')) AS dt;

-- PLI: TD_ColumnSummary Batch 5 (cols 41-50: wave2, limit_decrease_amt, report_groups_period, action_code_period, parent_tactic_id_period, report_date, fy_cmpgn_start, month_cmpgn_start, fy_cl_change, month_cl_change)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('wave2', 'limit_decrease_amt', 'report_groups_period', 'action_code_period', 'parent_tactic_id_period', 'report_date', 'fy_cmpgn_start', 'month_cmpgn_start', 'fy_cl_change', 'month_cl_change')) AS dt;

-- PLI: TD_ColumnSummary Batch 6 (cols 51-60: like_for_like, like_for_like_label, test, decisioned_acct, student_indicator, cli_offer, response_channel, response_source, channel_period_em_remind, em_reminder_control)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('like_for_like', 'like_for_like_label', 'test', 'decisioned_acct', 'student_indicator', 'cli_offer', 'response_channel', 'response_source', 'channel_period_em_remind', 'em_reminder_control')) AS dt;

-- PLI: TD_ColumnSummary Batch 7 (cols 61-70: pcl_expansion_pop, spid, spid_label, spid_proc_dt, model_score, decile, new_to_campaign, owner, cpc_dni, mobile_active_at_decision)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('pcl_expansion_pop', 'spid', 'spid_label', 'spid_proc_dt', 'model_score', 'decile', 'new_to_campaign', 'owner', 'cpc_dni', 'mobile_active_at_decision')) AS dt;

-- PLI: TD_ColumnSummary Batch 8 (cols 71-80: dm_creative_id, em_creative_id, low_grow_ind, low_revenue_ind, multi_card_ind, olb_active_90, gu, active, opn_prod_cnt, actv_prod_cnt)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('dm_creative_id', 'em_creative_id', 'low_grow_ind', 'low_revenue_ind', 'multi_card_ind', 'olb_active_90', 'gu', 'active', 'opn_prod_cnt', 'actv_prod_cnt')) AS dt;

-- PLI: TD_ColumnSummary Batch 9 (cols 81-90: actv_prod_srvc_cnt, ss_act_ind, ss_opn_ind, avg_yrs_rbc, rbc_tenure, life_stage, value_for_money, bi_clnt_seg, vulnrblty_cd, mny_in_potntl_cd)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('actv_prod_srvc_cnt', 'ss_act_ind', 'ss_opn_ind', 'avg_yrs_rbc', 'rbc_tenure', 'life_stage', 'value_for_money', 'bi_clnt_seg', 'vulnrblty_cd', 'mny_in_potntl_cd')) AS dt;

-- PLI: TD_ColumnSummary Batch 10 (cols 91-100: mny_out_potntl_cd, lifetm_val_5yr_clnt_cd, credit_phase, wallet_band, new_comer, ngen, ias, age_band, tibc, dor)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('mny_out_potntl_cd', 'lifetm_val_5yr_clnt_cd', 'credit_phase', 'wallet_band', 'new_comer', 'ngen', 'ias', 'age_band', 'tibc', 'dor')) AS dt;

-- PLI: TD_ColumnSummary Batch 11 (cols 101-110: mb, olb, csr_interactions, oando, oando_actioned, oando_pending, oando_declined, oando_approved, tactic_email, email_disposition)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('mb', 'olb', 'csr_interactions', 'oando', 'oando_actioned', 'oando_pending', 'oando_declined', 'oando_approved', 'tactic_email', 'email_disposition')) AS dt;

-- PLI: TD_ColumnSummary Batch 12 (cols 111-120: email_status, impression_olb, clicked_olb, hsbc_ind, usage_behaviour, cv_score, new_decile, mobile_banner, mobile_offer_hub, impression_mb)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('email_status', 'impression_olb', 'clicked_olb', 'hsbc_ind', 'usage_behaviour', 'cv_score', 'new_decile', 'mobile_banner', 'mobile_offer_hub', 'impression_mb')) AS dt;

-- PLI: TD_ColumnSummary Batch 13 (cols 121-125: pb_client, premier_client, dt_acct_open, clicked_mb, tsne_ind)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('pb_client', 'premier_client', 'dt_acct_open', 'clicked_mb', 'tsne_ind')) AS dt;

-- TPA: TD_ColumnSummary Batch 1 (cols 1-10: report_dt, mnemonic, clnt_no, like_for_like_group, tactic_id, decsn_year, decsn_month, target_seg, cmpgn_seg, strtgy_seg_typ)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('report_dt', 'mnemonic', 'clnt_no', 'like_for_like_group', 'tactic_id', 'decsn_year', 'decsn_month', 'target_seg', 'cmpgn_seg', 'strtgy_seg_typ')) AS dt;

-- TPA: TD_ColumnSummary Batch 2 (cols 11-20: act_ctl_seg, strtgy_seg_cd, tpa_ita, channel, chnl_dm, chnl_do, chnl_ec, chnl_em, chnl_im, chnl_in)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('act_ctl_seg', 'strtgy_seg_cd', 'tpa_ita', 'channel', 'chnl_dm', 'chnl_do', 'chnl_ec', 'chnl_em', 'chnl_im', 'chnl_in')) AS dt;

-- TPA: TD_ColumnSummary Batch 3 (cols 21-30: chnl_iu, chnl_iv, chnl_mb, chnl_md, chnl_rd, chnl_em_reminder, offer_prod_latest_group, offer_prod_latest, offer_prod_latest_name, offer_rate_latest)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('chnl_iu', 'chnl_iv', 'chnl_mb', 'chnl_md', 'chnl_rd', 'chnl_em_reminder', 'offer_prod_latest_group', 'offer_prod_latest', 'offer_prod_latest_name', 'offer_rate_latest')) AS dt;

-- TPA: TD_ColumnSummary Batch 4 (cols 31-40: offer_rate_months_latest, offer_fee_waiver_months_latest, offer_fee_waiver_latest, offer_bonus_points_latest, offer_description_latest, offer_cr_lmt_latest, test_group_latest, treatmt_start_dt, treatmt_end_dt, response_dt)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('offer_rate_months_latest', 'offer_fee_waiver_months_latest', 'offer_fee_waiver_latest', 'offer_bonus_points_latest', 'offer_description_latest', 'offer_cr_lmt_latest', 'test_group_latest', 'treatmt_start_dt', 'treatmt_end_dt', 'response_dt')) AS dt;

-- TPA: TD_ColumnSummary Batch 5 (cols 41-50: acct_no, days_to_respond, app_approved, app_completed, response_channel, response_channel_grp, product_applied, product_applied_name, num_coapps, num_auth_users)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('acct_no', 'days_to_respond', 'app_approved', 'app_completed', 'response_channel', 'response_channel_grp', 'product_applied', 'product_applied_name', 'num_coapps', 'num_auth_users')) AS dt;

-- TPA: TD_ColumnSummary Batch 6 (cols 51-60: cr_lmt_approved, asc_on_app, asc_on_app_source, times_targeted, model_score, expected_value, model_score_decile, expected_value_decile, csr_interactions, oando)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('cr_lmt_approved', 'asc_on_app', 'asc_on_app_source', 'times_targeted', 'model_score', 'expected_value', 'model_score_decile', 'expected_value_decile', 'csr_interactions', 'oando')) AS dt;

-- TPA: TD_ColumnSummary Batch 7 (cols 61-70: oando_actioned, oando_pending, oando_declined, oando_approved, tactic_email, email_disposition, email_status, impression_olb, clicked_olb, cv_score)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('oando_actioned', 'oando_pending', 'oando_declined', 'oando_approved', 'tactic_email', 'email_disposition', 'email_status', 'impression_olb', 'clicked_olb', 'cv_score')) AS dt;

-- TPA: TD_ColumnSummary Batch 8 (cols 71-79: impression_mb, clicked_mb, mobile_banner, tactic_call, cntct_atmpt_gnsis, call_ans_gnsis, agt_gnsis, hsbc_ind, rpc_gnsis)
SELECT * FROM TD_ColumnSummary (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('impression_mb', 'clicked_mb', 'mobile_banner', 'tactic_call', 'cntct_atmpt_gnsis', 'call_ans_gnsis', 'agt_gnsis', 'hsbc_ind', 'rpc_gnsis')) AS dt;

-- -------------------------------------------------------------------
-- SECTION 2: TD_UnivariateStatistics -- Numeric summary stats
-- -------------------------------------------------------------------

-- PCD: TD_UnivariateStatistics Batch 1 (cols 1-10: nibt_expected_value, nibt_expec_value_upgradepath, channelcost, offer_bonus_cash, offer_bonus_points, fy_start, fy_prod_change, responder_anyproduct, responder_targetproduct, responder_upgrade_path)
SELECT * FROM TD_UnivariateStatistics (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('nibt_expected_value', 'nibt_expec_value_upgradepath', 'channelcost', 'offer_bonus_cash', 'offer_bonus_points', 'fy_start', 'fy_prod_change', 'responder_anyproduct', 'responder_targetproduct', 'responder_upgrade_path') Stats('MEAN', 'MEDIAN', 'MINIMUM', 'MAXIMUM', 'STANDARD DEVIATION', 'VARIANCE', 'NULL COUNT', 'SKEWNESS', 'KURTOSIS', 'RANGE', 'PERCENTILES', 'UNIQUE ENTITY COUNT')) AS dt;

-- PCD: TD_UnivariateStatistics Batch 2 (cols 11-20: student_indicator, csr_interactions, tactic_email, email_status, oando, oando_actioned, oando_pending, oando_declined, oando_approved, opn_prod_cnt)
SELECT * FROM TD_UnivariateStatistics (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('student_indicator', 'csr_interactions', 'tactic_email', 'email_status', 'oando', 'oando_actioned', 'oando_pending', 'oando_declined', 'oando_approved', 'opn_prod_cnt') Stats('MEAN', 'MEDIAN', 'MINIMUM', 'MAXIMUM', 'STANDARD DEVIATION', 'VARIANCE', 'NULL COUNT', 'SKEWNESS', 'KURTOSIS', 'RANGE', 'PERCENTILES', 'UNIQUE ENTITY COUNT')) AS dt;

-- PCD: TD_UnivariateStatistics Batch 3 (cols 21-30: actv_prod_cnt, actv_prod_srvc_cnt, ss_act_ind, ss_opn_ind, avg_yrs_rbc, dor, mb, olb, impression_olb, clicked_olb)
SELECT * FROM TD_UnivariateStatistics (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('actv_prod_cnt', 'actv_prod_srvc_cnt', 'ss_act_ind', 'ss_opn_ind', 'avg_yrs_rbc', 'dor', 'mb', 'olb', 'impression_olb', 'clicked_olb') Stats('MEAN', 'MEDIAN', 'MINIMUM', 'MAXIMUM', 'STANDARD DEVIATION', 'VARIANCE', 'NULL COUNT', 'SKEWNESS', 'KURTOSIS', 'RANGE', 'PERCENTILES', 'UNIQUE ENTITY COUNT')) AS dt;

-- PLI: TD_UnivariateStatistics Batch 1 (cols 1-10: limit_increase_amt, limit_decrease_amt, cli_offer, responder_cli, decisioned_acct, student_indicator, channel_cc, channel_dm, channel_do, channel_ec)
SELECT * FROM TD_UnivariateStatistics (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('limit_increase_amt', 'limit_decrease_amt', 'cli_offer', 'responder_cli', 'decisioned_acct', 'student_indicator', 'channel_cc', 'channel_dm', 'channel_do', 'channel_ec') Stats('MEAN', 'MEDIAN', 'MINIMUM', 'MAXIMUM', 'STANDARD DEVIATION', 'VARIANCE', 'NULL COUNT', 'SKEWNESS', 'KURTOSIS', 'RANGE', 'PERCENTILES', 'UNIQUE ENTITY COUNT')) AS dt;

-- PLI: TD_UnivariateStatistics Batch 2 (cols 11-20: channel_em, channel_im, channel_in, channel_iu, channel_iv, channel_mb, channel_rd, channel_period_em_remind, em_reminder_control, fy_cmpgn_start)
SELECT * FROM TD_UnivariateStatistics (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('channel_em', 'channel_im', 'channel_in', 'channel_iu', 'channel_iv', 'channel_mb', 'channel_rd', 'channel_period_em_remind', 'em_reminder_control', 'fy_cmpgn_start') Stats('MEAN', 'MEDIAN', 'MINIMUM', 'MAXIMUM', 'STANDARD DEVIATION', 'VARIANCE', 'NULL COUNT', 'SKEWNESS', 'KURTOSIS', 'RANGE', 'PERCENTILES', 'UNIQUE ENTITY COUNT')) AS dt;

-- PLI: TD_UnivariateStatistics Batch 3 (cols 21-30: fy_cl_change, spid, model_score, decile, opn_prod_cnt, actv_prod_cnt, actv_prod_srvc_cnt, ss_act_ind, ss_opn_ind, avg_yrs_rbc)
SELECT * FROM TD_UnivariateStatistics (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('fy_cl_change', 'spid', 'model_score', 'decile', 'opn_prod_cnt', 'actv_prod_cnt', 'actv_prod_srvc_cnt', 'ss_act_ind', 'ss_opn_ind', 'avg_yrs_rbc') Stats('MEAN', 'MEDIAN', 'MINIMUM', 'MAXIMUM', 'STANDARD DEVIATION', 'VARIANCE', 'NULL COUNT', 'SKEWNESS', 'KURTOSIS', 'RANGE', 'PERCENTILES', 'UNIQUE ENTITY COUNT')) AS dt;

-- PLI: TD_UnivariateStatistics Batch 4 (cols 31-40: dor, mb, olb, csr_interactions, oando, oando_actioned, oando_pending, oando_declined, oando_approved, tactic_email)
SELECT * FROM TD_UnivariateStatistics (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('dor', 'mb', 'olb', 'csr_interactions', 'oando', 'oando_actioned', 'oando_pending', 'oando_declined', 'oando_approved', 'tactic_email') Stats('MEAN', 'MEDIAN', 'MINIMUM', 'MAXIMUM', 'STANDARD DEVIATION', 'VARIANCE', 'NULL COUNT', 'SKEWNESS', 'KURTOSIS', 'RANGE', 'PERCENTILES', 'UNIQUE ENTITY COUNT')) AS dt;

-- PLI: TD_UnivariateStatistics Batch 5 (cols 41-50: email_status, impression_olb, clicked_olb, mobile_banner, mobile_offer_hub, impression_mb, clicked_mb, pb_client, premier_client, cv_score)
SELECT * FROM TD_UnivariateStatistics (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('email_status', 'impression_olb', 'clicked_olb', 'mobile_banner', 'mobile_offer_hub', 'impression_mb', 'clicked_mb', 'pb_client', 'premier_client', 'cv_score') Stats('MEAN', 'MEDIAN', 'MINIMUM', 'MAXIMUM', 'STANDARD DEVIATION', 'VARIANCE', 'NULL COUNT', 'SKEWNESS', 'KURTOSIS', 'RANGE', 'PERCENTILES', 'UNIQUE ENTITY COUNT')) AS dt;

-- PLI: TD_UnivariateStatistics Batch 6 (cols 51-52: new_decile, tsne_ind)
SELECT * FROM TD_UnivariateStatistics (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('new_decile', 'tsne_ind') Stats('MEAN', 'MEDIAN', 'MINIMUM', 'MAXIMUM', 'STANDARD DEVIATION', 'VARIANCE', 'NULL COUNT', 'SKEWNESS', 'KURTOSIS', 'RANGE', 'PERCENTILES', 'UNIQUE ENTITY COUNT')) AS dt;

-- TPA: TD_UnivariateStatistics Batch 1 (cols 1-10: decsn_year, decsn_month, chnl_dm, chnl_do, chnl_ec, chnl_em, chnl_im, chnl_in, chnl_iu, chnl_iv)
SELECT * FROM TD_UnivariateStatistics (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('decsn_year', 'decsn_month', 'chnl_dm', 'chnl_do', 'chnl_ec', 'chnl_em', 'chnl_im', 'chnl_in', 'chnl_iu', 'chnl_iv') Stats('MEAN', 'MEDIAN', 'MINIMUM', 'MAXIMUM', 'STANDARD DEVIATION', 'VARIANCE', 'NULL COUNT', 'SKEWNESS', 'KURTOSIS', 'RANGE', 'PERCENTILES', 'UNIQUE ENTITY COUNT')) AS dt;

-- TPA: TD_UnivariateStatistics Batch 2 (cols 11-20: chnl_mb, chnl_md, chnl_rd, chnl_em_reminder, offer_rate_latest, offer_rate_months_latest, offer_fee_waiver_months_latest, offer_bonus_points_latest, offer_cr_lmt_latest, days_to_respond)
SELECT * FROM TD_UnivariateStatistics (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('chnl_mb', 'chnl_md', 'chnl_rd', 'chnl_em_reminder', 'offer_rate_latest', 'offer_rate_months_latest', 'offer_fee_waiver_months_latest', 'offer_bonus_points_latest', 'offer_cr_lmt_latest', 'days_to_respond') Stats('MEAN', 'MEDIAN', 'MINIMUM', 'MAXIMUM', 'STANDARD DEVIATION', 'VARIANCE', 'NULL COUNT', 'SKEWNESS', 'KURTOSIS', 'RANGE', 'PERCENTILES', 'UNIQUE ENTITY COUNT')) AS dt;

-- TPA: TD_UnivariateStatistics Batch 3 (cols 21-30: app_approved, app_completed, num_coapps, num_auth_users, cr_lmt_approved, times_targeted, model_score, expected_value, model_score_decile, expected_value_decile)
SELECT * FROM TD_UnivariateStatistics (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('app_approved', 'app_completed', 'num_coapps', 'num_auth_users', 'cr_lmt_approved', 'times_targeted', 'model_score', 'expected_value', 'model_score_decile', 'expected_value_decile') Stats('MEAN', 'MEDIAN', 'MINIMUM', 'MAXIMUM', 'STANDARD DEVIATION', 'VARIANCE', 'NULL COUNT', 'SKEWNESS', 'KURTOSIS', 'RANGE', 'PERCENTILES', 'UNIQUE ENTITY COUNT')) AS dt;

-- TPA: TD_UnivariateStatistics Batch 4 (cols 31-40: csr_interactions, oando, oando_actioned, oando_pending, oando_declined, oando_approved, tactic_email, email_status, impression_olb, clicked_olb)
SELECT * FROM TD_UnivariateStatistics (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('csr_interactions', 'oando', 'oando_actioned', 'oando_pending', 'oando_declined', 'oando_approved', 'tactic_email', 'email_status', 'impression_olb', 'clicked_olb') Stats('MEAN', 'MEDIAN', 'MINIMUM', 'MAXIMUM', 'STANDARD DEVIATION', 'VARIANCE', 'NULL COUNT', 'SKEWNESS', 'KURTOSIS', 'RANGE', 'PERCENTILES', 'UNIQUE ENTITY COUNT')) AS dt;

-- TPA: TD_UnivariateStatistics Batch 5 (cols 41-49: cv_score, impression_mb, clicked_mb, mobile_banner, tactic_call, cntct_atmpt_gnsis, call_ans_gnsis, agt_gnsis, rpc_gnsis)
SELECT * FROM TD_UnivariateStatistics (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('cv_score', 'impression_mb', 'clicked_mb', 'mobile_banner', 'tactic_call', 'cntct_atmpt_gnsis', 'call_ans_gnsis', 'agt_gnsis', 'rpc_gnsis') Stats('MEAN', 'MEDIAN', 'MINIMUM', 'MAXIMUM', 'STANDARD DEVIATION', 'VARIANCE', 'NULL COUNT', 'SKEWNESS', 'KURTOSIS', 'RANGE', 'PERCENTILES', 'UNIQUE ENTITY COUNT')) AS dt;

-- -------------------------------------------------------------------
-- SECTION 3: TD_CategoricalSummary -- Categorical distributions
-- -------------------------------------------------------------------

-- PCD: TD_CategoricalSummary Batch 1 (cols 1-10: mnemonic, product_at_decision, product_grouping_at_decision, product_name_at_decision, relationship_mgmt, channels, responder, strategy_seg_cd, cmpgn_seg, act_ctl_seg)
SELECT * FROM TD_CategoricalSummary (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('mnemonic', 'product_at_decision', 'product_grouping_at_decision', 'product_name_at_decision', 'relationship_mgmt', 'channels', 'responder', 'strategy_seg_cd', 'cmpgn_seg', 'act_ctl_seg')) AS dt;

-- PCD: TD_CategoricalSummary Batch 2 (cols 11-20: fulfillment_channel, credit_phase, wallet_band, life_stage, bi_clnt_seg, tibc, age_band, new_comer, ngen, ias)
SELECT * FROM TD_CategoricalSummary (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('fulfillment_channel', 'credit_phase', 'wallet_band', 'life_stage', 'bi_clnt_seg', 'tibc', 'age_band', 'new_comer', 'ngen', 'ias')) AS dt;

-- PCD: TD_CategoricalSummary Batch 3 (cols 21-24: rbc_tenure, value_for_money, hsbc_ind, invitation_to_upgrade)
SELECT * FROM TD_CategoricalSummary (ON dl_mr_prod.cards_pcd_ongoing_decis_resp AS InputTable USING TargetColumns('rbc_tenure', 'value_for_money', 'hsbc_ind', 'invitation_to_upgrade')) AS dt;

-- PLI: TD_CategoricalSummary Batch 1 (cols 1-10: mnemonic, cellcode, tst_grp_cd, strategy_id, rpt_grp_cd, offer_description, increase_decrease, action_code, channel, product_current)
SELECT * FROM TD_CategoricalSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('mnemonic', 'cellcode', 'tst_grp_cd', 'strategy_id', 'rpt_grp_cd', 'offer_description', 'increase_decrease', 'action_code', 'channel', 'product_current')) AS dt;

-- PLI: TD_CategoricalSummary Batch 2 (cols 11-20: product_name_current, product_grouping_current, wave, wave2, test, response_channel, response_source, credit_phase, wallet_band, life_stage)
SELECT * FROM TD_CategoricalSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('product_name_current', 'product_grouping_current', 'wave', 'wave2', 'test', 'response_channel', 'response_source', 'credit_phase', 'wallet_band', 'life_stage')) AS dt;

-- PLI: TD_CategoricalSummary Batch 3 (cols 21-30: bi_clnt_seg, tibc, age_band, new_comer, ngen, ias, rbc_tenure, value_for_money, hsbc_ind, usage_behaviour)
SELECT * FROM TD_CategoricalSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('bi_clnt_seg', 'tibc', 'age_band', 'new_comer', 'ngen', 'ias', 'rbc_tenure', 'value_for_money', 'hsbc_ind', 'usage_behaviour')) AS dt;

-- PLI: TD_CategoricalSummary Batch 4 (cols 31-40: low_grow_ind, low_revenue_ind, multi_card_ind, olb_active_90, active, cpc_dni, owner, new_to_campaign, spid_label, like_for_like)
SELECT * FROM TD_CategoricalSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('low_grow_ind', 'low_revenue_ind', 'multi_card_ind', 'olb_active_90', 'active', 'cpc_dni', 'owner', 'new_to_campaign', 'spid_label', 'like_for_like')) AS dt;

-- PLI: TD_CategoricalSummary Batch 5 (cols 41-44: like_for_like_label, pcl_expansion_pop, mobile_active_at_decision, newimm_seg)
SELECT * FROM TD_CategoricalSummary (ON dl_mr_prod.cards_pli_decision_resp AS InputTable USING TargetColumns('like_for_like_label', 'pcl_expansion_pop', 'mobile_active_at_decision', 'newimm_seg')) AS dt;

-- TPA: TD_CategoricalSummary Batch 1 (cols 1-10: mnemonic, like_for_like_group, tactic_id, target_seg, cmpgn_seg, strtgy_seg_typ, act_ctl_seg, strtgy_seg_cd, tpa_ita, channel)
SELECT * FROM TD_CategoricalSummary (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('mnemonic', 'like_for_like_group', 'tactic_id', 'target_seg', 'cmpgn_seg', 'strtgy_seg_typ', 'act_ctl_seg', 'strtgy_seg_cd', 'tpa_ita', 'channel')) AS dt;

-- TPA: TD_CategoricalSummary Batch 2 (cols 11-20: offer_prod_latest_group, offer_prod_latest, offer_prod_latest_name, offer_fee_waiver_latest, offer_description_latest, test_group_latest, response_channel, response_channel_grp, product_applied, product_applied_name)
SELECT * FROM TD_CategoricalSummary (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('offer_prod_latest_group', 'offer_prod_latest', 'offer_prod_latest_name', 'offer_fee_waiver_latest', 'offer_description_latest', 'test_group_latest', 'response_channel', 'response_channel_grp', 'product_applied', 'product_applied_name')) AS dt;

-- TPA: TD_CategoricalSummary Batch 3 (cols 21-24: asc_on_app, asc_on_app_source, email_disposition, hsbc_ind)
SELECT * FROM TD_CategoricalSummary (ON dl_mr_prod.cards_tpa_pcq_decision_resp AS InputTable USING TargetColumns('asc_on_app', 'asc_on_app_source', 'email_disposition', 'hsbc_ind')) AS dt;

-- ===================================================================
-- END -- Teradata Built-in EDA complete
-- ===================================================================