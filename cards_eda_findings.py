"""
Cards Decision & Response — EDA Findings
=========================================
Generated: 2026-03-11
Source: cards_eda_v2.py output (HTML report)
Tables: dl_mr_prod (accessed via EDW/Trino)

==========================================================================
1. ROW COUNTS
==========================================================================

Table                   Rows
-----------------------|--------------
PCD Ongoing             17,076,178
PLI                     20,427,387
TPA PCQ                 30,489,940
TOTAL                   67,993,505


==========================================================================
2. DATA RECENCY
==========================================================================

PCD:
  - response_start: 2022-12-01 to 2026-04-17
  - response_end:   range visible in report
  - dt_prod_change: range visible in report
  - success_dt_1/2: range visible in report

PLI:
  - decision_dt:        None / None  *** 100% NULL — USELESS COLUMN ***
  - actual_strt_dt:     2022-11-24 to 2026-03-05
  - dt_acct_open:       1972-12-07 to 2025-07-31
  - spid_proc_dt:       range in report

TPA:
  - report_dt:          2023-07-27 to 2026-03-10
  - treatmt_start_dt:   range in report
  - response_dt:        range in report


==========================================================================
3. DUPLICATE KEY ANALYSIS
==========================================================================

PCD (acct_no, clnt_no, tactic_id_parent, response_start, response_end):
  - rows_per_key=1, num_groups=17,076,178
  - CLEAN — no duplicates. Primary index is unique.

PLI (parent_tactic_id, acct_no, clnt_no):
  - rows_per_key=1: 20,427,221 groups
  - rows_per_key=2: 83 groups
  - MINOR — 83 duplicate key pairs (166 rows). Low risk but investigate.

TPA (clnt_no, tactic_id, target_seg, strtgy_seg_cd, treatmt_start_dt):
  - rows_per_key=1:  30,286,257
  - rows_per_key=2:  98,571
  - rows_per_key=3:  1,045
  - rows_per_key=4+:  goes up to 10
  - SIGNIFICANT — ~200K+ rows involved in duplicates. NEEDS INVESTIGATION.
  - Possible causes: multiple offers per treatment period, re-targeting, data load issues.


==========================================================================
4. NULL ANALYSIS — CRITICAL COLUMNS (100% NULL)
==========================================================================

PCD (100% null):
  - test_value
  - test_description

PLI (100% null):
  - cellcode
  - newimm_seg
  - like_for_like_label
  - decision_dt

TPA (100% null):
  - like_for_like_group
  - chnl_lu
  - chnl_ln
  - chnl_ec
  - offer_fee_waiver_months_latest

These columns carry zero information. Can be excluded from any analysis.


==========================================================================
5. NULL ANALYSIS — HIGH NULL COLUMNS (>80%)
==========================================================================

PCD:
  - success_cd_1: 98.40%
  - nibt_expected_value: ~98% (and related nibt fields)
  - fulfillment_channel: ~98%
  - success_cd_2, success_dt_1, success_dt_2: ~98%
  - email/OLB fields: moderate nulls

PLI:
  - Demographic fields (credit_phase, life_stage, age_band, wallet_band,
    bi_clnt_seg, new_comer, ngen, ias): all ~87.30% null
  - limit_increase_amt: 82.60%
  - Response fields: ~82.50%
  - Mobile fields: ~56%

TPA:
  - Response/application fields: ~97.90%
  - impression/clicked/mobile: ~97.70%
  - tactic_call: 97.40%


==========================================================================
6. KEY CATEGORICAL DISTRIBUTIONS
==========================================================================

PCD:
  - mnemonic: PCD-NBO (100%) — single campaign type
  - product_at_decision: RVC 24.28%, CL2 20.29%, IAV 14.89%
  - relationship_mgmt: TEAM 80.93%
  - responder: 0.No Change 98.03% (only 2% respond)
  - act_ctl_seg: Action 77.69%, Control 22.31%
  - credit_phase: Emerging Prospects 34.53%
  - life_stage: Builders 30.95%
  - age_band: 25-34 = 26.81%
  - wallet_band: RBC Exclusive 46.16%
  - ias: Mass Retail 64.86%
  - hsbc_ind: RBC Only 94.38%
  - new_comer: None 94.43%
  - invitation_to_upgrade: N 95.65%

PLI:
  - mnemonic: NBO-PCL (100%) — single campaign type
  - increase_decrease: No Change 82.52%
  - product_current: IAV 20.55%, CL2 15.47%, RVC 12.50%
  - test: Not CPC/DNI 88.59%, DNI 11.41%
  - usage_behaviour: Transactor 63.13%, Revolver 36.37%
  - spid_label: Mass Market 51.77%, Premium 39.62%
  - hsbc_ind: RBC Only 98.43%
  - new_to_campaign: Not New 88.01%

TPA:
  - mnemonic: pcq (100%) — single campaign type
  - target_seg: BAU 88.92%, LNG 7.14%, L&G 2.36%
  - act_ctl_seg: Action 95.51%, Control 4.49%
  - cmpgn_seg: NextGen-3 49.34%
  - offer_prod_latest_group: Mass Market 46.04%
  - offer_prod_latest: IAV 29.54%, ION 17.76%
  - hsbc_ind: RBC Only 97.14%


==========================================================================
7. NUMERIC SUMMARIES
==========================================================================

PCD:
  - nibt_expected_value: 175 - 729, avg 465.75
  - offer_bonus_points: 6,000 - 35,000, avg 21,096
  - channelcost: range in report
  - opn_prod_cnt, actv_prod_cnt, avg_yrs_rbc: ranges in report

PLI:
  - limit_increase_amt: 0 - 300,500, avg 6,497.71
  - limit_decrease_amt: -40,000 to -100, avg -8,726.65
  - cli_offer: 500 - 15,000, avg 7,157.43
  - model_score: -999,999 to 10, avg 2.08
    *** min=-999999 is a SENTINEL VALUE — filter these in analysis ***
  - cv_score: range in report

TPA:
  - offer_rate_latest: 1.75 - 5.00
  - offer_bonus_points_latest: 1,000 - 35,000, avg 23,654
  - model_score: 0.00 - 1.00, avg 0.17 (proper probability)
  - expected_value: 0.00 - 874.00, avg 52.66
  - times_targeted: 1 - 63, avg 15.56
  - days_to_respond: range in report
  - cr_lmt_approved: range in report


==========================================================================
8. RESPONSE & CONVERSION RATES
==========================================================================

PCD:
  - resp_any_pct: 2.00%
  - resp_target_pct: 1.60%
  - resp_upgrade_path: in report
  - Very low conversion — 98% no change

PLI:
  - responder_cli_pct: 17.30%
  - decisioned_acct: 100.00% (all decisioned)
  - Better conversion than PCD

TPA:
  - app_completed_pct: 2.10%
  - app_approved_pct: 1.00%
  - Very low application funnel — 97.9% don't even apply


==========================================================================
9. CHANNEL DEPLOYMENT
==========================================================================

PCD (flag-based Y/N):
  - channel_deploy_cc: Y 77.39%
  - channel_deploy_dm: values in report
  - channel_deploy_rd: N 100% (never used)

PLI (binary flags):
  - cc: 100%, do: 100%, in: 100% (always on)
  - rd: 0% (never used)

TPA (binary flags):
  - do: 90.50%
  - em: 65.20%
  - in: 100% (always on)
  - mb: 34.70%
  - iv: 24.10%


==========================================================================
10. OANDO (OFFERS AND OPPORTUNITIES) FUNNEL
==========================================================================

         oando%  actioned%  pending%  declined%  approved%
PCD      77.40   6.30       varies    varies     varies
PLI     100.00   20.50      9.20      4.70       15.60
TPA      72.10   3.90       varies    varies     1.70

PLI has 100% OandO and highest actioned rate.
TPA has lowest actioned rate.


==========================================================================
11. EMAIL & DIGITAL
==========================================================================

PCD:
  - tactic_email_pct: 56.40%
  - email disposition: None 43.59%, eMail Open 36.43%
  - olb_pct: 45.30%, mb_pct: 79.10%
  - dor_pct: 286.90%  *** ANOMALY — over 100%, investigate ***

PLI:
  - tactic_email_pct: 58.30%
  - olb_pct: 5.70%, mb_pct: 30.00%
  - impression_olb max: 4, clicked_olb max: 2

TPA:
  - tactic_email_pct: 35.50%
  - email disposition: None 64.48%, eMail Open 19.56%
  - impression_olb max: 2, clicked_mb max: 1


==========================================================================
12. TPA CALL CENTER (GENESIS)
==========================================================================

  - tactic_call_pct: 2.60%
  - contact_attempt_pct: 3.00%
  - call_answered_pct: in report
  - agent_pct: in report
  - rpc_pct: in report


==========================================================================
13. TPA DECISION TIMING
==========================================================================

  decsn_year:
    2022: 27.87%
    2023: 22.04%
    2024: 21.01%
    2025: 24.74%
    2026: 4.34% (partial year)

  decsn_month: roughly even 7-10% each


==========================================================================
14. ISSUES & ACTION ITEMS
==========================================================================

CRITICAL:
  [1] TPA duplicate keys — ~200K+ rows with duplicate primary index.
      ACTION: Investigate root cause. Check if multiple offers per
      treatment period, re-targeting, or data load issues.

  [2] PLI model_score min = -999999 — sentinel/missing value coded as number.
      ACTION: Filter model_score <= -999999 in any model analysis.

  [3] PCD dor_pct = 286.90% — value exceeds 100%.
      ACTION: Check if 'dor' is a count not a flag. If count, don't
      use percentage interpretation.

MODERATE:
  [4] PLI decision_dt is 100% NULL across all 20.4M rows.
      ACTION: Confirm if this field is deprecated or loaded elsewhere.

  [5] Multiple columns 100% null (test_value, test_description in PCD;
      cellcode, newimm_seg in PLI; like_for_like_group in TPA).
      ACTION: Exclude from analysis. Confirm if deprecated.

  [6] PLI 83 duplicate key pairs.
      ACTION: Low priority but worth understanding.

  [7] High null rates on demographic fields in PLI (~87%) vs PCD (~3-4%).
      ACTION: Understand why PLI demographic data is so sparse.
      Possible join enrichment issue.

LOW:
  [8] All three tables have single mnemonic (PCD-NBO, NBO-PCL, pcq).
      These are campaign-specific tables, not general customer tables.

  [9] Response rates very low (PCD 2%, TPA 1% approval).
      Normal for credit card offer campaigns but worth benchmarking.


==========================================================================
15. CROSS-TABLE OBSERVATIONS
==========================================================================

  - All three tables share: oando fields, email fields, demographic fields
    (credit_phase, wallet_band, life_stage, bi_clnt_seg, etc.), digital
    fields (impression_olb, clicked_olb).

  - Common join keys: acct_no, clnt_no appear in all three tables.

  - PCD has best demographic coverage (~3-4% null).
    PLI has worst (~87% null). TPA varies.

  - Product overlap: IAV, CL2, RVC, ION appear across tables.

  - hsbc_ind: ~94-98% "RBC Only" across all tables.

  - Each table represents a different campaign type:
    PCD = Product Change/Upgrade offers
    PLI = Pre-approved Credit Limit Increases
    TPA = Third Party Acquisition (new card applications)
"""
