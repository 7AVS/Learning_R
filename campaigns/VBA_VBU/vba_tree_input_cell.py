"""
VBA decision tree input — analytical file builder.

Produces one row per VBA responder (gross_response = 1), pulling from:
  1. nbo_vba_rbol_combined        — curated VBA campaign table
                                    (outcomes + targeting + channels + engagement)
  2. DLY_FULL_PORTFOLIO           — pre-treatment spending + attrition history
  3. vba_ucp_business_slice.parquet — UCP-business demographics (already on disk)

Population : mnc='VBA', gross_response=1, treatmt_strt_dt >= 2025-08-01
Target     : visa_app_approved (1 = approved, 0 = declined, conditional on response)
Output     : data/vba_tree_input.parquet  — feed (a subset of) columns to the tree tool.

Notes:
  - Pre-treatment window for spending = 90 days before treatmt_strt_dt
    (PCQ used post-treatment for a different purpose — for a tree predicting
     approval, post-treatment features would leak the outcome).
  - VBA is an upgrade campaign on existing accounts → portfolio data is
    primary card behavior, not new-card behavior.
  - Catalog prefix `dw00_im.d3cv12a.dly_full_portfolio` parallels the existing
    Starburst-style prefix used for `dw00_im.dl_mr_prod.nbo_vba_rbol_combined`.
    If your env exposes the table under a different prefix, adjust here.
"""

import pandas as pd
from pathlib import Path

DATA = Path("/home/jovyan/Cards/VBA/data")
OUT  = Path("/home/jovyan/Cards/VBA/output")
DATA.mkdir(parents=True, exist_ok=True)
OUT.mkdir(parents=True, exist_ok=True)


# ============================================================================
# 1. Curated VBA dataset — population + features + outcomes
# ----------------------------------------------------------------------------
# Excluded intentionally:
#   visa_asc_on_app    — not in VBA curated table (PCQ-only field)
#   rbol_*  (16 cols)  — RBOL campaign, not VBA
#   lmt1/lmt2/rate/gu/blip/cpc_chng/visa_onoff/num_descn/num_target/vbo/nibt
#                       — undocumented in schema
#   chnl_iv/chnl_om/chnl_zz — channel codes not in dictionary
#   oando/oando_pending/oando_declined/oando_approved/agt_gnsis/
#   tactic_call/tactic_email/comparison/treatmt_mn
#                       — undocumented in schema
# ============================================================================
vba_curated_sql = """
SELECT
    -- Identity (kept for row identification, not as tree features)
    clnt_no,
    acct_no,
    visa_acct_no,
    tactic_id,
    report_date,

    -- Treatment metadata (stratifiers — do not feed to the tree as features)
    mnc,
    control,
    test_group,
    tst_grp_cd,
    wave,
    treatmt_strt_dt,
    treatmt_end_dt,
    visa_offer_prod,
    visa_offer_test,
    visa_fee,
    email_creative_id,

    -- Targeting / model output (pre-treatment — OK as features)
    segment,
    model,
    score,
    decile,
    hsbc_ind,
    hsbc_indicator,
    tpa_ita_indicator,
    vba_tpa_rank,
    vba_ita_rank,

    -- Channel exposure (treatment characteristics)
    channel,
    chnl_dm,
    chnl_em,
    chnl_do,
    chnl_im,
    chnl_in,
    chnl_iu,
    chnl_rd,

    -- Engagement / response activity
    -- Timing unconfirmed (pre-treatment history vs campaign-window only).
    -- Keep in the output for inspection; exclude from tree feature set
    -- until timing is verified — otherwise potential leakage.
    csr_interactions,
    cntct_atmpt_gnsis,
    call_ans_gnsis,
    rpc_gnsis,
    email_disposition,
    email_status,
    oando_actioned,
    visa_response_channel,

    -- Outcomes (target lives here — visa_app_approved)
    gross_response,
    net_response,
    response_dt,
    prod_acq,
    cr_lmt,
    visa_app_started,
    visa_app_completed,
    visa_app_approved,
    visa_app_declined,
    visa_date_app_dec,
    visa_response_dt,
    visa_prod_acq,
    visa_cr_lmt

FROM dw00_im.dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc = 'VBA'
  AND gross_response = 1
  AND treatmt_strt_dt >= DATE '2025-08-01'
"""

vba_curated = pd.read_sql_query(vba_curated_sql, con=EDW)
print(f"Curated cohort: {len(vba_curated):,} rows | "
      f"{vba_curated['clnt_no'].nunique():,} unique clients")
print("Approval split (visa_app_approved):")
print(vba_curated['visa_app_approved'].value_counts(dropna=False))


# ============================================================================
# 2. DLY_FULL_PORTFOLIO — pre-treatment spending + attrition history
# ----------------------------------------------------------------------------
# Join key: acct_no (existing primary card account on the curated table).
# Time filter: dt_record_ext < treatmt_strt_dt → strictly pre-treatment.
#   - Spending aggregates use a 90-day pre-treatment window
#   - Attrition / risk flags use full lifetime up to treatmt_strt_dt
# ============================================================================
vba_portfolio_sql = """
WITH cohort AS (
    SELECT
        clnt_no,
        acct_no,
        MIN(treatmt_strt_dt) AS treatmt_strt_dt
    FROM dw00_im.dl_mr_prod.nbo_vba_rbol_combined
    WHERE mnc = 'VBA'
      AND gross_response = 1
      AND treatmt_strt_dt >= DATE '2025-08-01'
    GROUP BY clnt_no, acct_no
)
SELECT
    c.clnt_no,
    c.acct_no,

    -- Pre-treatment spending (90-day window before treatmt_strt_dt)
    SUM(CASE WHEN p.dt_record_ext >= c.treatmt_strt_dt - INTERVAL '90' DAY
             THEN p.net_prch_amt_dly END)                AS pre_purch_90d,
    AVG(CASE WHEN p.dt_record_ext >= c.treatmt_strt_dt - INTERVAL '90' DAY
             THEN p.bal_current END)                     AS pre_avg_bal_90d,
    AVG(CASE WHEN p.dt_record_ext >= c.treatmt_strt_dt - INTERVAL '90' DAY
             THEN p.accum_dly_bal_mtd END)               AS pre_avg_dly_bal_mtd_90d,
    MAX(CASE WHEN p.dt_record_ext >= c.treatmt_strt_dt - INTERVAL '90' DAY
             THEN p.lst_ann_fee_chrg_amt END)            AS pre_last_ann_fee_90d,

    -- Lifetime attrition / risk flags (any time before treatmt_strt_dt)
    MAX(CASE WHEN p.status = 'VOL'  THEN 1 ELSE 0 END)   AS ever_vol_pre,
    MAX(CASE WHEN p.status = 'BKPT' THEN 1 ELSE 0 END)   AS ever_bkpt_pre,
    MAX(CASE WHEN p.status = 'WOFF' THEN 1 ELSE 0 END)   AS ever_woff_pre,
    MAX(CASE WHEN p.status = 'COLL' THEN 1 ELSE 0 END)   AS ever_coll_pre

FROM cohort c
LEFT JOIN dw00_im.d3cv12a.dly_full_portfolio p
    ON p.acct_no       = c.acct_no
   AND p.dt_record_ext < c.treatmt_strt_dt
GROUP BY c.clnt_no, c.acct_no
"""

vba_portfolio = pd.read_sql_query(vba_portfolio_sql, con=EDW)
print(f"\nPortfolio enrichment: {len(vba_portfolio):,} rows | "
      f"{vba_portfolio['clnt_no'].nunique():,} unique clients")


# ============================================================================
# 3. UCP-business parquet (already on disk — manual HDFS download)
# ============================================================================
ucp = pd.read_parquet(DATA / "vba_ucp_business_slice.parquet")
print(f"\nUCP-business: {len(ucp):,} rows | "
      f"{ucp['clnt_no'].nunique():,} unique clients")


# ============================================================================
# 4. Merge → one analytical row per (clnt_no, acct_no)
# ============================================================================
tree_input = vba_curated.merge(vba_portfolio, on=['clnt_no', 'acct_no'], how='left')
tree_input = tree_input.merge(ucp,            on='clnt_no',              how='left')

print(f"\nFinal analytical file: {len(tree_input):,} rows × {tree_input.shape[1]} columns")
print("Target class balance (visa_app_approved):")
print(tree_input['visa_app_approved'].value_counts(dropna=False))


# ============================================================================
# 5. Save
# ============================================================================
out_path = DATA / "vba_tree_input.parquet"
tree_input.to_parquet(out_path, index=False)
print(f"\nSaved: {out_path}")
