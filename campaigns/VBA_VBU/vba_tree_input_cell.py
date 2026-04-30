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
  - Tree is descriptive, not predictive. Goal: show where approvals fall
    among responders. All fields — pre-treatment, treatment, post-treatment
    — are valid splitter candidates. No leakage filtering required.
  - Post-treatment portfolio window = [treatmt_strt_dt, treatmt_strt_dt + 90d].
  - VBA is an upgrade campaign on existing accounts → portfolio data is
    primary card behavior, not new-card behavior.
  - Hard date floor `dt_record_ext >= DATE '2025-08-01'` is a spool-space
    guard for Teradata — DLY_FULL_PORTFOLIO is enormous; without a
    partition-pruning predicate the join exhausts spool.
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

    -- Engagement / response activity (campaign-window engagement)
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


# === 2. DLY_FULL_PORTFOLIO — latest post-treatment event per acct_no =======
# Event-driven table. PCQ pattern: take the most recent row per account
# after treatmt_strt_dt. One row per acct_no, no window aggregation.

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
    p.acct_no,
    p.dt_record_ext        AS post_event_dt,
    p.bal_current          AS post_bal,
    p.accum_dly_bal_mtd    AS post_dly_bal_mtd,
    p.lst_ann_fee_chrg_amt AS post_last_ann_fee,
    p.net_prch_amt_dly     AS post_purch_latest,
    p.status               AS post_status
FROM dw00_im.d3cv12a.dly_full_portfolio p
INNER JOIN cohort c
    ON c.acct_no = p.acct_no
   AND p.dt_record_ext >= c.treatmt_strt_dt
   AND p.dt_record_ext >= DATE '2025-08-01'
   AND p.dt_record_ext <= DATE '2026-08-01'
QUALIFY ROW_NUMBER() OVER (PARTITION BY p.acct_no ORDER BY p.dt_record_ext DESC) = 1
"""

vba_portfolio = pd.read_sql_query(vba_portfolio_sql, con=EDW)
print(f"\nPortfolio (latest event per acct_no): {len(vba_portfolio):,} rows")


# ============================================================================
# 3. UCP-business parquet (already on disk — manual HDFS download)
# ============================================================================
ucp = pd.read_parquet(DATA / "vba_ucp_business_slice.parquet")
print(f"\nUCP-business: {len(ucp):,} rows | "
      f"{ucp['clnt_no'].nunique():,} unique clients")


# ============================================================================
# 4. Merge → one analytical row per (clnt_no, acct_no)
# ============================================================================
tree_input = vba_curated.merge(vba_portfolio, on='acct_no',  how='left')
tree_input = tree_input.merge(ucp,            on='clnt_no',  how='left')

# Snapshot maturity — how mature each portfolio event is relative to treatment / response
for c in ['post_event_dt', 'treatmt_strt_dt', 'visa_response_dt']:
    tree_input[c] = pd.to_datetime(tree_input[c], errors='coerce')
tree_input['days_post_treatment'] = (tree_input['post_event_dt'] - tree_input['treatmt_strt_dt']).dt.days
tree_input['days_post_response']  = (tree_input['post_event_dt'] - tree_input['visa_response_dt']).dt.days

print(f"\nFinal analytical file: {len(tree_input):,} rows × {tree_input.shape[1]} columns")
print("Target class balance (visa_app_approved):")
print(tree_input['visa_app_approved'].value_counts(dropna=False))


# ============================================================================
# 5. Save
# ============================================================================
out_path = DATA / "vba_tree_input.parquet"
tree_input.to_parquet(out_path, index=False)
print(f"\nSaved: {out_path}")
