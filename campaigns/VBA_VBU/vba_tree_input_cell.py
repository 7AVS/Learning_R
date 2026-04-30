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


# === 2. DLY_FULL_PORTFOLIO — batched fetch, aggregate in pandas ============
# Pull raw portfolio rows by acct_no in batches, aggregate locally to avoid
# Teradata spool exhaustion that the single-query join was hitting.

cohort = (vba_curated[['clnt_no', 'acct_no', 'treatmt_strt_dt']]
          .drop_duplicates()
          .dropna(subset=['acct_no']))
cohort['treatmt_strt_dt'] = pd.to_datetime(cohort['treatmt_strt_dt'])
print(f"\nCohort for portfolio fetch: {len(cohort):,} (clnt_no, acct_no) pairs")

BATCH_SIZE = 1000
accts = cohort['acct_no'].unique()
raw_parts = []

for i in range(0, len(accts), BATCH_SIZE):
    batch = accts[i:i + BATCH_SIZE]
    in_list = "(" + ",".join(f"'{a}'" for a in batch) + ")"
    sql = f"""
    SELECT acct_no, dt_record_ext, status,
           net_prch_amt_dly, bal_current, accum_dly_bal_mtd, lst_ann_fee_chrg_amt
    FROM dw00_im.d3cv12a.dly_full_portfolio
    WHERE acct_no IN {in_list}
      AND dt_record_ext >= DATE '2025-08-01'
      AND dt_record_ext <= DATE '2026-08-01'
    """
    raw_parts.append(pd.read_sql_query(sql, con=EDW))
    print(f"  batch {i // BATCH_SIZE + 1}: {len(raw_parts[-1]):,} rows")

raw = pd.concat(raw_parts, ignore_index=True)
raw['dt_record_ext'] = pd.to_datetime(raw['dt_record_ext'])

# Apply per-account 90-day post-treatment window in pandas
raw = raw.merge(cohort[['acct_no', 'treatmt_strt_dt']].drop_duplicates(),
                on='acct_no', how='inner')
in_window = raw[(raw['dt_record_ext'] >= raw['treatmt_strt_dt']) &
                (raw['dt_record_ext'] <= raw['treatmt_strt_dt'] + pd.Timedelta(days=90))]

spend = in_window.groupby('acct_no').agg(
    post_purch_90d           = ('net_prch_amt_dly',     'sum'),
    post_avg_bal_90d         = ('bal_current',          'mean'),
    post_avg_dly_bal_mtd_90d = ('accum_dly_bal_mtd',    'mean'),
    post_last_ann_fee_90d    = ('lst_ann_fee_chrg_amt', 'max'),
).reset_index()

status_flags = (in_window.assign(
        post_vol  = (in_window['status'] == 'VOL').astype(int),
        post_bkpt = (in_window['status'] == 'BKPT').astype(int),
        post_woff = (in_window['status'] == 'WOFF').astype(int),
        post_coll = (in_window['status'] == 'COLL').astype(int),
    )
    .groupby('acct_no')[['post_vol', 'post_bkpt', 'post_woff', 'post_coll']]
    .max()
    .reset_index())

vba_portfolio = spend.merge(status_flags, on='acct_no', how='outer')
print(f"Portfolio enrichment: {len(vba_portfolio):,} rows")


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

print(f"\nFinal analytical file: {len(tree_input):,} rows × {tree_input.shape[1]} columns")
print("Target class balance (visa_app_approved):")
print(tree_input['visa_app_approved'].value_counts(dropna=False))


# ============================================================================
# 5. Save
# ============================================================================
out_path = DATA / "vba_tree_input.parquet"
tree_input.to_parquet(out_path, index=False)
print(f"\nSaved: {out_path}")
