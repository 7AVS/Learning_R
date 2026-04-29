"""VBA deep-dive workstream — curated NBA table + UCP-business + portfolio enrichment.

Source of truth: `dw00_im.dl_mr_prod.nbo_vba_rbol_combined`
                 (already pre-joins tactic, channel, email engagement,
                  application funnel, and OSC attribution)

Distinct from `vba_summary_vintage_cell.py`, which is the reconciliation
workstream built off raw Casper + SCOT extracts. Do not mix the two — these
files answer different questions and report through different paths.

──────────────────────────────────────────────────────────────────────────
Workflow:
──────────────────────────────────────────────────────────────────────────

Each insight is delivered as its OWN Excel that contains both (a) the SQL
that produced it and (b) the output rows. Atomic, auditable, reproducible.
Excel is the local "repo" until a proper one is available. Do NOT roll
multiple insights into a single mega-aggregated output.

──────────────────────────────────────────────────────────────────────────
Analytical roadmap (in order of build):
──────────────────────────────────────────────────────────────────────────

1. Deployment baseline                                         [done]
   File: vba_deployment_baseline.sql
   Volumes / Action vs Control / SRM eyeball / gross + approved.

2. Q1 conversion rollup + Q2 master analytical file            [scaffolded below]
   Q1: deployed → started → completed → approved/declined funnel,
       sliced by test_group × wave × visa_offer_prod × OSC bucket.
   Q2: per-client master with derived fields (days_to_response,
       responded_in_window, osc_bucket, booking_status).

3. UCP-business enrichment                                     [scaffolded below]
   UCP4 is ONE table at /prod/sz/tsz/00172/data/ucp4/ partitioned by
   MONTH_END_DATE. "Personal" vs "Business" is a CLNT_TYP filter on
   the same table — NOT a separate dataset. For VBA the filter is
   trim(col("CLNT_TYP")) == "Business". Pattern: per-month-end alignment
   + last_day(today - 1 month) ceiling clamp. Pull EVERYONE in the
   campaign (responders + non-responders) — narrowing happens at
   slicing time. Daily variant exists at .../ucp4_daily/ (RUN_DATE
   key) but isn't needed for client-grain enrichment.

4. Decision tree input                                         [TODO]
   Combines (master Q2) + (UCP-business) + (portfolio enrichment
   from item 6). Target variable to be picked AFTER the enriched
   dataset is built so we can see the data first; candidates:
   response Y/N, booking_status (offer vs acquired), approval
   conditional on response.

5. Slicing queries — each saved as its own Excel:             [TODO]
   • Offer vs acquired product breakdown (visa_offer_prod vs
     visa_prod_acq; match / mismatch / unknown). Includes both
     TPA and non-TPA so we see whether mismatch happens outside
     the TPA path too.
   • WestJet product performance (specific product slice).
   • TPA vs ITA breakdown applied to every angle.
   (More to be added as questions arise — one Excel per question.)

6. Balance / spending enrichment                               [TODO]
   Mirror PCQ Q2 pattern (pcq_next_best_card_eda.sql Q2):
   join DLY_FULL_PORTFOLIO post-treatment for bal_current,
   accum_dly_bal_mtd, lst_ann_fee_chrg_amt, net_prch_amt_dly.

7. Voluntary attrition                                         [TODO]
   PCQ Q2 status flags (BKPT/COLL/FRD/INV/OPEN/VOL/WOFF) — filter
   to VOL for voluntary churn signal.

──────────────────────────────────────────────────────────────────────────
Cells below cover item 2 (Q1 + Q2) and a UCP scaffolding placeholder
pointing at the personal-UCP path. The placeholder will be retargeted
at the UCP-business path once confirmed.
"""


# === Cell 3a: VBA EDA — Q1 conversion rollup + Q2 master analytical file ===
# Mirrors the PCQ EDA structure (pcq_next_best_card_eda.sql).
# Q1: deployed → started → completed → approved/declined funnel, sliced by
#     test_group × wave × visa_offer_prod × visa_asc_on_app bucket.
# Q2: per-client master file with derived fields for the tree / segmentation work.
# Filters: mnc='VBA' (RBOL track excluded), treatmt_strt_dt >= 2025-08-01.
# Requires: an EDW cursor in the variable `EDW` (set up in your notebook header).

import pandas as pd
from pathlib import Path

DATA = Path("/home/jovyan/Cards/VBA/data")
OUT  = Path("/home/jovyan/Cards/VBA/output")
DATA.mkdir(parents=True, exist_ok=True)
OUT.mkdir(parents=True, exist_ok=True)

# --- Q1: conversion rollup ---
vba_q1_sql = """
SELECT
    test_group,
    wave,
    visa_offer_prod,
    COUNT(*)                              AS deployed,
    SUM(visa_app_started)                 AS started,
    SUM(visa_app_completed)               AS completed,
    SUM(visa_app_approved)                AS approved,
    SUM(visa_app_declined)                AS declined,
    SUM(gross_response)                   AS gross_response,
    SUM(net_response)                     AS net_response
FROM dw00_im.dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc = 'VBA'
  AND treatmt_strt_dt >= DATE '2025-08-01'
GROUP BY test_group, wave, visa_offer_prod
ORDER BY test_group, wave, visa_offer_prod
"""
vba_q1 = pd.read_sql_query(vba_q1_sql, con=EDW)
vba_q1.to_csv(OUT / "vba_q1_conversion_rollup.csv", index=False)
print(f"Q1 conversion rollup: {len(vba_q1):,} rows -> output/vba_q1_conversion_rollup.csv")


# --- Q2: per-client master file ---
vba_q2_sql = """
SELECT
    -- Identity / treatment
    clnt_no, tactic_id, wave, segment,
    test_group, tst_grp_cd, control,
    treatmt_strt_dt, treatmt_end_dt, treatmt_mn,

    -- Targeting / model
    decile, score, nibt, model, rate,

    -- Channel
    channel,
    chnl_dm, chnl_do, chnl_em, chnl_im, chnl_rd,
    chnl_iu, chnl_in, chnl_om, chnl_iv, chnl_zz,
    csr_interactions, gu,

    -- Email engagement
    email_creative_id, email_disposition, email_status,

    -- O&O actions / call center
    oando, oando_actioned, oando_pending, oando_declined, oando_approved,
    tactic_call, cntct_atmpt_gnsis, call_ans_gnsis, agt_gnsis, rpc_gnsis,

    -- Generic response
    gross_response, net_response, response_dt, prod_acq, cr_lmt,

    -- VBA conversion track
    visa_offer_prod, visa_offer_test, visa_fee, visa_onoff, visa_acct_no,
    visa_app_started, visa_app_completed, visa_app_approved, visa_app_declined,
    visa_date_app_dec, visa_response_dt,
    visa_prod_acq, visa_cr_lmt, visa_response_channel,

    -- Other flags
    hsbc_ind, vba_tpa_rank, tpa_ita_indicator, hsbc_indicator, vba_ita_rank,

    -- Derived: days from treatment to events
    (CAST(visa_response_dt  AS DATE) - CAST(treatmt_strt_dt AS DATE)) AS days_to_response,
    (CAST(visa_date_app_dec AS DATE) - CAST(treatmt_strt_dt AS DATE)) AS days_to_app_decision,

    -- Derived: in-window flag (90-day measurement window)
    CASE WHEN visa_response_dt BETWEEN treatmt_strt_dt AND treatmt_strt_dt + 90
         THEN 1 ELSE 0 END AS responded_in_window,

    -- Derived: booking status (offered product vs product they actually acquired)
    CASE
        WHEN visa_app_approved = 1 AND visa_offer_prod = visa_prod_acq  THEN 'match'
        WHEN visa_app_approved = 1 AND visa_offer_prod <> visa_prod_acq THEN 'mismatch'
        WHEN visa_app_approved = 1                                       THEN 'unknown'
        ELSE NULL
    END AS booking_status

FROM dw00_im.dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc = 'VBA'
  AND treatmt_strt_dt >= DATE '2025-08-01'
"""
vba_master = pd.read_sql_query(vba_q2_sql, con=EDW)
vba_master.to_parquet(DATA / "vba_master.parquet", index=False, compression="zstd")
print(f"Q2 master:           {len(vba_master):,} rows × {len(vba_master.columns)} cols -> data/vba_master.parquet")


# === Cell 3b: UCP-business slice (YARN-SPARK + HDFS) =======================
# RUN ON YARN-SPARK KERNEL.
# Reads UCP-BUSINESS (a separate table from the unified ucp4) per the field
# prefix `tsz_00172_data_ucp4_business.*` shown on the developer schema.
# Likely HDFS path: /prod/sz/tsz/00172/data/ucp4_business/. Verify on first run.
#
# Pipeline:
#   1. Identify VBA participants from tactic events HDFS (SUBSTR(TACTIC_ID,8,3)='VBA').
#   2. Compute MONTH_END_DATE per client = last_day(treatmt_strt_dt), clamped at
#      last_day(today - 1 month) so current-month treatments fall back to the
#      previous month's partition (UCP partitions don't exist for open months).
#   3. Read just the needed UCP-business month-end partitions.
#   4. CLNT_TYP = 'Business' filter (defensive — should be all-Business already).
#   5. Inner-join on (CLNT_NO, MONTH_END_DATE).
#   6. Save thin slice to /user/427966379/ for download to local data/.
#
# Curated 91-field list per campaigns/VBA_VBU/schemas/ucp_business_curated_fields.md

from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim

# Disable auto-broadcast for the join below. The vba_clients side gets too
# large to broadcast for big VBA campaigns and triggers driver OOM. Sort-merge
# join is slightly slower on small data but doesn't blow up the driver.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# --- pread helper (from developer's best-practices notebook) ---
def pread(path, partition_key, partition_value=""):
    full_path = str(path) + str(partition_key) + "=" + str(partition_value) + "*"
    return spark.read.option("basePath", path).parquet(full_path)

TACTIC_BASE = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
UCP_B_PATH  = "/prod/sz/tsz/00172/data/ucp4_business/"
UCP_KEY     = "MONTH_END_DATE"
OUT_HDFS    = "/user/427966379/vba_ucp_business_slice.parquet"

YEARS       = ["2025", "2026"]
TARGET_MNES = ["VBA"]
START_DT    = "2025-08-01"

# Curated UCP-business fields (91 total per the schema doc, grouped here)
ucp_fields_client_info = [
    "clnt_typ", "clnt_no", "active_email_ind", "dlqy_ind",
    "dt_opened", "lang_seg_cd", "non_rsdt_tax_cd", "post_cd",
]
ucp_fields_account = [
    "onlin_bnkg_enrlmnt_dt", "tenure_rbc_rng", "tenure_rbc_years",
    "entitlement_cd", "fsa_cd", "gu_no_seg_cd", "reln_mg_unit_no",
    "srvc_cnt", "digital_trans_ind", "mobile_auth_ind",
    "mobile_trans_ind", "olb_auth_ind",
]
ucp_fields_eligibility = [
    "olb_enrolled_ind", "cpc_ent_eligible", "cpc_dm_eligible",
    "cpc_tm_eligible", "cpc_olb_eligible", "myadvisor_status",
]
ucp_fields_product = [
    "actv_prod_cnt", "actv_prod_srvc_cnt", "opn_prod_cnt",
    "bpol_ind", "mnthly_pac_amt", "rel_tp_seg_cd",
]
ucp_fields_transactions = [
    # Authentication / total counts
    "ac_trans_cnt",
    "atm_auth_cnt", "atm_trans_cnt", "atm_trans_ind",
    "branch_auth_cnt", "branch_trans_cnt", "branch_trans_ind",
    "ivr_auth_cnt", "ivr_trans_cnt",
    "mobile_auth_cnt", "mobile_auth_nqb_cnt", "mobile_trans_cnt",
    "olb_auth_cnt", "olb_trans_cnt",
    # Account transfer
    "ac_trans_acct_transfer_cnt",
    "atm_trans_acct_transfer_cnt",
    "ivr_trans_acct_transfer_cnt",
    "mobile_trans_acct_transfer_amt", "mobile_trans_acct_transfer_cnt",
    "olb_trans_acct_transfer_amt", "olb_trans_acct_transfer_cnt",
    # Bill payment
    "ac_trans_bill_pymnt_cnt",
    "atm_trans_bill_pymnt_cnt",
    "ivr_trans_bill_pymnt_amt", "ivr_trans_bill_pymnt_cnt",
    "mobile_trans_bill_pymnt_amt", "mobile_trans_bill_pymnt_cnt",
    "olb_trans_bill_pymnt_amt", "olb_trans_bill_pymnt_cnt",
    # E-transfer / IMT / TPP (digital channels only)
    "mobile_trans_etransfer_amt", "mobile_trans_etransfer_cnt",
    "mobile_trans_imt_cnt",
    "mobile_trans_tpp_amt", "mobile_trans_tpp_cnt",
    "olb_trans_etransfer_amt", "olb_trans_etransfer_cnt",
    "olb_trans_imt_cnt",
    "olb_trans_tpp_amt", "olb_trans_tpp_cnt",
    # NMI FS (prefix meaning not documented — verify with data steward)
    "nmi_fs_act_cls_cnt", "nmi_fs_act_opn_cnt", "nmi_fs_act_opn_cls_cnt",
    "nmi_fs_dep_cnt", "nmi_fs_wl_cnt",
]
ucp_fields_misc = [
    "trans_memo_bp_cnt", "trans_memo_tpp_cnt",
    "ac_trans_ccpmt_cnt", "atm_trans_ccpmt_cnt",
    "atm_trans_dep_amt", "atm_trans_dep_cnt",
    "atm_trans_wl_amt", "atm_trans_wl_cnt",
    "branch_trans_trvls_chq_cnt", "calendar_appt_cnt",
]
ucp_fields_call_center = [
    "ivr_trans_ccpmt_cnt",
    "mobile_trans_mobile_ccpmt_amt", "mobile_trans_mobile_ccpmt_cnt",
    "mobile_trans_mobile_chq_dep_cnt",
    "olb_trans_olb_ccpmt_amt", "olb_trans_olb_ccpmt_cnt",
]
ucp_fields_segmentation = [
    "bus_seg", "bsc",
    # month_end_date is the partition key — joined separately, not pulled as a feature
]

UCP_FIELDS = (ucp_fields_client_info + ucp_fields_account + ucp_fields_eligibility +
              ucp_fields_product + ucp_fields_transactions + ucp_fields_misc +
              ucp_fields_call_center + ucp_fields_segmentation)

# 1. Identify VBA participants from tactic events HDFS, with month-end clamp
tactic_paths = [f"{TACTIC_BASE}EVNT_STRT_DT={y}*" for y in YEARS]
vba_clients = (spark.read.option("basePath", TACTIC_BASE).parquet(*tactic_paths)
    .filter(F.substring(F.col("TACTIC_ID"), 8, 3).isin(TARGET_MNES))
    .filter(F.col("TREATMT_STRT_DT") >= F.lit(START_DT))
    .withColumn("CLNT_NO",        F.regexp_replace(F.trim(F.col("TACTIC_EVNT_ID")), "^0+", ""))
    .withColumn("MONTH_END_DATE",
                F.least(F.last_day("TREATMT_STRT_DT"),
                        F.last_day(F.add_months(F.current_date(), -1))))
    .select("CLNT_NO", "MONTH_END_DATE")
    .distinct())

# 2. Discover which UCP-business month-end partitions we need
month_ends = sorted(row["MONTH_END_DATE"].strftime("%Y-%m-%d")
                    for row in vba_clients.select("MONTH_END_DATE").distinct().collect())
ucp_paths  = [f"{UCP_B_PATH}{UCP_KEY}={me}" for me in month_ends]

# 3. Read UCP-business for those month-ends + filter Business + inner-join to participants
ucp_slice = (spark.read.option("basePath", UCP_B_PATH).parquet(*ucp_paths)
    .filter(trim(col("CLNT_TYP")) == "Business")
    .select(UCP_KEY, *UCP_FIELDS)
    .withColumn(UCP_KEY, F.col(UCP_KEY).cast("date"))
    .join(vba_clients, on=["clnt_no", UCP_KEY], how="inner"))

ucp_slice.write.mode("overwrite").parquet(OUT_HDFS)
print(f"UCP-business slice -> {OUT_HDFS}")
print(f"  rows:       {ucp_slice.count():,}")
print(f"  fields:     {len(UCP_FIELDS)} (target 91; transaction metrics partial — see schema doc)")
print(f"  month-ends: {month_ends}")


# === Cell 3c: Final enriched dataset (LOCAL pandas) ========================
# After downloading vba_ucp_slice.parquet into data/, join the Q2 master file
# to UCP on (clnt_no, month_end_date) to produce the deep-dive dataset.

import pandas as pd
from pathlib import Path

DATA = Path("/home/jovyan/Cards/VBA/data")

vba_master = pd.read_parquet(DATA / "vba_master.parquet")
ucp_slice  = pd.read_parquet(DATA / "vba_ucp_slice.parquet")

vba_master["treatmt_strt_dt"] = pd.to_datetime(vba_master["treatmt_strt_dt"])
# UCP cap: latest available partition is last_day(today - 1 month). Current-month
# treatments get clamped to the previous month-end so the join key matches Cell 3b.
ucp_ceiling = pd.Timestamp.today().normalize().replace(day=1) - pd.Timedelta(days=1)
vba_master["month_end_date"]  = (vba_master["treatmt_strt_dt"] + pd.offsets.MonthEnd(0)).clip(upper=ucp_ceiling)
ucp_slice.columns = [c.lower() for c in ucp_slice.columns]
ucp_slice["month_end_date"] = pd.to_datetime(ucp_slice["month_end_date"])
ucp_slice = ucp_slice.rename(columns={c: f"ucp_{c}" for c in ucp_slice.columns
                                       if c not in ("clnt_no", "month_end_date")})

vba_enriched = vba_master.merge(ucp_slice, on=["clnt_no", "month_end_date"], how="left")
vba_enriched.to_parquet(DATA / "vba_enriched.parquet", index=False, compression="zstd")

n_total = len(vba_enriched)
n_ucp   = vba_enriched["ucp_income_after_tax_rng"].notna().sum()
print(f"VBA enriched: {n_total:,} rows × {len(vba_enriched.columns)} cols")
print(f"  UCP matched: {n_ucp:,} of {n_total:,}")
