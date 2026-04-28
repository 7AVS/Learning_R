"""VBA summary + 0-90 vintage — Jupyter cell. Uses vba_df / casper / scot."""

import pandas as pd
from pathlib import Path

DATA = Path("/home/jovyan/Cards/VBA/data")     # parquet inputs (in-memory DFs may also come from cells above)
OUT  = Path("/home/jovyan/Cards/VBA/output")   # CSV outputs
OUT.mkdir(parents=True, exist_ok=True)

keys = ["treatmt_strt_dt", "tst_grp_cd"]

# Tactic: VBA only
t = vba_df[vba_df["tactic_id"].str[7:10] == "VBA"].copy()
t["treatmt_strt_dt"] = pd.to_datetime(t["treatmt_strt_dt"])
leads = t.groupby(keys).size()

# Responses: Casper > SCOT priority dedup, earliest first
r = pd.concat([casper, scot], ignore_index=True)
r["visa_response_dt"] = pd.to_datetime(r["visa_response_dt"])
r["prio"] = r["response_source"].map({"Casper": 0, "Scott": 1, "SCOT": 1})
r = r.sort_values(["clnt_no", "visa_response_dt", "prio"])

def window(resp):
    j = t.merge(resp, on="clnt_no")
    j["day"] = (j["visa_response_dt"] - j["treatmt_strt_dt"]).dt.days
    return j[(j["day"] >= 0) & (j["day"] <= 90)]

client = window(r.drop_duplicates("clnt_no"))
acct   = window(r.dropna(subset=["visa_acct_no"]).drop_duplicates(["clnt_no", "visa_acct_no"]))

# Summary: leads + client/account counts
s = pd.DataFrame({
    "leads":      leads,
    "client_any": client.groupby(keys).size(),
    "acct_any":   acct.groupby(keys).size(),
}).fillna(0).astype(int)
s.reset_index().to_csv(OUT / "vba_summary.csv", index=False)

# Vintage curve 0..90 — daily + cumulative at client and account level
client_daily = client.groupby(keys + ["day"]).size().rename("client_daily")
acct_daily   = acct.groupby(keys + ["day"]).size().rename("acct_daily")
v = (leads.reset_index(name="leads")
       .merge(pd.DataFrame({"day": range(91)}), how="cross")
       .merge(client_daily.reset_index(), on=keys + ["day"], how="left")
       .merge(acct_daily.reset_index(), on=keys + ["day"], how="left"))
v["client_daily"] = v["client_daily"].fillna(0).astype(int)
v["acct_daily"]   = v["acct_daily"].fillna(0).astype(int)
v = v.sort_values(keys + ["day"]).reset_index(drop=True)
v["client_cum"] = v.groupby(keys)["client_daily"].cumsum()
v["acct_cum"]   = v.groupby(keys)["acct_daily"].cumsum()
v.to_csv(OUT / "vba_vintage_curve.csv", index=False)


# === Cell 2: Test/Control aggregated views (for day-90 validation) ===========
# Uses s and v from Cell 1 in memory (re-run Cell 1 first if schema changed).
# tst_grp_cd rolled up: C -> Control, T -> Test, else Other.
# Same column structure in both outputs — day-90 vintage row should equal the
# summary row for the matching (treatmt_strt_dt, tc) cohort. Printed only.

def tc(code):
    c = str(code)[:1].upper()
    return "Control" if c == "C" else "Test" if c == "T" else "Other"

agg_keys = ["treatmt_strt_dt", "tc"]
metrics  = ["client_any", "acct_any"]

s_flat = s.reset_index()
s_flat["tc"] = s_flat["tst_grp_cd"].apply(tc)
s_tc = s_flat.groupby(agg_keys)[["leads"] + metrics].sum().reset_index()

v_flat = v.copy()
v_flat["tc"] = v_flat["tst_grp_cd"].apply(tc)
v90 = v_flat[v_flat["day"] == 90].rename(columns={"client_cum": "client_any", "acct_cum": "acct_any"})
v_tc = v90.groupby(agg_keys)[["leads"] + metrics].sum().reset_index()

for col in metrics:
    s_tc[col + "_rate"] = s_tc[col] / s_tc["leads"]
    v_tc[col + "_rate"] = v_tc[col] / v_tc["leads"]

fmt = {c: "{:.2%}".format for c in s_tc.columns if c.endswith("_rate")}
print("Summary T/C:")
print(s_tc.to_string(index=False, formatters=fmt))
print("\nVintage day-90 T/C:")
print(v_tc.to_string(index=False, formatters=fmt))


# === Cell 3a: VBA EDA — Q1 conversion rollup + Q2 master analytical file ===
# Mirrors the PCQ EDA structure (pcq_next_best_card_eda.sql).
# Q1: deployed → started → completed → approved/declined funnel, sliced by
#     test_group × wave × visa_offer_prod × visa_osc_on_app bucket.
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
    COALESCE(visa_osc_on_app, 'NO_OSC')   AS osc_bucket,
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
GROUP BY test_group, wave, visa_offer_prod, COALESCE(visa_osc_on_app, 'NO_OSC')
ORDER BY test_group, wave, visa_offer_prod, osc_bucket
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
    visa_date_app_dec, visa_response_dt, visa_osc_on_app,
    visa_prod_acq, visa_cr_lmt, visa_response_channel,

    -- Other flags
    hsbc_ind, vba_tpa_rank, tpa_ita_indicator, hsbc_indicator, vba_ita_rank,

    -- Derived: days from treatment to events
    (CAST(visa_response_dt  AS DATE) - CAST(treatmt_strt_dt AS DATE)) AS days_to_response,
    (CAST(visa_date_app_dec AS DATE) - CAST(treatmt_strt_dt AS DATE)) AS days_to_app_decision,

    -- Derived: in-window flag (90-day measurement window)
    CASE WHEN visa_response_dt BETWEEN treatmt_strt_dt AND treatmt_strt_dt + 90
         THEN 1 ELSE 0 END AS responded_in_window,

    -- Derived: OSC bucket (raw for now — refine to Period/Other/None once we know the campaign codes)
    COALESCE(visa_osc_on_app, 'NO_OSC') AS osc_bucket,

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


# === Cell 3b: UCP slice (YARN-SPARK + HDFS) ================================
# RUN ON YARN-SPARK KERNEL.
# Identifies VBA clients + their month-ends from tactic events HDFS, then
# pulls UCP per-client per-month-end. Saves a thin slice to /user/427966379/
# for download into the local data/ folder.

from pyspark.sql import functions as F

TACTIC_BASE = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
UCP_BASE    = "/prod/sz/tsz/00172/data/ucp4"
OUT_HDFS    = "/user/427966379/vba_ucp_slice.parquet"

YEARS       = ["2025", "2026"]
TARGET_MNES = ["VBA"]
START_DT    = "2025-08-01"

ucp_fields = [
    # Income / wealth
    "INCOME_AFTER_TAX_RNG", "PROF_TOT_ANNUAL", "PROF_SEG_CD",
    # RBC product depth — Visa/credit-card holdings excluded (leakage)
    # (T=Transactional, I=Investment, B=Borrower)
    "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT",
    "ACTV_PROD_CNT", "MULTI_PROD_RBT_TOT_IND",
    # Tenure
    "TENURE_RBC_YEARS",
    # Credit eligibility
    "CREDIT_SCORE_RNG", "DLQY_IND",
    # OFI footprint (M=mutual fund, L=lending, C=cards, I=investment, T=transactional)
    "OFI_M_PROD_CNT", "OFI_L_PROD_CNT", "OFI_C_PROD_CNT",
    "OFI_I_PROD_CNT", "OFI_T_PROD_CNT",
    # Targeting context
    "REL_TP_SEG_CD",
]

# Identify VBA clients + the month-end of each treatment.
# UCP partitions only exist up to the previous month-end (no current-month
# partition until that month closes), so cap MONTH_END_DATE at last_day(today - 1 month).
# For an April treatment with today = April 28, the cap maps it to March 31.
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

month_ends = sorted(row["MONTH_END_DATE"].strftime("%Y-%m-%d")
                    for row in vba_clients.select("MONTH_END_DATE").distinct().collect())
ucp_paths  = [f"{UCP_BASE}/MONTH_END_DATE={me}" for me in month_ends]

# Read UCP for those month-ends, inner-join to keep only VBA-relevant clients
ucp_slice = (spark.read.option("basePath", UCP_BASE).parquet(*ucp_paths)
    .select("CLNT_NO", "MONTH_END_DATE", *ucp_fields)
    .withColumn("MONTH_END_DATE", F.col("MONTH_END_DATE").cast("date"))
    .join(vba_clients, on=["CLNT_NO", "MONTH_END_DATE"], how="inner"))

ucp_slice.write.mode("overwrite").parquet(OUT_HDFS)
print(f"UCP slice -> {OUT_HDFS}")
print(f"  rows:       {ucp_slice.count():,}")
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
