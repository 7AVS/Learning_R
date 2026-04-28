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


# === Cell 3: VBA population (HDFS) + UCP enrichment, saved to HDFS =========
# RUN THIS CELL ON THE YARN-SPARK KERNEL (not the local Jupyter pandas kernel).
# Reads tactic events and UCP from HDFS, joins in Spark, writes enriched
# parquet to /user/427966379/ for download into the local Jupyter env.
#
# Response signal (Casper/SCOT) is NOT joined here — that data is in the
# local pandas kernel. Join it after downloading the enriched parquet.

from pyspark.sql import functions as F

TACTIC_BASE   = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
UCP_BASE      = "/prod/sz/tsz/00172/data/ucp4"
UCP_PARTITION = "2026-01-31"
OUT_HDFS      = "/user/427966379/vba_enriched.parquet"

YEARS         = ["2025", "2026"]
TARGET_MNES   = ["VBA"]            # add "VBU" if you want both campaigns
START_DT      = "2025-08-01"

ucp_fields = [
    # Income / wealth
    "INCOME_AFTER_TAX_RNG", "PROF_TOT_ANNUAL", "PROF_SEG_CD",
    # Current Visa card holdings (upgrade headroom)
    "CC_VISA_ALL_TOT_IND", "CC_VISA_CLSIC_TOT_IND", "CC_VISA_CLSIC_RWD_TOT_IND",
    "CC_VISA_GOLD_PRFR_TOT_IND", "CC_VISA_INF_TOT_IND", "CC_VISA_IAV_TOT_IND",
    # RBC product depth (T=Transactional, I=Investment, B=Borrower, C=Cards)
    "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT",
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

# 1. Tactic events from HDFS — VBA participants only
tactic_paths = [f"{TACTIC_BASE}EVNT_STRT_DT={y}*" for y in YEARS]
tactic_sdf = (spark.read.option("basePath", TACTIC_BASE).parquet(*tactic_paths)
    .filter(F.substring(F.col("TACTIC_ID"), 8, 3).isin(TARGET_MNES))
    .filter(F.col("TREATMT_STRT_DT") >= F.lit(START_DT))
    .withColumn("MNE",        F.substring(F.col("TACTIC_ID"), 8, 3))
    .withColumn("CLNT_NO",    F.regexp_replace(F.trim(F.col("TACTIC_EVNT_ID")), "^0+", ""))
    .withColumn("TST_GRP_CD", F.trim(F.col("TST_GRP_CD")))
    .select("CLNT_NO", "TACTIC_ID", "MNE", "TST_GRP_CD",
            "TREATMT_STRT_DT", "TREATMT_END_DT", "TACTIC_CELL_CD",
            "TACTIC_DECISN_VRB_INFO")
    .distinct())

# 2. UCP partition from HDFS
ucp_sdf = (spark.read.parquet(f"{UCP_BASE}/MONTH_END_DATE={UCP_PARTITION}")
    .select("CLNT_NO", *ucp_fields))

# 3. Left-join in Spark — keeps participants even if UCP doesn't have them
enriched = tactic_sdf.join(ucp_sdf, on="CLNT_NO", how="left")

# 4. Write to HDFS user folder
enriched.write.mode("overwrite").parquet(OUT_HDFS)

n_total = enriched.count()
n_ucp   = enriched.filter(F.col("INCOME_AFTER_TAX_RNG").isNotNull()).count()
print(f"VBA enriched -> {OUT_HDFS}")
print(f"  rows:        {n_total:,}")
print(f"  UCP matched: {n_ucp:,} of {n_total:,}")
