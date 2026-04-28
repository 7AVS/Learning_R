"""VBA summary + 0-90 vintage — Jupyter cell. Uses vba_df / casper / scot."""

import pandas as pd
from pathlib import Path

OUT  = Path("/home/jovyan/Cards/VBA")
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


# === Cell 3: UCP enrichment of VBA participants =============================
# Builds a per-participant analytical dataset:
#   t (all VBA participants)  +  in-window response signal from client  +
#   UCP demographic/behavioral fields curated for a business credit card upgrade.
# Reads UCP partition from HDFS, filters to VBA clients only, merges in pandas.
# Output: vba_enriched.parquet
#
# Spark is pre-initialized on Lumina (no builder/stop needed).

from pyspark.sql import functions as F

UCP_PATH      = "/prod/sz/tsz/00172/data/ucp4"
UCP_PARTITION = "2026-01-31"   # latest known partition; align to VBA start as needed

ucp_fields = [
    # Income / wealth
    "INCOME_AFTER_TAX_RNG", "PROF_TOT_ANNUAL", "PROF_SEG_CD",
    # Current Visa card holdings (upgrade headroom)
    "CC_VISA_ALL_TOT_IND", "CC_VISA_CLSIC_TOT_IND", "CC_VISA_CLSIC_RWD_TOT_IND",
    "CC_VISA_GOLD_PRFR_TOT_IND", "CC_VISA_INF_TOT_IND", "CC_VISA_IAV_TOT_IND",
    # RBC product depth — *_TOT_CNT is product count within category:
    # T=Transactional, I=Investment, B=Borrower, C=Cards
    "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT",
    "ACTV_PROD_CNT", "MULTI_PROD_RBT_TOT_IND",
    # Tenure
    "TENURE_RBC_YEARS",
    # Credit eligibility
    "CREDIT_SCORE_RNG", "DLQY_IND",
    # OFI footprint by category (M=mutual fund, L=lending, C=cards, I=investment, T=transactional)
    "OFI_M_PROD_CNT", "OFI_L_PROD_CNT", "OFI_C_PROD_CNT",
    "OFI_I_PROD_CNT", "OFI_T_PROD_CNT",
    # Targeting context
    "REL_TP_SEG_CD",
]

# 1. VBA base + responder flag
resp_signal = client[["clnt_no", "treatmt_strt_dt", "visa_response_dt",
                      "visa_app_approved", "visa_acct_no", "day"]]
vba_full = t.merge(resp_signal, on=["clnt_no", "treatmt_strt_dt"], how="left")
vba_full["responded"] = vba_full["visa_response_dt"].notna().astype(int)

# 2. Pull the UCP partition, filtered to VBA clients only
client_ids = vba_full["clnt_no"].drop_duplicates().tolist()
ucp_sdf = (spark.read.parquet(f"{UCP_PATH}/MONTH_END_DATE={UCP_PARTITION}")
                 .select(F.col("CLNT_NO").alias("clnt_no"),
                         *[F.col(c).alias(f"UCP_{c}") for c in ucp_fields])
                 .filter(F.col("clnt_no").isin(client_ids)))
ucp_pdf = ucp_sdf.toPandas()

# 3. Merge UCP onto VBA base
vba_enriched = vba_full.merge(ucp_pdf, on="clnt_no", how="left")
vba_enriched.to_parquet(OUT / "vba_enriched.parquet", index=False, compression="zstd")

print(f"VBA enriched: {len(vba_enriched):,} rows × {len(vba_enriched.columns)} cols")
print(f"UCP coverage: {vba_enriched['UCP_INCOME_AFTER_TAX_RNG'].notna().mean():.1%} of participants matched in UCP")
print(vba_enriched.head().to_string())
