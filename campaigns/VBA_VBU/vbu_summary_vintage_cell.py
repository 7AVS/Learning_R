"""VBU summary + 0-90 vintage — Jupyter cell. Uses vba_df / casper / scot.

Mirrors the VBA cell. Filters tactic to MNE='VBU' and writes vbu_* outputs.
NOTE: success here is defined as a Casper/SCOT *application* signal — same as
VBA. If VBU customers upgrade in-place without applying, this misses them.
For the portfolio-change methodology (Daniel Chin's vbu_vintage_original.sql)
extract dly_full_portfolio and write a separate cell.
"""

import pandas as pd
from pathlib import Path

OUT  = Path("/home/jovyan/Cards/VBA")   # shared working folder
keys = ["treatmt_strt_dt", "tst_grp_cd"]

# Tactic: VBU only
t = vba_df[vba_df["tactic_id"].str[7:10] == "VBU"].copy()
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
s.reset_index().to_csv(OUT / "vbu_summary.csv", index=False)

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
v.to_csv(OUT / "vbu_vintage_curve.csv", index=False)


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
print("VBU Summary T/C:")
print(s_tc.to_string(index=False, formatters=fmt))
print("\nVBU Vintage day-90 T/C:")
print(v_tc.to_string(index=False, formatters=fmt))
