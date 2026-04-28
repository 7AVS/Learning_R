"""VBA summary + 0-90 vintage — Jupyter cell. Uses vba_df / casper / scot."""

import pandas as pd
from pathlib import Path

OUT  = Path("/home/jovyan/Cards/VBA")
keys = ["treatmt_strt_dt", "tst_grp_cd"]

# Tactic: VBA only
t = vba_df[vba_df["tactic_id"].str[7:10] == "VBA"].copy()
t["treatmt_strt_dt"] = pd.to_datetime(t["treatmt_strt_dt"])
leads = t.groupby(keys).size()

# Responses: Casper > SCOT, earliest first
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

# Summary (counts only)
s = pd.DataFrame({
    "leads":       leads,
    "client_resp": client.groupby(keys).size(),
    "client_appr": client[client["visa_app_approved"] == 1].groupby(keys).size(),
    "acct_appr":   acct.groupby(keys).size(),
}).fillna(0).astype(int)
s.reset_index().to_csv(OUT / "vba_summary.csv", index=False)

# Vintage curve: one row per (cohort, day) for day in 0..90
daily = client.groupby(keys + ["day"]).size().rename("resp")
v = (leads.reset_index(name="leads")
       .merge(pd.DataFrame({"day": range(91)}), how="cross")
       .merge(daily.reset_index(), on=keys + ["day"], how="left"))
v["resp"] = v["resp"].fillna(0).astype(int)
v = v.sort_values(keys + ["day"])
v["resp_cum"] = v.groupby(keys)["resp"].cumsum()
v.to_csv(OUT / "vba_vintage_curve.csv", index=False)
