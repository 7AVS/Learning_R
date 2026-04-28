"""VBA summary + 0-90 vintage — Jupyter cell. Uses vba_df / casper / scot."""

import pandas as pd
from pathlib import Path

OUT  = Path("/home/jovyan/Cards/VBA")
keys = ["treatmt_strt_dt", "tst_grp_cd"]

# Tactic: VBA only
t = vba_df[vba_df["tactic_id"].str[7:10] == "VBA"].copy()
t["treatmt_strt_dt"] = pd.to_datetime(t["treatmt_strt_dt"])
leads = t.groupby(keys).size()

# Responses: tag source (primary=Casper, secondary=SCOT), priority dedup, earliest first
r = pd.concat([casper, scot], ignore_index=True)
r["visa_response_dt"] = pd.to_datetime(r["visa_response_dt"])
r["src"]  = r["response_source"].map({"Casper": "primary", "Scott": "secondary", "SCOT": "secondary"})
r["prio"] = r["src"].map({"primary": 0, "secondary": 1})
r = r.sort_values(["clnt_no", "visa_response_dt", "prio"])

def window(resp):
    j = t.merge(resp, on="clnt_no")
    j["day"] = (j["visa_response_dt"] - j["treatmt_strt_dt"]).dt.days
    return j[(j["day"] >= 0) & (j["day"] <= 90)]

client = window(r.drop_duplicates("clnt_no"))
acct   = window(r.dropna(subset=["visa_acct_no"]).drop_duplicates(["clnt_no", "visa_acct_no"]))

def by_src(df, group_keys, name):
    return pd.DataFrame({
        f"{name}_any":       df.groupby(group_keys).size(),
        f"{name}_primary":   df[df["src"] == "primary"].groupby(group_keys).size(),
        f"{name}_secondary": df[df["src"] == "secondary"].groupby(group_keys).size(),
    })

# Summary: counts split by source
s = pd.concat([
    leads.rename("leads"),
    by_src(client, keys, "client_resp"),
    by_src(client[client["visa_app_approved"] == 1], keys, "client_appr"),
    by_src(acct, keys, "acct_appr"),
], axis=1).fillna(0).astype(int)
s.reset_index().to_csv(OUT / "vba_summary.csv", index=False)

# Vintage curve 0..90 — daily + cumulative, client level, split by source
daily = by_src(client, keys + ["day"], "resp")
v = (leads.reset_index(name="leads")
       .merge(pd.DataFrame({"day": range(91)}), how="cross")
       .merge(daily.reset_index(), on=keys + ["day"], how="left"))
for col in ["resp_any", "resp_primary", "resp_secondary"]:
    v[col] = v[col].fillna(0).astype(int)
v = v.sort_values(keys + ["day"]).reset_index(drop=True)
for col in ["resp_any", "resp_primary", "resp_secondary"]:
    v[col + "_cum"] = v.groupby(keys)[col].cumsum()
v.to_csv(OUT / "vba_vintage_curve.csv", index=False)
