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


# === Cell 2: Test/Control aggregated views (for day-90 validation) ===========
# Reads the CSVs from Cell 1. Rolls up tst_grp_cd by first letter:
# C -> Control, T -> Test, else Other. Printed only — no disk save.
# At day 90 the vintage client_resp_* should equal the summary client_resp_*
# for the matching (treatmt_strt_dt, tc) cohort.

s_csv = pd.read_csv(OUT / "vba_summary.csv")
v_csv = pd.read_csv(OUT / "vba_vintage_curve.csv")

def tc(code):
    c = str(code)[:1].upper()
    return "Control" if c == "C" else "Test" if c == "T" else "Other"

s_csv["tc"] = s_csv["tst_grp_cd"].apply(tc)
v_csv["tc"] = v_csv["tst_grp_cd"].apply(tc)

agg_keys = ["treatmt_strt_dt", "tc"]

summary_metrics = [
    "client_resp_any", "client_resp_primary", "client_resp_secondary",
    "client_appr_any", "client_appr_primary", "client_appr_secondary",
    "acct_appr_any",   "acct_appr_primary",   "acct_appr_secondary",
]
vintage_metrics = ["client_resp_any", "client_resp_primary", "client_resp_secondary"]

# Summary T/C — counts + rates for client and account
s_tc = s_csv.groupby(agg_keys)[["leads"] + summary_metrics].sum().reset_index()
for col in summary_metrics:
    s_tc[col + "_rate"] = s_tc[col] / s_tc["leads"]

# Vintage at day=90 — column names aligned to summary for direct comparison
v90 = v_csv[v_csv["day"] == 90].rename(columns={
    "resp_any_cum":       "client_resp_any",
    "resp_primary_cum":   "client_resp_primary",
    "resp_secondary_cum": "client_resp_secondary",
})
v_tc = v90.groupby(agg_keys)[["leads"] + vintage_metrics].sum().reset_index()
for col in vintage_metrics:
    v_tc[col + "_rate"] = v_tc[col] / v_tc["leads"]

print("Summary T/C:")
print(s_tc.to_string(index=False))
print("\nVintage day-90 T/C:")
print(v_tc.to_string(index=False))
