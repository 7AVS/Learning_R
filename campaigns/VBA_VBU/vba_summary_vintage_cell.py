"""
VBA summary + 0-90 vintage — Jupyter cell (pandas).

Drop in as the next cell after the three SQL extracts. Uses in-memory DataFrames:
    vba_df  (tactic, from DG6V01.tactic_evnt_ip_ar_hist, MNE in {VBA,VBU})
    casper  (Casper response stream)
    scot    (SCOT response stream)

Outputs (CSV):
    /home/jovyan/Cards/VBA/vba_summary.csv
    /home/jovyan/Cards/VBA/vba_vintage_curve.csv

Methodology notes:
    - Filter tactic to MNE='VBA' before the join; VBU success uses a different source
      (dly_full_portfolio product change), do that in a separate run.
    - Response stream is deduped BEFORE the window filter, with Casper > SCOT priority
      on date ties. This matches the existing reference analysis exactly so numbers
      reconcile against it (residual overlap from late-2025 is handled by the priority).
    - Two grains, one denominator (leads):
        client-level   -> dedup to 1 row per clnt_no
        account-level  -> dedup to 1 row per (clnt_no, visa_acct_no), only where acct exists
      account_approvals = cards added in window. account_approval_rate can exceed
      client_approval_rate when a client opens >1 card.
"""

from pathlib import Path
import pandas as pd

OUT          = Path("/home/jovyan/Cards/VBA")
WINDOW_DAYS  = 90
TARGET_MNE   = "VBA"

# ---------- Tactic prep ----------
t = vba_df.copy()
t["mne"] = t["tactic_id"].str.slice(7, 10)             # SUBSTR(id, 8, 3) in Teradata
t["treatmt_strt_dt"] = pd.to_datetime(t["treatmt_strt_dt"])
t["treatmt_end_dt"]  = pd.to_datetime(t["treatmt_end_dt"])
t = t[t["mne"] == TARGET_MNE].copy()

def fiscal_qtr(d):
    fy = d.year + (1 if d.month >= 11 else 0)
    q  = (((d.month - 11) % 12) // 3) + 1
    return f"FY{fy}Q{q}"
t["fiscal_qtr"] = t["treatmt_strt_dt"].apply(fiscal_qtr)

cohort_keys = ["mne", "fiscal_qtr", "tst_grp_cd"]
print(f"VBA tactic rows: {len(t):,}")

# ---------- Response stream: priority dedup ----------
src_priority = {"Casper": 0, "Scott": 1, "SCOT": 1}

c = casper.copy()
s = scot.copy()
c["visa_response_dt"] = pd.to_datetime(c["visa_response_dt"])
s["visa_response_dt"] = pd.to_datetime(s["visa_response_dt"])
r = pd.concat([c, s], ignore_index=True, sort=False)
r["visa_app_approved"] = r["visa_app_approved"].fillna(0).astype(int)
r["src_priority"]      = r["response_source"].map(src_priority).fillna(2)

# Client grain: 1 row per clnt_no, earliest date, Casper wins on tie
r_client = (r.sort_values(["clnt_no", "visa_response_dt", "src_priority"])
              .drop_duplicates(subset=["clnt_no"], keep="first")
              .drop(columns=["src_priority"]))

# Account grain: 1 row per (clnt_no, visa_acct_no)
r_acct = (r[r["visa_acct_no"].notna()]
          .sort_values(["clnt_no", "visa_acct_no", "visa_response_dt", "src_priority"])
          .drop_duplicates(subset=["clnt_no", "visa_acct_no"], keep="first")
          .drop(columns=["src_priority"]))

print(f"client-grain responders: {len(r_client):,}  |  account-grain responders: {len(r_acct):,}")

# ---------- Join + window ----------
def join_window(tactic_df, resp_df):
    j = tactic_df.merge(resp_df, on="clnt_no", how="inner")
    j["days_since"] = (j["visa_response_dt"] - j["treatmt_strt_dt"]).dt.days
    return j[(j["days_since"] >= 0) & (j["days_since"] <= WINDOW_DAYS)].copy()

client_in = join_window(t, r_client)
acct_in   = join_window(t, r_acct)

# ---------- Output 1: summary ----------
leads = t.groupby(cohort_keys).size().reset_index(name="leads")

client_resp = client_in.groupby(cohort_keys).size().reset_index(name="client_responses")
client_apr  = (client_in[client_in["visa_app_approved"] == 1]
               .groupby(cohort_keys).size().reset_index(name="client_approvals"))
acct_apr    = acct_in.groupby(cohort_keys).size().reset_index(name="account_approvals")

summary = (leads
           .merge(client_resp, on=cohort_keys, how="left")
           .merge(client_apr,  on=cohort_keys, how="left")
           .merge(acct_apr,    on=cohort_keys, how="left"))
for col in ["client_responses", "client_approvals", "account_approvals"]:
    summary[col] = summary[col].fillna(0).astype(int)
summary["client_response_rate"]  = summary["client_responses"]  / summary["leads"]
summary["client_approval_rate"]  = summary["client_approvals"]  / summary["leads"]
summary["account_approval_rate"] = summary["account_approvals"] / summary["leads"]

OUT.mkdir(parents=True, exist_ok=True)
summary.to_csv(OUT / "vba_summary.csv", index=False)
print("\nvba_summary.csv:")
print(summary.to_string(index=False))

# ---------- Output 2: 0..90 vintage curve ----------
client_daily = client_in.groupby(cohort_keys + ["days_since"]).size().reset_index(name="client_responders_day")
acct_daily   = acct_in.groupby(cohort_keys + ["days_since"]).size().reset_index(name="account_responders_day")

day_grid = pd.DataFrame({"days_since": range(WINDOW_DAYS + 1)})
scaffold = leads.merge(day_grid, how="cross")

vintage = (scaffold
           .merge(client_daily, on=cohort_keys + ["days_since"], how="left")
           .merge(acct_daily,   on=cohort_keys + ["days_since"], how="left"))
for col in ["client_responders_day", "account_responders_day"]:
    vintage[col] = vintage[col].fillna(0).astype(int)

vintage = vintage.sort_values(cohort_keys + ["days_since"]).reset_index(drop=True)
g = vintage.groupby(cohort_keys)
vintage["client_responders_cum"]     = g["client_responders_day"].cumsum()
vintage["account_responders_cum"]    = g["account_responders_day"].cumsum()
vintage["client_cum_response_rate"]  = vintage["client_responders_cum"]  / vintage["leads"]
vintage["account_cum_response_rate"] = vintage["account_responders_cum"] / vintage["leads"]

vintage.to_csv(OUT / "vba_vintage_curve.csv", index=False)
print(f"\nvba_vintage_curve.csv ({len(vintage)} rows). Sample at days 0/30/60/90:")
print(vintage[vintage["days_since"].isin([0, 30, 60, 90])].to_string(index=False))
