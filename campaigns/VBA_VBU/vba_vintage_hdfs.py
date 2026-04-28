"""
VBA summary + 0-90 vintage from HDFS parquets (pandas).

Inputs (parquet, written by the EDW-cursor extracts):
    /home/jovyan/Cards/VBA/vba_vbu_tactic.parquet
    /home/jovyan/Cards/VBA/casper.parquet
    /home/jovyan/Cards/VBA/scot.parquet

Outputs (CSV, written next to this script's working dir):
    vba_summary.csv         leads + client/account success counts and rates by MNE x fiscal_qtr x tst_grp_cd
    vba_vintage_curve.csv   cumulative client- and account-level response rate by day 0..90 per cohort

Success grain:
    - Client-level: distinct clnt_no who responded in [strt, strt+90].
    - Account-level: distinct visa_acct_no in window. visa_acct_no is only set on
      approved Casper rows / fulfilled SCOT rows, so account_approvals == account_responses.
"""

import pandas as pd
from pathlib import Path

# ---------- Config ----------
SRC          = Path("/home/jovyan/Cards/VBA")
OUT          = Path(".")
START_DT     = pd.Timestamp("2025-08-01")
WINDOW_DAYS  = 90
TARGET_MNE   = "VBA"   # "VBU" or None to keep both

# ---------- Load ----------
tactic = pd.read_parquet(SRC / "vba_vbu_tactic.parquet")
casper = pd.read_parquet(SRC / "casper.parquet")
scot   = pd.read_parquet(SRC / "scot.parquet")

print(f"tactic rows: {len(tactic):,}")
print(f"casper rows: {len(casper):,}")
print(f"scot rows  : {len(scot):,}")

# ---------- Tactic prep ----------
# Teradata SUBSTR(tactic_id, 8, 3) == 1-indexed pos 8..10 -> Python slice [7:10]
tactic["mne"] = tactic["tactic_id"].str.slice(7, 10)
tactic["treatmt_strt_dt"] = pd.to_datetime(tactic["treatmt_strt_dt"])
tactic["treatmt_end_dt"]  = pd.to_datetime(tactic["treatmt_end_dt"])

if TARGET_MNE:
    tactic = tactic[tactic["mne"] == TARGET_MNE].copy()

# RBC fiscal year starts Nov 1
def fiscal_qtr(dt):
    fy = dt.year + (1 if dt.month >= 11 else 0)
    m  = (dt.month - 11) % 12          # Nov->0, Dec->1, ..., Oct->11
    q  = (m // 3) + 1
    return f"FY{fy}Q{q}"

tactic["fiscal_qtr"] = tactic["treatmt_strt_dt"].apply(fiscal_qtr)

print(f"tactic after MNE filter ({TARGET_MNE}): {len(tactic):,}")
print(tactic.groupby(["mne", "fiscal_qtr", "tst_grp_cd"]).size().to_string())

# ---------- Response stream prep ----------
# No dedup of the union: nunique on clnt_no / visa_acct_no handles overlap correctly.
# Casper and SCOT should be near-mutually-exclusive (different channels) post-Aug 2025.
casper["visa_response_dt"] = pd.to_datetime(casper["visa_response_dt"])
scot["visa_response_dt"]   = pd.to_datetime(scot["visa_response_dt"])
resp = pd.concat([casper, scot], ignore_index=True, sort=False)
resp["visa_app_approved"]  = resp["visa_app_approved"].fillna(0).astype(int)
print(f"raw response rows (Casper U SCOT): {len(resp):,}")

# ---------- Join + window ----------
joined = tactic.merge(resp, on="clnt_no", how="inner")
joined["days_since"] = (joined["visa_response_dt"] - joined["treatmt_strt_dt"]).dt.days
in_win = joined[(joined["days_since"] >= 0) & (joined["days_since"] <= WINDOW_DAYS)].copy()
print(f"in-window response rows: {len(in_win):,}")

cohort_keys = ["mne", "fiscal_qtr", "tst_grp_cd"]

# ---------- Output 1: summary ----------
leads = tactic.groupby(cohort_keys).size().reset_index(name="leads")

resp_agg = (in_win.groupby(cohort_keys)
                  .agg(client_responses=("clnt_no", "nunique"),
                       account_approvals=("visa_acct_no", "nunique"))   # nunique drops NaN
                  .reset_index())

apr_agg = (in_win[in_win["visa_app_approved"] == 1]
           .groupby(cohort_keys)
           .agg(client_approvals=("clnt_no", "nunique"))
           .reset_index())

summary = leads.merge(resp_agg, on=cohort_keys, how="left").merge(apr_agg, on=cohort_keys, how="left")
for c in ["client_responses", "client_approvals", "account_approvals"]:
    summary[c] = summary[c].fillna(0).astype(int)
summary["client_response_rate"]  = summary["client_responses"]  / summary["leads"]
summary["client_approval_rate"]  = summary["client_approvals"]  / summary["leads"]
summary["account_approval_rate"] = summary["account_approvals"] / summary["leads"]

summary.to_csv(OUT / "vba_summary.csv", index=False)
print(f"\nvba_summary.csv ({len(summary)} rows):")
print(summary.to_string(index=False))

# ---------- Output 2: 0..90 vintage curve (client + account) ----------
# Per (cohort, clnt_no): earliest in-window day -> client first-response day
client_first = (in_win.groupby(cohort_keys + ["clnt_no"])["days_since"]
                       .min().reset_index())
client_daily = (client_first.groupby(cohort_keys + ["days_since"])
                            .size().reset_index(name="client_responders_day"))

# Per (cohort, visa_acct_no): earliest in-window day -> account first-response day
acct_first = (in_win[in_win["visa_acct_no"].notna()]
              .groupby(cohort_keys + ["visa_acct_no"])["days_since"]
              .min().reset_index())
acct_daily = (acct_first.groupby(cohort_keys + ["days_since"])
                        .size().reset_index(name="account_responders_day"))

day_grid = pd.DataFrame({"days_since": range(WINDOW_DAYS + 1)})
scaffold = leads.merge(day_grid, how="cross")

vintage = (scaffold
           .merge(client_daily, on=cohort_keys + ["days_since"], how="left")
           .merge(acct_daily,   on=cohort_keys + ["days_since"], how="left"))
for c in ["client_responders_day", "account_responders_day"]:
    vintage[c] = vintage[c].fillna(0).astype(int)

vintage = vintage.sort_values(cohort_keys + ["days_since"]).reset_index(drop=True)
g = vintage.groupby(cohort_keys)
vintage["client_responders_cum"]    = g["client_responders_day"].cumsum()
vintage["account_responders_cum"]   = g["account_responders_day"].cumsum()
vintage["client_cum_response_rate"]  = vintage["client_responders_cum"]  / vintage["leads"]
vintage["account_cum_response_rate"] = vintage["account_responders_cum"] / vintage["leads"]

vintage.to_csv(OUT / "vba_vintage_curve.csv", index=False)
print(f"\nvba_vintage_curve.csv ({len(vintage)} rows) -> day 0..{WINDOW_DAYS} per cohort")
print(vintage[vintage["days_since"].isin([0, 30, 60, 90])].to_string(index=False))
