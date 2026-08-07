# unsub_analysis_notebook.py — REFACTORED REFERENCE (2026-08-06) mirroring
# Andre's merged POD notebook (unsub_unified_analysis_POD), his order, all
# charts MATPLOTLIB (static images: stable at any size/resolution).
# SELF-CONTAINED: cold start needs duckdb, pandas, numpy, matplotlib and
# the delivered CSVs in ONE folder (pod or local; set BASE). The three
# OVERLAP caches + pm_asks_results.csv are derived files created by their
# own cells / pm_asks_recompute.py.
#
# SECTION NUMBERING (renumbered per Andre — old labels in parentheses):
#   Q0  LOB landscape small-multiples          (was Q0)
#   Q1  Volume vs Rate concentration           (was Q0b, suptitle said Q2)
#   Q2  12-month curve + FWC timing            (was Q1, suptitle said Q6)
#   Q3  Cards deep dive by action type         (was Q2)
#   Q4  Contact frequency                      (was Q3)
#   Q5  Who unsubscribes — rep ratios          (was Q4)
#   D0  Delta section — validation gates
#   D1  Spend headline (then vs now)
#   D2  Profitability + product count
#   A1  PROFIT CHECK — population-fixed        (stakeholder ask #3)
#   A2  ATTRITION                              (stakeholder ask #2)
#   A3  OVERLAP (FIFA isolated) + programs + top-10 combos  (ask #1)
# Chart conventions (plot_revision_prompt.py G1-G8): percent w/ 2 decimals,
# n on every rate, small-base guard <10K, say WHICH Cards definition, no
# causal language, no_ucp_match shown, provenance ignored.

# %% [0] Setup + data load (DuckDB views over the delivered CSVs)
import os
import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib.patches import Patch

pd.set_option("display.width", 160)
pd.set_option("display.max_columns", 20)
pd.set_option("display.max_rows", 60)

HDFS_OUT = "hdfs:///user/427966379/unsub_unified/out/"
BASE = os.path.expanduser("~/unsub_unified_out/")            # pod local
# BASE = r"\\maple.fg.rbc.com\...\Cards\Unsubs\output"       # laptop share
USE_HDFS = "spark" in globals()

def load_cube(fname, alts=(), **read_csv_kwargs):
    """HDFS-first (pod), local-CSV fallback. alts = alternate filenames."""
    for cand in (fname,) + tuple(alts):
        if USE_HDFS:
            try:
                pdf = (spark.read.csv(HDFS_OUT + cand, header=True,
                                      inferSchema=True).toPandas())
                print(f"  {cand:28s} <- HDFS ({len(pdf):,} rows)")
                return pdf
            except Exception as e:
                print(f"  {cand:28s} HDFS miss ({type(e).__name__}) -> local")
        if os.path.exists(os.path.join(BASE, cand)):
            pdf = pd.read_csv(os.path.join(BASE, cand), **read_csv_kwargs)
            print(f"  {cand:28s} <- local ({len(pdf):,} rows)")
            return pdf
        print(f"  {cand:28s} not in {BASE} -> next candidate")
    raise FileNotFoundError(f"none of {(fname,) + tuple(alts)} found on HDFS or in {BASE}")

SMALL_BASE = 10_000   # G3 small-base guard

# ONE palette block (merge dupes removed; C_LINE red per Andre 2026-08-06)
C_THEN = "#003168"   # Dark Blue
C_NOW  = "#FCA311"   # Sunburst
C_LINE = "#B00020"   # Red (secondary)
C_POS  = "#AABA0A"   # Apple
colors_at = {
    "Pre_Attract": "#003168", "Attract": "#87AFBF", "Deepen": "#51B5E0",
    "Onboard": "#588886", "Retain": "#FFC72C", "Fulfillment": "#B8A970",
    "Regulatory": "#899299", "Operational": "#C1B5A5",
}
lob_colors = {
    "CARDS": "#003168", "LOYALTY": "#87AFBF", "PSI": "#AABA0A",
    "PBA": "#FFC72C", "COMMERCIAL": "#588886", "RBC_BANK": "#51B5E0",
    "UNKNOWN": "#899299", "HEF": "#FCA311", "AUTO": "#B8A970",
    "INS": "#C1B5A5", "PL": "#6F6E6F", "FIFA": "#FCA311",
}

def compact_n(v):
    v = float(v)
    if abs(v) >= 1_000_000: return f"{v/1_000_000:.1f}M"
    if abs(v) >= 1_000:     return f"{v/1_000:.1f}K"
    return f"{v:.0f}"

def style_ax(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

con = duckdb.connect()
print(f"Loading cubes (USE_HDFS={USE_HDFS}):")
_frames = {}
for view, fname in [("a1", "a1_mne_share.csv"), ("a1_lob", "a1_lob_dedup.csv"),
                    ("a2", "a2_mne_rates.csv"), ("a3", "a3_contact_cube.csv"),
                    ("a4", "a4_profile_cube.csv"), ("b", "b_before_after_cube.csv"),
                    ("b_delta", "b_delta_summary.csv")]:
    _frames[view] = load_cube(fname)
    con.register(view, _frames[view])
_frames["mapping"] = load_cube("mapping Mne.csv", alts=("mapping_mne.csv",))
con.register("mapping", _frames["mapping"])

_cdf = load_cube("c_monthly_curve.csv", encoding="latin-1", on_bad_lines="skip")
def _pick(cols, *needles):
    hits = [c2 for c2 in cols if any(n in c2.lower() for n in needles)]
    assert hits, f"c_monthly_curve: no column matching {needles} in {list(cols)} - STOP"
    return hits[0]
_cols = _cdf.columns
_cdf = _cdf.rename(columns={
    _pick(_cols, "mne"): "mne", _pick(_cols, "ym", "month"): "ym",
    _pick(_cols, "send", "deliver"): "sends", _pick(_cols, "unsub"): "unsubs_attributed",
})[["mne", "ym", "sends", "unsubs_attributed"]]
_cdf["ym"] = _cdf["ym"].astype(str).str.strip()
_cdf["sends"] = pd.to_numeric(_cdf["sends"], errors="coerce")
_cdf["unsubs_attributed"] = pd.to_numeric(_cdf["unsubs_attributed"], errors="coerce")
con.register("c", _cdf)

PM_CSV = os.path.join(BASE, "pm_asks_results.csv")
HAS_PM = os.path.exists(PM_CSV)
if HAS_PM:
    con.execute(f"CREATE OR REPLACE VIEW pm AS SELECT * FROM read_csv_auto('{PM_CSV}')")

lob_dedup = con.execute("SELECT label, unique_unsub_clients FROM a1_lob").df()
ENTERPRISE_DEDUP  = int(lob_dedup.loc[lob_dedup["label"] == "ENTERPRISE",     "unique_unsub_clients"].iloc[0])
CARDS_LOB_DEDUP   = int(lob_dedup.loc[lob_dedup["label"] == "CARDS_LOB_ALL",  "unique_unsub_clients"].iloc[0])
CARDS_EX_FWC_DEDUP = int(lob_dedup.loc[lob_dedup["label"] == "CARDS_EX_FWC",  "unique_unsub_clients"].iloc[0])

print("Views registered. Rows per cube:")
for v in ["a1", "a1_lob", "a2", "a3", "a4", "b", "b_delta", "c", "mapping"] + (["pm"] if HAS_PM else []):
    print(f"  {v:10s} {con.execute(f'SELECT COUNT(*) FROM {v}').fetchone()[0]:,}")
print(f"pm_asks_results.csv present: {HAS_PM}")
print(f"Deduped: enterprise {ENTERPRISE_DEDUP:,} · cards LOB {CARDS_LOB_DEDUP:,} · cards ex-FWC {CARDS_EX_FWC_DEDUP:,}")

# %% [markdown]
# # Unsub Analysis — Run 2026-08-03
# Analyses querying the unsub cubes via DuckDB. Source CSVs: full population.
#
# **Two Cards LOB definitions used in this notebook:**
# - **Cards LOB (mapping file):** all MNEs tagged CARDS in `mapping Mne.csv`,
#   incl FWC/FIFA; deduped uniques from `a1_lob_dedup.csv`.
# - **Cards pod (12 MNEs):** PCQ PCL PCD AUH CLI CRV VBA VBU CEC VIF MET —
#   matches the pipeline's cards_unsub flag.
#
# One person can unsubscribe from several campaigns — campaign counts add
# up to more than the number of unique people.

# %% [1] Q0 — Monthly Sends and Unsub Rate by LOB (small multiples, top 6)
# bars = delivered emails; red line = unsubs per delivered email %
q0 = """
WITH lob_month AS (
    SELECT c.ym,
           COALESCE(TRIM(m.LOB_Manual), 'UNKNOWN') AS lob_manual,
           SUM(c.sends) AS sends,
           SUM(c.unsubs_attributed) AS unsubs
    FROM c
    LEFT JOIN mapping m ON TRIM(c.mne) = TRIM(m.MNEMONIC)
    WHERE c.ym BETWEEN '202508' AND '202606'
      AND c.sends IS NOT NULL AND c.unsubs_attributed IS NOT NULL
    GROUP BY 1, 2
), ranked AS (
    SELECT lob_manual FROM lob_month GROUP BY 1
    ORDER BY SUM(sends) DESC LIMIT 6
)
SELECT lm.ym, lm.lob_manual, lm.sends, lm.unsubs,
       ROUND(lm.unsubs * 100.0 / NULLIF(lm.sends, 0), 3) AS unsub_per_email_pct
FROM lob_month lm JOIN ranked r USING (lob_manual)
ORDER BY lm.lob_manual, lm.ym
"""
df0 = con.execute(q0).df()
lob_order = (df0.groupby("lob_manual")["sends"].sum()
             .sort_values(ascending=False).index.tolist())
months = sorted(df0["ym"].unique())
mpos = range(len(months))
ymax_sends = df0["sends"].max() * 1.1
ymax_rate = df0["unsub_per_email_pct"].max() * 1.1

fig, axes = plt.subplots(3, 2, figsize=(16, 14), sharex=True, sharey=True)
axes = axes.flatten()
for ax, lob in zip(axes, lob_order):
    sub = df0[df0["lob_manual"] == lob].set_index("ym").reindex(months)
    ax.bar(mpos, sub["sends"].fillna(0), color=C_THEN, alpha=0.75, edgecolor="white")
    tot_s, tot_u = sub["sends"].sum(), sub["unsubs"].sum()
    ax.set_title(f"{lob}  (total sends: {compact_n(tot_s)} | avg unsub rate: "
                 f"{tot_u * 100.0 / tot_s:.2f}%)", fontweight="bold")
    ax.set_ylabel("Sends"); ax.set_ylim(0, ymax_sends)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x/1e6:.1f}M"))
    ax.tick_params(axis="y", labelleft=True)
    style_ax(ax)
    ax2 = ax.twinx()
    ax2.plot(mpos, sub["unsub_per_email_pct"], color=C_LINE, marker="o",
             linewidth=1.8, markersize=4)
    ax2.set_ylim(0, ymax_rate)
    ax2.set_ylabel("Unsubs per email %", color=C_LINE, fontsize=8)
    ax2.tick_params(axis="y", colors=C_LINE)
    ax2.spines["top"].set_visible(False)
for i in range(len(lob_order), len(axes)):
    axes[i].set_visible(False)
for ax in axes[:len(lob_order)]:
    ax.set_xticks(list(mpos)); ax.set_xticklabels(months, rotation=45, fontsize=8)
fig.suptitle("Q0: Monthly Sends and Unsub Rate by LOB — Aug 2025 to Jun 2026\n"
             "(bars = delivered emails; red line = unsubs per delivered email %  |  "
             "LOB from mapping file)", fontsize=12, fontweight="bold", y=0.995)
plt.tight_layout(rect=[0, 0, 1, 0.97]); plt.show()

# %% [markdown]
# ## Q1: Volume vs Rate — Who Concentrates Unsubs? Jan to Apr 2026
# Unsub rate % = unsubs / senders x 100 (a2 senders = unique clients
# mailed). Rate ranking requires senders >= 10,000 — smaller audiences
# produce noisy rates.

# %% [2] Q1 — top 10 by volume and by rate, colored by LOB
q1a = """
SELECT TRIM(a2.mne) AS mne, a2.senders, a2.unsubs_attributed,
       ROUND(a2.unsubs_attributed * 100.0 / NULLIF(a2.senders, 0), 2) AS unsub_rate_pct
FROM a2 WHERE a2.senders > 0
ORDER BY a2.unsubs_attributed DESC LIMIT 10
"""
q1b = f"""
SELECT TRIM(a2.mne) AS mne, a2.senders, a2.unsubs_attributed,
       ROUND(a2.unsubs_attributed * 100.0 / NULLIF(a2.senders, 0), 2) AS unsub_rate_pct
FROM a2 WHERE a2.senders >= {SMALL_BASE}
ORDER BY unsub_rate_pct DESC LIMIT 10
"""
d1a, d1b = con.execute(q1a).df(), con.execute(q1b).df()
lobmap = con.execute(
    "SELECT TRIM(MNEMONIC) AS mne, UPPER(TRIM(LOB_Manual)) AS lob FROM mapping").df()
for d_ in (d1a, d1b):
    d_["mne"] = d_["mne"].str.strip()
    d_.merge(lobmap, on="mne", how="left")
d1a = d1a.merge(lobmap, on="mne", how="left")
d1b = d1b.merge(lobmap, on="mne", how="left")
display(d1a); display(d1b)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
for ax, d_, val, ttl in [(ax1, d1a.iloc[::-1], "unsubs_attributed", "Top 10 by Volume"),
                         (ax2, d1b.iloc[::-1], "unsub_rate_pct", "Top 10 by Rate % (senders >= 10K)")]:
    cols = [lob_colors.get(l, "#899299") for l in d_["lob"]]
    ax.barh(d_["mne"], d_[val], color=cols)
    for y_, (v, n_) in enumerate(zip(d_[val], d_["senders"])):
        lab = f" {compact_n(v)}" if val == "unsubs_attributed" else f" {v:.2f}%"
        ax.text(v, y_, lab + f"  (n={compact_n(n_)})", va="center", fontsize=8)
    ax.set_title(ttl, fontweight="bold"); style_ax(ax)
    ax.set_xlim(0, d_[val].max() * 1.35)
pcq_in_both = "PCQ" in set(d1a["mne"]) and "PCQ" in set(d1b["mne"])
fig.suptitle("Q1: Who Concentrates Unsubs — Volume vs Rate, Jan to Apr 2026"
             + ("\n(PCQ is the only Cards-pod campaign in BOTH top-10s)" if pcq_in_both else ""),
             fontsize=12, fontweight="bold")
_h = [Patch(color=c_, label=l_) for l_, c_ in lob_colors.items()
      if l_ in set(d1a["lob"]) | set(d1b["lob"])]
fig.legend(handles=_h, loc="lower center", ncol=6, frameon=False, fontsize=8)
plt.tight_layout(rect=[0, 0.05, 1, 0.93]); plt.show()

# %% [markdown]
# ## Q2: 12-Month Unsub Curve — Aug 2025 to Jul 2026
# Monthly view, full 12-month range. Red series = Cards LOB per mapping
# file. Last 1-2 months (202606-202607) are immature — identity-bridge
# rows lag loading.

# %% [3] Q2 — curve build + FWC timing check
cards_mnes = con.execute(
    "SELECT TRIM(MNEMONIC) AS mne FROM mapping WHERE UPPER(TRIM(LOB_Manual)) = 'CARDS'"
).df()["mne"].tolist()
print(f"Cards MNEs ({len(cards_mnes)}):", sorted(cards_mnes))
_in_cards = ", ".join(f"'{m}'" for m in cards_mnes)
q2 = f"""
SELECT c.ym,
       SUM(c.unsubs_attributed) AS enterprise_unsubs,
       SUM(CASE WHEN TRIM(c.mne) IN ({_in_cards}) THEN c.unsubs_attributed ELSE 0 END) AS cards_unsubs,
       SUM(c.sends) AS enterprise_sends,
       ROUND(SUM(CASE WHEN TRIM(c.mne) IN ({_in_cards}) THEN c.unsubs_attributed ELSE 0 END)
             * 100.0 / NULLIF(SUM(c.unsubs_attributed), 0), 1) AS cards_pct
FROM c WHERE c.ym BETWEEN '202508' AND '202607'
GROUP BY 1 ORDER BY 1
"""
curve = con.execute(q2).df()
curve["immature"] = curve["ym"].isin(["202606", "202607"])
mature = curve[~curve["immature"]]
peak_pct = mature["cards_pct"].max()
peak_month = mature.loc[mature["cards_pct"].idxmax(), "ym"]
display(curve)
print(f"Peak Cards share (mature months only): {peak_pct:.1f}% in {peak_month}")

fwc_timing = con.execute("""
SELECT c.ym, SUM(c.sends) AS fwc_sends, SUM(c.unsubs_attributed) AS fwc_unsubs
FROM c WHERE TRIM(c.mne) = 'FWC' AND c.ym BETWEEN '202508' AND '202607'
GROUP BY 1 ORDER BY 1""").df()
print("--- FWC TIMING CHECK: sends vs unsubs by month ---")
display(fwc_timing)

fig, (axp1, axp2, axp3) = plt.subplots(3, 1, figsize=(14, 14))
x = range(len(curve))
# Panel 1: stacked bar — Cards LOB vs rest of enterprise
rest = curve["enterprise_unsubs"] - curve["cards_unsubs"]
axp1.bar(x, curve["cards_unsubs"], color=C_LINE, label="Cards LOB (mapping file)")
axp1.bar(x, rest, bottom=curve["cards_unsubs"], color=C_THEN, alpha=0.75,
         label="Rest of Enterprise")
axp1.set_xticks(list(x)); axp1.set_xticklabels(curve["ym"], rotation=45, fontsize=8)
axp1.yaxis.set_major_formatter(FuncFormatter(lambda v_, _: compact_n(v_)))
axp1.set_title("Monthly unsub events — enterprise, Cards share stacked", fontweight="bold")
axp1.legend(frameon=False, fontsize=8); style_ax(axp1)
# Panel 2: Cards share % — same event basis as the monthly bars
mature_avg = mature["cards_pct"].mean()
axp2.plot(x, curve["cards_pct"], color=C_LINE, marker="o")
for xi, (p_, imm) in enumerate(zip(curve["cards_pct"], curve["immature"])):
    axp2.annotate(f"{p_:.1f}", (xi, p_), textcoords="offset points", xytext=(0, 6),
                  fontsize=8, color="#888" if imm else C_LINE)
axp2.axhline(mature_avg, color="#999", linestyle="--", linewidth=1)
axp2.text(0, mature_avg, f" mature avg {mature_avg:.1f}%", fontsize=8, va="bottom")
axp2.set_xticks(list(x)); axp2.set_xticklabels(curve["ym"], rotation=45, fontsize=8)
axp2.set_ylabel("Cards share of monthly unsub events (%)")
axp2.set_title(f"Cards unsub share — peaked at ~{peak_pct:.1f}% in {peak_month} "
               "(202606-202607 immature, greyed)", fontweight="bold")
style_ax(axp2)
# Panel 3: FWC timing check
xf = range(len(fwc_timing))
axp3.bar(xf, fwc_timing["fwc_sends"], color=C_THEN, alpha=0.75, label="FWC Sends (delivered emails)")
axp3b = axp3.twinx()
axp3b.plot(xf, fwc_timing["fwc_unsubs"], color=C_LINE, marker="o", label="FWC Unsubs")
axp3.set_xticks(list(xf)); axp3.set_xticklabels(fwc_timing["ym"], rotation=45, fontsize=8)
axp3.yaxis.set_major_formatter(FuncFormatter(lambda v_, _: compact_n(v_)))
axp3b.tick_params(axis="y", colors=C_LINE)
axp3.set_title("FWC Timing Check: unsubs followed FWC send waves with 0-1 month lag",
               fontweight="bold")
style_ax(axp3); axp3b.spines["top"].set_visible(False)
fig.suptitle(f"Q2: Cards unsub share peaked at ~{peak_pct:.0f}% in {peak_month}, "
             "coinciding with the FIFA campaign (FWC)", fontsize=13, fontweight="bold")
plt.tight_layout(rect=[0, 0, 1, 0.96]); plt.show()
print("Note: Cards deduped unique-person share (20.5%) not shown above — "
      "different basis (unique clients, not events).")

# %% [markdown]
# ## Q3: Deep Dive — Cards by Action Type
# 1. Which action types concentrate the most unsubs?
# 2. Which have the highest unsub rate % (unsubs/senders x 100)?
# 3. Monthly curve by action type — is the Feb-Apr spike concentrated in
#    one type?
# Cards deduped unique-person share (Jan-Apr 2026) = 20.5% — different
# basis (unique clients; campaign counts add to more than unique people).

# %% [4] Q3 — tables + 4-panel figure + audience/rate two-panel
cards_action = con.execute("""
SELECT TRIM(m.ACTION_TYPE) AS action_type, COUNT(*) AS n_mnes,
       SUM(a2.senders) AS total_senders, SUM(a2.unsubs_attributed) AS total_unsubs,
       ROUND(SUM(a2.unsubs_attributed) * 100.0 / NULLIF(SUM(a2.senders), 0), 2) AS unsub_rate_pct
FROM a2 JOIN mapping m ON TRIM(a2.mne) = TRIM(m.MNEMONIC)
WHERE UPPER(TRIM(m.LOB_Manual)) = 'CARDS'
GROUP BY 1 ORDER BY total_unsubs DESC""").df()
cards_mne_detail = con.execute("""
SELECT TRIM(m.ACTION_TYPE) AS action_type, TRIM(a2.mne) AS mne,
       TRIM(m.MNE_DESC) AS description, a2.senders, a2.unsubs_attributed AS unsubs,
       ROUND(a2.unsubs_attributed * 100.0 / NULLIF(a2.senders, 0), 2) AS unsub_rate_pct
FROM a2 JOIN mapping m ON TRIM(a2.mne) = TRIM(m.MNEMONIC)
WHERE UPPER(TRIM(m.LOB_Manual)) = 'CARDS' AND a2.senders > 0
ORDER BY a2.senders DESC""").df()
cards_monthly_action = con.execute("""
SELECT c.ym, TRIM(m.ACTION_TYPE) AS action_type,
       SUM(c.unsubs_attributed) AS unsubs
FROM c JOIN mapping m ON TRIM(c.mne) = TRIM(m.MNEMONIC)
WHERE UPPER(TRIM(m.LOB_Manual)) = 'CARDS' AND c.ym BETWEEN '202508' AND '202607'
GROUP BY 1, 2 ORDER BY 1, 2""").df()
print("--- CARDS UNSUBS BY ACTION_TYPE ---"); display(cards_action)
print("--- ALL CARDS MNEs ---"); display(cards_mne_detail)

def action_label(row):
    if row["action_type"] == "Pre_Attract" and row["n_mnes"] == 1:
        return "FWC (FIFA)"
    return f"{row['action_type']} ({row['n_mnes']} MNEs)"
cards_action["label"] = cards_action.apply(action_label, axis=1)

fig, axes4 = plt.subplots(4, 1, figsize=(14, 20),
                          gridspec_kw={"height_ratios": [1.1, 1.0, 1.2, 1.2]})
# Panel 1: volume by action type
d_ = cards_action.iloc[::-1]
axes4[0].barh(d_["label"], d_["total_unsubs"],
              color=[colors_at.get(a, "#899299") for a in d_["action_type"]])
for y_, v in enumerate(d_["total_unsubs"]):
    axes4[0].text(v, y_, f" {v:,.0f}", va="center", fontsize=9, fontweight="bold")
axes4[0].set_title("Volume by Action Type (total unsubs, Jan-Apr)", fontweight="bold")
axes4[0].set_xlim(0, d_["total_unsubs"].max() * 1.2); style_ax(axes4[0])
# Panel 2: rate % (G1/G2/G3)
axes4[1].barh(d_["label"], d_["unsub_rate_pct"],
              color=[colors_at.get(a, "#899299") for a in d_["action_type"]])
for y_, (r_, n_) in enumerate(zip(d_["unsub_rate_pct"], d_["total_senders"])):
    badge = " ⚠" if n_ < SMALL_BASE else ""
    axes4[1].text(r_, y_, f" {r_:.2f}% (n={compact_n(n_)}){badge}", va="center", fontsize=9)
axes4[1].set_title("Unsub Rate by Action Type (unsubs/senders x 100)", fontweight="bold")
axes4[1].set_xlim(0, d_["unsub_rate_pct"].max() * 1.35); style_ax(axes4[1])
# Panel 3: monthly trend by action type
pv = cards_monthly_action.pivot(index="ym", columns="action_type", values="unsubs").fillna(0)
for at_ in pv.columns:
    axes4[2].plot(range(len(pv)), pv[at_], marker="o", markersize=3,
                  label=at_, color=colors_at.get(at_, "#899299"))
axes4[2].axvspan(list(pv.index).index("202602"), list(pv.index).index("202604"),
                 color="#FCA311", alpha=0.10)
axes4[2].set_xticks(range(len(pv))); axes4[2].set_xticklabels(pv.index, rotation=45, fontsize=8)
axes4[2].yaxis.set_major_formatter(FuncFormatter(lambda v_, _: compact_n(v_)))
axes4[2].legend(frameon=False, fontsize=8, ncol=2)
axes4[2].set_title("Monthly Trend by Action Type (Feb-Apr spike concentrated in FWC)",
                   fontweight="bold"); style_ax(axes4[2])
# Panel 4: MNE landscape scatter (log-x senders vs rate, bubble = unsubs)
det = cards_mne_detail[cards_mne_detail["senders"] > 0]
axes4[3].scatter(det["senders"], det["unsub_rate_pct"],
                 s=(det["unsubs"] / det["unsubs"].max() * 900 + 20),
                 c=[colors_at.get(a, "#899299") for a in det["action_type"]], alpha=0.75)
for _, r_ in det.iterrows():
    mark = "△ " if r_["senders"] < SMALL_BASE else ""
    axes4[3].annotate(f"{mark}{r_['mne']}", (r_["senders"], r_["unsub_rate_pct"]),
                      textcoords="offset points", xytext=(6, 3), fontsize=8)
axes4[3].set_xscale("log")
axes4[3].set_xlabel("senders (unique clients mailed, log scale)")
axes4[3].set_ylabel("unsub rate %")
axes4[3].set_title(f"MNE Landscape: Audience Size vs Unsub Rate "
                   f"(bubble = unsub volume | △ = < {SMALL_BASE:,} senders)",
                   fontweight="bold"); style_ax(axes4[3])
fig.suptitle("Q3: Deep Dive — Cards Unsubs by Action Type (Cards LOB, mapping file)",
             fontsize=13, fontweight="bold")
plt.tight_layout(rect=[0, 0, 1, 0.97]); plt.show()

# TWO-PANEL layout (T6: no dual-axis combo) — audience size + rate per MNE
cards_vol = cards_mne_detail.copy()
cards_vol["small_base"] = cards_vol["senders"].apply(lambda s_: "⚠" if s_ < SMALL_BASE else "")
d_ = cards_vol.sort_values("senders", ascending=True)
fig, (axl, axr) = plt.subplots(1, 2, figsize=(16, max(5, len(d_) * 0.35)))
axl.barh(d_["mne"], d_["senders"],
         color=[colors_at.get(a, "#899299") for a in d_["action_type"]])
for y_, v in enumerate(d_["senders"]):
    axl.text(v, y_, f" n={compact_n(v)}", va="center", fontsize=8)
axl.set_title("Audience Size (unique clients mailed)", fontweight="bold")
axl.set_xlim(0, d_["senders"].max() * 1.25); style_ax(axl)
axr.barh(d_["mne"], d_["unsub_rate_pct"],
         color=[colors_at.get(a, "#899299") for a in d_["action_type"]])
for y_, (r_, b_) in enumerate(zip(d_["unsub_rate_pct"], d_["small_base"])):
    axr.text(r_, y_, f" {r_:.2f}%{b_}", va="center", fontsize=8)
axr.set_title("Unsub Rate %", fontweight="bold")
axr.set_xlim(0, d_["unsub_rate_pct"].max() * 1.3); style_ax(axr)
_h = [Patch(color=c_, label=a_) for a_, c_ in colors_at.items()
      if a_ in set(d_["action_type"])]
fig.legend(handles=_h, loc="lower center", ncol=4, frameon=False, fontsize=8)
fig.suptitle(f"Cards MNEs: Audience Size and Unsub Rate (Cards LOB, mapping file)\n"
             f"⚠ = < {SMALL_BASE:,} senders (small base)", fontweight="bold")
plt.tight_layout(rect=[0, 0.05, 1, 0.93]); plt.show()

# %% [markdown]
# ## Q4: Contact Frequency — Jan to Apr 2026
# Cards-email view. PRIMARY: distribution of stayers vs unsubs by # Cards
# emails received. SECONDARY: unsub rate by bucket. Survivorship caveat:
# selection, not treatment effect.

# %% [5] Q4 — contact frequency, two panels
q4b_sql = """
SELECT n_emails_cards_bucket AS bucket,
       SUM(clients_total) AS clients, SUM(stayers) AS stayers,
       SUM(unsubs_any) AS unsubs_any, SUM(unsubs_cards) AS unsubs_cards
FROM a3 GROUP BY 1
ORDER BY CASE bucket WHEN '0' THEN 0 WHEN '1-2' THEN 1 WHEN '3-5' THEN 2
                     WHEN '6-10' THEN 3 WHEN '11+' THEN 4 ELSE 9 END
"""
df4 = con.execute(q4b_sql).df()
display(df4)
dist = df4[df4["bucket"] != "0"].copy()
dist["pct_of_unsubs"] = dist["unsubs_cards"] / dist["unsubs_cards"].sum() * 100
dist["pct_of_stayers"] = dist["stayers"] / dist["stayers"].sum() * 100
dist["rate_pct"] = dist["unsubs_cards"] / dist["clients"] * 100

fig, (axl, axr) = plt.subplots(1, 2, figsize=(14, 5.5))
xb = np.arange(len(dist)); w = 0.38
axl.bar(xb - w/2, dist["pct_of_stayers"], w, color=C_THEN, label="stayers")
axl.bar(xb + w/2, dist["pct_of_unsubs"], w, color=C_NOW, label="Cards unsubs")
for xi, (s_, u_) in zip(xb, zip(dist["pct_of_stayers"], dist["pct_of_unsubs"])):
    axl.text(xi - w/2, s_, f"{s_:.0f}%", ha="center", va="bottom", fontsize=9)
    axl.text(xi + w/2, u_, f"{u_:.0f}%", ha="center", va="bottom", fontsize=9, fontweight="bold")
axl.set_xticks(xb)
axl.set_xticklabels([f"{b}\n(n={compact_n(c_)})" for b, c_ in zip(dist["bucket"], dist["clients"])])
axl.set_xlabel("Cards emails received (Jan-Apr)"); axl.set_ylabel("% of group")
axl.set_title("Distribution: stayers vs unsubs", fontweight="bold")
axl.legend(frameon=False); style_ax(axl)
axr.bar(dist["bucket"], dist["rate_pct"], color=C_THEN)
for xi, r_ in enumerate(dist["rate_pct"]):
    axr.text(xi, r_, f"{r_:.2f}%", ha="center", va="bottom", fontsize=9, fontweight="bold")
axr.set_ylim(0, dist["rate_pct"].max() * 1.25)
axr.set_ylabel("unsubscribed in window (%)")
axr.set_title("Cards unsubs concentrate at first contact (1-2 emails)\n"
              "(survivorship: selection, not treatment effect)", fontweight="bold")
style_ax(axr)
fig.suptitle("Q4: Contact Frequency (Cards Emails Only) — Jan to Apr 2026",
             fontsize=12, fontweight="bold")
plt.tight_layout(rect=[0, 0, 1, 0.93]); plt.show()

# %% [markdown]
# ## Q5: Who Unsubscribes — Age, Tenure, Products — Jan to Apr 2026
# UCP match ~90.8%; no_ucp_match (held_* = -1) shown separately, never
# folded into 0. Representation ratio = share of unsubs in a band / share
# of stayers in that band; > 1 = over-represented among unsubs.

# %% [6] Q5 — rep-ratio pulls + 3-panel chart
def _band_pull(band_col, extra_where=""):
    return con.execute(f"""
    SELECT {band_col} AS band,
           SUM(clients_total) AS clients, SUM(stayers) AS stayers,
           SUM(unsubs_any) AS unsubs_any, SUM(unsubs_cards) AS unsubs_cards
    FROM a4 {extra_where} GROUP BY 1""").df()

q5_age = _band_pull("age_band")
q5_ten = _band_pull("tenure_band")
q5_tibc = con.execute("""
SELECT held_t, held_i, held_b, held_c,
       SUM(clients_total) AS clients, SUM(stayers) AS stayers,
       SUM(unsubs_any) AS unsubs_any, SUM(unsubs_cards) AS unsubs_cards
FROM a4 GROUP BY 1, 2, 3, 4""").df()
print("--- BY AGE BAND (incl no_ucp_match) ---"); display(q5_age)
print("--- BY TENURE BAND ---"); display(q5_ten)

def calc_rep_ratio(df_, unsub_col="unsubs_cards", stayer_col="stayers"):
    out = df_.copy()
    out["rep_ratio"] = ((out[unsub_col] / out[unsub_col].sum())
                        / (out[stayer_col] / out[stayer_col].sum()))
    return out

def _tibc_label(r_):
    if r_["held_t"] == -1:
        return "no_ucp_match"
    parts = [n_ for f_, n_ in [("held_t", "Transaction"), ("held_i", "Investment"),
                               ("held_b", "Borrowing"), ("held_c", "Credit")]
             if r_[f_] == 1]
    return " + ".join(parts) if parts else "none"
q5_tibc["band"] = q5_tibc.apply(_tibc_label, axis=1)
q5_tibc = (q5_tibc.groupby("band", as_index=False)
           [["clients", "stayers", "unsubs_any", "unsubs_cards"]].sum())

no_ucp = q5_tibc[q5_tibc["band"] == "no_ucp_match"]
no_ucp_n = int(no_ucp["clients"].sum())
no_ucp_pct = no_ucp_n / q5_tibc["clients"].sum() * 100

AGE_ORDER = ["<25", "25-34", "35-49", "50-64", "65+"]
TEN_ORDER = ["<1yr", "1-3yr", "4-7yr", "8-15yr", "16yr+"]
fig, (axA, axT, axP) = plt.subplots(1, 3, figsize=(17, 6))
for ax, df_, order, ttl in [
        (axA, q5_age, AGE_ORDER, "By Age\n(Younger over-represented among unsubs)"),
        (axT, q5_ten, TEN_ORDER, "By Tenure\n(Cards unsubs peak at 4-7yr)"),
        (axP, q5_tibc, None, "Product Mix (TIBC)\n(Credit-holding combos over-represented)")]:
    d_ = df_[df_["band"] != "no_ucp_match"].copy()
    d_ = calc_rep_ratio(d_)
    if order:
        d_ = d_.set_index("band").reindex(order).dropna().reset_index()
    else:
        d_ = d_.sort_values("clients", ascending=False).head(8).iloc[::-1]
    hh = 0.35
    yy = np.arange(len(d_))
    d_any = calc_rep_ratio(d_, unsub_col="unsubs_any")
    ax.barh(yy + hh/2, d_any["rep_ratio"], hh, color=C_THEN, label="Any RBC unsub")
    ax.barh(yy - hh/2, d_["rep_ratio"], hh, color=C_LINE, label="Cards unsub")
    ax.axvline(1.0, color="#666", linewidth=1)
    ax.set_yticks(yy)
    ax.set_yticklabels([f"{b} (n={compact_n(c_)})" for b, c_ in zip(d_["band"], d_["clients"])],
                       fontsize=8)
    ax.set_xlabel("Representation ratio (1.0 = proportional)")
    ax.set_title(ttl, fontweight="bold", fontsize=10)
    style_ax(ax)
axA.legend(frameon=False, fontsize=8)
fig.suptitle("Q5: Representation Ratio — Who Unsubscribes vs Stayers? Jan to Apr 2026\n"
             "(ratio > 1 = over-represented among unsubs)", fontsize=12, fontweight="bold")
plt.tight_layout(rect=[0, 0, 1, 0.92]); plt.show()
print(f"Excludes {no_ucp_n:,} clients with no UCP match ({no_ucp_pct:.1f}%).")

# %% [markdown]
# # D: Delta Section — Cards Cohort Then vs Now (Jun 30 2025 -> Jun 30 2026)
# Cohort = 4,783,193 clients mailed by a Cards campaign on/before the
# anchor. STAYERS = no Cards-marketing unsub by anchor; LEAVERS_ALL = did.
# Spend = avg monthly CARD spend (3-mo window / 3, Cards only, DFP-matched).
# Profitability = UCP annual estimate (all RBC products; not validated LTV).
# Product count = categories held (0-4: Transaction/Investment/Borrowing/
# Credit). This is composition and observed change, not a treatment effect.

# %% [7] D0 — validation gates (run before any D chart)
b_delta = _frames["b_delta"]
piv = b_delta.pivot_table(index=["group", "metric"], columns="period",
                          values="value", aggfunc="first")
def get_val(group, metric, period):
    return piv.loc[(group, metric), period]
def get_n(group):
    return get_val(group, "n_clients", "then")
# --- SANITY GATE 1: cohort anchor ---
cohort_check = get_n("STAYERS") + get_n("LEAVERS_ALL")
assert abs(cohort_check - 4_783_193) < 1, f"FAIL: cohort anchor = {cohort_check:,.0f}"
# --- SANITY GATE 2: campaign groups sum to LEAVERS_ALL ---
camp_sum = sum(get_n(g) for g in ["PCL", "PCD", "PCQ", "LEAVERS_OTHER"]
               if (g, "n_clients") in piv.index)
assert abs(camp_sum - get_n("LEAVERS_ALL")) < 1, f"FAIL: campaign sum {camp_sum:,.0f}"
# --- SANITY GATE 3: delta sign spot-check (PCQ spend) ---
spot_delta = get_val("PCQ", "spend_avg", "delta")
computed = get_val("PCQ", "spend_avg", "now") - get_val("PCQ", "spend_avg", "then")
assert pd.isna(spot_delta) or abs(spot_delta - computed) < 0.01, "FAIL: delta != now - then"
SANITY_OK = True
print("SANITY GATES PASSED: cohort anchor, campaign sum, delta arithmetic.")

# %% [8] D1 — spend headline (then vs now, DFP-matched clients only)
# DFP match caveat: leavers ~50% no-match vs stayers ~27% — averages are
# over matched clients only; no_dfp counts shown in the table.
grps_d = ["STAYERS", "LEAVERS_ALL"]
d1 = pd.DataFrame({
    "group": grps_d,
    "n": [get_n(g) for g in grps_d],
    "spend_then": [get_val(g, "spend_avg", "then") for g in grps_d],
    "spend_now": [get_val(g, "spend_avg", "now") for g in grps_d],
})
d1["spend_delta"] = d1["spend_now"] - d1["spend_then"]
d1["delta_pct"] = d1["spend_delta"] / d1["spend_then"] * 100
display(d1)
print("Excludes LEAVERS_OTHER (not shown).")
fig, ax = plt.subplots(figsize=(9, 5.5))
xg = np.arange(2); w = 0.36
ax.bar(xg - w/2, d1["spend_then"], w, color=C_THEN, label="Then (Jun 2025)")
ax.bar(xg + w/2, d1["spend_now"], w, color=C_NOW, label="Now (Jun 2026)")
for xi, r_ in d1.iterrows():
    ax.text(xi - w/2, r_["spend_then"], f"${r_['spend_then']:,.0f}", ha="center",
            va="bottom", fontsize=9, fontweight="bold")
    ax.text(xi + w/2, r_["spend_now"], f"${r_['spend_now']:,.0f}", ha="center",
            va="bottom", fontsize=9, fontweight="bold")
    ax.text(xi, max(r_["spend_then"], r_["spend_now"]) * 1.12,
            f"{'+' if r_['spend_delta'] >= 0 else ''}${r_['spend_delta']:,.0f} "
            f"({r_['delta_pct']:+.1f}%)", ha="center", fontsize=10,
            color=C_POS if r_["spend_delta"] >= 0 else C_LINE, fontweight="bold")
ax.set_ylim(0, d1[["spend_then", "spend_now"]].values.max() * 1.3)
ax.set_xticks(xg)
ax.set_xticklabels([f"{g}\n(n={compact_n(n_)})" for g, n_ in zip(d1["group"], d1["n"])])
ax.set_ylabel("avg monthly card spend ($, DFP-matched)")
ax.legend(frameon=False); style_ax(ax)
ax.set_title("D1: Average Monthly Card Spend — Then vs Now\n"
             "(Cards products only; delta = now minus then)", fontweight="bold")
plt.tight_layout(); plt.show()

# %% [9] D2 — profitability + product count (avg-vs-median skew check)
d2 = pd.DataFrame({
    "group": grps_d,
    "prof_avg_then": [get_val(g, "prof_avg", "then") for g in grps_d],
    "prof_avg_now": [get_val(g, "prof_avg", "now") for g in grps_d],
    "prof_med_then": [get_val(g, "prof_med", "then") for g in grps_d],
    "prof_med_now": [get_val(g, "prof_med", "now") for g in grps_d],
    "prod_then": [get_val(g, "prod_cnt_avg", "then") for g in grps_d],
    "prod_now": [get_val(g, "prod_cnt_avg", "now") for g in grps_d],
})
d2["avg_delta_pct"] = (d2["prof_avg_now"] - d2["prof_avg_then"]) / d2["prof_avg_then"] * 100
d2["med_delta_pct"] = (d2["prof_med_now"] - d2["prof_med_then"]) / d2["prof_med_then"] * 100
display(d2)
_l = d2[d2["group"] == "LEAVERS_ALL"].iloc[0]
diverges = abs(_l["avg_delta_pct"] - _l["med_delta_pct"]) > 20
lead_metric = "median" if diverges else "avg"
print("Avg and median diverge — leading with MEDIAN." if diverges
      else "Avg and median are broadly consistent — leading with AVERAGE.")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
pcol_then = "prof_med_then" if lead_metric == "median" else "prof_avg_then"
pcol_now = "prof_med_now" if lead_metric == "median" else "prof_avg_now"
ttl1 = ("D2a: Annual Profitability (MEDIAN)\nleading with median — tail outliers inflate average"
        if lead_metric == "median" else
        "D2a: Annual Profitability (average)\navg and median consistent")
for ax, tc, nc, ttl, fmt in [(ax1, pcol_then, pcol_now, ttl1, "${:,.0f}"),
                             (ax2, "prod_then", "prod_now",
                              "D2b: Product Count (categories held)\ndelta = now minus then", "{:.2f}")]:
    for xi, r_ in d2.iterrows():
        ax.bar(xi - 0.18, r_[tc], 0.36, color=C_THEN)
        ax.bar(xi + 0.18, r_[nc], 0.36, color=C_NOW)
        ax.text(xi - 0.18, r_[tc], fmt.format(r_[tc]), ha="center", va="bottom", fontsize=8.5)
        ax.text(xi + 0.18, r_[nc], fmt.format(r_[nc]), ha="center", va="bottom", fontsize=8.5)
        d_ = r_[nc] - r_[tc]
        ax.text(xi, max(r_[tc], r_[nc]) * 1.12,
                (f"+{d_:,.0f}" if ttl.startswith("D2a") else f"{d_:+.3f}"),
                ha="center", fontsize=9.5, fontweight="bold",
                color=C_POS if d_ >= 0 else C_LINE)
    ax.set_ylim(0, d2[[tc, nc]].values.max() * 1.3)
    ax.set_xticks(range(2))
    ax.set_xticklabels([f"{g}\n(n={compact_n(get_n(g))})" for g in grps_d])
    ax.set_title(ttl, fontweight="bold", fontsize=10); style_ax(ax)
ax2.set_ylabel("Avg product categories held (0-4)")
fig.legend(handles=[Patch(color=C_THEN, label="Then (Jun 2025)"),
                    Patch(color=C_NOW, label="Now (Jun 2026)")],
           loc="lower center", ncol=2, frameon=False, fontsize=9)
plt.tight_layout(rect=[0, 0.06, 1, 1]); plt.show()
print("Excludes LEAVERS_OTHER (not shown). UCP no-match excluded from averages.")

# %% [markdown]
# # A: Stakeholder follow-up — the three asks (2026-08-06 feedback email)
# 1. Loyalty x Cards OVERLAP (A3) · 2. ATTRITION (A2) · 3. PROFIT
# population check (A1). Definitions as the D section: cohort 4,783,193,
# anchors Jun 30 2025 -> Jun 30 2026, "leaver" = Cards-marketing unsub by
# anchor (NOT account closure). No-UCP-match shown, never dropped silently.

# %% [markdown]
# ## A1: PROFIT CHECK — profit recomputed on a fixed population
# ELI5: 100 leavers in June 2025; a year later 6 no longer appear in the
# profitability data. ORIGINAL basis averages only the 94 present (grading
# the class after dropouts left). FIXED basis keeps all 100 — vanished
# count $0. Result (run 2026-08-06): finding SURVIVES — leavers $550->$688
# (+25.1%), stayers $795->$982 (+23.6%); vanished were low-value ($134 /
# $183). Basis (b) is the reported number. The check surfaced: leavers
# vanish at 5.9% vs 2.6% (2.2x) — feeds A2.

# %% [10] A1 — profit three bases (needs pm_asks_results.csv)
if not HAS_PM:
    print("SKIP: pm_asks_results.csv not in BASE - run spotlight/"
          "pm_asks_recompute.py in the pod once, then rerun this cell.")
else:
    pmw = (con.execute("SELECT * FROM pm").df()
           .pivot_table(index=["table", "grp"], columns="metric",
                        values="value", aggfunc="first"))
    prof = pmw.loc["profit_three_ways"]
    grps = ["stayer", "leaver"]
    ns = [int(prof.loc[g, "n_then_matched"]) for g in grps]
    bases = [("(a) ORIGINAL basis — survivors only", "avg_then_survivors", "avg_now_survivors"),
             ("(b) FIXED basis — everyone kept, vanished = $0", "avg_then_all", "avg_now_zerofill")]
    fig, axes = plt.subplots(1, 2, figsize=(13, 6), sharey=True)
    ymax = 0
    for ax, (label, tc, nc) in zip(axes, bases):
        thin = [float(prof.loc[g, tc]) for g in grps]
        now = [float(prof.loc[g, nc]) for g in grps]
        ymax = max([ymax] + thin + now)
        xg = np.arange(2); w = 0.36
        ax.bar(xg - w/2, thin, w, color=C_THEN, label="avg profit Jun 2025")
        ax.bar(xg + w/2, now, w, color=C_NOW, label="avg profit Jun 2026")
        for xi, (t, n_) in zip(xg, zip(thin, now)):
            ax.text(xi - w/2, t + 12, f"${t:,.0f}", ha="center", va="bottom",
                    fontsize=9, fontweight="bold")
            ax.text(xi + w/2, n_ + 12, f"${n_:,.0f}", ha="center", va="bottom",
                    fontsize=9, fontweight="bold")
            d_, dp = n_ - t, (n_ - t) / t * 100
            ax.text(xi, max(t, n_) * 1.18,
                    f"{'+' if d_ >= 0 else ''}${d_:,.0f}  ({dp:+.1f}%)", ha="center",
                    fontsize=10.5, color=C_POS if d_ >= 0 else C_LINE, fontweight="bold")
        ax.set_xticks(xg)
        ax.set_xticklabels([f"{g.upper()}\nn = {n_:,}" for g, n_ in zip(grps, ns)])
        ax.set_title(label, fontweight="bold", fontsize=11, pad=10); style_ax(ax)
    for ax in axes:
        ax.set_ylim(0, ymax * 1.35)
    axes[0].set_ylabel("avg annual profit estimate ($, UCP)")
    _h, _l = axes[0].get_legend_handles_labels()
    fig.legend(_h, _l, loc="upper right", frameon=False, fontsize=9, ncol=2,
               bbox_to_anchor=(0.99, 0.90))
    fig.suptitle("A1 PROFIT CHECK: Did unsubscribers' profit really grow? "
                 "Same data, two ways of counting", fontsize=12.5, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.85]); plt.show()
    print("HOW TO READ: (a) silently drops clients who vanished by 2026;"
          " (b) keeps every June-2025 client, vanished at $0. The story"
          " holds in (b): both groups grow, leavers slightly faster in %.")

# %% [markdown]
# ## A2: ATTRITION — do unsubscribers actually leave more? Yes, on every cut.
# ELI5: of clients who HAD a card in June 2025 — a year later: still has
# cards / no cards but still a client ("lost cards") / not in the data at
# all ("vanished" = left-bank PROXY; no official closure field here).
# Result: leavers lose cards 1.91% vs 1.63% (x1.17), vanish 1.74% vs
# 1.30% (x1.34); whole-relationship vanish 5.9% vs 2.6% (2.2x).
# Caveat attached: descriptive, not causal — groups not matched.

# %% [11] A2 — attrition + population ledger (needs pm_asks_results.csv)
if not HAS_PM:
    print("SKIP: pm_asks_results.csv not in BASE - see A1 note.")
else:
    led = pmw.loc["population_ledger"]
    att = pmw.loc["card_attrition"]
    grps = ["stayer", "leaver"]
    lost = [float(att.loc[g, "lost_cards_now"] / att.loc[g, "held_cards_then"] * 100) for g in grps]
    van  = [float(att.loc[g, "vanished_from_ucp_now"] / att.loc[g, "held_cards_then"] * 100) for g in grps]
    ns_a = [int(att.loc[g, "held_cards_then"]) for g in grps]
    fig, (axl, axr) = plt.subplots(1, 2, figsize=(13, 5.5))
    xg = np.arange(2); w = 0.36
    axl.bar(xg - w/2, lost, w, color=C_THEN, label="no cards anymore (still a client)")
    axl.bar(xg + w/2, van, w, color=C_LINE, label="gone from the data (left-bank PROXY)")
    for xi, (l_, v_) in zip(xg, zip(lost, van)):
        axl.text(xi - w/2, l_ + 0.03, f"{l_:.2f}%", ha="center", va="bottom",
                 fontsize=9.5, fontweight="bold")
        axl.text(xi + w/2, v_ + 0.03, f"{v_:.2f}%", ha="center", va="bottom",
                 fontsize=9.5, fontweight="bold")
    axl.set_xticks(xg)
    axl.set_xticklabels([f"{g.upper()}\nheld cards Jun 2025: n = {n_:,}"
                         for g, n_ in zip(grps, ns_a)])
    axl.set_ylim(0, max(lost + van) * 1.4)
    axl.set_ylabel("% of Jun-2025 cardholders")
    axl.set_title("Of clients who HAD cards in Jun 2025,\nwho exited by Jun 2026?",
                  fontweight="bold", fontsize=11)
    axl.legend(frameon=False, fontsize=8.5, loc="upper left"); style_ax(axl)
    m_now = [float(led.loc[g, "matched_now"]) for g in grps]
    v_now = [float(led.loc[g, "vanished_now"]) for g in grps]
    xgl = [g.upper() for g in grps]
    axr.bar(xgl, m_now, color="#899299")
    axr.bar(xgl, v_now, bottom=m_now, color=C_LINE)
    for i, g in enumerate(grps):
        mt = float(led.loc[g, "matched_then"])
        axr.text(i, m_now[i] / 2, f"still found\n{compact_n(m_now[i])}", ha="center",
                 va="center", fontsize=9, color="white", fontweight="bold")
        axr.text(i, m_now[i] + v_now[i], f"vanished: {v_now[i]:,.0f}\n({v_now[i] / mt * 100:.1f}%)",
                 ha="center", va="bottom", fontsize=9, fontweight="bold", color=C_LINE)
    axr.set_ylim(0, (np.array(m_now) + np.array(v_now)).max() * 1.28)
    axr.yaxis.set_major_formatter(FuncFormatter(lambda v_, _: compact_n(v_)))
    axr.set_title("Where each group's Jun-2025 clients ended up\n(the counts behind A1's fix)",
                  fontweight="bold", fontsize=11); style_ax(axr)
    fig.suptitle("A2 ATTRITION: Do unsubscribers leave more? (descriptive — groups not matched)",
                 fontsize=12.5, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.92]); plt.show()

# %% [markdown]
# ## A3: OVERLAP — Loyalty x Cards, FIFA isolated
# Groups (mutually exclusive, by which exposures DELIVERED in Jan-Apr):
# Cards only (ex-FIFA) · FIFA only · Loyalty only · All three (partial
# combos counted but not charted). CLEAN ATTRIBUTION: a group's unsub rate
# counts ONLY unsubs on that scope's own lists. Red-team caveats: volume
# confound, targeting selection, window truncation. Mechanics per
# 2026-08-05 verification: disposition 4 = completed per-list opt-out;
# client-level flags, no event inflation.

# %% [12] A3 pull — FIFA-isolated cube, three caches, auto-invalidates
OVERLAP_CSV = os.path.join(BASE, "pm_overlap_results.csv")
_ALL_CACHES = [OVERLAP_CSV,
               os.path.join(BASE, "pm_overlap_detail.csv"),
               os.path.join(BASE, "pm_overlap_mne.csv")]
def _caches_current():
    if not all(os.path.exists(p) for p in _ALL_CACHES):
        return False
    try:
        return "mailed_fwc" in pd.read_csv(OVERLAP_CSV, nrows=0).columns
    except Exception:
        return False

if _caches_current():
    print("CACHED: all three overlap caches exist (v2 schema) - no Teradata pull.")
else:
    for _p in _ALL_CACHES:
        if os.path.exists(_p):
            os.remove(_p); print(f"removed stale cache {_p}")
    import getpass
    import teradatasql
    lobs = con.execute("SELECT TRIM(MNEMONIC) AS mne, UPPER(TRIM(LOB_Manual)) AS lob FROM mapping").df()
    CARDS_L = sorted(set(lobs.loc[lobs["lob"] == "CARDS", "mne"]) - {"FWC"})
    LOY_L = sorted(set(lobs.loc[lobs["lob"] == "LOYALTY", "mne"]))
    assert CARDS_L and LOY_L, "mapping gave empty LOB lists - STOP"
    _in = lambda ms: ", ".join(f"'{m}'" for m in ms)
    _EV = """
        SELECT consumer_id_hashed, TREATMENT_ID,
               SUBSTR(TREATMENT_ID, 8, 3) AS mne,
               MAX(CASE WHEN disposition_cd = 1 THEN 1 ELSE 0 END) AS sent,
               MAX(CASE WHEN disposition_cd = 4 THEN 1 ELSE 0 END) AS unsub
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_dt_tm >= DATE '2026-01-01'
          AND disposition_dt_tm <  DATE '2026-05-01'
          AND disposition_cd IN (1, 4)
          AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
          AND SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
          AND SUBSTR(TREATMENT_ID, 8, 3) IN (%(cards)s, 'FWC', %(loy)s)
        GROUP BY 1, 2, 3
    """
    _IDS = """
        SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
        FROM DTZV01.VENDOR_FEEDBACK_MASTER
        WHERE load_tm >= DATE '2025-10-01' AND CLNT_NO IS NOT NULL
    """
    _sql = ("WITH ev AS (" + _EV + "), ids AS (" + _IDS + """), cl AS (
        SELECT i.CLNT_NO,
               MAX(CASE WHEN e.mne IN (%(cards)s) AND e.sent = 1 THEN 1 ELSE 0 END) AS mailed_cards,
               MAX(CASE WHEN e.mne = 'FWC'        AND e.sent = 1 THEN 1 ELSE 0 END) AS mailed_fwc,
               MAX(CASE WHEN e.mne IN (%(loy)s)   AND e.sent = 1 THEN 1 ELSE 0 END) AS mailed_loy,
               MAX(CASE WHEN e.mne IN (%(cards)s) AND e.unsub = 1 THEN 1 ELSE 0 END) AS unsub_cards,
               MAX(CASE WHEN e.mne = 'FWC'        AND e.unsub = 1 THEN 1 ELSE 0 END) AS unsub_fwc,
               MAX(CASE WHEN e.mne IN (%(loy)s)   AND e.unsub = 1 THEN 1 ELSE 0 END) AS unsub_loy,
               SUM(CASE WHEN e.mne IN (%(cards)s) AND e.sent = 1 THEN 1 ELSE 0 END) AS emails_cards,
               SUM(CASE WHEN e.mne = 'FWC'        AND e.sent = 1 THEN 1 ELSE 0 END) AS emails_fwc,
               SUM(CASE WHEN e.mne IN (%(loy)s)   AND e.sent = 1 THEN 1 ELSE 0 END) AS emails_loy,
               COUNT(DISTINCT CASE WHEN e.mne IN (%(cards)s) AND e.sent = 1 THEN e.mne END) AS mnes_cards,
               COUNT(DISTINCT CASE WHEN e.mne IN (%(loy)s)   AND e.sent = 1 THEN e.mne END) AS mnes_loy
        FROM ev e
        INNER JOIN ids i
           ON i.consumer_id_hashed = e.consumer_id_hashed
          AND i.TREATMENT_ID = e.TREATMENT_ID
        WHERE MOD(ABS(i.CLNT_NO), 10) = %(bite)d
        GROUP BY 1
    )
    SELECT mailed_cards, mailed_fwc, mailed_loy, mnes_cards, mnes_loy,
           COUNT(*) AS clients,
           SUM(unsub_cards) AS unsub_cards, SUM(unsub_fwc) AS unsub_fwc,
           SUM(unsub_loy) AS unsub_loy,
           SUM(CASE WHEN unsub_cards = 1 OR unsub_fwc = 1 OR unsub_loy = 1
                    THEN 1 ELSE 0 END) AS unsub_any,
           SUM(emails_cards) AS emails_cards, SUM(emails_fwc) AS emails_fwc,
           SUM(emails_loy) AS emails_loy
    FROM cl GROUP BY 1, 2, 3, 4, 5
    """)
    _sql_mne = ("WITH ev AS (" + _EV + "), ids AS (" + _IDS + """), cl AS (
        SELECT i.CLNT_NO,
               MAX(CASE WHEN e.mne IN (%(cards)s) AND e.sent = 1 THEN 1 ELSE 0 END) AS mailed_cards,
               MAX(CASE WHEN e.mne = 'FWC'        AND e.sent = 1 THEN 1 ELSE 0 END) AS mailed_fwc,
               MAX(CASE WHEN e.mne IN (%(loy)s)   AND e.sent = 1 THEN 1 ELSE 0 END) AS mailed_loy
        FROM ev e
        INNER JOIN ids i
           ON i.consumer_id_hashed = e.consumer_id_hashed
          AND i.TREATMENT_ID = e.TREATMENT_ID
        WHERE MOD(ABS(i.CLNT_NO), 10) = %(bite)d
        GROUP BY 1
    )
    SELECT c2.mailed_cards, c2.mailed_fwc, c2.mailed_loy, e.mne,
           COUNT(DISTINCT CASE WHEN e.sent = 1 THEN i.CLNT_NO END) AS clients_mailed,
           COUNT(DISTINCT CASE WHEN e.unsub = 1 THEN i.CLNT_NO END) AS clients_unsub
    FROM ev e
    INNER JOIN ids i
       ON i.consumer_id_hashed = e.consumer_id_hashed
      AND i.TREATMENT_ID = e.TREATMENT_ID
    INNER JOIN cl c2 ON c2.CLNT_NO = i.CLNT_NO
    GROUP BY 1, 2, 3, 4
    """)
    if "EDW" not in globals():
        _u = input("Enter your username: ")
        _p = getpass.getpass("Enter your password: ")
        EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com",
                                  user=_u, password=_p, logmech="LDAP")
    parts, parts_mne = [], []
    for bite in range(10):
        kw = {"cards": _in(CARDS_L), "loy": _in(LOY_L), "bite": bite}
        parts.append(pd.read_sql(_sql % kw, EDW))
        parts_mne.append(pd.read_sql(_sql_mne % kw, EDW))
        print(f"bite {bite} done: {parts[-1]['clients'].sum():,.0f} clients")
    _FLAGS = ["mailed_cards", "mailed_fwc", "mailed_loy"]
    detail = (pd.concat(parts)
              .groupby(_FLAGS + ["mnes_cards", "mnes_loy"], as_index=False).sum())
    detail.to_csv(os.path.join(BASE, "pm_overlap_detail.csv"), index=False)
    mne_cube = (pd.concat(parts_mne).groupby(_FLAGS + ["mne"], as_index=False).sum())
    mne_cube.to_csv(os.path.join(BASE, "pm_overlap_mne.csv"), index=False)
    agg = detail.copy()
    agg["sum_mnes_cards"] = agg["mnes_cards"] * agg["clients"]
    agg["sum_mnes_loy"] = agg["mnes_loy"] * agg["clients"]
    ov = (agg.groupby(_FLAGS, as_index=False)
          [["clients", "unsub_cards", "unsub_fwc", "unsub_loy", "unsub_any",
            "emails_cards", "emails_fwc", "emails_loy",
            "sum_mnes_cards", "sum_mnes_loy"]].sum())
    ov.to_csv(OVERLAP_CSV, index=False)
    print(f"WROTE {OVERLAP_CSV} + detail + mne caches"); print(ov)

# %% [13] A3 chart — headline (clean attribution) + exposure
SEG_ORDER = ["Cards only (ex-FIFA)", "FIFA only", "Loyalty only",
             "Cards+FIFA", "Cards+Loyalty", "FIFA+Loyalty", "All three"]
def seg_name(c, f, l):
    return {(1, 0, 0): "Cards only (ex-FIFA)", (0, 1, 0): "FIFA only",
            (0, 0, 1): "Loyalty only", (1, 1, 0): "Cards+FIFA",
            (1, 0, 1): "Cards+Loyalty", (0, 1, 1): "FIFA+Loyalty",
            (1, 1, 1): "All three"}.get((int(c), int(f), int(l)))

if not _caches_current():
    print("SKIP: run cell [12] first (needs Teradata once).")
else:
    ov = pd.read_csv(OVERLAP_CSV)
    ov = ov[(ov[["mailed_cards", "mailed_fwc", "mailed_loy"]].sum(axis=1)) > 0].copy()
    ov["segment"] = ov.apply(lambda r: seg_name(r["mailed_cards"], r["mailed_fwc"],
                                                r["mailed_loy"]), axis=1)
    ov = ov.set_index("segment").reindex(SEG_ORDER).dropna(subset=["clients"])
    n_tot = int(ov["clients"].sum())
    KEEP = ["Cards only (ex-FIFA)", "FIFA only", "Loyalty only", "All three"]
    hidden = ov[~ov.index.isin(KEEP)]
    if len(hidden):
        print("not shown (partial combos):",
              {s_: int(v) for s_, v in hidden["clients"].items()})
    ovk = ov[ov.index.isin(KEEP)]
    segs = [s_ for s_ in KEEP if s_ in ovk.index]
    scope_style = [("unsub_cards", "mailed_cards", "emails_cards",
                    "Cards lists (ex-FIFA)", lob_colors["CARDS"]),
                   ("unsub_fwc", "mailed_fwc", "emails_fwc", "FIFA list", C_NOW),
                   ("unsub_loy", "mailed_loy", "emails_loy",
                    "Loyalty lists", lob_colors["LOYALTY"])]
    fig, (axh, axe) = plt.subplots(1, 2, figsize=(15, 6.5))
    xs = np.arange(len(segs)); wS = 0.26
    for k, (ucol, flag, ecol, nm, colr) in enumerate(scope_style):
        rates, emails, pos = [], [], []
        for si, s_ in enumerate(segs):
            if ovk.loc[s_, flag] == 1:
                pos.append(si + (k - 1) * wS)
                rates.append(float(ovk.loc[s_, ucol] / ovk.loc[s_, "clients"] * 100))
                emails.append(float(ovk.loc[s_, ecol] / ovk.loc[s_, "clients"]))
        axh.bar(pos, rates, wS, color=colr, label=nm)
        for p_, r_ in zip(pos, rates):
            axh.text(p_, r_ + 0.02, f"{r_:.2f}%", ha="center", va="bottom",
                     fontsize=9, fontweight="bold")
        axe.bar(pos, emails, wS, color=colr)
        for p_, v_ in zip(pos, emails):
            axe.text(p_, v_ + 0.05, f"{v_:.1f}", ha="center", va="bottom", fontsize=9)
    for ax, ylab, ttl in [
            (axh, "% of group's clients who unsubscribed\nfrom THAT scope's lists, Jan-Apr",
             "HEADLINE — clean attribution:\nunsubs counted only on the group's own lists"),
            (axe, "avg delivered emails per client, Jan-Apr",
             "EXPOSURE — how much mail did each group get?")]:
        ax.set_xticks(xs)
        ax.set_xticklabels([f"{s_}\nn = {int(ovk.loc[s_, 'clients']):,}" for s_ in segs],
                           fontsize=8)
        ax.set_ylabel(ylab); ax.set_title(ttl, fontweight="bold", fontsize=11)
        style_ax(ax)
    axh.set_ylim(0, axh.get_ylim()[1] * 1.15)
    axh.legend(frameon=False, fontsize=8.5, loc="upper left")
    fig.suptitle(f"A3 OVERLAP (FIFA isolated): unsub rate by mail-exposure group — Jan-Apr 2026\n"
                 f"Groups are MUTUALLY EXCLUSIVE clients (sum = {n_tot:,} of ~10.4M mailed "
                 "enterprise-wide); group = which exposures DELIVERED email in the window",
                 fontsize=11.5, fontweight="bold")
    fig.text(0.01, 0.01,
             "CLEAN ATTRIBUTION: a group's rate counts ONLY unsubs on that scope's own lists. "
             "Navy = Cards ex-FIFA, orange = FIFA, tundra = Loyalty — same colors both panels.",
             fontsize=8, style="italic")
    plt.tight_layout(rect=[0, 0.04, 1, 0.90]); plt.show()

# %% [14] A3b — WHICH programs, single-side groups (FIFA = orange bar inside Cards panel)
if not _caches_current():
    print("SKIP: run cell [12] first.")
else:
    mc = pd.read_csv(os.path.join(BASE, "pm_overlap_mne.csv"))
    mc = mc[(mc[["mailed_cards", "mailed_fwc", "mailed_loy"]].sum(axis=1)) > 0].copy()
    lm2 = (_frames["mapping"].assign(
        mne=lambda d: d[[c for c in d.columns if "MNEMONIC" in c.upper()][0]].astype(str).str.strip(),
        lob=lambda d: d[[c for c in d.columns if "LOB" in c.upper()][0]].astype(str).str.strip().str.upper())
        [["mne", "lob"]])
    mc["mne"] = mc["mne"].astype(str).str.strip()
    mc = mc.merge(lm2, on="mne", how="left")
    mc.loc[mc["mne"] == "FWC", "lob"] = "FIFA"
    mc = mc[mc["clients_mailed"] > 0].copy()
    mc["unsub_rate"] = mc["clients_unsub"] / mc["clients_mailed"] * 100
    mc["label_mne"] = np.where(mc["clients_mailed"] < SMALL_BASE,
                               mc["mne"] + " △", mc["mne"])
    mc["panel"] = np.where(mc["mailed_loy"] == 0, "Cards side only (incl FIFA)",
                  np.where((mc["mailed_cards"] == 0) & (mc["mailed_fwc"] == 0),
                           "Loyalty only", "mixed"))
    pm_ = (mc[mc["panel"] != "mixed"]
           .groupby(["panel", "label_mne", "lob"], as_index=False)
           [["clients_mailed", "clients_unsub"]].sum())
    pm_["unsub_rate"] = pm_["clients_unsub"] / pm_["clients_mailed"] * 100
    panels = ["Cards side only (incl FIFA)", "Loyalty only"]
    fig, axs = plt.subplots(1, 2, figsize=(15, 6))
    for ax, seg in zip(axs, panels):
        top = (pm_[pm_["panel"] == seg]
               .sort_values("clients_mailed", ascending=True).tail(10))
        ax.barh(top["label_mne"], top["clients_mailed"],
                color=[lob_colors.get(l, "#899299") for l in top["lob"]])
        for y_, (v, r_) in enumerate(zip(top["clients_mailed"], top["unsub_rate"])):
            ax.text(v, y_, f" {compact_n(v)} · {r_:.2f}%", va="center", fontsize=8)
        ax.set_title(seg, fontweight="bold")
        ax.set_xlim(0, top["clients_mailed"].max() * 1.45); style_ax(ax)
    fig.suptitle("A3b WHICH programs — single-side groups, Jan-Apr 2026\n"
                 "label = clients mailed · that program's unsub rate within the group | "
                 "navy = Cards, orange = FIFA, tundra = Loyalty | △ = <10K mailed",
                 fontsize=11, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.88]); plt.show()

# %% [15] A3c — TOP 10 PROGRAM COMBINATIONS (own pull, own cache)
COMBO_CSV = os.path.join(BASE, "pm_overlap_combos.csv")
if not os.path.exists(COMBO_CSV):
    import getpass
    import teradatasql
    lobs = con.execute("SELECT TRIM(MNEMONIC) AS mne, UPPER(TRIM(LOB_Manual)) AS lob FROM mapping").df()
    CARDS_L = sorted(set(lobs.loc[lobs["lob"] == "CARDS", "mne"]) - {"FWC"})
    LOY_L = sorted(set(lobs.loc[lobs["lob"] == "LOYALTY", "mne"]))
    _in = lambda ms: ", ".join(f"'{m}'" for m in ms)
    _sql_combo = """
    WITH ev AS (
        SELECT consumer_id_hashed, TREATMENT_ID,
               SUBSTR(TREATMENT_ID, 8, 3) AS mne,
               MAX(CASE WHEN disposition_cd = 1 THEN 1 ELSE 0 END) AS sent,
               MAX(CASE WHEN disposition_cd = 4 THEN 1 ELSE 0 END) AS unsub
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_dt_tm >= DATE '2026-01-01'
          AND disposition_dt_tm <  DATE '2026-05-01'
          AND disposition_cd IN (1, 4)
          AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
          AND SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
          AND SUBSTR(TREATMENT_ID, 8, 3) IN (%(cards)s, 'FWC', %(loy)s)
        GROUP BY 1, 2, 3
    ), ids AS (
        SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
        FROM DTZV01.VENDOR_FEEDBACK_MASTER
        WHERE load_tm >= DATE '2025-10-01' AND CLNT_NO IS NOT NULL
    ), per_mne AS (
        SELECT i.CLNT_NO, e.mne,
               MAX(e.sent) AS sent, MAX(e.unsub) AS unsub
        FROM ev e
        INNER JOIN ids i
           ON i.consumer_id_hashed = e.consumer_id_hashed
          AND i.TREATMENT_ID = e.TREATMENT_ID
        WHERE MOD(ABS(i.CLNT_NO), 10) = %(bite)d
        GROUP BY 1, 2
    ), cl AS (
        SELECT CLNT_NO,
               TRIM(TRAILING '+' FROM (XMLAGG(CASE WHEN sent = 1
                    THEN TRIM(mne) || '+' END ORDER BY mne) (VARCHAR(600)))) AS combo,
               MAX(unsub) AS unsub_any
        FROM per_mne GROUP BY 1
    )
    SELECT combo, COUNT(*) AS clients, SUM(unsub_any) AS unsubs
    FROM cl WHERE combo IS NOT NULL
    GROUP BY 1 HAVING COUNT(*) >= 50
    """
    if "EDW" not in globals():
        _u = input("Enter your username: ")
        _p = getpass.getpass("Enter your password: ")
        EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com",
                                  user=_u, password=_p, logmech="LDAP")
    parts_c = []
    for bite in range(10):
        parts_c.append(pd.read_sql(
            _sql_combo % {"cards": _in(CARDS_L), "loy": _in(LOY_L), "bite": bite}, EDW))
        print(f"combo bite {bite}: {len(parts_c[-1]):,} combo rows")
    combos_df = pd.concat(parts_c).groupby("combo", as_index=False).sum()
    combos_df.to_csv(COMBO_CSV, index=False)
    print(f"WROTE {COMBO_CSV} ({len(combos_df):,} combos)")
else:
    print(f"CACHED: {COMBO_CSV} exists.")

cb = pd.read_csv(COMBO_CSV)
cb["n_programs"] = cb["combo"].str.count(r"\+") + 1
cb["unsub_rate"] = cb["unsubs"] / cb["clients"] * 100
top = (cb[cb["n_programs"] >= 2]
       .sort_values("clients", ascending=False).head(10)
       .sort_values("clients", ascending=True))
fig, ax = plt.subplots(figsize=(11, 5.5))
ax.barh(top["combo"], top["clients"], color=C_THEN)
for y_, (v, r_) in enumerate(zip(top["clients"], top["unsub_rate"])):
    ax.text(v, y_, f" {compact_n(v)} clients · {r_:.2f}%", va="center",
            fontsize=8.5, fontweight="bold")
ax.set_xlim(0, top["clients"].max() * 1.45)
ax.xaxis.set_major_formatter(FuncFormatter(lambda v_, _: compact_n(v_)))
style_ax(ax)
ax.set_title("A3c TOP 10 PROGRAM COMBINATIONS (2+ programs) — clients mailed, Jan-Apr 2026\n"
             "combo = exact set of Cards/FIFA/Loyalty programs delivered · "
             "label = clients · % unsubscribed from any of these lists | <50 clients/bite excluded",
             fontweight="bold", fontsize=10.5)
plt.tight_layout(); plt.show()
