# unsub_analysis_notebook.py — reference for Andre's POD notebook.
# ALL matplotlib, one chart per block (independent figures, no multi-panel
# walls). Consistency rules: campaign color = its LOB color everywhere
# (FIFA/FWC always orange); action-type charts use colors_at; every rate
# shows n; numbers abbreviated K/M via compact_n.
# ORDER: Q0 Q1 Q2(a-c) Q3(a-f) Q4 -> OVERLAP suite -> ATTRITION ->
# PROFIT CHECK -> D gates, D1 spend, D2a profitability, D2b product count
# -> Q5 representation ratios (kept next to profitability per Andre).

# %% [0] Setup + data load
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
BASE = os.path.expanduser("~/unsub_unified_out/")
USE_HDFS = "spark" in globals()

def load_cube(fname, alts=(), **read_csv_kwargs):
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
    raise FileNotFoundError(f"none of {(fname,) + tuple(alts)} found")

SMALL_BASE = 10_000

C_THEN = "#003168"; C_NOW = "#FCA311"; C_LINE = "#B00020"; C_POS = "#AABA0A"
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

fmt_compact = FuncFormatter(lambda v_, _: compact_n(v_))

con = duckdb.connect()
print(f"Loading cubes (USE_HDFS={USE_HDFS}):")
_frames = {}
for view, fname in [("a1", "a1_mne_share.csv"), ("a1_lob", "a1_lob_dedup.csv"),
                    ("a2", "a2_mne_rates.csv"), ("a3", "a3_contact_cube.csv"),
                    ("a4", "a4_profile_cube.csv"), ("b", "b_before_after_cube.csv")]:
    _frames[view] = load_cube(fname)
    con.register(view, _frames[view])
_frames["b_delta"] = load_cube("b_delta_summary.csv")
# pandas parses the literal string "n/a" as NaN on read - restore it; the
# pipeline writes period='n/a' on n_clients rows
_frames["b_delta"]["period"] = _frames["b_delta"]["period"].fillna("n/a")
con.register("b_delta", _frames["b_delta"])
_frames["mapping"] = load_cube("mapping Mne.csv", alts=("mapping_mne.csv",))
con.register("mapping", _frames["mapping"])

_cdf = load_cube("c_monthly_curve.csv", encoding="latin-1", on_bad_lines="skip")
def _pick(cols, *needles):
    hits = [c2 for c2 in cols if any(n in c2.lower() for n in needles)]
    assert hits, f"c_monthly_curve: no column matching {needles} - STOP"
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

# ONE campaign -> ONE color everywhere: LOB color; FWC always FIFA orange
_lobcol = next((c for c in _frames["mapping"].columns
                if c.strip().upper() == "LOB_MANUAL"),
               [c for c in _frames["mapping"].columns if "LOB" in c.upper()][0])
print(f"LOB column used for colors: {_lobcol!r}")
_lobmap_df = (_frames["mapping"].assign(
    mne=lambda d: d[[c for c in d.columns if "MNEMONIC" in c.upper()][0]].astype(str).str.strip(),
    lob=lambda d: d[_lobcol].astype(str).str.strip().str.upper())
    [["mne", "lob"]])
MNE_LOB = dict(zip(_lobmap_df["mne"], _lobmap_df["lob"]))
def mne_color(mne):
    if str(mne).strip() == "FWC":
        return lob_colors["FIFA"]
    return lob_colors.get(MNE_LOB.get(str(mne).strip(), "UNKNOWN"), lob_colors["UNKNOWN"])
_unmapped = sorted(set(_cdf["mne"].str.strip()) - set(MNE_LOB))
if _unmapped:
    print(f"MNEs not in mapping file (render grey/UNKNOWN): {_unmapped}")

print("Views registered. Rows per cube:")
for v in ["a1", "a1_lob", "a2", "a3", "a4", "b", "b_delta", "c", "mapping"] + (["pm"] if HAS_PM else []):
    print(f"  {v:10s} {con.execute(f'SELECT COUNT(*) FROM {v}').fetchone()[0]:,}")
print(f"pm_asks_results.csv present: {HAS_PM}")
print(f"Deduped: enterprise {ENTERPRISE_DEDUP:,} · cards LOB {CARDS_LOB_DEDUP:,} · cards ex-FWC {CARDS_EX_FWC_DEDUP:,}")

# %% [markdown]
# # Unsub Analysis — Run 2026-08-03
# **Two Cards LOB definitions:** (a) Cards LOB (mapping file, incl
# FWC/FIFA), deduped uniques from a1_lob_dedup; (b) Cards pod (12 MNEs):
# PCQ PCL PCD AUH CLI CRV VBA VBU CEC VIF MET. One person can unsubscribe
# from several campaigns — campaign counts exceed unique people.

# %% [1] Q0 — Monthly Sends and Unsub Rate by LOB (kept as-is)
q0 = """
WITH lob_month AS (
    SELECT c.ym, COALESCE(TRIM(m.LOB_Manual), 'UNKNOWN') AS lob_manual,
           SUM(c.sends) AS sends, SUM(c.unsubs_attributed) AS unsubs
    FROM c LEFT JOIN mapping m ON TRIM(c.mne) = TRIM(m.MNEMONIC)
    WHERE c.ym BETWEEN '202508' AND '202606'
      AND c.sends IS NOT NULL AND c.unsubs_attributed IS NOT NULL
    GROUP BY 1, 2
), ranked AS (
    SELECT lob_manual FROM lob_month GROUP BY 1 ORDER BY SUM(sends) DESC LIMIT 6
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
    ax.yaxis.set_major_formatter(fmt_compact)
    ax.tick_params(axis="y", labelleft=True); style_ax(ax)
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
             "(bars = delivered emails; red line = unsubs per delivered email %)",
             fontsize=12, fontweight="bold", y=0.995)
plt.tight_layout(rect=[0, 0, 1, 0.97]); plt.show()

# %% [markdown]
# ## Q1: Volume vs Rate — Who Concentrates Unsubs? Jan to Apr 2026
# Rate ranking requires senders >= 10,000. Campaign colors = LOB colors.

# %% [2] Q1 — top 10 by volume / by rate
q1a = """
SELECT TRIM(a2.mne) AS mne, a2.senders, a2.unsubs_attributed,
       ROUND(a2.unsubs_attributed * 100.0 / NULLIF(a2.senders, 0), 2) AS unsub_rate_pct
FROM a2 WHERE a2.senders > 0 ORDER BY a2.unsubs_attributed DESC LIMIT 10
"""
q1b = f"""
SELECT TRIM(a2.mne) AS mne, a2.senders, a2.unsubs_attributed,
       ROUND(a2.unsubs_attributed * 100.0 / NULLIF(a2.senders, 0), 2) AS unsub_rate_pct
FROM a2 WHERE a2.senders >= {SMALL_BASE} ORDER BY unsub_rate_pct DESC LIMIT 10
"""
d1a, d1b = con.execute(q1a).df(), con.execute(q1b).df()
display(d1a); display(d1b)

# %% [2b] Q1 chart
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
for ax, d_, val, ttl in [(ax1, d1a.iloc[::-1], "unsubs_attributed", "Top 10 by Volume"),
                         (ax2, d1b.iloc[::-1], "unsub_rate_pct", "Top 10 by Rate % (senders >= 10K)")]:
    ax.barh(d_["mne"], d_[val], color=[mne_color(m) for m in d_["mne"]])
    for y_, (v, n_) in enumerate(zip(d_[val], d_["senders"])):
        lab = f" {compact_n(v)}" if val == "unsubs_attributed" else f" {v:.2f}%"
        ax.text(v, y_, lab + f"  (n = {compact_n(n_)})", va="center", fontsize=8)
    ax.set_title(ttl, fontweight="bold"); style_ax(ax)
    ax.set_xlim(0, d_[val].max() * 1.35)
    if val == "unsubs_attributed":
        ax.xaxis.set_major_formatter(fmt_compact)
    else:
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v_, _: f"{v_:.1f}%"))
pcq_in_both = "PCQ" in set(d1a["mne"]) and "PCQ" in set(d1b["mne"])
fig.suptitle("Q1: Who Concentrates Unsubs — Volume vs Rate, Jan to Apr 2026"
             + ("\n(PCQ is the only Cards-pod campaign in BOTH top-10s)" if pcq_in_both else ""),
             fontsize=12, fontweight="bold")
_seen = sorted({("FIFA" if m == "FWC" else MNE_LOB.get(m, "UNKNOWN"))
                for m in list(d1a["mne"]) + list(d1b["mne"])})
fig.legend(handles=[Patch(color=lob_colors[l], label=l) for l in _seen if l in lob_colors],
           loc="lower center", ncol=6, frameon=False, fontsize=8)
plt.tight_layout(rect=[0, 0.05, 1, 0.93]); plt.show()

# %% [markdown]
# ## Q2: 12-Month Unsub Curve — Aug 2025 to Jul 2026
# Independent charts (a/b/c). Red = Cards LOB per mapping file.
# 202606-202607 immature (identity-bridge lag).

# %% [3] Q2 data
cards_mnes = con.execute(
    "SELECT TRIM(MNEMONIC) AS mne FROM mapping WHERE UPPER(TRIM(LOB_Manual)) = 'CARDS'"
).df()["mne"].tolist()
print(f"Cards MNEs ({len(cards_mnes)}):", sorted(cards_mnes))
_in_cards = ", ".join(f"'{m}'" for m in cards_mnes)
curve = con.execute(f"""
SELECT c.ym,
       SUM(c.unsubs_attributed) AS enterprise_unsubs,
       SUM(CASE WHEN TRIM(c.mne) IN ({_in_cards}) THEN c.unsubs_attributed ELSE 0 END) AS cards_unsubs,
       SUM(c.sends) AS enterprise_sends,
       ROUND(SUM(CASE WHEN TRIM(c.mne) IN ({_in_cards}) THEN c.unsubs_attributed ELSE 0 END)
             * 100.0 / NULLIF(SUM(c.unsubs_attributed), 0), 1) AS cards_pct
FROM c WHERE c.ym BETWEEN '202508' AND '202607'
GROUP BY 1 ORDER BY 1""").df()
curve["immature"] = curve["ym"].isin(["202606", "202607"])
mature = curve[~curve["immature"]]
peak_pct = mature["cards_pct"].max()
peak_month = mature.loc[mature["cards_pct"].idxmax(), "ym"]
mature_avg = mature["cards_pct"].mean()
display(curve)
print(f"Peak Cards share (mature months only): {peak_pct:.1f}% in {peak_month}")
fwc_timing = con.execute("""
SELECT c.ym, SUM(c.sends) AS fwc_sends, SUM(c.unsubs_attributed) AS fwc_unsubs
FROM c WHERE TRIM(c.mne) = 'FWC' AND c.ym BETWEEN '202508' AND '202607'
GROUP BY 1 ORDER BY 1""").df()
display(fwc_timing)
print("Note: Cards deduped unique-person share (20.5%) is a different basis "
      "(unique clients, not events).")

# %% [4] Q2a — monthly unsub events, Cards share stacked
fig, ax = plt.subplots(figsize=(13, 5.5))
x = range(len(curve))
rest = curve["enterprise_unsubs"] - curve["cards_unsubs"]
ax.bar(x, curve["cards_unsubs"], color=C_LINE, label="Cards LOB (mapping file)")
ax.bar(x, rest, bottom=curve["cards_unsubs"], color=C_THEN, alpha=0.75,
       label="Rest of Enterprise")
_f0, _f1 = list(curve["ym"]).index("202602"), list(curve["ym"]).index("202604")
ax.axvspan(_f0 - 0.45, _f1 + 0.45, color=lob_colors["FIFA"], alpha=0.15)
_ytop = float(curve["enterprise_unsubs"].max())
ax.text((_f0 + _f1) / 2, _ytop * 1.02, "FIFA window", ha="center",
        fontsize=9, color="#B36B00", fontweight="bold")
_imm = list(curve["ym"]).index("202606")
ax.axvspan(_imm - 0.45, len(curve) - 0.55, color="#999999", alpha=0.15)
ax.text(_imm + 0.5, _ytop * 1.02, "immature\n(bridge lag)", ha="center",
        fontsize=8, color="#666")
ax.set_ylim(0, _ytop * 1.12)
ax.set_xticks(list(x)); ax.set_xticklabels(curve["ym"], rotation=45, fontsize=8)
ax.yaxis.set_major_formatter(fmt_compact)
ax.set_ylabel("unsub events per month")
ax.set_title("Q2a: Monthly unsub events — enterprise, Cards share stacked\n"
             "Aug 2025 to Jul 2026", fontweight="bold")
ax.legend(frameon=False); style_ax(ax)
plt.tight_layout(); plt.show()

# %% [5] Q2b — Cards share of monthly unsub events
fig, ax = plt.subplots(figsize=(13, 5.5))
x = range(len(curve))
ax.plot(x, curve["cards_pct"], color=C_LINE, marker="o", linewidth=2)
ax.fill_between(x, 0, curve["cards_pct"], color=C_LINE, alpha=0.12)
imm0 = list(curve["ym"]).index("202606")
ax.axvspan(imm0 - 0.5, len(curve) - 0.5, color="#999999", alpha=0.15)
ax.text(imm0 + 0.4, curve["cards_pct"].max() * 0.9, "immature\n(bridge lag)",
        fontsize=8, color="#666")
for xi, p_ in enumerate(curve["cards_pct"]):
    ax.annotate(f"{p_:.1f}", (xi, p_), textcoords="offset points", xytext=(0, 7),
                fontsize=8, ha="center")
ax.axhline(mature_avg, color="#999", linestyle="--", linewidth=1)
ax.text(0, mature_avg + 0.2, f"mature avg {mature_avg:.1f}%", fontsize=8)
ax.set_ylim(0, curve["cards_pct"].max() * 1.28)
ax.annotate(f"peak {peak_pct:.1f}%", (list(curve["ym"]).index(peak_month), peak_pct),
            textcoords="offset points", xytext=(0, 10), fontsize=10,
            fontweight="bold", color=C_LINE, ha="center")
ax.set_xticks(list(x)); ax.set_xticklabels(curve["ym"], rotation=45, fontsize=8)
ax.set_ylabel("Cards share of monthly unsub events (%)")
ax.set_title(f"Q2b: Cards unsub share peaked at ~{peak_pct:.0f}% in {peak_month}, "
             "coinciding with the FIFA campaign (FWC)", fontweight="bold")
style_ax(ax)
plt.tight_layout(); plt.show()

# %% [6] Q2c — FWC timing check (chart only; table in Q2 data cell)
fig, ax = plt.subplots(figsize=(13, 5.5))
xf = range(len(fwc_timing))
ax.bar(xf, fwc_timing["fwc_sends"], color=mne_color("FWC"), alpha=0.85,
       label="FWC sends (delivered emails)")
axb = ax.twinx()
axb.plot(xf, fwc_timing["fwc_unsubs"], color=C_LINE, marker="o", label="FWC unsubs")
ax.set_xticks(list(xf)); ax.set_xticklabels(fwc_timing["ym"], rotation=45, fontsize=8)
ax.yaxis.set_major_formatter(fmt_compact)
ax.set_ylabel("FWC delivered emails")
axb.set_ylabel("FWC unsub events", color=C_LINE)
axb.tick_params(axis="y", colors=C_LINE)
ax.set_title("Q2c: FWC Timing Check — unsubs followed FWC send waves with 0-1 month lag",
             fontweight="bold")
style_ax(ax); axb.spines["top"].set_visible(False)
_h1, _l1 = ax.get_legend_handles_labels(); _h2, _l2 = axb.get_legend_handles_labels()
ax.legend(_h1 + _h2, _l1 + _l2, frameon=False, fontsize=8)
plt.tight_layout(); plt.show()

# %% [markdown]
# ## Q3: Deep Dive — Cards by Action Type
# Independent charts (a-f). Action-type bars use the action-type palette;
# per-campaign charts use LOB colors (FWC = orange, always).

# %% [7] Q3 data
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
SELECT c.ym, TRIM(m.ACTION_TYPE) AS action_type, SUM(c.unsubs_attributed) AS unsubs
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

# %% [8] Q3a — volume by action type
d_ = cards_action.iloc[::-1]
fig, ax = plt.subplots(figsize=(11, 5))
ax.barh(d_["label"], d_["total_unsubs"],
        color=[lob_colors["FIFA"] if l_ == "FWC (FIFA)" else colors_at.get(a, "#899299")
               for l_, a in zip(d_["label"], d_["action_type"])])
for y_, v in enumerate(d_["total_unsubs"]):
    ax.text(v, y_, f" {compact_n(v)}", va="center", fontsize=9, fontweight="bold")
ax.set_xlim(0, d_["total_unsubs"].max() * 1.2)
ax.xaxis.set_major_formatter(fmt_compact); style_ax(ax)
ax.set_title("Q3a: Volume by Action Type — total unsubs, Jan-Apr 2026", fontweight="bold")
plt.tight_layout(); plt.show()

# %% [9] Q3b — rate by action type
fig, ax = plt.subplots(figsize=(11, 5))
ax.barh(d_["label"], d_["unsub_rate_pct"],
        color=[lob_colors["FIFA"] if l_ == "FWC (FIFA)" else colors_at.get(a, "#899299")
               for l_, a in zip(d_["label"], d_["action_type"])])
for y_, (r_, n_) in enumerate(zip(d_["unsub_rate_pct"], d_["total_senders"])):
    badge = " ⚠" if n_ < SMALL_BASE else ""
    ax.text(r_, y_, f" {r_:.2f}%  (n = {compact_n(n_)}){badge}", va="center", fontsize=9)
ax.set_xlim(0, d_["unsub_rate_pct"].max() * 1.4); style_ax(ax)
ax.set_title("Q3b: Unsub Rate by Action Type — unsubs / senders x 100, Jan-Apr 2026\n"
             f"⚠ = < {SMALL_BASE:,} senders (small base)", fontweight="bold")
plt.tight_layout(); plt.show()

# %% [10] Q3c — monthly trend by action type
pv = cards_monthly_action.pivot(index="ym", columns="action_type", values="unsubs").fillna(0)
fig, ax = plt.subplots(figsize=(13, 5.5))
for at_ in pv.columns:
    ax.plot(range(len(pv)), pv[at_], marker="o", markersize=3, label=at_,
            color=colors_at.get(at_, "#899299"))
ax.axvspan(list(pv.index).index("202602"), list(pv.index).index("202604"),
           color="#FCA311", alpha=0.10)
ax.set_xticks(range(len(pv))); ax.set_xticklabels(pv.index, rotation=45, fontsize=8)
ax.yaxis.set_major_formatter(fmt_compact)
ax.set_ylabel("unsub events per month")
ax.legend(frameon=False, fontsize=8, ncol=2)
ax.set_title("Q3c: Monthly Trend by Action Type — Feb-Apr spike concentrated in FWC",
             fontweight="bold")
style_ax(ax)
plt.tight_layout(); plt.show()

# %% [11] Q3d — MNE landscape (audience vs rate, bubble = unsub volume)
det = cards_mne_detail[cards_mne_detail["senders"] > 0]
fig, ax = plt.subplots(figsize=(13, 6))
ax.scatter(det["senders"], det["unsub_rate_pct"],
           s=(det["unsubs"] / det["unsubs"].max() * 900 + 20),
           c=[mne_color(m) for m in det["mne"]], alpha=0.8)
for _, r_ in det.iterrows():
    mark = "⚠ " if r_["senders"] < SMALL_BASE else ""
    ax.annotate(f"{mark}{r_['mne']}", (r_["senders"], r_["unsub_rate_pct"]),
                textcoords="offset points", xytext=(6, 3), fontsize=8)
ax.set_xscale("log")
ax.xaxis.set_major_formatter(fmt_compact)
ax.set_xlabel("senders — unique clients mailed (log scale)")
ax.set_ylabel("unsub rate %")
ax.set_title(f"Q3d: MNE Landscape — Audience Size vs Unsub Rate, Jan-Apr 2026\n"
             f"bubble = unsub volume | ⚠ = < {SMALL_BASE:,} senders | color = LOB (FWC = orange)",
             fontweight="bold")
style_ax(ax)
plt.tight_layout(); plt.show()

# %% [12] Q3e — audience size per Cards MNE
d_ = cards_mne_detail.sort_values("senders", ascending=True)
fig, ax = plt.subplots(figsize=(11, max(5, len(d_) * 0.32)))
ax.barh(d_["mne"], d_["senders"], color=[mne_color(m) for m in d_["mne"]])
for y_, v in enumerate(d_["senders"]):
    ax.text(v, y_, f" n = {compact_n(v)}", va="center", fontsize=8)
ax.set_xlim(0, d_["senders"].max() * 1.25)
ax.xaxis.set_major_formatter(fmt_compact); style_ax(ax)
ax.set_title("Q3e: Cards MNEs — Audience Size (unique clients mailed), Jan-Apr 2026",
             fontweight="bold")
plt.tight_layout(); plt.show()

# %% [13] Q3f — unsub rate per Cards MNE
fig, ax = plt.subplots(figsize=(11, max(5, len(d_) * 0.32)))
ax.barh(d_["mne"], d_["unsub_rate_pct"], color=[mne_color(m) for m in d_["mne"]])
for y_, (r_, n_) in enumerate(zip(d_["unsub_rate_pct"], d_["senders"])):
    badge = " ⚠" if n_ < SMALL_BASE else ""
    ax.text(r_, y_, f" {r_:.2f}%  (n = {compact_n(n_)}){badge}", va="center", fontsize=8)
ax.set_xlim(0, d_["unsub_rate_pct"].max() * 1.45); style_ax(ax)
ax.set_title(f"Q3f: Cards MNEs — Unsub Rate %, Jan-Apr 2026\n"
             f"⚠ = < {SMALL_BASE:,} senders (small base)", fontweight="bold")
plt.tight_layout(); plt.show()

# %% [markdown]
# ## Q4: Contact Frequency — Jan to Apr 2026
# Cards-email view. Survivorship caveat: selection, not treatment effect.
# KNOWN LIMITATION (Andre 2026-08-06): "1-2 emails" = emails WITHIN the
# window, not the client's first-ever contact — long-standing recipients
# can appear in the 1-2 bucket. Planned fix: extend the pipeline's a3
# cube with a pre-window lookback (e.g. emails in the prior 3 months) so
# "new to campaign" is identified properly. Pipeline lives in
# spotlight/unsub_unified.py — nothing lost. Same caveat applies to the
# OVERLAP exposure panel.

# %% [14] Q4 — distribution + rate by bucket
df4 = con.execute("""
SELECT n_emails_cards_bucket AS bucket,
       SUM(clients_total) AS clients, SUM(stayers) AS stayers,
       SUM(leavers) AS unsubs_any,
       SUM(leavers_cards_unsub_subset) AS unsubs_cards
FROM a3 GROUP BY 1
ORDER BY CASE bucket WHEN '0' THEN 0 WHEN '1-2' THEN 1 WHEN '3-5' THEN 2
                     WHEN '6-10' THEN 3 WHEN '11+' THEN 4 ELSE 9 END""").df()
display(df4)
dist = df4[df4["bucket"] != "0"].copy()
dist["pct_of_unsubs"] = dist["unsubs_cards"] / dist["unsubs_cards"].sum() * 100
dist["pct_of_stayers"] = dist["stayers"] / dist["stayers"].sum() * 100
dist["rate_pct"] = dist["unsubs_cards"] / dist["clients"] * 100

# %% [14b] Q4 chart
fig, (axl, axr) = plt.subplots(1, 2, figsize=(14, 5.5))
xb = np.arange(len(dist)); w = 0.38
axl.bar(xb - w/2, dist["pct_of_stayers"], w, color=C_THEN, label="stayers")
axl.bar(xb + w/2, dist["pct_of_unsubs"], w, color=C_NOW, label="Cards unsubs")
for xi, (s_, u_) in zip(xb, zip(dist["pct_of_stayers"], dist["pct_of_unsubs"])):
    axl.text(xi - w/2, s_, f"{s_:.0f}%", ha="center", va="bottom", fontsize=9)
    axl.text(xi + w/2, u_, f"{u_:.0f}%", ha="center", va="bottom", fontsize=9, fontweight="bold")
axl.set_xticks(xb)
axl.set_xticklabels([f"{b}\n(n = {compact_n(c_)})" for b, c_ in zip(dist["bucket"], dist["clients"])])
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
# ## Q4-LB: Contact Frequency with PRE-WINDOW LOOKBACK (the fix)
# Answers the limitation above: splits every in-window bucket by whether
# the client had ANY Cards email in Oct-Dec 2025 (pre-window). "New to
# Cards mail" 1-2 clients really are first-contact; "mailed before" 1-2
# clients are long-standing recipients who happened to get few in-window.
# Own Teradata pull (bites, cached pm_q4_lookback.csv) — pipeline untouched.

# %% [14c] Q4-LB pull + table
Q4LB_CSV = os.path.join(BASE, "pm_q4_lookback.csv")
if not os.path.exists(Q4LB_CSV):
    import getpass
    import teradatasql
    _cards_all = con.execute(
        "SELECT TRIM(MNEMONIC) AS mne FROM mapping WHERE UPPER(TRIM(LOB_Manual)) = 'CARDS'"
    ).df()["mne"].tolist()
    _inc = ", ".join(f"'{m}'" for m in _cards_all)
    _sql_lb = """
    WITH sends AS (
        SELECT consumer_id_hashed, TREATMENT_ID,
               MIN(CAST(disposition_dt_tm AS DATE)) AS send_dt
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd = 1
          AND disposition_dt_tm >= DATE '2025-10-01'
          AND disposition_dt_tm <  DATE '2026-05-01'
          AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
          AND SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
          AND SUBSTR(TREATMENT_ID, 8, 3) IN (%(cards)s)
        GROUP BY 1, 2
    ), u AS (
        SELECT DISTINCT consumer_id_hashed
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd = 4
          AND disposition_dt_tm >= DATE '2026-01-01'
          AND disposition_dt_tm <  DATE '2026-05-01'
          AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
          AND SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
          AND SUBSTR(TREATMENT_ID, 8, 3) IN (%(cards)s)
    ), ids AS (
        SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
        FROM DTZV01.VENDOR_FEEDBACK_MASTER
        WHERE load_tm >= DATE '2025-07-01' AND CLNT_NO IS NOT NULL
    ), cl AS (
        SELECT i.CLNT_NO,
               SUM(CASE WHEN s.send_dt >= DATE '2026-01-01' THEN 1 ELSE 0 END) AS in_cnt,
               SUM(CASE WHEN s.send_dt <  DATE '2026-01-01' THEN 1 ELSE 0 END) AS pre_cnt,
               MAX(CASE WHEN u.consumer_id_hashed IS NOT NULL THEN 1 ELSE 0 END) AS unsub_cards
        FROM sends s
        INNER JOIN ids i
           ON i.consumer_id_hashed = s.consumer_id_hashed
          AND i.TREATMENT_ID = s.TREATMENT_ID
        LEFT JOIN u ON u.consumer_id_hashed = s.consumer_id_hashed
        WHERE MOD(ABS(i.CLNT_NO), 10) = %(bite)d
        GROUP BY 1
    )
    SELECT CASE WHEN in_cnt = 0 THEN '0'
                WHEN in_cnt <= 2 THEN '1-2'
                WHEN in_cnt <= 5 THEN '3-5'
                WHEN in_cnt <= 10 THEN '6-10'
                ELSE '11+' END AS bucket,
           CASE WHEN pre_cnt = 0 THEN 'new to Cards mail'
                ELSE 'mailed before window' END AS prior_contact,
           COUNT(*) AS clients,
           SUM(unsub_cards) AS unsubs_cards
    FROM cl
    WHERE in_cnt > 0
    GROUP BY 1, 2
    """
    if "EDW" not in globals():
        _u = input("Enter your username: ")
        _p = getpass.getpass("Enter your password: ")
        EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com",
                                  user=_u, password=_p, logmech="LDAP")
    _parts = []
    for bite in range(10):
        _parts.append(pd.read_sql(_sql_lb % {"cards": _inc, "bite": bite}, EDW))
        print(f"lookback bite {bite}: {_parts[-1]['clients'].sum():,.0f} clients")
    lb = pd.concat(_parts).groupby(["bucket", "prior_contact"], as_index=False).sum()
    lb.to_csv(Q4LB_CSV, index=False)
    print(f"WROTE {Q4LB_CSV}")
else:
    print(f"CACHED: {Q4LB_CSV} exists.")
lb = pd.read_csv(Q4LB_CSV)
lb["rate_pct"] = lb["unsubs_cards"] / lb["clients"] * 100
display(lb.sort_values(["bucket", "prior_contact"]))

# %% [14d] Q4-LB chart — unsub rate by bucket, new vs pre-mailed
_border = ["1-2", "3-5", "6-10", "11+"]
lbp = lb[lb["bucket"].isin(_border)].copy()
fig, ax = plt.subplots(figsize=(11, 5.5))
xb = np.arange(len(_border)); w = 0.38
for off, pc_, colr in [(-w/2, "new to Cards mail", C_LINE),
                       (w/2, "mailed before window", C_THEN)]:
    d_ = (lbp[lbp["prior_contact"] == pc_].set_index("bucket")
          .reindex(_border))
    ax.bar(xb + off, d_["rate_pct"], w, color=colr, label=pc_)
    for xi, (r_, n_) in zip(xb + off, zip(d_["rate_pct"], d_["clients"])):
        if pd.notna(r_):
            ax.text(xi, r_ + 0.02, f"{r_:.2f}%\n(n = {compact_n(n_)})",
                    ha="center", va="bottom", fontsize=8, fontweight="bold")
ax.set_xticks(xb); ax.set_xticklabels(_border)
ax.set_xlabel("Cards emails received IN window (Jan-Apr)")
ax.set_ylabel("Cards unsub rate in window (%)")
ax.set_ylim(0, lbp["rate_pct"].max() * 1.35)
ax.legend(frameon=False)
ax.set_title("Q4-LB: First contact, properly defined — unsub rate by in-window bucket,\n"
             "split by pre-window history (Oct-Dec 2025 Cards mail) | Cards LOB incl FIFA",
             fontweight="bold")
style_ax(ax)
plt.tight_layout(); plt.show()

# %% [markdown]
# ## Q4 v2: Contact Frequency over the EXTENDED period (Oct 2025 - Apr 2026)
# Andre's actual ask (2026-08-06): same stayers-vs-leavers DISTRIBUTION
# comparison as Q4, but bands counted over the window PLUS a 3-month
# lookback, so "1-2" genuinely means <= 2 Cards emails across 7 months —
# no long-standing recipient can pollute the low bands.
# Population unchanged: clients mailed IN window (Jan-Apr); unsub = in-window.
# Own cache pm_q4_lookback_v2.csv at (in_cnt, pre_cnt) grain — bands are
# rebuilt locally, so re-banding needs no re-pull. (This grain can also
# reproduce Q4-LB v1 exactly; v1 cells kept untouched above.)
# CAVEAT (printed on chart): unsubscribing STOPS the email count — a Feb
# unsubscriber mechanically lands in a low band. Leavers tilt low partly
# by construction; the lookback softens but does not remove this.

# %% [14e] Q4 v2 pull + banding
Q4LB2_CSV = os.path.join(BASE, "pm_q4_lookback_v2.csv")
if not os.path.exists(Q4LB2_CSV):
    import getpass
    import teradatasql
    _cards_all = con.execute(
        "SELECT TRIM(MNEMONIC) AS mne FROM mapping WHERE UPPER(TRIM(LOB_Manual)) = 'CARDS'"
    ).df()["mne"].tolist()
    _inc = ", ".join(f"'{m}'" for m in _cards_all)
    _sql_lb2 = """
    WITH sends AS (
        SELECT consumer_id_hashed, TREATMENT_ID,
               MIN(CAST(disposition_dt_tm AS DATE)) AS send_dt
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd = 1
          AND disposition_dt_tm >= DATE '2025-10-01'
          AND disposition_dt_tm <  DATE '2026-05-01'
          AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
          AND SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
          AND SUBSTR(TREATMENT_ID, 8, 3) IN (%(cards)s)
        GROUP BY 1, 2
    ), u AS (
        SELECT DISTINCT consumer_id_hashed
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd = 4
          AND disposition_dt_tm >= DATE '2026-01-01'
          AND disposition_dt_tm <  DATE '2026-05-01'
          AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
          AND SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
          AND SUBSTR(TREATMENT_ID, 8, 3) IN (%(cards)s)
    ), ids AS (
        SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
        FROM DTZV01.VENDOR_FEEDBACK_MASTER
        WHERE load_tm >= DATE '2025-07-01' AND CLNT_NO IS NOT NULL
    ), cl AS (
        SELECT i.CLNT_NO,
               SUM(CASE WHEN s.send_dt >= DATE '2026-01-01' THEN 1 ELSE 0 END) AS in_cnt,
               SUM(CASE WHEN s.send_dt <  DATE '2026-01-01' THEN 1 ELSE 0 END) AS pre_cnt,
               MAX(CASE WHEN u.consumer_id_hashed IS NOT NULL THEN 1 ELSE 0 END) AS unsub_cards
        FROM sends s
        INNER JOIN ids i
           ON i.consumer_id_hashed = s.consumer_id_hashed
          AND i.TREATMENT_ID = s.TREATMENT_ID
        LEFT JOIN u ON u.consumer_id_hashed = s.consumer_id_hashed
        WHERE MOD(ABS(i.CLNT_NO), 10) = %(bite)d
        GROUP BY 1
    )
    SELECT in_cnt, pre_cnt,
           COUNT(*) AS clients,
           SUM(unsub_cards) AS unsubs_cards
    FROM cl
    WHERE in_cnt > 0
    GROUP BY 1, 2
    """
    if "EDW" not in globals():
        _u = input("Enter your username: ")
        _p = getpass.getpass("Enter your password: ")
        EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com",
                                  user=_u, password=_p, logmech="LDAP")
    _parts = []
    for bite in range(10):
        _parts.append(pd.read_sql(_sql_lb2 % {"cards": _inc, "bite": bite}, EDW))
        print(f"v2 lookback bite {bite}: {_parts[-1]['clients'].sum():,.0f} clients")
    lb2 = pd.concat(_parts).groupby(["in_cnt", "pre_cnt"], as_index=False).sum()
    lb2.to_csv(Q4LB2_CSV, index=False)
    print(f"WROTE {Q4LB2_CSV}")
else:
    print(f"CACHED: {Q4LB2_CSV} exists.")
lb2 = pd.read_csv(Q4LB2_CSV)
lb2["total_cnt"] = lb2["in_cnt"] + lb2["pre_cnt"]
_edges2 = [(1, 2, "1-2"), (3, 5, "3-5"), (6, 10, "6-10"), (11, 20, "11-20"), (21, None, "21+")]
lb2["band"] = pd.cut(lb2["total_cnt"], bins=[0, 2, 5, 10, 20, np.inf],
                     labels=[e[2] for e in _edges2])
q4v2 = (lb2.groupby("band", as_index=False, observed=True)
        [["clients", "unsubs_cards"]].sum())
q4v2["stayers"] = q4v2["clients"] - q4v2["unsubs_cards"]
q4v2["pct_of_stayers"] = q4v2["stayers"] / q4v2["stayers"].sum() * 100
q4v2["pct_of_unsubs"] = q4v2["unsubs_cards"] / q4v2["unsubs_cards"].sum() * 100
q4v2["rate_pct"] = q4v2["unsubs_cards"] / q4v2["clients"] * 100
print("Bands on TOTAL Cards emails Oct 2025 - Apr 2026 (population: mailed Jan-Apr):")
display(q4v2)

# %% [14f] Q4 v2 chart — distribution + rate, extended-period bands
fig, (axl, axr) = plt.subplots(1, 2, figsize=(14, 5.5))
xb = np.arange(len(q4v2)); w = 0.38
axl.bar(xb - w/2, q4v2["pct_of_stayers"], w, color=C_THEN, label="stayers")
axl.bar(xb + w/2, q4v2["pct_of_unsubs"], w, color=C_NOW, label="Cards unsubs")
for xi, (s_, u_) in zip(xb, zip(q4v2["pct_of_stayers"], q4v2["pct_of_unsubs"])):
    axl.text(xi - w/2, s_, f"{s_:.0f}%", ha="center", va="bottom", fontsize=9)
    axl.text(xi + w/2, u_, f"{u_:.0f}%", ha="center", va="bottom", fontsize=9, fontweight="bold")
axl.set_xticks(xb)
axl.set_xticklabels([f"{b}\n(n = {compact_n(c_)})" for b, c_ in zip(q4v2["band"], q4v2["clients"])])
axl.set_xlabel("TOTAL Cards emails, Oct 2025 - Apr 2026 (window + 3mo lookback)")
axl.set_ylabel("% of group")
axl.set_title("Distribution: stayers vs unsubs — honest bands\n"
              "(\"1-2\" = truly <= 2 emails across 7 months)", fontweight="bold")
axl.legend(frameon=False); style_ax(axl)
axr.bar(q4v2["band"].astype(str), q4v2["rate_pct"], color=C_THEN)
for xi, r_ in enumerate(q4v2["rate_pct"]):
    axr.text(xi, r_, f"{r_:.2f}%", ha="center", va="bottom", fontsize=9, fontweight="bold")
axr.set_ylim(0, q4v2["rate_pct"].max() * 1.25)
axr.set_ylabel("unsubscribed in window (%)")
axr.set_title("Unsub rate by extended-period band", fontweight="bold")
style_ax(axr)
fig.suptitle("Q4 v2: Contact Frequency, bands over Oct 2025 - Apr 2026 — Jan-Apr unsubs\n"
             "CAVEAT: unsubscribing stops the count — leavers tilt toward low bands "
             "partly by construction", fontsize=11, fontweight="bold")
plt.tight_layout(rect=[0, 0, 1, 0.90]); plt.show()

# %% [markdown]
# ## OVERLAP — Loyalty x Cards, FIFA isolated
# Groups (mutually exclusive, by which exposures DELIVERED in Jan-Apr):
# Cards only (ex-FIFA) · FIFA only · Loyalty only · All three (partial
# combos counted, not charted). CLEAN ATTRIBUTION: a group's unsub rate
# counts ONLY unsubs on its own lists. Caveats: volume confound,
# targeting selection, window truncation. Disposition 4 = completed
# per-list opt-out (verified 2026-08-05); client-level flags.

# %% [15] OVERLAP pull — three caches, auto-invalidates on schema change
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

# %% [16] OVERLAP headline + exposure
SEG_ORDER = ["Cards only (ex-FIFA)", "FIFA only", "Loyalty only",
             "Cards+FIFA", "Cards+Loyalty", "FIFA+Loyalty", "All three"]
def seg_name(c, f, l):
    return {(1, 0, 0): "Cards only (ex-FIFA)", (0, 1, 0): "FIFA only",
            (0, 0, 1): "Loyalty only", (1, 1, 0): "Cards+FIFA",
            (1, 0, 1): "Cards+Loyalty", (0, 1, 1): "FIFA+Loyalty",
            (1, 1, 1): "All three"}.get((int(c), int(f), int(l)))

OVERLAP_READY = _caches_current()
if not OVERLAP_READY:
    print("SKIP: run the OVERLAP pull cell first (needs Teradata once).")
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
    display(ovk.reset_index()[["segment", "clients", "unsub_cards", "unsub_fwc",
                               "unsub_loy", "unsub_any"]])

# %% [16b] OVERLAP chart (chart only)
if OVERLAP_READY:
    scope_style = [("unsub_cards", "mailed_cards", "emails_cards",
                    "Cards lists (ex-FIFA)", lob_colors["CARDS"]),
                   ("unsub_fwc", "mailed_fwc", "emails_fwc", "FIFA list",
                    lob_colors["FIFA"]),
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
            (axh, "% of the group's clients who unsubscribed\nfrom its own lists, Jan-Apr",
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
    fig.suptitle(f"OVERLAP (FIFA isolated): unsub rate by mail-exposure group — Jan-Apr 2026\n"
                 f"Groups are MUTUALLY EXCLUSIVE clients (sum = {n_tot:,} of ~10.4M mailed "
                 "enterprise-wide); group = which exposures DELIVERED email in the window",
                 fontsize=11.5, fontweight="bold")
    fig.text(0.01, 0.01,
             "A Cards-only client closing a Loyalty list is NOT counted (and vice versa). "
             "Navy = Cards ex-FIFA, orange = FIFA, tundra = Loyalty — same colors both panels.",
             fontsize=8, style="italic")
    plt.tight_layout(rect=[0, 0.04, 1, 0.90]); plt.show()

# %% [17] OVERLAP deep-dive — WHICH programs, single-side groups
if not _caches_current():
    print("SKIP: run the OVERLAP pull cell first.")
else:
    mc = pd.read_csv(os.path.join(BASE, "pm_overlap_mne.csv"))
    mc = mc[(mc[["mailed_cards", "mailed_fwc", "mailed_loy"]].sum(axis=1)) > 0].copy()
    mc["mne"] = mc["mne"].astype(str).str.strip()
    mc = mc[mc["clients_mailed"] > 0].copy()
    mc["unsub_rate"] = mc["clients_unsub"] / mc["clients_mailed"] * 100
    mc["label_mne"] = np.where(mc["clients_mailed"] < SMALL_BASE,
                               mc["mne"] + " ⚠", mc["mne"])
    mc["panel"] = np.where(mc["mailed_loy"] == 0, "Cards side only (incl FIFA)",
                  np.where((mc["mailed_cards"] == 0) & (mc["mailed_fwc"] == 0),
                           "Loyalty only", "mixed"))
    pm_ = (mc[mc["panel"] != "mixed"]
           .groupby(["panel", "label_mne", "mne"], as_index=False)
           [["clients_mailed", "clients_unsub"]].sum())
    pm_["unsub_rate"] = pm_["clients_unsub"] / pm_["clients_mailed"] * 100
    panels = ["Cards side only (incl FIFA)", "Loyalty only"]
    fig, axs = plt.subplots(1, 2, figsize=(15, 6))
    for ax, seg in zip(axs, panels):
        top = (pm_[pm_["panel"] == seg]
               .sort_values("clients_mailed", ascending=True).tail(10))
        ax.barh(top["label_mne"], top["clients_mailed"],
                color=[mne_color(m) for m in top["mne"]])
        for y_, (v, r_) in enumerate(zip(top["clients_mailed"], top["unsub_rate"])):
            ax.text(v, y_, f" {compact_n(v)} · {r_:.2f}%", va="center", fontsize=8)
        ax.set_title(seg, fontweight="bold")
        ax.set_xlim(0, top["clients_mailed"].max() * 1.45)
        ax.xaxis.set_major_formatter(fmt_compact); style_ax(ax)
    fig.suptitle("OVERLAP deep-dive — WHICH programs, single-side groups, Jan-Apr 2026\n"
                 "label = clients mailed · that program's unsub rate within the group | "
                 "campaign colors = LOB (FWC = orange) | ⚠ = <10K mailed",
                 fontsize=11, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.88]); plt.show()

# %% [18] TOP 10 PROGRAM COMBINATIONS (own pull, own cache)
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

# %% [18b] TOP 10 combinations chart
fig, ax = plt.subplots(figsize=(11, 5.5))
ax.barh(top["combo"], top["clients"], color=C_THEN)
for y_, (v, r_) in enumerate(zip(top["clients"], top["unsub_rate"])):
    ax.text(v, y_, f" {compact_n(v)} clients · {r_:.2f}%", va="center",
            fontsize=8.5, fontweight="bold")
ax.set_xlim(0, top["clients"].max() * 1.45)
ax.xaxis.set_major_formatter(fmt_compact); style_ax(ax)
ax.set_title("TOP 10 PROGRAM COMBINATIONS (2+ programs) — clients mailed, Jan-Apr 2026\n"
             "combo = exact set of Cards/FIFA/Loyalty programs delivered · "
             "label = clients · % unsubscribed from any of these lists",
             fontweight="bold", fontsize=10.5)
plt.tight_layout(); plt.show()

# %% [markdown]
# ## ATTRITION — do unsubscribers actually leave more? Yes, on every cut.
# Of clients who HAD a card in June 2025, a year later: still has cards /
# "lost cards" (no card category, still a client) / "no longer present"
# (absent from the bank's client data at June 2026 — closest available
# signal for leaving the bank; no official closure field in this data).
# LEAVER here = client who unsubscribed from Cards marketing email on or
# before Jun 30 2025. Descriptive, not causal — groups not matched
# (leavers skew younger / 4-7yr tenure).

# %% [19] ATTRITION — exit rates + where everyone ended up
if not HAS_PM:
    print("SKIP: pm_asks_results.csv not in BASE - run pm_asks_recompute.py once.")
else:
    pmw = (con.execute("SELECT * FROM pm").df()
           .pivot_table(index=["table", "grp"], columns="metric",
                        values="value", aggfunc="first"))
    led = pmw.loc["population_ledger"]
    att = pmw.loc["card_attrition"]
    grps = ["stayer", "leaver"]
    lost = [float(att.loc[g, "lost_cards_now"] / att.loc[g, "held_cards_then"] * 100) for g in grps]
    gone = [float(att.loc[g, "vanished_from_ucp_now"] / att.loc[g, "held_cards_then"] * 100) for g in grps]
    ns_a = [int(att.loc[g, "held_cards_then"]) for g in grps]
    glabel = {"stayer": "STAYERS — no Cards unsub by Jun 30 2025",
              "leaver": "LEAVERS — unsubscribed from Cards\nmarketing email by Jun 30 2025"}
    display(att); display(led)

# %% [19b] ATTRITION chart 1 — exit rates
if HAS_PM:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    xg = np.arange(2); w = 0.36
    ax.bar(xg - w/2, lost, w, color=C_THEN, label="lost cards (no card category, still a client)")
    ax.bar(xg + w/2, gone, w, color=C_LINE, label="no longer present in bank data (left-bank proxy)")
    for xi, (l_, v_) in zip(xg, zip(lost, gone)):
        ax.text(xi - w/2, l_ + 0.03, f"{l_:.2f}%", ha="center", va="bottom",
                fontsize=9.5, fontweight="bold")
        ax.text(xi + w/2, v_ + 0.03, f"{v_:.2f}%", ha="center", va="bottom",
                fontsize=9.5, fontweight="bold")
    ax.set_xticks(xg)
    ax.set_xticklabels([f"{glabel[g]}\nheld cards Jun 2025: n = {compact_n(n_)}"
                        for g, n_ in zip(grps, ns_a)], fontsize=8.5)
    ax.set_ylim(0, max(lost + gone) * 1.4)
    ax.set_ylabel("% of Jun-2025 cardholders")
    ax.set_title("ATTRITION: Of clients who HAD cards in Jun 2025, who exited by Jun 2026?\n"
                 "(descriptive — groups not matched)", fontweight="bold")
    ax.legend(frameon=False, fontsize=8.5, loc="upper left"); style_ax(ax)
    plt.tight_layout(); plt.show()

# %% [19c] ATTRITION chart 2 — where everyone ended up
if HAS_PM:
    m_now = [float(led.loc[g, "matched_now"]) for g in grps]
    g_now = [float(led.loc[g, "vanished_now"]) for g in grps]
    fig, ax = plt.subplots(figsize=(9, 5.5))
    xl = [glabel[g] for g in grps]
    ax.bar(xl, m_now, color="#899299")
    ax.bar(xl, g_now, bottom=m_now, color=C_LINE)
    for i, g in enumerate(grps):
        mt = float(led.loc[g, "matched_then"])
        ax.text(i, m_now[i] / 2, f"still present\n{compact_n(m_now[i])}", ha="center",
                va="center", fontsize=9, color="white", fontweight="bold")
        ax.text(i, m_now[i] + g_now[i],
                f"no longer present: {g_now[i]:,.0f} ({g_now[i] / mt * 100:.1f}%)",
                ha="center", va="bottom", fontsize=9, fontweight="bold", color=C_LINE)
    ax.set_ylim(0, (np.array(m_now) + np.array(g_now)).max() * 1.25)
    ax.yaxis.set_major_formatter(fmt_compact)
    ax.tick_params(axis="x", labelsize=8.5)
    ax.set_title("Where each group's Jun-2025 clients ended up by Jun 2026\n"
                 "WHOLE MAILED COHORT — includes clients with NO card (acquisition\n"
                 "audiences); this is relationship presence, NOT card attrition",
                 fontweight="bold", fontsize=10)
    style_ax(ax)
    plt.tight_layout(); plt.show()

# %% [markdown]
# ## PROFIT CHECK — profit recomputed on a fixed population
# Panel (a) "survivors only": averages run over clients still present in
# June 2026 — its n is the SURVIVOR count. Panel (b) "everyone from the
# Jun-2025 anchor": every anchored client stays in the denominator, the
# no-longer-present count as $0 — its n is the FULL anchored count.
# Result: finding survives — both groups grow, leavers slightly faster
# in % terms; basis (b) is the reported number.

# %% [20] PROFIT CHECK — two bases, per-basis denominators
if not HAS_PM:
    print("SKIP: pm_asks_results.csv not in BASE - see ATTRITION note.")
else:
    pmw = (con.execute("SELECT * FROM pm").df()
           .pivot_table(index=["table", "grp"], columns="metric",
                        values="value", aggfunc="first"))
    prof = pmw.loc["profit_three_ways"]
    led = pmw.loc["population_ledger"]
    grps = ["stayer", "leaver"]
    n_full = {g: int(prof.loc[g, "n_then_matched"]) for g in grps}
    n_surv = {g: int(led.loc[g, "matched_now"]) for g in grps}
    bases = [("(a) survivors only", "avg_then_survivors", "avg_now_survivors", n_surv),
             ("(b) everyone from the Jun-2025 anchor\n(no-longer-present counted as $0)",
              "avg_then_all", "avg_now_zerofill", n_full)]
    fig, axes = plt.subplots(1, 2, figsize=(13, 6), sharey=True)
    ymax = 0
    for ax, (label, tc, nc, nmap) in zip(axes, bases):
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
        ax.set_xticklabels([f"{g.upper()}\nn = {nmap[g]:,}" for g in grps])
        ax.set_title(label, fontweight="bold", fontsize=10.5, pad=10); style_ax(ax)
    for ax in axes:
        ax.set_ylim(0, ymax * 1.35)
    axes[0].set_ylabel("avg annual profit estimate ($, UCP)")
    _h, _l = axes[0].get_legend_handles_labels()
    fig.legend(_h, _l, loc="upper right", frameon=False, fontsize=9, ncol=2,
               bbox_to_anchor=(0.99, 0.90))
    fig.suptitle("PROFIT CHECK: Did unsubscribers' profit really grow? "
                 "Same data, two ways of counting — note each panel's own n",
                 fontsize=12, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.85]); plt.show()

# %% [markdown]
# # D: Delta Section — Cards Cohort Then vs Now (Jun 30 2025 -> Jun 30 2026)
# Cohort 4,783,193 mailed by Cards on/before the anchor. STAYERS = no
# Cards-marketing unsub by anchor; LEAVERS_ALL = did. Spend = avg monthly
# CARD spend (3-mo/3, DFP-matched). Profitability = UCP annual estimate
# (whole relationship, not validated LTV). Composition, not treatment.

# %% [21] D0 — validation gates (source-verified names, run before D charts)
b_delta = _frames["b_delta"]
piv = b_delta.pivot_table(index=["group", "metric"], columns="period",
                          values="value", aggfunc="first")
def get_val(group, metric, period):
    return piv.loc[(group, metric), period]
def get_n(group):
    return get_val(group, "n_clients", "n/a")
cohort_check = get_n("STAYERS") + get_n("LEAVERS_ALL")
assert abs(cohort_check - 4_783_193) < 1, f"FAIL: cohort anchor = {cohort_check:,.0f}"
_subgroups = [g for g in piv.index.get_level_values(0).unique()
              if g not in ("STAYERS", "LEAVERS_ALL")]
camp_sum = sum(get_n(g) for g in _subgroups)
assert abs(camp_sum - get_n("LEAVERS_ALL")) < 1, \
    f"FAIL: sub-groups sum {camp_sum:,.0f} != LEAVERS_ALL {get_n('LEAVERS_ALL'):,.0f}"
spot_delta = get_val("LEAVERS_ALL", "spend_monthly_avg", "delta")
computed = (get_val("LEAVERS_ALL", "spend_monthly_avg", "now")
            - get_val("LEAVERS_ALL", "spend_monthly_avg", "then"))
assert pd.isna(spot_delta) or abs(spot_delta - computed) < 0.01, "FAIL: delta != now - then"
SANITY_OK = True
print("SANITY GATES PASSED: cohort anchor, sub-group sum, delta arithmetic.")

# %% [22] D1 — average monthly card spend, then vs now
grps_d = ["STAYERS", "LEAVERS_ALL"]
d1 = pd.DataFrame({
    "group": grps_d,
    "n": [get_n(g) for g in grps_d],
    "spend_then": [get_val(g, "spend_monthly_avg", "then") for g in grps_d],
    "spend_now": [get_val(g, "spend_monthly_avg", "now") for g in grps_d],
})
d1["spend_delta"] = d1["spend_now"] - d1["spend_then"]
d1["delta_pct"] = d1["spend_delta"] / d1["spend_then"] * 100
display(d1)
print("Excludes LEAVERS_OTHER (not shown). DFP no-match higher for leavers.")

# %% [22b] D1 chart
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
ax.set_xticklabels([f"{g}\ncohort n = {compact_n(n_)}\n(avg over DFP-matched cardholders only)"
                    for g, n_ in zip(d1["group"], d1["n"])], fontsize=8.5)
ax.set_ylabel("avg monthly card spend ($, DFP-matched)")
ax.legend(frameon=False); style_ax(ax)
ax.set_title("D1: Average Monthly Card Spend — Then vs Now\n"
             "(Cards products only; DFP-matched clients; delta = now minus then)",
             fontweight="bold")
plt.tight_layout(); plt.show()

# %% [markdown]
# D2a (annual profitability) DROPPED per Andre 2026-08-06 — duplicate
# of PROFIT CHECK above. Median-skew check lives in git history.

# %% [24] D2b — product count (categories held)
pc = pd.DataFrame({
    "group": grps_d,
    "then": [get_val(g, "prod_cnt_avg", "then") for g in grps_d],
    "now": [get_val(g, "prod_cnt_avg", "now") for g in grps_d],
})
display(pc)

# %% [24b] D2b chart
fig, ax = plt.subplots(figsize=(9, 5.5))
for xi, r_ in pc.iterrows():
    ax.bar(xi - 0.18, r_["then"], 0.36, color=C_THEN)
    ax.bar(xi + 0.18, r_["now"], 0.36, color=C_NOW)
    ax.text(xi - 0.18, r_["then"], f"{r_['then']:.2f}", ha="center", va="bottom", fontsize=9)
    ax.text(xi + 0.18, r_["now"], f"{r_['now']:.2f}", ha="center", va="bottom", fontsize=9)
    d_ = r_["now"] - r_["then"]
    ax.text(xi, max(r_["then"], r_["now"]) * 1.1, f"{d_:+.3f}", ha="center",
            fontsize=10, fontweight="bold", color=C_POS if d_ >= 0 else C_LINE)
ax.set_ylim(0, pc[["then", "now"]].values.max() * 1.3)
ax.set_xticks(range(2))
ax.set_xticklabels([f"{g}\n(n = {compact_n(get_n(g))})" for g in grps_d])
ax.set_ylabel("avg product categories held (0-4)")
ax.legend(handles=[Patch(color=C_THEN, label="Then (Jun 2025)"),
                   Patch(color=C_NOW, label="Now (Jun 2026)")], frameon=False)
ax.set_title("D2b: Product Count (categories held) — delta = now minus then",
             fontweight="bold"); style_ax(ax)
plt.tight_layout(); plt.show()

# %% [markdown]
# ## Q5: Who Unsubscribes — Representation Ratios (next to profitability)
# REBUILT to the original notebook's method after the prior version
# produced wrong patterns. Reference values to verify against (Aug-3 run):
# age <25 and 25-34 over-represented (~1.3-1.4x); tenure PEAKS at 4-7yr;
# Credit-only most over-represented. If a panel disagrees, STOP and check
# the a4 aggregation before trusting anything on this chart.
# Representation ratio = band's share of unsubs / band's share of
# stayers. no_ucp_match excluded from panels, size reported.

# %% [25] Q5 — pulls + three independent charts
q5_age = con.execute("""
SELECT age_band AS band, SUM(clients_total) AS clients, SUM(stayers) AS stayers,
       SUM(leavers) AS unsubs_any, SUM(leavers_cards_unsub) AS unsubs_cards
FROM a4 GROUP BY 1""").df()
q5_ten = con.execute("""
SELECT tenure_band AS band, SUM(clients_total) AS clients, SUM(stayers) AS stayers,
       SUM(leavers) AS unsubs_any, SUM(leavers_cards_unsub) AS unsubs_cards
FROM a4 GROUP BY 1""").df()
q5_tibc = con.execute("""
SELECT held_t, held_i, held_b, held_c,
       SUM(clients_total) AS clients, SUM(stayers) AS stayers,
       SUM(leavers) AS unsubs_any, SUM(leavers_cards_unsub) AS unsubs_cards
FROM a4 GROUP BY 1, 2, 3, 4""").df()
print("--- BY AGE BAND (raw, incl any no-match bucket) ---"); display(q5_age)
print("--- BY TENURE BAND ---"); display(q5_ten)

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
no_ucp_n = int(q5_tibc.loc[q5_tibc["band"] == "no_ucp_match", "clients"].sum())
no_ucp_pct = no_ucp_n / q5_tibc["clients"].sum() * 100

def rep_ratio(df_, col):
    d_ = df_[~df_["band"].astype(str).str.contains("no_ucp", case=False, na=False)].copy()
    d_["ratio"] = (d_[col] / d_[col].sum()) / (d_["stayers"] / d_["stayers"].sum())
    return d_

def q5_chart(df_, order, ttl, fname_note=""):
    d_any = rep_ratio(df_, "unsubs_any")
    d_crd = rep_ratio(df_, "unsubs_cards")
    if order:
        bands = [o for o in order if o in set(d_any["band"])]
        extras = sorted(set(d_any["band"]) - set(bands))   # never drop silently
        if extras:
            print(f"extra bands appended (not in standard order): {extras}")
        bands = bands + extras
        d_any = d_any.set_index("band").reindex(bands).reset_index()
        d_crd = d_crd.set_index("band").reindex(bands).reset_index()
    else:
        # TIBC: top 12 combos by unsub volume (Andre's original LIMIT 12)
        d_any = d_any.sort_values("unsubs_any", ascending=False).head(12).iloc[::-1]
        d_crd = d_crd.set_index("band").reindex(d_any["band"]).reset_index()
    fig, ax = plt.subplots(figsize=(9, max(5.5, len(d_any) * 0.5)))
    hh = 0.35; yy = np.arange(len(d_any))
    # DIVERGING bars around 1.0 (Andre's original style): bar = ratio - 1,
    # anchored left=1, so over-represented grows right, under grows left
    ax.barh(yy + hh/2, d_any["ratio"] - 1, hh, left=1, color=C_THEN,
            alpha=0.7, label="Any RBC unsub")
    ax.barh(yy - hh/2, d_crd["ratio"] - 1, hh, left=1, color=C_NOW,
            alpha=0.9, label="Cards unsub")
    for y_, (ra, rc) in enumerate(zip(d_any["ratio"], d_crd["ratio"])):
        ax.text(ra + (0.01 if ra >= 1 else -0.01), y_ + hh/2, f"{ra:.2f}",
                va="center", ha="left" if ra >= 1 else "right", fontsize=8)
        ax.text(rc + (0.01 if rc >= 1 else -0.01), y_ - hh/2, f"{rc:.2f}",
                va="center", ha="left" if rc >= 1 else "right", fontsize=8)
    ax.axvline(1.0, color="black", linewidth=0.8)
    ax.set_yticks(yy)
    ax.set_yticklabels([f"{b} (n = {compact_n(c_)})"
                        for b, c_ in zip(d_any["band"], d_any["clients"])], fontsize=8.5)
    ax.set_xlabel("Representation ratio (1.0 = proportional)")
    ax.set_title(ttl + fname_note, fontweight="bold"); style_ax(ax)
    ax.legend(frameon=False, fontsize=8)
    plt.tight_layout(); plt.show()

AGE_ORDER = ["<25", "25-34", "35-49", "50-64", "65+"]
TEN_ORDER = ["<1yr", "1-3yr", "4-7yr", "8-15yr", "16yr+"]
print("age bands in data:", sorted(q5_age["band"].astype(str).unique()))
print("tenure bands in data:", sorted(q5_ten["band"].astype(str).unique()))
print(f"Excludes {no_ucp_n:,} clients with no UCP match ({no_ucp_pct:.1f}%).")

# %% [25b] Q5a chart — by age
q5_chart(q5_age, AGE_ORDER,
         "Q5a: Representation Ratio by Age — Jan to Apr 2026\n(>1 = over-represented among unsubs)")

# %% [25c] Q5b chart — by tenure
q5_chart(q5_ten, TEN_ORDER,
         "Q5b: Representation Ratio by Tenure — Jan to Apr 2026\n(reference: Cards unsubs peak at 4-7yr)")

# %% [25d] Q5c chart — by product mix
q5_chart(q5_tibc, None,
         "Q5c: Representation Ratio by Product Mix (TIBC) — Jan to Apr 2026\n(top 12 combos by unsub volume)")

# %% [markdown]
# ## Q5 RATE VERSIONS (candidates — originals kept above for side-by-side)
# Same cube, same bands, but plots the plain unsub RATE per band
# (unsubs / clients, %) instead of the representation ratio. Dashed lines =
# overall rate per series. Annotation = rate and rate-index vs overall
# (rate ÷ overall rate — near-identical to the rep ratio at these low rates).
# Caveat carried on chart: Cards series is denominated on ALL RBC-mailed
# clients, not Cards-mailed only.

# %% [25e] Q5 rate-version helper
def q5_rate_chart(df_, order, ttl):
    d_ = df_[~df_["band"].astype(str).str.contains("no_ucp", case=False, na=False)].copy()
    d_["rate_any"] = d_["unsubs_any"] / d_["clients"] * 100
    d_["rate_crd"] = d_["unsubs_cards"] / d_["clients"] * 100
    ov_any = d_["unsubs_any"].sum() / d_["clients"].sum() * 100
    ov_crd = d_["unsubs_cards"].sum() / d_["clients"].sum() * 100
    if order:
        bands = [o for o in order if o in set(d_["band"])]
        extras = sorted(set(d_["band"]) - set(bands))   # never drop silently
        if extras:
            print(f"extra bands appended (not in standard order): {extras}")
        d_ = d_.set_index("band").reindex(bands + extras).reset_index()
    else:
        # TIBC: same top-12-by-unsub-volume selection/order as the original chart
        d_ = d_.sort_values("unsubs_any", ascending=False).head(12).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, max(5.5, len(d_) * 0.55)))
    hh = 0.35; yy = np.arange(len(d_))
    ax.barh(yy + hh/2, d_["rate_any"], hh, color=C_THEN, alpha=0.7,
            label="Any RBC unsub rate")
    ax.barh(yy - hh/2, d_["rate_crd"], hh, color=C_NOW, alpha=0.9,
            label="Cards unsub rate")
    for y_, r_ in d_.reset_index(drop=True).iterrows():
        ax.text(r_["rate_any"] + 0.015, y_ + hh/2,
                f"{r_['rate_any']:.2f}%  ({r_['rate_any']/ov_any:.2f}x)",
                va="center", fontsize=8)
        ax.text(r_["rate_crd"] + 0.015, y_ - hh/2,
                f"{r_['rate_crd']:.2f}%  ({r_['rate_crd']/ov_crd:.2f}x)",
                va="center", fontsize=8)
    ax.axvline(ov_any, color=C_THEN, linewidth=0.9, linestyle="--")
    ax.axvline(ov_crd, color=C_NOW, linewidth=0.9, linestyle="--")
    ax.text(ov_any, len(d_) - 0.3, f" overall {ov_any:.2f}%", color=C_THEN,
            fontsize=7.5, ha="left", va="bottom")
    ax.text(ov_crd, -0.45, f" overall {ov_crd:.2f}%", color=C_NOW,
            fontsize=7.5, ha="left", va="top")
    ax.set_yticks(yy)
    ax.set_yticklabels([f"{b} (n = {compact_n(c_)})"
                        for b, c_ in zip(d_["band"], d_["clients"])], fontsize=8.5)
    ax.set_xlabel("Unsub rate, % of mailed clients (label: rate and x vs overall)")
    ax.set_xlim(0, d_["rate_any"].max() * 1.35)
    ax.set_title(ttl, fontweight="bold"); style_ax(ax)
    ax.legend(frameon=False, fontsize=8, loc="lower right")
    plt.tight_layout(); plt.show()

# %% [25f] Q5a RATE version — by age
q5_rate_chart(q5_age, AGE_ORDER,
              "Q5a v2: Unsub RATE by Age — Jan to Apr 2026\n"
              "(Cards series denominated on all RBC-mailed clients, not Cards-mailed)")

# %% [25g] Q5b RATE version — by tenure
q5_rate_chart(q5_ten, TEN_ORDER,
              "Q5b v2: Unsub RATE by Tenure — Jan to Apr 2026\n"
              "(Cards series denominated on all RBC-mailed clients, not Cards-mailed)")

# %% [25h] Q5c RATE version — by product mix
q5_rate_chart(q5_tibc, None,
              "Q5c v2: Unsub RATE by Product Mix (TIBC) — Jan to Apr 2026\n"
              "(same top-12-by-volume combos as original; Cards series on all RBC-mailed)")

# %% [markdown]
# ## Q5a v3: is it propensity, or over-contacting? (age x email volume)
# Andre's question (2026-08-06): "<25 unsubs more — but do we also MAIL each
# <25 client more?" The per-CLIENT rate (v2) can't separate the two. This
# view can:
# - LEFT: Cards unsubs per 1,000 Cards emails SENT (per-email propensity)
# - RIGHT: avg Cards emails per client (contact intensity)
# Read: per-email flat + intensity higher for young => over-contacting story;
# per-email still elevated for young => genuine propensity.
# Universe: Cards-mailed clients ONLY (n_emails_cards >= 1) — this also fixes
# v2's denominator caveat. Input built by spotlight/q5_age_volume_recompute.py
# (Lumina Spark, reads landed ucp_enriched_a3_v1 — no new Teradata pull).

# %% [25i] Q5a v3 — per-email rate + contact intensity by age
Q5V3_CSV = os.path.join(BASE, "q5_age_volume.csv")
if not os.path.exists(Q5V3_CSV):
    print(f"MISSING: {Q5V3_CSV}\nRun spotlight/q5_age_volume_recompute.py in Lumina first "
          "(reads ucp_enriched_a3_v1, writes this CSV). Skipping cell.")
else:
    v3 = pd.read_csv(Q5V3_CSV)
    display(v3.sort_values("age_band"))
    d3 = v3[~v3["age_band"].astype(str).str.contains("no_ucp", case=False, na=False)].copy()
    _no3 = v3.loc[v3["age_band"].astype(str).str.contains("no_ucp", case=False, na=False), "clients"].sum()
    print(f"Excluded from chart: {int(_no3):,} Cards-mailed clients with no UCP match.")
    bands3 = [o for o in AGE_ORDER if o in set(d3["age_band"])]
    extras3 = sorted(set(d3["age_band"]) - set(bands3))   # never drop silently
    if extras3:
        print(f"extra bands appended (not in standard order): {extras3}")
    d3 = d3.set_index("age_band").reindex(bands3 + extras3).reset_index()
    ov_pe = d3["cards_unsubs"].sum() / d3["emails_cards"].sum() * 1000
    ov_epc = d3["emails_cards"].sum() / d3["clients"].sum()
    fig, (axl, axr) = plt.subplots(1, 2, figsize=(14, 5.5))
    x3 = np.arange(len(d3))
    axl.bar(x3, d3["unsubs_per_1k_emails"], 0.6, color=C_NOW)
    for xi, r_ in enumerate(d3["unsubs_per_1k_emails"]):
        axl.text(xi, r_, f"{r_:.2f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
    axl.axhline(ov_pe, color="black", linewidth=0.9, linestyle="--")
    axl.text(len(d3) - 0.5, ov_pe, f" overall {ov_pe:.2f}", fontsize=8, va="bottom", ha="right")
    axl.set_ylabel("Cards unsubs per 1,000 Cards emails sent")
    axl.set_title("Per-EMAIL propensity\n(volume-adjusted — the honest comparison)",
                  fontweight="bold")
    axr.bar(x3, d3["emails_per_client"], 0.6, color=C_THEN)
    for xi, e_ in enumerate(d3["emails_per_client"]):
        axr.text(xi, e_, f"{e_:.1f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
    axr.axhline(ov_epc, color="black", linewidth=0.9, linestyle="--")
    axr.text(len(d3) - 0.5, ov_epc, f" overall {ov_epc:.1f}", fontsize=8, va="bottom", ha="right")
    axr.set_ylabel("avg Cards emails per client, Jan-Apr")
    axr.set_title("Contact INTENSITY\n(are we simply mailing this band more?)",
                  fontweight="bold")
    for ax_ in (axl, axr):
        ax_.set_xticks(x3)
        ax_.set_xticklabels([f"{b}\n(n = {compact_n(c_)})"
                             for b, c_ in zip(d3["age_band"], d3["clients"])], fontsize=8.5)
        style_ax(ax_)
    fig.suptitle("Q5a v3: Cards unsubs by Age — propensity vs contact volume — Jan to Apr 2026\n"
                 "Universe: Cards-mailed clients only (fixes v2 denominator caveat)",
                 fontsize=11, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.90]); plt.show()

# %% [markdown]
# ## D5: Behavior Migration — where did each then-segment end up?
# (Andre's original cell, restored.) Rows sum to 100%. Italic = <500
# clients (muted).

# %% [26] D5 — behavior migration matrices
b_cube = _frames["b"]
seg_keep = ['Revolver', 'Transactor', 'Dormant']
cube_filt = b_cube[b_cube['seg_then'].isin(seg_keep) & b_cube['seg_now'].isin(seg_keep)].copy()
excluded_stayers_n = int(b_cube[~b_cube['seg_then'].isin(seg_keep)]['stayers'].sum())
excluded_leavers_n = int(b_cube[~b_cube['seg_then'].isin(seg_keep)]['leavers'].sum())
stay_mat = cube_filt.pivot_table(index='seg_then', columns='seg_now', values='stayers', aggfunc='sum').fillna(0)
leave_mat = cube_filt.pivot_table(index='seg_then', columns='seg_now', values='leavers', aggfunc='sum').fillna(0)
stay_mat = stay_mat.reindex(index=seg_keep, columns=seg_keep, fill_value=0)
leave_mat = leave_mat.reindex(index=seg_keep, columns=seg_keep, fill_value=0)
stay_share = stay_mat.div(stay_mat.sum(axis=1).replace(0, np.nan), axis=0) * 100
leave_share = leave_mat.div(leave_mat.sum(axis=1).replace(0, np.nan), axis=0) * 100
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
vmax = max(stay_share.values.max(), leave_share.values.max())
for ax, share, cnt, ttl in [
        (axes[0], stay_share, stay_mat, f'STAYERS (n={compact_n(stay_mat.values.sum())})'),
        (axes[1], leave_share, leave_mat, f'LEAVERS_ALL (n={compact_n(leave_mat.values.sum())})')]:
    im = ax.imshow(share.values, cmap='Blues', aspect='auto', vmin=0, vmax=vmax)
    ax.set_xticks(range(3)); ax.set_xticklabels(seg_keep)
    ax.set_yticks(range(3)); ax.set_yticklabels(seg_keep)
    for i in range(3):
        for j in range(3):
            s_val = share.values[i, j]; c_val = int(cnt.values[i, j])
            muted = c_val < 500
            color = 'grey' if muted else ('white' if s_val > vmax * 0.6 else 'black')
            ax.text(j, i, f"{s_val:.1f}%\n(n={c_val:,})", ha='center', va='center',
                    fontsize=9, color=color, fontstyle='italic' if muted else 'normal')
    ax.set_title(ttl, fontweight='bold')
    ax.set_xlabel('Segment now (Jun 2026)'); ax.set_ylabel('Segment then (Jun 2025)')
fig.suptitle('D5: Behavior Migration - Where did each then-segment end up?\n'
             'Rows sum to 100%. Italic = <500 clients (muted).',
             fontweight='bold', fontsize=11)
plt.tight_layout(); plt.show()
print(f'Excludes other/no_data segments: {excluded_stayers_n:,} stayers, '
      f'{excluded_leavers_n:,} leavers in excluded seg_then categories.')

# %% [markdown]
# ## D6: Tier(then) x Segment(then) — leaver rate and count
# (Andre's original cell, restored — the requested template.)

# %% [27] D6 — tier x segment heatmap
tier_keep = ['High', 'Mid', 'Low']
maya6 = b_cube.groupby(['tier', 'seg_then'], as_index=False)[['leavers', 'stayers']].sum()
maya6['base'] = maya6['leavers'] + maya6['stayers']
maya6['leaver_rate_pct'] = np.where(maya6['base'] > 0,
                                    maya6['leavers'] * 100.0 / maya6['base'], np.nan)
maya_filt = maya6[maya6['tier'].isin(tier_keep) & maya6['seg_then'].isin(seg_keep)]
excl_tier_n = int(maya6[~maya6['tier'].isin(tier_keep)]['leavers'].sum())
excl_tier_labels = sorted(maya6[~maya6['tier'].isin(tier_keep)]['tier'].unique().tolist())
total_leavers_d6 = int(b_cube['leavers'].sum())
excl_pct = excl_tier_n * 100.0 / total_leavers_d6 if total_leavers_d6 > 0 else 0
p_rate = maya_filt.pivot(index='tier', columns='seg_then', values='leaver_rate_pct')
p_cnt = maya_filt.pivot(index='tier', columns='seg_then', values='leavers')
p_rate = p_rate.reindex(index=tier_keep, columns=seg_keep)
p_cnt = p_cnt.reindex(index=tier_keep, columns=seg_keep)
fig, ax = plt.subplots(figsize=(8, 4))
im = ax.imshow(p_rate.values, cmap='Reds', aspect='auto')
ax.set_xticks(range(len(seg_keep))); ax.set_xticklabels(seg_keep)
ax.set_yticks(range(len(tier_keep))); ax.set_yticklabels(tier_keep)
rate_max = np.nanmax(p_rate.values)
for i in range(len(tier_keep)):
    for j in range(len(seg_keep)):
        rv = p_rate.values[i, j]; lv = p_cnt.values[i, j]
        txt = 'n/a' if pd.isna(rv) else f"{rv:.2f}%\n(n={int(lv):,})"
        ax.text(j, i, txt, ha='center', va='center', fontsize=9,
                color='white' if (pd.notna(rv) and rv > rate_max * 0.55) else 'black')
plt.colorbar(im, ax=ax, label='Leaver rate %', shrink=0.8)
ax.set_title('D6 (requested template): Tier(then) x Segment(then) - leaver rate and count\n'
             'High/Mid/Low x Revolver/Transactor/Dormant only',
             fontweight='bold', fontsize=10)
ax.set_xlabel('Segment then (Jun 2025)'); ax.set_ylabel('Tier then (Jun 2025)')
plt.tight_layout(); plt.show()
print(f'Total leavers in cohort: {total_leavers_d6:,}')
print(f'Shown in D6 (High/Mid/Low tiers): {total_leavers_d6 - excl_tier_n:,} '
      f'({(total_leavers_d6 - excl_tier_n) * 100 / total_leavers_d6:.1f}%)')
print(f'Excluded (tiers {excl_tier_labels}): {excl_tier_n:,} ({excl_pct:.1f}%)')

# %% [markdown]
# # Part B2 — quarterly unsub vintages (supersedes 19-series/22-series for slide use)
# Leaver(q) = first-ever cards-marketing unsub INSIDE q's window (flow, not
# a stock at a fixed anchor). 2025Q1: unsub Jan-Mar 2025, PRE Dec-2024,
# POST +12m Mar-2026. 2025Q2: unsub Apr-Jun 2025, PRE Mar-2025, POST +12m
# Jun-2026. Stayer(q) = cards-mailed in q's window, zero cards unsub
# through q's POST month-end. Source: unsub_unified.py Part B2 (Cells
# [19]-[28]) -> v_cohort_ledger / v_then_now / v_decile / v_repeat_anomaly.
# Cells [19]-[27] above are UNTOUCHED (19-series/22-series still work off
# the old stock-at-anchor cubes) - this section is additive, not a re-point.

# %% [30] [B2 · COHORT 25Q1/Q2+12m] load
V_FILES = ["v_cohort_ledger.csv", "v_then_now.csv", "v_decile.csv", "v_repeat_anomaly.csv"]
HAS_V = all(os.path.exists(os.path.join(BASE, f)) for f in V_FILES)
if not HAS_V:
    print(f"SKIP Part B2: not all of {V_FILES} present in {BASE} - run unsub_unified.py's Part B2 first.")
else:
    v_ledger = load_cube("v_cohort_ledger.csv")
    v_then_now = load_cube("v_then_now.csv")
    v_decile = load_cube("v_decile.csv")
    v_repeat = load_cube("v_repeat_anomaly.csv")
    display(v_ledger); display(v_then_now); display(v_decile); display(v_repeat)

# %% [markdown]
# ## B2 LEDGER — of clients who had cards PRE, where are they +12m POST?
# Three mutually exclusive states (still has cards / lost cards, still a
# client / no longer in bank data). n_offbook_post_of_card_pre is an
# ADDITIVE column beyond the brief's literal cube spec — needed to build
# this exact 3-way split of the card-at-PRE base (see pipeline Cell [25]
# comment).

# %% [31] [B2 · COHORT 25Q1/Q2+12m] LEDGER table — card-at-PRE 3-way split
if HAS_V:
    _qs_v = ["2025Q1", "2025Q2"]
    _grps_v = ["STAYERS", "LEAVERS"]
    _led_idx = v_ledger.set_index(["cohort_q", "group_tag"])
    _led_rows = []
    for _q in _qs_v:
        for _g in _grps_v:
            if (_q, _g) not in _led_idx.index:
                print(f"WARNING: v_ledger missing ({_q}, {_g}) - skipping this cohort/group combo.")
                continue
            _r = _led_idx.loc[(_q, _g)]
            _base = float(_r["n_card_pre"])
            _lost = float(_r["n_lostcard_still_client_post"])
            _gone = float(_r["n_offbook_post_of_card_pre"])
            _still = _base - _lost - _gone
            _led_rows.append({
                "cohort_q": _q, "group_tag": _g, "n_card_pre": _base,
                "still_has_cards": _still, "lost_cards_still_client": _lost, "left_bank_proxy": _gone,
                "pct_still": _still / _base * 100 if _base else np.nan,
                "pct_lost": _lost / _base * 100 if _base else np.nan,
                "pct_gone": _gone / _base * 100 if _base else np.nan,
            })
    led_pivot = pd.DataFrame(_led_rows)
    display(led_pivot)

# %% [31b] LEDGER chart — 100%-stacked, Q1/Q2 x STAYERS/LEAVERS (4 bars)
if HAS_V:
    _labels_v = [f"{r['cohort_q']}\n{r['group_tag']}\nn(card@PRE)={compact_n(r['n_card_pre'])}"
                 for r in led_pivot.to_dict("records")]
    _x = np.arange(len(led_pivot))
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.bar(_x, led_pivot["pct_still"], color="#899299", label="still has cards")
    ax.bar(_x, led_pivot["pct_lost"], bottom=led_pivot["pct_still"], color=C_THEN,
           label="lost cards, still a client")
    _bot2 = led_pivot["pct_still"] + led_pivot["pct_lost"]
    ax.bar(_x, led_pivot["pct_gone"], bottom=_bot2, color=C_LINE,
           label="no longer in bank data (left-bank proxy)")
    for _xi, _r in zip(_x, led_pivot.to_dict("records")):
        ax.text(_xi, _r["pct_still"] / 2, f"{_r['pct_still']:.1f}%", ha="center", va="center",
                fontsize=9, color="white", fontweight="bold")
        ax.text(_xi, _r["pct_still"] + _r["pct_lost"] / 2, f"{_r['pct_lost']:.1f}%", ha="center",
                va="center", fontsize=9, color="white", fontweight="bold")
        ax.text(_xi, _r["pct_still"] + _r["pct_lost"] + _r["pct_gone"] / 2, f"{_r['pct_gone']:.1f}%",
                ha="center", va="center", fontsize=9, color="white", fontweight="bold")
    ax.set_xticks(_x); ax.set_xticklabels(_labels_v, fontsize=8.5)
    ax.set_ylim(0, 100); ax.set_ylabel("% of card-at-PRE clients")
    ax.legend(frameon=False, fontsize=8.5, loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=3)
    ax.set_title("B2 LEDGER: of clients who had cards PRE, where are they +12m POST?\n"
                 "(left-bank = no UCP match — proxy)", fontweight="bold")
    style_ax(ax)
    plt.tight_layout(); plt.show()

# %% [markdown]
# ## B2 DECILE — PRE-spend distribution, leavers vs stayers
# Decile 1 = lowest PRE spend; decile 0 = no PRE spend match (DFP no-row),
# shown separately at the left. Flat 10% line = what an even split would
# look like.

# %% [32] [B2 · COHORT 25Q1/Q2+12m] DECILE table
if HAS_V:
    _dec = v_decile.copy()
    _dec["cohort_q"] = _dec["cohort_q"].astype(str)
    _dec["pct"] = _dec["n"] / _dec.groupby(["cohort_q", "group_tag"])["n"].transform("sum") * 100
    dec_pivot = (_dec.pivot_table(index=["cohort_q", "spend_decile_pre"], columns="group_tag",
                                  values="pct").reset_index())
    display(dec_pivot)

# %% [32b] DECILE chart — leavers vs stayers paired bars, Q1/Q2 subplots
if HAS_V:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5), sharey=True)
    for ax, _q in zip(axes, ["2025Q1", "2025Q2"]):
        _sub = _dec[_dec["cohort_q"] == _q].sort_values("spend_decile_pre")
        _deciles = sorted(_sub["spend_decile_pre"].unique())
        _xg = np.arange(len(_deciles)); _w = 0.38
        _stay_pct = [_sub[(_sub["spend_decile_pre"] == d) & (_sub["group_tag"] == "STAYERS")]["pct"].sum()
                     for d in _deciles]
        _leave_pct = [_sub[(_sub["spend_decile_pre"] == d) & (_sub["group_tag"] == "LEAVERS")]["pct"].sum()
                      for d in _deciles]
        ax.bar(_xg - _w / 2, _stay_pct, _w, color=C_THEN, label="STAYERS")
        ax.bar(_xg + _w / 2, _leave_pct, _w, color=C_LINE, label="LEAVERS")
        ax.axhline(10, color="grey", linestyle="--", linewidth=1, label="10% flat reference")
        ax.set_xticks(_xg)
        ax.set_xticklabels(["0\n(no PRE spend)"] + [str(int(d)) for d in _deciles[1:]], fontsize=8.5)
        ax.set_title(_q, fontweight="bold"); ax.set_xlabel("PRE-spend decile (1 = lowest)")
        style_ax(ax)
    axes[0].set_ylabel("% of group"); axes[0].legend(frameon=False, fontsize=8.5)
    fig.suptitle("B2 DECILE: PRE-spend distribution, leavers vs stayers", fontweight="bold")
    plt.tight_layout(); plt.show()

# %% [markdown]
# ## B2 THEN/NOW — avg spend and profit, PRE vs POST (matched-both basis)
# matched_both = clients with a non-null value at BOTH periods for that
# metric — the primary, conservative read (all_pre is in the CSV for
# reference but not charted here).

# %% [33] [B2 · COHORT 25Q1/Q2+12m] THEN/NOW table
if HAS_V:
    _tn = v_then_now[v_then_now["basis"] == "matched_both"].copy()
    _tn["avg_pre"] = _tn["sum_pre"] / _tn["n"]
    _tn["avg_post"] = _tn["sum_post"] / _tn["n"]
    _tn["delta_pct"] = np.where((_tn["n"] != 0) & (_tn["avg_pre"] != 0),
                                 (_tn["avg_post"] - _tn["avg_pre"]) / _tn["avg_pre"] * 100, np.nan)
    tn_show = (_tn[_tn["metric"].isin(["spend_3mo", "prof_annual"])]
               [["cohort_q", "group_tag", "metric", "n", "avg_pre", "avg_post", "delta_pct"]]
               .sort_values(["metric", "cohort_q", "group_tag"]))
    display(tn_show)

# %% [33b] THEN/NOW chart — delta % bars, spend + profit subplots
if HAS_V:
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.5))
    for ax, _metric, _ttl in zip(axes, ["spend_3mo", "prof_annual"],
                                  ["avg spend (3mo, matched-both)", "avg annual profit (UCP, matched-both)"]):
        _sub = tn_show[tn_show["metric"] == _metric]
        _labels_tn = [f"{r.cohort_q}\n{r.group_tag}" for r in _sub.itertuples()]
        _xg = np.arange(len(_sub))
        _colors = [C_POS if v >= 0 else C_LINE for v in _sub["delta_pct"]]
        ax.bar(_xg, _sub["delta_pct"], color=_colors)
        for _xi, _v in zip(_xg, _sub["delta_pct"]):
            ax.text(_xi, _v, f"{_v:+.1f}%", ha="center", va="bottom" if _v >= 0 else "top",
                    fontsize=9, fontweight="bold")
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_xticks(_xg); ax.set_xticklabels(_labels_tn, fontsize=8.5)
        ax.set_title(_ttl, fontweight="bold", fontsize=10.5)
        style_ax(ax)
    axes[0].set_ylabel("% change, PRE -> POST")
    fig.suptitle("B2 THEN/NOW: % change PRE -> POST, matched-both basis", fontweight="bold")
    plt.tight_layout(); plt.show()

# %% [markdown]
# ## B2 REPEAT ANOMALY — leavers with more than one cards-mkt unsub
# Repeat unsub = the client unsubscribed, was re-subscribed to a cards-mkt
# list at some point, then unsubscribed again — median_gap_days_1to2 is
# the gap between their 1st and 2nd unsub for bands '2' and '3+'. Feeds
# `evidence_repeat_unsub.sql`.

# %% [34] [B2 · COHORT 25Q1/Q2+12m] REPEAT ANOMALY — table only, no chart
if HAS_V:
    display(v_repeat)
