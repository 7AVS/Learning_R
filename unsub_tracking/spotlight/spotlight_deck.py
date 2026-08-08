# spotlight_deck.py — Power Pack Q3 spotlight: the 2-slide exhibits ONLY.
# Story: unsub_tracking/spotlight/story_2slides_2026-08-07.md
# Numbers trace: results_2026-08-07_full_notebook_run.md (run of commit 6d6a2a2).
# Exploration lives in unsub_analysis_notebook.py — this notebook builds the
# PURPOSE-BUILT deck exhibits: Exhibit 1 (Cards unsub rate, monthly) and
# Exhibit 2 (re-contact-after-gap + action-type), plus a text-only value block.
# CUT 2026-08-07 (approved, stakeholder-signed-off): 3 exhibits / 5 findings ->
# 2 exhibits / 3 findings. See story doc's "CUT 2026-08-07" section for what and why.
# Expected magnitudes are asserted (±15%) against the 2026-08-07 run so a silent
# data change cannot ship a different story.

# %% [0] Setup — cubes + caches (subset of the analysis notebook's loader)
import os
import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

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

C_THEN = "#003168"; C_LINE = "#B00020"; C_MUTE = "#9AA7B4"
lob_colors = {"FIFA": "#FCA311"}  # only key still read (Exhibit 2 PCQ reference line)

def style_ax(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

con = duckdb.connect()
print(f"Loading cubes (USE_HDFS={USE_HDFS}):")
_frames = {}
for view, fname in [("a2", "a2_mne_rates.csv")]:
    _frames[view] = load_cube(fname)
    con.register(view, _frames[view])
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
# bare .astype(str) on a float64 column (e.g. from a null row) stringifies as "202508.0",
# which silently breaks the string BETWEEN upper bound and drops the last month.
_cdf["ym"] = pd.to_numeric(_cdf["ym"], errors="coerce")
_cdf = _cdf.dropna(subset=["ym"])
_cdf["ym"] = _cdf["ym"].astype(int).astype(str)
_cdf["sends"] = pd.to_numeric(_cdf["sends"], errors="coerce")
_cdf["unsubs_attributed"] = pd.to_numeric(_cdf["unsubs_attributed"], errors="coerce")
con.register("c", _cdf)

PM_CSV = os.path.join(BASE, "pm_asks_results.csv")
HAS_PM = os.path.exists(PM_CSV)
if HAS_PM:
    con.execute(f"CREATE OR REPLACE VIEW pm AS SELECT * FROM read_csv_auto('{PM_CSV}')")
print(f"pm_asks_results.csv present: {HAS_PM}")

def need_cache(fname):
    p = os.path.join(BASE, fname)
    if not os.path.exists(p):
        print(f"MISSING cache {p} — run the matching pull in unsub_analysis_notebook.py first.")
        return None
    return pd.read_csv(p)

def expect(name, got, ref, tol=0.15):
    ok = abs(got - ref) <= abs(ref) * tol
    print(f"  {'OK ' if ok else 'DRIFT'} {name}: got {got:.3g}, expected ~{ref:.3g} (2026-08-07 run)")
    if not ok:
        print(f"  ** {name} moved >15% vs the run the story was written on — re-check the story. **")
    return ok

# %% [markdown]
# ## SLIDE 1 — Exhibit 1: Cards unsub rate, monthly
# Single panel: Cards unsub per delivered email, by month. Mature months vs
# immature trailing months (bridge lag) shaded. Comparator (CARDS vs
# ENTERPRISE all-LOB vs ENTERPRISE ex-LOYALTY) computed over the SAME mature
# window as the chart — one window on this slide, not two.

# %% [1] Exhibit 1 — data
MATURE_YM_MAX = "202605"   # unchanged immaturity rule from the prior build: the last 2 pulled
                            # months (202606, 202607) are bridge-lag immature.
PULL_YM_MAX = "202607"     # last month present in data / chart's upper pull bound.
                            # Changing MATURE_YM_MAX and/or PULL_YM_MAX is sufficient — no other edit needed.
cards_mnes = con.execute(
    "SELECT TRIM(MNEMONIC) AS mne FROM mapping WHERE UPPER(TRIM(LOB_Manual)) = 'CARDS'"
).df()["mne"].tolist()
_in_cards = ", ".join(f"'{m}'" for m in cards_mnes)

curve = con.execute(f"""
SELECT c.ym,
       SUM(CASE WHEN TRIM(c.mne) IN ({_in_cards}) THEN c.unsubs_attributed ELSE 0 END) AS cards_unsubs,
       SUM(CASE WHEN TRIM(c.mne) IN ({_in_cards}) THEN c.sends ELSE 0 END) AS cards_sends,
       ROUND(SUM(CASE WHEN TRIM(c.mne) IN ({_in_cards}) THEN c.unsubs_attributed ELSE 0 END)
             * 100.0 / NULLIF(SUM(CASE WHEN TRIM(c.mne) IN ({_in_cards}) THEN c.sends ELSE 0 END), 0), 3)
             AS cards_rate_pct
FROM c WHERE c.ym BETWEEN '202508' AND '{PULL_YM_MAX}'
GROUP BY 1 ORDER BY 1""").df()
curve["immature"] = curve["ym"] > MATURE_YM_MAX
mature = curve.loc[~curve["immature"]]

WINDOW = f"202508–{MATURE_YM_MAX}"   # the ONE window on this slide

comp = con.execute(f"""
SELECT UPPER(TRIM(m.LOB_Manual)) AS lob,
       SUM(c.unsubs_attributed) AS unsubs, SUM(c.sends) AS sends
FROM c JOIN mapping m ON TRIM(c.mne) = TRIM(m.MNEMONIC)
WHERE c.ym BETWEEN '202508' AND '{MATURE_YM_MAX}'
GROUP BY 1""").df()

_cards = comp.loc[comp["lob"] == "CARDS"]
_loy = comp.loc[comp["lob"] == "LOYALTY"]
_ex_loy = comp.loc[comp["lob"] != "LOYALTY"]
cards_rate = float(_cards["unsubs"].sum() * 100.0 / _cards["sends"].sum())
loyalty_rate = float(_loy["unsubs"].sum() * 100.0 / _loy["sends"].sum())
loyalty_share = float(_loy["sends"].sum() * 100.0 / comp["sends"].sum())
ent_all_rate = float(comp["unsubs"].sum() * 100.0 / comp["sends"].sum())
ent_ex_loy_rate = float(_ex_loy["unsubs"].sum() * 100.0 / _ex_loy["sends"].sum())

comp_table = pd.DataFrame([
    {"row": "CARDS", "per_send_pct": cards_rate},
    {"row": "ENTERPRISE, all LOBs", "per_send_pct": ent_all_rate},
    {"row": "ENTERPRISE, excluding LOYALTY", "per_send_pct": ent_ex_loy_rate},
])
print(f"Comparator, unsub per delivered email (%), CARDS window {WINDOW}, computed from raw counts:")
print(comp_table.to_string(index=False))
print(f"LOYALTY per-send %: {loyalty_rate:.2f}  |  LOYALTY share of {WINDOW} sends: {loyalty_share:.0f}%"
      "  (the reason the all-LOB average sits above CARDS/ex-LOYALTY)")

expect("CARDS per-send %", cards_rate, 0.18)
expect("LOYALTY per-send %", loyalty_rate, 0.37)
expect("ENTERPRISE ex-LOYALTY per-send %", ent_ex_loy_rate, 0.18)
# 0.18 is a hand estimate back-computed from 2-dp LOB rates; if this fires, trust the code and report the computed value.
expect("ENTERPRISE all-LOB per-send %", ent_all_rate, 0.27)
# 0.27 is a hand estimate back-computed from 2-dp LOB rates; if this fires, trust the code and report the computed value.

_peak_idx = mature["cards_rate_pct"].idxmax()
_peak_val = float(mature.loc[_peak_idx, "cards_rate_pct"])
_peak_ym = mature.loc[_peak_idx, "ym"]
print(f"NEW (no baseline — record after this run): peak CARDS monthly unsub-per-send rate "
      f"{_peak_val:.3f}% in {_peak_ym}. Not comparable to the old 17.6 - that was Cards' share "
      f"of enterprise unsub EVENTS (a different metric), now dropped from the deck.")

# %% [2] Exhibit 1 — chart
fig, ax = plt.subplots(figsize=(13, 6))
xm = np.arange(len(curve))
ax.plot(xm, curve["cards_rate_pct"], color=C_LINE, marker="o", linewidth=2, zorder=5)
for xi, (v_, imm) in enumerate(zip(curve["cards_rate_pct"], curve["immature"])):
    ax.annotate(f"{v_:.2f}", (xi, v_), textcoords="offset points", xytext=(0, 7),
                ha="center", fontsize=8, color=C_MUTE if imm else "black",
                fontweight="normal" if imm else "bold", zorder=6)
ax.set_ylim(0, curve["cards_rate_pct"].max() * 1.35)
_immx = [i for i, f_ in enumerate(curve["immature"]) if f_]
if _immx:
    ax.axvspan(min(_immx) - 0.5, max(_immx) + 0.5, color="grey", alpha=0.15)
    ax.text(np.mean(_immx), ax.get_ylim()[1] * 0.97, "immature (bridge lag)",
            ha="center", va="top", fontsize=8, color="dimgrey")
_fifa_row = curve.loc[curve["ym"] == "202604"]
if len(_fifa_row):
    _fx = int(_fifa_row.index[0]); _fy = float(_fifa_row["cards_rate_pct"].iloc[0])
    ax.annotate("FIFA wave (Apr 2026)", (_fx, _fy), textcoords="offset points",
                xytext=(-10, 18), ha="center", fontsize=9, fontweight="bold", color=C_LINE, zorder=6)
ax.set_xticks(xm); ax.set_xticklabels(curve["ym"], rotation=45, fontsize=8)
ax.set_ylabel(f"unsub per delivered email (%), CARDS — {WINDOW}")
style_ax(ax)
ax.set_title(f"EXHIBIT 1 — Cards unsub rate: {cards_rate:.2f}% per delivered email, "
             f"{WINDOW} (mature months)", fontweight="bold", loc="left")

_lines = [
    f"CARDS: {cards_rate:.2f}% per send",
    f"ENTERPRISE (all LOBs): {ent_all_rate:.2f}% per send",
    f"ENTERPRISE (ex-LOYALTY): {ent_ex_loy_rate:.2f}% per send  —  LOYALTY {loyalty_rate:.2f}% "
    f"per send, {loyalty_share:.0f}% of sends",
]
for i_, ln in enumerate(_lines):
    ax.text(0.99, 0.95 - i_ * 0.07, ln, transform=ax.transAxes, ha="right", va="top",
            fontsize=9, color="dimgrey")

plt.tight_layout(); plt.show()

# %% [markdown]
# ## SLIDE 2 — Exhibit 2: where unsub risk concentrates
# Two panels, paired bars, each with its own y-axis, unit label, and basis line.
# No ratio axis, no shared scale — the multiple is a small text annotation inside
# each panel. (A) re-contacted after a gap vs true first-contact. (B) acquisition-
# type (Attract) vs deepen-type (Deepen) actions, PCQ marked as reference.

# %% [3] Exhibit 2 — data
lb1 = need_cache("pm_q4_lookback.csv")
if lb1 is not None:
    g = lb1.set_index(["bucket", "prior_contact"])
    assert ("1-2", "mailed before window") in g.index and ("1-2", "new to Cards mail") in g.index, (
        "pm_q4_lookback.csv missing expected (bucket, prior_contact) rows - check for the two "
        "expected prior_contact values 'mailed before window' / 'new to Cards mail' in that file."
    )
    rb = g.loc[("1-2", "mailed before window"), "unsubs_cards"] / g.loc[("1-2", "mailed before window"), "clients"] * 100
    rn = g.loc[("1-2", "new to Cards mail"), "unsubs_cards"] / g.loc[("1-2", "new to Cards mail"), "clients"] * 100
    print(f"re-contact: after-gap {rb:.2f}% vs true first-contact {rn:.2f}%")
    expect("re-contacted after a gap %", rb, 1.27)
    expect("true first-contact %", rn, 0.33)
else:
    rb = rn = None
    print("SKIP panel A — pm_q4_lookback.csv missing.")

act = con.execute("""
SELECT TRIM(m.ACTION_TYPE) AS action_type,
       SUM(a2.senders) AS senders, SUM(a2.unsubs_attributed) AS unsubs
FROM a2 JOIN mapping m ON TRIM(a2.mne) = TRIM(m.MNEMONIC)
WHERE UPPER(TRIM(m.LOB_Manual)) = 'CARDS'
GROUP BY 1""").df().set_index("action_type")
assert "Attract" in act.index and "Deepen" in act.index, (
    "act is missing 'Attract' and/or 'Deepen' - check ACTION_TYPE values in the mapping file."
)
ratt = act.loc["Attract", "unsubs"] / act.loc["Attract", "senders"] * 100
rdee = act.loc["Deepen", "unsubs"] / act.loc["Deepen", "senders"] * 100
print(f"audience: Attract {ratt:.2f}% vs Deepen {rdee:.2f}%")
expect("Attract (acquisition) %", ratt, 0.56)
expect("Deepen %", rdee, 0.28)

pcq = con.execute("SELECT a2.senders, a2.unsubs_attributed FROM a2 WHERE TRIM(a2.mne) = 'PCQ'").df()
assert len(pcq) == 1, (
    f"expected exactly one PCQ row in a2_mne_rates.csv, got {len(pcq)} - check for mnemonic "
    "casing/whitespace/drift in a2_mne_rates.csv."
)
pcq_rate = float(pcq["unsubs_attributed"].iloc[0] * 100.0 / pcq["senders"].iloc[0])
expect("PCQ %", pcq_rate, 0.58)

# a2_mne_rates.csv window: resolved from unsub_unified.py provenance — a2_mne_rates is _stamp()-ed
# "WIN_A Jan-Apr 2026" (WIN_A_FLOOR=2026-01-01, WIN_A_CEIL=2026-05-01 half-open). Not read from the
# CSV itself (the stamp columns are on the Spark cube, not re-exported into a2_mne_rates.csv), so
# this is a code-verified constant, not a runtime read.
A2_WINDOW = "WIN_A Jan-Apr 2026"
print(f"a2_mne_rates.csv window: {A2_WINDOW} (source: unsub_unified.py _stamp() call on a2_mne_rates)")

RECONTACT_BASIS = ("% of clients, 1-2 in-window (delivered Cards emails Oct 2025 - Apr 2026; "
                    "unsub window Jan-Apr 2026)")
ACTION_BASIS = f"% of senders, {A2_WINDOW}"

# %% [4] Exhibit 2 — chart
fig, (axA, axB) = plt.subplots(1, 2, figsize=(13, 5.5))

if rb is not None:
    axA.bar(["re-contacted\nafter a gap", "true first\ncontact"], [rb, rn],
            color=[C_LINE, C_THEN], width=0.5)
    for xi, v_ in enumerate([rb, rn]):
        axA.text(xi, v_, f"{v_:.2f}%", ha="center", va="bottom", fontsize=11, fontweight="bold")
    axA.text(0.5, 0.92, f"x{rb / rn:.1f}", transform=axA.transAxes, ha="center", fontsize=13,
              fontweight="bold", color=C_LINE)
axA.set_ylabel("% of clients who unsubscribed")
axA.set_title("Re-contact after a gap", fontweight="bold", loc="left")
style_ax(axA)
axA.text(0.5, -0.24, RECONTACT_BASIS, transform=axA.transAxes, ha="center", va="top",
         fontsize=7.5, color="dimgrey", wrap=True)

axB.bar(["acquisition-type\nactions (Attract)", "deepen-type\nactions (Deepen)"], [ratt, rdee],
        color=[C_LINE, C_THEN], width=0.5)
for xi, v_ in enumerate([ratt, rdee]):
    axB.text(xi, v_, f"{v_:.2f}%", ha="center", va="bottom", fontsize=11, fontweight="bold")
axB.axhline(pcq_rate, color=lob_colors["FIFA"], linewidth=1.2, linestyle="--")
axB.text(1.0, pcq_rate, f" PCQ {pcq_rate:.2f}%", va="bottom", ha="right", fontsize=9,
          fontweight="bold", color=lob_colors["FIFA"])
axB.text(0.5, 0.92, f"x{ratt / rdee:.1f}", transform=axB.transAxes, ha="center", fontsize=13,
          fontweight="bold", color=C_LINE)
axB.set_ylabel("% of recipients who unsubscribed")
axB.set_title("Action type", fontweight="bold", loc="left")
style_ax(axB)
axB.text(0.5, -0.24, ACTION_BASIS, transform=axB.transAxes, ha="center", va="top",
         fontsize=7.5, color="dimgrey", wrap=True)

fig.suptitle("EXHIBIT 2 — where unsub risk concentrates: two comparisons, each within its own basis",
             fontsize=12, fontweight="bold")
plt.tight_layout(rect=[0, 0.08, 1, 0.90]); plt.show()

# %% [markdown]
# ## SLIDE 2 — Value (text only, no chart)
# Two guarded sentences: leavers' profit still grows; leavers' card spend turns
# negative while stayers' stays positive. Descriptive — groups not matched.

# %% [5] Value — text only
if not HAS_PM:
    print("SKIP: pm_asks_results.csv missing - run pm_asks_recompute.py once.")
else:
    pmw = (con.execute("SELECT * FROM pm").df()
           .pivot_table(index=["table", "grp"], columns="metric",
                        values="value", aggfunc="first"))
    prof = pmw.loc["profit_three_ways"]
    then_stayer = float(prof.loc["stayer", "avg_then_all"])
    now_stayer = float(prof.loc["stayer", "avg_now_zerofill"])
    then_leaver = float(prof.loc["leaver", "avg_then_all"])
    now_leaver = float(prof.loc["leaver", "avg_now_zerofill"])
    pct_stayer = (now_stayer / then_stayer - 1) * 100
    pct_leaver = (now_leaver / then_leaver - 1) * 100

    bd = load_cube("b_delta_summary.csv")
    bd["period"] = bd["period"].fillna("n/a")
    _mcol = _pick(bd.columns, "metric"); _pcol = _pick(bd.columns, "period")
    _gcol = _pick(bd.columns, "group", "grp"); _vcol = _pick(bd.columns, "value")
    sp = bd[bd[_mcol] == "spend_monthly_avg"].pivot_table(
        index=_gcol, columns=_pcol, values=_vcol, aggfunc="first")
    dpct_stayer = (sp.loc["STAYERS", "now"] / sp.loc["STAYERS", "then"] - 1) * 100
    dpct_leaver = (sp.loc["LEAVERS_ALL", "now"] / sp.loc["LEAVERS_ALL", "then"] - 1) * 100

    expect("leaver profit growth %", pct_leaver, 25.1)
    expect("stayer profit growth %", pct_stayer, 23.6)
    expect("stayer spend growth %", dpct_stayer, 2.3)
    expect("leaver spend growth %", dpct_leaver, -1.1)

    # PROFIT window: pm_asks_results.csv carries no window_label column to read at runtime.
    # Hardcoded from pm_asks_recompute.py provenance — it reads b_ucp_v3, which unsub_unified.py
    # anchors to T0_ANCHOR_B=2025-06-30 ("then") / T1_ANCHOR_B=2026-06-30 ("now"), both closed.
    PROFIT_WINDOW = "Jun 2025 -> Jun 2026"
    _PROFIT_THEN, _PROFIT_NOW = "2025-06-30", "2026-06-30"
    if "window_label" in bd.columns and bd["window_label"].notna().any():
        SPEND_WINDOW = bd["window_label"].dropna().iloc[0]
    else:
        SPEND_WINDOW = "UNKNOWN - b_delta_summary.csv has no window_label column this run"
    print(f"profit window: {PROFIT_WINDOW}  (source: code inspection, not a data column)")
    print(f"spend window:  {SPEND_WINDOW}  (source: b_delta_summary.csv window_label column)")
    if _PROFIT_THEN in SPEND_WINDOW and _PROFIT_NOW in SPEND_WINDOW:
        print("windows match (same calendar anchors).")
    else:
        print(f"WINDOW MISMATCH: profit={PROFIT_WINDOW}  spend={SPEND_WINDOW}")

    print(f"""
VALUE (descriptive — groups not matched; leavers skew younger, 4-7yr tenure):
- Leavers' average annual profit still grows: +{pct_leaver:.1f}% ({PROFIT_WINDOW}, everyone-anchored
  basis, no-longer-present clients counted at $0).
- Leavers' card spend turns negative: {dpct_leaver:+.1f}%, while stayers' card spend is positive:
  {dpct_stayer:+.1f}% ({SPEND_WINDOW}, DFP-matched).
""")

# %% [6] On-slide definitions + CALL (copy blocks for the deck)
print("""ON-SLIDE DEFINITIONS (must ship with the exhibits)
- Unsub = completed per-list opt-out (disposition 4, verified 2026-08-05), ATTRIBUTED
  to the list unsubbed. All exhibits are attribution-based (attribution != exposure).
- Value numbers (profit, spend) are DESCRIPTIVE (groups not matched; leavers skew younger,
  4-7yr tenure).

CALL (business-case line, slide 2):
Unsub rate is an early relationship-thinning signal. Protect the channel where the
relationship is shallow: cadence/suppression tests on (a) PCQ acquisition audiences and
(b) re-contact-after-gap cohorts - both measurable with existing randomization.""")
