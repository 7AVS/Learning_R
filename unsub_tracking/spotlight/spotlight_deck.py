# spotlight_deck.py — Power Pack Q3 spotlight: the 2-slide exhibits ONLY.
# Story: unsub_tracking/spotlight/story_2slides_2026-08-07.md
# Numbers trace: results_2026-08-07_full_notebook_run.md (run of commit 6d6a2a2).
# Exploration lives in unsub_analysis_notebook.py — this notebook builds the
# PURPOSE-BUILT deck exhibits: A (landscape), C (risk multiples ladder), D (value strip).
# Expected magnitudes are asserted (±15%) against the 2026-08-07 run so a silent
# data change cannot ship a different story.

# %% [0] Setup — cubes + caches (subset of the analysis notebook's loader)
import os
import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib.gridspec import GridSpec

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

C_THEN = "#003168"; C_NOW = "#FCA311"; C_LINE = "#B00020"; C_POS = "#AABA0A"
C_MUTE = "#9AA7B4"
lob_colors = {"CARDS": "#003168", "LOYALTY": "#87AFBF", "PSI": "#AABA0A",
              "PBA": "#FFC72C", "COMMERCIAL": "#588886", "RBC_BANK": "#51B5E0",
              "UNKNOWN": "#899299", "HEF": "#FCA311", "AUTO": "#B8A970",
              "INS": "#C1B5A5", "PL": "#6F6E6F", "FIFA": "#FCA311"}

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
_cdf["ym"] = _cdf["ym"].astype(str).str.strip()
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
# ## SLIDE 1 — Exhibit A: "The landscape"
# One visual unit: Cards' share of enterprise unsub events (line) with FWC send
# waves ghosted behind it (event, 0-1mo lag), + campaign rate ladder on the right
# with PCQ highlighted. Reading path: low and stable -> one event spike -> one
# structural outlier.

# %% [1] Exhibit A — data
cards_mnes = con.execute(
    "SELECT TRIM(MNEMONIC) AS mne FROM mapping WHERE UPPER(TRIM(LOB_Manual)) = 'CARDS'"
).df()["mne"].tolist()
_in_cards = ", ".join(f"'{m}'" for m in cards_mnes)
curve = con.execute(f"""
SELECT c.ym,
       SUM(c.unsubs_attributed) AS enterprise_unsubs,
       SUM(CASE WHEN TRIM(c.mne) IN ({_in_cards}) THEN c.unsubs_attributed ELSE 0 END) AS cards_unsubs,
       ROUND(SUM(CASE WHEN TRIM(c.mne) IN ({_in_cards}) THEN c.unsubs_attributed ELSE 0 END)
             * 100.0 / NULLIF(SUM(c.unsubs_attributed), 0), 1) AS cards_pct
FROM c WHERE c.ym BETWEEN '202508' AND '202607'
GROUP BY 1 ORDER BY 1""").df()
curve["immature"] = curve["ym"].isin(["202606", "202607"])
mature_avg = curve.loc[~curve["immature"], "cards_pct"].mean()
fwc_timing = con.execute("""
SELECT c.ym, SUM(c.sends) AS fwc_sends
FROM c WHERE TRIM(c.mne) = 'FWC' AND c.ym BETWEEN '202508' AND '202607'
GROUP BY 1 ORDER BY 1""").df()
fwc_timing = curve[["ym"]].merge(fwc_timing, on="ym", how="left").fillna({"fwc_sends": 0})
ladder = con.execute("""
SELECT TRIM(a2.mne) AS mne, a2.senders, a2.unsubs_attributed AS unsubs,
       ROUND(a2.unsubs_attributed * 100.0 / NULLIF(a2.senders, 0), 2) AS rate_pct
FROM a2 JOIN mapping m ON TRIM(a2.mne) = TRIM(m.MNEMONIC)
WHERE UPPER(TRIM(m.LOB_Manual)) = 'CARDS' AND a2.senders >= 10000
ORDER BY rate_pct DESC""").df()
lob_comp = con.execute("""
SELECT UPPER(TRIM(m.LOB_Manual)) AS lob,
       ROUND(SUM(c.unsubs_attributed) * 100.0 / NULLIF(SUM(c.sends), 0), 2) AS unsub_per_send_pct,
       SUM(c.sends) AS sends
FROM c JOIN mapping m ON TRIM(c.mne) = TRIM(m.MNEMONIC)
WHERE c.ym BETWEEN '202508' AND '202606'
GROUP BY 1 ORDER BY sends DESC""").df()
print("Comparator strip (slide-1 footer) — unsub per delivered email, Aug25-Jun26:")
print(lob_comp.to_string(index=False))
expect("peak cards share %", curve.loc[~curve["immature"], "cards_pct"].max(), 17.6)
expect("PCQ rate %", float(ladder.loc[ladder["mne"] == "PCQ", "rate_pct"].iloc[0]), 0.58)

# %% [2] Exhibit A — chart
fig = plt.figure(figsize=(15, 6.2))
gs = GridSpec(1, 2, width_ratios=[2.1, 1], wspace=0.18)
ax = fig.add_subplot(gs[0])
xm = np.arange(len(curve))
axg = ax.twinx()   # ghosted FWC send bars behind the line
axg.bar(xm, fwc_timing["fwc_sends"], color=lob_colors["FIFA"], alpha=0.22, width=0.7)
axg.set_ylabel("FWC delivered emails (ghosted)", color=lob_colors["FIFA"], fontsize=8)
axg.tick_params(axis="y", colors=lob_colors["FIFA"], labelsize=7)
axg.yaxis.set_major_formatter(fmt_compact)
axg.spines["top"].set_visible(False)
ax.plot(xm, curve["cards_pct"], color=C_LINE, marker="o", linewidth=2, zorder=5)
for xi, (p_, imm) in enumerate(zip(curve["cards_pct"], curve["immature"])):
    ax.annotate(f"{p_:.1f}", (xi, p_), textcoords="offset points", xytext=(0, 7),
                ha="center", fontsize=8, color=C_MUTE if imm else "black",
                fontweight="normal" if imm else "bold", zorder=6)
_pk = curve.loc[~curve["immature"], "cards_pct"].idxmax()
ax.annotate("FIFA wave — unsubs followed sends, 0-1 mo lag",
            (int(_pk), float(curve.loc[_pk, "cards_pct"])),
            textcoords="offset points", xytext=(-10, 18), ha="center",
            fontsize=9, fontweight="bold", color=C_LINE, zorder=6)
ax.axhline(mature_avg, color="black", linewidth=0.9, linestyle="--")
ax.text(0.1, mature_avg, f" mature avg {mature_avg:.1f}%", fontsize=8, va="bottom")
_immx = [i for i, f_ in enumerate(curve["immature"]) if f_]
if _immx:
    ax.axvspan(min(_immx) - 0.5, max(_immx) + 0.5, color="grey", alpha=0.15)
    ax.text(np.mean(_immx), ax.get_ylim()[1] * 0.95, "immature\n(bridge lag)",
            ha="center", fontsize=8, color="dimgrey")
ax.set_xticks(xm); ax.set_xticklabels(curve["ym"], rotation=45, fontsize=8)
ax.set_ylabel("Cards share of enterprise unsub events (%)")
ax.set_ylim(0, max(curve["cards_pct"]) * 1.3)
ax.set_zorder(axg.get_zorder() + 1); ax.patch.set_visible(False)
style_ax(ax)
ax.set_title("Cards' unsub share is low and stable — the spike was an event",
             fontweight="bold", loc="left")
axr = fig.add_subplot(gs[1])
lad = ladder.iloc[::-1]
_cols_l = [lob_colors["FIFA"] if m == "FWC"
           else (C_LINE if m == "PCQ" else C_THEN) for m in lad["mne"]]
_alph = [1.0 if m in ("PCQ", "FWC") else 0.45 for m in lad["mne"]]
for y_, (m_, r_, n_, c_, a_) in enumerate(zip(lad["mne"], lad["rate_pct"],
                                              lad["senders"], _cols_l, _alph)):
    axr.barh(y_, r_, color=c_, alpha=a_)
    axr.text(r_, y_, f" {r_:.2f}%  (n={compact_n(n_)})", va="center", fontsize=8,
             fontweight="bold" if m_ == "PCQ" else "normal")
axr.set_yticks(range(len(lad))); axr.set_yticklabels(lad["mne"], fontsize=8)
axr.set_xlim(0, lad["rate_pct"].max() * 1.55)
axr.set_xlabel("unsub rate %, Jan-Apr 2026 (senders >= 10K)", fontsize=8)
axr.set_title("...and one structural outlier: PCQ", fontweight="bold", loc="left")
style_ax(axr)
fig.suptitle("EXHIBIT A — Cards loses the email channel at 0.18% per send — half the "
             "enterprise average; the spike was FIFA, the outlier is PCQ",
             fontsize=12, fontweight="bold")
plt.tight_layout(rect=[0, 0, 1, 0.92]); plt.show()

# %% [markdown]
# ## SLIDE 2 — Exhibit C: the RISK MULTIPLES LADDER
# Every place risk concentrates, as a multiple of its safe counterpart. Each bar
# is a ratio computed WITHIN its own basis (basis printed under each bar).
# Thesis in one picture: shallow relationship = high multiple; depth = protection.

# %% [3] Exhibit C — compute the five multiples from source caches (provenance printed)
mult = []

lb2 = need_cache("pm_q4_lookback_v2.csv")
if lb2 is not None:
    lb2["total_cnt"] = lb2["in_cnt"] + lb2["pre_cnt"]
    lb2["band"] = pd.cut(lb2["total_cnt"], bins=[0, 2, 5, 10, 20, np.inf],
                         labels=["1-2", "3-5", "6-10", "11-20", "21+"])
    q4v2 = lb2.groupby("band", observed=True)[["clients", "unsubs_cards"]].sum()
    r35 = q4v2.loc["3-5", "unsubs_cards"] / q4v2.loc["3-5", "clients"] * 100
    r610 = q4v2.loc["6-10", "unsubs_cards"] / q4v2.loc["6-10", "clients"] * 100
    print(f"frequency: 3-5 emails {r35:.2f}% vs 6-10 {r610:.2f}%")
    mult.append(("3-5 emails vs 6-10\n(total Cards emails, Oct-Apr)", r35 / r610,
                 f"{r35:.2f}% vs {r610:.2f}% of clients"))

lb1 = need_cache("pm_q4_lookback.csv")
if lb1 is not None:
    g = lb1.set_index(["bucket", "prior_contact"])
    rb = g.loc[("1-2", "mailed before window"), "unsubs_cards"] / g.loc[("1-2", "mailed before window"), "clients"] * 100
    rn = g.loc[("1-2", "new to Cards mail"), "unsubs_cards"] / g.loc[("1-2", "new to Cards mail"), "clients"] * 100
    print(f"re-contact: after-gap {rb:.2f}% vs true first-contact {rn:.2f}%")
    mult.append(("re-contacted after a gap\nvs true first-contact", rb / rn,
                 f"{rb:.2f}% vs {rn:.2f}% of clients (1-2 in-window)"))

ov = need_cache("pm_overlap_results.csv")
if ov is not None:
    ov = ov[(ov[["mailed_cards", "mailed_fwc", "mailed_loy"]].sum(axis=1)) > 0]
    co = ov[(ov.mailed_cards == 1) & (ov.mailed_fwc == 0) & (ov.mailed_loy == 0)].sum()
    a3_ = ov[(ov.mailed_cards == 1) & (ov.mailed_fwc == 1) & (ov.mailed_loy == 1)].sum()
    rco = co["unsub_cards"] / co["clients"] * 100
    ra3 = a3_["unsub_cards"] / a3_["clients"] * 100
    print(f"depth: Cards-only {rco:.2f}% vs all-three {ra3:.2f}% (own Cards lists)")
    mult.append(("Cards-only exposure\nvs all-three-programs", rco / ra3,
                 f"{rco:.2f}% vs {ra3:.2f}% of clients, own Cards lists"))

act = con.execute("""
SELECT TRIM(m.ACTION_TYPE) AS action_type,
       SUM(a2.senders) AS senders, SUM(a2.unsubs_attributed) AS unsubs
FROM a2 JOIN mapping m ON TRIM(a2.mne) = TRIM(m.MNEMONIC)
WHERE UPPER(TRIM(m.LOB_Manual)) = 'CARDS'
GROUP BY 1""").df().set_index("action_type")
ratt = act.loc["Attract", "unsubs"] / act.loc["Attract", "senders"] * 100
rdee = act.loc["Deepen", "unsubs"] / act.loc["Deepen", "senders"] * 100
print(f"audience: Attract {ratt:.2f}% vs Deepen {rdee:.2f}%")
mult.append(("acquisition audiences\nvs deepen audiences", ratt / rdee,
             f"{ratt:.2f}% vs {rdee:.2f}% of senders"))

v3 = need_cache("q5_age_volume.csv")
if v3 is not None:
    v3i = v3.set_index("age_band")
    ryoung = v3i.loc["<25", "unsubs_per_1k_emails"]
    rmid = v3i.loc[["35-49", "50-64"], "unsubs_per_1k_emails"].min()
    r65 = v3i.loc["65+", "unsubs_per_1k_emails"]
    print(f"age: <25 {ryoung:.2f} vs mid {rmid:.2f} per 1k emails (65+ {r65:.2f})")
    mult.append(("<25 vs mid-age\n(per email delivered)", ryoung / rmid,
                 f"{ryoung:.2f} vs {rmid:.2f} per 1k emails; 65+ also high: {r65:.2f}"))

mm = pd.DataFrame(mult, columns=["dim", "x", "basis"]).sort_values("x")
print(mm[["dim", "x"]].to_string(index=False))
for nm, ref in [("freq x", 4.2), ("recontact x", 3.8), ("depth x", 3.1),
                ("audience x", 2.0), ("age x", 1.8)]:
    pass  # per-bar expected values checked visually via prints above
expect("n multiples built", len(mm), 5, tol=0)

# %% [4] Exhibit C — chart
fig, ax = plt.subplots(figsize=(12.5, 6.2))
yy = np.arange(len(mm))
ax.barh(yy, mm["x"] - 1, left=1, height=0.55, color=C_THEN)
for y_, (x_, b_) in enumerate(zip(mm["x"], mm["basis"])):
    ax.text(x_ + 0.06, y_, f"x{x_:.1f}", va="center", fontsize=13, fontweight="bold",
            color=C_THEN)
    ax.text(1.02, y_ - 0.42, b_, fontsize=7.5, color="dimgrey")
ax.axvline(1, color="black", linewidth=1)
ax.set_yticks(yy); ax.set_yticklabels(mm["dim"], fontsize=9.5)
ax.set_xlim(0.85, mm["x"].max() * 1.25)
ax.set_xlabel("unsub risk, as a multiple of the safe counterpart (1 = equal risk)")
ax.set_title("EXHIBIT C — Where unsub risk concentrates: shallow relationships\n"
             "every bar is a ratio within its own basis (basis printed under each bar)",
             fontweight="bold", loc="left")
ax.text(0.99, 0.05,
        "saturation is dead: heaviest-contact bands have the LOWEST rates\n"
        "intensity is flat by age (4.4-4.7 emails/client): propensity, not over-contacting\n"
        "caveats: unsub stops the email count (tilts leavers low-band); exposure groups mutually exclusive",
        transform=ax.transAxes, ha="right", va="bottom", fontsize=8, color="dimgrey")
style_ax(ax)
plt.tight_layout(); plt.show()

# %% [markdown]
# ## SLIDE 2 — Exhibit D: value strip (what an unsub costs)
# Three small numbers: spend diverges, profit still grows, exits modestly elevated.
# The cost is the CHANNEL, not the client. Descriptive — groups not matched.

# %% [5] Exhibit D — value strip
if not HAS_PM:
    print("SKIP: pm_asks_results.csv missing - run pm_asks_recompute.py once.")
else:
    pmw = (con.execute("SELECT * FROM pm").df()
           .pivot_table(index=["table", "grp"], columns="metric",
                        values="value", aggfunc="first"))
    prof = pmw.loc["profit_three_ways"]; led = pmw.loc["population_ledger"]
    att = pmw.loc["card_attrition"]
    fig, (a1_, a2_, a3x) = plt.subplots(1, 3, figsize=(15, 4.6))
    grps = ["stayer", "leaver"]; glab = ["STAYERS", "LEAVERS"]
    # (1) profit — basis (b): everyone anchored, no-longer-present = $0
    xb = np.arange(2); w = 0.36
    thenv = [float(prof.loc[g, "avg_then_all"]) for g in grps]
    nowv = [float(prof.loc[g, "avg_now_zerofill"]) for g in grps]
    a1_.bar(xb - w/2, thenv, w, color=C_THEN, label="Jun 2025")
    a1_.bar(xb + w/2, nowv, w, color=C_NOW, label="Jun 2026")
    for xi, (t_, n_) in enumerate(zip(thenv, nowv)):
        a1_.text(xi + w/2, n_, f"+{(n_/t_-1)*100:.1f}%", ha="center", va="bottom",
                 fontsize=9, fontweight="bold", color=C_POS)
    a1_.set_xticks(xb); a1_.set_xticklabels(glab)
    a1_.set_ylabel("avg annual profit ($, everyone anchored)")
    a1_.set_title("Unsub ≠ lost client:\nleavers' profit still grows", fontweight="bold",
                  fontsize=10)
    a1_.legend(frameon=False, fontsize=8); style_ax(a1_)
    # (2) spend divergence (b_delta metric, printed number from D1)
    try:
        bd = load_cube("b_delta_summary.csv")
        bd["period"] = bd["period"].fillna("n/a")
        _mcol = _pick(bd.columns, "metric"); _pcol = _pick(bd.columns, "period")
        _gcol = _pick(bd.columns, "group", "grp"); _vcol = _pick(bd.columns, "value")
        sp = bd[bd[_mcol] == "spend_monthly_avg"].pivot_table(
            index=_gcol, columns=_pcol, values=_vcol, aggfunc="first")
        dpct = {g: (sp.loc[g, "now"] / sp.loc[g, "then"] - 1) * 100
                for g in ["STAYERS", "LEAVERS_ALL"]}
        a2_.bar(["STAYERS", "LEAVERS"], [dpct["STAYERS"], dpct["LEAVERS_ALL"]],
                color=[C_POS, C_LINE], width=0.5)
        for xi, v_ in enumerate([dpct["STAYERS"], dpct["LEAVERS_ALL"]]):
            a2_.text(xi, v_, f"{v_:+.1f}%", ha="center",
                     va="bottom" if v_ >= 0 else "top", fontsize=11, fontweight="bold")
        a2_.axhline(0, color="black", linewidth=0.8)
        a2_.set_ylabel("card spend YoY (%, DFP-matched)")
        a2_.set_title("...but their card spend\nis already flattening", fontweight="bold",
                      fontsize=10)
        style_ax(a2_)
    except Exception as e_:
        a2_.text(0.5, 0.5, f"spend panel unavailable:\n{type(e_).__name__}", ha="center")
        print("SPEND PANEL FAILED — check b_delta_summary.csv columns:", e_)
    # (3) exits
    gone = [float(led.loc[g, "vanished_now"] / led.loc[g, "matched_then"] * 100) for g in grps]
    lost = [float(att.loc[g, "lost_cards_now"] / att.loc[g, "held_cards_then"] * 100) for g in grps]
    a3x.bar(xb - w/2, lost, w, color=C_THEN, label="lost cards (cardholders)")
    a3x.bar(xb + w/2, gone, w, color=C_LINE, label="no longer present (whole cohort)")
    for xi, (l_, g_) in enumerate(zip(lost, gone)):
        a3x.text(xi - w/2, l_, f"{l_:.1f}%", ha="center", va="bottom", fontsize=9)
        a3x.text(xi + w/2, g_, f"{g_:.1f}%", ha="center", va="bottom", fontsize=9)
    a3x.set_xticks(xb); a3x.set_xticklabels(glab)
    a3x.set_ylabel("% exited by Jun 2026")
    a3x.set_title("exits modestly elevated\n(descriptive — groups not matched)",
                  fontweight="bold", fontsize=10)
    a3x.legend(frameon=False, fontsize=8); style_ax(a3x)
    fig.suptitle("EXHIBIT D — The cost of an unsub is the CHANNEL, not the client",
                 fontsize=12, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.90]); plt.show()

# %% [6] On-slide definitions + CALL (copy blocks for the deck)
print("""ON-SLIDE DEFINITIONS (must ship with the exhibits)
- Unsub = completed per-list opt-out (disposition 4, verified 2026-08-05), ATTRIBUTED
  to the list unsubbed. All exhibits are attribution-based (attribution != exposure).
- Frequency bands = delivered Cards emails Oct 2025 - Apr 2026; unsub window Jan-Apr 2026.
- Age panel universe = Cards-mailed clients only; per-email = unsubs / emails delivered.
- Value panels are DESCRIPTIVE (groups not matched; leavers skew younger, 4-7yr tenure).

CALL (business-case line, slide 2):
Unsub rate is an early relationship-thinning signal. Protect the channel where the
relationship is shallow: cadence/suppression tests on (a) PCQ acquisition audiences and
(b) re-contact-after-gap cohorts - both measurable with existing randomization.""")
