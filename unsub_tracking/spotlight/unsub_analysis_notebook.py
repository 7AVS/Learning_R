# unsub_analysis_notebook.py — repo-owned rebuild of the CSV analysis
# notebook. SELF-CONTAINED: cold start needs only (1) Python with duckdb,
# pandas, numpy, plotly; (2) the delivered CSVs in ONE folder. Runs on
# the pod (jovyan) or a local machine — set BASE below, nothing else.
# Chart conventions follow spotlight/plot_revision_prompt.py G1-G8:
# percent format w/ 2 decimals, n on every rate, small-base guard <10k,
# say WHICH Cards definition, no causal language, 9.2% no-match shown.

# %% [0] Setup + data load (DuckDB views over the delivered CSVs)
import os
import duckdb
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

pd.set_option("display.width", 160)
pd.set_option("display.max_columns", 20)
pd.set_option("display.max_rows", 60)

# --- data sources ------------------------------------------------------
# On the pod (Spark kernel) the loader reads the HDFS copies the pipeline
# landed; anywhere else it falls back to the CSV folder automatically.
# Small derived files (pm_asks_results, pm_overlap cache) always live in
# the local folder.
HDFS_OUT = "hdfs:///user/427966379/unsub_unified/out/"
BASE = os.path.expanduser("~/unsub_unified_out/")            # pod local
# BASE = r"\\maple.fg.rbc.com\...\Cards\Unsubs\output"       # laptop share
USE_HDFS = "spark" in globals()

def load_cube(fname, alts=(), **read_csv_kwargs):
    """HDFS-first (pod), local-CSV fallback. `alts` = alternate filenames
    tried in order after `fname` (naming differs between copies)."""
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

# RBC brand palette (Section 4.2 Charts and Graphs)
C_THEN = "#003168"   # Dark Blue
C_NOW  = "#FCA311"   # Sunburst
C_LINE = "#B00020"   # Red (secondary) — per Andre 2026-08-06, was Sky blue
C_POS  = "#AABA0A"   # Apple

# LOB colour map (same scheme as the local analysis notebook)
lob_colors = {
    "CARDS": "#003168", "LOYALTY": "#87AFBF", "PSI": "#AABA0A",
    "PBA": "#FFC72C", "COMMERCIAL": "#588886", "RBC_BANK": "#51B5E0",
    "UNKNOWN": "#899299", "HEF": "#FCA311", "AUTO": "#B8A970",
    "INS": "#C1B5A5", "PL": "#6F6E6F",
}

def compact_n(v):
    v = float(v)
    if abs(v) >= 1_000_000: return f"{v/1_000_000:.1f}M"
    if abs(v) >= 1_000:     return f"{v/1_000:.1f}K"
    return f"{v:.0f}"

con = duckdb.connect()
print(f"Loading cubes (USE_HDFS={USE_HDFS}):")
_frames = {}
for view, fname in [("a1", "a1_mne_share.csv"), ("a1_lob", "a1_lob_dedup.csv"),
                    ("a2", "a2_mne_rates.csv"), ("a3", "a3_contact_cube.csv"),
                    ("a4", "a4_profile_cube.csv"), ("b", "b_before_after_cube.csv")]:
    _frames[view] = load_cube(fname)
    con.register(view, _frames[view])
# mapping file: Andre's pod copy is named "mapping Mne.csv" (space, capital
# M) — try that first, underscore variant as fallback.
_frames["mapping"] = load_cube("mapping Mne.csv", alts=("mapping_mne.csv",))
con.register("mapping", _frames["mapping"])

_cdf = load_cube("c_monthly_curve.csv", encoding="latin-1", on_bad_lines="skip")
# The landed CSV carries provenance/audit columns beyond the 4 analytical
# ones (G8: provenance ignored). Select by name, never blind-rename.
def _pick(cols, *needles):
    hits = [c2 for c2 in cols if any(n in c2.lower() for n in needles)]
    assert hits, f"c_monthly_curve: no column matching {needles} in {list(cols)} - STOP"
    return hits[0]
_cols = _cdf.columns
_cdf = _cdf.rename(columns={
    _pick(_cols, "mne"): "mne",
    _pick(_cols, "ym", "month"): "ym",
    _pick(_cols, "send", "deliver"): "sends",
    _pick(_cols, "unsub"): "unsubs_attributed",
})[["mne", "ym", "sends", "unsubs_attributed"]]
_cdf["ym"] = _cdf["ym"].astype(str).str.strip()
_cdf["sends"] = pd.to_numeric(_cdf["sends"], errors="coerce")
_cdf["unsubs_attributed"] = pd.to_numeric(_cdf["unsubs_attributed"], errors="coerce")
con.register("c", _cdf)
print("c columns used:", dict(zip(["mne", "ym", "sends", "unsubs_attributed"],
      [_pick(_cols, "mne"), _pick(_cols, "ym", "month"),
       _pick(_cols, "send", "deliver"), _pick(_cols, "unsub")])))

# pm_asks_results.csv exists only after spotlight/pm_asks_recompute.py has
# run once in the pod — guard so this notebook still works without it.
PM_CSV = os.path.join(BASE, "pm_asks_results.csv")
HAS_PM = os.path.exists(PM_CSV)
if HAS_PM:
    con.execute(f"CREATE OR REPLACE VIEW pm AS SELECT * FROM read_csv_auto('{PM_CSV}')")

lob_dedup = con.execute("SELECT label, unique_unsub_clients FROM a1_lob").df()
ENTERPRISE_DEDUP  = int(lob_dedup.loc[lob_dedup["label"] == "ENTERPRISE",     "unique_unsub_clients"].iloc[0])
CARDS_LOB_DEDUP   = int(lob_dedup.loc[lob_dedup["label"] == "CARDS_LOB_ALL",  "unique_unsub_clients"].iloc[0])
CARDS_EX_FWC_DEDUP = int(lob_dedup.loc[lob_dedup["label"] == "CARDS_EX_FWC",  "unique_unsub_clients"].iloc[0])

print("Views registered. Rows per cube:")
for v in ["a1", "a1_lob", "a2", "a3", "a4", "b", "c", "mapping"] + (["pm"] if HAS_PM else []):
    print(f"  {v:8s} {con.execute(f'SELECT COUNT(*) FROM {v}').fetchone()[0]:,}")
print(f"pm_asks_results.csv present: {HAS_PM}")
print(f"Deduped: enterprise {ENTERPRISE_DEDUP:,} · cards LOB {CARDS_LOB_DEDUP:,} · cards ex-FWC {CARDS_EX_FWC_DEDUP:,}")

# %% [1] Q0 — Monthly sends + unsub rate by LOB (small multiples, top 6)
# bars = delivered emails; line = unsubs per delivered email % (G1: the
# c-cube denominators are delivered EVENTS, so the rate is labeled
# "per delivered email", not per sender).
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

_titles, _subs = [], {}
for lob in lob_order:
    sub = df0[df0["lob_manual"] == lob].set_index("ym").reindex(months)
    _subs[lob] = sub
    tot_s, tot_u = sub["sends"].sum(), sub["unsubs"].sum()
    _titles.append(f"{lob}  (total sends: {compact_n(tot_s)} | "
                   f"avg unsub rate: {tot_u * 100.0 / tot_s:.2f}%)")
fig = make_subplots(rows=3, cols=2, subplot_titles=_titles,
                    specs=[[{"secondary_y": True}] * 2 for _ in range(3)],
                    shared_xaxes=True, vertical_spacing=0.09)
for i, lob in enumerate(lob_order):
    r, c = i // 2 + 1, i % 2 + 1
    sub = _subs[lob]
    fig.add_trace(go.Bar(x=months, y=sub["sends"].fillna(0), marker_color=C_THEN,
                         opacity=0.75, showlegend=False), row=r, col=c)
    fig.add_trace(go.Scatter(x=months, y=sub["unsub_per_email_pct"],
                             mode="lines+markers", line=dict(color=C_LINE, width=2),
                             marker=dict(size=5), showlegend=False),
                  row=r, col=c, secondary_y=True)
    fig.update_yaxes(range=[0, ymax_sends], row=r, col=c, secondary_y=False,
                     tickformat="~s")
    fig.update_yaxes(range=[0, ymax_rate], row=r, col=c, secondary_y=True,
                     color=C_LINE, showgrid=False)
fig.update_layout(template="plotly_white", height=950,
    title=("Q0: Monthly Sends and Unsub Rate by LOB — Aug 2025 to Jun 2026<br>"
           "<sup>bars = delivered emails · red line = unsubs per delivered email % · "
           "LOB from Andre's mapping file</sup>"))
fig.show()

# %% [markdown]
# ---
# # Stakeholder follow-up — the three asks from the 2026-08-06 feedback email
#
# Feedback on the spotlight asked us to narrow focus to three things:
#
# 1. **Loyalty x Cards overlap** — is unsub% higher when a client gets both
#    Loyalty and Cards mail vs one alone? *Status: pending — needs one new
#    client-grain pull keyed on the LOYALTY rows of the mapping file. Not
#    in this notebook yet.*
# 2. **Attrition** — do unsubscribers leave Cards and/or the bank more
#    than stayers? *Answered below (cell [ATTRITION]).*
# 3. **Profit population check** — the original then->now profit compared
#    only clients present at BOTH anchors. If more leavers than stayers
#    drop out between anchors, the surviving remnant is better-selected
#    and the comparison is biased. *Answered below (cell [PROFIT CHECK]) by
#    holding the THEN population fixed.*
#
# **Definitions used throughout** (same as the delta section):
# cohort = 4,783,193 clients mailed by a Cards campaign on/before
# Jun 30 2025 · "leaver" = client with a Cards-marketing unsubscribe by
# that anchor (NOT account closure, NOT attrition) · then = Jun 30 2025,
# now = Jun 30 2026 · profit = UCP annual estimate · clients with no UCP
# match are shown explicitly, never silently dropped.

# %% [markdown]
# ## PROFIT CHECK — Profit, recomputed on a fixed population
#
# **The question this answers:** "did profit really grow for
# unsubscribers, or did the comparison quietly drop the clients who
# left?"
#
# **Explain it like I'm five:** picture 100 leavers in June 2025,
# averaging $550 profit each. A year later, 6 of the 100 no longer
# appear in the bank's profitability data at all. The ORIGINAL number
# averaged only the 94 still present — like grading a class after the
# failing students dropped out. The FIXED number keeps all 100 in the
# denominator: the 6 who vanished count as $0 now. If the "growth" was
# just dropouts leaving, the fixed number exposes it.
#
# **What each bar pair below means:** left panel (a) = the original,
# survivors-only math, shown so the correction is visible. Right panel
# (b) = the fixed math — this is the number we report. Each pair: dark
# blue = June 2025 average, orange = June 2026 average; the delta above
# each pair shows the change in $ AND %.
#
# **Result (run 2026-08-06):** the finding survives. Population-fixed:
# leavers $550 -> $688 (**+$138 = +25.1%**), stayers $795 -> $982
# (**+$187 = +23.6%**). Profit rises for both groups either way; the old
# basis was mildly flattering both (the vanished were low-value: $134
# leaver / $183 stayer averages). Report basis (b).
#
# **What the check DID surface:** leavers VANISH from the profitability
# data at **5.9% vs 2.6%** for stayers — 2.2x. Who disappears differs
# even though the profit of those who stay does not. That feeds ATTRITION.

# %% [3] PM ask #3 — profit, three bases (needs pm_asks_results.csv)
# (a) survivors-only = published basis; (b) population-fixed, vanished at
# $0 now = the PM's requested basis; delta shown for both. G2: n labeled.
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
    xlabels = [f"{g.upper()}<br>n = {n:,}" for g, n in zip(grps, ns)]
    bases = [("(a) ORIGINAL basis — survivors only", "avg_then_survivors", "avg_now_survivors"),
             ("(b) FIXED basis — everyone kept, vanished = $0", "avg_then_all", "avg_now_zerofill")]
    fig = make_subplots(rows=1, cols=2, subplot_titles=[b[0] for b in bases],
                        shared_yaxes=True)
    _ymax = 0.0
    for ci, (_, tc, nc) in enumerate(bases, start=1):
        thin = [float(prof.loc[g, tc]) for g in grps]
        now = [float(prof.loc[g, nc]) for g in grps]
        _ymax = max(_ymax, *thin, *now)
        fig.add_trace(go.Bar(name="avg profit Jun 2025", x=xlabels, y=thin,
                             marker_color=C_THEN, text=[f"${t:,.0f}" for t in thin],
                             textposition="outside", cliponaxis=False, showlegend=(ci == 1)),
                      row=1, col=ci)
        fig.add_trace(go.Bar(name="avg profit Jun 2026", x=xlabels, y=now,
                             marker_color=C_NOW, text=[f"${n_:,.0f}" for n_ in now],
                             textposition="outside", cliponaxis=False, showlegend=(ci == 1)),
                      row=1, col=ci)
        for xi in range(len(grps)):
            d = now[xi] - thin[xi]
            dp = d / thin[xi] * 100
            fig.add_annotation(x=xlabels[xi], y=max(thin[xi], now[xi]) * 1.2,
                               text=f"<b>{'+' if d >= 0 else ''}${d:,.0f}  ({dp:+.1f}%)</b>",
                               showarrow=False, row=1, col=ci,
                               font=dict(color=C_POS if d >= 0 else C_LINE, size=13))
    fig.update_yaxes(range=[0, _ymax * 1.38])
    fig.update_yaxes(title_text="avg annual profit estimate ($, UCP)", row=1, col=1)
    fig.update_layout(barmode="group", template="plotly_white", height=520,
        title=("PROFIT CHECK: Did unsubscribers' profit really grow? Same data, two ways of counting<br>"
               "<sup>(a) drops clients who vanished by 2026 · (b) keeps every June-2025 client, "
               "vanished count as $0 now — (b) is the reported basis</sup>"),
        legend=dict(orientation="h", yanchor="bottom", y=1.06, xanchor="right", x=1))
    fig.show()
    print("HOW TO READ: panel (a) is the original math - it silently drops"
          " clients who vanished by 2026. Panel (b) keeps every June-2025"
          " client; vanished ones count as $0 now. The story holds in (b):"
          " both groups grow, leavers slightly faster in % terms.")

# %% [markdown]
# ## ATTRITION — Do unsubscribers actually leave more? Yes, on every cut.
#
# **The question this answers:** "unsubscribing is annoying but free —
# does anything REAL follow it? Do these clients drop their card, or the
# bank, more than everyone else?"
#
# **Explain it like I'm five:** take everyone who HAD a credit card in
# June 2025. Look again in June 2026. Three things can be true: (1) still
# has cards; (2) still visible in the data but the card category is gone
# = "lost cards"; (3) not in the data at all anymore = "vanished" — our
# closest available signal for "left the bank" (labeled a PROXY because
# this dataset has no official account-closure field). Compare how often
# (2) and (3) happen for unsubscribers vs everyone else.
#
# **Left chart** = those two exit rates, unsubscribers (leavers) vs
# stayers. **Right chart** = the population ledger behind PROFIT CHECK: of each
# group's June-2025 clients, how many were still findable in June 2026
# vs vanished — the raw counts the percentages come from.
#
# **Result (run 2026-08-06):** leavers exit more on both cuts — lost
# cards **1.91% vs 1.63%** (x1.17), vanished **1.74% vs 1.30%** (x1.34);
# combined ~3.7% vs ~2.9%. At the whole-relationship level (any client,
# not just cardholders): vanished **5.9% vs 2.6%** — 2.2x.
#
# **Caveat that stays attached:** descriptive, not causal. Leavers skew
# younger / 4-7yr tenure; part of the gap is who they are, not what the
# unsubscribe did. Groups are not matched here.

# %% [4] PM ask #2 — card attrition + population ledger (needs pm CSV)
if not HAS_PM:
    print("SKIP: pm_asks_results.csv not in BASE - see cell [3] note.")
else:
    led = pmw.loc["population_ledger"]
    att = pmw.loc["card_attrition"]
    grps = ["stayer", "leaver"]
    lost = [float(att.loc[g, "lost_cards_now"] / att.loc[g, "held_cards_then"] * 100) for g in grps]
    van  = [float(att.loc[g, "vanished_from_ucp_now"] / att.loc[g, "held_cards_then"] * 100) for g in grps]
    ns = [int(att.loc[g, "held_cards_then"]) for g in grps]
    xl = [f"{g.upper()}<br>held cards Jun 2025: n = {n:,}" for g, n in zip(grps, ns)]
    fig = make_subplots(rows=1, cols=2, subplot_titles=[
        "Of clients who HAD cards in Jun 2025,<br>who exited by Jun 2026?",
        "Where each group's Jun-2025 clients ended up<br>(the counts behind PROFIT CHECK's fix)"])
    fig.add_trace(go.Bar(name="no cards anymore (still a client)", x=xl, y=lost,
                         marker_color=C_THEN, text=[f"{v:.2f}%" for v in lost],
                         textposition="outside", cliponaxis=False), row=1, col=1)
    fig.add_trace(go.Bar(name="gone from the data (left-bank PROXY)", x=xl, y=van,
                         marker_color=C_LINE, text=[f"{v:.2f}%" for v in van],
                         textposition="outside", cliponaxis=False), row=1, col=1)
    fig.update_yaxes(range=[0, max(lost + van) * 1.4], row=1, col=1,
                     title_text="% of Jun-2025 cardholders")
    xg = [g.upper() for g in grps]
    m_now = [float(led.loc[g, "matched_now"]) for g in grps]
    v_now = [float(led.loc[g, "vanished_now"]) for g in grps]
    fig.add_trace(go.Bar(name="still found in Jun 2026", x=xg, y=m_now,
                         marker_color=C_THEN), row=1, col=2)
    fig.add_trace(go.Bar(name="vanished by Jun 2026", x=xg, y=v_now, base=m_now,
                         marker_color=C_LINE), row=1, col=2)
    for i, g in enumerate(grps):
        mt = float(led.loc[g, "matched_then"])
        fig.add_annotation(x=xg[i], y=m_now[i] + v_now[i],
                           text=f"<b>vanished: {v_now[i]:,.0f} ({v_now[i] / mt * 100:.1f}%)</b>",
                           showarrow=False, yshift=16,
                           font=dict(color=C_LINE, size=11), row=1, col=2)
    fig.update_yaxes(tickformat="~s", row=1, col=2)
    fig.update_layout(barmode="group", template="plotly_white", height=560,
        margin=dict(t=150),
        title=("ATTRITION: Do unsubscribers leave more? "
               "<sup>(descriptive — groups not matched)</sup>"),
        legend=dict(orientation="h", yanchor="bottom", y=1.14, xanchor="right", x=1))
    fig.show()
    print("HOW TO READ: left = among cardholders, exit is modestly higher"
          " for leavers on both cuts. Right = at whole-relationship level"
          " leavers vanish at 5.9% vs 2.6% - 2.2x the stayer rate.")

# %% [markdown]
# ## OVERLAP — Loyalty x Cards overlap: is unsub% higher when a client gets both?
#
# **The question this answers:** "does receiving BOTH Loyalty and Cards
# mail come with more unsubscribing than receiving just one?"
#
# **Explain it like I'm five — how a client lands in a bucket:** for
# Jan-Apr 2026 we look at each client's DELIVERED emails (disposition 1
# rows — the send records we verified are written same-day and
# completely). If every delivered email in the window came from Cards
# mnemonics only -> bucket "Cards only". Only Loyalty mnemonics ->
# "Loyalty only". At least one of each -> "Both". Cards/Loyalty
# membership = the LOB_Manual column of the mapping file, same as every
# other chart here.
#
# **"Cards only" does NOT mean the client gets no other mail.** They may
# receive PSI, PBA, insurance, anything else — the buckets only describe
# exposure to these TWO LOBs, inside this 4-month window. Given how
# heavily clients are cross-mailed, expect "Both" to be a large bucket —
# the bucket sizes (n on the axis) are themselves a finding.
#
# **What "unsubscribed" means here:** >=1 disposition-4 row in-window on
# that scope's lists. Per the 2026-08-05 verification: a disposition 4
# is a COMPLETED, deliberate per-list opt-out (abandoned attempts write
# nothing; same-day multi-list rows are separate deliberate choices
# ~92% of the time). One client unsubbing 3 Cards lists counts ONCE
# (client-level flag, no event inflation).
#
# **Red-team notes (why a higher "Both" bar alone proves nothing):**
# 1. VOLUME confound — "Both" clients get more total email; more
#    exposure alone raises unsub chances.
# 2. SELECTION — targeting chooses who gets both (e.g. engaged Avion
#    cardholders); those people differ from single-LOB clients.
# 3. WINDOW truncation — a client mailed Loyalty in December but not in
#    Jan-Apr reads "Cards only" here. Buckets are window-relative.
# This chart answers the PM's DESCRIPTIVE question (is the rate higher
# in the overlap?). Separating volume vs synergy vs selection would be
# a follow-up design, not this chart.
#
# **Mechanics:** one aggregated Teradata pull (client grain never leaves
# the server), 10 client-number bites for spool safety, result cached to
# pm_overlap_results.csv — Teradata is touched once, ever.

# %% [5] OVERLAP pull — one aggregated cube, cached to CSV (pod + Teradata)
OVERLAP_CSV = os.path.join(BASE, "pm_overlap_results.csv")
_ALL_CACHES = [OVERLAP_CSV,
               os.path.join(BASE, "pm_overlap_detail.csv"),
               os.path.join(BASE, "pm_overlap_mne.csv")]
if all(os.path.exists(p) for p in _ALL_CACHES):
    print("CACHED: all three overlap caches exist - no Teradata pull needed.")
else:
    for _p in _ALL_CACHES:      # stale/partial cache set -> rebuild all three
        if os.path.exists(_p):
            os.remove(_p)
            print(f"removed stale cache {_p}")
    import getpass
    import teradatasql
    lobs = con.execute("SELECT TRIM(MNEMONIC) AS mne, TRIM(LOB_Manual) AS lob FROM mapping").df()
    CARDS_L = sorted(set(lobs.loc[lobs["lob"] == "CARDS", "mne"]))
    LOY_L = sorted(set(lobs.loc[lobs["lob"] == "LOYALTY", "mne"]))
    assert CARDS_L and LOY_L, f"mapping gave cards={len(CARDS_L)} loyalty={len(LOY_L)} - STOP"
    print(f"mapping: {len(CARDS_L)} CARDS mnes, {len(LOY_L)} LOYALTY mnes")
    _in = lambda ms: ", ".join(f"'{m}'" for m in ms)

    _sql = """
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
          AND SUBSTR(TREATMENT_ID, 8, 3) IN (%(cards)s, %(loy)s)
        GROUP BY 1, 2, 3
    ), ids AS (
        SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
        FROM DTZV01.VENDOR_FEEDBACK_MASTER
        WHERE load_tm >= DATE '2025-10-01' AND CLNT_NO IS NOT NULL
    ), cl AS (
        SELECT i.CLNT_NO,
               MAX(CASE WHEN e.mne IN (%(cards)s) AND e.sent = 1 THEN 1 ELSE 0 END) AS mailed_cards,
               MAX(CASE WHEN e.mne IN (%(loy)s)   AND e.sent = 1 THEN 1 ELSE 0 END) AS mailed_loy,
               MAX(CASE WHEN e.mne IN (%(cards)s) AND e.unsub = 1 THEN 1 ELSE 0 END) AS unsub_cards,
               MAX(CASE WHEN e.mne IN (%(loy)s)   AND e.unsub = 1 THEN 1 ELSE 0 END) AS unsub_loy,
               SUM(CASE WHEN e.mne IN (%(cards)s) AND e.sent = 1 THEN 1 ELSE 0 END) AS emails_cards,
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
    SELECT mailed_cards, mailed_loy, mnes_cards, mnes_loy,
           COUNT(*) AS clients,
           SUM(unsub_cards) AS unsub_cards,
           SUM(unsub_loy) AS unsub_loy,
           SUM(CASE WHEN unsub_cards = 1 OR unsub_loy = 1 THEN 1 ELSE 0 END) AS unsub_either,
           SUM(emails_cards) AS emails_cards,
           SUM(emails_loy) AS emails_loy
    FROM cl
    GROUP BY 1, 2, 3, 4
    """

    # second query: WHICH programs each group received (bucket x mnemonic)
    _sql_mne = """
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
          AND SUBSTR(TREATMENT_ID, 8, 3) IN (%(cards)s, %(loy)s)
        GROUP BY 1, 2, 3
    ), ids AS (
        SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
        FROM DTZV01.VENDOR_FEEDBACK_MASTER
        WHERE load_tm >= DATE '2025-10-01' AND CLNT_NO IS NOT NULL
    ), cl AS (
        SELECT i.CLNT_NO,
               MAX(CASE WHEN e.mne IN (%(cards)s) AND e.sent = 1 THEN 1 ELSE 0 END) AS mailed_cards,
               MAX(CASE WHEN e.mne IN (%(loy)s)   AND e.sent = 1 THEN 1 ELSE 0 END) AS mailed_loy
        FROM ev e
        INNER JOIN ids i
           ON i.consumer_id_hashed = e.consumer_id_hashed
          AND i.TREATMENT_ID = e.TREATMENT_ID
        WHERE MOD(ABS(i.CLNT_NO), 10) = %(bite)d
        GROUP BY 1
    )
    SELECT c2.mailed_cards, c2.mailed_loy, e.mne,
           COUNT(DISTINCT CASE WHEN e.sent = 1 THEN i.CLNT_NO END) AS clients_mailed,
           COUNT(DISTINCT CASE WHEN e.unsub = 1 THEN i.CLNT_NO END) AS clients_unsub
    FROM ev e
    INNER JOIN ids i
       ON i.consumer_id_hashed = e.consumer_id_hashed
      AND i.TREATMENT_ID = e.TREATMENT_ID
    INNER JOIN cl c2 ON c2.CLNT_NO = i.CLNT_NO
    GROUP BY 1, 2, 3
    """
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
    detail = (pd.concat(parts)
              .groupby(["mailed_cards", "mailed_loy", "mnes_cards", "mnes_loy"],
                       as_index=False).sum())
    detail.to_csv(os.path.join(BASE, "pm_overlap_detail.csv"), index=False)
    mne_cube = (pd.concat(parts_mne)
                .groupby(["mailed_cards", "mailed_loy", "mne"], as_index=False).sum())
    mne_cube.to_csv(os.path.join(BASE, "pm_overlap_mne.csv"), index=False)
    # aggregate 4-row cube for the headline/exposure chart (schema-stable)
    agg = detail.copy()
    agg["sum_mnes_cards"] = agg["mnes_cards"] * agg["clients"]
    agg["sum_mnes_loy"] = agg["mnes_loy"] * agg["clients"]
    ov = (agg.groupby(["mailed_cards", "mailed_loy"], as_index=False)
          [["clients", "unsub_cards", "unsub_loy", "unsub_either",
            "emails_cards", "emails_loy", "sum_mnes_cards", "sum_mnes_loy"]].sum())
    ov.to_csv(OVERLAP_CSV, index=False)
    print(f"WROTE {OVERLAP_CSV} + pm_overlap_detail.csv + pm_overlap_mne.csv:")
    print(ov)

# %% [6] OVERLAP chart — unsub% by overlap segment (self-explanatory build)
if not os.path.exists(OVERLAP_CSV):
    print("SKIP: run cell [5] on the pod first (needs Teradata once).")
else:
    ov = pd.read_csv(OVERLAP_CSV)
    ov = ov[(ov["mailed_cards"] == 1) | (ov["mailed_loy"] == 1)]
    segname = {(1, 0): "Cards only", (0, 1): "Loyalty only", (1, 1): "Both"}
    ov["segment"] = ov.apply(lambda r: segname.get(
        (int(r["mailed_cards"]), int(r["mailed_loy"]))), axis=1)
    order = ["Cards only", "Loyalty only", "Both"]
    ov = ov.set_index("segment").reindex(order)
    n_tot = int(ov["clients"].sum())

    has_vol = "emails_cards" in ov.columns
    # CLEAN ATTRIBUTION (Andre 2026-08-06): each group's unsub rate counts
    # ONLY its own LOB's lists; Both shows its two scoped rates.
    r_co = ov.loc["Cards only", "unsub_cards"] / ov.loc["Cards only", "clients"] * 100
    r_lo = ov.loc["Loyalty only", "unsub_loy"] / ov.loc["Loyalty only", "clients"] * 100
    r_bc = ov.loc["Both", "unsub_cards"] / ov.loc["Both", "clients"] * 100
    r_bl = ov.loc["Both", "unsub_loy"] / ov.loc["Both", "clients"] * 100
    xcats = [f"{s}<br>n = {int(ov.loc[s, 'clients']):,}" for s in order]
    cards_scope = [r_co, None, r_bc]
    loy_scope = [None, r_lo, r_bl]
    fig = make_subplots(rows=1, cols=2, subplot_titles=[
        "HEADLINE — clean attribution:<br>unsubs counted only on the group's own lists",
        "EXPOSURE — how much mail did each group get?<br>(avg emails; distinct programs)"])
    fig.add_trace(go.Bar(name="Cards lists", x=xcats, y=cards_scope,
                         marker_color=lob_colors["CARDS"],
                         text=[f"{v:.2f}%" if v is not None else "" for v in cards_scope],
                         textposition="outside", cliponaxis=False), row=1, col=1)
    fig.add_trace(go.Bar(name="Loyalty lists", x=xcats, y=loy_scope,
                         marker_color=lob_colors["LOYALTY"],
                         text=[f"{v:.2f}%" if v is not None else "" for v in loy_scope],
                         textposition="outside", cliponaxis=False), row=1, col=1)
    fig.update_yaxes(range=[0, max(r_co, r_lo, r_bc, r_bl) * 1.35], row=1, col=1,
                     title_text="% of group's clients who unsubscribed<br>from THAT LOB's lists, Jan-Apr")
    if has_vol:
        ec = ov["emails_cards"] / ov["clients"]
        el = ov["emails_loy"] / ov["clients"]
        mcnt = ov["sum_mnes_cards"] / ov["clients"]
        mlnt = ov["sum_mnes_loy"] / ov["clients"]
        fig.add_trace(go.Bar(name="Cards emails", x=xcats, y=ec,
                             marker_color=lob_colors["CARDS"], showlegend=False,
                             text=[f"{v:.1f}<br>({m:.1f} programs)" if v > 0 else ""
                                   for v, m in zip(ec, mcnt)],
                             textposition="outside", cliponaxis=False), row=1, col=2)
        fig.add_trace(go.Bar(name="Loyalty emails", x=xcats, y=el,
                             marker_color=lob_colors["LOYALTY"], showlegend=False,
                             text=[f"{v:.1f}<br>({m:.1f} programs)" if v > 0 else ""
                                   for v, m in zip(el, mlnt)],
                             textposition="outside", cliponaxis=False), row=1, col=2)
        fig.update_yaxes(range=[0, max(ec.max(), el.max()) * 1.4], row=1, col=2,
                         title_text="avg delivered emails per client, Jan-Apr")
    else:
        fig.add_annotation(x=0.78, y=0.5, xref="paper", yref="paper", showarrow=False,
                           text="Exposure panel needs the re-pull:<br>rerun cell [5] once.")
    fig.update_layout(barmode="group", template="plotly_white", height=620,
        margin=dict(t=160, b=110),
        title=("OVERLAP: Does getting BOTH Loyalty and Cards mail come with more unsubscribing? — Jan-Apr 2026<br>"
               f"<sup>Groups are MUTUALLY EXCLUSIVE clients (each counted once, sum = {n_tot:,} of the ~10.4M mailed "
               "enterprise-wide; the rest got neither LOB's mail). Group = which of the two LOBs DELIVERED "
               "email in the window.</sup>"),
        legend=dict(orientation="h", yanchor="bottom", y=1.16, xanchor="right", x=1))
    fig.add_annotation(x=0, y=-0.16, xref="paper", yref="paper", showarrow=False,
                       align="left", font=dict(size=10, color="#555"),
                       text=("<i>CLEAN ATTRIBUTION: a group's rate counts ONLY unsubs on that LOB's own lists — "
                             "a Cards-only client closing a Loyalty list is NOT counted (and vice versa). "
                             "Dark blue = Cards lists, tundra = Loyalty lists — same colors both panels.</i>"))
    fig.show()
    print("HOW TO READ: left = each group's unsub rate on ITS OWN lists"
          " (Both = two scoped rates). Right = how much mail each group"
          " received - the volume context for the left panel.")

# %% [markdown]
# ## OVERLAP deep-dive A — how many programs did each client really get?
#
# The exposure panel's "avg 1.1 programs" is a mixture: most clients got
# exactly one program's mail, a minority got several. This chart shows the
# real split: within each group, the share of clients who received mail
# from 1 / 2 / 3 / 4+ DISTINCT programs (their own two-LOB scope,
# Jan-Apr). Needs pm_overlap_detail.csv (the cell-[5] re-pull).

# %% [7] Program-count distribution per group
DETAIL_CSV = os.path.join(BASE, "pm_overlap_detail.csv")
if not os.path.exists(DETAIL_CSV):
    print("SKIP: pm_overlap_detail.csv missing - delete pm_overlap_results.csv "
          "and rerun cell [5] once (it now writes all three caches).")
else:
    det = pd.read_csv(DETAIL_CSV)
    det = det[(det["mailed_cards"] == 1) | (det["mailed_loy"] == 1)].copy()
    segname = {(1, 0): "Cards only", (0, 1): "Loyalty only", (1, 1): "Both"}
    det["segment"] = det.apply(lambda r: segname.get(
        (int(r["mailed_cards"]), int(r["mailed_loy"]))), axis=1)
    det["n_programs"] = (det["mnes_cards"] + det["mnes_loy"]).clip(lower=1)
    det["prog_bin"] = det["n_programs"].map(
        lambda v: "1" if v == 1 else ("2" if v == 2 else ("3" if v == 3 else "4+")))
    dist = (det.groupby(["segment", "prog_bin"])["clients"].sum()
            .unstack(fill_value=0).reindex(["Cards only", "Loyalty only", "Both"]))
    share = dist.div(dist.sum(axis=1), axis=0) * 100
    print(dist)
    long = share.reset_index().melt(id_vars="segment",
                                    var_name="programs", value_name="share")
    fig = px.bar(long, x="segment", y="share", color="programs",
                 color_discrete_sequence=["#003168", "#51B5E0", "#87AFBF", "#FCA311"],
                 text=long["share"].map(lambda v: f"{v:.0f}%" if v >= 3 else ""),
                 title=("How many DISTINCT programs mailed each client? — Jan-Apr 2026<br>"
                        "<sup>share of each group's clients by number of programs "
                        "(their own Cards/Loyalty scope) · groups mutually exclusive</sup>"))
    fig.update_layout(barmode="stack", yaxis_title="% of group's clients",
                      xaxis_title="", legend_title="programs",
                      template="plotly_white", height=480)
    fig.show()

# %% [markdown]
# ## OVERLAP deep-dive B — WHICH programs are these?
#
# Same groups, opened up by mnemonic: distinct clients mailed per program
# within each group (top 12 per group), colored by LOB, with each
# program's same-scope unsub rate labeled. Needs pm_overlap_mne.csv.

# %% [8] Program (mnemonic) composition per group
MNE_CSV = os.path.join(BASE, "pm_overlap_mne.csv")
if not os.path.exists(MNE_CSV):
    print("SKIP: pm_overlap_mne.csv missing - delete pm_overlap_results.csv "
          "and rerun cell [5] once.")
else:
    mc = pd.read_csv(MNE_CSV)
    mc = mc[(mc["mailed_cards"] == 1) | (mc["mailed_loy"] == 1)].copy()
    segname = {(1, 0): "Cards only", (0, 1): "Loyalty only", (1, 1): "Both"}
    mc["segment"] = mc.apply(lambda r: segname.get(
        (int(r["mailed_cards"]), int(r["mailed_loy"]))), axis=1)
    lobmap = (_frames["mapping"].assign(
        mne=lambda d: d[[c for c in d.columns if "MNEMONIC" in c.upper()][0]].str.strip(),
        lob=lambda d: d[[c for c in d.columns if "LOB" in c.upper()][0]].str.strip())
        [["mne", "lob"]])
    mc["mne"] = mc["mne"].str.strip()
    mc = mc.merge(lobmap, on="mne", how="left")
    mc = mc[mc["clients_mailed"] > 0].copy()   # drop cross-scope unsub-only rows (inf%)
    mc["unsub_rate"] = mc["clients_unsub"] / mc["clients_mailed"] * 100
    mc.loc[mc["clients_mailed"] < SMALL_BASE, "mne"] = (
        mc.loc[mc["clients_mailed"] < SMALL_BASE, "mne"] + " △")  # small-base badge
    segs = ["Cards only", "Loyalty only", "Both"]
    fig = make_subplots(rows=1, cols=3, subplot_titles=segs, shared_yaxes=False)
    for ci, seg in enumerate(segs, start=1):
        top = (mc[mc["segment"] == seg]
               .sort_values("clients_mailed", ascending=True).tail(12))
        fig.add_trace(go.Bar(
            x=top["clients_mailed"], y=top["mne"], orientation="h",
            marker_color=[lob_colors.get(l, "#899299") for l in top["lob"]],
            text=[f"{v:,.0f} · {r:.2f}%" for v, r in
                  zip(top["clients_mailed"], top["unsub_rate"])],
            textposition="outside", cliponaxis=False, showlegend=False), row=1, col=ci)
        fig.update_xaxes(tickformat="~s", row=1, col=ci,
                         range=[0, float(top["clients_mailed"].max()) * 1.45])
    fig.update_layout(
        title=("WHICH programs mailed each group — clients mailed per mnemonic, Jan-Apr 2026<br>"
               "<sup>label = clients mailed · that program's unsub rate within the group | "
               "color: dark blue = Cards LOB, tundra = Loyalty LOB | △ = <10K mailed (small base)</sup>"),
        template="plotly_white", height=520)
    fig.show()
