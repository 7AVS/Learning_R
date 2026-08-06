# unsub_analysis_notebook.py — repo-owned rebuild of the CSV analysis
# notebook. SELF-CONTAINED: cold start needs only (1) Python with duckdb,
# pandas, numpy, matplotlib; (2) the delivered CSVs in ONE folder. Runs on
# the pod (jovyan) or a local machine — set BASE below, nothing else.
# Chart conventions follow spotlight/plot_revision_prompt.py G1-G8:
# percent format w/ 2 decimals, n on every rate, small-base guard <10k,
# say WHICH Cards definition, no causal language, 9.2% no-match shown.

# %% [0] Setup + data load (DuckDB views over the delivered CSVs)
import os
import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

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

fig, axes = plt.subplots(3, 2, figsize=(16, 14), sharex=True, sharey=True)
axes = axes.flatten()
for ax, lob in zip(axes, lob_order):
    sub = df0[df0["lob_manual"] == lob].set_index("ym").reindex(months)
    ax.bar(mpos, sub["sends"].fillna(0), color=C_THEN, alpha=0.7, edgecolor="white")
    tot_s, tot_u = sub["sends"].sum(), sub["unsubs"].sum()
    ax.set_title(f"{lob}  (total sends: {compact_n(tot_s)} | avg unsub rate: "
                 f"{tot_u * 100.0 / tot_s:.2f}%)", fontweight="bold")
    ax.set_ylabel("Sends"); ax.set_ylim(0, ymax_sends)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x/1e6:.1f}M"))
    ax.tick_params(axis="y", labelleft=True)
    ax.spines["top"].set_visible(False); ax.spines["right"].set_visible(False)
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
             "(bars = delivered emails; line = unsubs per delivered email %  |  "
             "LOB from Andre's mapping file)", fontsize=12, fontweight="bold", y=0.995)
plt.tight_layout(rect=[0, 0, 1, 0.97]); plt.show()

# %% [markdown]
# ---
# # Stakeholder follow-up — three asks (2026-08-06)
#
# Feedback on the spotlight asked us to narrow focus to three things:
#
# 1. **Loyalty x Cards overlap** — is unsub% higher when a client gets both
#    Loyalty and Cards mail vs one alone? *Status: pending — needs one new
#    client-grain pull keyed on the LOYALTY rows of the mapping file. Not
#    in this notebook yet.*
# 2. **Attrition** — do unsubscribers leave Cards and/or the bank more
#    than stayers? *Answered below (cell [PM-2]).*
# 3. **Profit population check** — the original then->now profit compared
#    only clients present at BOTH anchors. If more leavers than stayers
#    drop out between anchors, the surviving remnant is better-selected
#    and the comparison is biased. *Answered below (cell [PM-1]) by
#    holding the THEN population fixed.*
#
# **Definitions used throughout** (same as the delta section):
# cohort = 4,783,193 clients mailed by a Cards campaign on/before
# Jun 30 2025 · "leaver" = client with a Cards-marketing unsubscribe by
# that anchor (NOT account closure, NOT attrition) · then = Jun 30 2025,
# now = Jun 30 2026 · profit = UCP annual estimate · clients with no UCP
# match are shown explicitly, never silently dropped.

# %% [markdown]
# ## PM-1 — Profit, recomputed on a fixed population
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
# even though the profit of those who stay does not. That feeds PM-2.

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
    fig, axes = plt.subplots(1, 2, figsize=(13, 6), sharey=True)
    ymax = 0
    for ax, (label, then_col, now_col) in zip(axes, [
            ("(a) ORIGINAL basis — survivors only", "avg_then_survivors", "avg_now_survivors"),
            ("(b) FIXED basis — everyone kept, vanished = $0", "avg_then_all", "avg_now_zerofill")]):
        grps = ["stayer", "leaver"]
        x = np.arange(len(grps)); w = 0.36
        thin = [prof.loc[g, then_col] for g in grps]
        now = [prof.loc[g, now_col] for g in grps]
        ax.bar(x - w/2, thin, w, color=C_THEN, label="avg profit Jun 2025")
        ax.bar(x + w/2, now, w, color=C_NOW, label="avg profit Jun 2026")
        for xi, (t, n) in zip(x, zip(thin, now)):
            ax.text(xi - w/2, t + 12, f"${t:,.0f}", ha="center", va="bottom",
                    fontsize=9, fontweight="bold")
            ax.text(xi + w/2, n + 12, f"${n:,.0f}", ha="center", va="bottom",
                    fontsize=9, fontweight="bold")
            d, dp = n - t, (n - t) / t * 100
            ax.text(xi, max(t, n) * 1.16,
                    f"{'+' if d >= 0 else ''}${d:,.0f}  ({dp:+.1f}%)",
                    ha="center", fontsize=10.5,
                    color=C_POS if d >= 0 else C_LINE, fontweight="bold")
        ymax = max(ymax, max(now + thin))
        ns = [int(prof.loc[g, "n_then_matched"]) for g in grps]
        ax.set_xticks(x)
        ax.set_xticklabels([f"{g.upper()}\nn = {n:,}" for g, n in zip(grps, ns)])
        ax.set_title(label, fontweight="bold", fontsize=11, pad=10)
        ax.spines["top"].set_visible(False); ax.spines["right"].set_visible(False)
    for ax in axes:
        ax.set_ylim(0, ymax * 1.32)   # headroom so labels never collide
    axes[0].set_ylabel("avg annual profit estimate ($, UCP)")
    axes[0].legend(frameon=False, loc="upper left")
    fig.suptitle("PM-1: Did unsubscribers' profit really grow? Same data, two ways of counting",
                 fontsize=12.5, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.94]); plt.show()
    print("HOW TO READ: panel (a) is the original math - it silently drops"
          " clients who vanished by 2026. Panel (b) keeps every June-2025"
          " client; vanished ones count as $0 now. The story holds in (b):"
          " both groups grow, leavers slightly faster in % terms.")

# %% [markdown]
# ## PM-2 — Do unsubscribers actually leave more? Yes, on every cut.
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
# stayers. **Right chart** = the population ledger behind PM-1: of each
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
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.5))
    # LEFT — exit rates among June-2025 cardholders
    grps = ["stayer", "leaver"]
    lost = [att.loc[g, "lost_cards_now"] / att.loc[g, "held_cards_then"] * 100 for g in grps]
    van  = [att.loc[g, "vanished_from_ucp_now"] / att.loc[g, "held_cards_then"] * 100 for g in grps]
    x = np.arange(len(grps)); w = 0.36
    axes[0].bar(x - w/2, lost, w, color=C_THEN, label="no cards anymore (still a client)")
    axes[0].bar(x + w/2, van, w, color=C_LINE, label="gone from the data (left-bank PROXY)")
    for xi, (l, v) in zip(x, zip(lost, van)):
        axes[0].text(xi - w/2, l + 0.03, f"{l:.2f}%", ha="center", va="bottom",
                     fontsize=9.5, fontweight="bold")
        axes[0].text(xi + w/2, v + 0.03, f"{v:.2f}%", ha="center", va="bottom",
                     fontsize=9.5, fontweight="bold")
    ns = [int(att.loc[g, "held_cards_then"]) for g in grps]
    axes[0].set_xticks(x)
    axes[0].set_xticklabels([f"{g.upper()}\nheld cards Jun 2025: n = {n:,}"
                             for g, n in zip(grps, ns)])
    axes[0].set_ylim(0, max(lost + van) * 1.35)
    axes[0].set_ylabel("% of Jun-2025 cardholders")
    axes[0].set_title("Of clients who HAD cards in Jun 2025,\nwho exited by Jun 2026?",
                      fontweight="bold", fontsize=11, pad=8)
    axes[0].legend(frameon=False, fontsize=8.5, loc="upper left")
    # RIGHT — the ledger: where each group's Jun-2025 clients ended up
    bot = np.zeros(2)
    for col, colr, lab in [("matched_now", C_THEN, "still found in Jun 2026"),
                           ("vanished_now", C_LINE, "vanished by Jun 2026")]:
        vals = [float(led.loc[g, col]) for g in grps]
        axes[1].bar([g.upper() for g in grps], vals, bottom=bot, color=colr, label=lab)
        bot += np.array(vals)
    for i, g in enumerate(grps):
        mt = float(led.loc[g, "matched_then"])
        vn = float(led.loc[g, "vanished_now"])
        axes[1].text(i, bot[i], f"vanished: {vn:,.0f}\n({vn / mt * 100:.1f}%)",
                     ha="center", va="bottom", fontsize=9, fontweight="bold", color=C_LINE)
    axes[1].set_ylim(0, bot.max() * 1.25)
    axes[1].set_title("Where each group's Jun-2025 clients ended up\n(the counts behind PM-1's fix)",
                      fontweight="bold", fontsize=11, pad=8)
    axes[1].yaxis.set_major_formatter(FuncFormatter(lambda v_, _: compact_n(v_)))
    axes[1].legend(frameon=False, fontsize=8.5, loc="center right")
    for a in axes:
        a.spines["top"].set_visible(False); a.spines["right"].set_visible(False)
    fig.suptitle("PM-2: Do unsubscribers leave more? (descriptive — groups not matched)",
                 fontsize=12.5, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.93]); plt.show()
    print("HOW TO READ: left = among cardholders, exit is modestly higher"
          " for leavers on both cuts. Right = at whole-relationship level"
          " leavers vanish at 5.9% vs 2.6% - 2.2x the stayer rate.")

# %% [markdown]
# ## PM-3 — Loyalty x Cards overlap: is unsub% higher when a client gets both?
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

# %% [5] PM-3 pull — one aggregated cube, cached to CSV (pod + Teradata)
OVERLAP_CSV = os.path.join(BASE, "pm_overlap_results.csv")
if os.path.exists(OVERLAP_CSV):
    print(f"CACHED: {OVERLAP_CSV} exists - no Teradata pull. Delete it to re-pull.")
else:
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
               MAX(CASE WHEN e.mne IN (%(loy)s)   AND e.unsub = 1 THEN 1 ELSE 0 END) AS unsub_loy
        FROM ev e
        INNER JOIN ids i
           ON i.consumer_id_hashed = e.consumer_id_hashed
          AND i.TREATMENT_ID = e.TREATMENT_ID
        WHERE MOD(ABS(i.CLNT_NO), 10) = %(bite)d
        GROUP BY 1
    )
    SELECT mailed_cards, mailed_loy,
           COUNT(*) AS clients,
           SUM(unsub_cards) AS unsub_cards,
           SUM(unsub_loy) AS unsub_loy,
           SUM(CASE WHEN unsub_cards = 1 OR unsub_loy = 1 THEN 1 ELSE 0 END) AS unsub_either
    FROM cl
    GROUP BY 1, 2
    """
    _u = input("Enter your username: ")
    _p = getpass.getpass("Enter your password: ")
    EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com",
                              user=_u, password=_p, logmech="LDAP")
    parts = []
    for bite in range(10):
        q = _sql % {"cards": _in(CARDS_L), "loy": _in(LOY_L), "bite": bite}
        parts.append(pd.read_sql(q, EDW))
        print(f"bite {bite} done: {parts[-1]['clients'].sum():,.0f} clients")
    ov = (pd.concat(parts)
          .groupby(["mailed_cards", "mailed_loy"], as_index=False).sum())
    ov.to_csv(OVERLAP_CSV, index=False)
    print(f"WROTE {OVERLAP_CSV}:")
    print(ov)

# %% [6] PM-3 chart — unsub% by overlap segment
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
    x = np.arange(len(order)); w = 0.27
    fig, ax = plt.subplots(figsize=(10, 5.5))
    for off, col, colr, lab in [(-w, "unsub_cards", C_THEN, "unsub on a Cards list"),
                                (0.0, "unsub_loy", C_LINE, "unsub on a Loyalty list"),
                                (w, "unsub_either", C_NOW, "unsub on either")]:
        rate = ov[col] / ov["clients"] * 100
        ax.bar(x + off, rate, w, color=colr, label=lab)
        for xi, r_ in zip(x + off, rate):
            ax.text(xi, r_, f"{r_:.2f}%", ha="center", va="bottom",
                    fontsize=8.5, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{s}\n(n={int(ov.loc[s, 'clients']):,})" for s in order])
    ax.set_ylabel("unsubscribed in window (% of mailed clients)")
    ax.set_title("PM-3: Unsub rate by Loyalty x Cards mail overlap — Jan-Apr 2026\n"
                 "mailed = >=1 delivered send from that LOB (mapping file) | "
                 "unsub = >=1 list unsubscribe in window", fontweight="bold", fontsize=11)
    ax.legend(frameon=False)
    ax.spines["top"].set_visible(False); ax.spines["right"].set_visible(False)
    plt.tight_layout(); plt.show()
