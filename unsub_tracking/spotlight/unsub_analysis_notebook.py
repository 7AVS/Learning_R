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

# --- pick ONE base folder (pod / local) -------------------------------
BASE = os.path.expanduser("~/unsub_unified_out/")            # pod
# BASE = r"\\maple.fg.rbc.com\...\Cards\Unsubs\output"       # local share

SMALL_BASE = 10_000   # G3 small-base guard

# RBC brand palette (Section 4.2 Charts and Graphs)
C_THEN = "#003168"   # Dark Blue
C_NOW  = "#FCA311"   # Sunburst
C_LINE = "#51B5E0"   # Sky
C_POS  = "#AABA0A"   # Apple

def compact_n(v):
    v = float(v)
    if abs(v) >= 1_000_000: return f"{v/1_000_000:.1f}M"
    if abs(v) >= 1_000:     return f"{v/1_000:.1f}K"
    return f"{v:.0f}"

con = duckdb.connect()
for view, fname in [("a1", "a1_mne_share.csv"), ("a1_lob", "a1_lob_dedup.csv"),
                    ("a2", "a2_mne_rates.csv"), ("a3", "a3_contact_cube.csv"),
                    ("a4", "a4_profile_cube.csv"), ("b", "b_before_after_cube.csv"),
                    ("mapping", "mapping_mne.csv")]:
    con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_csv_auto('{os.path.join(BASE, fname)}')")

_cdf = pd.read_csv(os.path.join(BASE, "c_monthly_curve.csv"),
                   encoding="latin-1", on_bad_lines="skip")
_cdf.columns = ["mne", "ym", "sends", "unsubs_attributed"]
_cdf["sends"] = pd.to_numeric(_cdf["sends"], errors="coerce")
_cdf["unsubs_attributed"] = pd.to_numeric(_cdf["unsubs_attributed"], errors="coerce")
con.execute("CREATE OR REPLACE VIEW c AS SELECT * FROM _cdf")

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

# %% [2] S1 — Cards MNE x month heatmap (unsub counts, Cards LOB mapping)
s1 = """
SELECT TRIM(c.mne) AS mne, c.ym, SUM(c.unsubs_attributed) AS unsubs
FROM c JOIN mapping m ON TRIM(c.mne) = TRIM(m.MNEMONIC)
WHERE TRIM(m.LOB_Manual) = 'CARDS'
  AND c.ym BETWEEN '202508' AND '202606'
GROUP BY 1, 2
"""
df1 = con.execute(s1).df()
piv = (df1.pivot(index="mne", columns="ym", values="unsubs")
       .fillna(0).astype(int))
piv = piv.loc[piv.sum(axis=1).sort_values(ascending=False).index]

fig, ax = plt.subplots(figsize=(14, 0.45 * len(piv) + 2))
im = ax.imshow(piv.values, aspect="auto", cmap="YlOrRd")
ax.set_xticks(range(len(piv.columns))); ax.set_xticklabels(piv.columns, rotation=45, fontsize=8)
ax.set_yticks(range(len(piv.index)));  ax.set_yticklabels(piv.index, fontsize=8)
for r in range(piv.shape[0]):
    for cix in range(piv.shape[1]):
        v = piv.values[r, cix]
        if v > 0:
            ax.text(cix, r, compact_n(v), ha="center", va="center", fontsize=6.5,
                    color="white" if v > piv.values.max() * 0.6 else "#333")
ax.set_title("S1: Cards MNE x Month — Unsub Counts (Cards LOB, mapping file)\n"
             "Aug 2025 to Jun 2026 | One glance: who spiked when",
             fontweight="bold")
plt.tight_layout(); plt.show()

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
# **The concern (verbatim logic):** "then" population 4MM at $800 vs
# "now" 3MM at $1,000 is not a fair comparison — the missing 1MM took
# their profit with them.
#
# **What we did:** kept the THEN population fixed and computed the "now"
# average two ways: **(a) survivors only** — the original basis, shown
# for transparency; **(b) population-fixed** — clients who vanished from
# UCP by "now" stay in the denominator at $0.
#
# **Result (run 2026-08-06):** the finding survives the correction.
# Population-fixed: leavers $550 -> $688 (**+$138, +25.1%**), stayers
# $795 -> $982 (**+$187, +23.6%**). Profit rises for both groups on both
# bases; the survivors-only basis was mildly inflating both (the vanished
# averaged only $134 leaver / $183 stayer — low-value clients). Basis (b)
# is the number we report going forward.
#
# **What the check DID surface:** leavers vanish from UCP at **5.9% vs
# 2.6%** for stayers (2.2x) — see the ledger chart. That is a real
# difference in who disappears, and it feeds the attrition answer below.

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
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)
    for ax, (label, then_col, now_col) in zip(axes, [
            ("(a) survivors only — published basis", "avg_then_survivors", "avg_now_survivors"),
            ("(b) population-fixed — vanished at $0", "avg_then_all", "avg_now_zerofill")]):
        grps = ["stayer", "leaver"]
        x = np.arange(len(grps)); w = 0.38
        thin = [prof.loc[g, then_col] for g in grps]
        now = [prof.loc[g, now_col] for g in grps]
        ax.bar(x - w/2, thin, w, color=C_THEN, label="then (Jun 2025)")
        ax.bar(x + w/2, now, w, color=C_NOW, label="now (Jun 2026)")
        for xi, (t, n) in zip(x, zip(thin, now)):
            ax.text(xi - w/2, t, f"${t:,.0f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
            ax.text(xi + w/2, n, f"${n:,.0f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
            d = n - t
            ax.text(xi, max(t, n) * 1.08, f"Δ {'+' if d >= 0 else ''}${d:,.0f}",
                    ha="center", fontsize=10, color=C_POS if d >= 0 else "#B00020",
                    fontweight="bold")
        ns = [int(prof.loc[g, "n_then_matched"]) for g in grps]
        ax.set_xticks(x)
        ax.set_xticklabels([f"{g}\n(n={n:,})" for g, n in zip(grps, ns)])
        ax.set_title(label, fontweight="bold", fontsize=10)
        ax.spines["top"].set_visible(False); ax.spines["right"].set_visible(False)
    axes[0].set_ylabel("avg annual profit estimate ($, UCP)")
    axes[0].legend(frameon=False)
    fig.suptitle("PM ask: profit then->now on two population bases — stayers vs leavers\n"
                 "'leaver' = Cards-marketing unsub by Jun 30 2025 anchor, NOT attrition | "
                 "UCP-matched clients; no-match shown in ledger below",
                 fontsize=11, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.93]); plt.show()

# %% [markdown]
# ## PM-2 — Do unsubscribers leave more? Yes, on every cut.
#
# Base: clients holding the card category at "then". Two exits measured:
# **lost the card category** (still visible in UCP, no cards now) and
# **vanished from UCP entirely** (no record at "now" — a left-the-bank
# PROXY; confirming true bank exit needs a relationship-status source we
# don't have in this data).
#
# **Result (run 2026-08-06):** leavers lose the card category at
# **1.91% vs 1.63%** for stayers (x1.17) and vanish at **1.74% vs 1.30%**
# (x1.34) — combined ~3.7% vs ~2.9%. At the whole-relationship level the
# gap is wider: 5.9% vs 2.6% vanished (ledger). Unsubscribers are not
# just going deaf — a measurably larger share of them is drifting out.
#
# **Caveat that stays attached:** descriptive, not causal. Leavers skew
# younger and 4-7yr tenure; some of this gap is who they are, not what
# the unsubscribe did. Groups are not matched here.

# %% [4] PM ask #2 — card attrition + population ledger (needs pm CSV)
if not HAS_PM:
    print("SKIP: pm_asks_results.csv not in BASE - see cell [3] note.")
else:
    led = pmw.loc["population_ledger"]
    att = pmw.loc["card_attrition"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    # left: attrition rates
    grps = ["stayer", "leaver"]
    lost = [att.loc[g, "lost_cards_now"] / att.loc[g, "held_cards_then"] * 100 for g in grps]
    van  = [att.loc[g, "vanished_from_ucp_now"] / att.loc[g, "held_cards_then"] * 100 for g in grps]
    x = np.arange(len(grps)); w = 0.38
    axes[0].bar(x - w/2, lost, w, color=C_THEN, label="lost card category (visible in UCP)")
    axes[0].bar(x + w/2, van, w, color=C_NOW, label="vanished from UCP (left-bank PROXY)")
    for xi, (l, v) in zip(x, zip(lost, van)):
        axes[0].text(xi - w/2, l, f"{l:.2f}%", ha="center", va="bottom", fontsize=9, fontweight="bold")
        axes[0].text(xi + w/2, v, f"{v:.2f}%", ha="center", va="bottom", fontsize=9, fontweight="bold")
    ns = [int(att.loc[g, "held_cards_then"]) for g in grps]
    axes[0].set_xticks(x)
    axes[0].set_xticklabels([f"{g}\n(held cards then, n={n:,})" for g, n in zip(grps, ns)])
    axes[0].set_ylabel("% of card-holders at then")
    axes[0].set_title("Card attrition Jun 2025 -> Jun 2026\n(descriptive - groups differ in age/tenure mix)",
                      fontweight="bold", fontsize=10)
    axes[0].legend(frameon=False, fontsize=8)
    # right: ledger
    bot = np.zeros(2)
    for col, colr, lab in [("matched_now", C_THEN, "matched at now"),
                           ("vanished_now", C_NOW, "vanished by now"),]:
        vals = [led.loc[g, col] for g in grps]
        axes[1].bar(grps, vals, bottom=bot, color=colr, label=lab)
        bot += np.array(vals, dtype=float)
    axes[1].set_title("Population ledger (then-cohort, UCP-matched)\nthe survivorship the (a) basis hides",
                      fontweight="bold", fontsize=10)
    axes[1].yaxis.set_major_formatter(FuncFormatter(lambda v_, _: compact_n(v_)))
    axes[1].legend(frameon=False, fontsize=8)
    for a in axes:
        a.spines["top"].set_visible(False); a.spines["right"].set_visible(False)
    plt.tight_layout(); plt.show()

# %% [markdown]
# ## PM-3 — Loyalty x Cards overlap: is unsub% higher when a client gets both?
#
# **The ask:** compare unsub% for clients mailed by BOTH Loyalty and Cards
# vs Cards-only vs Loyalty-only (Jan-Apr 2026).
#
# **Method:** no banked table holds client x mnemonic grain (the pipeline
# collapses it server-side by design), and the source vendor tables live
# in Teradata, not HDFS — so the fastest correct path on the pod is ONE
# aggregated Teradata pull that returns an 8-number cube; nothing
# client-grain ever leaves the server. Loyalty / Cards definitions come
# from the mapping file's LOB_Manual column (Andre's mapping) — the same
# definition every other chart in this notebook uses. The pull runs in
# 10 client-number bites (spool safety) and caches its result to
# pm_overlap_results.csv — reruns and local runs never touch Teradata.
#
# **Definitions:** mailed = >=1 delivered send (disposition 1) from that
# LOB's mnemonics in-window · unsub = >=1 unsubscribe (disposition 4) on
# that scope in-window · rates shown with their n (mailed clients).

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
