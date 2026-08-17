# %% [markdown]
# # 44 — the emailable-base waterfall, UCP source (view 1 of 2)
#
# Two snapshots, client-level transitions, five buckets:
#   START eligible (month A)
#   + existing clients who OPENED consent (in both months, flag 0 -> 1)
#   + NEW arrivals eligible in B (absent in A; split new-to-bank vs re-entered by dt_opened)
#   - existing clients who LOST consent (in both months, flag 1 -> 0)
#   - ATTRITION (eligible in A, absent from B)
#   = END eligible (month B)
# One bar each, no LOB colors. Flag parameterized: CPC_EM_ELIGIBLE (email consent view,
# default) vs CPC_ENT_ELIGIBLE (the mock's enterprise flag) - one-line switch.
# CPC-MTHLY-sourced view 2 comes as its own pack once "active RB personal client" is
# defined there. Assumes live `spark` session.

# %% [0] parameters
from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim

UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"
MONTH_A  = "2025-01-31"          # start endpoint (mock window)
MONTH_B  = "2026-07-31"          # end endpoint
FLAG     = "CPC_EM_ELIGIBLE"     # or "CPC_ENT_ELIGIBLE" to reproduce the mock's number

# %% [1] Load the two snapshots, slim
def load_month(m):
    d = spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={m}/")
    cols = ["CLNT_NO", FLAG] + [c for c in d.columns if c.upper() == "DT_OPENED"]
    return (d.select([col(c) for c in cols])
              .withColumn("elig", (trim(col(FLAG).cast("string")) == "1").cast("int"))
              .drop(FLAG))

a = load_month(MONTH_A).withColumnRenamed("elig", "elig_a")
b = load_month(MONTH_B).withColumnRenamed("elig", "elig_b")
has_dt = "DT_OPENED" in [c.upper() for c in b.columns]
print(f"{MONTH_A}: {a.count():,} clients | {MONTH_B}: {b.count():,} clients | dt_opened available: {has_dt}")

# %% [2] The waterfall - client-level transitions between the two snapshots
j = (a.select("CLNT_NO", "elig_a")
      .join(b.select(["CLNT_NO", "elig_b"] + (["DT_OPENED"] if has_dt else [])),
            "CLNT_NO", "full_outer"))

j = j.withColumn("bucket",
    F.when((col("elig_a") == 1) & (col("elig_b") == 1), "stayed eligible (no bar)")
     .when((col("elig_a") == 1) & (col("elig_b") == 0), "- lost consent (in both months, flag 1->0)")
     .when((col("elig_a") == 1) & col("elig_b").isNull(), "- attrition (eligible in A, gone from B)")
     .when((col("elig_a") == 0) & (col("elig_b") == 1), "+ opened consent (in both months, flag 0->1)")
     .when(col("elig_a").isNull() & (col("elig_b") == 1),
           F.when(col("DT_OPENED") > F.lit(MONTH_A), "+ new to bank (opened after A, eligible in B)")
            .otherwise("+ re-entered universe (existing client, absent in A)") if has_dt
           else F.lit("+ new arrival (absent in A, eligible in B)"))
     .when((col("elig_a") == 0) & col("elig_b").isNull(), "ineligible in A, gone from B (no bar)")
     .when(col("elig_a").isNull() & (col("elig_b") == 0), "arrived ineligible (no bar)")
     .otherwise("stayed ineligible (no bar)"))

wf = j.groupBy("bucket").count().orderBy(F.desc("count")).toPandas()
print(f"Waterfall components, {MONTH_A} -> {MONTH_B}, flag = {FLAG}:")
print(wf.to_string(index=False))

# %% [3] The identity check - start + adds - drops must equal end
import pandas as pd
get_n = lambda pat: int(wf.loc[wf["bucket"].str.contains(pat, regex=False), "count"].sum())
start = get_n("stayed eligible") + get_n("- lost consent") + get_n("- attrition")
adds  = sum(get_n(p) for p in ["+ opened consent", "+ new to bank", "+ re-entered", "+ new arrival"])
drops = get_n("- lost consent") + get_n("- attrition")
end   = start + adds - drops
summary = pd.DataFrame([
    {"waterfall element": f"START eligible @ {MONTH_A}", "n_clients": start},
    {"waterfall element": "+ opened consent (existing)", "n_clients": get_n("+ opened consent")},
    {"waterfall element": "+ new to bank / arrivals", "n_clients": adds - get_n("+ opened consent")},
    {"waterfall element": "- lost consent", "n_clients": -get_n("- lost consent")},
    {"waterfall element": "- attrition", "n_clients": -get_n("- attrition")},
    {"waterfall element": f"END eligible @ {MONTH_B} (computed)", "n_clients": end},
])
print(summary.to_string(index=False))
direct_end = int(b.filter(col("elig_b") == 1).count())
print(f"\nEND measured directly in {MONTH_B}: {direct_end:,} - identity {'HOLDS' if direct_end == end else 'BROKEN - investigate before using'}")

# %% [4] Deck-ready table - tab-separated, paste straight into Excel/PowerPoint
n_new   = get_n("+ new to bank")
n_reent = get_n("+ re-entered") + get_n("+ new arrival") - get_n("+ new to bank") \
          if not has_dt else get_n("+ re-entered")
n_open  = get_n("+ opened consent")
n_lost  = get_n("- lost consent")
n_attr  = get_n("- attrition")

deck = pd.DataFrame([
    ["Emailable clients", MONTH_A, start, round(start/1e6, 2)],
    ["+ New to bank", f"{MONTH_A} → {MONTH_B}", n_new, round(n_new/1e6, 2)],
    ["+ Re-entered client base", f"{MONTH_A} → {MONTH_B}", n_reent, round(n_reent/1e6, 2)],
    ["+ Existing clients opting in", f"{MONTH_A} → {MONTH_B}", n_open, round(n_open/1e6, 2)],
    ["− Lost consent (unsubscribed)", f"{MONTH_A} → {MONTH_B}", -n_lost, round(-n_lost/1e6, 2)],
    ["− Client attrition", f"{MONTH_A} → {MONTH_B}", -n_attr, round(-n_attr/1e6, 2)],
    ["Emailable base", MONTH_B, end, round(end/1e6, 2)],
], columns=["element", "period", "clients", "clients_MM"])
print("Copy-paste block (tab-separated):")
print(deck.to_csv(sep="\t", index=False))

# %% [5] The waterfall chart - deck style (broken axis, MM labels, dotted connectors)
import matplotlib.pyplot as plt

navy, blue, lightblue, amber, grey = "#16436e", "#2a78d6", "#7fb2e6", "#e08214", "#8a8f98"

adds_total  = n_new + n_reent + n_open
bars = [
    ("Emailable\nclients\n" + MONTH_A[:7], start,       None,   "endpoint"),
    ("Subscribes",                          adds_total,  start,  "add"),
    ("Lost\nconsent",                      -n_lost,      start + adds_total, "drop"),
    ("Client\nattrition",                  -n_attr,      start + adds_total - n_lost, "drop"),
    ("Emailable\nbase\n" + MONTH_B[:7],     end,         None,   "endpoint"),
]

lo = min(start, end) * 0.955 / 1e6      # broken-axis floor just under the smallest bar
fig, ax = plt.subplots(figsize=(11, 5.5))
xpos = range(len(bars))
running_tops = []
for i, (label, val, base_abs, kind) in enumerate(bars):
    v = val / 1e6
    if kind == "endpoint":
        ax.bar(i, v - lo, bottom=lo, width=0.62, color=navy, zorder=3)
        ax.text(i, v + 0.03, f"{v:,.2f}", ha="center", va="bottom",
                fontsize=11, fontweight="bold", color="#222222")
        running_tops.append(v)
    else:
        base = base_abs / 1e6
        top = base + v
        colr = blue if kind == "add" else amber
        ax.bar(i, v, bottom=base, width=0.62, color=colr, zorder=3)
        ax.text(i, max(base, top) + 0.03, f"{v:+.2f}", ha="center", va="bottom",
                fontsize=11, fontweight="bold", color="#222222")
        running_tops.append(top)
# dotted connectors between consecutive bar tops
for i in range(len(bars) - 1):
    y = running_tops[i] if bars[i][3] != "drop" else (bars[i][1] + bars[i][2]) / 1e6
    y_next_base = y
    ax.plot([i + 0.31, i + 1 - 0.31], [y, y], ls=":", lw=1.2, color=grey, zorder=2)

ax.set_xticks(list(xpos))
ax.set_xticklabels([b[0] for b in bars], fontsize=10)
ax.set_ylabel("# clients in MM")
ax.set_ylim(lo, max(running_tops) * 1.012)
ax.spines[["top", "right"]].set_visible(False)
# axis-break glyph
ax.text(-0.68, lo, "≈", fontsize=14, color="#444444", va="center")
ax.set_title(f"Emailable base waterfall — {MONTH_A} to {MONTH_B}  (flag: {FLAG})",
             fontweight="bold", fontsize=12, loc="left")
ax.text(0.99, -0.16, "Subscribes = new to bank + re-entered + existing opt-ins. Source: UCP monthly snapshots.",
        transform=ax.transAxes, ha="right", fontsize=8.5, color="#555555")
plt.tight_layout(); plt.show()
