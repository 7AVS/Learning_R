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
display(wf)

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
display(summary)
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
display(deck)

# %% [5] The waterfall chart - mock style: ONE Subscribes bar and ONE Unsubscribes bar,
# each stacked by component, segment labels on the stacks
import matplotlib.pyplot as plt

navy      = "#16436e"   # endpoints
blues     = ["#2a78d6", "#7fb2e6", "#bcd7f2"]   # subscribe components (one hue, stepped)
ambers    = ["#e08214", "#f5c26b"]              # unsubscribe components
grey      = "#8a8f98"

sub_parts = [("New to bank", n_new), ("Re-entered", n_reent), ("Opted in", n_open)]
uns_parts = [("Client attrition", n_attr), ("Lost consent", n_lost)]
adds_total, drops_total = sum(v for _, v in sub_parts), sum(v for _, v in uns_parts)

lo = min(start, end) * 0.955 / 1e6
fig, ax = plt.subplots(figsize=(11, 5.8))

# endpoint bars
ax.bar(0, start/1e6 - lo, bottom=lo, width=0.6, color=navy, zorder=3)
ax.text(0, start/1e6 + 0.03, f"{start/1e6:,.2f}", ha="center", fontsize=11, fontweight="bold")
ax.bar(3, end/1e6 - lo, bottom=lo, width=0.6, color=navy, zorder=3)
ax.text(3, end/1e6 + 0.03, f"{end/1e6:,.2f}", ha="center", fontsize=11, fontweight="bold")

# Subscribes: stacked upward from START
base = start / 1e6
for (lbl, v), c in zip(sub_parts, blues):
    h = v / 1e6
    ax.bar(1, h, bottom=base, width=0.6, color=c, zorder=3,
           edgecolor="white", linewidth=1.5, label=lbl)
    if h > 0.015:
        ax.text(1, base + h/2, f"{h:.2f}", ha="center", va="center", fontsize=9.5,
                color="#222222")
    base += h
ax.text(1, base + 0.03, f"+{adds_total/1e6:.2f}", ha="center", fontsize=11, fontweight="bold")
top_after_adds = base

# Unsubscribes: stacked downward from the post-adds level
base = top_after_adds
for (lbl, v), c in zip(uns_parts, ambers):
    h = v / 1e6
    ax.bar(2, -h, bottom=base, width=0.6, color=c, zorder=3,
           edgecolor="white", linewidth=1.5, label=lbl)
    if h > 0.015:
        ax.text(2, base - h/2, f"-{h:.2f}", ha="center", va="center", fontsize=9.5,
                color="#222222")
    base -= h
ax.text(2, top_after_adds + 0.03, f"-{drops_total/1e6:.2f}", ha="center", fontsize=11,
        fontweight="bold")

# dotted connectors
ax.plot([0.3, 0.7], [start/1e6]*2, ls=":", lw=1.2, color=grey)
ax.plot([1.3, 1.7], [top_after_adds]*2, ls=":", lw=1.2, color=grey)
ax.plot([2.3, 2.7], [(top_after_adds - drops_total/1e6)]*2, ls=":", lw=1.2, color=grey)

ax.set_xticks([0, 1, 2, 3])
ax.set_xticklabels([f"Emailable\nclients\n{MONTH_A[:7]}", "Subscribes",
                    "Unsubscribes", f"Emailable\nbase\n{MONTH_B[:7]}"], fontsize=10)
ax.set_ylabel("# clients in MM")
ax.set_ylim(lo, (top_after_adds) * 1.012)
ax.spines[["top", "right"]].set_visible(False)
ax.text(-0.68, lo, "≈", fontsize=14, color="#444444", va="center")
ax.legend(loc="upper left", fontsize=9, frameon=False)
ax.set_title(f"Emailable base waterfall — {MONTH_A} to {MONTH_B}  (flag: {FLAG})",
             fontweight="bold", fontsize=12, loc="left")
plt.tight_layout(); plt.show()

# %% [6] THE PATH - the same flow components computed month by month, not A-to-B
# A-to-B nets everything (a client flipping off and back inside the window vanishes).
# This cell chains CONSECUTIVE month pairs and measures each month's gross flows:
# new-to-bank, re-entered, opted in / lost consent, attrition. The monthly sums will
# NOT equal the A-to-B bars - that difference = within-period churn, and it is real.
MONTHS_CHAIN = ["2026-01-31", "2026-02-28", "2026-03-31", "2026-04-30",
                "2026-05-31", "2026-06-30", "2026-07-31"]   # <- edit to taste

snap = {}
for m in MONTHS_CHAIN:
    snap[m] = load_month(m).cache()

flows = []
for m0, m1 in zip(MONTHS_CHAIN[:-1], MONTHS_CHAIN[1:]):
    p = (snap[m0].select("CLNT_NO", col("elig").alias("e0"))
          .join(snap[m1].select(["CLNT_NO", col("elig").alias("e1")] +
                                (["DT_OPENED"] if has_dt else [])),
                "CLNT_NO", "full_outer"))
    agg = p.agg(
        F.sum(F.when((col("e0") == 0) & (col("e1") == 1), 1).otherwise(0)).alias("opted_in"),
        F.sum(F.when((col("e0") == 1) & (col("e1") == 0), 1).otherwise(0)).alias("lost_consent"),
        F.sum(F.when((col("e0") == 1) & col("e1").isNull(), 1).otherwise(0)).alias("attrition"),
        F.sum(F.when(col("e0").isNull() & (col("e1") == 1) &
                     (col("DT_OPENED") > F.lit(m0)) if has_dt
                     else col("e0").isNull() & (col("e1") == 1), 1).otherwise(0)).alias("new_to_bank"),
        F.sum(F.when(col("e0").isNull() & (col("e1") == 1) &
                     ((col("DT_OPENED") <= F.lit(m0)) | col("DT_OPENED").isNull()) if has_dt
                     else F.lit(False), 1).otherwise(0)).alias("re_entered"),
    ).toPandas()
    agg.insert(0, "month", m1[:7])
    flows.append(agg)

fdf = pd.concat(flows, ignore_index=True)
print("Gross monthly flows (each month vs the previous one):")
display(fdf)
print("\nSums over the chain vs the A-to-B waterfall (difference = within-period churn):")
display(fdf[["opted_in", "lost_consent", "attrition", "new_to_bank", "re_entered"]].sum().to_frame('total'))

fig, ax = plt.subplots(figsize=(11, 4.8))
x = range(len(fdf))
pos_bottom = [0]*len(fdf)
for lbl, colname, c in [("New to bank", "new_to_bank", blues[0]),
                        ("Re-entered", "re_entered", blues[1]),
                        ("Opted in", "opted_in", blues[2])]:
    vals = (fdf[colname] / 1e3).tolist()
    ax.bar(x, vals, bottom=pos_bottom, width=0.6, color=c, edgecolor="white",
           linewidth=1, label=lbl, zorder=3)
    pos_bottom = [a + b for a, b in zip(pos_bottom, vals)]
neg_bottom = [0]*len(fdf)
for lbl, colname, c in [("Client attrition", "attrition", ambers[0]),
                        ("Lost consent", "lost_consent", ambers[1])]:
    vals = (-fdf[colname] / 1e3).tolist()
    ax.bar(x, vals, bottom=neg_bottom, width=0.6, color=c, edgecolor="white",
           linewidth=1, label=lbl, zorder=3)
    neg_bottom = [a + b for a, b in zip(neg_bottom, vals)]
ax.axhline(0, color="#444444", lw=1)
ax.set_xticks(list(x)); ax.set_xticklabels(fdf["month"], fontsize=10)
ax.set_ylabel("clients (thousands)")
ax.legend(loc="upper left", fontsize=9, frameon=False, ncol=2)
ax.spines[["top", "right"]].set_visible(False)
ax.set_title(f"Monthly gross flows of the emailable base  (flag: {FLAG})",
             fontweight="bold", fontsize=12, loc="left")
plt.tight_layout(); plt.show()
