# %% [0] 32 — CPC 1012 flip vs last email decision: plots
# Reads the two CSVs saved from 32_cpc_1012_last_email_gap.sql.
# Read rule: attribution needs a tower at day 0-1. Flat spread = cadence coincidence.
import pandas as pd
import matplotlib.pyplot as plt

BUCKETS = "32_cpc_1012_gap_by_month.csv"   # OUTPUT A
DAYS    = "32_cpc_1012_gap_days.csv"       # OUTPUT B

bk = pd.read_csv(BUCKETS)
dy = pd.read_csv(DAYS)
display(bk.pivot_table(index="flip_month", columns="gap_bucket",
                       values="n_clients", aggfunc="sum", fill_value=0))

# %% [1] rollup table — the decision view
roll = bk.groupby("gap_bucket", as_index=False)["n_clients"].sum()
roll["share_pct"] = (roll["n_clients"] / roll["n_clients"].sum() * 100).round(1)
display(roll)

# %% [2] day-level histogram 0-90 — the chart that decides it
fig, ax = plt.subplots(figsize=(12, 5))
ax.bar(dy["gap_days"], dy["n_clients"], width=0.9, color="#2a78d6")
if not dy[dy["gap_days"] <= 1].empty:
    d01 = dy[dy["gap_days"] <= 1]["n_clients"].sum()
    ax.annotate(f"day 0-1: {d01:,}", xy=(1, dy[dy['gap_days'] <= 1]['n_clients'].max()),
                xytext=(8, dy["n_clients"].max() * 0.9), fontsize=11, fontweight="bold",
                arrowprops=dict(arrowstyle="->", color="#52514e"))
ax.set_xlabel("days from last email decision to the 1012 change")
ax.set_ylabel("clients")
ax.set_title("Most recent 1012 change to No (18 mo) — days since last email decision\n"
             "If email clicks drove the change, day 0-1 towers. Flat = contact-cadence coincidence.",
             fontweight="bold")
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# %% [3] bucket bar — meeting version
order = ["1_same_or_next_day", "2_within_week", "3_within_month",
         "4_within_quarter", "5_over_90_days", "6_no_email_found"]
labels = ["same/next day", "2-7 days", "8-30 days", "31-90 days", ">90 days", "no email found"]
r = roll.set_index("gap_bucket").reindex(order)
fig, ax = plt.subplots(figsize=(10, 5))
ax.barh(labels[::-1], r["n_clients"][::-1], color="#2a78d6")
for i, v in enumerate(r["n_clients"][::-1]):
    if pd.notna(v):
        ax.text(v, i, f" {int(v):,} ({r['share_pct'][::-1].iloc[i]}%)", va="center", fontsize=10)
ax.set_xlabel("clients")
ax.set_title("Where the last email decision sits relative to the 1012 change", fontweight="bold")
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()
