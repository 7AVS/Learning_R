# %% [markdown]
# # 44b — the same waterfall, Spark SQL surface (SQL text, HDFS underneath)
#
# Identical logic to pack 44, but the parquet snapshots are registered as TEMP VIEWS
# and every step is a visible SQL statement run by spark.sql(). Same engine, same
# data - just SQL instead of the DataFrame API. (UCP is not in Starburst/Trino;
# Spark SQL is the only SQL route to it.) Assumes live `spark` session.

# %% [0] parameters + register the two snapshots as SQL views
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"
MONTH_A  = "2026-01-31"
MONTH_B  = "2026-07-31"
FLAG     = "CPC_EM_ELIGIBLE"     # or CPC_ENT_ELIGIBLE

spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={MONTH_A}/").createOrReplaceTempView("ucp_a")
spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={MONTH_B}/").createOrReplaceTempView("ucp_b")
print("views registered: ucp_a, ucp_b")

# %% [1] The waterfall buckets - one SQL
wf = spark.sql(f"""
-- step 1: read each snapshot; the flag is stored as 1/0, this just reads it as a number
WITH a AS (SELECT CLNT_NO,
                  CAST(TRIM(CAST({FLAG} AS STRING)) = '1' AS INT) AS elig_a   -- 1 = emailable
           FROM ucp_a),
     b AS (SELECT CLNT_NO,
                  CAST(TRIM(CAST({FLAG} AS STRING)) = '1' AS INT) AS elig_b,
                  DT_OPENED                                                    -- when the client joined the bank
           FROM ucp_b)
-- step 2: line up every client across the two months (FULL OUTER = keep people who
-- exist in only one of them) and name what happened to each
SELECT CASE
         WHEN a.elig_a = 1 AND b.elig_b = 1 THEN 'stayed eligible (no bar)'
         WHEN a.elig_a = 1 AND b.elig_b = 0 THEN '- lost consent (flag 1->0)'
         WHEN a.elig_a = 1 AND b.CLNT_NO IS NULL THEN '- attrition (gone from B)'
         WHEN a.elig_a = 0 AND b.elig_b = 1 THEN '+ opened consent (flag 0->1)'
         WHEN a.CLNT_NO IS NULL AND b.elig_b = 1 AND b.DT_OPENED > DATE('{MONTH_A}')
              THEN '+ new to bank (opened after A)'
         WHEN a.CLNT_NO IS NULL AND b.elig_b = 1
              THEN '+ re-entered universe'
         ELSE 'no bar (other)'
       END AS bucket,
       COUNT(*) AS n_clients
FROM a FULL OUTER JOIN b ON a.CLNT_NO = b.CLNT_NO
GROUP BY 1
ORDER BY n_clients DESC
""").toPandas()
print(f"Waterfall components {MONTH_A} -> {MONTH_B}, flag = {FLAG}:")
print(wf.to_string(index=False))
print("\nCopy-paste block (tab-separated):")
print(wf.to_csv(sep="\t", index=False))

# %% [2] Identity check - SQL both sides
end_direct = spark.sql(f"""
SELECT COUNT(*) AS n FROM ucp_b
WHERE TRIM(CAST({FLAG} AS STRING)) = '1'
""").collect()[0]["n"]
g = lambda pat: int(wf.loc[wf["bucket"].str.contains(pat, regex=False), "n_clients"].sum())
start = g("stayed eligible") + g("- lost consent") + g("- attrition")
end   = start \
      + g("+ opened consent") + g("+ new to bank") + g("+ re-entered") \
      - g("- lost consent") - g("- attrition")
print(f"START {start:,} -> computed END {end:,} | measured END {end_direct:,} "
      f"| identity {'HOLDS' if end == end_direct else 'BROKEN'}")

# %% [3] Monthly chain - one SQL per consecutive pair (visible, parameterized)
import pandas as pd
MONTHS_CHAIN = ["2026-01-31", "2026-02-28", "2026-03-31", "2026-04-30",
                "2026-05-31", "2026-06-30", "2026-07-31"]

PAIR_SQL = """
-- same two-step shape as [1], one month against the next:
-- e0/e1 = was the client emailable last month / this month (flag stored as 1/0)
WITH m0 AS (SELECT CLNT_NO, CAST(TRIM(CAST({flag} AS STRING)) = '1' AS INT) AS e0
            FROM ucp_m0),
     m1 AS (SELECT CLNT_NO, CAST(TRIM(CAST({flag} AS STRING)) = '1' AS INT) AS e1,
                   DT_OPENED
            FROM ucp_m1)
-- each SUM counts one kind of movement between the two months
SELECT SUM(CASE WHEN m0.e0 = 0 AND m1.e1 = 1 THEN 1 ELSE 0 END)              AS opted_in,
       SUM(CASE WHEN m0.e0 = 1 AND m1.e1 = 0 THEN 1 ELSE 0 END)              AS lost_consent,
       SUM(CASE WHEN m0.e0 = 1 AND m1.CLNT_NO IS NULL THEN 1 ELSE 0 END)     AS attrition,
       SUM(CASE WHEN m0.CLNT_NO IS NULL AND m1.e1 = 1
                 AND m1.DT_OPENED > DATE('{m0}') THEN 1 ELSE 0 END)          AS new_to_bank,
       SUM(CASE WHEN m0.CLNT_NO IS NULL AND m1.e1 = 1
                 AND (m1.DT_OPENED <= DATE('{m0}') OR m1.DT_OPENED IS NULL)
                THEN 1 ELSE 0 END)                                           AS re_entered
FROM m0 FULL OUTER JOIN m1 ON m0.CLNT_NO = m1.CLNT_NO
"""

flows = []
for m0, m1 in zip(MONTHS_CHAIN[:-1], MONTHS_CHAIN[1:]):
    spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={m0}/").createOrReplaceTempView("ucp_m0")
    spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={m1}/").createOrReplaceTempView("ucp_m1")
    r = spark.sql(PAIR_SQL.format(flag=FLAG, m0=m0)).toPandas()
    r.insert(0, "month", m1[:7])
    flows.append(r)

fdf = pd.concat(flows, ignore_index=True)
print("Gross monthly flows (each month vs the previous):")
print(fdf.to_string(index=False))
print("\nCopy-paste block for PowerPoint (tab-separated):")
print(fdf.to_csv(sep="\t", index=False))

# %% [4] Deck-ready table - the waterfall as readable rows (paste into Excel/PowerPoint)
n_new, n_reent, n_open = g("+ new to bank"), g("+ re-entered"), g("+ opened consent")
n_lost, n_attr = g("- lost consent"), g("- attrition")
deck = pd.DataFrame([
    ["Emailable clients", MONTH_A, start, round(start/1e6, 2)],
    ["+ New to bank", f"{MONTH_A} → {MONTH_B}", n_new, round(n_new/1e6, 2)],
    ["+ Re-entered client base", f"{MONTH_A} → {MONTH_B}", n_reent, round(n_reent/1e6, 2)],
    ["+ Existing clients opting in", f"{MONTH_A} → {MONTH_B}", n_open, round(n_open/1e6, 2)],
    ["− Lost consent (unsubscribed)", f"{MONTH_A} → {MONTH_B}", -n_lost, round(-n_lost/1e6, 2)],
    ["− Client attrition", f"{MONTH_A} → {MONTH_B}", -n_attr, round(-n_attr/1e6, 2)],
    ["Emailable base", MONTH_B, end, round(end/1e6, 2)],
], columns=["element", "period", "clients", "clients_MM"])
print(deck.to_string(index=False))
print("\nCopy-paste block (tab-separated):")
print(deck.to_csv(sep="\t", index=False))

# %% [5] The waterfall chart - mock style (unchanged from pack 44)
import matplotlib.pyplot as plt

navy   = "#16436e"
blues  = ["#2a78d6", "#7fb2e6", "#bcd7f2"]
ambers = ["#e08214", "#f5c26b"]
grey   = "#8a8f98"

sub_parts = [("New to bank", n_new), ("Re-entered", n_reent), ("Opted in", n_open)]
uns_parts = [("Client attrition", n_attr), ("Lost consent", n_lost)]
adds_total, drops_total = sum(v for _, v in sub_parts), sum(v for _, v in uns_parts)

lo = min(start, end) * 0.955 / 1e6
fig, ax = plt.subplots(figsize=(11, 5.8))
ax.bar(0, start/1e6 - lo, bottom=lo, width=0.6, color=navy, zorder=3)
ax.text(0, start/1e6 + 0.03, f"{start/1e6:,.2f}", ha="center", fontsize=11, fontweight="bold")
ax.bar(3, end/1e6 - lo, bottom=lo, width=0.6, color=navy, zorder=3)
ax.text(3, end/1e6 + 0.03, f"{end/1e6:,.2f}", ha="center", fontsize=11, fontweight="bold")

base = start / 1e6
for (lbl, v), c in zip(sub_parts, blues):
    h = v / 1e6
    ax.bar(1, h, bottom=base, width=0.6, color=c, zorder=3,
           edgecolor="white", linewidth=1.5, label=lbl)
    if h > 0.015:
        ax.text(1, base + h/2, f"{h:.2f}", ha="center", va="center", fontsize=9.5)
    base += h
ax.text(1, base + 0.03, f"+{adds_total/1e6:.2f}", ha="center", fontsize=11, fontweight="bold")
top_after_adds = base

base = top_after_adds
for (lbl, v), c in zip(uns_parts, ambers):
    h = v / 1e6
    ax.bar(2, -h, bottom=base, width=0.6, color=c, zorder=3,
           edgecolor="white", linewidth=1.5, label=lbl)
    if h > 0.015:
        ax.text(2, base - h/2, f"-{h:.2f}", ha="center", va="center", fontsize=9.5)
    base -= h
ax.text(2, top_after_adds + 0.03, f"-{drops_total/1e6:.2f}", ha="center", fontsize=11,
        fontweight="bold")

ax.plot([0.3, 0.7], [start/1e6]*2, ls=":", lw=1.2, color=grey)
ax.plot([1.3, 1.7], [top_after_adds]*2, ls=":", lw=1.2, color=grey)
ax.plot([2.3, 2.7], [(top_after_adds - drops_total/1e6)]*2, ls=":", lw=1.2, color=grey)
ax.set_xticks([0, 1, 2, 3])
ax.set_xticklabels([f"Emailable\nclients\n{MONTH_A[:7]}", "Subscribes",
                    "Unsubscribes", f"Emailable\nbase\n{MONTH_B[:7]}"], fontsize=10)
ax.set_ylabel("# clients in MM")
ax.set_ylim(lo, top_after_adds * 1.012)
ax.spines[["top", "right"]].set_visible(False)
ax.text(-0.68, lo, "≈", fontsize=14, color="#444444", va="center")
ax.legend(loc="upper left", fontsize=9, frameon=False)
ax.set_title(f"Emailable base waterfall — {MONTH_A} to {MONTH_B}  (flag: {FLAG})",
             fontweight="bold", fontsize=12, loc="left")
plt.tight_layout(); plt.show()

# %% [6] Monthly gross-flows chart (unchanged from pack 44; data table printed in [3])
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
