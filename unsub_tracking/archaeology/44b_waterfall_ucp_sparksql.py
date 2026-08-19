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
display(wf)

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
display(fdf)

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
display(deck)

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

# %% [7] ACTIVE_EMAIL_IND vs the consent flag - "consented but no live address" measured
# Question: how many clients hold email consent but no active email address (and the
# reverse)? Those consented-but-dead-address clients are NOT really targetable - if the
# count is material, the deck's emailable line should require BOTH.
xt = spark.sql(f"""
-- one snapshot (month B), every client cross-classified on the two indicators
SELECT CASE WHEN TRIM(CAST({FLAG} AS STRING)) = '1' THEN 'consent flag = 1'
            ELSE 'consent flag = 0' END                          AS consent_flag,
       CASE WHEN TRIM(CAST(ACTIVE_EMAIL_IND AS STRING)) = '1' THEN 'active email address'
            ELSE 'NO active email address' END                   AS address_status,
       COUNT(*) AS n_clients
FROM ucp_b
GROUP BY 1, 2
ORDER BY 1, 2
""").toPandas()
xt["pct_of_universe"] = (100 * xt["n_clients"] / xt["n_clients"].sum()).round(2)
print(f"Consent flag ({FLAG}) x active-address, {MONTH_B}:")
display(xt)

# %% [8] Product-count attrition - did 'gone' clients wind down first, and who sits at
# zero products while still in the snapshot (hidden attrition)?
pa = spark.sql(f"""
-- month A vs month B per client: eligibility transition x open-product transition.
-- 'gone' = no row in B. OPN_PROD_CNT 0 while still present = relationship emptied
-- but the record lingers - hidden attrition the disappearance test misses.
WITH a AS (SELECT CLNT_NO,
                  CAST(TRIM(CAST({FLAG} AS STRING)) = '1' AS INT) AS elig_a,
                  CASE WHEN OPN_PROD_CNT >= 1 THEN '1+' ELSE '0' END AS prod_a
           FROM ucp_a),
     b AS (SELECT CLNT_NO,
                  CAST(TRIM(CAST({FLAG} AS STRING)) = '1' AS INT) AS elig_b,
                  CASE WHEN OPN_PROD_CNT >= 1 THEN '1+' ELSE '0' END AS prod_b
           FROM ucp_b)
SELECT CASE WHEN a.elig_a = 1 AND b.CLNT_NO IS NULL      THEN 'attrition (gone from B)'
            WHEN a.elig_a = 1 AND b.elig_b = 0           THEN 'lost eligibility (still in B)'
            WHEN a.elig_a = 1 AND b.elig_b = 1           THEN 'stayed eligible'
            ELSE                                              'other' END AS eligibility_path,
       a.prod_a                                    AS open_products_month_A,
       COALESCE(b.prod_b, 'gone')                  AS open_products_month_B,
       COUNT(*)                                    AS n_clients
FROM a LEFT JOIN b ON a.CLNT_NO = b.CLNT_NO
WHERE a.elig_a = 1
GROUP BY 1, 2, 3
ORDER BY 1, 4 DESC
""").toPandas()
print(f"Eligible clients in {MONTH_A}: eligibility path x product-count path to {MONTH_B}:")
display(pa)

# %% [9] THE FLAG-IS-1012 TEST - UCP's EM flag vs CPC monthly standing, client level
# Andre's read from the background docs: CPC_EM_ELIGIBLE is built from the 1012 consent.
# Test: pull the standing-No client list from CPC_RB_PREF_MTHLY (same month-end) via
# teradatasql, join to the UCP snapshot, cross-tab. Clean derivation = standing No -> 0,
# everything else -> 1 (allowing small timing drift).
try:
    import teradatasql
except ImportError:
    get_ipython().system("pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")
    import teradatasql
import getpass, pandas as pd

# one Teradata connection per kernel - prompts ONCE, later cells reuse EDW
if "EDW" not in globals():
    EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com",
                              user=input("Teradata username: "),
                              password=getpass.getpass("Teradata password: "),
                              logmech="LDAP")

cur = EDW.cursor()
cur.execute(f"""
    SELECT CLNT_NO
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
      AND MTH_END_DT = DATE '{MONTH_B}'
""")
no_list = pd.DataFrame(cur.fetchall(), columns=["CLNT_NO"])
print(f"standing-No clients at {MONTH_B} pulled from Teradata: {len(no_list):,}")

spark.createDataFrame(no_list).createOrReplaceTempView("cpc_no_1012")
m9 = spark.sql(f"""
-- every UCP client: does the CPC book say 'standing No on 1012' for them, and what
-- does the UCP flag say?
SELECT CASE WHEN n.CLNT_NO IS NOT NULL THEN 'CPC standing = explicit No (5002)'
            ELSE                            'CPC standing = Yes / blank / no row' END AS cpc_position,
       CASE WHEN TRIM(CAST(u.{FLAG} AS STRING)) = '1' THEN 'flag = 1' ELSE 'flag = 0' END AS ucp_flag,
       COUNT(*) AS n_clients
FROM ucp_b u
LEFT JOIN cpc_no_1012 n ON u.CLNT_NO = n.CLNT_NO
GROUP BY 1, 2
ORDER BY 1, 2
""").toPandas()
m9["pct"] = (100 * m9["n_clients"] / m9["n_clients"].sum()).round(2)
print(f"UCP {FLAG} vs CPC 1012 standing, both at {MONTH_B}:")
display(m9)

# %% [10] The two deck lines, monthly - consent (flag = 1) and reachable (flag = 1 AND
# active email address). One count-SQL per month-end; the table feeds both charts below.
import pandas as pd
MONTHS_LINE = ["2025-01-31", "2025-02-28", "2025-03-31", "2025-04-30", "2025-05-31",
               "2025-06-30", "2025-07-31", "2025-08-31", "2025-09-30", "2025-10-31",
               "2025-11-30", "2025-12-31",
               "2026-01-31", "2026-02-28", "2026-03-31", "2026-04-30", "2026-05-31",
               "2026-06-30", "2026-07-31"]          # pack 44's full frame (mock window)

LINE_SQL = f"""
-- one snapshot: clients holding email consent, and the subset with a live address
SELECT COUNT(CASE WHEN TRIM(CAST({FLAG} AS STRING)) = '1' THEN 1 END)  AS n_consent,
       COUNT(CASE WHEN TRIM(CAST({FLAG} AS STRING)) = '1'
                   AND TRIM(CAST(ACTIVE_EMAIL_IND AS STRING)) = '1'
                  THEN 1 END)                                          AS n_reachable
FROM ucp_m
"""

rows = []
for m in MONTHS_LINE:
    spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={m}/").createOrReplaceTempView("ucp_m")
    r = spark.sql(LINE_SQL).toPandas()
    r.insert(0, "month_end", m)
    rows.append(r)
lines = pd.concat(rows, ignore_index=True)
lines["n_consent_no_address"] = lines["n_consent"] - lines["n_reachable"]
print(f"Monthly consent ({FLAG} = 1) and reachable (consent AND active email address):")
display(lines)

# %% [11] Chart 1 - consent line only (shown first; reachable held back for the follow-up)
import matplotlib.pyplot as plt
navy = "#16436e"

fig, ax = plt.subplots(figsize=(11, 4.6))
x = range(len(lines))
ax.plot(x, lines["n_consent"] / 1e6, color=navy, lw=2.2, marker="o", ms=4)
ax.text(len(lines) - 1, lines["n_consent"].iloc[-1] / 1e6 + 0.06,
        f"{lines['n_consent'].iloc[-1]/1e6:,.2f}", ha="right", fontsize=10, fontweight="bold")
ax.set_xticks(list(x))
ax.set_xticklabels([m[:7] for m in lines["month_end"]], fontsize=9, rotation=45)
ax.set_ylabel("# clients in MM")
ax.spines[["top", "right"]].set_visible(False)
ax.set_title(f"Clients with email consent ({FLAG} = 1) - monthly",
             fontweight="bold", fontsize=12, loc="left")
plt.tight_layout(); plt.show()

# %% [12] Chart 2 - reachable line, with the consent line kept for reference (the gap
# between them = consented clients with no active email address)
fig, ax = plt.subplots(figsize=(11, 4.6))
blue = "#2a78d6"
ax.plot(x, lines["n_consent"] / 1e6, color=navy, lw=1.6, marker="o", ms=3,
        label=f"Email consent ({FLAG} = 1)")
ax.plot(x, lines["n_reachable"] / 1e6, color=blue, lw=2.2, marker="o", ms=4,
        label="Reachable (consent AND active email address)")
ax.fill_between(x, lines["n_reachable"] / 1e6, lines["n_consent"] / 1e6,
                color=blue, alpha=0.10)
ax.text(len(lines) - 1, lines["n_reachable"].iloc[-1] / 1e6 - 0.15,
        f"{lines['n_reachable'].iloc[-1]/1e6:,.2f}", ha="right", fontsize=10, fontweight="bold")
ax.set_xticks(list(x))
ax.set_xticklabels([m[:7] for m in lines["month_end"]], fontsize=9, rotation=45)
ax.set_ylabel("# clients in MM")
ax.legend(loc="lower right", fontsize=9, frameon=False)
ax.spines[["top", "right"]].set_visible(False)
ax.set_title("Reachable emailable base vs consent - monthly",
             fontweight="bold", fontsize=12, loc="left")
plt.tight_layout(); plt.show()

# %% [13] LAND today's EM_DTL snapshot to HDFS - starts the monthly archive (the table is
# a daily-overwritten current snapshot; without this land, address/kill/spam layers have
# no history). Idempotent: skips if this LOAD_DT is already landed. ~20M rows, chunked.
try:
    import teradatasql
except ImportError:
    get_ipython().system("pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")
    import teradatasql
import getpass, pandas as pd

EM_DTL_BASE = "/user/427966379/unsub_cpc/em_dtl_snapshots/"

# reuses the session connection from [9]; prompts only if [9] was skipped this kernel
if "EDW" not in globals():
    EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com",
                              user=input("Teradata username: "),
                              password=getpass.getpass("Teradata password: "),
                              logmech="LDAP")

cur = EDW.cursor()
cur.execute("SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL")
load_dt = str(cur.fetchall()[0][0])[:10]
target = f"{EM_DTL_BASE}load_dt={load_dt}/"

jvm = spark._jvm
fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
already = fs.exists(jvm.org.apache.hadoop.fs.Path(target + "_SUCCESS"))
if already:
    print(f"load_dt {load_dt} already landed at {target} - skipping pull")
else:
    chunks, total = [], 0
    for c in pd.read_sql(f"""
        SELECT CLNT_NO, DELIVERABLE_EM_ADDR_IND, VALID_EM_ADDR_IND, EM_ELIGIBLE_IND,
               EM3_ELIGIBLE_IND, CPC1012_IND, EMAIL_KILL_CLNT_IND, SPAM_COMPLAINT_EM_IND
        FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
        WHERE LOAD_DT = DATE '{load_dt}'
    """, EDW, chunksize=1_000_000):
        chunks.append(c); total += len(c)
        print(f"  pulled {total:,} rows...")
    pdf = pd.concat(chunks, ignore_index=True)
    spark.createDataFrame(pdf).write.mode("overwrite").parquet(target)
    landed = spark.read.parquet(target).count()
    print(f"landed {landed:,} rows at {target} (pulled {total:,}) "
          f"| {'MATCH' if landed == total else 'MISMATCH - investigate'}")

# %% [14] ADDRESS-FLAG CROSS-CHECK - UCP's ACTIVE_EMAIL_IND vs EM_DTL's address flags,
# client level. Question: is the address code UCP carries the same information CIDM
# carries? Clean result = UCP active-email clients sit in EM_DTL with valid+deliverable
# addresses; UCP no-address clients are missing from EM_DTL or flagged N.
# Timing caveat: UCP snapshot = 2026-07-31 month-end, EM_DTL = today's load
# (same ~17-day drift as pack 42 [11c] - expect small off-diagonals, not zero).
spark.read.parquet(f"{EM_DTL_BASE}load_dt={load_dt}/").createOrReplaceTempView("em_dtl")

xm = spark.sql(f"""
-- every client either side: UCP address flag x EM_DTL address status
SELECT CASE WHEN u.CLNT_NO IS NULL                                   THEN 'not in UCP (EM_DTL only)'
            WHEN TRIM(CAST(u.ACTIVE_EMAIL_IND AS STRING)) = '1'      THEN 'UCP: active email = 1'
            ELSE                                                          'UCP: active email = 0' END AS ucp_address_flag,
       CASE WHEN e.CLNT_NO IS NULL                                   THEN 'not in EM_DTL'
            WHEN e.VALID_EM_ADDR_IND = 'Y'
             AND e.DELIVERABLE_EM_ADDR_IND = 'Y'                     THEN 'EM_DTL: valid + deliverable'
            WHEN e.VALID_EM_ADDR_IND = 'Y'                           THEN 'EM_DTL: valid, not deliverable'
            ELSE                                                          'EM_DTL: address not valid' END AS em_dtl_address_status,
       COUNT(*) AS n_clients
FROM ucp_b u
FULL OUTER JOIN em_dtl e ON u.CLNT_NO = e.CLNT_NO
GROUP BY 1, 2
ORDER BY 3 DESC
""").toPandas()
xm["pct"] = (100 * xm["n_clients"] / xm["n_clients"].sum()).round(2)
print(f"UCP ACTIVE_EMAIL_IND ({MONTH_B}) x EM_DTL address flags (load {load_dt}):")
display(xm)

# %% [15] LAND CPC_RB_PREF_MTHLY 1012 slices (view-2 waterfall source) - one parquet per
# month-end, idempotent. ~26M rows per month, chunked with progress (~15-25 min each).
try:
    import teradatasql
except ImportError:
    get_ipython().system("pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")
    import teradatasql
import getpass, pandas as pd

MTHLY_BASE = "/user/427966379/unsub_cpc/cpc_mthly_1012/"

if "EDW" not in globals():
    EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com",
                              user=input("Teradata username: "),
                              password=getpass.getpass("Teradata password: "),
                              logmech="LDAP")

jvm = spark._jvm
fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
MONTH_2024 = "2024-01-31"          # long-frame anchor (waterfall first bar at Jan-2024)
for m in [MONTH_2024, MONTH_A, MONTH_B]:
    target = f"{MTHLY_BASE}mth={m}/"
    if fs.exists(jvm.org.apache.hadoop.fs.Path(target + "_SUCCESS")):
        print(f"{m} already landed - skipping")
        continue
    chunks, total = [], 0
    for c in pd.read_sql(f"""
        SELECT CLNT_NO, CLNT_CONSENT_TYP
        FROM DDWV01.CPC_RB_PREF_MTHLY
        WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '{m}'
    """, EDW, chunksize=1_000_000):
        chunks.append(c); total += len(c)
        print(f"  {m}: pulled {total:,} rows...")
    pdf = pd.concat(chunks, ignore_index=True)
    spark.createDataFrame(pdf).write.mode("overwrite").parquet(target)
    landed = spark.read.parquet(target).count()
    print(f"{m}: landed {landed:,} rows | {'MATCH' if landed == total else 'MISMATCH - investigate'}")

# %% [16] VIEW-2 WATERFALL - consent from CPC MTHLY, activity from UCP.
# State = 1012-eligible (consent <> 5002; blank = Yes on 1012 per dictionary) AND
# active client (in UCP that month with OPN_PROD_CNT >= 1). DT_OPENED (UCP-only field)
# splits new-to-bank vs re-entered. Attrition = was emailable-active, now inactive -
# the CPC book never drops clients, so UCP activity is the attrition signal (Andre's
# rule); 'gone from CPC book' kept as its own bucket (expect ~0; if not, investigate).
spark.read.parquet(f"{MTHLY_BASE}mth={MONTH_A}/").createOrReplaceTempView("cpc_a")
spark.read.parquet(f"{MTHLY_BASE}mth={MONTH_B}/").createOrReplaceTempView("cpc_b")

wf2 = spark.sql(f"""
WITH ua AS (SELECT CLNT_NO, CASE WHEN OPN_PROD_CNT >= 1 THEN 1 ELSE 0 END AS act FROM ucp_a),
     ub AS (SELECT CLNT_NO, CASE WHEN OPN_PROD_CNT >= 1 THEN 1 ELSE 0 END AS act,
                   DT_OPENED FROM ucp_b),
     -- emailable-active at A: 1012 not explicit-No AND active per UCP
     a AS (SELECT c.CLNT_NO, c.CLNT_CONSENT_TYP AS cons_a,
                  CASE WHEN c.CLNT_CONSENT_TYP <> 5002
                        AND COALESCE(u.act, 0) = 1 THEN 1 ELSE 0 END AS em_a
           FROM cpc_a c LEFT JOIN ua u ON c.CLNT_NO = u.CLNT_NO),
     b AS (SELECT c.CLNT_NO, c.CLNT_CONSENT_TYP AS cons_b, u.DT_OPENED,
                  COALESCE(u.act, 0) AS act_b,
                  CASE WHEN c.CLNT_CONSENT_TYP <> 5002
                        AND COALESCE(u.act, 0) = 1 THEN 1 ELSE 0 END AS em_b
           FROM cpc_b c LEFT JOIN ub u ON c.CLNT_NO = u.CLNT_NO)
SELECT CASE
         WHEN a.em_a = 1 AND b.em_b = 1                       THEN 'stayed emailable (no bar)'
         WHEN a.em_a = 1 AND b.CLNT_NO IS NULL                THEN '- gone from CPC book (expect ~0)'
         WHEN a.em_a = 1 AND b.act_b = 0                      THEN '- attrition (inactive at B per UCP)'
         WHEN a.em_a = 1 AND b.cons_b = 5002                  THEN '- lost consent (1012 -> explicit No)'
         WHEN a.em_a = 0 AND b.em_b = 1 AND a.cons_a = 5002   THEN '+ opened consent (explicit No -> eligible)'
         WHEN a.em_a = 0 AND b.em_b = 1                       THEN '+ re-activated (was inactive at A)'
         WHEN a.CLNT_NO IS NULL AND b.em_b = 1
              AND b.DT_OPENED > DATE('{MONTH_A}')             THEN '+ new to bank (opened after A)'
         WHEN a.CLNT_NO IS NULL AND b.em_b = 1                THEN '+ re-entered (record predates A)'
         -- no-bar decomposition: the CPC book (26M) is wider than the emailable-active
         -- universe (~14M) - label why each no-bar client is outside it
         WHEN a.cons_a = 5002 AND b.cons_b = 5002             THEN 'no bar - explicit No both months'
         WHEN a.em_a = 0 AND b.em_b = 0
              AND a.CLNT_NO IS NOT NULL
              AND b.CLNT_NO IS NOT NULL                       THEN 'no bar - inactive / not in UCP both months'
         WHEN a.CLNT_NO IS NULL                               THEN 'no bar - arrived ineligible'
         WHEN b.CLNT_NO IS NULL                               THEN 'no bar - left while ineligible'
         ELSE 'no bar (other)'
       END AS bucket,
       COUNT(*) AS n_clients
FROM a FULL OUTER JOIN b ON a.CLNT_NO = b.CLNT_NO
GROUP BY 1
ORDER BY n_clients DESC
""").toPandas()
print(f"View-2 waterfall {MONTH_A} -> {MONTH_B} (consent = CPC MTHLY 1012, activity = UCP):")
display(wf2)

# %% [17] View-2 identity check + deck table (same shape as [2]/[4])
end2_direct = spark.sql(f"""
SELECT COUNT(*) AS n
FROM cpc_b c
JOIN ucp_b u ON c.CLNT_NO = u.CLNT_NO
WHERE c.CLNT_CONSENT_TYP <> 5002 AND u.OPN_PROD_CNT >= 1
""").collect()[0]["n"]
g2 = lambda pat: int(wf2.loc[wf2["bucket"].str.contains(pat, regex=False), "n_clients"].sum())
start2 = g2("stayed emailable") + g2("- gone") + g2("- attrition") + g2("- lost consent")
end2 = start2 \
     + g2("+ opened consent") + g2("+ re-activated") + g2("+ new to bank") + g2("+ re-entered") \
     - g2("- gone") - g2("- attrition") - g2("- lost consent")
print(f"START {start2:,} -> computed END {end2:,} | measured END {end2_direct:,} "
      f"| identity {'HOLDS' if end2 == end2_direct else 'BROKEN'}")

deck2 = pd.DataFrame([
    ["Emailable clients (CPC 1012 x active)", MONTH_A, start2, round(start2/1e6, 2)],
    ["+ New to bank", f"{MONTH_A} → {MONTH_B}", g2("+ new to bank"), round(g2("+ new to bank")/1e6, 2)],
    ["+ Re-entered client base", f"{MONTH_A} → {MONTH_B}", g2("+ re-entered"), round(g2("+ re-entered")/1e6, 2)],
    ["+ Re-activated (was inactive)", f"{MONTH_A} → {MONTH_B}", g2("+ re-activated"), round(g2("+ re-activated")/1e6, 2)],
    ["+ Existing clients opting in", f"{MONTH_A} → {MONTH_B}", g2("+ opened consent"), round(g2("+ opened consent")/1e6, 2)],
    ["− Lost consent (1012 explicit No)", f"{MONTH_A} → {MONTH_B}", -g2("- lost consent"), round(-g2("- lost consent")/1e6, 2)],
    ["− Client attrition (inactive per UCP)", f"{MONTH_A} → {MONTH_B}", -g2("- attrition") - g2("- gone"), round((-g2("- attrition") - g2("- gone"))/1e6, 2)],
    ["Emailable base (CPC 1012 x active)", MONTH_B, end2, round(end2/1e6, 2)],
], columns=["element", "period", "clients", "clients_MM"])
display(deck2)

# %% [18] View-2 waterfall chart - same mock style as [5]; data = the deck2 table above
import matplotlib.pyplot as plt

navy   = "#16436e"
blues2 = ["#2a78d6", "#7fb2e6", "#a9c9ee", "#d4e5f7"]
ambers = ["#e08214", "#f5c26b"]
grey   = "#8a8f98"

sub2 = [("New to bank", g2("+ new to bank")), ("Re-entered", g2("+ re-entered")),
        ("Re-activated", g2("+ re-activated")), ("Opted in", g2("+ opened consent"))]
uns2 = [("Client attrition", g2("- attrition") + g2("- gone")), ("Lost consent", g2("- lost consent"))]
adds2, drops2 = sum(v for _, v in sub2), sum(v for _, v in uns2)

lo = min(start2, end2) * 0.955 / 1e6
fig, ax = plt.subplots(figsize=(11, 5.8))
ax.bar(0, start2/1e6 - lo, bottom=lo, width=0.6, color=navy, zorder=3)
ax.text(0, start2/1e6 + 0.03, f"{start2/1e6:,.2f}", ha="center", fontsize=11, fontweight="bold")
ax.bar(3, end2/1e6 - lo, bottom=lo, width=0.6, color=navy, zorder=3)
ax.text(3, end2/1e6 + 0.03, f"{end2/1e6:,.2f}", ha="center", fontsize=11, fontweight="bold")

base = start2 / 1e6
for (lbl, v), c in zip(sub2, blues2):
    h = v / 1e6
    ax.bar(1, h, bottom=base, width=0.6, color=c, zorder=3,
           edgecolor="white", linewidth=1.5, label=lbl)
    if h > 0.015:
        ax.text(1, base + h/2, f"{h:.2f}", ha="center", va="center", fontsize=9.5)
    base += h
ax.text(1, base + 0.03, f"+{adds2/1e6:.2f}", ha="center", fontsize=11, fontweight="bold")
top2 = base

base = top2
for (lbl, v), c in zip(uns2, ambers):
    h = v / 1e6
    ax.bar(2, -h, bottom=base, width=0.6, color=c, zorder=3,
           edgecolor="white", linewidth=1.5, label=lbl)
    if h > 0.015:
        ax.text(2, base - h/2, f"-{h:.2f}", ha="center", va="center", fontsize=9.5)
    base -= h
ax.text(2, top2 + 0.03, f"-{drops2/1e6:.2f}", ha="center", fontsize=11, fontweight="bold")

ax.plot([0.3, 0.7], [start2/1e6]*2, ls=":", lw=1.2, color=grey)
ax.plot([1.3, 1.7], [top2]*2, ls=":", lw=1.2, color=grey)
ax.plot([2.3, 2.7], [(top2 - drops2/1e6)]*2, ls=":", lw=1.2, color=grey)
ax.set_xticks([0, 1, 2, 3])
ax.set_xticklabels([f"Emailable\nclients\n{MONTH_A[:7]}", "Subscribes",
                    "Unsubscribes", f"Emailable\nbase\n{MONTH_B[:7]}"], fontsize=10)
ax.set_ylabel("# clients in MM")
ax.set_ylim(lo, top2 * 1.012)
ax.spines[["top", "right"]].set_visible(False)
ax.text(-0.68, lo, "≈", fontsize=14, color="#444444", va="center")
ax.legend(loc="upper left", fontsize=9, frameon=False)
ax.set_title(f"Emailable base waterfall — {MONTH_A} to {MONTH_B}  (consent: CPC 1012 × active per UCP)",
             fontweight="bold", fontsize=12, loc="left")
plt.tight_layout(); plt.show()

# %% [19] SKETCH WATERFALL (pics/Screenshot 2026-08-19 142040) - pure CPC frame, Jan-24 -> Jul-26.
# START = all clients 1012 = 5001 @ 2024-01-31 (CPC_RB_PREF_MTHLY). + new 5001 by 2026-07-31.
# Unsub bar SPLIT: CPC-closed (5001 -> 5002) / vendor-feedback unsubs / their overlap.
# END = 5001 @ 2026-07-31 official, and TRUE = official minus vendor unsubs CPC never recorded.
# Vendor unsub set = sf_unsubscribe (client-keyed SFMC log), main BU only, 2024-01 -> 2026-07.
from trino.dbapi import connect as trino_connect
from trino.auth import BasicAuthentication
import getpass, pandas as pd

if "EDL" not in globals():
    _tu = input("Trino username: ")
    _tp = getpass.getpass("Trino password: ")
    EDL = trino_connect(host="strplvaexh0001.fg.rbc.com", port=8443, catalog="edl0_im",
                        user=_tu, auth=BasicAuthentication(_tu, _tp),
                        http_scheme="https", verify=False)

tcur = EDL.cursor()
tcur.execute("""
    SELECT DISTINCT subscriberkey
    FROM prod_uq20_digital.sf_unsubscribe
    WHERE oybaccountid = '1068860'
      AND substr(eventdate, 1, 10) >= '2024-01-01'
      AND substr(eventdate, 1, 10) <  '2026-08-01'
""")
vk = pd.DataFrame(tcur.fetchall(), columns=["clnt_no"])
vk["clnt_no"] = pd.to_numeric(vk["clnt_no"], errors="coerce")
vk = vk.dropna().astype({"clnt_no": "int64"})
print(f"vendor (SFMC) distinct unsub clients 2024-01 -> 2026-07: {len(vk):,}")
spark.createDataFrame(vk).createOrReplaceTempView("vendor_unsubs")

spark.read.parquet(f"{MTHLY_BASE}mth={MONTH_2024}/").createOrReplaceTempView("cpc_2024")
spark.read.parquet(f"{MTHLY_BASE}mth={MONTH_B}/").createOrReplaceTempView("cpc_b2")

sk = spark.sql("""
WITH a AS (SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_a FROM cpc_2024),
     b AS (SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b FROM cpc_b2),
     j AS (SELECT COALESCE(a.CLNT_NO, b.CLNT_NO) AS clnt_no, a.cons_a, b.cons_b,
                  CASE WHEN v.clnt_no IS NOT NULL THEN 1 ELSE 0 END AS vendor_unsub
           FROM a FULL OUTER JOIN b ON a.CLNT_NO = b.CLNT_NO
           LEFT JOIN vendor_unsubs v ON COALESCE(a.CLNT_NO, b.CLNT_NO) = v.clnt_no)
SELECT SUM(CASE WHEN cons_a = 5001 THEN 1 ELSE 0 END)                                        AS start_5001_jan24,
       SUM(CASE WHEN cons_a = 5001 AND cons_b = 5001 THEN 1 ELSE 0 END)                      AS stayed_5001,
       SUM(CASE WHEN (cons_a IS NULL OR cons_a <> 5001) AND cons_b = 5001 THEN 1 ELSE 0 END) AS new_5001,
       SUM(CASE WHEN cons_a = 5001 AND cons_b = 5002 THEN 1 ELSE 0 END)                      AS cpc_closed,
       SUM(CASE WHEN cons_a = 5001 AND cons_b = 5002 AND vendor_unsub = 1 THEN 1 ELSE 0 END) AS overlap_both,
       SUM(CASE WHEN cons_a = 5001 AND (cons_b IS NULL OR cons_b NOT IN (5001, 5002))
                THEN 1 ELSE 0 END)                                                           AS left_other,
       SUM(CASE WHEN cons_b = 5001 THEN 1 ELSE 0 END)                                        AS end_5001_jul26,
       SUM(CASE WHEN cons_b = 5001 AND vendor_unsub = 1 THEN 1 ELSE 0 END)                   AS vendor_unsub_still_open
FROM j
""").toPandas()

r = sk.iloc[0]
identity_ok = (r.start_5001_jan24 + r.new_5001 - r.cpc_closed - r.left_other) == r.end_5001_jul26
wf3 = pd.DataFrame([
    ["START: subscribers (1012 = 5001)", "2024-01-31", int(r.start_5001_jan24)],
    ["+ new subscribers (5001 by Jul-26)", "2024-02 → 2026-07", int(r.new_5001)],
    ["− unsub, CPC-closed only (5001→5002, no vendor record)", "2024-02 → 2026-07", -int(r.cpc_closed - r.overlap_both)],
    ["− unsub, BOTH systems (5001→5002 AND vendor record)", "2024-02 → 2026-07", -int(r.overlap_both)],
    ["− left 5001 other (blank / no row at B)", "2024-02 → 2026-07", -int(r.left_other)],
    ["END official: subscribers (1012 = 5001)", "2026-07-31", int(r.end_5001_jul26)],
    ["  of which vendor-unsubscribed, CPC never closed (blind spot)", "2024-01 → 2026-07", int(r.vendor_unsub_still_open)],
    ["END true: official minus the blind spot", "2026-07-31", int(r.end_5001_jul26 - r.vendor_unsub_still_open)],
], columns=["element", "period", "n_clients"])
print(f"Sketch waterfall (pure CPC 1012, Jan-24 -> Jul-26) | identity "
      f"{'HOLDS' if identity_ok else 'BROKEN'}:")
display(wf3)

# %% [20] Sketch waterfall chart - 4 bars per the sketch (blue / green / stacked unsub / gold)
import matplotlib.pyplot as plt

blue, green, gold = "#4472c4", "#70ad47", "#c49102"
greys = ["#a6a6a6", "#d0d0d0", "#fbe5d6"]   # cpc-only / overlap / vendor-still-open (sketch colors)
grey_line = "#8a8f98"

start_v   = r.start_5001_jan24 / 1e6
new_v     = r.new_5001 / 1e6
cpc_only  = (r.cpc_closed - r.overlap_both) / 1e6
overlap_v = r.overlap_both / 1e6
vend_open = r.vendor_unsub_still_open / 1e6
other_v   = r.left_other / 1e6
end_off   = r.end_5001_jul26 / 1e6
end_true  = (r.end_5001_jul26 - r.vendor_unsub_still_open) / 1e6

lo = min(start_v, end_true) * 0.93
fig, ax = plt.subplots(figsize=(11.5, 6))
ax.bar(0, start_v - lo, bottom=lo, width=0.6, color=blue, zorder=3)
ax.text(0, start_v + 0.06, f"{start_v:,.2f}", ha="center", fontsize=11, fontweight="bold")

ax.bar(1, new_v, bottom=start_v, width=0.6, color=green, zorder=3)
ax.text(1, start_v + new_v + 0.06, f"+{new_v:.2f}", ha="center", fontsize=11, fontweight="bold")
top = start_v + new_v

base = top
for lbl, v, c in [("Unsub - CPC closed only", cpc_only, greys[0]),
                  ("Unsub - both systems (overlap)", overlap_v, greys[1]),
                  ("Vendor unsub, CPC still open", vend_open, greys[2]),
                  ("Left 5001 other (blank/no row)", other_v, "#e8e8e8")]:
    ax.bar(2, -v, bottom=base, width=0.6, color=c, zorder=3,
           edgecolor="white", linewidth=1.2, label=lbl)
    if v > 0.03:
        ax.text(2, base - v/2, f"-{v:.2f}", ha="center", va="center", fontsize=9)
    base -= v
ax.text(2, top + 0.06, f"-{(top - base):.2f}", ha="center", fontsize=11, fontweight="bold")

ax.bar(3, end_true - lo, bottom=lo, width=0.6, color=gold, zorder=3)
ax.bar(3, vend_open, bottom=end_true, width=0.6, color="#e7d091", zorder=3,
       label="Blind spot (vendor unsub, still 5001)")
ax.text(3, end_off + 0.06, f"{end_off:,.2f} official", ha="center", fontsize=10, fontweight="bold")
ax.text(3, end_true - 0.10, f"{end_true:,.2f} true", ha="center", fontsize=10,
        fontweight="bold", color="white")

ax.plot([0.3, 0.7], [start_v]*2, ls=":", lw=1.2, color=grey_line)
ax.plot([1.3, 1.7], [top]*2, ls=":", lw=1.2, color=grey_line)
ax.set_xticks([0, 1, 2, 3])
ax.set_xticklabels(["Subscribers\n1012 = 5001\n2024-01", "New\nsubscribers",
                    "Unsubscribes\n(split by system)", "Subscribers\n2026-07\nofficial vs true"],
                   fontsize=10)
ax.set_ylabel("# clients in MM")
ax.set_ylim(lo, top * 1.015)
ax.spines[["top", "right"]].set_visible(False)
ax.text(-0.68, lo, "≈", fontsize=14, color="#444444", va="center")
ax.legend(loc="upper left", fontsize=8.5, frameon=False)
ax.set_title("Subscribers waterfall — CPC 1012 vs vendor feedback, Jan-2024 to Jul-2026",
             fontweight="bold", fontsize=12, loc="left")
plt.tight_layout(); plt.show()
