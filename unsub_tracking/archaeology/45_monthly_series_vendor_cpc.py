# %% [markdown]
# # 45 — deck build, SQL-first / audit-session edition
#
# Built to walk OTHER TEAMS through live: every dataset is ONE visible SQL against the
# warehouse (no saved subsets, no screen-hopping); heavy joins run server-side so the
# 16GB pod only ever receives small result tables. Plots live at the END, each with its
# underlying data table displayed right above it (PowerPoint rebuilds use the numbers).
#
#   [1] monthly UNSUBS x MNE since 2024-01 — one SQL (clnt_no grain, first-unsub-of-month)
#   [2] monthly SENDS x MNE since 2024-01 — one SQL template, month loop (the heavy scan)
#   [3] CPC 1012 standing per month-end — one SQL
#   [4] UCP monthly flows — Spark SQL (ucp4 exists only on HDFS)
#   [5] the subscribers waterfall Jan-24 -> Jul-26 — ONE SQL, eight numbers
#   [6] plots: waterfall + monthly comparison (data tables adjacent)
#
# THE LOCKED EVENT+MASTER MERGE (canon: spotlight/unsub_analysis_notebook.py ~528-563):
# join on BOTH keys (consumer_id_hashed AND TREATMENT_ID); MASTER as DISTINCT
# (hash, TREATMENT_ID, CLNT_NO) triples, CLNT_NO IS NOT NULL; EVENT side shape-guarded
# to 10-char dated treatment ids (excludes DEFAULT etc., documented rule); MASTER scan
# anchored by SUBSTR(TREATMENT_ID,1,7) julian deployment range — for unsubs it reaches
# back 3 months before the frame (an unsub references the SEND's master row).

# %% [0] connection - prompts ONCE per kernel
try:
    import teradatasql
except ImportError:
    get_ipython().system("pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")
    import teradatasql
import getpass
import pandas as pd

if "EDW" not in globals():
    EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com",
                              user=input("Teradata username: "),
                              password=getpass.getpass("Teradata password: "),
                              logmech="LDAP")

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print("EDW + spark ready")

# %% [1] MONTHLY UNSUBS x MNE since 2024-01 — one SQL, clnt_no grain.
# Dedup: first unsub of the month per client (multi-MNE clients count once, under the
# first event's MNE) -> per-MNE rows SUM to distinct clients per month.
# Julian anchors: '2023274' = 2023-10-01 (frame floor minus 3mo), '2026212' = 2026-07-31.
UNSUB_SQL = """
WITH ev AS (
    SELECT m.CLNT_NO,
           e.disposition_dt_tm AS dt,
           e.TREATMENT_ID,
           TRIM(EXTRACT(YEAR FROM e.disposition_dt_tm)) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM e.disposition_dt_tm) < 10
                       THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM e.disposition_dt_tm))       AS unsub_month
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2023274' AND '2026212'
                  AND CLNT_NO IS NOT NULL) m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'
      AND e.disposition_dt_tm <  DATE '2026-08-01'
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
),
ranked AS (
    SELECT unsub_month, CLNT_NO,
           SUBSTR(TREATMENT_ID, 8, 3) AS mne,
           ROW_NUMBER() OVER (PARTITION BY CLNT_NO, unsub_month
                              ORDER BY dt ASC, SUBSTR(TREATMENT_ID, 8, 3) ASC,
                                       TREATMENT_ID ASC) AS rn
    FROM ev
)
SELECT unsub_month, mne, CAST(COUNT(*) AS BIGINT) AS n_clients
FROM ranked
WHERE rn = 1
GROUP BY 1, 2
ORDER BY 1, 2
"""
vfb_un = pd.read_sql(UNSUB_SQL, EDW)
vfb_un.columns = [c.lower() for c in vfb_un.columns]
print("Vendor UNSUBS monthly x MNE (clnt_no grain, first-unsub-of-month dedup):")
display(vfb_un)

vfb_un_tot = (vfb_un.groupby("unsub_month", as_index=False)["n_clients"].sum()
                    .rename(columns={"unsub_month": "month", "n_clients": "clients_unsub"}))
print("Monthly unsub totals (distinct clients - per-MNE rows sum exactly):")
display(vfb_un_tot)

# %% [2] MONTHLY SENDS x MNE since 2024-01 — same join, disposition 1. A single 31-month
# send scan is a TDWM kill risk, so this loops one month at a time (one SQL template,
# month injected); results accumulate in memory - rerun re-queries, nothing saved.
SEND_SQL = """
WITH j AS (
    SELECT m.CLNT_NO, SUBSTR(e.TREATMENT_ID, 8, 3) AS mne
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '{j_lo}' AND '{j_hi}'
                  AND CLNT_NO IS NOT NULL) m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '{m0}'
      AND e.disposition_dt_tm <  DATE '{m1}'
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
)
SELECT COALESCE(mne, 'ALL_TOTAL') AS mne,
       CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS n_clients
FROM j
GROUP BY GROUPING SETS ((mne), ())
"""

def _julian(iso):
    d = pd.Timestamp(iso)
    return f"{d.year}{d.dayofyear:03d}"

send_parts = []
for m0 in pd.date_range("2024-01-01", "2026-07-01", freq="MS").strftime("%Y-%m-%d"):
    m1 = (pd.Timestamp(m0) + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")
    part = pd.read_sql(SEND_SQL.format(
        m0=m0, m1=m1,
        j_lo=_julian(pd.Timestamp(m0) - pd.offsets.MonthBegin(3)),   # multi-wave margin
        j_hi=_julian(pd.Timestamp(m1) - pd.offsets.Day(1))), EDW)
    part.columns = [c.lower() for c in part.columns]
    part.insert(0, "month", m0[:7])
    send_parts.append(part)
    print(f"{m0[:7]}: {len(part)} mne rows")
vfb_sd = pd.concat(send_parts, ignore_index=True)
print("Vendor SENDS monthly x MNE (distinct clnt_no; ALL_TOTAL = true monthly reach):")
display(vfb_sd)

# %% [3] CPC 1012 STANDING per month-end since 2024-01 — one SQL, no attribution
# (CPC carries no MNE). 5002 = explicit No, 5003 = blank, 5001 = Yes.
cpc_m = pd.read_sql("""
    SELECT MTH_END_DT, CLNT_CONSENT_TYP,
           CAST(COUNT(*) AS BIGINT) AS n_clients
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012
      AND MTH_END_DT >= DATE '2024-01-31'
    GROUP BY 1, 2
    ORDER BY 1, 2
""", EDW)
cpc_piv = (cpc_m.pivot_table(index="MTH_END_DT", columns="CLNT_CONSENT_TYP",
                             values="n_clients", aggfunc="sum")
                .rename(columns={5001: "n_5001_yes", 5002: "n_5002_no", 5003: "n_5003_blank"})
                .reset_index())
print("CPC 1012 standing per month-end (2024-01 -> latest), by consent value:")
display(cpc_piv)

# %% [4] UCP MONTHLY FLOWS since 2024-01 — Spark SQL (ucp4 exists only on HDFS; this is
# the one source the warehouse can't serve). Flag flips month over month; missing
# partitions reported, not fatal. Assumes live `spark`.
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"
FLAG = "CPC_EM_ELIGIBLE"

jvm = spark._jvm
fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
_month_ends = pd.date_range("2024-01-31", "2026-07-31", freq="M").strftime("%Y-%m-%d").tolist()
_avail = [m for m in _month_ends
          if fs.exists(jvm.org.apache.hadoop.fs.Path(f"{UCP_BASE}MONTH_END_DATE={m}/"))]
_missing = [m for m in _month_ends if m not in _avail]
if _missing:
    print(f"ucp4 partitions MISSING: {_missing} - flow starts at {_avail[0] if _avail else 'NONE'}")

flow_parts = []
for m0, m1 in zip(_avail[:-1], _avail[1:]):
    spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={m0}/").createOrReplaceTempView("u_m0")
    spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={m1}/").createOrReplaceTempView("u_m1")
    row = spark.sql(f"""
        WITH m0 AS (SELECT CLNT_NO, CAST(TRIM(CAST({FLAG} AS STRING)) = '1' AS INT) AS e0 FROM u_m0),
             m1 AS (SELECT CLNT_NO, CAST(TRIM(CAST({FLAG} AS STRING)) = '1' AS INT) AS e1 FROM u_m1)
        SELECT SUM(CASE WHEN m0.e0 = 1 AND m1.e1 = 0 THEN 1 ELSE 0 END)          AS lost_consent,
               SUM(CASE WHEN m0.e0 = 0 AND m1.e1 = 1 THEN 1 ELSE 0 END)          AS opted_in,
               SUM(CASE WHEN m0.e0 = 1 AND m1.CLNT_NO IS NULL THEN 1 ELSE 0 END) AS attrition
        FROM m0 FULL OUTER JOIN m1 ON m0.CLNT_NO = m1.CLNT_NO
    """).toPandas()
    row.insert(0, "month", m1[:7])
    flow_parts.append(row)
    print(f"{m1[:7]}: lost {int(row.lost_consent[0]):,} | opted {int(row.opted_in[0]):,} "
          f"| attrition {int(row.attrition[0]):,}")
ucp_flow = pd.concat(flow_parts, ignore_index=True)
print(f"UCP monthly flows ({FLAG}):")
display(ucp_flow)

# %% [5] CPC MONTHLY UNSUBS (1012 writes to 5002) split by WRITER: 7020 (the SFMC email
# backfeed) vs all other application systems. Source = DDWV01.CPC_RB_PREF (the proven
# mirror with the write timestamp) - monthly flow by CHG_TMSTMP month.
# SURVIVOR CAVEAT (state once, small): the standing table keeps only each client's
# LATEST 1012 row, so a 5002 later overwritten (re-consent) drops out of this flow -
# re-consent measured at ~4.7K over 2.5 years, so the shave is negligible.
cpc_writes = pd.read_sql("""
    SELECT TRIM(EXTRACT(YEAR FROM CAST(CHG_TMSTMP AS DATE))) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)) < 10
                       THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)))   AS chg_month,
           CASE WHEN APP_SYS_CD = 7020 THEN '7020 email backfeed'
                ELSE 'other writers' END                          AS writer,
           CAST(COUNT(*) AS BIGINT)                               AS n_writes_to_no
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012
      AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2024-01-01'
    GROUP BY 1, 2
    ORDER BY 1, 2
""", EDW)
cpc_writes.columns = [c.lower() for c in cpc_writes.columns]
print("CPC 1012 -> explicit No, monthly writes by writer (7020 = SFMC backfeed vs others):")
display(cpc_writes)

# %% [6] THE SUBSCRIBERS WATERFALL Jan-24 -> Jul-26 — ONE SQL, eight numbers, all joins
# server-side (two 26M-row CPC month slices + the vendor unsub set never leave Teradata).
# Sketch: pics/Screenshot 2026-08-19 142040. START = 1012 = 5001 @ 2024-01-31; unsub bar
# split CPC-closed / vendor / overlap; END official vs TRUE (minus vendor unsubs CPC
# never recorded).
WATERFALL_SQL = """
WITH a AS (      -- CPC book at the start anchor
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_a
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2024-01-31'
),
b AS (           -- CPC book at the end anchor
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2026-07-31'
),
v AS (           -- vendor unsub clients in the frame (locked EVENT+MASTER merge)
    SELECT DISTINCT m.CLNT_NO
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2023274' AND '2026212'
                  AND CLNT_NO IS NOT NULL) m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'
      AND e.disposition_dt_tm <  DATE '2026-08-01'
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
),
j AS (           -- every client either anchor, with the vendor flag
    SELECT COALESCE(a.CLNT_NO, b.CLNT_NO) AS clnt_no, a.cons_a, b.cons_b,
           CASE WHEN v.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS vendor_unsub
    FROM a
    FULL OUTER JOIN b ON a.CLNT_NO = b.CLNT_NO
    LEFT JOIN v ON v.CLNT_NO = COALESCE(a.CLNT_NO, b.CLNT_NO)
)
SELECT CAST(SUM(CASE WHEN cons_a = 5001 THEN 1 ELSE 0 END) AS BIGINT)                       AS start_5001_jan24,
       CAST(SUM(CASE WHEN cons_a = 5001 AND cons_b = 5001 THEN 1 ELSE 0 END) AS BIGINT)     AS stayed_5001,
       CAST(SUM(CASE WHEN (cons_a IS NULL OR cons_a <> 5001) AND cons_b = 5001
                     THEN 1 ELSE 0 END) AS BIGINT)                                          AS new_5001,
       CAST(SUM(CASE WHEN cons_a = 5001 AND cons_b = 5002 THEN 1 ELSE 0 END) AS BIGINT)     AS cpc_closed,
       CAST(SUM(CASE WHEN cons_a = 5001 AND cons_b = 5002 AND vendor_unsub = 1
                     THEN 1 ELSE 0 END) AS BIGINT)                                          AS overlap_both,
       CAST(SUM(CASE WHEN cons_a = 5001 AND (cons_b IS NULL OR cons_b NOT IN (5001, 5002))
                     THEN 1 ELSE 0 END) AS BIGINT)                                          AS left_other,
       CAST(SUM(CASE WHEN cons_b = 5001 THEN 1 ELSE 0 END) AS BIGINT)                       AS end_5001_jul26,
       CAST(SUM(CASE WHEN cons_b = 5001 AND vendor_unsub = 1 THEN 1 ELSE 0 END) AS BIGINT)  AS vendor_unsub_still_open
FROM j
"""
sk = pd.read_sql(WATERFALL_SQL, EDW)
sk.columns = [c.lower() for c in sk.columns]
r = sk.iloc[0]
identity_ok = (r.start_5001_jan24 + r.new_5001 - r.cpc_closed - r.left_other) == r.end_5001_jul26
wf = pd.DataFrame([
    ["START: subscribers (1012 = 5001)", "2024-01-31", int(r.start_5001_jan24)],
    ["+ new subscribers (5001 by Jul-26)", "2024-02 → 2026-07", int(r.new_5001)],
    ["− unsub, CPC-closed only (5001→5002, no vendor record)", "2024-02 → 2026-07", -int(r.cpc_closed - r.overlap_both)],
    ["− unsub, BOTH systems (5001→5002 AND vendor record)", "2024-02 → 2026-07", -int(r.overlap_both)],
    ["− left 5001 other (blank / no row at end)", "2024-02 → 2026-07", -int(r.left_other)],
    ["END official: subscribers (1012 = 5001)", "2026-07-31", int(r.end_5001_jul26)],
    ["  of which vendor-unsubscribed, CPC never closed (blind spot)", "2024-01 → 2026-07", int(r.vendor_unsub_still_open)],
    ["END true: official minus the blind spot", "2026-07-31", int(r.end_5001_jul26 - r.vendor_unsub_still_open)],
], columns=["element", "period", "n_clients"])
print(f"Subscribers waterfall (CPC 1012 vs vendor), Jan-24 -> Jul-26 | identity "
      f"{'HOLDS' if identity_ok else 'BROKEN'}:")
display(wf)

# %% [7] PLOTS - at the end, each with its underlying data table displayed adjacent
# (PowerPoint rebuild uses these numbers, not the image). The four deck outputs:
# 7a waterfall | 7b vendor monthly unsub bars | 7c CPC monthly 5002-writes split
# 7020-vs-others | 7d vendor-vs-UCP comparison.
import matplotlib.pyplot as plt

# --- 7a. waterfall chart (data = the wf table above, re-displayed here) ---
display(wf)
blue, green, gold = "#4472c4", "#70ad47", "#c49102"
greys = ["#a6a6a6", "#d0d0d0", "#fbe5d6"]
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

# --- 7b. vendor monthly unsubs, bar chart (data = vfb_un_tot, displayed here) ---
print("Vendor monthly unsub clients (plot data):")
display(vfb_un_tot)
fig, ax = plt.subplots(figsize=(11.5, 4.6))
xb = range(len(vfb_un_tot))
ax.bar(xb, vfb_un_tot["clients_unsub"] / 1e3, width=0.65, color="#16436e", zorder=3)
ax.set_xticks(list(xb))
ax.set_xticklabels(vfb_un_tot["month"], fontsize=8.5, rotation=45)
ax.set_ylabel("clients (thousands)")
ax.spines[["top", "right"]].set_visible(False)
ax.set_title("Monthly unsubscribes — vendor feedback (distinct clients), since 2024-01",
             fontweight="bold", fontsize=12, loc="left")
plt.tight_layout(); plt.show()

# --- 7c. CPC monthly 1012 -> 5002 writes, split 7020 vs other writers (data displayed) ---
cpcw_piv = (cpc_writes.pivot_table(index="chg_month", columns="writer",
                                   values="n_writes_to_no", aggfunc="sum")
                      .fillna(0).reset_index())
print("CPC 1012 -> explicit No per month, by writer (plot data):")
display(cpcw_piv)
fig, ax = plt.subplots(figsize=(11.5, 4.6))
xc = range(len(cpcw_piv))
for col, color in [("7020 email backfeed", "#e08214"), ("other writers", "#16436e")]:
    if col in cpcw_piv.columns:
        ax.plot(xc, cpcw_piv[col] / 1e3, lw=2.2, marker="o", ms=4, color=color, label=col)
ax.set_xticks(list(xc))
ax.set_xticklabels(cpcw_piv["chg_month"], fontsize=8.5, rotation=45)
ax.set_ylabel("writes to No (thousands)")
ax.legend(loc="upper right", fontsize=9, frameon=False)
ax.spines[["top", "right"]].set_visible(False)
ax.set_title("CPC 1012 opt-outs per month — SFMC backfeed (7020) vs all other writers",
             fontweight="bold", fontsize=12, loc="left")
plt.tight_layout(); plt.show()

# --- 7d. monthly unsub comparison, vendor vs UCP (data table displayed first) ---
cmp = (vfb_un_tot
       .merge(ucp_flow[["month", "lost_consent"]], on="month", how="outer")
       .rename(columns={"clients_unsub": "vendor_unsub_clients",
                        "lost_consent": "ucp_lost_consent"})
       .sort_values("month").reset_index(drop=True))
print("Monthly unsubs - vendor feedback vs UCP flag flow (plot data):")
display(cmp)

fig, ax = plt.subplots(figsize=(11.5, 4.8))
x = range(len(cmp))
ax.plot(x, cmp["vendor_unsub_clients"] / 1e3, color="#16436e", lw=2.2, marker="o", ms=4,
        label="Vendor feedback - distinct unsub clients")
ax.plot(x, cmp["ucp_lost_consent"] / 1e3, color="#e08214", lw=2.2, marker="o", ms=4,
        label="UCP - lost consent (flag 1 → 0)")
ax.set_xticks(list(x))
ax.set_xticklabels(cmp["month"], fontsize=8.5, rotation=45)
ax.set_ylabel("clients (thousands)")
ax.legend(loc="upper right", fontsize=9, frameon=False)
ax.spines[["top", "right"]].set_visible(False)
ax.set_title("Monthly unsubscribes — vendor feedback vs UCP consent flag, since 2024-01",
             fontweight="bold", fontsize=12, loc="left")
plt.tight_layout(); plt.show()
