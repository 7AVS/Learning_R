# %% [markdown]
# # 45 — the Jan-2024 deck build: subscribers waterfall + monthly unsub series
#
# Everything the sketch deck needs, one file (assumes a live `spark` session):
#   [1] land CPC_RB_PREF_MTHLY 1012 slices (2024-01-31, 2026-07-31) - full book + write metadata
#   [2] vendor feedback monthly, dispo 1/4 x MNE, since 2024-01 (bites, idempotent)
#   [3] CPC 1012 standing per month-end since 2024-01 (5001/5002/5003)
#   [4] UCP monthly unsub flow (flag 1 -> 0) since 2024-01 (as far back as ucp4 goes)
#   [5] sketch waterfall data (pics/Screenshot 2026-08-19 142040): CPC vs vendor vs overlap
#   [6] sketch waterfall chart
#   [7] monthly-unsub comparison chart (vendor vs UCP), data table adjacent
# All lands are skip-if-landed - a killed session resumes where it stopped.

# %% [0] connections + helpers - prompts ONCE per kernel, later cells reuse EDW / EDL
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

UCP_BASE   = "/prod/sz/tsz/00172/data/ucp4/"
MTHLY_BASE = "/user/427966379/unsub_cpc/cpc_mthly_1012/"
MONTH_2024 = "2024-01-31"     # waterfall first-bar anchor
MONTH_END  = "2026-07-31"     # waterfall end anchor

jvm = spark._jvm
fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
print("EDW connection + HDFS helpers ready")

# %% [1] LAND CPC_RB_PREF_MTHLY 1012 slices - full book (5001/5002/blank, no consent
# filter) + CHG_TMSTMP + APP_SYS_CD so organic-vs-bulk/administrative slicing needs no
# re-pull. ~26M rows per month, chunked with progress (~20 min each, once ever).
for m in [MONTH_2024, MONTH_END]:
    target = f"{MTHLY_BASE}mth={m}/"
    if fs.exists(jvm.org.apache.hadoop.fs.Path(target + "_SUCCESS")):
        print(f"{m} already landed - skipping")
        continue
    chunks, total = [], 0
    for c in pd.read_sql(f"""
        SELECT CLNT_NO, CLNT_CONSENT_TYP, CHG_TMSTMP, APP_SYS_CD
        FROM DDWV01.CPC_RB_PREF_MTHLY
        WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '{m}'
    """, EDW, chunksize=1_000_000):
        chunks.append(c); total += len(c)
        print(f"  {m}: pulled {total:,} rows...")
    pdf = pd.concat(chunks, ignore_index=True)
    spark.createDataFrame(pdf).write.mode("overwrite").parquet(target)
    landed = spark.read.parquet(target).count()
    print(f"{m}: landed {landed:,} rows | {'MATCH' if landed == total else 'MISMATCH - investigate'}")

# %% [2] VENDOR MONTHLY since 2024-01 - EVENT + MASTER together, clnt_no ALWAYS.
# Unsub attribution = FIRST unsub of the month per clnt_no (multi-MNE clients count
# ONCE, under the first event's MNE - per-MNE counts therefore SUM to distinct clients).
# Sends = distinct clnt_no per MNE + an ALL_TOTAL row (grouping sets - a client mailed
# by 3 MNEs is 1 in the total). One bounded bite per month, three lands each,
# idempotent. MNE = raw SUBSTR(TREATMENT_ID, 8, 3) - LOB rollup happens at slice time.
# MASTER bounded +-3mo around the month (load_tm lags disposition; same margin logic
# as unsub_unified). MASTER is not 1:1 - GROUP BY collapses to distinct id->clnt pairs.
VFB_BASE = "/user/427966379/unsub_cpc/vendor_monthly_mne/"
VFB_MONTHS = pd.date_range("2024-01-01", "2026-07-01", freq="MS").strftime("%Y-%m-%d").tolist()

_MAST = """
    mast AS (
        SELECT consumer_id_hashed, CLNT_NO
        FROM DTZV01.VENDOR_FEEDBACK_MASTER
        WHERE load_tm >= ADD_MONTHS(DATE '{m0}', -3)
          AND load_tm <  ADD_MONTHS(DATE '{m0}', 4)
          AND CLNT_NO IS NOT NULL
        GROUP BY 1, 2
    )"""

for m0 in VFB_MONTHS:
    m1 = (pd.Timestamp(m0) + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")
    tag = m0[:7]
    if fs.exists(jvm.org.apache.hadoop.fs.Path(f"{VFB_BASE}unsub_clients/month={tag}/_SUCCESS")):
        print(f"{tag} already landed - skipping")
        continue

    # (a) unsubs: first unsub of the month per clnt_no -> that event's MNE
    un = pd.read_sql(f"""
        WITH {_MAST.format(m0=m0)},
        j AS (
            SELECT m.CLNT_NO, SUBSTR(e.TREATMENT_ID, 8, 3) AS mne, e.disposition_dt_tm AS dt
            FROM DTZV01.VENDOR_FEEDBACK_EVENT e
            JOIN mast m ON e.consumer_id_hashed = m.consumer_id_hashed
            WHERE e.disposition_cd = 4
              AND e.disposition_dt_tm >= DATE '{m0}'
              AND e.disposition_dt_tm <  DATE '{m1}'
        ),
        first_unsub AS (
            SELECT CLNT_NO, mne
            FROM (SELECT CLNT_NO, mne,
                         ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY dt ASC) AS rn
                  FROM j) t
            WHERE rn = 1
        )
        SELECT mne, CAST(COUNT(*) AS BIGINT) AS n_clients
        FROM first_unsub
        GROUP BY 1
    """, EDW)
    un.insert(0, "month", tag)
    spark.createDataFrame(un).write.mode("overwrite").parquet(f"{VFB_BASE}unsub_mne/month={tag}/")

    # (b) sends: distinct clnt_no per MNE + ALL_TOTAL row (true monthly reach)
    sd = pd.read_sql(f"""
        WITH {_MAST.format(m0=m0)},
        j AS (
            SELECT m.CLNT_NO, SUBSTR(e.TREATMENT_ID, 8, 3) AS mne
            FROM DTZV01.VENDOR_FEEDBACK_EVENT e
            JOIN mast m ON e.consumer_id_hashed = m.consumer_id_hashed
            WHERE e.disposition_cd = 1
              AND e.disposition_dt_tm >= DATE '{m0}'
              AND e.disposition_dt_tm <  DATE '{m1}'
        )
        SELECT COALESCE(mne, 'ALL_TOTAL') AS mne,
               CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS n_clients
        FROM j
        GROUP BY GROUPING SETS ((mne), ())
    """, EDW)
    sd.insert(0, "month", tag)
    spark.createDataFrame(sd).write.mode("overwrite").parquet(f"{VFB_BASE}send_mne/month={tag}/")

    # (c) the unsub CLIENT LIST for the month (feeds the waterfall's vendor set - ~30K rows)
    ul = pd.read_sql(f"""
        WITH {_MAST.format(m0=m0)}
        SELECT DISTINCT m.CLNT_NO
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        JOIN mast m ON e.consumer_id_hashed = m.consumer_id_hashed
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= DATE '{m0}'
          AND e.disposition_dt_tm <  DATE '{m1}'
    """, EDW)
    ul.insert(0, "month", tag)
    spark.createDataFrame(ul).write.mode("overwrite").parquet(f"{VFB_BASE}unsub_clients/month={tag}/")
    print(f"{tag}: unsub mne rows {len(un)} | send mne rows {len(sd)} | unsub clients {len(ul):,}")

vfb_un = spark.read.parquet(f"{VFB_BASE}unsub_mne/").toPandas().sort_values(["month", "mne"])
print("Vendor UNSUBS monthly x MNE (clnt_no grain, first-unsub-of-month dedup - rows sum to distinct clients):")
display(vfb_un)

vfb_sd = spark.read.parquet(f"{VFB_BASE}send_mne/").toPandas().sort_values(["month", "mne"])
print("Vendor SENDS monthly x MNE (distinct clnt_no per MNE; ALL_TOTAL = true monthly reach):")
display(vfb_sd)

vfb_tot = (vfb_un.groupby("month", as_index=False)["n_clients"].sum()
                 .rename(columns={"n_clients": "clients_unsub"})
                 .merge(vfb_sd.loc[vfb_sd.mne == "ALL_TOTAL", ["month", "n_clients"]]
                              .rename(columns={"n_clients": "clients_sent"}),
                        on="month", how="left"))
print("Monthly totals (distinct clnt_no):")
display(vfb_tot)

# %% [3] CPC MONTHLY since 2024-01 - standing 1012 counts per month-end by consent value
# (5002 explicit No + 5003 blank; 5001 lands free). One server-side aggregate, no
# attribution - CPC carries no MNE.
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

# %% [4] UCP MONTHLY UNSUB FLOW since 2024-01 - clients whose CPC_EM_ELIGIBLE flag went
# 1 -> 0 month over month (the UCP view of 'lost consent'). Runs as far back as ucp4
# partitions exist - missing months are reported, not fatal. Each pair's counts landed.
FLAG = "CPC_EM_ELIGIBLE"
UCPFLOW_BASE = "/user/427966379/unsub_cpc/ucp_monthly_flows/"
UCP_MONTH_ENDS = pd.date_range("2024-01-31", "2026-07-31", freq="M").strftime("%Y-%m-%d").tolist()

_avail = [m for m in UCP_MONTH_ENDS
          if fs.exists(jvm.org.apache.hadoop.fs.Path(f"{UCP_BASE}MONTH_END_DATE={m}/"))]
_missing = [m for m in UCP_MONTH_ENDS if m not in _avail]
if _missing:
    print(f"ucp4 partitions MISSING for: {_missing} - flow starts at {_avail[0] if _avail else 'NONE'}")

for m0, m1 in zip(_avail[:-1], _avail[1:]):
    target = f"{UCPFLOW_BASE}month={m1[:7]}/"
    if fs.exists(jvm.org.apache.hadoop.fs.Path(target + "_SUCCESS")):
        print(f"{m1[:7]} already landed - skipping")
        continue
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
    spark.createDataFrame(row).write.mode("overwrite").parquet(target)
    print(f"{m1[:7]}: lost_consent {int(row.lost_consent[0]):,} | "
          f"opted_in {int(row.opted_in[0]):,} | attrition {int(row.attrition[0]):,}")

ucp_flow = spark.read.parquet(UCPFLOW_BASE).toPandas().sort_values("month")
print(f"UCP monthly flows ({FLAG}), earliest available -> 2026-07:")
display(ucp_flow)

# %% [5] SKETCH WATERFALL data (pics/Screenshot 2026-08-19 142040) - pure CPC frame,
# Jan-24 -> Jul-26. START = 1012 = 5001 @ 2024-01-31. + new 5001 by 2026-07-31.
# Unsub bar SPLIT: CPC-closed (5001 -> 5002) / vendor-feedback unsubs / overlap.
# END = 5001 @ 2026-07-31 official, and TRUE = official minus vendor unsubs CPC never
# recorded. Vendor set = union of [2]'s monthly unsub CLIENT LISTS (EVENT+MASTER,
# clnt_no grain) - [2] must have run first.
spark.sql(f"""
    SELECT DISTINCT CLNT_NO AS clnt_no
    FROM parquet.`{VFB_BASE}unsub_clients/`
""").createOrReplaceTempView("vendor_unsubs")
print(f"vendor (EVENT+MASTER) distinct unsub clients 2024-01 -> 2026-07: "
      f"{spark.table('vendor_unsubs').count():,}")

spark.read.parquet(f"{MTHLY_BASE}mth={MONTH_2024}/").createOrReplaceTempView("cpc_2024")
spark.read.parquet(f"{MTHLY_BASE}mth={MONTH_END}/").createOrReplaceTempView("cpc_end")

sk = spark.sql("""
WITH a AS (SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_a FROM cpc_2024),
     b AS (SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b FROM cpc_end),
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

# %% [6] SKETCH WATERFALL chart - 4 bars per the sketch (blue / green / stacked unsub / gold)
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

# %% [7] MONTHLY UNSUB COMPARISON - vendor (true distinct clients) vs UCP lost-consent,
# one time axis. Data = the display() table below (vendor from [2], UCP from [4]).
cmp = (vfb_tot[["month", "clients_unsub"]]
       .merge(ucp_flow[["month", "lost_consent"]], on="month", how="outer")
       .rename(columns={"clients_unsub": "vendor_unsub_clients",
                        "lost_consent": "ucp_lost_consent"})
       .sort_values("month").reset_index(drop=True))
print("Monthly unsubs - vendor feedback vs UCP flag flow:")
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
