# !! READS DDWV01.CPC_RB_PREF_LOG, WHICH IS BROKEN (~1% of writes). DO NOT RUN. Historical only. Andre 2026-09-04 !!
# %% [markdown]
# # 33 — CPC preference tables EDA: which one can we actually use?
#
# The log table gave us trouble; before building anything else, profile every CPC
# preference table side by side: reachable? what grain? how many clients? what date
# range? do they agree on 1012 standing? Then pick.
#
# All outputs inline. Same connection as pack 32 / unsub_unified.

# %% [0] connect + proof round-trip
try:
    import teradatasql
except ImportError:
    get_ipython().system("pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")
    import teradatasql
import getpass
import pandas as pd
import matplotlib.pyplot as plt

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")

TD_HOST = "Teradata-dns-sysa.fg.rbc.com"
EDW = teradatasql.connect(host=TD_HOST, user=username, password=password, logmech="LDAP")

def edw_pd(sql, chunksize=1_000_000):
    cur = EDW.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    parts = []
    while True:
        rows = cur.fetchmany(chunksize)
        if not rows:
            break
        parts.append(pd.DataFrame(rows, columns=cols))
    cur.close()
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=cols)

display(edw_pd("SELECT USER AS usr, SESSION AS sess, CURRENT_TIMESTAMP AS ts"))

# %% [1] CONFIG — the candidate tables. Add/fix names here and rerun everything below.
TABLES = [
    "DG6V01.CPC_CLNT_PREF_CHC",   # client preference CHC - the 4th table from the pic
    "DDWV01.CPC_RB_PREF",         # current-state snapshot (per schema doc)
    "DDWV01.CPC_RB_PREF_MTHLY",   # monthly snapshots - needs a month-end filter
    "DDWV01.CPC_RB_PREF_LOG",     # change log (~1% capture for 1012 - demoted)
]

# %% [2] REACHABILITY — can we read each table at all? Verbatim error if not.
for t in TABLES:
    try:
        edw_pd(f"SELECT * FROM {t} SAMPLE 1")
        print(f"OK        {t}")
    except Exception as e:
        print(f"FAILED    {t}\n          {str(e)[:300]}")

# %% [3] RAW LOOK — SAMPLE 10 per table, all columns, so we see grain and fields
pd.set_option("display.max_colwidth", 80)
samples = {}
for t in TABLES:
    try:
        samples[t] = edw_pd(f"SELECT * FROM {t} SAMPLE 10")
        print(f"--- {t} — {len(samples[t].columns)} columns ---")
        display(samples[t])
    except Exception as e:
        print(f"--- {t}: SKIPPED ({str(e)[:120]}) ---")

# %% [4] SCALE — rows, distinct clients, distinct switches per table
# One number set per table: how big, how many clients, how many PREF_IDs.
for t in TABLES:
    if t not in samples:
        continue
    try:
        display(edw_pd(f"""
SELECT '{t}' AS table_name,
       CAST(COUNT(*) AS BIGINT) AS n_rows,      -- BIGINT: MTHLY >2.1B rows overflows COUNT
       COUNT(DISTINCT CLNT_NO)  AS n_clients,
       COUNT(DISTINCT PREF_ID)  AS n_pref_ids
FROM {t}
"""))
    except Exception as e:
        print(f"{t}: scale query failed — {str(e)[:200]}")

# %% [5] DATE COLUMNS — min/max of every date/timestamp column, per table
# Detected from the sample dtypes + names; shows history depth and snapshot cadence.
for t, s in samples.items():
    date_cols = [c for c in s.columns
                 if "DT" in c.upper() or "TMSTMP" in c.upper() or "DATE" in c.upper()
                 or str(s[c].dtype).startswith(("datetime", "object")) and "TM" in c.upper()]
    date_cols = list(dict.fromkeys(date_cols))
    if not date_cols:
        print(f"{t}: no date-like columns detected in sample")
        continue
    sel = ", ".join(f"MIN({c}) AS min_{c}, MAX({c}) AS max_{c}" for c in date_cols)
    try:
        print(f"--- {t} ---")
        display(edw_pd(f"SELECT {sel} FROM {t}"))
    except Exception as e:
        print(f"{t}: date-range query failed — {str(e)[:200]}")

# %% [6] MONTHLY TABLE — snapshot cadence check
# If CPC_RB_PREF_MTHLY holds one row per client x pref x MONTH, an unfiltered read
# multiplies everything. Count rows per snapshot period to see the cadence and
# find the column to filter on (edit the column name below if [3] shows a different one).
MTHLY = "DDWV01.CPC_RB_PREF_MTHLY"
MTHLY_SNAP_COL = "MTH_END_DT"   # confirmed from [3]/[5] run 2026-08-13
if MTHLY in samples and MTHLY_SNAP_COL:
    display(edw_pd(f"""
SELECT {MTHLY_SNAP_COL} AS snapshot_period,
       CAST(COUNT(*) AS BIGINT) AS n_rows, COUNT(DISTINCT CLNT_NO) AS n_clients
FROM {MTHLY}
WHERE PREF_ID = 1012          -- one switch only: full-table GROUP BY is >2.1B rows
GROUP BY 1 ORDER BY 1 DESC
"""))
else:
    print("Set MTHLY_SNAP_COL from the [3] sample, then rerun this cell.")

# %% [7] THE DECIDER — do the tables agree on 1012 standing?
# Same question to every table: how many clients currently have 1012 = No (5002)?
# For the LOG that means latest row per client; for a snapshot it is a direct filter.
# Agreement -> any table works; disagreement -> the freshest/most complete one wins.
for t in samples:
    is_log = t.endswith("_LOG")
    try:
        if is_log:
            q = f"""
WITH latest AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP
    FROM {t}
    WHERE PREF_ID = 1012
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
)
SELECT '{t}' AS table_name, CLNT_CONSENT_TYP, COUNT(*) AS n_clients
FROM latest GROUP BY 2 ORDER BY 3 DESC
"""
        else:
            q = f"""
SELECT '{t}' AS table_name, CLNT_CONSENT_TYP, COUNT(DISTINCT CLNT_NO) AS n_clients
FROM {t}
WHERE PREF_ID = 1012
GROUP BY 2 ORDER BY 3 DESC
"""
        display(edw_pd(q))
    except Exception as e:
        print(f"{t}: 1012 standing failed — {str(e)[:200]}")
# NOTE: for the MTHLY table this counts across ALL snapshots until [6]'s column is
# known — read its numbers only after adding the month-end filter.

# %% [8] TRUE FLIP TIMING from the CURRENT-STATE table
# CPC_RB_PREF row = client's standing NOW + CHG_TMSTMP of the LAST change.
# Grouping the 3.26M standing No's by change-month = monthly arrivals into No.
# (Censored only by later re-flips, which the log showed are rare.)
# THIS replaces the pack-32 log-based monthly volume — the log holds ~1% of No's.
arr = edw_pd("""
SELECT TRIM(EXTRACT(YEAR FROM CAST(CHG_TMSTMP AS DATE))) || '-' ||
         TRIM(CASE WHEN EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)) < 10 THEN '0' ELSE '' END) ||
         TRIM(EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)))  AS chg_month,
       COUNT(*)                                              AS n_clients_arrived_at_no
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2024-01-01'
GROUP BY 1
ORDER BY 1
""")
display(arr)
fig, ax = plt.subplots(figsize=(12, 4.5))
ax.bar(arr["chg_month"], arr["n_clients_arrived_at_no"], color="#2a78d6")
for x, v in zip(arr["chg_month"], arr["n_clients_arrived_at_no"]):
    ax.text(x, v, f"{int(v):,}", ha="center", va="bottom", fontsize=8)
ax.set_ylabel("clients")
ax.set_title("1012: clients whose CURRENT standing became No, by month of last change (CPC_RB_PREF)\n"
             "compare against the log-based ~140/mo from pack 32",
             fontweight="bold")
ax.tick_params(axis="x", rotation=45)
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# %% [9] WHO WROTE the standing No's — writers on the current-state table
# Same writers question as pack 32 [7], but on the table that actually has the volume.
wr2 = edw_pd("""
SELECT APP_SYS_CD, CAST(COUNT(*) AS BIGINT) AS n_clients
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
GROUP BY 1 ORDER BY 2 DESC
""")
SYS_DESC = {
    7001: "Sales Platform (branch/service delivery staff)", 7002: "DI Client Source",
    7003: "Royal Direct / Client View (contact centre)", 7004: "Online Banking",
    7005: "Service Platform", 7006: "RBC Banking (STaR UI, batch/purge)",
    7007: "RBC Express", 7008: "DS Client Source", 7009: "BridgeTrack", 7010: "CASPER",
    7012: "Retail Banking Investment System F200", 7013: "Retail Banking Investment System 5G10",
    7014: "Term Investment System 4V00", 7015: "SAP / RCT-LINX desktop", 7016: "RBC.COM",
    7017: "D&H/AMIA/CMG (telemarketer)", 7018: "CART", 7019: "IRIS",
    7020: "Exact Target (email ESP)", 7021: "TSYS", 7022: "RD Fulfillment",
    7023: "Assisted Multi Product Application", 7024: "VOX (telemarketing vendor)",
    7025: "ZEDD telemarketing / CASL Tool (context-dep.)", 7026: "APAC (telemarketing vendor)",
    7027: "D&H", 7028: "CPC-CA (MCA)", 7029: "RCL TPA",
    7030: "GISP (WM) / ADHOC Data Source (context-dep.)", 7999: "Default Application System",
    99999: "batch update (SRF consolidation)",
}
wr2["system"] = [SYS_DESC.get(c, "?? not in dictionary") for c in wr2["APP_SYS_CD"]]
wr2["share_pct"] = (wr2["n_clients"] / wr2["n_clients"].sum() * 100).round(1)
display(wr2)

# %% [10] WRITERS x YEAR — when did each system write the standing No's? (1012=5002)
# Dates the 672,775 ESP-written No's: decade-old stock vs ongoing flow. Also
# fingerprints the 2024-03 spike (pack 07 hypothesis: HSBC migration load).
wy = edw_pd("""
SELECT EXTRACT(YEAR FROM CHG_TMSTMP) AS chg_year, APP_SYS_CD,
       CAST(COUNT(*) AS BIGINT) AS n_clients
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
GROUP BY 1, 2
ORDER BY 1, 3 DESC
""")
p = wy.pivot_table(index="chg_year", columns="APP_SYS_CD", values="n_clients",
                   aggfunc="sum", fill_value=0)
p["TOTAL"] = p.sum(axis=1)
display(p)

# %% [10b] the 2024-03 spike fingerprint — which writer owns it?
display(edw_pd("""
SELECT APP_SYS_CD, CAST(COUNT(*) AS BIGINT) AS n_clients
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2024-03-01' AND CHG_TMSTMP < DATE '2024-04-01'
GROUP BY 1 ORDER BY 2 DESC
"""))
