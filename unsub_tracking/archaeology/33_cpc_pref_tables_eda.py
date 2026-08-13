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
    "DDWV01.CPC_RB_PREF",         # current-state snapshot (per schema doc)
    "DDWV01.CPC_RB_PREF_MTHLY",   # monthly snapshots - needs a month-end filter
    "DDWV01.CPC_RB_PREF_LOG",     # change log (the one that gave us trouble)
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
       COUNT(*)                 AS n_rows,
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
MTHLY_SNAP_COL = None   # <- set from the [3] sample, e.g. "MTH_END_DT"; None = skip
if MTHLY in samples and MTHLY_SNAP_COL:
    display(edw_pd(f"""
SELECT {MTHLY_SNAP_COL} AS snapshot_period,
       COUNT(*) AS n_rows, COUNT(DISTINCT CLNT_NO) AS n_clients
FROM {MTHLY}
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
