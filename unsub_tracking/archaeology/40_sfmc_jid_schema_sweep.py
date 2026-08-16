# %% [markdown]
# # 40 — schema probes: hunting SFMC send-level data (jid) in the warehouse
#
# Goal: find any EDW dataset carrying the SFMC Job ID (jid = the exact email send behind
# each unsubscribe) or a send log. The unsubscribe URL carries jid into the SFMC DEs;
# the question is whether any extract of that reaches Teradata. Dictionary-only sweeps
# first (instant), then a freshness kit for whatever candidates surface.
# Read hits with the usual discipline: SAMPLE 10 before believing a name.

# %% [0] connect + proof round-trip
try:
    import teradatasql
except ImportError:
    get_ipython().system("pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")
    import teradatasql
import getpass
import pandas as pd

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")

EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com", user=username,
                          password=password, logmech="LDAP")

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
pd.set_option("display.max_rows", 300)
pd.set_option("display.max_colwidth", 80)

# %% [1] Columns that smell like a send-job id, anywhere in the warehouse
print("Every column whose name suggests an email send/job identifier:")
display(edw_pd("""
SELECT DatabaseName, TableName, ColumnName
FROM DBC.ColumnsV
WHERE ColumnName LIKE ANY ('%JOB%ID%', '%JID%', '%JOB_NO%', '%SEND%ID%', '%DEPLOY%ID%',
                           '%SUBSCRIBER%')
ORDER BY DatabaseName, TableName, ColumnName
"""))

# %% [2] Tables that smell like SFMC / ESP / opt-out feeds
print("Every table/view whose name suggests an SFMC, ESP, send-log or opt-out feed")
print("(RESP family excluded so ESP does not match RESPONSE):")
display(edw_pd("""
SELECT DatabaseName, TableName, TableKind          -- T = table, V = view
FROM DBC.TablesV
WHERE ( TableName LIKE ANY ('%SFMC%', '%EXACT%', '%EMC%', '%SENDLOG%', '%SEND_LOG%',
                            '%OPTOUT%', '%OPT_OUT%', '%UNSUB%', '%CASL%', '%MKT_CLOUD%')
        OR (TableName LIKE '%ESP%' AND TableName NOT LIKE '%RESP%') )
ORDER BY DatabaseName, TableName
"""))

# %% [3] Everything living where the known SFMC->EDW pipe lands
print("All objects in DTZV01 / DTZTAU (VENDOR_FEEDBACK lives here - what else does?):")
display(edw_pd("""
SELECT DatabaseName, TableName, TableKind
FROM DBC.TablesV
WHERE DatabaseName IN ('DTZV01', 'DTZTAU')
ORDER BY DatabaseName, TableName
"""))

# %% [4] Freshness / metadata kit - run for any candidate pattern from [1]-[3]
# Dictionary caveats, so the columns are read right:
#  - LastAlterTimeStamp/Name = last DDL change, NOT last data load
#  - LastAccessTimeStamp/AccessCount = readers; NULL unless use-count logging is on
#  - size_gb ~ 0 = empty shell; views (TableKind V) have no size at all
PATTERN = '%UNSUB%'          # <- edit and rerun per candidate family
meta = edw_pd(f"""
SELECT t.DatabaseName, t.TableName, t.TableKind,
       t.CreateTimeStamp, t.CreatorName,
       t.LastAlterTimeStamp, t.LastAlterName,
       t.LastAccessTimeStamp, t.AccessCount,
       CAST(s.CurrentPerm AS FLOAT) / 1024/1024/1024 AS size_gb
FROM DBC.TablesV t
LEFT JOIN (SELECT DatabaseName, TableName, SUM(CurrentPerm) AS CurrentPerm
           FROM DBC.TableSizeV GROUP BY 1, 2) s
       ON s.DatabaseName = t.DatabaseName AND s.TableName = t.TableName
WHERE t.TableName LIKE '{PATTERN}'
ORDER BY t.LastAlterTimeStamp DESC
""")
print(f"Metadata for pattern {PATTERN} - created/altered/accessed/size per object:")
display(meta)

# %% [5] Per-finalist freshness - the table's own clock (edit and run per candidate)
# Dictionary cannot say when DATA last landed. Two checks per finalist:
CANDIDATE = "DTZV01.VENDOR_FEEDBACK_EVENT"     # <- edit
DATE_COL  = "disposition_dt_tm"                # <- edit (find it via SAMPLE first)

print(f"--- {CANDIDATE}: raw look ---")
display(edw_pd(f"SELECT * FROM {CANDIDATE} SAMPLE 5"))
print(f"--- {CANDIDATE}: its own clock (latest {DATE_COL}) ---")
display(edw_pd(f"SELECT MAX({DATE_COL}) AS latest_data, COUNT(*) AS n_rows FROM {CANDIDATE}"))
print(f"--- {CANDIDATE}: stats recency (maintained tables get stats recollected) ---")
display(edw_pd(f"""
SELECT StatsName, LastCollectTimeStamp
FROM DBC.StatsV
WHERE DatabaseName = '{CANDIDATE.split('.')[0]}' AND TableName = '{CANDIDATE.split('.')[1]}'
ORDER BY LastCollectTimeStamp DESC
"""))
