# 52_cpc_positions_to_edl.py
# Two EDL tables, two snapshots, same logic: who is contactable by CPC at each anchor.
#   anchor table = Aug-24 (2024-08-31)   target table = Jun-26 (2026-06-30)
# Contactable = personal client, active, CPC 1012 (banking email) = 5001 at that month-end.
# Volumes differ by design (joiners, leavers). Stakeholder request 2026-08-27.

# %% [0] EDW connect - repo standard; reuses EDW if the kernel already has it
try:
    EDW
    print("EDW already in kernel - reusing")
except NameError:
    try:
        import teradatasql
    except ImportError:
        get_ipython().system("pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")
        import teradatasql
    import getpass
    username = input("Enter your username: ")
    password = getpass.getpass("Enter your password: ")
    TD_HOST = "Teradata-dns-sysa.fg.rbc.com"
    EDW = teradatasql.connect(host=TD_HOST, user=username, password=password, logmech="LDAP")
_cur = EDW.cursor(); _cur.execute("SELECT 1"); print("EDW round-trip returned:", _cur.fetchall()); _cur.close()

# %% [1] parameters
EDL_DB = "prod_zp10_nba_analytics_staging"
SNAPSHOTS = {                       # table name -> month-end anchor
    "unsub_cpc_1012_anchor_aug24": "2024-08-31",
    "unsub_cpc_1012_target_jun26": "2026-06-30",   # CPC July-26 not loaded at request time
}

# %% [2] the extract - one anchor, one snapshot. Same SQL for both; only the date changes.
def sql_contactable(anchor):
    return f"""
/* CONTACTABLE BY CPC AT {anchor} - one row per client.
   In: personal (CLNT_TYP = 1 on RB_CLNT_DLY, CLNT_TYP_CD = 1 on CPC), active (CLNT_STS = 'A'
   on the {anchor} RB_CLNT_DLY snapshot), CPC preference 1012 = 5001 on the {anchor} month-end.
   Out: business clients, inactive, 1012 = 5002 / 5003 / other / no row. Other gates not read. */
SELECT p.CLNT_NO,
       DATE '{anchor}'          AS anchor_dt,
       p.CLNT_CONSENT_TYP       AS cpc_1012,        -- always 5001 here
       p.APP_SYS_CD             AS cpc_1012_writer, -- 7020 = email backfeed
       1                        AS contactable
FROM DDWV01.CPC_RB_PREF_MTHLY p
INNER JOIN (SELECT DISTINCT CLNT_NO FROM DDWV01.RB_CLNT_DLY
            WHERE SNAP_DT = DATE '{anchor}' AND CLNT_STS = 'A' AND CLNT_TYP = 1) u
        ON u.CLNT_NO = p.CLNT_NO
WHERE p.PREF_ID = 1012 AND p.MTH_END_DT = DATE '{anchor}' AND p.CLNT_TYP_CD = 1
  AND p.CLNT_CONSENT_TYP = 5001
"""

# %% [3] pull each snapshot into Spark (JDBC, Teradata does the work)
def pull(sql):
    return (spark.read.format("jdbc")
            .option("driver", "com.teradata.jdbc.TeraDriver")
            .option("url", "jdbc:teradata://TERADATA-DNS-SYSA.FG.RBC.COM/LOGMECH=LDAP")
            .option("dbtable", f"({sql}) AS src")
            .option("user", username).option("password", password)
            .option("fetchsize", "50000")
            .load())
dfs = {}
for tbl, anchor in SNAPSHOTS.items():
    dfs[tbl] = pull(sql_contactable(anchor)).cache()
    print(f"{tbl}: {dfs[tbl].count():,} rows contactable at {anchor}")

# %% [4] write to EDL - the database's registered location + table name (absolute path)
DB_LOC = [r for r in spark.sql(f"DESCRIBE DATABASE {EDL_DB}").collect() if r[0].strip().lower().startswith("location")][0][1]
for tbl, df in dfs.items():
    path = f"{DB_LOC.rstrip('/')}/{tbl}"
    spark.sql(f"DROP TABLE IF EXISTS {EDL_DB}.{tbl}")
    df.write.mode("overwrite").option("path", path).saveAsTable(f"{EDL_DB}.{tbl}")
    print("written ->", f"{EDL_DB}.{tbl}", "@", path)

# %% [5] proof: read each back from the catalog; count must equal what was pulled
for tbl, df in dfs.items():
    spark.catalog.refreshTable(f"{EDL_DB}.{tbl}")
    n_back, n_pulled = spark.table(f"{EDL_DB}.{tbl}").count(), df.count()
    assert n_back == n_pulled, f"{tbl}: read-back {n_back:,} != pulled {n_pulled:,}"
    print(f"PASS {EDL_DB}.{tbl}: {n_back:,} rows")
display(spark.table(f"{EDL_DB}.unsub_cpc_1012_target_jun26").limit(5))

# %% [6] add cpc_optout_month to the ANCHOR table (stakeholder ask 2026-08-28):
# first month-end after Aug-24 where the client's CPC 1012 = 5002 (closed). NULL = never closed
# through Jun-26. Reads the monthly snapshots, so a close-then-reopen shows its first close month.
# Absent-from-CPC months are not counted as a change (no row != opted out); missing_cpc_months counts them.
ANCHOR_TBL = "unsub_cpc_1012_anchor_aug24"
SQL_OPTOUT = """
SELECT a.CLNT_NO,
       DATE '2024-08-31'                       AS anchor_dt,
       a.CLNT_CONSENT_TYP                      AS cpc_1012,
       a.APP_SYS_CD                            AS cpc_1012_writer,
       1                                       AS contactable,
       MIN(CASE WHEN m.CLNT_CONSENT_TYP = 5002 THEN m.MTH_END_DT END)                  AS cpc_optout_month,        -- first month 1012 = 5002 (closed); 5003/blank NOT counted
       MIN(CASE WHEN m.CLNT_CONSENT_TYP = 5002 THEN m.APP_SYS_CD END)                   AS cpc_optout_writer_first, -- writer on the first 5002 month (ties: lowest code)
       22 - COUNT(m.CLNT_NO)                                                            AS missing_cpc_months     -- how many of the 22 month-ends Sep-24..Jun-26 have NO 1012 row (0 = full history, 22 = gone after Aug-24)
FROM DDWV01.CPC_RB_PREF_MTHLY a
INNER JOIN (SELECT DISTINCT CLNT_NO FROM DDWV01.RB_CLNT_DLY
            WHERE SNAP_DT = DATE '2024-08-31' AND CLNT_STS = 'A' AND CLNT_TYP = 1) u
        ON u.CLNT_NO = a.CLNT_NO
LEFT JOIN DDWV01.CPC_RB_PREF_MTHLY m
       ON m.CLNT_NO = a.CLNT_NO AND m.PREF_ID = 1012 AND m.CLNT_TYP_CD = 1
      AND m.MTH_END_DT > DATE '2024-08-31' AND m.MTH_END_DT <= DATE '2026-06-30'
WHERE a.PREF_ID = 1012 AND a.MTH_END_DT = DATE '2024-08-31' AND a.CLNT_TYP_CD = 1
  AND a.CLNT_CONSENT_TYP = 5001
GROUP BY a.CLNT_NO, a.CLNT_CONSENT_TYP, a.APP_SYS_CD
"""
df_a2 = pull(SQL_OPTOUT).cache()
n_a2 = df_a2.count()
print(f"anchor with optout month: {n_a2:,} rows (must equal the anchor table count)")
display(df_a2.groupBy("cpc_optout_month").count().orderBy("cpc_optout_month"))   # NULL row = never opted out

path = f"{DB_LOC.rstrip('/')}/{ANCHOR_TBL}"
spark.sql(f"DROP TABLE IF EXISTS {EDL_DB}.{ANCHOR_TBL}")
df_a2.write.mode("overwrite").option("path", path).saveAsTable(f"{EDL_DB}.{ANCHOR_TBL}")
spark.catalog.refreshTable(f"{EDL_DB}.{ANCHOR_TBL}")
n_back = spark.table(f"{EDL_DB}.{ANCHOR_TBL}").count()
assert n_back == n_a2, f"read-back {n_back:,} != {n_a2:,}"
print(f"PASS {EDL_DB}.{ANCHOR_TBL} rewritten with cpc_optout_month: {n_back:,} rows")

# %% [7] VALIDATE the anchor table's cpc_optout_month. Four checks, each its own answer.
from pyspark.sql import functions as F
ta = spark.table(f"{EDL_DB}.{ANCHOR_TBL}")

# check 1 - grain: no duplicate clients, count unchanged
n, nd = ta.count(), ta.select("CLNT_NO").distinct().count()
print(f"check 1 grain: {n:,} rows, {nd:,} distinct clients ->", "PASS" if n == nd else "FAIL - duplicates")

# check 2 - shape: opt-outs by month. Expect a hump around Mar-25..Jun-25 (7020 run-rate) and
# the total non-null to be >= 174,996 (Q3b closes among still-active: 72,346 + 8,604 + 94,046),
# because this also counts clients who closed then went inactive or re-opened.
by_month = ta.groupBy("cpc_optout_month").count().orderBy("cpc_optout_month")
display(by_month)
n_opt = ta.filter("cpc_optout_month IS NOT NULL").count()
print(f"check 2 total with an opt-out month: {n_opt:,}  (floor from Q3b closes = 174,996) ->", "PASS" if n_opt >= 174_996 else "LOOK - fewer than the waterfall's closes")

# check 3 - independent recompute in Teradata with a different method (window, not MIN/CASE):
# per client, the first month where 1012 = 5002 and the PREVIOUS month was 5001. Compare month totals.
SQL_RECOMP = """
SELECT first_close_month, CAST(COUNT(*) AS BIGINT) AS clients
FROM (
    SELECT CLNT_NO, MIN(MTH_END_DT) AS first_close_month
    FROM (
        SELECT m.CLNT_NO, m.MTH_END_DT, m.CLNT_CONSENT_TYP,
               LAG(m.CLNT_CONSENT_TYP) OVER (PARTITION BY m.CLNT_NO ORDER BY m.MTH_END_DT) AS prev_cons
        FROM DDWV01.CPC_RB_PREF_MTHLY m
        INNER JOIN (SELECT p.CLNT_NO FROM DDWV01.CPC_RB_PREF_MTHLY p
                    INNER JOIN (SELECT DISTINCT CLNT_NO FROM DDWV01.RB_CLNT_DLY
                                WHERE SNAP_DT = DATE '2024-08-31' AND CLNT_STS = 'A' AND CLNT_TYP = 1) u
                            ON u.CLNT_NO = p.CLNT_NO
                    WHERE p.PREF_ID = 1012 AND p.MTH_END_DT = DATE '2024-08-31' AND p.CLNT_TYP_CD = 1
                      AND p.CLNT_CONSENT_TYP = 5001) sb
                ON sb.CLNT_NO = m.CLNT_NO
        WHERE m.PREF_ID = 1012 AND m.CLNT_TYP_CD = 1
          AND m.MTH_END_DT BETWEEN DATE '2024-08-31' AND DATE '2026-06-30'
    ) x
    WHERE CLNT_CONSENT_TYP = 5002 AND prev_cons = 5001
    GROUP BY CLNT_NO
) y
GROUP BY 1 ORDER BY 1
"""
recomp = pull(SQL_RECOMP).toPandas()
mine = by_month.filter("cpc_optout_month IS NOT NULL").toPandas().rename(columns={"cpc_optout_month": "first_close_month", "count": "table_clients"})
cmp = recomp.merge(mine, on="first_close_month", how="outer").fillna(0)
cmp["diff"] = cmp["clients"] - cmp["table_clients"]
display(cmp)
print("check 3: 'diff' should be 0 or small positive (recompute needs prev month = 5001; table counts a first 5002 even after a missing month)")

# check 4 - eyeball 10 clients: full monthly path vs the month the table chose
sample_ids = [r.CLNT_NO for r in ta.filter("cpc_optout_month IS NOT NULL").limit(10).collect()]
SQL_PATH = f"""
SELECT CLNT_NO, MTH_END_DT, CLNT_CONSENT_TYP, APP_SYS_CD
FROM DDWV01.CPC_RB_PREF_MTHLY
WHERE PREF_ID = 1012 AND CLNT_TYP_CD = 1
  AND CLNT_NO IN ({",".join(str(i) for i in sample_ids)})
  AND MTH_END_DT BETWEEN DATE '2024-08-31' AND DATE '2026-06-30'
ORDER BY CLNT_NO, MTH_END_DT
"""
display(pull(SQL_PATH))
display(ta.filter(F.col("CLNT_NO").isin(sample_ids)).select("CLNT_NO", "cpc_optout_month", "cpc_optout_writer_first", "missing_cpc_months"))
print("check 4: for each client, the first month showing 5002 in the path must equal cpc_optout_month")
