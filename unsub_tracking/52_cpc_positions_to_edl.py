# 52_cpc_positions_to_edl.py
# Save the Aug-24 opted-in universe with its CPC 1012 position at BOTH anchors
# (Aug-24 and Jun-26) to an EDL table, so the NBA analytics stakeholder can track
# profitability of "stayed opted in" vs "did not" (request 2026-08-27).
# Also carries the Salesforce-unsub flag so the CPC-vs-SF gap is visible on the
# same rows (Andre: do what was asked, then show the gap).
#
# Grain: one row per CLNT_NO in the start book (cons 1012 = 5001 AND active at
# 2024-08-31, personal). Expected rows = 12,545,962 (45_audit_queries Q3b).
# End anchor = 2026-06-30 (CPC July-26 not loaded at request time); parameter.
#
# Engines: SQL runs Teradata-direct; result lands in Spark and is written with
# the exact saveAsTable pattern the stakeholder sent. Auditable SQL, one proof
# per cell, no success prints without a server round-trip.

# %% [0] EDW connect - repo standard (packs 32/33); skips if the kernel already has EDW
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
START_ANCHOR = "2024-08-31"     # start book: cons 1012 = 5001 AND active here
END_ANCHOR   = "2026-06-30"     # CPC July-26 not loaded -> June; move to 2026-07-31 when it lands
SF_FLOOR     = "2024-09-01"     # Salesforce unsub window (matches Q3b)
SF_CEIL      = "2026-07-01"     # exclusive; = END_ANCHOR + 1 day
MASTER_LO, MASTER_HI = "2024153", "2026181"   # VENDOR_FEEDBACK_MASTER treatment-id julian prefix range

EDL_DB     = "prod_zp10_nba_analytics_staging"
EDL_TABLE  = "unsub_cpc_1012_positions_aug24_jun26"
EDL_PATH   = f"prod/16131/app/ZP10/lab/data/tde/measurement/dev/{EDL_DB}.db/{EDL_TABLE}"
print(f"target: {EDL_DB}.{EDL_TABLE}\npath:   {EDL_PATH}")

# %% [2] the extract - Teradata-direct. One row per start-book client.
SQL_POSITIONS = f"""
WITH u_a AS (   -- active at start anchor (personal)
    SELECT DISTINCT CLNT_NO FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '{START_ANCHOR}' AND CLNT_STS = 'A' AND CLNT_TYP = 1
),
u_b AS (        -- active at end anchor (personal)
    SELECT DISTINCT CLNT_NO FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '{END_ANCHOR}' AND CLNT_STS = 'A' AND CLNT_TYP = 1
),
a AS (          -- CPC 1012 at start anchor
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_a, APP_SYS_CD AS writer_a
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '{START_ANCHOR}' AND CLNT_TYP_CD = 1
),
b AS (          -- CPC 1012 at end anchor
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b, APP_SYS_CD AS writer_b
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '{END_ANCHOR}' AND CLNT_TYP_CD = 1
),
v AS (          -- first Salesforce unsub click in the window, per client
    SELECT CLNT_NO, MIN(disposition_dt_tm) AS first_sf_unsub_dt_tm
    FROM (
        SELECT m.CLNT_NO, e.disposition_dt_tm
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '{MASTER_LO}' AND '{MASTER_HI}'
                      AND CLNT_NO IS NOT NULL) m
          ON  m.consumer_id_hashed = e.consumer_id_hashed
          AND m.TREATMENT_ID       = e.TREATMENT_ID
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= DATE '{SF_FLOOR}'
          AND e.disposition_dt_tm <  DATE '{SF_CEIL}'
          AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
          AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    ) x
    GROUP BY CLNT_NO
)
SELECT a.CLNT_NO,
       DATE '{START_ANCHOR}'                                         AS anchor_start_dt,
       a.cons_a                                                      AS cpc_1012_start,      -- always 5001 here
       1                                                             AS opted_in_start,      -- by construction
       DATE '{END_ANCHOR}'                                           AS anchor_end_dt,
       b.cons_b                                                      AS cpc_1012_end,        -- 5001 / 5002 / 5003 / NULL (not in CPC)
       b.writer_b                                                    AS cpc_1012_writer_end, -- 7020 = email backfeed
       CASE WHEN ub.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END            AS active_end,
       CASE WHEN b.cons_b = 5001 AND ub.CLNT_NO IS NOT NULL
            THEN 1 ELSE 0 END                                        AS opted_in_end,        -- the stakeholder's 1/0
       CASE WHEN v.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END             AS sf_unsub_in_window,  -- Salesforce said no
       v.first_sf_unsub_dt_tm
FROM a
INNER JOIN u_a ON u_a.CLNT_NO = a.CLNT_NO
LEFT  JOIN b   ON b.CLNT_NO   = a.CLNT_NO
LEFT  JOIN u_b ub ON ub.CLNT_NO = a.CLNT_NO
LEFT  JOIN v   ON v.CLNT_NO   = a.CLNT_NO
WHERE a.cons_a = 5001
"""
print(SQL_POSITIONS)

# %% [3] pull into Spark. Preferred: Spark JDBC (Teradata runs it, executors receive it -
# no driver-memory pass for 12.5M rows). Fallback: chunked teradatasql -> pandas -> Spark.
import pandas as pd
try:
    df_pos = (spark.read.format("jdbc")
              .option("driver", "com.teradata.jdbc.TeraDriver")
              .option("url", "jdbc:teradata://TERADATA-DNS-SYSA.FG.RBC.COM/LOGMECH=LDAP")
              .option("dbtable", f"({SQL_POSITIONS}) AS src")
              .option("user", username).option("password", password)
              .option("partitionColumn", "CLNT_NO").option("lowerBound", "1")
              .option("upperBound", "999999999").option("numPartitions", "16")
              .option("fetchsize", "50000")
              .load())
    print("loaded via Spark JDBC")
except Exception as e:
    print("JDBC path failed ->", type(e).__name__, str(e)[:200], "\nfalling back to chunked teradatasql")
    parts = []
    for i, chunk in enumerate(pd.read_sql(SQL_POSITIONS, EDW, chunksize=500_000)):
        parts.append(chunk); print(f"  chunk {i}: {len(chunk):,} rows")
    pdf = pd.concat(parts, ignore_index=True)
    pdf["CLNT_NO"] = pdf["CLNT_NO"].astype("int64")     # never float -> scientific notation
    df_pos = spark.createDataFrame(pdf)
df_pos = df_pos.cache()
n_rows = df_pos.count()
print(f"rows in Spark: {n_rows:,}  (expected 12,545,962 from Q3b start_5001_aug24)")

# %% [4] proof 1: universe. Every row is opted-in at start; row count matches the waterfall start bar.
assert n_rows == 12_545_962, f"start book mismatch: {n_rows:,} vs 12,545,962 - anchors or filters differ from Q3b"
bad = df_pos.filter("cpc_1012_start <> 5001 OR opted_in_start <> 1").count()
assert bad == 0, f"{bad:,} rows not 5001 at start"
print("PASS: 12,545,962 rows, all 5001 + active at", START_ANCHOR)

# %% [5] proof 2: end-anchor distribution - the stakeholder's stay/leave split, plus the SF overlay.
from pyspark.sql import functions as F
summary = (df_pos.groupBy("opted_in_end", "sf_unsub_in_window")
           .agg(F.count("*").alias("clients"))
           .orderBy("opted_in_end", "sf_unsub_in_window"))
display(summary)
print("read: opted_in_end=1 & sf_unsub=1 is the gray-bar population (CPC open, Salesforce said no)")
display(df_pos.groupBy("cpc_1012_end", "active_end").agg(F.count("*").alias("clients")).orderBy("cpc_1012_end", "active_end"))

# %% [6] write to EDL - exact pattern the stakeholder sent (overwrite, explicit path, saveAsTable)
(df_pos.write.mode("overwrite")
       .option("path", EDL_PATH)
       .saveAsTable(f"{EDL_DB}.{EDL_TABLE}"))
print("write issued ->", f"{EDL_DB}.{EDL_TABLE}")

# %% [7] proof 3: read it back from the catalog (not from the cached df) - count + schema + 5 rows
t = spark.table(f"{EDL_DB}.{EDL_TABLE}")
n_back = t.count()
assert n_back == n_rows, f"read-back {n_back:,} != written {n_rows:,}"
t.printSchema()
display(t.limit(5))
print(f"PASS: {EDL_DB}.{EDL_TABLE} holds {n_back:,} rows; opted_in_end split:")
display(t.groupBy("opted_in_end").count())

# %% [8] hand-off note (copy into the reply to the stakeholder)
print(f"""
Table: {EDL_DB}.{EDL_TABLE}
Grain: one row per client opted-in (CPC 1012 = 5001, active, personal) at {START_ANCHOR}. Rows: {n_rows:,}.
Columns: cpc_1012_start / opted_in_start (=1), cpc_1012_end / cpc_1012_writer_end / active_end / opted_in_end (1 = still 5001 and active at {END_ANCHOR}),
         sf_unsub_in_window / first_sf_unsub_dt_tm (Salesforce unsubscribe click {SF_FLOOR}..{END_ANCHOR}).
Caveat: opted_in_end = 1 AND sf_unsub_in_window = 1 are clients CPC still shows as opted in but who unsubscribed in Salesforce
        (~474K at Jul-26). Profitability tracked on CPC alone treats them as stayers.
End anchor is {END_ANCHOR}: CPC July-26 was not loaded at request time.
""")
