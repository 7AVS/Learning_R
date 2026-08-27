# 52_cpc_positions_to_edl.py
# Save the Aug-24 opted-in universe as TWO EDL tables - anchor position (Aug-24)
# and target position (Jun-26), same clients, join on CLNT_NO - so the NBA analytics stakeholder can track
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
print("EDL db:", EDL_DB, "- two tables written in cell [6]: anchor (Aug-24) and target (Jun-26)")

# %% [2] the extract - Teradata-direct. One row per start-book client.
SQL_POSITIONS = f"""
/* ==========================================================================
   DATA DEFINITION - what this table is, and every cut applied to get it
   ==========================================================================
   WHO IS IN (one row each) - the START BOOK at {START_ANCHOR}:
     1. Personal clients only: CLNT_TYP = 1 on DDWV01.RB_CLNT_DLY and
        CLNT_TYP_CD = 1 on DDWV01.CPC_RB_PREF_MTHLY. Business clients out.
     2. Active at the start anchor: CLNT_STS = 'A' on RB_CLNT_DLY at
        SNAP_DT = {START_ANCHOR}. Inactive / closed / null-status out.
     3. Email consent OPEN at the start anchor: CPC preference 1012
        (Banking - Email, CASL) with CLNT_CONSENT_TYP = 5001 on the
        {START_ANCHOR} month-end snapshot. 5002 (closed), 5003 (blank),
        any other value, or no 1012 row at all -> out.
     Personal is required in BOTH systems; the waterfall (Q3b) required it in
     CPC only, so this table has 417 fewer rows (12,545,545 vs 12,545,962).
     Clients who joined, re-activated or re-consented AFTER {START_ANCHOR}
     are NOT here (this is the stakeholder's "all opted in at Aug-24" base).

   WHAT WE RECORD FOR EACH - the END POSITION at {END_ANCHOR}:
     - cpc_1012_end: raw 1012 value on the {END_ANCHOR} snapshot
       (5001 open / 5002 closed / 5003 blank / NULL = no 1012 row that month)
     - cpc_1012_writer_end: APP_SYS_CD that last wrote the row
       (7020 = email backfeed from Salesforce; other codes = branch, call
       centre, batch)
     - active_end: 1 if CLNT_STS = 'A' at {END_ANCHOR}, else 0
     - opted_in_end: 1 only if cpc_1012_end = 5001 AND active_end = 1.
       This is the stakeholder's stay (1) / leave (0) flag. A client who
       is still 5001 but inactive counts as 0 (not contactable).
     No precedence rule is applied here - every client keeps all raw
     fields, so any rule can be applied downstream.

   OVERLAY - Salesforce side (not used for opted_in_end; provided so the
   CPC-vs-Salesforce gap is visible on the same rows):
     - sf_unsub_in_window: 1 if the client has at least one
       disposition_cd = 4 (unsubscribe click) in DTZV01.VENDOR_FEEDBACK_EVENT
       between {SF_FLOOR} and {END_ANCHOR} inclusive, joined to
       VENDOR_FEEDBACK_MASTER on (consumer_id_hashed, TREATMENT_ID) to get
       CLNT_NO; MASTER scanned for treatment-id prefix {MASTER_LO}..{MASTER_HI}.
       Any list / any mnemonic - not scoped to banking or cards email.
     - first_sf_unsub_dt_tm: the earliest such click.
     opted_in_end = 1 AND sf_unsub_in_window = 1 = CPC still says yes,
     client said no in Salesforce (the gray-bar population).

   WHAT IS NOT HERE:
     - Other CPC gates (1046 rewards newsletter, program-specific 1004..1045):
       only 1012 is read. A client whose unsubscribe landed on a program
       gate shows as opted_in_end = 1 here.
     - CPC writes between the two anchors: only the two month-end snapshots
       are read; a close-then-reopen inside the window is invisible.
     - July-2026: not loaded in CPC at request time; END_ANCHOR = {END_ANCHOR}.
     - Business clients, non-active clients at start, clients not 5001 at start.
   ========================================================================== */
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
# SQL defined; runs in cell [3]

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
# Q3b start bar = 12,545,962 with personal from CPC only (CLNT_TYP_CD = 1). This table ALSO requires
# CLNT_TYP = 1 on RB_CLNT_DLY (personal in both systems), which drops the clients typed personal in
# CPC but not in RB_CLNT_DLY. Measured 2026-08-27: 12,545,545 -> 417 fewer (0.003%). Tolerance 0.01%.
Q3B_START = 12_545_962
delta = Q3B_START - n_rows
print(f"vs Q3b start bar: {n_rows:,} = {Q3B_START:,} - {delta:,}  ({delta / Q3B_START:.4%} - RB_CLNT_DLY CLNT_TYP=1 cut)")
assert abs(delta) <= Q3B_START * 0.0001, f"start book off by {delta:,} - more than the type-filter drift; anchors or filters differ from Q3b"
bad = df_pos.filter("cpc_1012_start <> 5001 OR opted_in_start <> 1").count()
assert bad == 0, f"{bad:,} rows not 5001 at start"
print(f"PASS: {n_rows:,} rows, all 5001 + active + personal (both systems) at", START_ANCHOR)

# %% [5] proof 2: end-anchor distribution - the stakeholder's stay/leave split, plus the SF overlay.
from pyspark.sql import functions as F
summary = (df_pos.groupBy("opted_in_end", "sf_unsub_in_window")
           .agg(F.count("*").alias("clients"))
           .orderBy("opted_in_end", "sf_unsub_in_window"))
display(summary)
print("read: opted_in_end=1 & sf_unsub=1 is the gray-bar population (CPC open, Salesforce said no)")
display(df_pos.groupBy("cpc_1012_end", "active_end").agg(F.count("*").alias("clients")).orderBy("cpc_1012_end", "active_end"))

# %% [6] write TWO tables to EDL - one per position, same clients in both (join on CLNT_NO).
# Path: the database's own registered location + table name (absolute). A relative path
# resolves into the user's HDFS home and the catalog then points somewhere else -> 0 rows on read.
T_ANCHOR = "unsub_cpc_1012_anchor_aug24"
T_TARGET = "unsub_cpc_1012_target_jun26"
DB_LOC = [r for r in spark.sql(f"DESCRIBE DATABASE {EDL_DB}").collect() if r[0].strip().lower().startswith("location")][0][1]
PATH_ANCHOR = f"{DB_LOC.rstrip('/')}/{T_ANCHOR}"
PATH_TARGET = f"{DB_LOC.rstrip('/')}/{T_TARGET}"
print("db location:", DB_LOC)

df_anchor = df_pos.select("CLNT_NO", "anchor_start_dt", "cpc_1012_start", "opted_in_start")
df_target = df_pos.select("CLNT_NO", "anchor_end_dt", "cpc_1012_end", "cpc_1012_writer_end",
                          "active_end", "opted_in_end", "sf_unsub_in_window", "first_sf_unsub_dt_tm")

spark.sql(f"DROP TABLE IF EXISTS {EDL_DB}.{T_ANCHOR}")
spark.sql(f"DROP TABLE IF EXISTS {EDL_DB}.{T_TARGET}")
df_anchor.write.mode("overwrite").option("path", PATH_ANCHOR).saveAsTable(f"{EDL_DB}.{T_ANCHOR}")
df_target.write.mode("overwrite").option("path", PATH_TARGET).saveAsTable(f"{EDL_DB}.{T_TARGET}")
print("written ->", f"{EDL_DB}.{T_ANCHOR}", "and", f"{EDL_DB}.{T_TARGET}")

# %% [7] proof 3: read BOTH back from the catalog - counts match, keys match 1:1, schema, 5 rows each
spark.catalog.refreshTable(f"{EDL_DB}.{T_ANCHOR}"); spark.catalog.refreshTable(f"{EDL_DB}.{T_TARGET}")
ta = spark.table(f"{EDL_DB}.{T_ANCHOR}")
tt = spark.table(f"{EDL_DB}.{T_TARGET}")
na, nt = ta.count(), tt.count()
assert na == n_rows and nt == n_rows, f"read-back anchor {na:,} / target {nt:,} != written {n_rows:,}"
nj = ta.join(tt, "CLNT_NO", "inner").count()
assert nj == n_rows, f"anchor-target join {nj:,} != {n_rows:,} - keys do not line up 1:1"
ta.printSchema(); display(ta.limit(5))
tt.printSchema(); display(tt.limit(5))
print(f"PASS: both tables hold {n_rows:,} rows and join 1:1 on CLNT_NO")
display(tt.groupBy("opted_in_end", "sf_unsub_in_window").count().orderBy("opted_in_end", "sf_unsub_in_window"))

# %% [8] hand-off note (copy into the reply to the stakeholder)
print(f"""
Two tables in {EDL_DB}, same {n_rows:,} clients, join on CLNT_NO:
  {T_ANCHOR}  - the universe: personal, active, CPC 1012 = 5001 at {START_ANCHOR}. cpc_1012_start, opted_in_start (=1).
  {T_TARGET}  - their position at {END_ANCHOR}: cpc_1012_end (raw), cpc_1012_writer_end (7020 = email backfeed),
                active_end, opted_in_end (1 = still 5001 AND active), sf_unsub_in_window / first_sf_unsub_dt_tm (Salesforce click, any list).
Caveat: opted_in_end = 1 AND sf_unsub_in_window = 1 = CPC still shows opted in, client unsubscribed in Salesforce (~474K at Jul-26).
Profitability tracked on CPC alone treats them as stayers. Full cut list is in the SQL header of the extract.
End anchor is {END_ANCHOR}: CPC July-26 was not loaded at request time.
""")
