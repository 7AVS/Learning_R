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

# liveness-checked connection: a stale EDW (pod restart / idle timeout) fails with
# "sending StartRequest message" on the next query - probe with SELECT 1, reconnect if dead
def _edw_alive():
    try:
        _c = EDW.cursor(); _c.execute("SELECT 1"); _c.fetchall()
        return True
    except Exception:
        return False

if "EDW" not in globals() or not _edw_alive():
    EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com",
                              user=input("Teradata username: "),
                              password=getpass.getpass("Teradata password: "),
                              logmech="LDAP")
    print("(new Teradata session)")

# KERNEL: run on the Lumina/YARN PySpark kernel (same as packs 44/44b) - `spark` is
# pre-initialized and Kerberos-authenticated there. Brain_Pyspark_Local_Mode has no
# Kerberos ticket: getOrCreate() builds a second, unauthenticated context and every
# HDFS call fails with "Client cannot authenticate via:[TOKEN, KERBEROS]".
try:
    spark                                   # pre-initialized session? use it, never rebuild
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

# every output table also drops as a CSV (some are too big for a notebook cell).
# Same writable-dir probe as unsub_unified: /home/jovyan first (Jupyter home), then
# fallbacks - so the files show up in the notebook file browser.
import os
OUT = None
for _cand in ("/home/jovyan", os.path.expanduser("~"), os.getcwd(), "/tmp"):
    try:
        _try = os.path.join(_cand, "pack45_outputs")
        os.makedirs(_try, exist_ok=True)
        _t = os.path.join(_try, ".writetest")
        open(_t, "w").write("x"); os.remove(_t)
        OUT = _try + os.sep
        break
    except Exception:
        continue

def save_csv(df, name):
    if OUT is None:
        print(f"({name}: no writable local dir - CSV skipped)")
        return
    df.to_csv(OUT + name + ".csv", index=False)
    print(f"saved {OUT}{name}.csv ({len(df):,} rows)")

print("EDW + spark ready | CSVs ->", OUT)

# %% [1] MONTHLY UNSUBS x MNE since 2024-01 — one SQL, clnt_no grain.
# Dedup: first unsub of the month per client (multi-MNE clients count once, under the
# first event's MNE) -> per-MNE rows SUM to distinct clients per month.
# Julian anchors: '2023274' = 2023-10-01 (frame floor minus 3mo), '2026212' = 2026-07-31.
# CACHE: first run lands the result to HDFS; reruns read it back in seconds. The SQL
# below is still the single audit artifact - the cache never changes the logic.
CACHE = "/user/427966379/unsub_cpc/pack45_cache/"

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
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
jvm = spark._jvm
fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

if fs.exists(jvm.org.apache.hadoop.fs.Path(CACHE + "unsub_mne/_SUCCESS")):
    vfb_un = spark.read.parquet(CACHE + "unsub_mne/").toPandas()
    print("(read from cache - delete the dir to force a re-pull)")
else:
    vfb_un = pd.read_sql(UNSUB_SQL, EDW)
    vfb_un.columns = [c.lower() for c in vfb_un.columns]
    spark.createDataFrame(vfb_un).write.mode("overwrite").parquet(CACHE + "unsub_mne/")
vfb_un = vfb_un.sort_values(["unsub_month", "mne"]).reset_index(drop=True)
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

for m0 in pd.date_range("2024-01-01", "2026-07-01", freq="MS").strftime("%Y-%m-%d"):
    m1 = (pd.Timestamp(m0) + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")
    target = f"{CACHE}send_mne/month={m0[:7]}/"
    if fs.exists(jvm.org.apache.hadoop.fs.Path(target + "_SUCCESS")):
        print(f"{m0[:7]}: cached - skipping")
        continue
    part = pd.read_sql(SEND_SQL.format(
        m0=m0, m1=m1,
        j_lo=_julian(pd.Timestamp(m0) - pd.offsets.MonthBegin(3)),   # multi-wave margin
        j_hi=_julian(pd.Timestamp(m1) - pd.offsets.Day(1))), EDW)
    part.columns = [c.lower() for c in part.columns]
    part.insert(0, "month", m0[:7])
    spark.createDataFrame(part).write.mode("overwrite").parquet(target)
    print(f"{m0[:7]}: {len(part)} mne rows landed")
vfb_sd = (spark.read.parquet(f"{CACHE}send_mne/").toPandas()
               .sort_values(["month", "mne"]).reset_index(drop=True))
print("Vendor SENDS monthly x MNE (distinct clnt_no; ALL_TOTAL = true monthly reach):")
display(vfb_sd)

# %% [3] CPC 1012 STANDING per month-end since 2024-01 — one SQL, no attribution
# (CPC carries no MNE). 5002 = explicit No, 5003 = blank, 5001 = Yes.
CPC_STANDING_SQL = """
    SELECT MTH_END_DT, CLNT_CONSENT_TYP,
           CAST(COUNT(*) AS BIGINT) AS n_clients
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012
      AND MTH_END_DT >= DATE '2024-01-31'
    GROUP BY 1, 2
    ORDER BY 1, 2
"""
if fs.exists(jvm.org.apache.hadoop.fs.Path(CACHE + "cpc_standing/_SUCCESS")):
    cpc_m = spark.read.parquet(CACHE + "cpc_standing/").toPandas()
    print("(read from cache)")
else:
    cpc_m = pd.read_sql(CPC_STANDING_SQL, EDW)
    cpc_m["MTH_END_DT"] = cpc_m["MTH_END_DT"].astype(str)
    spark.createDataFrame(cpc_m).write.mode("overwrite").parquet(CACHE + "cpc_standing/")
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
    target = f"{CACHE}ucp_flows/month={m1[:7]}/"
    if fs.exists(jvm.org.apache.hadoop.fs.Path(target + "_SUCCESS")):
        flow_parts.append(spark.read.parquet(target).toPandas())
        print(f"{m1[:7]}: cached - skipping")
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
    flow_parts.append(row)
    print(f"{m1[:7]}: lost {int(row.lost_consent[0]):,} | opted {int(row.opted_in[0]):,} "
          f"| attrition {int(row.attrition[0]):,}")
ucp_flow = pd.concat(flow_parts, ignore_index=True)
print(f"UCP monthly flows ({FLAG}):")
display(ucp_flow)

# %% [4b] UCP SAFETY CHECKS @ 2026-07-31 - (a) client-type mix: any NON-personal clients
# in the universe? (b) open-product distribution: clients with zero/null open products.
# Column-guarded: if the type column isn't found, prints the real column list - no guessing.
u_chk = spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE=2026-07-31/")
u_chk.createOrReplaceTempView("u_chk")

_type_col = next((c for c in u_chk.columns
                  if c.upper() in ("CLNT_TYP", "CLNT_TYP_CD", "CLNT_TYPE", "CLIENT_TYPE")), None)
if _type_col is None:
    print("No client-type column found under the expected names. UCP columns are:")
    print(sorted(u_chk.columns))
else:
    typ = spark.sql(f"""
        SELECT {_type_col} AS client_type, COUNT(*) AS n_clients
        FROM u_chk GROUP BY 1 ORDER BY 2 DESC
    """).toPandas()
    print(f"UCP client-type mix @ 2026-07-31 (column: {_type_col}):")
    display(typ)

prod = spark.sql("""
    SELECT CASE WHEN OPN_PROD_CNT IS NULL THEN 'null'
                WHEN OPN_PROD_CNT = 0     THEN '0 products'
                ELSE                           '1+ products' END AS open_products,
           COUNT(*) AS n_clients
    FROM u_chk GROUP BY 1 ORDER BY 1
""").toPandas()
print("UCP open-product distribution @ 2026-07-31 (zero/null = in universe but no open products):")
display(prod)

# joint cross-tab (type x products) - feeds population_profiles in [8]; the two
# marginals above are pivots of this
_tsel_chk = f"CAST({_type_col} AS STRING)" if _type_col else "'no type column'"
ucp_joint = spark.sql(f"""
    SELECT {_tsel_chk} AS client_type,
           CASE WHEN OPN_PROD_CNT IS NULL THEN 'null products'
                WHEN OPN_PROD_CNT = 0     THEN '0 products'
                ELSE                           '1+ products' END AS open_products,
           COUNT(*) AS n_clients
    FROM u_chk GROUP BY 1, 2 ORDER BY 3 DESC
""").toPandas()

# %% [5] CPC MONTHLY UNSUBS (1012 writes to 5002) split by WRITER: 7020 (the SFMC email
# backfeed) vs all other application systems. Source = DDWV01.CPC_RB_PREF (the proven
# mirror with the write timestamp) - monthly flow by CHG_TMSTMP month.
# SURVIVOR CAVEAT (state once, small): the standing table keeps only each client's
# LATEST 1012 row, so a 5002 later overwritten (re-consent) drops out of this flow -
# re-consent measured at ~4.7K over 2.5 years, so the shave is negligible.
CPC_WRITES_SQL = """
    SELECT TRIM(EXTRACT(YEAR FROM CAST(CHG_TMSTMP AS DATE))) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)) < 10
                       THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)))   AS chg_month,
           APP_SYS_CD                                             AS app_sys_cd,
           CAST(COUNT(*) AS BIGINT)                               AS n_writes_to_no
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012
      AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2024-01-01'
    GROUP BY 1, 2
    ORDER BY 1, 2
"""
cpc_writes = None
if fs.exists(jvm.org.apache.hadoop.fs.Path(CACHE + "cpc_writes/_SUCCESS")):
    cpc_writes = spark.read.parquet(CACHE + "cpc_writes/").toPandas()
    if "app_sys_cd" not in cpc_writes.columns:
        print("(cache has the old schema - re-pulling)")
        cpc_writes = None
    else:
        print("(read from cache)")
if cpc_writes is None:
    cpc_writes = pd.read_sql(CPC_WRITES_SQL, EDW)
    cpc_writes.columns = [c.lower() for c in cpc_writes.columns]
    spark.createDataFrame(cpc_writes).write.mode("overwrite").parquet(CACHE + "cpc_writes/")
print("CPC 1012 -> explicit No, monthly writes by writer (7020 = SFMC backfeed vs others):")
display(cpc_writes)

# %% [6] THE SUBSCRIBERS WATERFALL Jan-24 -> Jul-26 — ONE SQL, eight numbers, all joins
# server-side (two 26M-row CPC month slices + the vendor unsub set never leave Teradata).
# Sketch: pics/Screenshot 2026-08-19 142040. START = 1012 = 5001 @ 2024-01-31; unsub bar
# split CPC-closed / vendor / overlap; END official vs TRUE (minus vendor unsubs CPC
# never recorded).
WATERFALL_SQL = """
WITH a AS (      -- CPC book at the start anchor (Aug-24, per the target slide)
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_a
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2024-08-31'
),
b AS (           -- CPC book at the end anchor; APP_SYS_CD = the LAST write's system,
                 -- i.e. for a closed client, WHO closed the gate
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b, APP_SYS_CD AS writer_b
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2026-07-31'
),
v AS (           -- vendor unsub clients in the frame (locked EVENT+MASTER merge)
    SELECT DISTINCT m.CLNT_NO
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2024153' AND '2026212'
                  AND CLNT_NO IS NOT NULL) m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-09-01'
      AND e.disposition_dt_tm <  DATE '2026-08-01'
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
),
j AS (           -- every client either anchor, with the vendor flag + closing writer
    SELECT COALESCE(a.CLNT_NO, b.CLNT_NO) AS clnt_no, a.cons_a, b.cons_b, b.writer_b,
           CASE WHEN v.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS vendor_unsub
    FROM a
    FULL OUTER JOIN b ON a.CLNT_NO = b.CLNT_NO
    LEFT JOIN v ON v.CLNT_NO = COALESCE(a.CLNT_NO, b.CLNT_NO)
)
SELECT CAST(SUM(CASE WHEN cons_a = 5001 THEN 1 ELSE 0 END) AS BIGINT)                       AS start_5001_aug24,
       CAST(SUM(CASE WHEN (cons_a IS NULL OR cons_a <> 5001) AND cons_b = 5001
                     THEN 1 ELSE 0 END) AS BIGINT)                                          AS new_5001,
       -- the four unsub segments of the target slide:
       CAST(SUM(CASE WHEN cons_a = 5001 AND cons_b = 5002 AND writer_b = 7020
                      AND vendor_unsub = 0 THEN 1 ELSE 0 END) AS BIGINT)                    AS seg_email_cpc,
       CAST(SUM(CASE WHEN cons_a = 5001 AND cons_b = 5002 AND vendor_unsub = 1
                     THEN 1 ELSE 0 END) AS BIGINT)                                          AS seg_overlap,
       CAST(SUM(CASE WHEN cons_b = 5001 AND vendor_unsub = 1 THEN 1 ELSE 0 END) AS BIGINT)  AS seg_email_sf_open,
       CAST(SUM(CASE WHEN cons_a = 5001 AND cons_b = 5002 AND writer_b <> 7020
                      AND vendor_unsub = 0 THEN 1 ELSE 0 END) AS BIGINT)                    AS seg_ac_branch,
       CAST(SUM(CASE WHEN cons_a = 5001 AND (cons_b IS NULL OR cons_b NOT IN (5001, 5002))
                     THEN 1 ELSE 0 END) AS BIGINT)                                          AS left_other,
       CAST(SUM(CASE WHEN cons_b = 5001 THEN 1 ELSE 0 END) AS BIGINT)                       AS end_5001_jul26
FROM j
"""
sk = None
if fs.exists(jvm.org.apache.hadoop.fs.Path(CACHE + "waterfall/_SUCCESS")):
    sk = spark.read.parquet(CACHE + "waterfall/").toPandas()
    if "start_5001_aug24" not in sk.columns:
        print("(cache has the old schema - re-pulling)")
        sk = None
    else:
        print("(read from cache - delete the dir to force a re-pull)")
if sk is None:
    sk = pd.read_sql(WATERFALL_SQL, EDW)
    sk.columns = [c.lower() for c in sk.columns]
    spark.createDataFrame(sk).write.mode("overwrite").parquet(CACHE + "waterfall/")
r = sk.iloc[0]
cpc_closed = int(r.seg_email_cpc + r.seg_overlap + r.seg_ac_branch)
identity_ok = (r.start_5001_aug24 + r.new_5001 - cpc_closed - r.left_other) == r.end_5001_jul26
wf = pd.DataFrame([
    ["START: subscribers (1012 = 5001)", "2024-08-31", int(r.start_5001_aug24)],
    ["+ new subscribers (5001 by Jul-26)", "2024-09 → 2026-07", int(r.new_5001)],
    ["− E-mail CPC (closed by 7020, no Salesforce record)", "2024-09 → 2026-07", -int(r.seg_email_cpc)],
    ["− Overlap Salesforce & CPC (closed AND Salesforce record)", "2024-09 → 2026-07", -int(r.seg_overlap)],
    ["− E-mail Salesforce (Salesforce record, CPC still open)", "2024-09 → 2026-07", -int(r.seg_email_sf_open)],
    ["− AC/Branch & other writers (closed by non-7020 systems)", "2024-09 → 2026-07", -int(r.seg_ac_branch)],
    ["− left 5001 other (blank / no row at end)", "2024-09 → 2026-07", -int(r.left_other)],
    ["END official: subscribers (1012 = 5001)", "2026-07-31", int(r.end_5001_jul26)],
    ["END contactable: official minus E-mail Salesforce segment", "2026-07-31", int(r.end_5001_jul26 - r.seg_email_sf_open)],
], columns=["element", "period", "n_clients"])
print(f"Subscribers waterfall (target-slide segments), Aug-24 -> Jul-26 | CPC identity "
      f"{'HOLDS' if identity_ok else 'BROKEN'} (E-mail Salesforce segment sits outside "
      f"the CPC identity - it reduces contactable, not the CPC book):")
display(wf)

# %% [6b] THE WATERFALL CUBE - the analyst's intermediary table. One row per
# combination of every dimension the deck needs; the waterfall (any view, any
# universe filter) is BUILT from this CSV by pivoting, never asserted by code.
# Grain: consent_aug24 x consent_jul26 x closing_writer x salesforce_unsub x
#        client_type x open_products -> n_clients.
# Requires the client-level join (UCP attributes), so the pieces land once:
ANCHOR = "2024-08-31"

# (a) full CPC 1012 book at both anchors (ALL consent values + the last writer)
for _m, _name in [(ANCHOR, "cpc_full_aug24"), ("2026-07-31", "cpc_full_jul26")]:
    if fs.exists(jvm.org.apache.hadoop.fs.Path(CACHE + _name + "/_SUCCESS")):
        print(f"{_name}: already landed - skipping")
        continue
    chunks, total = [], 0
    for c in pd.read_sql(f"""
        SELECT CLNT_NO, CLNT_CONSENT_TYP, APP_SYS_CD
        FROM DDWV01.CPC_RB_PREF_MTHLY
        WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '{_m}'
    """, EDW, chunksize=1_000_000):
        chunks.append(c); total += len(c)
        print(f"  {_name}: pulled {total:,} rows...")
    spark.createDataFrame(pd.concat(chunks, ignore_index=True)) \
         .write.mode("overwrite").parquet(CACHE + _name + "/")
spark.read.parquet(CACHE + "cpc_full_aug24/").createOrReplaceTempView("cpc_a")
spark.read.parquet(CACHE + "cpc_full_jul26/").createOrReplaceTempView("cpc_b")

# (b) vendor unsub client list, frame Sep-24 -> Jul-26 (locked EVENT+MASTER merge)
if not fs.exists(jvm.org.apache.hadoop.fs.Path(CACHE + "vendor_unsub_clients/_SUCCESS")):
    chunks, total = [], 0
    for c in pd.read_sql("""
        SELECT DISTINCT m.CLNT_NO
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2024153' AND '2026212'
                      AND CLNT_NO IS NOT NULL) m
          ON  m.consumer_id_hashed = e.consumer_id_hashed
          AND m.TREATMENT_ID       = e.TREATMENT_ID
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= DATE '2024-09-01'
          AND e.disposition_dt_tm <  DATE '2026-08-01'
          AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
          AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    """, EDW, chunksize=500_000):
        chunks.append(c); total += len(c)
        print(f"  vendor unsub clients: pulled {total:,}...")
    spark.createDataFrame(pd.concat(chunks, ignore_index=True)) \
         .write.mode("overwrite").parquet(CACHE + "vendor_unsub_clients/")
spark.read.parquet(CACHE + "vendor_unsub_clients/").createOrReplaceTempView("vendor_u")

# (c) UCP attributes at the anchor (client type + open products)
if not fs.exists(jvm.org.apache.hadoop.fs.Path(f"{UCP_BASE}MONTH_END_DATE={ANCHOR}/")):
    _parts = sorted(p.getPath().getName() for p in
                    fs.listStatus(jvm.org.apache.hadoop.fs.Path(UCP_BASE)))
    print(f"WARNING: no UCP partition at {ANCHOR}; earliest available: "
          f"{_parts[0] if _parts else 'NONE'}")
spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={ANCHOR}/").createOrReplaceTempView("u_a")
_tcol = next((c for c in spark.table("u_a").columns
              if c.upper() in ("CLNT_TYP", "CLNT_TYP_CD", "CLNT_TYPE", "CLIENT_TYPE")), None)
_tsel = f"CAST(u.{_tcol} AS STRING)" if _tcol else "'no type column'"

# (d) the cube itself - every deck dimension on one aggregated table
waterfall_cube = spark.sql(f"""
    SELECT CASE WHEN a.CLNT_NO IS NULL      THEN 'no row'
                WHEN a.CLNT_CONSENT_TYP = 5001 THEN '5001 yes'
                WHEN a.CLNT_CONSENT_TYP = 5002 THEN '5002 no'
                WHEN a.CLNT_CONSENT_TYP = 5003 THEN '5003 blank'
                ELSE CAST(a.CLNT_CONSENT_TYP AS STRING) END        AS consent_aug24,
           CASE WHEN b.CLNT_NO IS NULL      THEN 'no row'
                WHEN b.CLNT_CONSENT_TYP = 5001 THEN '5001 yes'
                WHEN b.CLNT_CONSENT_TYP = 5002 THEN '5002 no'
                WHEN b.CLNT_CONSENT_TYP = 5003 THEN '5003 blank'
                ELSE CAST(b.CLNT_CONSENT_TYP AS STRING) END        AS consent_jul26,
           CASE WHEN b.APP_SYS_CD = 7020 THEN '7020 email backfeed'
                WHEN b.APP_SYS_CD IS NULL THEN 'n/a'
                ELSE 'other writers' END                           AS closing_writer,
           CASE WHEN v.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END       AS salesforce_unsub,
           CASE WHEN u.CLNT_NO IS NULL THEN 'not in UCP'
                ELSE {_tsel} END                                   AS client_type,
           CASE WHEN u.CLNT_NO IS NULL      THEN 'not in UCP'
                WHEN u.OPN_PROD_CNT IS NULL THEN 'null products'
                WHEN u.OPN_PROD_CNT = 0     THEN '0 products'
                ELSE                             '1+ products' END AS open_products,
           COUNT(*)                                                AS n_clients
    FROM cpc_a a
    FULL OUTER JOIN cpc_b b ON a.CLNT_NO = b.CLNT_NO
    LEFT JOIN vendor_u v ON v.CLNT_NO = COALESCE(a.CLNT_NO, b.CLNT_NO)
    LEFT JOIN u_a u      ON u.CLNT_NO = COALESCE(a.CLNT_NO, b.CLNT_NO)
    GROUP BY 1, 2, 3, 4, 5, 6
""").toPandas().sort_values("n_clients", ascending=False).reset_index(drop=True)
print(f"WATERFALL CUBE - {len(waterfall_cube)} rows; every deck view is a pivot of this "
      f"(start bar = consent_aug24 = '5001 yes' under whatever universe filter you choose):")
display(waterfall_cube)
save_csv(waterfall_cube, "waterfall_cube")

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

start_v   = r.start_5001_aug24 / 1e6
new_v     = r.new_5001 / 1e6
seg_cpc   = r.seg_email_cpc / 1e6
seg_ovl   = r.seg_overlap / 1e6
seg_sf    = r.seg_email_sf_open / 1e6
seg_acb   = r.seg_ac_branch / 1e6
other_v   = r.left_other / 1e6
end_off   = r.end_5001_jul26 / 1e6
end_true  = (r.end_5001_jul26 - r.seg_email_sf_open) / 1e6

lo = min(start_v, end_true) * 0.93
fig, ax = plt.subplots(figsize=(11.5, 6))
ax.bar(0, start_v - lo, bottom=lo, width=0.6, color=blue, zorder=3)
ax.text(0, start_v + 0.06, f"{start_v:,.2f}", ha="center", fontsize=11, fontweight="bold")
ax.bar(1, new_v, bottom=start_v, width=0.6, color=green, zorder=3)
ax.text(1, start_v + new_v + 0.06, f"+{new_v:.2f}", ha="center", fontsize=11, fontweight="bold")
top = start_v + new_v
base = top
for lbl, v, c in [("E-mail CPC (7020)", seg_cpc, "#6fa8dc"),
                  ("Overlap Salesforce & CPC", seg_ovl, "#7f7f7f"),
                  ("E-mail Salesforce (CPC open)", seg_sf, "#cfcfcf"),
                  ("AC/Branch & other writers", seg_acb, "#a4c2f4"),
                  ("Left 5001 other (blank/no row)", other_v, "#e8e8e8")]:
    ax.bar(2, -v, bottom=base, width=0.6, color=c, zorder=3,
           edgecolor="white", linewidth=1.2, label=lbl)
    if v > 0.03:
        ax.text(2, base - v/2, f"-{v:.2f}", ha="center", va="center", fontsize=9)
    base -= v
ax.text(2, top + 0.06, f"-{(top - base):.2f}", ha="center", fontsize=11, fontweight="bold")
ax.bar(3, end_true - lo, bottom=lo, width=0.6, color=gold, zorder=3)
ax.bar(3, seg_sf, bottom=end_true, width=0.6, color="#e7d091", zorder=3,
       label="E-mail Salesforce (in official, not contactable)")
ax.text(3, end_off + 0.06, f"{end_off:,.2f} official", ha="center", fontsize=10, fontweight="bold")
ax.text(3, end_true - 0.10, f"{end_true:,.2f} contactable", ha="center", fontsize=10,
        fontweight="bold", color="white")
ax.plot([0.3, 0.7], [start_v]*2, ls=":", lw=1.2, color=grey_line)
ax.plot([1.3, 1.7], [top]*2, ls=":", lw=1.2, color=grey_line)
ax.set_xticks([0, 1, 2, 3])
ax.set_xticklabels(["Contactable\n1012 = 5001\n2024-08", "Subscribes",
                    "Unsubscribes\n(split by system)", "Contactable\n2026-07"],
                   fontsize=10)
ax.set_ylabel("# Clients in MM")
ax.set_ylim(lo, top * 1.015)
ax.spines[["top", "right"]].set_visible(False)
ax.text(-0.68, lo, "≈", fontsize=14, color="#444444", va="center")
ax.legend(loc="upper left", fontsize=8.5, frameon=False)
ax.set_title("Landscape of the Personal Client contactable base — Aug-2024 to Jul-2026",
             fontweight="bold", fontsize=12, loc="left")
plt.tight_layout(); plt.show()

# --- 7b. vendor monthly unsubs, STACKED by LOB (data displayed first) ---
# LOB map: Cards = the verified 32-MNE catalog (unsub_unified MNE_CATALOG);
# Loyalty = VRE + VME (Avion Rewards email programs - EVIDENCE-BASED, not from an
# official LOB doc [flagged to Andre]); everything else = Others (incl. DI until
# its MNE codes are named).
CARDS_MNES_32 = {"FWC","VIF","PCQ","PCL","PCD","COB","CRV","VBA","WJR","MWA","VBU","CEC",
                 "AUH","CLL","MVP","BCO","VLI","WJF","WJA","POT","MET","WNH","OTC","RPF",
                 "HCD","VCL","BAF","CRO","AML","MEF","PON","CLI"}
LOYALTY_MNES = {"VRE", "VME"}
def _lob(mne):
    if mne in LOYALTY_MNES: return "Loyalty"
    if mne in CARDS_MNES_32: return "Cards"
    return "Others"
vfb_lob = (vfb_un.assign(lob=vfb_un["mne"].map(_lob))
                 .groupby(["unsub_month", "lob"], as_index=False)["n_clients"].sum()
                 .pivot_table(index="unsub_month", columns="lob", values="n_clients",
                              aggfunc="sum").fillna(0).reset_index())
print("Vendor monthly unsubs by LOB (plot data):")
display(vfb_lob)

fig, ax = plt.subplots(figsize=(11.5, 4.6))
xb = range(len(vfb_lob))
bottom = [0.0] * len(vfb_lob)
for lob, color in [("Loyalty", "#16436e"), ("Cards", "#6fa8dc"), ("Others", "#f1c232")]:
    if lob in vfb_lob.columns:
        vals = (vfb_lob[lob] / 1e3).tolist()
        ax.bar(xb, vals, bottom=bottom, width=0.65, color=color, label=lob, zorder=3)
        bottom = [a + b for a, b in zip(bottom, vals)]
ax.set_xticks(list(xb))
ax.set_xticklabels(vfb_lob["unsub_month"], fontsize=8.5, rotation=45)
ax.set_ylabel("clients (thousands)")
ax.legend(loc="upper right", fontsize=9, frameon=False, ncol=3)
ax.spines[["top", "right"]].set_visible(False)
ax.set_title("Monthly unsubscribes by LOB — vendor feedback (distinct clients), since 2024-01",
             fontweight="bold", fontsize=12, loc="left")
plt.tight_layout(); plt.show()

# --- 7c. CPC monthly 1012 -> 5002 writes, split 7020 vs other writers (data displayed;
# derived from the raw app_sys_cd table - full writer detail lives in the [5] CSV) ---
cpc_writes["writer"] = cpc_writes["app_sys_cd"].map(
    lambda c: "7020 email backfeed" if c == 7020 else "other writers")
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

# %% [8] OUTPUT CSVs - tidy GROUP BY summaries (Andre 2026-08-20): every column means
# ONE thing - a dimension holding values or a single measure. No pre-pivoted wides
# (no writes_7020 column headers), no label rows. Decode columns welcome (code + what
# it means). Andre builds every pivot himself from these. Five files:

# 1. vendor by month x mne - two measures adjacent, same grain
vm = (vfb_un.rename(columns={"unsub_month": "month", "n_clients": "unsub_clients"})
       .merge(vfb_sd.loc[vfb_sd.mne != "ALL_TOTAL"]
                    .rename(columns={"n_clients": "send_clients"}),
              on=["month", "mne"], how="outer")
       .sort_values(["month", "mne"]).reset_index(drop=True))
save_csv(vm, "vendor_monthly_mne")

# 2. CPC standing by month x consent code, with the decode column
_consent_decode = {5001: "yes", 5002: "no", 5003: "blank"}
cs = (cpc_m.rename(columns={"MTH_END_DT": "month_end", "CLNT_CONSENT_TYP": "consent_cd"})
       .assign(consent_meaning=lambda d: d["consent_cd"].map(_consent_decode).fillna("other"))
       [["month_end", "consent_cd", "consent_meaning", "n_clients"]])
save_csv(cs, "cpc_standing_monthly")

# 3. CPC writes by month x writer code, with the decode column (known codes only;
#    unknown codes stay blank rather than guessed)
_writer_decode = {7020: "SFMC email backfeed", 7001: "branch sales platform",
                  7003: "call centre", 7006: "internal batch", 7999: "default batch system"}
cwr = (cpc_writes[["chg_month", "app_sys_cd", "n_writes_to_no"]]
       .rename(columns={"chg_month": "month"})
       .assign(writer_desc=lambda d: d["app_sys_cd"].map(_writer_decode).fillna(""))
       [["month", "app_sys_cd", "writer_desc", "n_writes_to_no"]])
save_csv(cwr, "cpc_writes_monthly")

# 4. month-grain totals - each column a single measure of the month, no crossed dims
mt = (vfb_un_tot.rename(columns={"clients_unsub": "vendor_unsub_clients"})
      .merge(vfb_sd.loc[vfb_sd.mne == "ALL_TOTAL", ["month", "n_clients"]]
                   .rename(columns={"n_clients": "vendor_sends_total"}),
             on="month", how="outer")
      .merge(ucp_flow.rename(columns={"lost_consent": "ucp_lost_consent",
                                      "opted_in": "ucp_opted_in",
                                      "attrition": "ucp_attrition"}),
             on="month", how="outer")
      .sort_values("month").reset_index(drop=True))
save_csv(mt, "monthly_totals")

# 5. waterfall_cube.csv - saved in [6b]; already tidy (dimension columns + n_clients)
print("output contract: vendor_monthly_mne, cpc_standing_monthly, cpc_writes_monthly, "
      "monthly_totals, waterfall_cube ([6b]) - all tidy summaries, you pivot")
