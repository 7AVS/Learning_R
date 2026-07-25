# RESERVOIR EXTRACT - Teradata -> HDFS landing ONLY. No evidence here: run once, then work in cpc_evidence_hdfs.py.
# Every land() is idempotent (skip-if-landed) and restartable per bite.

# %% [0] Bootstrap - install teradatasql from RBC artifactory (your existing pip idiom); run ONCE per kernel
# If this install fails, the platform ask is: "need teradatasql package or Teradata JDBC jar in Spark session"
get_ipython().system("./environment/bin/python -m pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")

# %% [1] Connections - EDL = Trino (your working cell), EDW = teradatasql (pure Python, no jar, LDAP per your PROD profile)
import getpass
import pandas as pd
import teradatasql

# pandas>=2.0 removed DataFrame.iteritems but Spark 3.3's createDataFrame still calls it - restore the alias
if not hasattr(pd.DataFrame, "iteritems"):
    pd.DataFrame.iteritems = pd.DataFrame.items
from trino.dbapi import connect
from trino.auth import BasicAuthentication

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")

TRINO_HOST = "strplvaexh0001.fg.rbc.com"     # letter l confirmed by DNS; digit-1 spelling does not resolve
TD_HOST    = "Teradata-dns-sysa.fg.rbc.com"  # from your PROD profile; resolves to 10.174.185.83

# verify=False is the platform norm (your working cell; verified TLS tested once 2026-07-24 - corp cert not in trust store)
import urllib3, warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")
EDL = connect(host=TRINO_HOST, port=8443, catalog="edl0_im", user=username,
              auth=BasicAuthentication(username, password), http_scheme="https", verify=False)

EDW = teradatasql.connect(host=TD_HOST, user=username, password=password, logmech="LDAP")

def edw_pd(sql, chunksize=1_000_000):
    # cursor -> pandas in chunks with live progress, so streaming never looks stuck
    import time
    parts, n, t0 = [], 0, time.time()
    for c in pd.read_sql(sql, EDW, chunksize=chunksize):
        parts.append(c); n += len(c)
        print("  ...", n, "rows pulled,", int(time.time() - t0), "s elapsed", flush=True)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

# PROOF, not prints: round-trip both connections and show what the SERVER returned
cur = EDW.cursor()
cur.execute("SELECT USER, SESSION, CURRENT_TIMESTAMP")
print("EDW round-trip (teradatasql) returned:", cur.fetchall())
cur.close()

tcur = EDL.cursor()
tcur.execute("SELECT 1")
print("EDL round-trip (trino) returned:", tcur.fetchall())
tcur.close()

# %% [2] Reservoir helpers - land once to HDFS, skip ONLY if the same query already landed (SQL manifest guards staleness)
import hashlib, re
from pyspark.sql import functions as F
BASE = "hdfs:///user/427966379/unsub_cpc/"

def _sqlkey(sql):
    return hashlib.md5(re.sub(r"\s+", " ", sql).strip().upper().encode()).hexdigest()

def landed(name):
    try:
        spark.read.parquet(BASE + name).limit(1).collect()
        return True
    except Exception:
        return False

def _to_spark(pdf, name):
    # all-NULL columns break Spark type inference - attach them as typed nulls and SAY which they were
    allnull = [c for c in pdf.columns if pdf[c].isna().all()]
    if not allnull:
        return spark.createDataFrame(pdf)
    print(name, ": columns 100% NULL in this pull ->", allnull, "(landed as string nulls)")
    sdf = spark.createDataFrame(pdf[[c for c in pdf.columns if c not in allnull]])
    for c in allnull:
        sdf = sdf.withColumn(c, F.lit(None).cast("string"))
    return sdf.select(*[c for c in pdf.columns])

def land(name, sql, replace=False):
    meta = BASE + "_meta/" + name.replace("/", "_")
    key = _sqlkey(sql)
    if landed(name) and not replace:
        try:
            old = spark.read.parquet(meta).collect()[0]
            if old["sql_md5"] == key:
                print(name, ": already landed with SAME query,", old["rows"], "rows (", old["landed_at"], ") - SKIP")
                return
            print(name, ": !! QUERY CHANGED since it landed", old["landed_at"], "- OLD data kept. Rerun land(name, sql, replace=True) to re-pull.")
            return
        except Exception:
            print(name, ": landed pre-manifest (no stored SQL). If you changed the query since, rerun with replace=True.")
            return
    pdf = edw_pd(sql)
    assert len(pdf) > 0, name + " pulled zero rows - investigate before proceeding"
    _to_spark(pdf, name).write.mode("overwrite").parquet(BASE + name)
    nback = spark.read.parquet(BASE + name).count()
    assert nback == len(pdf), name + " HDFS readback mismatch: pulled " + str(len(pdf)) + " readback " + str(nback)
    spark.createDataFrame([(name, key, sql, __import__("datetime").datetime.now().isoformat(), len(pdf))],
                          ["name", "sql_md5", "sql_text", "landed_at", "rows"]).write.mode("overwrite").parquet(meta)
    print(name, ": landed", len(pdf), "rows, HDFS readback confirms", nback, "| manifest written")

print("helpers defined: land()/landed() with SQL manifest | reservoir BASE =", BASE)

# %% [3] EXTRACT unsub_base chunk 1/4 (EVENT disp=4 Jul25-Jun26; MASTER load_tm 2025-06..2025-10)
land("unsub_base/c1", """
SELECT DISTINCT m.CLNT_NO, e.disposition_dt_tm AS unsub_tm, m.TREATMENT_ID
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2025-07-01' AND e.disposition_dt_tm < DATE '2026-07-01'
  AND m.load_tm >= DATE '2025-06-01' AND m.load_tm < DATE '2025-10-01'
""")

# %% [4] EXTRACT unsub_base chunk 2/4 (MASTER load_tm 2025-10..2026-02)
land("unsub_base/c2", """
SELECT DISTINCT m.CLNT_NO, e.disposition_dt_tm AS unsub_tm, m.TREATMENT_ID
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2025-07-01' AND e.disposition_dt_tm < DATE '2026-07-01'
  AND m.load_tm >= DATE '2025-10-01' AND m.load_tm < DATE '2026-02-01'
""")

# %% [5] EXTRACT unsub_base chunk 3/4 (MASTER load_tm 2026-02..2026-05)
land("unsub_base/c3", """
SELECT DISTINCT m.CLNT_NO, e.disposition_dt_tm AS unsub_tm, m.TREATMENT_ID
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2025-07-01' AND e.disposition_dt_tm < DATE '2026-07-01'
  AND m.load_tm >= DATE '2026-02-01' AND m.load_tm < DATE '2026-05-01'
""")

# %% [6] EXTRACT unsub_base chunk 4/4 (MASTER load_tm 2026-05..2026-08)
land("unsub_base/c4", """
SELECT DISTINCT m.CLNT_NO, e.disposition_dt_tm AS unsub_tm, m.TREATMENT_ID
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2025-07-01' AND e.disposition_dt_tm < DATE '2026-07-01'
  AND m.load_tm >= DATE '2026-05-01' AND m.load_tm < DATE '2026-08-01'
""")

# %% [7a] SIZE FIRST - measure before pulling (4 rows back in seconds; decides transfer expectations on data, not estimates)
print(edw_pd("""
SELECT PREF_ID, COUNT(*) AS hist_rows, COUNT(DISTINCT CLNT_NO) AS clients
FROM DDWV01.CPC_RB_PREF_LOG
WHERE PREF_ID IN (1002, 1012, 1014, 1006)
GROUP BY 1 ORDER BY 1
"""))

# %% [7] EXTRACT cpc preference slice (FULL history per switch - one bite per switch, each restartable; 1007 dropped, unused by any evidence)
for _pref in [1002, 1012, 1014, 1006]:
    land("cpc_pref/p" + str(_pref), """
SELECT CLNT_NO, PREF_ID, CLNT_CONSENT_TYP, CHG_TMSTMP, APP_SYS_CD
FROM DDWV01.CPC_RB_PREF_LOG
WHERE PREF_ID = """ + str(_pref))

# %% [8] EXTRACT q2 recipients (DISTINCT clients with a send, per month - pandas-sized; feeds E4/E5)
land("q2_recipients/m04", """
SELECT DISTINCT m.CLNT_NO
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd = 1
  AND e.disposition_dt_tm >= DATE '2026-04-01' AND e.disposition_dt_tm < DATE '2026-05-01'
  AND m.load_tm >= DATE '2026-03-01' AND m.load_tm < DATE '2026-06-01'
""")

# %% [9] EXTRACT q2 recipients May
land("q2_recipients/m05", """
SELECT DISTINCT m.CLNT_NO
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd = 1
  AND e.disposition_dt_tm >= DATE '2026-05-01' AND e.disposition_dt_tm < DATE '2026-06-01'
  AND m.load_tm >= DATE '2026-04-01' AND m.load_tm < DATE '2026-07-01'
""")

# %% [10] EXTRACT q2 recipients June
land("q2_recipients/m06", """
SELECT DISTINCT m.CLNT_NO
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd = 1
  AND e.disposition_dt_tm >= DATE '2026-06-01' AND e.disposition_dt_tm < DATE '2026-07-01'
  AND m.load_tm >= DATE '2026-05-01' AND m.load_tm < DATE '2026-08-01'
""")

# %% [11] EXTRACT post-unsub send detail (cohort-restricted server-side: only pre-Apr unsubscribers' sends; pandas-sized; feeds E8)
land("postunsub_sends", """
WITH ub AS (
  SELECT m.CLNT_NO, e.disposition_dt_tm AS unsub_tm, m.TREATMENT_ID,
         ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO ORDER BY e.disposition_dt_tm ASC, m.TREATMENT_ID ASC) AS rn
  FROM DTZV01.VENDOR_FEEDBACK_EVENT e
  INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
  WHERE e.disposition_cd = 4
    AND e.disposition_dt_tm >= DATE '2025-07-01' AND e.disposition_dt_tm < DATE '2026-04-01'
    AND m.load_tm >= DATE '2025-06-01' AND m.load_tm < DATE '2026-05-01'
),
cohort AS (SELECT CLNT_NO, unsub_tm, SUBSTR(TREATMENT_ID, 8, 3) AS unsub_mne FROM ub WHERE rn = 1)
SELECT c.CLNT_NO, c.unsub_tm, c.unsub_mne,
       SUBSTR(m.TREATMENT_ID, 8, 3) AS mne, e.disposition_cd, e.disposition_dt_tm
FROM cohort c
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m ON m.CLNT_NO = c.CLNT_NO
INNER JOIN DTZV01.VENDOR_FEEDBACK_EVENT e
  ON e.consumer_id_hashed = m.consumer_id_hashed AND e.TREATMENT_ID = m.TREATMENT_ID
WHERE e.disposition_cd IN (1, 5)
  AND e.disposition_dt_tm >= DATE '2026-04-01' AND e.disposition_dt_tm < DATE '2026-07-01'
  AND m.load_tm >= DATE '2026-03-01' AND m.load_tm < DATE '2026-08-01'
""")

# %% [12] EXTRACT gate x campaign aggregate (server-side; tiny result; feeds E6/E7)
land("gate_mne_agg", """
WITH latest AS (
  SELECT CLNT_NO, PREF_ID, CLNT_CONSENT_TYP,
         ROW_NUMBER() OVER (PARTITION BY CLNT_NO, PREF_ID ORDER BY CHG_TMSTMP DESC) AS rn
  FROM DDWV01.CPC_RB_PREF_LOG
  WHERE PREF_ID IN (1002, 1012, 1014, 1006) AND CHG_TMSTMP < DATE '2026-04-01'
),
gates AS (SELECT CLNT_NO, PREF_ID FROM latest WHERE rn = 1 AND CLNT_CONSENT_TYP = 5002)
SELECT g.PREF_ID, SUBSTR(m.TREATMENT_ID, 8, 3) AS mne,
       COUNT(DISTINCT g.CLNT_NO) AS clients, COUNT(*) AS send_rows
FROM gates g
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m ON m.CLNT_NO = g.CLNT_NO
INNER JOIN DTZV01.VENDOR_FEEDBACK_EVENT e
  ON e.consumer_id_hashed = m.consumer_id_hashed AND e.TREATMENT_ID = m.TREATMENT_ID
WHERE e.disposition_cd = 1
  AND e.disposition_dt_tm >= DATE '2026-04-01' AND e.disposition_dt_tm < DATE '2026-07-01'
  AND m.load_tm >= DATE '2026-03-01' AND m.load_tm < DATE '2026-08-01'
GROUP BY 1, 2
""")

# %% [13] EXTRACT blank-MNE identification sample (red-team #3's last hole: WHAT are the treatments with blank pos 8-10?)
# Tiny pull: top 500 blank-mne treatments by send volume, Apr-Jun era, with subject line + channel/category fields.
# Subject lines settle marketing-vs-service; feeds R1 in cpc_evidence_hdfs.py.
land("blank_mne_sample", """
SELECT TOP 500
       m.TREATMENT_ID, m.email_subj_line, m.channel_type_cd, m.cntct_mthd_typ,
       m.category_cd, m.sub_category_cd, m.product_code,
       COUNT(*) AS send_rows
FROM DTZV01.VENDOR_FEEDBACK_MASTER m
WHERE m.load_tm >= DATE '2026-03-01' AND m.load_tm < DATE '2026-08-01'
  AND TRIM(COALESCE(SUBSTR(m.TREATMENT_ID, 8, 3), '')) = ''
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY send_rows DESC
""")

# %% [14] EXTRACT q2 recipients NAMED-campaign only (DEFAULT/blank-MNE stream excluded server-side; feeds E9 recut)
# Blank-MNE verdict 2026-07-25: TREATMENT_ID='DEFAULT' = mail outside campaign taxonomy (service + broken-template + untagged marketing).
for _m, _w in {"m04": ("2026-04-01", "2026-05-01", "2026-03-01", "2026-06-01"),
               "m05": ("2026-05-01", "2026-06-01", "2026-04-01", "2026-07-01"),
               "m06": ("2026-06-01", "2026-07-01", "2026-05-01", "2026-08-01")}.items():
    land("q2_recipients_named/" + _m, """
SELECT DISTINCT m.CLNT_NO
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd = 1
  AND e.disposition_dt_tm >= DATE '%s' AND e.disposition_dt_tm < DATE '%s'
  AND m.load_tm >= DATE '%s' AND m.load_tm < DATE '%s'
  AND TRIM(COALESCE(SUBSTR(m.TREATMENT_ID, 8, 3), '')) <> ''
""" % _w)

# %% [15] EXTRACT all-switch CPC landing for the unsub cohort (T1 blind-spot: do unsubs write to a switch we DON'T watch?)
# Cohort-restricted (unsubscribers only) + CHG_TMSTMP >= 2025-07-01 + No/blank writes only -> bounded; same join shape as [11]/[12].
# NO PREF_ID filter here (that is the whole point). Feeds archaeology/23_cpc_landscape.py T1a/T1b.
_landing_sql = """
SELECT c.CLNT_NO, c.PREF_ID, c.CLNT_CONSENT_TYP, c.CHG_TMSTMP, c.APP_SYS_CD
FROM DDWV01.CPC_RB_PREF_LOG c
INNER JOIN (
  SELECT DISTINCT m.CLNT_NO
  FROM DTZV01.VENDOR_FEEDBACK_EVENT e
  INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
  WHERE e.disposition_cd = 4
    AND e.disposition_dt_tm >= DATE '2025-07-01' AND e.disposition_dt_tm < DATE '2026-07-01'
    AND m.load_tm >= DATE '2025-06-01' AND m.load_tm < DATE '2026-08-01'
) u ON c.CLNT_NO = u.CLNT_NO
WHERE c.CHG_TMSTMP >= DATE '2025-07-01'
  AND (c.CLNT_CONSENT_TYP IN (5002, 5003) OR c.CLNT_CONSENT_TYP IS NULL)
"""
# size probe first (measure before pulling - 3 numbers back fast)
print("size probe (rows/clients/switches to land):")
print(edw_pd("SELECT COUNT(*) AS landing_rows, COUNT(DISTINCT CLNT_NO) AS clients, COUNT(DISTINCT PREF_ID) AS switches FROM ("
             + _landing_sql + ") x"))
land("cpc_landing_allsw", _landing_sql)
# FALLBACK if TDWM kills the single pull: land per CHG_TMSTMP quarter, then 23 reads cpc_landing_allsw/* (recursiveFileLookup):
# for _a,_b in [("2025-07-01","2025-10-01"),("2025-10-01","2026-01-01"),("2026-01-01","2026-04-01"),("2026-04-01","2026-07-01"),("2026-07-01","2026-10-01")]:
#     land("cpc_landing_allsw/"+_a[:7], _landing_sql + " AND c.CHG_TMSTMP >= DATE '%s' AND c.CHG_TMSTMP < DATE '%s'" % (_a,_b))

# %% [16] EXTRACT email-address count per 1002=No client (GRANULARITY GUARD for the T3 19% leak)
# Andre's catch: vendor grain = email address (consumer_id_hashed), CPC/joins = CLNT_NO. If a 1002=No client holds
# several emails, "got email" may be a NEW address, not the do-not-solicit gate failing. Small cohort (~49K), built like [12].
land("no1002_email_card", """
WITH latest AS (
  SELECT CLNT_NO, CLNT_CONSENT_TYP,
         ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) AS rn
  FROM DDWV01.CPC_RB_PREF_LOG
  WHERE PREF_ID = 1002 AND CHG_TMSTMP < DATE '2026-04-01'
),
no1002 AS (SELECT CLNT_NO FROM latest WHERE rn = 1 AND CLNT_CONSENT_TYP = 5002)
SELECT m.CLNT_NO,
       COUNT(DISTINCT m.consumer_id_hashed) AS n_emails,
       COUNT(*) AS master_rows
FROM DTZV01.VENDOR_FEEDBACK_MASTER m
INNER JOIN no1002 g ON g.CLNT_NO = m.CLNT_NO
WHERE m.load_tm >= DATE '2025-06-01' AND m.load_tm < DATE '2026-08-01'
GROUP BY m.CLNT_NO
""")

print("reservoir complete (incl. cpc_landing_allsw + no1002_email_card) - evidence: cpc_evidence_hdfs.py | landscape: archaeology/23_cpc_landscape.py")
