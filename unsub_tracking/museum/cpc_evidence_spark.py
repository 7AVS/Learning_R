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

TRINO_HOST = "strplvaexh0001.fg.rbc.com"     # letter l confirmed by DNS (diagnosis cell 5); digit-1 spelling does not resolve
TD_HOST    = "Teradata-dns-sysa.fg.rbc.com"  # from your PROD profile; resolves to 10.174.185.83

EDL = connect(host=TRINO_HOST, port=8443, catalog="edl0_im", user=username,
              auth=BasicAuthentication(username, password), http_scheme="https", verify=False)

EDW = teradatasql.connect(host=TD_HOST, user=username, password=password, logmech="LDAP")

def edw_pd(sql, chunksize=1_000_000):
    # cursor -> pandas in chunks; safe for multi-million-row pulls
    parts = [c for c in pd.read_sql(sql, EDW, chunksize=chunksize)]
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

# %% [2] Reservoir helpers - land once to HDFS via spark, skip if already landed
BASE = "hdfs:///user/427966379/unsub_cpc/"

def landed(name):
    try:
        spark.read.parquet(BASE + name).limit(1).collect()
        return True
    except Exception:
        return False

def land(name, sql):
    if landed(name):
        print(name, ": already landed,", spark.read.parquet(BASE + name).count(), "rows - SKIP")
        return
    pdf = edw_pd(sql)
    assert len(pdf) > 0, name + " pulled zero rows - investigate before proceeding"
    spark.createDataFrame(pdf).write.mode("overwrite").parquet(BASE + name)
    nback = spark.read.parquet(BASE + name).count()
    assert nback == len(pdf), name + " HDFS readback mismatch: pulled " + str(len(pdf)) + " readback " + str(nback)
    print(name, ": landed", len(pdf), "rows, HDFS readback confirms", nback)

print("helpers defined: land()/landed() | reservoir BASE =", BASE)

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

# %% [7] EXTRACT cpc preference slice (5 switches, FULL history - consent standing needs latest answer ever; small table)
land("cpc_pref", """
SELECT CLNT_NO, PREF_ID, CLNT_CONSENT_TYP, CHG_TMSTMP, APP_SYS_CD
FROM DDWV01.CPC_RB_PREF_LOG
WHERE PREF_ID IN (1002, 1012, 1014, 1006, 1007)
""")

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

# %% [13] Load reservoir + derive (pure Spark/pandas from here - Teradata is done)
from pyspark.sql import functions as F, Window as W
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

ub  = spark.read.parquet(BASE + "unsub_base/*")
rec = spark.read.parquet(BASE + "q2_recipients/*").distinct()
cpc = spark.read.parquet(BASE + "cpc_pref")
ps  = spark.read.parquet(BASE + "postunsub_sends")
gm  = spark.read.parquet(BASE + "gate_mne_agg")

w = W.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").asc(), F.col("TREATMENT_ID").asc())
uf = (ub.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
        .select("CLNT_NO", "unsub_tm", F.expr("substring(TREATMENT_ID, 8, 3)").alias("unsub_mne")))
uf.cache(); print("unsub_first clients:", uf.count())

def cpc_standing(as_of_expr):
    ww = W.partitionBy("CLNT_NO", "PREF_ID").orderBy(F.col("CHG_TMSTMP").desc())
    base = cpc.filter("CHG_TMSTMP < " + as_of_expr) if as_of_expr else cpc
    return base.withColumn("rn", F.row_number().over(ww)).filter("rn = 1")

uf.createOrReplaceTempView("uf"); cpc.createOrReplaceTempView("cpc")

# %% [14] EVIDENCE 1 - two consent worlds, monthly volumes (35x claim)
spark.sql("""
SELECT 'email_unsub' AS consent_world, date_format(unsub_tm, 'yyyyMM') AS month_yyyymm, COUNT(*) AS clients
FROM uf GROUP BY 2
UNION ALL
SELECT 'cpc_optout', date_format(CHG_TMSTMP, 'yyyyMM'), COUNT(DISTINCT CLNT_NO)
FROM cpc
WHERE CLNT_CONSENT_TYP = 5002 AND PREF_ID IN (1002, 1012, 1014)
  AND CHG_TMSTMP >= DATE '2025-07-01' AND CHG_TMSTMP < DATE '2026-07-01'
GROUP BY 2
ORDER BY 1, 2
""").show(30, False)

# %% [15] EVIDENCE 2 - the blind gate + before/after split (99.6% claim)
st = cpc_standing(None).filter("CLNT_CONSENT_TYP = 5002 AND PREF_ID IN (1002, 1012, 1014)")
earliest = st.groupBy("CLNT_NO").agg(F.min("CHG_TMSTMP").alias("first_optout_tm"))
j = uf.join(earliest, "CLNT_NO", "left")
total   = j.count()
with_ex = j.filter("first_optout_tm IS NOT NULL").count()
before  = j.filter("first_optout_tm <  unsub_tm").count()
after   = j.filter("first_optout_tm >= unsub_tm").count()
print("unsub_clients_total          ", total)
print("with_explicit_cpc_optout     ", with_ex)
print("without_explicit_cpc_optout  ", total - with_ex)
print("optout_recorded_before_unsub ", before)
print("optout_recorded_after_unsub  ", after)
assert before + after == with_ex

# %% [16] EVIDENCE 3 - no bridge: flips by writer x had-prior-unsub
spark.sql("""
WITH flips AS (
  SELECT CLNT_NO, PREF_ID, APP_SYS_CD, CHG_TMSTMP FROM cpc
  WHERE CLNT_CONSENT_TYP = 5002 AND PREF_ID IN (1002, 1012, 1014)
    AND CHG_TMSTMP >= DATE '2025-07-01' AND CHG_TMSTMP < DATE '2026-07-01')
SELECT f.PREF_ID, f.APP_SYS_CD,
       CASE WHEN u.CLNT_NO IS NOT NULL AND u.unsub_tm < f.CHG_TMSTMP THEN 'Y' ELSE 'N' END AS had_prior_unsub,
       COUNT(*) AS flips
FROM flips f LEFT JOIN uf u ON u.CLNT_NO = f.CLNT_NO
GROUP BY 1, 2, 3 ORDER BY 1, 4 DESC
""").show(40, False)

# %% [17] EVIDENCE 4 - the leaking gate, per switch x exclusivity (19.2% / 47% claims)
gates = cpc_standing("DATE '2026-04-01'").filter("CLNT_CONSENT_TYP = 5002 AND PREF_ID IN (1002, 1012, 1014)") \
        .select("CLNT_NO", "PREF_ID")
nflags = gates.groupBy("CLNT_NO").agg(F.count("*").alias("n"))
gates = gates.join(nflags, "CLNT_NO").withColumn("exclusivity", F.when(F.col("n") == 1, "only_this_flag").otherwise("multi_flag"))
got = rec.withColumn("got", F.lit(1))
(gates.join(got, "CLNT_NO", "left")
      .groupBy("PREF_ID", "exclusivity")
      .agg(F.countDistinct("CLNT_NO").alias("optout_clients"), F.sum("got").alias("got_email_apr_jun"))
      .orderBy("PREF_ID", "exclusivity")).show(10, False)
allsw = gates.select("CLNT_NO").distinct().join(got, "CLNT_NO", "left")
print("ALL_SWITCHES:", allsw.count(), "optout clients,", allsw.filter("got = 1").count(), "got email Apr-Jun")

# %% [18] EVIDENCE 5 - does the channel honor unsubs? (10.4% claim)
pre = uf.filter("unsub_tm < DATE '2026-04-01'"); pre.cache()
pre_n = pre.count()
print("unsub_before_apr_clients ", pre_n)
print("got_email_apr_jun        ", pre.join(got, "CLNT_NO", "inner").count())

# %% [19] EVIDENCE 6+7 - which campaigns reach opted-out clients, per switch (from server-side aggregate)
we7 = W.partitionBy("PREF_ID").orderBy(F.col("clients").desc())
gm.withColumn("rk", F.row_number().over(we7)).filter("rk <= 12").orderBy("PREF_ID", "rk").show(48, False)

# %% [20] EVIDENCE 8 - the post-unsub waterfall (exclusion order fixed; labels match SQL version)
print("0 unsubscribed before Apr 2026 (cohort)                 ", pre_n)
r1 = ps.filter("disposition_cd = 1")
print("1 gross: received any send Apr-Jun                      ", r1.select("CLNT_NO").distinct().count())
r2 = r1.filter("disposition_dt_tm >= unsub_tm + INTERVAL 14 DAYS")
print("2 excl. sends within 14 days of unsub (CASL proxy)      ", r2.select("CLNT_NO").distinct().count())
bounced = ps.filter("disposition_cd = 5").select("CLNT_NO", "mne").distinct().withColumn("b", F.lit(1))
r3 = r2.join(bounced, ["CLNT_NO", "mne"], "left").filter("b IS NULL")
print("3 excl. clients whose every send hardbounced (mne proxy)", r3.select("CLNT_NO").distinct().count())
rec5001 = (cpc.filter("CLNT_CONSENT_TYP = 5001 AND PREF_ID IN (1002, 1012)")
              .select("CLNT_NO", "CHG_TMSTMP")
              .join(pre.select("CLNT_NO", "unsub_tm"), "CLNT_NO")
              .filter("CHG_TMSTMP > unsub_tm AND CHG_TMSTMP < DATE '2026-07-01'")
              .select("CLNT_NO").distinct().withColumn("rc", F.lit(1)))
r4 = r3.join(rec5001, "CLNT_NO", "left").filter("rc IS NULL")
print("4 excl. CPC-side re-consents after unsub                ", r4.select("CLNT_NO").distinct().count())
same = r4.filter("mne = unsub_mne").select("CLNT_NO").distinct()
cross = r4.select("CLNT_NO").distinct().subtract(same)
print("5 residual: cross-campaign only (different mne)         ", cross.count())
print("6 residual: same campaign as unsubbed (in-program leak) ", same.count())

# %% [21] SUMMARY pointer - each number above IS the summary; windows stated per cell
print("email unsubs, 12-mo total (Jul25-Jun26):", total)
print("story: E1 volumes 35x | E2 blind gate + split | E3 writers | E4 gate leak | E5 channel honor | E6/E7 gate x campaign | E8 waterfall")
