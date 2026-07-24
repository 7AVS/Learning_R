# %% [0] Connections - EDL = Trino (your working cell), EDW = Teradata via Spark JDBC (platform sample + your PROD profile: LOGMECH=LDAP)
# Paste your two hostnames. Everything else is your validated patterns verbatim.
import getpass
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")

TRINO_HOST = "<YOUR_TRINO_HOST>"            # from your working Trino cell
TD_HOST    = "<YOUR_TERADATA_HOST>"         # from your PROD profile (Teradata-dns-...)

EDL = connect(host=TRINO_HOST, port=8443, catalog="edl0_im", user=username,
              auth=BasicAuthentication(username, password), http_scheme="https", verify=False)

TD_DRIVER = "com.teradata.jdbc.TeraDriver"
TD_URL = "jdbc:teradata://" + TD_HOST + "/LOGMECH=LDAP,TMODE=TERA,CHARSET=UTF8,ENCRYPTDATA=ON"

def edw(sql):
    # platform sample's load_data, with the LDAP-corrected URL; returns a Spark dataframe
    return (spark.read.format("jdbc")
            .option("driver", TD_DRIVER)
            .option("url", TD_URL)
            .option("dbtable", "(" + sql + ") as src")
            .option("user", username)
            .option("password", password)
            .load())

print("EDL (trino) + edw() (teradata jdbc) ready.")

# %% [1] Reservoir helpers - land once to HDFS, skip if already landed
BASE = "hdfs:///user/427966379/unsub_cpc/"

def landed(name):
    try:
        spark.read.parquet(BASE + name).limit(1).collect()
        return True
    except Exception:
        return False

def land(name, sql):
    if landed(name):
        n = spark.read.parquet(BASE + name).count()
        print(name, ": already landed,", n, "rows - SKIP")
        return
    df = edw(sql)
    df.write.mode("overwrite").parquet(BASE + name)
    n = spark.read.parquet(BASE + name).count()
    assert n > 0, name + " landed zero rows - investigate before proceeding"
    print(name, ": landed", n, "rows")

# %% [2] EXTRACT unsub_base chunk 1/4 (EVENT disp=4 Jul25-Jun26; MASTER load_tm 2025-06..2025-10)
land("unsub_base/c1", """
SELECT DISTINCT m.CLNT_NO, e.disposition_dt_tm AS unsub_tm, m.TREATMENT_ID
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2025-07-01' AND e.disposition_dt_tm < DATE '2026-07-01'
  AND m.load_tm >= DATE '2025-06-01' AND m.load_tm < DATE '2025-10-01'
""")

# %% [3] EXTRACT unsub_base chunk 2/4 (MASTER load_tm 2025-10..2026-02)
land("unsub_base/c2", """
SELECT DISTINCT m.CLNT_NO, e.disposition_dt_tm AS unsub_tm, m.TREATMENT_ID
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2025-07-01' AND e.disposition_dt_tm < DATE '2026-07-01'
  AND m.load_tm >= DATE '2025-10-01' AND m.load_tm < DATE '2026-02-01'
""")

# %% [4] EXTRACT unsub_base chunk 3/4 (MASTER load_tm 2026-02..2026-05)
land("unsub_base/c3", """
SELECT DISTINCT m.CLNT_NO, e.disposition_dt_tm AS unsub_tm, m.TREATMENT_ID
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2025-07-01' AND e.disposition_dt_tm < DATE '2026-07-01'
  AND m.load_tm >= DATE '2026-02-01' AND m.load_tm < DATE '2026-05-01'
""")

# %% [5] EXTRACT unsub_base chunk 4/4 (MASTER load_tm 2026-05..2026-08)
land("unsub_base/c4", """
SELECT DISTINCT m.CLNT_NO, e.disposition_dt_tm AS unsub_tm, m.TREATMENT_ID
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2025-07-01' AND e.disposition_dt_tm < DATE '2026-07-01'
  AND m.load_tm >= DATE '2026-05-01' AND m.load_tm < DATE '2026-08-01'
""")

# %% [6] EXTRACT q2 send detail April (EVENT disp IN (1,5); MASTER +/-1mo margin)
land("q2_send_detail/m04", """
SELECT m.CLNT_NO, SUBSTR(m.TREATMENT_ID, 8, 3) AS mne, e.disposition_cd, e.disposition_dt_tm
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd IN (1, 5)
  AND e.disposition_dt_tm >= DATE '2026-04-01' AND e.disposition_dt_tm < DATE '2026-05-01'
  AND m.load_tm >= DATE '2026-03-01' AND m.load_tm < DATE '2026-06-01'
""")

# %% [7] EXTRACT q2 send detail May
land("q2_send_detail/m05", """
SELECT m.CLNT_NO, SUBSTR(m.TREATMENT_ID, 8, 3) AS mne, e.disposition_cd, e.disposition_dt_tm
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd IN (1, 5)
  AND e.disposition_dt_tm >= DATE '2026-05-01' AND e.disposition_dt_tm < DATE '2026-06-01'
  AND m.load_tm >= DATE '2026-04-01' AND m.load_tm < DATE '2026-07-01'
""")

# %% [8] EXTRACT q2 send detail June
land("q2_send_detail/m06", """
SELECT m.CLNT_NO, SUBSTR(m.TREATMENT_ID, 8, 3) AS mne, e.disposition_cd, e.disposition_dt_tm
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd IN (1, 5)
  AND e.disposition_dt_tm >= DATE '2026-06-01' AND e.disposition_dt_tm < DATE '2026-07-01'
  AND m.load_tm >= DATE '2026-05-01' AND m.load_tm < DATE '2026-08-01'
""")

# %% [9] EXTRACT cpc preference slice (5 switches, FULL history - consent standing needs the latest answer ever; small table)
land("cpc_pref", """
SELECT CLNT_NO, PREF_ID, CLNT_CONSENT_TYP, CHG_TMSTMP, APP_SYS_CD
FROM DDWV01.CPC_RB_PREF_LOG
WHERE PREF_ID IN (1002, 1012, 1014, 1006, 1007)
""")

# %% [10] Register views - everything below is Spark on the reservoir, no Teradata
from pyspark.sql import functions as F, Window as W
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

ub  = spark.read.parquet(BASE + "unsub_base/*")
sd  = spark.read.parquet(BASE + "q2_send_detail/*")
cpc = spark.read.parquet(BASE + "cpc_pref")

w = W.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").asc(), F.col("TREATMENT_ID").asc())
uf = (ub.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
        .select("CLNT_NO", "unsub_tm", F.expr("substring(TREATMENT_ID, 8, 3)").alias("unsub_mne")))
uf.cache(); print("unsub_first clients:", uf.count())

def cpc_standing(as_of_expr):
    ww = W.partitionBy("CLNT_NO", "PREF_ID").orderBy(F.col("CHG_TMSTMP").desc())
    base = cpc.filter("CHG_TMSTMP < " + as_of_expr) if as_of_expr else cpc
    return base.withColumn("rn", F.row_number().over(ww)).filter("rn = 1")

uf.createOrReplaceTempView("uf"); sd.createOrReplaceTempView("sd"); cpc.createOrReplaceTempView("cpc")

# %% [11] EVIDENCE 1 - two consent worlds, monthly volumes (35x claim)
e1 = spark.sql("""
SELECT 'email_unsub' AS consent_world, date_format(unsub_tm, 'yyyyMM') AS month_yyyymm, COUNT(*) AS clients
FROM uf GROUP BY 2
UNION ALL
SELECT 'cpc_optout', date_format(CHG_TMSTMP, 'yyyyMM'), COUNT(DISTINCT CLNT_NO)
FROM cpc
WHERE CLNT_CONSENT_TYP = 5002 AND PREF_ID IN (1002, 1012, 1014)
  AND CHG_TMSTMP >= DATE '2025-07-01' AND CHG_TMSTMP < DATE '2026-07-01'
GROUP BY 2
ORDER BY 1, 2
""")
e1.show(30, False)

# %% [12] EVIDENCE 2 - the blind gate + before/after split (99.6% claim)
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

# %% [13] EVIDENCE 3 - no bridge: flips by writer x had-prior-unsub
e3 = spark.sql("""
WITH flips AS (
  SELECT CLNT_NO, PREF_ID, APP_SYS_CD, CHG_TMSTMP FROM cpc
  WHERE CLNT_CONSENT_TYP = 5002 AND PREF_ID IN (1002, 1012, 1014)
    AND CHG_TMSTMP >= DATE '2025-07-01' AND CHG_TMSTMP < DATE '2026-07-01')
SELECT f.PREF_ID, f.APP_SYS_CD,
       CASE WHEN u.CLNT_NO IS NOT NULL AND u.unsub_tm < f.CHG_TMSTMP THEN 'Y' ELSE 'N' END AS had_prior_unsub,
       COUNT(*) AS flips
FROM flips f LEFT JOIN uf u ON u.CLNT_NO = f.CLNT_NO
GROUP BY 1, 2, 3 ORDER BY 1, 4 DESC
""")
e3.show(40, False)

# %% [14] EVIDENCE 4 - the leaking gate, per switch x exclusivity (19.2% / 47% claims)
gates = cpc_standing("DATE '2026-04-01'").filter("CLNT_CONSENT_TYP = 5002 AND PREF_ID IN (1002, 1012, 1014)") \
        .select("CLNT_NO", "PREF_ID")
nflags = gates.groupBy("CLNT_NO").agg(F.count("*").alias("n"))
gates = gates.join(nflags, "CLNT_NO").withColumn("exclusivity", F.when(F.col("n") == 1, "only_this_flag").otherwise("multi_flag"))
got = sd.filter("disposition_cd = 1").select("CLNT_NO").distinct().withColumn("got", F.lit(1))
e4 = (gates.join(got, "CLNT_NO", "left")
      .groupBy("PREF_ID", "exclusivity")
      .agg(F.countDistinct("CLNT_NO").alias("optout_clients"), F.sum("got").alias("got_email_apr_jun"))
      .orderBy("PREF_ID", "exclusivity"))
e4.show(10, False)
allsw = gates.select("CLNT_NO").distinct().join(got, "CLNT_NO", "left")
print("ALL_SWITCHES:", allsw.count(), "optout clients,", allsw.filter("got = 1").count(), "got email Apr-Jun")

# %% [15] EVIDENCE 5 - does the channel honor unsubs? (10.4% claim)
pre = uf.filter("unsub_tm < DATE '2026-04-01'")
pre.cache()
pre_n = pre.count()
got5 = pre.join(got, "CLNT_NO", "inner").count()
print("unsub_before_apr_clients ", pre_n)
print("got_email_apr_jun        ", got5)

# %% [16] EVIDENCE 6+7 - which campaigns reach opted-out clients, per switch (incl. 1006 topic gate)
g4 = cpc_standing("DATE '2026-04-01'").filter("CLNT_CONSENT_TYP = 5002 AND PREF_ID IN (1002, 1012, 1014, 1006)") \
     .select("CLNT_NO", "PREF_ID")
sends1 = sd.filter("disposition_cd = 1")
e7 = (g4.join(sends1, "CLNT_NO")
      .groupBy("PREF_ID", "mne")
      .agg(F.countDistinct("CLNT_NO").alias("clients"), F.count("*").alias("send_rows")))
we7 = W.partitionBy("PREF_ID").orderBy(F.col("clients").desc())
e7.withColumn("rk", F.row_number().over(we7)).filter("rk <= 12").orderBy("PREF_ID", "rk").show(48, False)

# %% [17] EVIDENCE 8 - the post-unsub waterfall (exclusion order fixed; labels match SQL version)
ps = sd.join(pre.select("CLNT_NO", "unsub_tm", "unsub_mne"), "CLNT_NO", "inner")
print("0 unsubscribed before Apr 2026 (cohort)                 ", pre_n)
r1 = ps.filter("disposition_cd = 1")
print("1 gross: received any send Apr-Jun                      ", r1.select("CLNT_NO").distinct().count())
r2 = r1.filter("disposition_dt_tm >= unsub_tm + INTERVAL 14 DAYS")
print("2 excl. sends within 14 days of unsub (CASL proxy)      ", r2.select("CLNT_NO").distinct().count())
bounced = ps.filter("disposition_cd = 5").select("CLNT_NO", "mne").distinct().withColumn("b", F.lit(1))
r3 = r2.join(bounced, ["CLNT_NO", "mne"], "left").filter("b IS NULL")
print("3 excl. clients whose every send hardbounced (mne proxy)", r3.select("CLNT_NO").distinct().count())
rec = (cpc.filter("CLNT_CONSENT_TYP = 5001 AND PREF_ID IN (1002, 1012)")
          .select("CLNT_NO", "CHG_TMSTMP").join(pre.select("CLNT_NO", "unsub_tm"), "CLNT_NO")
          .filter("CHG_TMSTMP > unsub_tm AND CHG_TMSTMP < DATE '2026-07-01'")
          .select("CLNT_NO").distinct().withColumn("rc", F.lit(1)))
r4 = r3.join(rec, "CLNT_NO", "left").filter("rc IS NULL")
print("4 excl. CPC-side re-consents after unsub                ", r4.select("CLNT_NO").distinct().count())
same = r4.filter("mne = unsub_mne").select("CLNT_NO").distinct()
cross = r4.select("CLNT_NO").distinct().subtract(same)
print("5 residual: cross-campaign only (different mne)         ", cross.count())
print("6 residual: same campaign as unsubbed (in-program leak) ", same.count())

# %% [18] SUMMARY pointer - each number above IS the summary; windows stated per cell
print("email unsubs, 12-mo total (Jul25-Jun26):", total)
print("story: E1 volumes 35x | E2 blind gate + split | E3 writers | E4 gate leak | E5 channel honor | E7 gate x campaign | E8 waterfall")
