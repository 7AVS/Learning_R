# DIAGNOSIS / CONSULTATION - Q&A backup cells off the HDFS reservoir. NOT deck content (deck = cpc_evidence_hdfs.py only).
# Needs a Spark session; no Teradata. Run cell [0] first.

# %% [0] Load reservoir
from pyspark.sql import functions as F, Window as W
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
BASE = "hdfs:///user/427966379/unsub_cpc/"
cpc = spark.read.option("recursiveFileLookup", "true").parquet(BASE + "cpc_pref")

def cpc_standing(as_of_expr):
    ww = W.partitionBy("CLNT_NO", "PREF_ID").orderBy(F.col("CHG_TMSTMP").desc())
    base = cpc.filter("CHG_TMSTMP < " + as_of_expr) if as_of_expr else cpc
    return base.withColumn("rn", F.row_number().over(ww)).filter("rn = 1")

print("cpc rows:", cpc.count())

# %% [1] D1 - blank provenance: who writes empty answers (5003/5004), when, per switch (Q&A backup for "what are the blanks?")
print("event composition per switch (5001 Yes / 5002 No / 5003 blank / 5004):")
cpc.groupBy("PREF_ID", "CLNT_CONSENT_TYP").count().orderBy("PREF_ID", "CLNT_CONSENT_TYP").show(20, False)
bl = cpc.filter("CLNT_CONSENT_TYP IN (5003, 5004)")
print("blank/5004 events by writer x switch (top 20):")
bl.groupBy("PREF_ID", "APP_SYS_CD", "CLNT_CONSENT_TYP").count().orderBy(F.col("count").desc()).show(20, False)
print("blank/5004 events by year:")
bl.groupBy(F.year("CHG_TMSTMP").alias("yr")).count().orderBy("yr").show(30, False)
print("STANDING state composition per switch (latest row per client - what clients sit at today):")
cpc_standing(None).groupBy("PREF_ID", "CLNT_CONSENT_TYP").count().orderBy("PREF_ID", "CLNT_CONSENT_TYP").show(20, False)
