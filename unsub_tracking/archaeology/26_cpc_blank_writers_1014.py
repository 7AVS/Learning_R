# Who writes the BLANK position on 1014? HDFS only, no Teradata.
#
# Question (Andre, 2026-07-25): a client with no row on 1014 is a true blank and is NOT an opt-out.
# A client WITH a row whose value is blank (5003) counts as an opt-out under the dictionary rule.
# So some system is writing a blank row rather than leaving the client absent, and that write decides
# whether 88,606 unsubscribers are counted or not. This pack asks which system, when, and how often.
#
# Every cell returns a named pandas table. Nothing is printed.

# %% [0] Load
import pandas as pd
from pyspark.sql import functions as F, Window as W
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

BASE = "hdfs:///user/427966379/unsub_cpc/"
cpc = spark.read.option("recursiveFileLookup", "true").parquet(BASE + "cpc_pref")
cpc.cache()

VAL = F.when(F.col("CLNT_CONSENT_TYP") == 5001, "yes") \
       .when(F.col("CLNT_CONSENT_TYP") == 5002, "no") \
       .when(F.col("CLNT_CONSENT_TYP") == 5003, "blank") \
       .otherwise(F.concat(F.lit("other_"), F.col("CLNT_CONSENT_TYP").cast("string")))
cpc = cpc.withColumn("value", VAL).withColumn("year", F.year("CHG_TMSTMP"))

# %% [1] A1 - who writes each value, per switch. Full history, events and clients.
a1 = (cpc.groupBy("PREF_ID", "value", "APP_SYS_CD")
         .agg(F.count("*").alias("events"), F.countDistinct("CLNT_NO").alias("clients"))
         .orderBy("PREF_ID", "value", F.col("events").desc())).toPandas()
a1

# %% [2] A2 - blank writers on 1014 by year. Is it one historical batch or ongoing?
a2 = (cpc.filter("PREF_ID = 1014 AND CLNT_CONSENT_TYP = 5003")
         .groupBy("year", "APP_SYS_CD")
         .agg(F.count("*").alias("events"), F.countDistinct("CLNT_NO").alias("clients"))
         .orderBy("year", F.col("events").desc())).toPandas()
a2

# %% [3] A3 - is a blank the client's FIRST 1014 row, or does it overwrite an earlier answer?
w = W.partitionBy("CLNT_NO", "PREF_ID").orderBy(F.col("CHG_TMSTMP").asc())
seq = (cpc.filter("PREF_ID = 1014")
          .withColumn("rn", F.row_number().over(w))
          .withColumn("prev_value", F.lag("value").over(w))
          .withColumn("prev_sys", F.lag("APP_SYS_CD").over(w)))

a3 = (seq.filter("value = 'blank'")
         .withColumn("position", F.when(F.col("rn") == 1, "first_row_for_client").otherwise("overwrites_prior"))
         .groupBy("position", "prev_value", "APP_SYS_CD")
         .agg(F.count("*").alias("events"), F.countDistinct("CLNT_NO").alias("clients"))
         .orderBy(F.col("events").desc())).toPandas()
a3

# %% [4] A4 - what is a client's 1014 standing after the blank write? latest row per client.
wl = W.partitionBy("CLNT_NO", "PREF_ID").orderBy(F.col("CHG_TMSTMP").desc())
a4 = (cpc.filter("PREF_ID = 1014")
         .withColumn("rn", F.row_number().over(wl)).filter("rn = 1")
         .groupBy("value", "APP_SYS_CD")
         .agg(F.countDistinct("CLNT_NO").alias("clients_standing"))
         .orderBy(F.col("clients_standing").desc())).toPandas()
a4

# %% [5] A5 - blank writes on 1014 by month across the study window. Ongoing, or dormant?
a5 = (cpc.filter("PREF_ID = 1014 AND CLNT_CONSENT_TYP = 5003")
         .filter("CHG_TMSTMP >= DATE'2024-01-01'")
         .groupBy(F.date_format("CHG_TMSTMP", "yyyyMM").alias("month"), "APP_SYS_CD")
         .agg(F.count("*").alias("events"), F.countDistinct("CLNT_NO").alias("clients"))
         .orderBy("month", F.col("events").desc())).toPandas()
a5

# %% [6] A6 - do the same writers behave differently across switches? blank share per writer.
tot = cpc.groupBy("PREF_ID", "APP_SYS_CD").agg(F.count("*").alias("all_events"))
blk = (cpc.filter("CLNT_CONSENT_TYP = 5003")
          .groupBy("PREF_ID", "APP_SYS_CD").agg(F.count("*").alias("blank_events")))
a6 = (tot.join(blk, ["PREF_ID", "APP_SYS_CD"], "left").fillna(0, ["blank_events"])
         .orderBy("PREF_ID", F.col("all_events").desc())).toPandas()
a6
