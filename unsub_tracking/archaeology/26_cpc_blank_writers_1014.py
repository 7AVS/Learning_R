# Which system wrote the client's CURRENT blank position on 1014? HDFS only, no Teradata.
#
# Question (Andre, 2026-07-25): a client with no row on 1014 is a true blank and is NOT an opt-out.
# A client whose LATEST 1014 event is blank (5003) IS counted as an opt-out under the dictionary rule.
# So some system is writing a blank row rather than leaving the client absent, and that write is what
# puts 88,606 unsubscribers into the counted population. Which system, and when.
#
# Standing = the LAST event per (client, switch). Earlier events are irrelevant - this is an event log
# and only the latest position stands. Nothing here counts events; every cell counts clients at standing.
# Every cell returns a named pandas table. Nothing is printed.

# NOTE: each table is shown with display(), not left as a bare name - a bare name only renders when it
# is the LAST expression of a cell, so running this file as one block would show only the final table.

# %% [0] Load + reduce to the standing row per client and switch
import pandas as pd
from IPython.display import display
from pyspark.sql import functions as F, Window as W
pd.set_option("display.max_rows", 200)
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

BASE = "hdfs:///user/427966379/unsub_cpc/"
cpc = spark.read.option("recursiveFileLookup", "true").parquet(BASE + "cpc_pref")

w = W.partitionBy("CLNT_NO", "PREF_ID").orderBy(F.col("CHG_TMSTMP").desc())
standing = (cpc.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
               .withColumn("value", F.when(F.col("CLNT_CONSENT_TYP") == 5001, "yes")
                                     .when(F.col("CLNT_CONSENT_TYP") == 5002, "no")
                                     .when(F.col("CLNT_CONSENT_TYP") == 5003, "blank")
                                     .otherwise(F.concat(F.lit("other_"), F.col("CLNT_CONSENT_TYP").cast("string"))))
               .withColumn("year_of_standing_write", F.year("CHG_TMSTMP")))
standing.cache()

# %% [1] A1 - who wrote the standing position on 1014, by value
a1 = (standing.filter("PREF_ID = 1014")
        .groupBy("value", "APP_SYS_CD")
        .agg(F.countDistinct("CLNT_NO").alias("clients_standing"))
        .orderBy("value", F.col("clients_standing").desc())).toPandas()
display("a1"); display(a1)

# %% [2] A2 - when was the standing blank written, and by whom
a2 = (standing.filter("PREF_ID = 1014 AND value = 'blank'")
        .groupBy("year_of_standing_write", "APP_SYS_CD")
        .agg(F.countDistinct("CLNT_NO").alias("clients_standing"))
        .orderBy("year_of_standing_write", F.col("clients_standing").desc())).toPandas()
display("a2"); display(a2)

# %% [3] A3 - same standing picture across all four monitored switches, for comparison
a3 = (standing.filter("PREF_ID IN (1002, 1006, 1012, 1014)")
        .groupBy("PREF_ID", "value", "APP_SYS_CD")
        .agg(F.countDistinct("CLNT_NO").alias("clients_standing"))
        .orderBy("PREF_ID", "value", F.col("clients_standing").desc())).toPandas()
display("a3"); display(a3)

# %% [4] A4 - restricted to the unsubscriber cohort: who wrote THEIR standing 1014 blank
uf = (spark.read.parquet(BASE + "unsub_base/*")
        .groupBy("CLNT_NO").agg(F.min("unsub_tm").alias("unsub_tm")))
a4 = (standing.filter("PREF_ID = 1014").join(uf, "CLNT_NO", "inner")
        .groupBy("value", "APP_SYS_CD")
        .agg(F.countDistinct("CLNT_NO").alias("unsubscribers_standing"))
        .orderBy("value", F.col("unsubscribers_standing").desc())).toPandas()
display("a4"); display(a4)

# %% [5] A5 - was the standing blank written before or after the client's unsubscribe
a5 = (standing.filter("PREF_ID = 1014 AND value = 'blank'").join(uf, "CLNT_NO", "inner")
        .withColumn("vs_unsub", F.when(F.col("CHG_TMSTMP") < F.col("unsub_tm"), "written_before_unsub")
                                 .otherwise("written_after_unsub"))
        .groupBy("vs_unsub", "APP_SYS_CD")
        .agg(F.countDistinct("CLNT_NO").alias("unsubscribers"))
        .orderBy("vs_unsub", F.col("unsubscribers").desc())).toPandas()
display("a5"); display(a5)
