# Which system wrote the client's CURRENT blank position on 1014? HDFS only, no Teradata.
#
# Question (Andre, 2026-07-25): a client with no row on 1014 is a true blank and is NOT an opt-out.
# A client whose LATEST 1014 event is blank (5003) IS counted as an opt-out under the dictionary rule.
# So some system is writing a blank row rather than leaving the client absent, and that write is what
# puts 88,606 unsubscribers into the counted population. Which system, and when.
#
# Standing = the LAST event per (client, switch). Earlier events are irrelevant - this is an event log
# and only the latest position stands. Nothing counts events; every cell counts clients at standing.

# %% [0] Load + reduce to the standing row per client and switch
import pandas as pd
from IPython.display import display, Markdown
from pyspark.sql import functions as F, Window as W

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
pd.set_option("display.max_rows", 200)

def T(label, df):
    """Render a titled table AND return it. A bare `df` renders only as a cell's last expression;
    this always renders, wherever it sits, and still hands back the object to export or plot."""
    out = df.toPandas() if hasattr(df, "toPandas") else df
    display(Markdown("**" + label + "**  ·  " + str(len(out)) + " rows"))
    display(out)
    return out

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

uf = (spark.read.parquet(BASE + "unsub_base/*")
        .groupBy("CLNT_NO").agg(F.min("unsub_tm").alias("unsub_tm")))

# %% [1] A1 - who wrote the standing 1014 position, by value
a1 = T("A1 - who wrote the STANDING 1014 position, by value",
       standing.filter("PREF_ID = 1014")
               .groupBy("value", "APP_SYS_CD")
               .agg(F.countDistinct("CLNT_NO").alias("clients_standing"))
               .orderBy("value", F.col("clients_standing").desc()))

# %% [2] A2 - when was the standing blank written, and by whom
a2 = T("A2 - year the standing 1014 blank was written, by writer system",
       standing.filter("PREF_ID = 1014 AND value = 'blank'")
               .groupBy("year_of_standing_write", "APP_SYS_CD")
               .agg(F.countDistinct("CLNT_NO").alias("clients_standing"))
               .orderBy("year_of_standing_write", F.col("clients_standing").desc()))

# %% [3] A3 - the same standing picture across all four monitored switches
a3 = T("A3 - standing position across 1002 / 1006 / 1012 / 1014, by value and writer",
       standing.filter("PREF_ID IN (1002, 1006, 1012, 1014)")
               .groupBy("PREF_ID", "value", "APP_SYS_CD")
               .agg(F.countDistinct("CLNT_NO").alias("clients_standing"))
               .orderBy("PREF_ID", "value", F.col("clients_standing").desc()))

# %% [4] A4 - unsubscribers only: who wrote THEIR standing 1014 position
a4 = T("A4 - unsubscribers only: who wrote THEIR standing 1014 position",
       standing.filter("PREF_ID = 1014").join(uf, "CLNT_NO", "inner")
               .groupBy("value", "APP_SYS_CD")
               .agg(F.countDistinct("CLNT_NO").alias("unsubscribers_standing"))
               .orderBy("value", F.col("unsubscribers_standing").desc()))

# %% [5] A5 - was that standing blank written before or after the client's unsubscribe
a5 = T("A5 - unsubscribers standing blank on 1014: written before or after their unsubscribe",
       standing.filter("PREF_ID = 1014 AND value = 'blank'").join(uf, "CLNT_NO", "inner")
               .withColumn("vs_unsub", F.when(F.col("CHG_TMSTMP") < F.col("unsub_tm"), "written_before_unsub")
                                        .otherwise("written_after_unsub"))
               .groupBy("vs_unsub", "APP_SYS_CD")
               .agg(F.countDistinct("CLNT_NO").alias("unsubscribers"))
               .orderBy("vs_unsub", F.col("unsubscribers").desc()))
