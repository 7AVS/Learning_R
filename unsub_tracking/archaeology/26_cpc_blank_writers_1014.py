# Which system wrote the client's CURRENT position on 1014? HDFS only, no Teradata.
#
# Question (Andre, 2026-07-25): a client with no row on 1014 is a true blank and is NOT an opt-out.
# A client whose LATEST 1014 event is blank (5003) IS counted as an opt-out under the dictionary rule.
# So some system writes a blank row rather than leaving the client absent, and that write is what puts
# 88,606 unsubscribers into the counted population. Which system, and when.
#
# Standing = the LAST event per (client, switch). Earlier events are irrelevant in an event log.
#
# OUTPUT SHAPE: tables are kept narrow so they stay legible in a photo of the screen. top_writer()
# AGGREGATES, it does not truncate - clients_total sums every writer in the group, and n_writers says
# how many are behind the top one. The only thing collapsed is the identity of the smaller writers;
# drop the top_writer() wrapper to list them all. NO CELL CAPS ITS ROW COUNT.

# %% [0] Load + reduce to the standing row per client and switch
import pandas as pd
from IPython.display import display, Markdown
from pyspark.sql import functions as F, Window as W

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
pd.set_option("display.max_rows", 40)
pd.set_option("display.width", 140)

def T(label, df):
    """Render a titled table AND return it. A bare `df` renders only as a cell's last expression;
    this always renders wherever it sits, and still hands back the object to export or plot."""
    out = df.toPandas() if hasattr(df, "toPandas") else df
    display(Markdown("**" + label + "**  ·  " + str(len(out)) + " rows"))
    display(out)
    return out

def top_writer(df, group_cols):
    """Collapse APP_SYS_CD to the dominant writer per group: its code, its clients, its share,
    plus how many other writers exist. Keeps the table small enough to photograph."""
    tot = df.groupBy(*group_cols).agg(F.sum("clients").alias("clients_total"),
                                      F.countDistinct("APP_SYS_CD").alias("n_writers"))
    w = W.partitionBy(*group_cols).orderBy(F.col("clients").desc())
    top = (df.withColumn("rk", F.row_number().over(w)).filter("rk = 1")
             .select(*group_cols, F.col("APP_SYS_CD").alias("top_writer"),
                     F.col("clients").alias("top_writer_clients")))
    return (tot.join(top, group_cols)
               .withColumn("top_writer_share", F.round(F.col("top_writer_clients") / F.col("clients_total"), 3))
               .select(*group_cols, "clients_total", "top_writer", "top_writer_share", "n_writers"))

BASE = "hdfs:///user/427966379/unsub_cpc/"
cpc = spark.read.option("recursiveFileLookup", "true").parquet(BASE + "cpc_pref")

w0 = W.partitionBy("CLNT_NO", "PREF_ID").orderBy(F.col("CHG_TMSTMP").desc())
standing = (cpc.withColumn("rn", F.row_number().over(w0)).filter("rn = 1")
               .withColumn("value", F.when(F.col("CLNT_CONSENT_TYP") == 5001, "yes")
                                     .when(F.col("CLNT_CONSENT_TYP") == 5002, "no")
                                     .when(F.col("CLNT_CONSENT_TYP") == 5003, "blank")
                                     .otherwise(F.lit("other")))
               .withColumn("yr", F.year("CHG_TMSTMP")))
standing.cache()

uf = (spark.read.parquet(BASE + "unsub_base/*")
        .groupBy("CLNT_NO").agg(F.min("unsub_tm").alias("unsub_tm")))

# %% [1] A1 - who wrote the standing 1014 position, one row per value.  4 rows
_g = (standing.filter("PREF_ID = 1014").groupBy("value", "APP_SYS_CD")
              .agg(F.countDistinct("CLNT_NO").alias("clients")))
a1 = T("A1 - standing 1014 position: clients, and the system that wrote most of them",
       top_writer(_g, ["value"]).orderBy(F.col("clients_total").desc()))

# %% [2] A2 - when the standing blank was written.  <= 10 rows
_g = (standing.filter("PREF_ID = 1014 AND value = 'blank'").groupBy("yr", "APP_SYS_CD")
              .agg(F.countDistinct("CLNT_NO").alias("clients")))
a2 = T("A2 - standing 1014 blank by year it was written, with the dominant writer that year",
       top_writer(_g, ["yr"]).orderBy("yr"))

# %% [3] A3 - the same picture across the four monitored switches, No and blank only.  <= 8 rows
_g = (standing.filter("PREF_ID IN (1002, 1006, 1012, 1014) AND value IN ('no','blank')")
              .groupBy("PREF_ID", "value", "APP_SYS_CD")
              .agg(F.countDistinct("CLNT_NO").alias("clients")))
a3 = T("A3 - standing No and blank per switch, with the dominant writer",
       top_writer(_g, ["PREF_ID", "value"]).orderBy("PREF_ID", "value"))

# %% [4] A4 - unsubscribers only: who wrote THEIR standing 1014 position.  4 rows
_g = (standing.filter("PREF_ID = 1014").join(uf, "CLNT_NO", "inner")
              .groupBy("value", "APP_SYS_CD").agg(F.countDistinct("CLNT_NO").alias("clients")))
a4 = T("A4 - unsubscribers: standing 1014 position and the system that wrote most of them",
       top_writer(_g, ["value"]).orderBy(F.col("clients_total").desc()))

# %% [5] A5 - was that standing blank written before or after the unsubscribe.  2 rows
_g = (standing.filter("PREF_ID = 1014 AND value = 'blank'").join(uf, "CLNT_NO", "inner")
              .withColumn("vs_unsub", F.when(F.col("CHG_TMSTMP") < F.col("unsub_tm"), "before_unsub")
                                       .otherwise("after_unsub"))
              .groupBy("vs_unsub", "APP_SYS_CD").agg(F.countDistinct("CLNT_NO").alias("clients")))
a5 = T("A5 - unsubscribers standing blank on 1014: was it written before or after they unsubscribed",
       top_writer(_g, ["vs_unsub"]).orderBy(F.col("clients_total").desc()))

# %% [6] A6 - THE MONTHLY SERIES. One row per month, one column per switch, blank counted as No on
# 1014. This is the table behind the first plot: email unsubscribes and CPC "No" as parallel curves.
# 12 rows x 5 columns.
IS_NO = (F.when(F.col("PREF_ID").isin(1014, 1015),
                F.col("CLNT_CONSENT_TYP").isin(5002, 5003) | F.col("CLNT_CONSENT_TYP").isNull())
          .otherwise(F.col("CLNT_CONSENT_TYP") == 5002))
WIN = "CHG_TMSTMP >= DATE'2025-07-01' AND CHG_TMSTMP < DATE'2026-07-01'"

_cpc_m = (cpc.filter(WIN).filter(F.col("PREF_ID").isin(1002, 1012, 1014))
             .withColumn("month", F.date_format("CHG_TMSTMP", "yyyyMM"))
             .filter(IS_NO)
             .groupBy("month").pivot("PREF_ID", [1002, 1012, 1014])
             .agg(F.countDistinct("CLNT_NO")))
_unsub_m = (spark.read.parquet(BASE + "unsub_base/*")
              .groupBy("CLNT_NO").agg(F.min("unsub_tm").alias("unsub_tm"))
              .withColumn("month", F.date_format("unsub_tm", "yyyyMM"))
              .groupBy("month").agg(F.countDistinct("CLNT_NO").alias("email_unsub")))

a6 = T("A6 - MONTHLY: clients written to No per switch (1014 includes blank) vs email unsubscribes",
       _cpc_m.join(_unsub_m, "month", "full_outer")
             .withColumnRenamed("1002", "cpc_1002_no")
             .withColumnRenamed("1012", "cpc_1012_no")
             .withColumnRenamed("1014", "cpc_1014_no_incl_blank")
             .select("month", "email_unsub", "cpc_1002_no", "cpc_1012_no", "cpc_1014_no_incl_blank")
             .orderBy("month"))

# %% [7] A7 - the same months, 1014 explicit-only alongside blank-inclusive, so the gap is visible.
# This is what the old ~800/mo chart was plotting. 12 rows x 4 columns.
a7 = T("A7 - MONTHLY 1014: blank-inclusive vs explicit-only, and the pooled 3-switch explicit series",
       cpc.filter(WIN).filter(F.col("PREF_ID").isin(1002, 1012, 1014))
          .withColumn("month", F.date_format("CHG_TMSTMP", "yyyyMM"))
          .groupBy("month")
          .agg(F.countDistinct(F.when((F.col("PREF_ID") == 1014) & IS_NO, F.col("CLNT_NO"))).alias("m1014_incl_blank"),
               F.countDistinct(F.when((F.col("PREF_ID") == 1014) & (F.col("CLNT_CONSENT_TYP") == 5002), F.col("CLNT_NO"))).alias("m1014_explicit_only"),
               F.countDistinct(F.when(F.col("CLNT_CONSENT_TYP") == 5002, F.col("CLNT_NO"))).alias("pooled_explicit_old_chart"))
          .orderBy("month"))
