# Monthly CPC "No" per switch, blank counted as No on 1014/1015. HDFS only.
# Returns a table. Nothing is printed.

from pyspark.sql import functions as F

cpc = spark.read.option("recursiveFileLookup", "true").parquet("hdfs:///user/427966379/unsub_cpc/cpc_pref")

is_no = (F.when(F.col("PREF_ID").isin(1014, 1015),
                F.col("CLNT_CONSENT_TYP").isin(5002, 5003) | F.col("CLNT_CONSENT_TYP").isNull())
          .otherwise(F.col("CLNT_CONSENT_TYP") == 5002))

monthly = (cpc
    .filter("CHG_TMSTMP >= DATE'2025-07-01' AND CHG_TMSTMP < DATE'2026-07-01'")
    .withColumn("is_no", is_no)
    .groupBy(F.date_format("CHG_TMSTMP", "yyyyMM").alias("month"), "PREF_ID")
    .agg(F.countDistinct(F.when(F.col("is_no"), F.col("CLNT_NO"))).alias("no_incl_blank"),
         F.countDistinct(F.when(F.col("CLNT_CONSENT_TYP") == 5002, F.col("CLNT_NO"))).alias("no_explicit_only"))
    .orderBy("PREF_ID", "month"))

df = monthly.toPandas()
df          # table out. df.to_csv("cpc_monthly_no.csv", index=False) if you want the file.
