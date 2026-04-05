from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder \
    .appName("SCOT Partition EDA") \
    .master("yarn") \
    .enableHiveSupport() \
    .getOrCreate()

# 1) Table metadata — check partition columns
spark.sql("DESCRIBE FORMATTED prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot").show(100, truncate=False)

# 2) List partitions (if any)
try:
    spark.sql("SHOW PARTITIONS prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot").show(50, truncate=False)
except Exception as e:
    print(f"No partitions or error: {e}")

# 3) Test with one client from diagnostics
test_clnt = "119337968"

scot = spark.read.parquet("/prod/sz/tsz/00222/data/CREDIT_APPLICATION_SNAPSHOT")

scot.filter(
    F.col("creditapplication_borrowers_borrowersrfnumber") == test_clnt
).select(
    "createddate",
    "creditapplication_borrowers_borrowersrfnumber",
    "creditapplication_createddatetime",
    "creditapplication_creditapplicationstatuscode",
    "creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory"
).show(truncate=False)
