# PCQ ODS EDA — Explore treatmt_dtl JSON fields
# Source: prod_x610_crm.ods_mr_hist (filtered to PCQ, 2026+)
# Purpose: Profile all key-value pairs across the three JSON treatment columns
#          to understand offer structure, test groups, and field coverage.

from pyspark.sql import functions as F

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

# ── 1. Filter to PCQ records ────────────────────────────────────────────
ods = spark.table("prod_x610_crm.ods_mr_hist") \
    .select("clnt_id", "prod_mn", "effectdate",
            "treatmt_dtl", "treatmt_dtl_en", "treatmt_adnl_dtl") \
    .where((F.col("prod_mn") == "PCQ")
           & (F.col("effectdate") >= F.expr("date '2026-01-01'")))

# ── 2. Parse JSON once, cache the small filtered set ────────────────────
parsed = ods.select(
    "clnt_id",
    F.from_json("treatmt_dtl",      "map<string,string>").alias("treatmt_dtl"),
    F.from_json("treatmt_dtl_en",   "map<string,string>").alias("treatmt_dtl_en"),
    F.from_json("treatmt_adnl_dtl", "map<string,string>").alias("treatmt_adnl_dtl")
).cache()

# ── 3. Explode each map directly (no element_at, no key enumeration) ────
dfs = []
for col_name in ["treatmt_dtl", "treatmt_dtl_en", "treatmt_adnl_dtl"]:
    dfs.append(
        parsed.select(
            "clnt_id",
            F.lit(col_name).alias("json_field"),
            F.explode(col_name)   # emits "key" and "value" columns
        )
    )

exploded = dfs[0].unionByName(dfs[1]).unionByName(dfs[2])

# ── 4. Aggregate: distinct clients per json_field / key / value ─────────
results = exploded.groupBy("json_field", "key", "value") \
    .agg(F.countDistinct("clnt_id").alias("clients"))

results.orderBy("json_field", "key", "value").show(100, truncate=False)

# ── 5. Cleanup ──────────────────────────────────────────────────────────
parsed.unpersist()
