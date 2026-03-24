# PCQ ODS EDA — Profile categorical JSON fields
# Source: prod_x610_crm.ods_mr_hist (filtered to PCQ, 2026+)
# Purpose: Discover distinct values for selected categorical keys across
#          the three JSON treatment columns.

from pyspark.sql import functions as F

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

# ── 1. Filter to PCQ records ────────────────────────────────────────────
ods = spark.table("prod_x610_crm.ods_mr_hist") \
    .select("clnt_id", "prod_mn", "effectdate",
            "treatmt_dtl", "treatmt_dtl_en", "treatmt_adnl_dtl") \
    .where((F.col("prod_mn") == "PCQ")
           & (F.col("effectdate") >= F.expr("date '2026-01-01'")))

# ── 2. Parse JSON once, cache the filtered set ─────────────────────────
parsed = ods \
    .withColumn("m_dtl",   F.from_json("treatmt_dtl",      "map<string,string>")) \
    .withColumn("m_dtlen", F.from_json("treatmt_dtl_en",    "map<string,string>")) \
    .withColumn("m_adnl",  F.from_json("treatmt_adnl_dtl", "map<string,string>")) \
    .drop("treatmt_dtl", "treatmt_dtl_en", "treatmt_adnl_dtl") \
    .cache()

# ── 3. Selected categorical keys only ──────────────────────────────────
keys_dtl = [
    "TREATMT_CD", "CTA_CD", "PAPP_PRD_IND", "PUBL_IND", "UPDT_FLAG",
    "LEAD_TYP", "LEAD_SCR", "PROD_FAMILY_CD", "PROD_NAME_CD", "PROD_CD",
    "PROD_CAT_MNEMONIC", "PRODUCT_TO", "ASC_CD", "ACTN_CD", "SRVC_ID",
    "ID_TYPE", "ID_SOURCE", "OFFER_TYPE", "BANNER_STS", "INTERCEPT_STS",
    "FEE_NO_FEE_CD", "REGION_CD", "PROV_CD", "GENDER",
    "TELECOM_MODE_TYP_1", "TELECOM_MODE_TYP_2",
    "TELECOM_PURPS_TYP_1", "TELECOM_PURPS_TYP_2",
    "MANAGEMENT_TYPE_CD", "LANG_CD", "CATEGORY", "SUBCATEGORY",
    "CAMPAIGN_SEED_STS",
]
keys_dtlen = [
    "SUBJECT_LINE", "PROD_FAMILY", "PROD_FAMILY_DTL", "PROD_CATEGORY",
]
keys_adnl = [
    "TST_GRP_CD", "RPT_GRP_CD", "STRTGY_SRC_CD", "LOB_SUBTYP_CD",
    "BUS_MKT_ID", "MNE", "PRIORITY_SCR", "RANKING",
]

# ── 4. Build stack expressions (element_at on cached parsed maps) ───────
def kv_array(map_col, json_field, keys):
    return F.array(*[
        F.struct(
            F.lit(json_field).alias("json_field"),
            F.lit(k).alias("key"),
            F.element_at(map_col, F.lit(k)).alias("value")
        )
        for k in keys
    ])

stacked = parsed.select(
    "clnt_id",
    F.concat(
        kv_array(F.col("m_dtl"),   "treatmt_dtl",      keys_dtl),
        kv_array(F.col("m_dtlen"), "treatmt_dtl_en",    keys_dtlen),
        kv_array(F.col("m_adnl"),  "treatmt_adnl_dtl",  keys_adnl),
    ).alias("kv")
)

exploded = stacked.select("clnt_id", F.explode_outer("kv").alias("kv")) \
    .select("clnt_id", "kv.json_field", "kv.key", "kv.value") \
    .where(F.col("value").isNotNull())

# ── 5. Aggregate: distinct clients per json_field / key / value ─────────
results = exploded.groupBy("json_field", "key", "value") \
    .agg(F.countDistinct("clnt_id").alias("clients"))

results.orderBy("json_field", "key", "value").show(100, truncate=False)

# ── 6. Cleanup ──────────────────────────────────────────────────────────
parsed.unpersist()
