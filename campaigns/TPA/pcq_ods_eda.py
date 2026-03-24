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

# ── 2. Parse JSON once, cache the small filtered set ────────────────────
parsed = ods.select(
    "clnt_id",
    F.from_json("treatmt_dtl",      "map<string,string>").alias("treatmt_dtl"),
    F.from_json("treatmt_dtl_en",   "map<string,string>").alias("treatmt_dtl_en"),
    F.from_json("treatmt_adnl_dtl", "map<string,string>").alias("treatmt_adnl_dtl")
).cache()

# ── 3. Selected categorical keys only (from original EDA) ───────────────
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

# ── 4. Explode maps, keep only selected keys ────────────────────────────
key_filter = keys_dtl + keys_dtlen + keys_adnl

dfs = []
for col_name in ["treatmt_dtl", "treatmt_dtl_en", "treatmt_adnl_dtl"]:
    dfs.append(
        parsed.select(
            "clnt_id",
            F.lit(col_name).alias("json_field"),
            F.explode(col_name)
        ).where(F.col("key").isin(key_filter))
    )

exploded = dfs[0].unionByName(dfs[1]).unionByName(dfs[2])

# ── 5. Aggregate: distinct clients per json_field / key / value ─────────
results = exploded.groupBy("json_field", "key", "value") \
    .agg(F.countDistinct("clnt_id").alias("clients"))

results.orderBy("json_field", "key", "value").show(100, truncate=False)

# ── 6. Cleanup ──────────────────────────────────────────────────────────
parsed.unpersist()
