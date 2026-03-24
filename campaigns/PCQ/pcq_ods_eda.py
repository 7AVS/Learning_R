# PCQ ODS EDA — Profile categorical JSON fields (sample-based, fast)
# Source: prod_x610_crm.ods_mr_hist (filtered to PCQ, 2026+)
# Purpose: Discover distinct values per selected categorical key.
#          Uses 10K row sample — no full scan, no shuffle, no groupBy.

from pyspark.sql import functions as F

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "20")

# ── 1. Filter to PCQ, sample 10K rows ──────────────────────────────────
ods = spark.table("prod_x610_crm.ods_mr_hist") \
    .select("clnt_id", "treatmt_dtl", "treatmt_dtl_en", "treatmt_adnl_dtl") \
    .where((F.col("prod_mn") == "PCQ")
           & (F.col("effectdate") >= F.expr("date '2026-01-01'"))) \
    .limit(10000)

# ── 2. Parse JSON once, cache ──────────────────────────────────────────
parsed = ods \
    .withColumn("m_dtl",   F.from_json("treatmt_dtl",      "map<string,string>")) \
    .withColumn("m_dtlen", F.from_json("treatmt_dtl_en",    "map<string,string>")) \
    .withColumn("m_adnl",  F.from_json("treatmt_adnl_dtl", "map<string,string>")) \
    .drop("treatmt_dtl", "treatmt_dtl_en", "treatmt_adnl_dtl") \
    .cache()

# ── 3. Collect distinct values per key (no explode, no groupBy) ─────────
keys_by_field = {
    "m_dtl": [
        "TREATMT_CD", "CTA_CD", "PAPP_PRD_IND", "PUBL_IND", "UPDT_FLAG",
        "LEAD_TYP", "LEAD_SCR", "PROD_FAMILY_CD", "PROD_NAME_CD", "PROD_CD",
        "PROD_CAT_MNEMONIC", "PRODUCT_TO", "ASC_CD", "ACTN_CD", "SRVC_ID",
        "ID_TYPE", "ID_SOURCE", "OFFER_TYPE", "BANNER_STS", "INTERCEPT_STS",
        "FEE_NO_FEE_CD", "REGION_CD", "PROV_CD", "GENDER",
        "TELECOM_MODE_TYP_1", "TELECOM_MODE_TYP_2",
        "TELECOM_PURPS_TYP_1", "TELECOM_PURPS_TYP_2",
        "MANAGEMENT_TYPE_CD", "LANG_CD", "CATEGORY", "SUBCATEGORY",
        "CAMPAIGN_SEED_STS",
    ],
    "m_dtlen": [
        "SUBJECT_LINE", "PROD_FAMILY", "PROD_FAMILY_DTL", "PROD_CATEGORY",
    ],
    "m_adnl": [
        "TST_GRP_CD", "RPT_GRP_CD", "STRTGY_SRC_CD", "LOB_SUBTYP_CD",
        "BUS_MKT_ID", "MNE", "PRIORITY_SCR", "RANKING",
    ],
}

for field, keys in keys_by_field.items():
    print(f"\n{'='*60}")
    print(f"  {field}")
    print(f"{'='*60}")
    for k in keys:
        vals = (parsed
                .select(F.col(field)[k].alias("v"))
                .where(F.col("v").isNotNull())
                .distinct()
                .collect())
        vals_list = sorted([r["v"] for r in vals])
        print(f"  {k:30s} -> {vals_list}")

# ── 4. Cleanup ──────────────────────────────────────────────────────────
parsed.unpersist()
