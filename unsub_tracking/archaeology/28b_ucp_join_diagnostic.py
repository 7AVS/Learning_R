# 28b - UCP JOIN DIAGNOSTIC. Run AFTER 28_unsub_value_ucp.py halts at "UCP anchor-month read
# returned zero rows". Requires the same kernel state (UCP_BASE, norm_clnt, F, _anchor_requests).
# Decides which one-line fix pack 28 needs:
#   "rows after ==Personal" == 0      -> the CLNT_TYP value 'Personal' is wrong; the distinct list
#                                        shows the real coding.
#   Personal fine, "overlap" == 0     -> CLNT_NO key/type mismatch; compare the two sample lines.

# %% [1] one-partition diagnosis
m = "2026-03-31"
raw = spark.read.option("basePath", UCP_BASE).parquet(UCP_BASE + "MONTH_END_DATE=" + m)
print("rows in partition          :", raw.count())
print("CLNT_TYP distinct (top 20) :", [r[0] for r in raw.select(F.trim(F.col("CLNT_TYP")).alias("t")).groupBy("t").count().orderBy(F.desc("count")).limit(20).collect()])
print("rows after ==Personal      :", raw.filter(F.trim(F.col("CLNT_TYP")) == "Personal").count())
print("UCP CLNT_NO raw samples    :", [r[0] for r in raw.select("CLNT_NO").limit(5).collect()])
print("UCP CLNT_NO normalized     :", [r[0] for r in raw.select(norm_clnt(F.col("CLNT_NO")).alias("c")).limit(5).collect()])
need = _anchor_requests.filter(F.col("ucp_anchor") == m).select("CLNT_NO")
print("cohort CLNT_NO samples     :", [r[0] for r in need.limit(5).collect()])
print("overlap WITHOUT type filter:", raw.withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO"))).join(need, "CLNT_NO", "leftsemi").count())
