# diag_a1_dupes.py - one question: WHY does a1_client have duplicate clnt_no rows.
# Paste-and-run as a single cell (brain local kernel, spark in scope). Read-only, no writes.
#
# Verdict key, from the src_file paths of each duplicated client:
#   - same clnt_no in TWO DIFFERENT bite_N folders -> MOD partitioning broke
#     (id type/precision issue) -> STOP, report back, deeper diagnosis needed.
#   - same clnt_no TWICE in the SAME bite_N folder -> a landing wrote rows twice
#     (partial chunk left by a dead kernel, then re-appended on resume) ->
#     fix: hdfs dfs -rm -r that one bite folder, rerun the file; it re-pulls
#     just that bite and skips the rest.

from pyspark.sql import functions as F

A1_DIR = "hdfs:///user/427966379/unsub_unified/a1_client_v1/"

_a1 = spark.read.parquet(A1_DIR + "*")
print("total rows :", _a1.count())
print("distinct   :", _a1.select("clnt_no").distinct().count())

_dups = _a1.groupBy("clnt_no").agg(F.count("*").alias("n")).filter("n > 1")
print("dup clients:", _dups.count())

(_a1.withColumn("src_file", F.input_file_name())
    .join(_dups.select("clnt_no"), "clnt_no")
    .select("clnt_no", "src_file")
    .orderBy("clnt_no")
    .show(40, truncate=False))

# Bonus check - are the duplicated rows IDENTICAL in content (pure double-write)
# or do they DIFFER (two genuinely different aggregations landed)? Identical
# content = pure landing artifact, even safer to fix by re-pulling the bite.
_dup_rows = _a1.join(_dups.select("clnt_no"), "clnt_no")
_identical = _dup_rows.distinct().groupBy("clnt_no").agg(F.count("*").alias("n_distinct"))
_identical.groupBy("n_distinct").count().show()
print("n_distinct=1 above means the duplicate rows are byte-identical copies.")
