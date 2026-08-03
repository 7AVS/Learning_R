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

# "bite_?" NOT "*"/"bite_*": the _ROWCOUNT/_REGIME sidecar files live inside the dir
# and match wider globs, adding one NULL-keyed row per bite - the root cause found
# 2026-08-03. Reading with "*" below would reproduce exactly the phantom rows.
_a1 = spark.read.parquet(A1_DIR + "bite_?")
print("schema     :", [(f.name, f.dataType.simpleString()) for f in _a1.schema.fields][:4])
print("total rows :", _a1.count())
print("distinct   :", _a1.select("clnt_no").distinct().count())
# per-bite-folder totals and distincts - localizes WHICH folder carries the excess:
for _k in range(10):
    try:
        _b = spark.read.parquet(A1_DIR + "bite_%d" % _k)
        print("  bite_%d: rows %d | distinct %d | schema clnt_no: %s"
              % (_k, _b.count(), _b.select("clnt_no").distinct().count(),
                 dict((f.name, f.dataType.simpleString()) for f in _b.schema.fields).get("clnt_no")))
    except Exception as _e:
        print("  bite_%d: unreadable (%s)" % (_k, type(_e).__name__))

# FIRST suspect when "dup clients: 1" but the lookup tables come back empty:
# NULL ids. All NULLs collapse into one distinct-count group ("1 dup client")
# but a join can never match NULL to NULL, so every downstream lookup is empty.
_nulls = _a1.filter(F.col("clnt_no").isNull()).count()
print("NULL clnt_no rows:", _nulls,
      "<- if this equals the excess-row count, case closed: unjoinable NULL ids,"
      " the pipeline now drops them loudly (see Cell [6] WARN)." if _nulls else "")

_dups = _a1.groupBy("clnt_no").agg(F.count("*").alias("n")).filter("n > 1")
print("dup clients:", _dups.count())

# collect() forces completion BEFORE printing - no lazy half-rendered tables,
# safe to screenshot the moment text appears.
_rows = (_a1.withColumn("src_file", F.input_file_name())
         .join(_dups.select("clnt_no"), "clnt_no")
         .select("clnt_no", "src_file")
         .orderBy("clnt_no")
         .collect())
print("---- duplicated client rows (%d) ----" % len(_rows))
for _r in _rows:
    # print just the bite folder, not the full parquet path - that's the verdict column
    _bite = [p for p in _r["src_file"].split("/") if p.startswith("bite_")]
    print("clnt_no:", _r["clnt_no"], "| folder:", _bite[0] if _bite else _r["src_file"][-60:])

# Bonus check - identical copies or differing rows?
_dup_rows = _a1.join(_dups.select("clnt_no"), "clnt_no")
_n_distinct = _dup_rows.distinct().count()
_n_total = _dup_rows.count()
print("dup rows total:", _n_total, "| distinct contents:", _n_distinct)
print("=> identical copies (pure double-write)" if _n_distinct * 2 <= _n_total or _n_distinct == 1
      else "=> contents DIFFER across copies - not a simple double-write")
# also show the full duplicated rows so we can see WHAT differs, if anything:
for _r in _dup_rows.orderBy("clnt_no").collect()[:12]:
    print(dict(_r.asDict()))
