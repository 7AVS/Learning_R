# download_parquet.py
# Paste this whole file into one cell and run it. Nothing else needed. No rerun of anything.
#
# The parquet already exists. Cell [1] and Cell [4] of spotlight.py landed all of it:
#   clientagg_v6    one row per client, bank-wide      (~12.3M rows)
#   cards_pair_v6   one row per client x cards mne
#   mne_agg_v6      campaign x month aggregates
#   ucp_enriched    age / tenure / products per client (~12.3M rows)
#
# This pulls them to the notebook folder and zips them. Shell only - hdfs dfs + zip - so it works
# on any kernel and does not touch Teradata or Spark.

import os
import subprocess

HDFS_BASE = "/user/427966379/unsub_spotlight"
WORK_DIR = "/home/jovyan/Unsub"
LOCAL_DIR = WORK_DIR + "/spotlight_parquet"
ZIP_PATH = WORK_DIR + "/spotlight_parquet.zip"

# Everything landed as parquet. Add or drop names here if a version suffix differs.
DATASETS = ["clientagg_v6", "cards_pair_v6", "mne_agg_v6", "ucp_enriched"]


def sh(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=3600)
    print((r.stdout or "") + (r.stderr or ""), end="")
    return r.returncode


print("=== what exists on HDFS ===")
sh("hdfs dfs -ls %s" % HDFS_BASE)

print("\n=== sizes (first number is the real download size) ===")
for d in DATASETS:
    sh("hdfs dfs -du -s -h %s/%s" % (HDFS_BASE, d))

print("\n=== pulling ===")
sh("rm -rf %s" % LOCAL_DIR)
sh("mkdir -p %s" % LOCAL_DIR)
got = []
for d in DATASETS:
    if sh("hdfs dfs -get -f %s/%s %s/%s" % (HDFS_BASE, d, LOCAL_DIR, d)) == 0:
        got.append(d)
    else:
        print("   skipped", d, "- not on HDFS under that name, see the listing above")

if not got:
    raise SystemExit("Nothing pulled. Check the -ls output above for the actual directory names.")

print("\n=== what came down ===")
sh("du -sh %s/*" % LOCAL_DIR)
sh("du -sh %s" % LOCAL_DIR)

print("\n=== zipping ===")
sh("rm -f %s" % ZIP_PATH)
sh("cd %s && zip -rq spotlight_parquet.zip spotlight_parquet" % WORK_DIR)
if not os.path.exists(ZIP_PATH):
    raise SystemExit("zip failed. The unzipped folder is at %s - download that instead." % LOCAL_DIR)

print("\nDONE.  %s  |  %.1f MB" % (ZIP_PATH, os.path.getsize(ZIP_PATH) / 1048576.0))
sh("ls -lh " + ZIP_PATH)
print("""
It is in the SAME folder as your notebooks (Unsub/). Right-click -> Download.

Then in VS Code - duckdb reads parquet off disk, no RAM ceiling:

  import duckdb
  duckdb.sql("SELECT * FROM 'spotlight_parquet/clientagg_v6/*/*.parquet' LIMIT 20").df()

  # unsubs by campaign
  duckdb.sql(\"\"\"
    SELECT mne, SUM(unsub_flag) AS leavers, COUNT(*) AS clients
    FROM 'spotlight_parquet/cards_pair_v6/*/*.parquet'
    GROUP BY 1 ORDER BY 2 DESC
  \"\"\").df()

Check the glob depth against what actually came down - bitten pulls land as
<name>/bite_N/part-*.parquet, so they need */*.parquet; single-level ones need *.parquet.
""")
