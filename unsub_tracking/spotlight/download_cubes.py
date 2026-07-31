# download_cubes.py
# Standalone. Paste this whole file into one cell and run it. Nothing else needed.
# Pulls every cube off HDFS, zips them, and tells you what to download.
#
# No Spark. Shell only (hdfs dfs + zip), so it runs on any kernel.
# Run it after spotlight.py finishes.

import os
import subprocess

HDFS_OUT = "/user/427966379/unsub_spotlight/out"      # "/out_smoke" if you ran SMOKE=True
# Land BOTH inside the notebook folder. Writing to /home/jovyan put them one level above the
# JupyterLab file browser's working directory, where they were invisible without navigating up.
WORK_DIR = "/home/jovyan/Unsub"
LOCAL_DIR = WORK_DIR + "/spotlight_out"
ZIP_PATH = WORK_DIR + "/spotlight_out.zip"


def sh(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=3600)
    out = (r.stdout or "") + (r.stderr or "")
    print(out, end="")
    return r.returncode, out


print("=== what is on HDFS ===")
rc, _ = sh("hdfs dfs -ls %s" % HDFS_OUT)
if rc != 0:
    raise SystemExit("Nothing at %s. If you ran SMOKE=True, change HDFS_OUT to .../out_smoke" % HDFS_OUT)

print("\n=== pulling to the pod ===")
sh("rm -rf %s" % LOCAL_DIR)
rc, _ = sh("hdfs dfs -get -f %s %s" % (HDFS_OUT, LOCAL_DIR))
if rc != 0:
    raise SystemExit("hdfs dfs -get failed - see the error above.")

print("\n=== what came down ===")
sh("du -sh %s/*" % LOCAL_DIR)

print("\n=== zipping ===")
sh("rm -f %s" % ZIP_PATH)
rc, _ = sh("cd %s && zip -rq spotlight_out.zip spotlight_out" % WORK_DIR)
if rc != 0 or not os.path.exists(ZIP_PATH):
    raise SystemExit("zip failed. The unzipped folder is still at %s - download that instead." % LOCAL_DIR)

size_mb = os.path.getsize(ZIP_PATH) / 1048576.0
print("\nDONE.  %s  |  %.1f MB" % (ZIP_PATH, size_mb))
sh("ls -lh " + ZIP_PATH)
print("""
NEXT:
  1. Jupyter file browser -> right-click spotlight_out.zip -> Download.
     Set the browser's download folder to the share and it lands there directly:
     \\\\maple.fg.rbc.com\\...\\Pod of Gold\\Cards\\Unsubs\\out
  2. Unzip on your laptop.
  3. In VS Code:

       import duckdb
       duckdb.sql("SELECT * FROM 'spotlight_out/cube1_profiling/*.csv' LIMIT 20").df()
""")
