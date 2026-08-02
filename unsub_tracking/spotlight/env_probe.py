# Environment probe - answers "where can this kernel actually write, and what can it reach".
# Run every cell in the AI Farm Jupyter pod. Run it TWICE: once on the YARN Spark kernel, once on
# the local kernel. The answers differ, and which differences are real is the whole question.
#
# Nothing here writes to a shared location or deletes anything. Every write goes to a temp file
# under the home directory and is removed in the same cell.

# %% [P1] Who am I, where am I, what filesystem am I standing on
import os, sys, socket, getpass, subprocess, tempfile

print("kernel python :", sys.executable)
print("hostname      :", socket.gethostname())
print("user          :", getpass.getuser())
print("cwd           :", os.getcwd())
print("home          :", os.path.expanduser("~"))
print("spark in scope:", "spark" in dir() or "spark" in globals())


# %% [P2] Can this kernel write to the POD's local disk at all
# If this fails, nothing local is possible from this kernel and the rest is moot.
_probe = os.path.join(os.path.expanduser("~"), "_env_probe.txt")
try:
    with open(_probe, "w") as fh:
        fh.write("probe\n")
    with open(_probe) as fh:
        _back = fh.read().strip()
    os.remove(_probe)
    print("LOCAL WRITE: OK - wrote and read back", repr(_back), "at", _probe)
except Exception as e:
    print("LOCAL WRITE: FAILED -", type(e).__name__, str(e)[:200])


# %% [P3] Spark -> local disk. THE key test.
# df.write.csv("file:///...") writes on the EXECUTORS, which are other machines - the file lands
# nowhere you can see. df.toPandas().to_csv(...) runs on the DRIVER, which is this pod. Only the
# second one works, and it is the one that matters, because every cube here is small.
# Skip this cell on the local kernel.
try:
    _t = spark.createDataFrame([(1, "a"), (2, "b")], ["n", "s"])
    _p = os.path.join(os.path.expanduser("~"), "_env_probe_spark.csv")
    _t.toPandas().to_csv(_p, index=False)
    print("SPARK -> DRIVER -> LOCAL CSV: OK, wrote", os.path.getsize(_p), "bytes to", _p)
    os.remove(_p)
except NameError:
    print("SPARK -> LOCAL: no spark session in this kernel - expected on the local kernel")
except Exception as e:
    print("SPARK -> LOCAL: FAILED -", type(e).__name__, str(e)[:300])


# %% [P4] Is there an HDFS CLI. If yes, HDFS is reachable from ANY kernel, no Spark required.
# `hdfs dfs -get` would let the local kernel pull parquet/CSV down without a Spark session.
for _cmd in (["hdfs", "version"], ["hadoop", "version"]):
    try:
        _r = subprocess.run(_cmd, capture_output=True, text=True, timeout=60)
        print(_cmd[0], "-> rc", _r.returncode, "|", (_r.stdout or _r.stderr).splitlines()[:1])
    except FileNotFoundError:
        print(_cmd[0], "-> NOT ON PATH")
    except Exception as e:
        print(_cmd[0], "->", type(e).__name__, str(e)[:150])


# %% [P5] Can the CLI actually list your HDFS home
try:
    _r = subprocess.run(["hdfs", "dfs", "-ls", "/user/427966379/"],
                        capture_output=True, text=True, timeout=120)
    print("rc", _r.returncode)
    print((_r.stdout or _r.stderr)[:2000])
except FileNotFoundError:
    print("hdfs CLI not present - skip")
except Exception as e:
    print(type(e).__name__, str(e)[:300])


# %% [P6] Can PYTHON read HDFS without Spark (pyarrow / webhdfs)
# If this works, the local kernel can query HDFS parquet directly and skip the Spark kernel.
try:
    import pyarrow as pa
    print("pyarrow", pa.__version__)
    try:
        import pyarrow.fs as pafs
        _hdfs = pafs.HadoopFileSystem("default")
        print("pyarrow HDFS: connected. /user/427966379 listing (first 10):")
        for _i in _hdfs.get_file_info(pafs.FileSelector("/user/427966379", recursive=False))[:10]:
            print("   ", _i.path)
    except Exception as e:
        print("pyarrow HDFS: FAILED -", type(e).__name__, str(e)[:300])
except ImportError:
    print("pyarrow NOT INSTALLED")


# %% [P6b] THE question for the brain-local kernel: can it read the UCP parquet on HDFS
# Three escalating checks: list the UCP root, list one month partition, read ONE row group
# of ONE parquet file (never the whole table - a month partition is large).
# If all three pass on brain-local, the unified pipeline runs end-to-end on this kernel.
# If only P5 (hdfs CLI) passed, plan B: `hdfs dfs -get` landed outputs to pod disk, read locally.
# If neither, all pulls + UCP joins must run on the YARN kernel; brain-local is analysis-only.
UCP_ROOT = "/prod/sz/tsz/00172/data/ucp4"   # from references/ucp/ canon - adjust if canon says otherwise
try:
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq
    _hdfs = pafs.HadoopFileSystem("default")
    _parts = [_i.path for _i in _hdfs.get_file_info(pafs.FileSelector(UCP_ROOT, recursive=False))]
    print("UCP root: OK,", len(_parts), "entries. Last 3:", _parts[-3:])
    _month = sorted([p for p in _parts if "MONTH_END_DATE=" in p])[-1]
    _files = [_i.path for _i in _hdfs.get_file_info(pafs.FileSelector(_month, recursive=False))
              if _i.path.endswith((".parquet", ".parq")) or _i.size and _i.size > 0]
    print("UCP month partition: OK,", len(_files), "files in", _month.split("/")[-1])
    _pf = pq.ParquetFile(_files[0], filesystem=_hdfs)
    _batch = next(_pf.iter_batches(batch_size=5))
    print("UCP READ: OK - 5 rows,", _batch.num_columns, "cols. Columns incl:",
          [c for c in _batch.schema.names if c.upper() in
           ("CLNT_NO", "CLNT_TYP", "AGE", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT")])
except Exception as e:
    print("UCP via pyarrow: FAILED -", type(e).__name__, str(e)[:300])


# %% [P6c] THE decisive test after P6b failed on libjvm: can the IN-SCOPE SPARK SESSION
# read the UCP parquet from HDFS. This is the same route spotlight.py already uses, so if
# this passes, the unified pipeline runs unchanged on this kernel. Reads 3 rows, nothing more.
UCP_ROOT = "/prod/sz/tsz/00172/data/ucp4"
try:
    # partition listing via hdfs CLI (proven working in P5), newest month:
    import subprocess
    _ls = subprocess.run(["hdfs", "dfs", "-ls", UCP_ROOT], capture_output=True, text=True, timeout=120)
    _months = sorted(l.split()[-1] for l in _ls.stdout.splitlines() if "MONTH_END_DATE=" in l)
    print("UCP partitions via hdfs CLI:", len(_months), "| newest:", _months[-1] if _months else "NONE")
    _df = spark.read.parquet(_months[-1])
    _pdf = _df.limit(3).toPandas()
    print("SPARK UCP READ: OK -", len(_pdf), "rows,", len(_pdf.columns), "cols")
    print("   key cols present:", [c for c in _pdf.columns if c.upper() in
          ("CLNT_NO", "CLNT_TYP", "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT")])
except Exception as e:
    print("SPARK UCP READ: FAILED -", type(e).__name__, str(e)[:300])
    print("   if this failed, plan B: run pulls+UCP join on the YARN kernel, land outputs,")
    print("   then 'hdfs dfs -get' them to pod disk (P5 proved the CLI works) for local analysis.")


# %% [P7] What is actually mounted - is the shared drive reachable from the pod at all
# A network share, if mounted, shows up here as cifs/nfs/smb/fuse. If nothing does, no amount of
# code fixes it - it is an infra request, not a coding problem.
try:
    with open("/proc/mounts") as fh:
        _m = fh.read().splitlines()
    _net = [l for l in _m if any(t in l for t in ("cifs", "nfs", "smb", "fuse", "9p", "sshfs"))]
    print("network-ish mounts:", len(_net))
    for l in _net:
        print("   ", l[:200])
    if not _net:
        print("   NONE - no network share is mounted in this pod")
except Exception as e:
    print("/proc/mounts unreadable:", type(e).__name__, str(e)[:150])

print("\ntop-level dirs (a mounted share often appears here):")
for _d in sorted(os.listdir("/")):
    _p = "/" + _d
    try:
        print("   %-20s %s" % (_d, "readable" if os.access(_p, os.R_OK) else "no read"))
    except Exception:
        pass


# %% [P8] Is home persistent or does it die with the pod
# If home is an emptyDir, everything written locally vanishes on restart and only HDFS survives.
_home = os.path.expanduser("~")
try:
    _r = subprocess.run(["df", "-h", _home], capture_output=True, text=True, timeout=60)
    print(_r.stdout or _r.stderr)
except Exception as e:
    print(type(e).__name__, str(e)[:150])
print("Look at the mount source above. overlay/tmpfs/emptyDir => NOT persistent, HDFS is the only "
      "durable store. A PVC or NFS source => local files survive a pod restart.")


# %% [P9] VERDICT - fill this in from the output above
print("""
Answer these from P1-P8, then decide the pipeline:

1. P3 OK?            -> Spark kernel CAN write local CSVs via toPandas(). No kernel switch needed.
2. P5 or P6 OK?      -> HDFS is reachable WITHOUT Spark. The local kernel can read UCP parquet
                        directly and the Spark kernel becomes optional.
3. P7 shows a share? -> a direct pod -> shared-drive write is possible; point the output there.
   P7 shows nothing? -> no code can reach the share. Either request a mount, or accept one manual
                        download step at the end.
4. P8 not persistent?-> never leave the only copy on local disk.

Best case (P3 and P6 both OK, P7 empty): stay on ONE kernel, read HDFS for UCP, write CSVs to the
pod with toPandas().to_csv(), download once at the end. That removes the kernel juggling and every
intermediate manual step except the last hop.
""")


# %% [P10] EXPORT THE RAW PARQUET - measure, stage locally, zip to one file
# Measured 2026-07-31: base_v2 104.5 M, ucp_enriched 14.4 M (the second column -du prints is the
# 3x-replicated footprint, not the download size). All 10 bites lands around 1 GB - still trivial.
#
# Why bother: at ~120 MB the RAW GRAIN fits on a laptop, so the cubes stop being the deliverable.
# Any new cut becomes a local duckdb query instead of a Spark rerun and another download round trip.
# Works from any kernel - hdfs CLI was confirmed rc 0 in P4/P5, no Spark session needed.
import subprocess

HDFS_BASE = "/user/427966379/unsub_spotlight"
LOCAL_PQ = "/home/jovyan/spotlight_out/parquet"
DATASETS = ["base_v2", "ucp_enriched"]


def sh(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=1800)
    print((r.stdout or "") + (r.stderr or ""), end="")
    return r.returncode


print("=== sizes on HDFS ===")
for d in DATASETS:
    sh("hdfs dfs -du -s -h %s/%s" % (HDFS_BASE, d))

print("\n=== staging to pod local disk (home is a PVC, confirmed persistent in P8) ===")
sh("mkdir -p %s" % LOCAL_PQ)
for d in DATASETS:
    if sh("hdfs dfs -get -f %s/%s %s/" % (HDFS_BASE, d, LOCAL_PQ)) != 0:
        print("!! get failed for", d, "- stopping before the zip so a partial export is not shipped")
        raise SystemExit(1)
sh("du -sh %s/*" % LOCAL_PQ)

print("\n=== one zip = one download ===")
sh("cd /home/jovyan/spotlight_out && rm -f spotlight_parquet.zip && "
   "zip -rq spotlight_parquet.zip parquet/ && ls -lh spotlight_parquet.zip")
print(r"""
Download /home/jovyan/spotlight_out/spotlight_parquet.zip via the Jupyter file browser.

Point the browser's download location at the share so it lands there directly:
  \maple.fg.rbc.com\data\Toronto\wrkgrp\wrkgrp16\Marketing Services & Transformation\
  Marketing Analytics\Pod of Gold\Cards\Unsubs\out

Then query it locally in VS Code - duckdb reads parquet off disk without loading it into RAM:

  import duckdb
  duckdb.sql(
      "SELECT mne, SUM(unsub_flag) AS leavers, COUNT(*) AS clients "
      "FROM 'parquet/base_v2/*/*.parquet' GROUP BY 1 ORDER BY 2 DESC"
  ).df()

Check the glob depth against the real layout - bites land as base_v2/bite_N/part-*.parquet, so
'parquet/base_v2/*/*.parquet' is right for the bitten base, and 'parquet/ucp_enriched/*.parquet'
for the single-level one.
""")
