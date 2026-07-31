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
