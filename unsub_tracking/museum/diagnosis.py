# %% [0] DIAGNOSIS - run every cell top to bottom, screenshot cell [9]'s summary. Nothing to type except credentials when prompted.
# Every test is guarded: a failure prints itself and the next cell still runs.
import sys, os, glob, socket, traceback
DIAG = {}
def ok(k, v):
    DIAG[k] = v
    print(("[OK]   " if not str(v).startswith("FAIL") else "[FAIL] ") + k + " : " + str(v))
print("python:", sys.version.split()[0], "| executable:", sys.executable)

# %% [1] Library availability
for lib in ["teradatasql", "trino", "pandas", "pyarrow", "teradataml"]:
    try:
        __import__(lib)
        ok("import " + lib, "available")
    except Exception as e:
        ok("import " + lib, "FAIL - " + str(e).split("(")[0])

# %% [2] Spark session + classpath configuration
try:
    ok("spark.version", spark.version)
    for k in ["spark.jars", "spark.jars.packages", "spark.driver.extraClassPath", "spark.executor.extraClassPath", "spark.submit.pyFiles"]:
        try:
            ok("conf " + k, spark.conf.get(k))
        except Exception:
            ok("conf " + k, "(not set)")
except NameError:
    ok("spark session", "FAIL - no pre-initialized spark in this kernel")

# %% [3] Is the Teradata JDBC driver class on the JVM classpath?
try:
    spark._jvm.java.lang.Class.forName("com.teradata.jdbc.TeraDriver")
    ok("TeraDriver on JVM classpath", "YES - JDBC route usable")
except Exception as e:
    ok("TeraDriver on JVM classpath", "FAIL - " + str(e).split(":")[-1].strip()[:80])

# %% [4] Hunt for the Teradata jar on this machine (so platform ask can name the path)
hits = []
for pat in ["/opt/cloudera/parcels/*/lib/*/jars/*tera*.jar", "/opt/cloudera/parcels/*/jars/*tera*.jar",
            "/opt/*/tera*.jar", "/usr/share/java/*tera*.jar", os.path.expanduser("~") + "/*tera*.jar"]:
    hits += glob.glob(pat)
ok("terajdbc jar files found", hits if hits else "none in common paths")

# %% [5] Hostname resolution - both Trino spellings (settles the 1-vs-l ambiguity) + Teradata host
for host in ["strp1vaexh0001.fg.rbc.com", "strplvaexh0001.fg.rbc.com", "Teradata-dns-sysa.fg.rbc.com"]:
    try:
        ip = socket.gethostbyname(host)
        ok("resolve " + host, ip)
    except Exception as e:
        ok("resolve " + host, "FAIL - " + str(e))

# %% [6] Credentials (one prompt, reused by tests below)
import getpass
username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")
print("credentials captured for this session only")

# %% [7] teradatasql direct connect test (pure Python - no Spark, no jar)
try:
    import teradatasql
    con = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com", user=username, password=password, logmech="LDAP")
    cur = con.cursor()
    cur.execute("SELECT 1")
    ok("teradatasql connect + SELECT 1", "SUCCESS - rows: " + str(cur.fetchall()))
    cur.close(); con.close()
except Exception as e:
    ok("teradatasql connect", "FAIL - " + str(e)[:300])

# %% [8] Spark JDBC tiny read test (only meaningful if cell [3] said YES)
try:
    df = (spark.read.format("jdbc")
          .option("driver", "com.teradata.jdbc.TeraDriver")
          .option("url", "jdbc:teradata://Teradata-dns-sysa.fg.rbc.com/LOGMECH=LDAP,TMODE=TERA,CHARSET=UTF8,ENCRYPTDATA=ON")
          .option("dbtable", "(SELECT 1 AS x) as src")
          .option("user", username).option("password", password).load())
    ok("spark jdbc SELECT 1", "SUCCESS - " + str(df.collect()))
except Exception as e:
    ok("spark jdbc SELECT 1", "FAIL - " + str(e).split("\n")[0][:200])

# %% [9] HDFS write/read test + SUMMARY (screenshot THIS output)
try:
    tdf = spark.createDataFrame([(1, "diag")], ["id", "tag"])
    tdf.write.mode("overwrite").parquet("hdfs:///user/427966379/unsub_cpc/diag_test")
    n = spark.read.parquet("hdfs:///user/427966379/unsub_cpc/diag_test").count()
    ok("HDFS write+read /user/427966379/unsub_cpc/", "SUCCESS - " + str(n) + " row")
except Exception as e:
    ok("HDFS write+read", "FAIL - " + str(e).split("\n")[0][:200])

print("\n================ DIAGNOSIS SUMMARY - screenshot from here down ================")
for k, v in DIAG.items():
    print(("[OK]   " if not str(v).startswith("FAIL") else "[FAIL] ") + k + " : " + str(v))
print("=================================================================================")
print("Decision guide:")
print(" - 'TeraDriver on JVM classpath: YES' or 'spark jdbc SELECT 1: SUCCESS' -> cpc_evidence_spark.py works AS IS")
print(" - 'teradatasql connect: SUCCESS' -> jar-free rewrite is the path (say the word)")
print(" - both FAIL -> platform ask: 'need Teradata JDBC jar in Spark session OR teradatasql package;")
print("   your own Teradata_Table_Read_Load sample fails with ClassNotFoundException in my kernel'")
