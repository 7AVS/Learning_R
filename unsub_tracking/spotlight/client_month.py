# client_month.py
# Standalone. Paste this whole file into one cell and run it. Depends on nothing else.
#
# One row per (client, month). This is the primitive that should have been pulled first - three
# separate brief questions are blocked without it:
#   - monthly unsubscribe RATE (clientagg only kept first/last send date, so there is no monthly
#     denominator: a client mailed in Aug and March looks identical to one mailed every month)
#   - contact load in the N months BEFORE a client's exposure to any given campaign
#   - any trailing window other than the 3m/6m that were hardcoded at extract time
#
# Every window question becomes a SUM over months after this, with no further pulls, ever.
#
# ENGINE: Cell [1] Teradata-direct. Everything after is PySpark on landed parquet.

# %% [0] CONFIG - the only things to touch
WIN_FLOOR    = "2025-08-01"   # send/unsub window opens
WIN_CEIL     = "2026-08-01"   # hard ceiling, so bites run at different times and still agree
MASTER_FLOOR = "2025-05-01"   # 3 months before WIN_FLOOR. load_tm is a RECORD-LOAD timestamp, not
                              # a send date - with no margin the inner join deletes in-window
                              # unsubs whose MASTER row loaded earlier (canon 20.3).

N_BITES = 10
SMOKE = True                  # True -> bite 0 only (10% of clients). Flip to False for the full run.
LAND_CHUNK_ROWS = 1_500_000   # createDataFrame on a whole bite exceeds spark.rpc.message.maxSize

CARDS_MNES = frozenset({"PCQ", "PCL", "PCD", "AUH", "CLI", "CRV",
                        "VBA", "VBU", "CRO", "CEC", "VIF", "MET"})
REGULATORY_MNES = frozenset({
    "AFD", "BPU", "BUK", "CFR", "EOE", "FNE", "FSA", "FSO", "FXR", "GAF", "HFC",
    "HPN", "IOO", "NST", "OTC", "PUK", "ROP", "TWI", "VMF", "VOA", "ZDC", "ZHX"})

CARDS_SQL = ", ".join("'" + m + "'" for m in sorted(CARDS_MNES))
REG_SQL = ", ".join("'" + m + "'" for m in sorted(REGULATORY_MNES))

BASE = "hdfs:///user/427966379/unsub_spotlight/"
SCHEMA_VERSION = 1
DIR = BASE + "client_month_v%d/" % SCHEMA_VERSION

import time
import getpass
import teradatasql
import pandas as pd
import subprocess
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, IntegerType

SCHEMA = StructType([
    StructField("clnt_no", LongType(), True),
    StructField("ym", IntegerType(), True),                    # yyyymm
    StructField("n_emails", LongType(), True),                 # distinct (campaign, day) sends
    StructField("n_campaigns", LongType(), True),              # distinct campaigns mailed that month
    StructField("n_emails_promo", LongType(), True),           # branded and not regulatory
    StructField("n_emails_regulatory", LongType(), True),
    StructField("n_emails_cards", LongType(), True),
    StructField("n_campaigns_cards", LongType(), True),
    StructField("n_unsub_events", LongType(), True),           # unsubs logged that month
])
COLS = [f.name for f in SCHEMA.fields]
print("CONFIG:", WIN_FLOOR, "to", WIN_CEIL, "| bites:", N_BITES, "| SMOKE:", SMOKE, "|", DIR)


# %% [1] PULL - the one expensive cell. Bitten, landed, resumable.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")
EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com", user=username, password=password,
                          logmech="LDAP")
_cur = EDW.cursor()
_cur.execute("SELECT USER, SESSION, CURRENT_TIMESTAMP")
print("EDW round-trip:", _cur.fetchall())
_cur.close()


def edw_pd(sql, chunksize=1_000_000):
    parts, n, t0 = [], 0, time.time()
    for c in pd.read_sql(sql, EDW, chunksize=chunksize):
        parts.append(c); n += len(c)
        print("   ...", n, "rows,", int(time.time() - t0), "s", flush=True)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _landed(name):
    """True only if the data AND its row-count marker agree. A run killed mid-write leaves a short
    directory, and without the marker the rerun would report it as complete."""
    try:
        n = spark.read.parquet(DIR + name).count()
    except Exception:
        return False
    try:
        marker = int(spark.read.text(DIR + name + "/_ROWCOUNT").collect()[0][0])
    except Exception:
        print(name, ": data present but no _ROWCOUNT marker - treating as partial, re-pulling")
        return False
    if marker != n:
        print(name, ": marker says", marker, "but found", n, "- partial, re-pulling")
        return False
    print(name, ": already landed,", n, "rows - SKIP")
    return True


def _prep(pdf):
    pdf = pdf.copy()
    pdf.columns = [c.lower() for c in pdf.columns]
    # int64, never float: pandas renders large float ids in scientific notation, which silently
    # zero-matches every downstream join.
    assert pd.to_numeric(pdf["clnt_no"], errors="coerce").isna().sum() == 0, "null clnt_no survived the SQL filter"
    pdf["clnt_no"] = pd.to_numeric(pdf["clnt_no"], errors="coerce").astype("int64")
    pdf["ym"] = pd.to_numeric(pdf["ym"], errors="coerce").fillna(0).astype("int32")
    for c in COLS[2:]:
        pdf[c] = pd.to_numeric(pdf[c], errors="coerce").fillna(0).astype("int64")
    return pdf[COLS]


def _write(pdf, name):
    first = True
    for s in range(0, len(pdf), LAND_CHUNK_ROWS):
        part = pdf.iloc[s:s + LAND_CHUNK_ROWS]
        (spark.createDataFrame(part, schema=SCHEMA)
              .write.mode("overwrite" if first else "append").parquet(DIR + name))
        first = False
        print("   ...", name, "chunk", s, "-", s + len(part), "written", flush=True)
    n = spark.read.parquet(DIR + name).count()
    # marker goes INSIDE the bite directory, not beside it - a sibling doubles the listing
    spark.createDataFrame([(str(n),)], "v string").coalesce(1).write.mode("overwrite").text(DIR + name + "/_ROWCOUNT")
    return n


def land_bite(bite):
    name = "bite_%d" % bite
    if _landed(name):
        return
    sql = """
    WITH ek AS (
        -- One row per (client, campaign, disposition, DAY). Same-day retries collapse; sends on
        -- different days stay separate, because a campaign id genuinely gets reused across months.
        SELECT consumer_id_hashed, TREATMENT_ID, disposition_cd,
               MIN(disposition_dt_tm) AS dt
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd IN (1, 4)
          AND disposition_dt_tm >= DATE '%(floor)s'
          AND disposition_dt_tm <  DATE '%(ceil)s'
          AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
          AND SUBSTR(TRIM(TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'
        GROUP BY 1, 2, 3, CAST(disposition_dt_tm AS DATE)
    ),
    joined AS (
        SELECT m.CLNT_NO AS clnt_no,
               SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne,
               ek.disposition_cd AS cd,
               (EXTRACT(YEAR FROM ek.dt) * 100 + EXTRACT(MONTH FROM ek.dt)) AS ym
        FROM ek
        -- MASTER is NOT one row per (consumer_id_hashed, TREATMENT_ID) - it duplicates per card.
        -- The bite predicate sits INSIDE this subquery: in the outer WHERE the full scan spools
        -- before the bite narrows anything, which is what threw 2646 repeatedly.
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE load_tm >= DATE '%(mfloor)s'
                      AND CLNT_NO IS NOT NULL
                      AND MOD(ABS(CLNT_NO), %(n)d) = %(b)d) m
          ON m.consumer_id_hashed = ek.consumer_id_hashed
         AND m.TREATMENT_ID = ek.TREATMENT_ID
    )
    SELECT clnt_no,
           ym,
           SUM(CASE WHEN cd = 1 THEN 1 ELSE 0 END)                          AS n_emails,
           COUNT(DISTINCT CASE WHEN cd = 1 THEN mne END)                    AS n_campaigns,
           SUM(CASE WHEN cd = 1 AND mne NOT IN (%(reg)s) THEN 1 ELSE 0 END) AS n_emails_promo,
           SUM(CASE WHEN cd = 1 AND mne IN (%(reg)s) THEN 1 ELSE 0 END)     AS n_emails_regulatory,
           SUM(CASE WHEN cd = 1 AND mne IN (%(cards)s) THEN 1 ELSE 0 END)   AS n_emails_cards,
           COUNT(DISTINCT CASE WHEN cd = 1 AND mne IN (%(cards)s) THEN mne END) AS n_campaigns_cards,
           SUM(CASE WHEN cd = 4 THEN 1 ELSE 0 END)                          AS n_unsub_events
    FROM joined
    GROUP BY clnt_no, ym
    """ % {"floor": WIN_FLOOR, "ceil": WIN_CEIL, "mfloor": MASTER_FLOOR,
           "n": N_BITES, "b": bite, "reg": REG_SQL, "cards": CARDS_SQL}

    pdf = edw_pd(sql)
    assert len(pdf) > 0, name + " pulled zero rows - investigate before proceeding"
    pdf = _prep(pdf)
    n = _write(pdf, name)
    assert n == len(pdf), "%s readback mismatch: pulled %d, read back %d" % (name, len(pdf), n)
    print(name, ": landed", n, "rows")


for _b in (range(1) if SMOKE else range(N_BITES)):
    land_bite(_b)
print("DONE - one row per (client, month) at", DIR)


# %% [2] PROOF - the three things this unblocks. All small, all print-only.
cm = spark.read.parquet(DIR + "bite_*")
_missing = [c for c in COLS if c not in cm.columns]
assert not _missing, "landed data is missing %s - found %s" % (_missing, cm.columns)
print("client_month rows:", cm.count(), "| distinct clients:", cm.select("clnt_no").distinct().count())

print("\n1. MONTHLY UNSUB RATE - counts only, derive the rate yourself")
(cm.groupBy("ym")
   .agg(F.countDistinct("clnt_no").alias("clients_mailed"),
        F.sum("n_emails").alias("emails"),
        F.sum("n_unsub_events").alias("unsub_events"))
   .orderBy("ym").show(15, truncate=False))

print("2. CONTACT LOAD BY LEAVER STATUS - full window, per client")
_lv = cm.groupBy("clnt_no").agg(F.max(F.when(F.col("n_unsub_events") > 0, 1).otherwise(0)).alias("leaver"),
                                F.sum("n_emails").alias("emails"),
                                F.sum("n_campaigns").alias("campaign_months"),
                                F.countDistinct("ym").alias("active_months"))
(_lv.groupBy("leaver")
    .agg(F.count("*").alias("clients"),
         F.expr("percentile_approx(emails, 0.5)").alias("median_emails"),
         F.expr("percentile_approx(active_months, 0.5)").alias("median_active_months"))
    .orderBy("leaver").show(truncate=False))

print("3. ANY TRAILING WINDOW, no re-pull. Example: last 3 months of the window.")
_max_ym = cm.agg(F.max("ym")).collect()[0][0]
_cut = _max_ym - 2 if _max_ym % 100 > 2 else _max_ym - 100 + 10
(cm.filter(F.col("ym") >= _cut).groupBy()
   .agg(F.countDistinct("clnt_no").alias("clients"),
        F.sum("n_emails").alias("emails")).show(truncate=False))
print("Change the cutoff and re-run cell [2] - the pull never runs again.")
