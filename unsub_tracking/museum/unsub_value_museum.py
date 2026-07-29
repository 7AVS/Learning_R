# UNSUB VALUE - MUSEUM. The value of an unsubscribe: who leaves, who stays, and how campaigns compare.
#
# WINDOW: sends and unsubs in March, April, May 2026. Bank-wide.
# ANCHOR: ONE UCP snapshot, 2026-02-28 - the month-end immediately before the window. Every client
#   on every side of every comparison is profiled at the same pre-treatment moment, which is what
#   makes leaver-vs-stayer a real comparison. The 12-month version of this file anchored each client
#   at their own treatment's launch, which fanned out to 30 UCP partitions and got the session killed
#   by YARN (2026-07-27); that machinery is gone. Git holds it.
#
# THREE SENDER TIERS, and why:
#   T1 senders_by_mne  - server-side COUNT(DISTINCT client) per MNE. ~200 rows. Bank-wide.
#                        Gives unsub_per_1000 for EVERY campaign in the bank at no transfer cost.
#   T2 senders_base    - DISTINCT CLNT_NO, bank-wide, one column. The mailed population, for the
#                        bank-wide baseline profile.
#   T3 senders_cards   - DISTINCT (CLNT_NO, mne), CARDS MNEs only, filtered server-side. Per-campaign
#                        sender/stayer profiles. Cards-only because bank-wide client x MNE is ~99M
#                        pairs off 143M send events - that pull was killed 2026-07-27. Non-cards
#                        campaigns therefore get counts and a rate, but no sender profile.
#
# POPULATIONS (one row per client):
#   leaver      - mailed in window AND unsubscribed in window
#   already_out - mailed in window, no unsub in window, but unsubscribed BEFORE it (the leak; 53,726
#                 clients in the Q2 run). Split out, NOT counted as a stayer - they were never
#                 reachable, so calling them "chose to stay" would be false.
#   stayer      - mailed, no unsub in window, no unsub before it
#
# ENGINE SPLIT: EDW (teradatasql) for vendor feedback; HDFS/Spark for UCP. There is no EDW
#   client-attribute table - the split is unavoidable, not a preference.
#
# ============================ HOW TO RUN THIS FILE ============================
#
#   ANALYSIS ONLY (the normal case)   run [1], then [9] onward.
#   FRESH PULL (rarely)               run [1], [1b], then [2]..[8], then [9] onward.
#   BRIEF TOP-UP (2026-07-29)         run [1], [1b], then [5b] and [7b] ONLY, then [9] onward.
#                                     Two new pulls, nothing already landed is touched.
#
# WHAT THE 2026-07-29 EDIT ADDED, and which brief item each one answers. Every new cell SKIPS with a
# printed reason if its landing is absent, so the whole file still runs on 2026-07-29 08:29's data:
#   [5b] cadence_by_mne   deployment waves per campaign        -> 1a "weekly, bi-weekly, monthly"
#   [7b] senders_wide     client x mne BANK-WIDE, stratified   -> 3b breadth, 3c co-occurrence
#   [19] tibc_mix in L7   computed since day one, never shown  -> 4a "depth of relationship (TIBC)"
#   [16] prod_band 3-4 split into 3 and 4                      -> 4 "bucket 1 through 4"
#   [20g] L13            pair rate vs both solo rates          -> 3c "do unsubs spike when
#                                                                    specific campaigns run together"
#   [6]  n_treatments -> n_mnes  (waves were being called campaigns)  -> 3b, correctly
#
# OUTPUT SHAPE. Ten csv exports became TWO, because there are only two grains:
#   unsub_segments   a row is a SEGMENT,  campaign is a scope column  -> WHO leaves
#   unsub_campaigns  a row is a CAMPAIGN or a PAIR, no client attrs   -> HOW MANY leave, HOW OFTEN
# The L-tables still print on screen - that is where the analysis is read. The seven older csvs are
# still written as backing detail for when a number is challenged.
#
# [1] holds imports, constants and helpers. It prompts for nothing, opens no connection and starts no
# Spark job. [1b] is the ONLY cell that asks for a password, and nothing below [8] needs it. So the
# analysis half runs from a cold kernel without re-pulling anything - it reads the landed parquet
# from HDFS and nothing else.
#
# There is no duplicated helper block: [1] is shared, and it is free. The dependency that used to
# force a re-pull was the EDW connection sitting inside the setup cell; it now sits alone in [1b].
#
# Two flags at the top of [1] decide how much work a run does:
#   PROVEN = True        skip the validation layer - readback counts, coverage, null profiles,
#                        orphan joins. Those answers are in RESULTS_CATALOG.md. Set False only when
#                        a window, an anchor or a pull changes.
#   STAGE_REBUILD = set()  reuse the staged frames on HDFS (ucp_spine, clients_ucp, banded,
#                        cards_send). Name one to rebuild it; STAGE_ALL for all four.
#
# Neither flag changes a measured number. They decide whether work already done is done again.
#
# OUTPUT CONTRACT
#   DURABLE : client_spine, summary (parquet), summary_csv - all on HDFS.
#   SCRATCH : the five raw EDW landings. They exist only so a kernel death mid-pull costs nothing.
#             Removable via the opt-in cleanup cell once the durable outputs verify.
#   NOTHING is written to the notebook working directory - local writes do not work from this
#   YARN/Spark kernel. pandas .to_csv / .to_excel appear nowhere in this file.
#
# Date: 2026-07-27.

# %% [1] SETUP - constants and helpers. NO EDW, NO password, NO Spark job. Run this always: it is
# the only cell the analysis half needs from above it, and it costs nothing.
import hashlib
import re
import pandas as pd
from IPython.display import display, Markdown
from pyspark.sql import functions as F, Window

# pandas>=2.0 removed DataFrame.iteritems but Spark 3.3's createDataFrame still calls it
if not hasattr(pd.DataFrame, "iteritems"):
    pd.DataFrame.iteritems = pd.DataFrame.items

# numpy>=1.24 removed np.bool/np.object/np.int/np.float, but Spark 3.3's pandas->Spark conversion
# still references np.bool for BooleanType columns ("AttributeError: module 'numpy' has no
# attribute 'bool'", seen on this kernel 2026-07-27). Restoring the aliases is the minimal fix.
import numpy as np
for _alias, _builtin in (("bool", bool), ("object", object), ("int", int),
                         ("float", float), ("str", str)):
    if not hasattr(np, _alias):
        setattr(np, _alias, _builtin)

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 220)
pd.set_option("display.max_rows", 80)

# ---- WINDOW. Every SQL bound below is derived from these two constants. Change them here only. ----
WIN_START = "2026-03-01"
WIN_END = "2026-06-01"
UCP_ANCHOR = "2026-02-28"          # month-end BEFORE the window: pre-treatment for every client
PRIOR_FLOOR = "2024-01-01"         # house rule: no scan reaches below 2024
WIN_MONTHS = [("m2026_03", "2026-03-01", "2026-04-01", "2026-02-01", "2026-05-01"),
              ("m2026_04", "2026-04-01", "2026-05-01", "2026-03-01", "2026-06-01"),
              ("m2026_05", "2026-05-01", "2026-06-01", "2026-04-01", "2026-07-01")]

# CARDS_MNES: sourced from UNSUB_TRACKING_KNOWLEDGE.md section 4 and cross-checked against
# archaeology/email_active_mnes.md. CTU and O2P are deliberately EXCLUDED per Andre 2026-07-26 -
# they involve cards but are reported inside async, out of the cards package.
CARDS_MNES = frozenset({"PCQ", "PCL", "PCD", "AUH", "CLI", "MVP", "CRV"})
CARDS_SQL_LIST = ", ".join("'" + m + "'" for m in sorted(CARDS_MNES))

BASE = "hdfs:///user/427966379/unsub_value_museum/"
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


def _sqlkey(sql):
    return hashlib.md5(re.sub(r"\s+", " ", sql).strip().upper().encode()).hexdigest()


def _reads_as_absent(msg):
    """Does this Spark error mean 'nothing is there', as opposed to 'I could not look'?

    The distinction is the whole point of landed(): a broken session must stop the run rather than
    masquerade as an empty reservoir (that mistake re-pulled everything once, 2026-07-27).

    "Unable to infer schema for Parquet" belongs on the ABSENT side and was missing from this list
    until 2026-07-29. Spark raises it after it has SUCCESSFULLY listed the path and found no parquet
    files - an empty directory, a directory holding only _SUCCESS, or on some Hadoop builds a path
    that simply is not there. Every one of those is "not landed". A session that genuinely cannot
    reach HDFS fails earlier and differently: ConnectException, IOException, auth. So this string is
    evidence the session WORKS, and treating it as an unverifiable state aborted a run that had
    nothing wrong with it.

    Matching is case-insensitive because the wording moved: Spark 3.4 wraps the same condition in
    the error class UNABLE_TO_INFER_SCHEMA.
    """
    m = msg.lower()
    return any(s in m for s in ("path does not exist", "path_not_found", "filenotfound",
                                "unable to infer schema", "unable_to_infer_schema"))


def landed(name):
    try:
        spark.read.parquet(BASE + name).limit(1).collect()
        return True
    except Exception as e:
        msg = str(e)
        if _reads_as_absent(msg):
            return False
        raise RuntimeError(name + ": cannot VERIFY HDFS state - refusing to pull anything. "
                           "Fix the spark/HDFS session first. Underlying error: " + msg[:300])


def _to_spark(pdf, name):
    allnull = [c for c in pdf.columns if pdf[c].isna().all()]
    if not allnull:
        return spark.createDataFrame(pdf)
    print(name, ": columns 100% NULL in this pull ->", allnull, "(landed as string nulls)")
    sdf = spark.createDataFrame(pdf[[c for c in pdf.columns if c not in allnull]])
    for c in allnull:
        sdf = sdf.withColumn(c, F.lit(None).cast("string"))
    return sdf.select(*[c for c in pdf.columns])


def land(name, sql, replace=False):
    # Same SQL already landed -> SKIP. Changed SQL -> re-pull and overwrite. Readable path with no
    # manifest = a killed mid-write -> treated as not landed. The SQL here is the source of truth.
    meta = BASE + "_meta/" + name.replace("/", "_")
    key = _sqlkey(sql)
    if landed(name) and not replace:
        try:
            old = spark.read.parquet(meta).collect()[0]
            if old["sql_md5"] == key:
                print(name, ": already landed with SAME query,", old["rows"], "rows (", old["landed_at"], ") - SKIP")
                return
            print(name, ": QUERY CHANGED since it landed", old["landed_at"], "- re-pulling and OVERWRITING")
        except Exception as e:
            msg = str(e)
            # Same classification as landed(), same reason. A manifest that was never written reads
            # back as "unable to infer schema" just as often as "path does not exist", and treating
            # that as unverifiable would refuse to re-pull data that is simply not there yet.
            if _reads_as_absent(msg):
                print(name, ": readable but NO manifest (partial/killed write) - re-pulling and OVERWRITING")
            else:
                raise RuntimeError(name + ": data readable but manifest CANNOT BE VERIFIED - refusing "
                                   "to re-pull on an unverifiable state. Underlying error: " + msg[:300])
    pdf = edw_pd(sql)
    assert len(pdf) > 0, name + " pulled zero rows - investigate before proceeding"
    _to_spark(pdf, name).write.mode("overwrite").parquet(BASE + name)
    nback = spark.read.parquet(BASE + name).count()
    assert nback == len(pdf), name + " HDFS readback mismatch: pulled " + str(len(pdf)) + " readback " + str(nback)
    spark.createDataFrame([(name, key, sql, __import__("datetime").datetime.now().isoformat(), len(pdf))],
                          ["name", "sql_md5", "sql_text", "landed_at", "rows"]).write.mode("overwrite").parquet(meta)
    print(name, ": landed", len(pdf), "rows, HDFS readback confirms", nback, "| manifest written")


def norm_clnt(col):
    # decimal(18,0) FIRST: CLNT_NO arrives as pandas float64 and float->string renders scientific
    # notation ('1.56314759E8'), which never matches UCP's integer strings. Cost a full debug cycle.
    return F.regexp_replace(F.trim(col.cast("decimal(18,0)").cast("string")), "^0+", "")


def T(label, df):
    """Render a titled table AND return it. Timestamps are stringified first - pandas 2.x rejects
    Spark's unit-less datetime64."""
    out = df
    if hasattr(out, "toPandas"):
        for f in out.schema.fields:
            if f.dataType.typeName() in ("timestamp", "date"):
                out = out.withColumn(f.name, F.col(f.name).cast("string"))
        out = out.toPandas()
    display(Markdown("**" + label + "**  ·  " + str(len(out)) + " rows"))
    display(out)
    return out


# STAGE - the reason a rerun used to cost as much as a first run.
#
# The Teradata pulls skip on a rerun (SQL md5 manifest). Nothing else did. .cache() lives in executor
# memory, so a kernel restart or an evicted executor threw it away and every downstream .count()
# recomputed the 15M-row UCP join from parquet. Sixty-odd actions x one shuffle each.
#
# stage() lands a frame on HDFS and reads it back. On the next run the frame is already there, so the
# build closure never executes and every table below reads a materialized file instead of a plan.
#
# STAGE_REBUILD is a SET OF NAMES, not a switch. Rebuilding all four costs the full first-run price;
# most edits only invalidate one. Name only what your edit actually changed:
#
#   set()                        - reuse everything (default, a rerun is minutes)
#   {"ucp_spine"}                - changed UCP_ANCHOR, UCP_COLS or the CLNT_TYP filter
#   {"clients_ucp", "banded"}    - changed buckets, mne/program, or the band cut points
#   {"banded"}                   - changed only a band definition or high_potential
#   {"cards_send"}               - changed CARDS_MNES or the columns L8/L9 read
#   STAGE_ALL                    - changed the window or the reservoir SQL
#
# Dependency order: ucp_spine -> clients_ucp -> banded -> cards_send. Rebuilding one CASCADES to the
# ones after it - see STAGE_CHILDREN. Naming a stage is enough; you do not have to name its children.
STAGE_ALL = {"ucp_spine", "clients_ucp", "banded", "cards_send"}
STAGE_REBUILD = set()
STAGE_CHILDREN = {"ucp_spine":  ["clients_ucp", "banded", "cards_send"],
                  "clients_ucp": ["banded", "cards_send"],
                  "banded":      ["cards_send"],
                  "cards_send":  []}

# PROVEN - the validation layer is a ONE-TIME cost, not a per-run tax.
#
# This file was built to prove itself: readback counts, coverage tables, null profiles, orphan-join
# checks. Every one of those is a full pass over 10-15M rows, and they run BEFORE any finding. Once
# a window has passed them and the numbers are in RESULTS_CATALOG.md, re-deriving them every run
# buys nothing - the answer is already written down.
#
# PROVEN = False -> run the proofs. Do this the FIRST time a window, an anchor or a pull changes.
# PROVEN = True  -> skip them and go straight to the analysis.
#
# What is NEVER skipped, because the analysis depends on the value and not on the reassurance:
# the profitability quintile cut points, the UCP schema probe, and every assert inside stage().
PROVEN = True


def stage(name, build, requires=None):
    """`requires` is the columns the CURRENT run needs from this frame. A staged parquet that lacks
    one is stale by definition, and reusing it is how a widened pull reaches a groupBy as a missing
    column two hundred lines later.

    The first version of this check compared the staged frame against the frame above it. That was
    useless: the frame above is staged too, so a stale parent and a stale child agree perfectly and
    the check passes. Comparing against a REQUIRED LIST is what makes it work - the list comes from
    the pull, which is the only thing that actually knows what was landed.
    """
    global STAGE_REBUILD
    _stale = []
    if name not in STAGE_REBUILD and landed(name) and requires:
        _have = set(spark.read.parquet(BASE + name).columns)
        _stale = [c for c in requires if c not in _have]

    if name not in STAGE_REBUILD and landed(name) and not _stale:
        df = spark.read.parquet(BASE + name)
        print("  stage", name, "- REUSED,", df.count(), "rows (build skipped)")
        return df

    if _stale:
        _why = "stale - missing " + str(_stale)
    elif name in STAGE_REBUILD:
        _why = "forced by STAGE_REBUILD"
    else:
        _why = "not on HDFS yet"

    # a rebuilt parent invalidates every child, or the next stage quietly reuses a frame built from
    # the version that just got replaced
    _kids = [k for k in STAGE_CHILDREN.get(name, []) if k not in STAGE_REBUILD]
    if _kids:
        STAGE_REBUILD = set(STAGE_REBUILD) | set(_kids)
        print("  stage", name, "rebuilds ->", _kids, "must rebuild too (cascade)")

    build().write.mode("overwrite").parquet(BASE + name)
    df = spark.read.parquet(BASE + name)
    print("  stage", name, "- BUILT (" + _why + "),", df.count(), "rows ->", BASE + name)
    return df


print("helpers defined | BASE =", BASE, "| window", WIN_START, "->", WIN_END, "| UCP anchor", UCP_ANCHOR)

# %% [2] FAIL-FAST GATE - prove the session and HDFS before any pull.
_ = spark.range(1).count()
try:
    _ = (spark.read.option("basePath", UCP_BASE).parquet(UCP_BASE + "MONTH_END_DATE=" + UCP_ANCHOR)
         .limit(1).collect())
except Exception as e:
    raise RuntimeError("Cannot read the UCP anchor partition " + UCP_ANCHOR + " at " + UCP_BASE +
                       " - either HDFS is unreachable from this session or that month-end does not "
                       "exist. Fix before pulling. Underlying error: " + str(e)[:300])
print("GATE PASSED - spark live, HDFS reachable, UCP anchor partition", UCP_ANCHOR, "readable.")

# %% [1b] EDW CONNECTION - the ONLY cell that asks for a password, and the only thing standing
# between a cold kernel and the analysis. SKIP IT to run analysis only: nothing from [9] down
# touches EDW. land() calls edw_pd, but Python resolves that name when land() RUNS, not when it is
# defined, so [1] is happy without this cell as long as you are not pulling.
get_ipython().system("./environment/bin/python -m pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")
import getpass
import teradatasql

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")

TD_HOST = "Teradata-dns-sysa.fg.rbc.com"
EDW = teradatasql.connect(host=TD_HOST, user=username, password=password, logmech="LDAP")


def edw_pd(sql, chunksize=1_000_000):
    import time
    parts, n, t0 = [], 0, time.time()
    for c in pd.read_sql(sql, EDW, chunksize=chunksize):
        parts.append(c); n += len(c)
        print("  ...", n, "rows pulled,", int(time.time() - t0), "s elapsed", flush=True)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


# PROOF, not prints: round-trip the connection and show what the SERVER returned.
_cur = EDW.cursor()
_cur.execute("SELECT USER, SESSION, CURRENT_TIMESTAMP")
print("EDW round-trip (teradatasql) returned:", _cur.fetchall())
_cur.close()

# %% [3] SIZE PROBE - event-only COUNT(*), no join, no DISTINCT. Gated on landed() so a completed
# reservoir costs nothing on a whole-file rerun.
_unlanded = [m for m in WIN_MONTHS if not landed("unsubs_3m/" + m[0])]
if not _unlanded:
    print("SIZE PROBE - all months already landed - SKIP")
else:
    _rows = []
    for _name, _ds, _de, _ls, _le in _unlanded:
        for _cd, _lab in ((1, "sends"), (4, "unsubs")):
            _p = edw_pd("""
SELECT COUNT(*) AS event_rows
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_cd = %d
  AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
""" % (_cd, _ds, _de))
            _rows.append({"month": _name, "kind": _lab, "event_rows": int(_p["event_rows"][0])})
    T("SIZE PROBE - raw event volume per month (upper bound on each pull; the pulls dedupe below this)",
      pd.DataFrame(_rows))

# %% [4] PULL - unsubs, one cell per month. disposition_cd=4. The derived table dedupes EVENT keys
# BEFORE joining MASTER, which is what keeps this off spool error 2646.
_UNSUB_SQL = """
SELECT DISTINCT m.CLNT_NO, ek.TREATMENT_ID, ek.disposition_dt_tm AS unsub_tm
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '%s' AND m.load_tm < DATE '%s'
"""
for _name, _ds, _de, _ls, _le in WIN_MONTHS:
    land("unsubs_3m/" + _name, _UNSUB_SQL % (_ds, _de, _ls, _le))

# %% [5] PULL - TIER 1: senders per MNE, aggregated SERVER-SIDE. ~200 rows come back. We never pull
# client-level sender rows bank-wide: that is ~99M client x MNE pairs off 143M send events, and it
# killed the session on 2026-07-27. Counting how many clients a campaign mailed never needed them.
land("senders_by_mne", """
SELECT SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne, COUNT(DISTINCT m.CLNT_NO) AS senders
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '2026-02-01' AND m.load_tm < DATE '2026-07-01'
GROUP BY 1
""" % (WIN_START, WIN_END))

# %% [5b] PULL - CADENCE per campaign. Brief item 1a: "frequency per campaign to understand if
# weekly, bi-weekly, monthly."
#
# EVENT ONLY - no MASTER join. Deployment cadence is a property of the TREATMENT_ID, and EVENT
# already carries TREATMENT_ID and the timestamp. Joining MASTER here would buy nothing but the
# client number, which cadence does not use. One table, one scan, ~200 rows back.
#
# TREATMENT_ID = TACTIC_ID = MNE + julian date, UNIQUE PER DEPLOYMENT WAVE. So COUNT(DISTINCT
# TREATMENT_ID) per MNE IS the number of waves that campaign ran in the window, and that number
# over the window length is the cadence the brief asks for. 13 waves over 92 days = weekly.
land("cadence_by_mne", """
SELECT SUBSTR(TREATMENT_ID, 8, 3) AS mne,
       COUNT(DISTINCT TREATMENT_ID)                  AS n_deployments,
       COUNT(DISTINCT CAST(disposition_dt_tm AS DATE)) AS send_days,
       MIN(CAST(disposition_dt_tm AS DATE))          AS first_send_dt,
       MAX(CAST(disposition_dt_tm AS DATE))          AS last_send_dt,
       COUNT(*)                                      AS send_events
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_cd = 1
  AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
GROUP BY 1
""" % (WIN_START, WIN_END))

# %% [6] PULL - TIER 2: the mailed population, bank-wide. Now carries CONTACT VOLUME and ENGAGEMENT.
#
# This used to be SELECT DISTINCT m.CLNT_NO - one column, deliberately narrow to survive spool. That
# DISTINCT is what made contact volume unanswerable: it threw away the count of the thing it was
# counting. Two of the three open questions from the exploration deck died on this line.
#
# Same scan, wider: disposition_cd goes from 1 to (1,2,3) and the client row carries counts instead
# of just existence. Still one row per client per month - the GROUP BY is server-side, so what
# crosses the wire is 10.6M x 7 narrow integers instead of 10.6M x 1.
#   1 = sent   2 = opened   3 = clicked
# ONE COUNT(DISTINCT) only; [5] already runs one over this window without spooling.
#
# MONTHLY, so a client mailed in March and April appears TWICE. [9] sums them - it must not DISTINCT
# them, or the volume collapses back to presence and we are where we started.
#
# 2026-07-29 - n_treatments BECAME n_mnes, a swap not an addition.
#   COUNT(DISTINCT TREATMENT_ID) counted DEPLOYMENT WAVES, not campaigns: TREATMENT_ID = TACTIC_ID =
#   MNE + julian date, unique per wave, so three PCQ sends read as three campaigns. L10 was printing
#   that number under the label "mean_distinct_campaigns". Brief item 3b asks for the number of
#   CAMPAIGNS a client was contacted by, which is COUNT(DISTINCT SUBSTR(TREATMENT_ID,8,3)).
#   Swapped rather than added because the comment above is load-bearing - a SECOND COUNT(DISTINCT)
#   over this window is the shape that spooled on 2026-07-27. n_send_events already carries the
#   frequency that n_treatments was standing in for, so nothing is lost.
#
#   CAVEAT, and it is real: this is monthly, and [9] SUMS across months. Distinct counts do not sum.
#   A client mailed by PCQ in March and again in April contributes n_mnes=1 twice, and [9] reports 2.
#   So n_mnes summed is an UPPER BOUND on campaign breadth - exact for a client active in one month,
#   inflated by up to 3x for a client active in all three. [9] therefore also keeps the per-month MAX
#   as a lower bound, and the truth is between them. The exact figure needs senders_wide ([7b]),
#   which is client x mne and can be counted distinctly after the union.
_SENDBASE_SQL = """
SELECT m.CLNT_NO,
       SUM(CASE WHEN ek.disposition_cd = 1 THEN 1 ELSE 0 END) AS n_send_events,
       COUNT(DISTINCT CASE WHEN ek.disposition_cd = 1
                           THEN SUBSTR(ek.TREATMENT_ID, 8, 3) END) AS n_mnes,
       SUM(CASE WHEN ek.disposition_cd = 2 THEN 1 ELSE 0 END) AS n_opens,
       SUM(CASE WHEN ek.disposition_cd = 3 THEN 1 ELSE 0 END) AS n_clicks,
       MAX(CASE WHEN ek.disposition_cd = 2 THEN 1 ELSE 0 END) AS opened,
       MAX(CASE WHEN ek.disposition_cd = 3 THEN 1 ELSE 0 END) AS clicked
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, disposition_cd
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd IN (1, 2, 3)
      AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '%s' AND m.load_tm < DATE '%s'
GROUP BY m.CLNT_NO
HAVING SUM(CASE WHEN ek.disposition_cd = 1 THEN 1 ELSE 0 END) > 0
"""
for _name, _ds, _de, _ls, _le in WIN_MONTHS:
    land("senders_base/" + _name, _SENDBASE_SQL % (_ds, _de, _ls, _le))

# %% [7] PULL - TIER 3: client x MNE for CARDS campaigns only, filtered server-side. This is what
# makes per-campaign sender and stayer profiles possible. Cards-only by necessity, not preference.
_SENDCARDS_SQL = """
SELECT DISTINCT m.CLNT_NO, SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
      AND SUBSTR(TREATMENT_ID, 8, 3) IN (%s)
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '%s' AND m.load_tm < DATE '%s'
"""
for _name, _ds, _de, _ls, _le in WIN_MONTHS:
    land("senders_cards/" + _name, _SENDCARDS_SQL % (_ds, _de, CARDS_SQL_LIST, _ls, _le))

# %% [7b] PULL - TIER 3W: client x MNE, BANK-WIDE, on a stratified population. Brief items 3b and 3c.
#
# WHY THIS EXISTS. Tier 3 is Cards-only, so co-occurrence ("do unsubscribes spike when specific
# campaigns run together") could only ever compare Cards against Cards. The question is how Cards
# compares to the REST OF THE BANK, and that needs a client x MNE pairing over all ~200 mnemonics.
#
# WHY IT IS NOT THE PULL THAT DIED. The abandoned 2026-07-27 pull was all ~200 MNEs x every client -
# ~99M pairs. Two facts shrink that by an order of magnitude without costing precision where it
# matters:
#   LEAVERS ARE RARE - 70,425 clients. Taken WHOLE. No sampling on the population being measured.
#   STAYERS ARE ONLY A BASELINE - 10.1M clients. A 1-in-10 sample is ample, and it is weighted back
#     up by SAMPLE_WEIGHT downstream. Never weight the leaver side: it is a census, not a sample.
# ~1.07M clients x ~9 campaigns each = ~10M pairs, chunked monthly = ~3.3M/month. senders_cards
# already moves ~3M/month without complaint, so this is a pull whose shape is already proven here.
#
# Both predicates resolve SERVER-SIDE - no client list crosses the wire. MOD(CLNT_NO,10) is the same
# spool-control pattern pack 19 adopted after two spool failures. The unsub side keys on
# consumer_id_hashed so it never touches MASTER a second time.
#
# already_out clients fall wherever the modulo puts them: they are neither leaver nor stayer here,
# and [9] flags any that arrive so the weighting cannot silently mis-scale them.
SAMPLE_MOD = 10                     # 1-in-10 stayers. Set to 1 for a census (expect ~99M pairs).
_SENDWIDE_SQL = """
SELECT DISTINCT m.CLNT_NO, SUBSTR(ek.TREATMENT_ID, 8, 3) AS mne
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '%s' AND m.load_tm < DATE '%s'
  AND ( MOD(m.CLNT_NO, %d) = 0
        OR m.consumer_id_hashed IN (
              SELECT DISTINCT consumer_id_hashed
              FROM DTZV01.VENDOR_FEEDBACK_EVENT
              WHERE disposition_cd = 4
                AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s') )
"""
PULL_WIDE = True                    # set False to skip - everything else still runs
if PULL_WIDE:
    for _name, _ds, _de, _ls, _le in WIN_MONTHS:
        land("senders_wide/" + _name,
             _SENDWIDE_SQL % (_ds, _de, _ls, _le, SAMPLE_MOD, WIN_START, WIN_END))
else:
    print("senders_wide SKIPPED (PULL_WIDE = False) - bank-wide 3b/3c will not be available.")

# %% [8] PULL - prior unsubscribers. Keeps the stayer pool honest: a client who opted out last year
# and still got mail is not someone who "chose to stay".
#
# Now carries WHEN they left and WHICH campaign they left through. DISTINCT CLNT_NO answered "is this
# client already out" and nothing else - it could not say how long ago or through whom, which is the
# whole of the third open question. Same scan, two more aggregates, still one row per client.
land("prior_unsubs", """
SELECT m.CLNT_NO,
       MAX(CAST(ek.disposition_dt_tm AS DATE)) AS last_prior_unsub_dt,
       MIN(CAST(ek.disposition_dt_tm AS DATE)) AS first_prior_unsub_dt,
       COUNT(*) AS n_prior_unsub_events,
       -- argmax in one pass: a fixed-format date sorts lexically as it sorts chronologically, so
       -- MAX over date||mne returns the LAST unsub's mnemonic at positions 11-13. MAX(SUBSTR(mne))
       -- alone would return whichever mnemonic sorts highest, which is nobody's exit route.
       SUBSTR(MAX(CAST(CAST(ek.disposition_dt_tm AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(10))
                  || SUBSTR(ek.TREATMENT_ID, 8, 3)), 11, 3) AS last_prior_unsub_mne
FROM (
    SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
) ek
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = ek.consumer_id_hashed AND m.TREATMENT_ID = ek.TREATMENT_ID
WHERE m.load_tm >= DATE '2023-12-01' AND m.load_tm < DATE '2026-04-01'
GROUP BY m.CLNT_NO
""" % (PRIOR_FLOOR, WIN_START))

# %% [9] READBACK - normalise CLNT_NO everywhere, prove every landing is present and non-empty.
def _rd(path):
    return spark.read.parquet(BASE + path).withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO")))


unsubs_raw = _rd("unsubs_3m/*")
senders_cards_raw = _rd("senders_cards/*").select("CLNT_NO", "mne").distinct()
senders_mne = spark.read.parquet(BASE + "senders_by_mne")

# senders_base is MONTHLY, so a client mailed in March and April has two rows. It used to be
# .distinct() on one column, which was correct for presence and destroys a count. SUM across months
# instead - March's 4 sends plus April's 3 is a client contacted 7 times, not a client contacted.
# MAX for the flags: opened once in any month is opened.
_sb = _rd("senders_base/*")
HAVE_CONTACT = "n_send_events" in _sb.columns
# BREADTH_COL - which column carries "how many campaigns", and is it the right one?
#   n_mnes       : COUNT(DISTINCT mnemonic). Campaigns. What brief 3b asks for.
#   n_treatments : COUNT(DISTINCT TREATMENT_ID). Deployment WAVES. What the pre-2026-07-29 landing
#                  holds, and NOT campaigns - see the note in [6]. Usable, but it must not be
#                  labelled as campaigns anywhere downstream, so the label follows the column.
BREADTH_COL = ("n_mnes" if "n_mnes" in _sb.columns
               else ("n_treatments" if "n_treatments" in _sb.columns else None))
BREADTH_IS_CAMPAIGNS = BREADTH_COL == "n_mnes"
BREADTH_LABEL = "campaigns" if BREADTH_IS_CAMPAIGNS else "deployment_waves"   # alias-safe, no spaces
if HAVE_CONTACT:
    _aggs = [F.sum("n_send_events").cast("int").alias("n_send_events"),
             F.sum("n_opens").cast("int").alias("n_opens"),
             F.sum("n_clicks").cast("int").alias("n_clicks"),
             F.max("opened").cast("int").alias("opened"),
             F.max("clicked").cast("int").alias("clicked")]
    if BREADTH_COL:
        # SUM is an upper bound and MAX a lower bound: the pull is monthly and distinct counts do
        # not sum. Both are kept because a single number here would be a guess wearing a name.
        _aggs += [F.sum(BREADTH_COL).cast("int").alias("breadth_upper"),
                  F.max(BREADTH_COL).cast("int").alias("breadth_lower")]
    senders_all_raw = _sb.groupBy("CLNT_NO").agg(*_aggs)
    if not BREADTH_IS_CAMPAIGNS:
        print("!! BREADTH COLUMN IS", BREADTH_COL, "- that is DEPLOYMENT WAVES, not campaigns.")
        print("   Brief 3b asks for campaigns. Re-run [1b] and [6] for n_mnes, or read every")
        print("   breadth figure below as waves. The label follows the column automatically.")
else:
    print("!! CONTACT VOLUME / ENGAGEMENT UNAVAILABLE - senders_base has only", _sb.columns)
    print("   That landing predates cell [6]'s rewrite. Re-run [1b] and [6] to get them.")
    print("   Everything else runs; L10 and L11 will skip.")
    senders_all_raw = _sb.select("CLNT_NO").distinct()

# prior_unsubs: one row per client either way, so no aggregation - but the date and mnemonic only
# exist if [8] has been re-run since its rewrite.
prior_raw = _rd("prior_unsubs")
HAVE_PRIOR_DETAIL = "last_prior_unsub_dt" in prior_raw.columns
if not HAVE_PRIOR_DETAIL:
    print("!! ALREADY-OUT DETAIL UNAVAILABLE - prior_unsubs has only", prior_raw.columns)
    print("   Re-run [1b] and [8] for the unsub date and mnemonic. L12 will skip.")
    prior_raw = prior_raw.select("CLNT_NO").distinct()

# Cadence and the wide pairing are both OPTIONAL landings: everything that existed before them still
# runs without them, and the cells that need them skip loudly rather than fail.
HAVE_CADENCE = landed("cadence_by_mne")
cadence_mne = spark.read.parquet(BASE + "cadence_by_mne") if HAVE_CADENCE else None
if not HAVE_CADENCE:
    print("!! CADENCE UNAVAILABLE - run [5b]. Brief 1a (weekly/bi-weekly/monthly) will be blank.")

# EVERY month, not just the first. A pull that died after March would leave HAVE_WIDE True on a
# one-month pairing measured against a three-month leaver set - every pair rate understated, and
# nothing on screen to say so. Partial counts as absent.
_wide_months = [m[0] for m in WIN_MONTHS if landed("senders_wide/" + m[0])]
HAVE_WIDE = len(_wide_months) == len(WIN_MONTHS)
if _wide_months and not HAVE_WIDE:
    print("!! senders_wide is PARTIAL -", _wide_months, "of", [m[0] for m in WIN_MONTHS], "landed.")
    print("   Treated as ABSENT. Re-run [7b] to finish it; land() will skip the months already in.")
if HAVE_WIDE:
    # DISTINCT after the union, not before: the pull is monthly, so a client mailed by PCQ in March
    # and April lands twice. This is the one place campaign breadth can be counted exactly.
    senders_wide_raw = _rd("senders_wide/*").select("CLNT_NO", "mne").distinct()
else:
    senders_wide_raw = None
    print("!! senders_wide NOT LANDED - run [7b]. Bank-wide 3b/3c unavailable; L13 falls back to")
    print("   CARDS-ONLY co-occurrence off senders_cards, which cannot compare cards to the bank.")

# 9 actions, four of them distinct() shuffles over 10M rows, to re-confirm counts already written to
# RESULTS_CATALOG.md. landed() already proved every path is readable and non-empty at pull time.
if not PROVEN:
    _counts = [
        {"dataset": "unsubs_3m (client x treatment)", "rows": unsubs_raw.count(),
         "distinct_clients": unsubs_raw.select("CLNT_NO").distinct().count()},
        {"dataset": "senders_base (distinct clients mailed)", "rows": senders_all_raw.count(),
         "distinct_clients": senders_all_raw.count()},
        {"dataset": "senders_cards (client x cards MNE)", "rows": senders_cards_raw.count(),
         "distinct_clients": senders_cards_raw.select("CLNT_NO").distinct().count()},
        {"dataset": "prior_unsubs (clients out before window)", "rows": prior_raw.count(),
         "distinct_clients": prior_raw.count()},
        {"dataset": "senders_by_mne (aggregate)", "rows": senders_mne.count(),
         "distinct_clients": None},
    ]
    T("READBACK - every landing, rows and distinct clients", pd.DataFrame(_counts))
    for _c in _counts:
        assert _c["rows"] > 0, _c["dataset"] + " read back empty - investigate before proceeding"
else:
    print("READBACK skipped (PROVEN = True). Counts are in RESULTS_CATALOG.md.")

# %% [10] MNE / PROGRAM - one derivation, used on BOTH sides of every join. The senders aggregate
# and the leaver frame previously derived mne differently (raw SUBSTR vs a DEFAULT-aware rule),
# which mis-joins the blank/DEFAULT stream. Same rule for everyone now.
def add_mne_program(df, tid_col=None, mne_col=None):
    if mne_col is None:
        df = df.withColumn("mne_raw", F.trim(F.substring(F.col(tid_col), 8, 3)))
    else:
        df = df.withColumn("mne_raw", F.trim(F.col(mne_col)))
    df = df.withColumn("mne", F.when((F.col("mne_raw") == "") | F.col("mne_raw").isNull(),
                                     F.lit("DEFAULT")).otherwise(F.col("mne_raw")))
    return df.withColumn(
        "program",
        F.when(F.col("mne") == "DEFAULT", F.lit("DEFAULT"))
         .when(F.col("mne").isin(*sorted(CARDS_MNES)), F.lit("CARDS"))
         .otherwise(F.lit("NON_CARDS"))).drop("mne_raw")


senders_mne = add_mne_program(senders_mne, mne_col="mne")
senders_cards_raw = add_mne_program(senders_cards_raw, mne_col="mne")

# %% [11] UCP SCHEMA PROBE - hard gate. Nothing downstream assumes a column name.
_REQUESTED = ["CLNT_NO", "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT",
              "C_TOT_CNT", "PROF_TOT_ANNUAL"]
_ucp_probe = spark.read.option("basePath", UCP_BASE).parquet(UCP_BASE + "MONTH_END_DATE=" + UCP_ANCHOR)
_actual = set(_ucp_probe.columns)
T("UCP SCHEMA PROBE - requested columns vs the live schema at " + UCP_ANCHOR,
  pd.DataFrame([{"column": c, "status": "PRESENT" if c in _actual else "MISSING"} for c in _REQUESTED]))
UCP_COLS = [c for c in _REQUESTED if c in _actual]
HAS_CLNT_TYP = "CLNT_TYP" in _actual
_missing = [c for c in _REQUESTED if c not in _actual]
assert not _missing, ("UCP is missing required columns " + str(_missing) + " at " + UCP_ANCHOR +
                      " - the four measured attributes cannot be built without them.")
print("UCP_COLS fixed:", UCP_COLS, "| HAS_CLNT_TYP =", HAS_CLNT_TYP)

# %% [12] UCP READ - ONE partition, one join. Written to HDFS rather than cached: writing severs the
# lineage, so a session death costs this step alone and never re-runs the scan.
_ucp = _ucp_probe
if HAS_CLNT_TYP:
    _ucp = _ucp.filter(F.trim(F.col("CLNT_TYP")) == "Personal")
_ucp = _ucp.select(*UCP_COLS).withColumn("CLNT_NO", norm_clnt(F.col("CLNT_NO")))

# UCP is EXPECTED to be one row per client per month-end but is not guaranteed to be: the
# 2026-02-28 partition carries 1 duplicate in 15.3M (2026-07-27). Dedupe deterministically and
# REPORT it - halting the run over a stray row would be absurd. Hard-fail only if duplication is
# material (>0.1%), which would mean the grain is not what we think it is.
# The row_number() window over 15.3M rows is a full sort shuffle - the single most expensive step in
# this file. It used to run TWICE: once for the post-dedupe .count() and again for the write. Order
# matters here: write first, then count off the written parquet, which is a metadata read.
_dedup_w = Window.partitionBy("CLNT_NO").orderBy(
    *[F.col(c).asc_nulls_last() for c in UCP_COLS if c != "CLNT_NO"])
_ucp_raw_for_count = _ucp
ucp = stage("ucp_spine",
            lambda: (_ucp.withColumn("_rn", F.row_number().over(_dedup_w))
                     .filter(F.col("_rn") == 1).drop("_rn")))
_n_ucp = ucp.count()

# The duplicate report costs one extra scan of the raw partition (no window). Skipped on a reuse -
# the dedupe already happened and its verdict is in this file's git history, not re-derivable value.
if "ucp_spine" in STAGE_REBUILD or not landed("ucp_spine"):
    _n_ucp_raw = _ucp_raw_for_count.count()
    _dupes = _n_ucp_raw - _n_ucp
    _dupe_pct = 100.0 * _dupes / _n_ucp_raw if _n_ucp_raw else 0.0
    print("UCP at", UCP_ANCHOR, ":", _n_ucp_raw, "rows ->", _n_ucp, "after dedupe on CLNT_NO (",
          _dupes, "duplicate rows dropped, %.4f%% )" % _dupe_pct)
    assert _dupe_pct < 0.1, ("UCP duplication at " + UCP_ANCHOR + " is %.3f%%" % _dupe_pct +
                             " - that is structural, not stray rows. The grain is not one row per "
                             "client per month-end; investigate before trusting any attribute.")

# No uniqueness assert here on purpose: row_number() == 1 makes one row per CLNT_NO by construction,
# and verifying it would cost a second 15.3M-row groupBy shuffle to re-prove what the window did.
# The claim is still checked - the UCP join in [14] asserts the row count does not fan out, which is
# the same proof paid for by a join we were doing anyway.
print("ucp_spine:", _n_ucp, "rows at", UCP_ANCHOR, "- one row per client by construction.")

# %% [13] POPULATIONS - one row per client, three mutually exclusive buckets.
_w = Window.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").desc(), F.col("TREATMENT_ID").desc())
leaver_last = (add_mne_program(unsubs_raw, tid_col="TREATMENT_ID")
               .withColumn("_rn", F.row_number().over(_w)).filter(F.col("_rn") == 1).drop("_rn")
               .select("CLNT_NO", "TREATMENT_ID", "mne", "program", "unsub_tm"))

# senders_all_raw now carries the contact and engagement columns, so `mailed` carries them too and
# they ride into every table below on the client row - no second join, no second scan.
mailed = senders_all_raw
clients = (mailed
           .join(leaver_last, "CLNT_NO", "left")
           .join(prior_raw.withColumn("_prior", F.lit(1)), "CLNT_NO", "left")
           .withColumn("bucket",
                       F.when(F.col("unsub_tm").isNotNull(), F.lit("leaver"))
                        .when(F.col("_prior") == 1, F.lit("already_out"))
                        .otherwise(F.lit("stayer")))
           .drop("_prior"))

if HAVE_CONTACT:
    # Bands, not raw counts: L7's ratios have to be re-cut INSIDE a band, and a band needs enough
    # clients to hold the comparison. Cut points are OURS - edit here and nowhere else.
    clients = (clients
               .withColumn("contact_band",
                           F.when(F.col("n_send_events") <= 1, "1")
                            .when(F.col("n_send_events") <= 3, "2-3")
                            .when(F.col("n_send_events") <= 6, "4-6")
                            .when(F.col("n_send_events") <= 12, "7-12").otherwise("13+"))
               .withColumn("engagement",
                           F.when(F.col("clicked") == 1, "clicked")
                            .when(F.col("opened") == 1, "opened_not_clicked")
                            .otherwise("never_opened")))
    if BREADTH_COL:
        # Brief 3b: "unsubs by number of campaigns client contacted by". Banded on the LOWER bound,
        # deliberately - the upper bound inflates by up to 3x for a client active all three months,
        # and a band built on an inflated number silently promotes clients into the wrong bucket.
        # The lower bound understates breadth but never mis-assigns direction.
        clients = clients.withColumn(
            "breadth_band",
            F.when(F.col("breadth_lower") <= 1, "1")
             .when(F.col("breadth_lower") <= 2, "2")
             .when(F.col("breadth_lower") <= 4, "3-4")
             .when(F.col("breadth_lower") <= 8, "5-8").otherwise("9+"))

if HAVE_PRIOR_DETAIL:
    clients = clients.withColumn(
        "days_since_prior_unsub",
        F.datediff(F.to_date(F.lit(WIN_START)), F.to_date(F.col("last_prior_unsub_dt"))))

# THE staleness contract. What the pull landed decides what every staged frame must carry, and
# stage() rebuilds anything that does not. This list - not a diff between two staged frames - is
# what makes the check work, because the pull is the only thing that knows what is actually there.
STAGE_NEEDS = ["CLNT_NO", "bucket", "mne", "program"]
if HAVE_CONTACT:
    STAGE_NEEDS += ["n_send_events", "n_opens", "n_clicks", "opened", "clicked",
                    "contact_band", "engagement"]
    if BREADTH_COL:
        STAGE_NEEDS += ["breadth_upper", "breadth_lower", "breadth_band"]
if HAVE_PRIOR_DETAIL:
    STAGE_NEEDS += ["last_prior_unsub_dt", "last_prior_unsub_mne", "days_since_prior_unsub"]
print("stages must carry:", STAGE_NEEDS)

# mne/program come from the client's OWN unsubscribe event, so only leavers have one. Left as NULL
# they render as a single blank row in a pivot, which reads as missing data. It is not missing - it
# is not applicable, and the distinction decides whether a pivot on program is meaningful:
# in the MAIN cube, program slices the 62,658 leavers ONLY. Every stayer and already_out client
# collapses into NO_UNSUB_EVENT. Per-campaign leaver-vs-stayer needs cards_cube, not this one.
clients = (clients
           .withColumn("mne", F.coalesce(F.col("mne"), F.lit("NO_UNSUB_EVENT")))
           .withColumn("program", F.coalesce(F.col("program"), F.lit("NO_UNSUB_EVENT"))))

# %% [14] JOIN UCP. Clients ACQUIRED during Mar-May cannot exist in the 2026-02-28 snapshot and will
# be unmatched - that is a real limit of a single pre-window anchor, counted in M0, never hidden.
#
# The bucket table used to be printed HERE, off `clients` before it was staged. That made the cheapest
# table in the file one of the most expensive: a groupBy on an unmaterialised plan re-ran the distinct
# over 10.6M senders and both left joins. It is printed below instead, off the staged parquet.
_clients_pre = clients
clients = stage("clients_ucp",
                lambda: _clients_pre.join(ucp, "CLNT_NO", "left").withColumn(
                    "ucp_matched",
                    F.when(F.col("AGE").isNotNull(), F.lit(True)).otherwise(F.lit(False))),
                requires=STAGE_NEEDS + ["ucp_matched", "AGE", "PROF_TOT_ANNUAL"])

# BUCKETS - one groupBy over a materialised file. The percentage denominator is the sum of the
# buckets themselves, which is the mailed population by construction, so no second count is needed.
_bucket_counts = clients.groupBy("bucket").count().toPandas()
_n_mailed = int(_bucket_counts["count"].sum())
_bucket_counts["pct_of_mailed"] = (100.0 * _bucket_counts["count"] / _n_mailed).round(2)
T("BUCKETS - every client mailed in " + WIN_START + ".." + WIN_END + " (leaver / already_out / stayer)",
  _bucket_counts)
print("already_out = mailed despite an unsub before the window (the leak). Kept OUT of the stayer",
      "pool: they were not reachable, so counting them as having 'chosen to stay' would be false.")

# The fan-out check costs a full recompute of the PRE-stage plan, so it cannot be paid for by the
# stage. It proves UCP is unique per client at this anchor - a property of the UCP partition, not of
# this window. Run it when the anchor changes; skip it when it has already held.
if not PROVEN:
    _before = _clients_pre.count()
    assert _n_mailed == _before, (
        "UCP join fanned out: " + str(_before) + " -> " + str(_n_mailed) + " - UCP is not unique "
        "per client at " + UCP_ANCHOR + ". Set STAGE_REBUILD = {'ucp_spine'} and investigate.")
    print("fan-out check passed:", _before, "clients in, ", _n_mailed, "out.")
else:
    print("fan-out check skipped (PROVEN = True). UCP uniqueness at", UCP_ANCHOR, "already held.")

# %% [15] M0 - COVERAGE & NULLS. Prints BEFORE any finding.
#
# M0c used to loop over UCP_COLS calling .count() per column: nine separate full passes over 10.6M
# rows to answer one question. It is one .agg() with nine sums - a single pass - whether or not the
# rest of M0 is gated. Coverage and null rates are properties of the UCP ANCHOR, not of the analysis,
# so once an anchor has passed they do not change until the anchor does.
if not PROVEN:
    m0a = (clients.groupBy("bucket")
           .agg(F.count("*").alias("clients"),
                F.sum(F.col("ucp_matched").cast("int")).alias("matched"))
           .withColumn("unmatched", F.col("clients") - F.col("matched"))
           .withColumn("match_pct", F.round(100.0 * F.col("matched") / F.col("clients"), 1)))
    T("M0a - UCP match rate by bucket | anchor " + UCP_ANCHOR, m0a)

    T("M0b - unmatched clients by MNE (leavers only - the only bucket carrying an MNE). SBB and "
      "other business-banking programs matched 0% on 2026-07-27: personal UCP cannot contain "
      "business clients",
      (clients.filter((~F.col("ucp_matched")) & (F.col("bucket") == "leaver"))
       .groupBy("mne", "program").count().orderBy(F.desc("count")).limit(15)))

    _m0c = clients.agg(
        F.sum(F.col("ucp_matched").cast("int")).alias("_matched"),
        *[F.sum(F.when(F.col("ucp_matched") & F.col(_c).isNull(), 1).otherwise(0)).alias(_c)
          for _c in UCP_COLS]).collect()[0]
    _n_matched = _m0c["_matched"]
    _nulls_pd = pd.DataFrame([
        {"field": _c, "null_among_matched": _m0c[_c],
         "pct_null_among_matched": round(100.0 * _m0c[_c] / _n_matched, 2) if _n_matched else 0.0}
        for _c in UCP_COLS])
    T("M0c - nulls among MATCHED clients, per UCP field (n matched = " + str(_n_matched) + ")",
      _nulls_pd)
    _worst = _nulls_pd["pct_null_among_matched"].max()
    print(("VERDICT: worst field is %.2f%% null among matched - " % _worst) +
          ("acceptable, medians below are trustworthy." if _worst < 5
           else "OVER 5% - every median downstream using that field is suspect."))
else:
    print("M0 coverage/nulls skipped (PROVEN = True). Anchor", UCP_ANCHOR,
          "coverage is in RESULTS_CATALOG.md: 90.1% matched, worst field 0.01% null.")

# %% [16] BANDING - one function, applied identically to every population. Cut points are OUR choice;
# none are documented in the repo. Edit them here and nowhere else.
HP_AGE, HP_TENURE, HP_PRODS = 35, 5, 2          # high_potential thresholds

# BAND_VERSION - the staleness contract for band CONTENTS, not band columns.
#
# stage() decides a frame is fresh by checking it carries the columns `requires` names. That works
# for a new column and fails silently for a redefined one: splitting prod_band's "3-4" into "3" and
# "4" changes every value in the column while leaving its name alone, so a rerun would happily reuse
# the old banding. (That failure mode already cost a run once - 26b0750, "the staleness guard
# compared stale against stale".)
#
# Fix: apply_bands stamps a column NAMED after this number, and it is in `requires`. Bump it and the
# name stops matching, so stage() sees a missing column and rebuilds banded -> cards_send by itself.
# BUMP THIS WHENEVER A CUT POINT, A BAND BOUNDARY OR AN HP THRESHOLD CHANGES.
#   v1 -> v2  contact/engagement bands added
#   v2 -> v3  2026-07-29: prod_band "3-4" split into "3" and "4" for brief item 4
BAND_VERSION = 3
BAND_STAMP = "band_v" + str(BAND_VERSION)
_prof_src = clients.filter(F.col("ucp_matched") & F.col("PROF_TOT_ANNUAL").isNotNull())
PROF_CUTS = _prof_src.approxQuantile("PROF_TOT_ANNUAL", [0.2, 0.4, 0.6, 0.8], 0.01)
assert len(PROF_CUTS) == 4, ("no non-null PROF_TOT_ANNUAL among matched clients - cannot cut "
                             "quintiles; check M0c before going further")
T("PROF QUINTILE CUT POINTS - computed over ALL MAILED matched clients (the reachable base), so "
  "'top quintile' means valuable relative to who we can actually reach",
  pd.DataFrame([{"p20": PROF_CUTS[0], "p40": PROF_CUTS[1], "p60": PROF_CUTS[2], "p80": PROF_CUTS[3]}]))


def apply_bands(df):
    df = df.withColumn("prod_cnt", sum(F.coalesce(F.col(c), F.lit(0))
                                       for c in ("T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT")))
    # 2026-07-29 - "3-4" SPLIT into "3" and "4". Brief item 4 asks to "bucket by number of products
    # 1 through 4", and a merged 3-4 bucket cannot answer it. 0 and 5+ stay: the population has them,
    # and collapsing a band would file clients under a count they do not hold.
    #
    # !! THIS CHANGES A STAGED FRAME'S CONTENTS WITHOUT CHANGING ITS COLUMNS. stage() checks that a
    # frame carries the columns it needs, which it still does - so a rerun would silently reuse the
    # OLD 3-4 banding and the split would never appear. Rebuild is forced below, not left to memory.
    df = df.withColumn("prod_band",
                       F.when(F.col("prod_cnt") <= 0, "0").when(F.col("prod_cnt") == 1, "1")
                        .when(F.col("prod_cnt") == 2, "2").when(F.col("prod_cnt") == 3, "3")
                        .when(F.col("prod_cnt") == 4, "4")
                        .otherwise("5+"))
    _held = [(F.coalesce(F.col(c), F.lit(0)) > 0).cast("int") for c in
             ("T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT")]
    df = df.withColumn("_n_cat", _held[0] + _held[1] + _held[2] + _held[3])
    df = df.withColumn("tibc_mix",
                       F.when(F.col("_n_cat") > 1, "multi")
                        .when(F.coalesce(F.col("T_TOT_CNT"), F.lit(0)) > 0, "T only")
                        .when(F.coalesce(F.col("I_TOT_CNT"), F.lit(0)) > 0, "I only")
                        .when(F.coalesce(F.col("B_TOT_CNT"), F.lit(0)) > 0, "B only")
                        .when(F.coalesce(F.col("C_TOT_CNT"), F.lit(0)) > 0, "C only")
                        .otherwise("none")).drop("_n_cat")
    df = df.withColumn("tenure_band",
                       F.when(F.col("TENURE_RBC_YEARS").isNull(), "unknown")
                        .when(F.col("TENURE_RBC_YEARS") <= 2, "0-2")
                        .when(F.col("TENURE_RBC_YEARS") <= 5, "3-5")
                        .when(F.col("TENURE_RBC_YEARS") <= 10, "6-10")
                        .when(F.col("TENURE_RBC_YEARS") <= 20, "11-20").otherwise("20+"))
    df = df.withColumn("age_band",
                       F.when(F.col("AGE").isNull(), "unknown")
                        .when(F.col("AGE") < 25, "<25").when(F.col("AGE") < 35, "25-34")
                        .when(F.col("AGE") < 45, "35-44").when(F.col("AGE") < 55, "45-54")
                        .when(F.col("AGE") < 65, "55-64").otherwise("65+"))
    df = df.withColumn("prof_quintile",
                       F.when(F.col("PROF_TOT_ANNUAL").isNull(), "unknown")
                        .when(F.col("PROF_TOT_ANNUAL") <= PROF_CUTS[0], "1")
                        .when(F.col("PROF_TOT_ANNUAL") <= PROF_CUTS[1], "2")
                        .when(F.col("PROF_TOT_ANNUAL") <= PROF_CUTS[2], "3")
                        .when(F.col("PROF_TOT_ANNUAL") <= PROF_CUTS[3], "4").otherwise("5"))
    df = df.withColumn("high_potential",
                       (F.col("AGE") < HP_AGE) & (F.col("TENURE_RBC_YEARS") <= HP_TENURE) &
                       (F.col("prod_cnt") <= HP_PRODS))
    return df.withColumn(BAND_STAMP, F.lit(BAND_VERSION))


banded = stage("banded", lambda: apply_bands(clients),
               requires=STAGE_NEEDS + ["age_band", "tenure_band", "prod_band", "prof_quintile",
                                       BAND_STAMP])
matched = banded.filter(F.col("ucp_matched")).cache()

# Two counts, one pass. _n_matched is defined here and not inside the M0 gate because the summary at
# the end reports it, and a figure the summary prints must not depend on whether proofs ran.
_bstats = banded.agg(F.count("*").alias("all"),
                     F.sum(F.col("ucp_matched").cast("int")).alias("matched")).collect()[0]
_n_banded, _n_matched = _bstats["all"], _bstats["matched"]
print("banded:", _n_banded, "clients |", _n_matched, "matched (all band stats below use "
      "the MATCHED denominator - unmatched clients have no bands and must not dilute a percentage)")

# %% [17] L1 - WHERE, by program and by MNE (leavers only). Every median and percentage uses the
# MATCHED denominator; matched_clients is a visible column so the reader can see it.
def _leaver_profile(df, keys):
    return (df.filter(F.col("bucket") == "leaver")
            .groupBy(*keys)
            .agg(F.count("*").alias("leaver_clients"),
                 F.sum(F.col("ucp_matched").cast("int")).alias("matched_clients"),
                 F.expr("percentile_approx(AGE, 0.5)").alias("median_age"),
                 F.expr("percentile_approx(TENURE_RBC_YEARS, 0.5)").alias("median_tenure_years"),
                 F.expr("percentile_approx(prod_cnt, 0.5)").alias("median_prod_cnt"),
                 F.expr("percentile_approx(PROF_TOT_ANNUAL, 0.5)").alias("median_prof_annual"),
                 F.round(100.0 * F.sum(F.when(F.col("ucp_matched") & (F.col("prod_band") == "1"), 1)
                                        .otherwise(0)) /
                         F.sum(F.col("ucp_matched").cast("int")), 1).alias("pct_single_product"),
                 F.round(100.0 * F.sum(F.when(F.col("ucp_matched") & (F.col("prof_quintile") == "5"), 1)
                                        .otherwise(0)) /
                         F.sum(F.col("ucp_matched").cast("int")), 1).alias("pct_top_prof_quintile"),
                 F.round(100.0 * F.sum(F.when(F.col("ucp_matched") & F.col("high_potential"), 1)
                                        .otherwise(0)) /
                         F.sum(F.col("ucp_matched").cast("int")), 1).alias("pct_high_potential")))


T("L1 - WHERE, by program | leavers only | medians and percentages on MATCHED clients",
  _leaver_profile(banded, ["program"]).orderBy(F.desc("leaver_clients")))
T("L1 - WHERE, top 20 MNEs by leaver volume | leavers only | matched denominators",
  _leaver_profile(banded, ["mne", "program"]).orderBy(F.desc("leaver_clients")).limit(20))

# %% [18] L6 - CAMPAIGN SUMMARY. senders (T1, bank-wide) joined to leavers: THE rate in this file.
_leavers_by_mne = (banded.filter(F.col("bucket") == "leaver").groupBy("mne", "program")
                   .agg(F.count("*").alias("unsubs")))
_prof_by_mne = _leaver_profile(banded, ["mne", "program"]).drop("leaver_clients")

l6 = (senders_mne.select("mne", "program", "senders")
      .join(_leavers_by_mne.drop("program"), "mne", "outer")
      .join(_prof_by_mne.drop("program"), "mne", "left")
      .withColumn("unsubs", F.coalesce(F.col("unsubs"), F.lit(0)))
      .withColumn("unsub_per_1000",
                  F.when(F.col("senders") > 0, F.round(1000.0 * F.col("unsubs") / F.col("senders"), 2)))
      .orderBy(F.desc("senders")))

if not PROVEN:
    _orphan_send = senders_mne.join(_leavers_by_mne, "mne", "left_anti").count()
    _orphan_leave = _leavers_by_mne.join(senders_mne, "mne", "left_anti").count()
    print("join check - MNEs with senders but no unsubs:", _orphan_send,
          "| MNEs with unsubs but no senders:", _orphan_leave,
          "(the second should be ~0; anything else means the two sides label MNE differently)")

T("L6 - CAMPAIGN SUMMARY | senders = distinct clients mailed " + WIN_START + ".." + WIN_END +
  " | profile columns describe that campaign's LEAVERS (matched only) | top 25 by senders, the CSV "
  "has every MNE | rows OVERLAP: a client mailed by several campaigns appears in several rows, so "
  "columns do not sum",
  l6.limit(25))

# %% [19] L7 - LEAVERS vs STAYERS, bank-wide. The comparison this file exists for. Both sides
# matched-only, so the denominators are equal in kind.
# tibc_mix ADDED 2026-07-29. It has been computed since the first version of this file and never
# reported - it was in the cube and in no table anyone read. Brief item 4a asks for exactly it:
# "what is the depth of their relationship with RBC (TIBC)". prod_band counts products; tibc_mix
# says which CATEGORIES are held, which is the question depth actually means.
# breadth_band rides along when the pull carried it - brief item 3b.
_DIMS = ["age_band", "tenure_band", "prod_band", "tibc_mix", "prof_quintile"]
if "breadth_band" in matched.columns:
    _DIMS.append("breadth_band")
_bk = {r["bucket"]: r["n"] for r in
       matched.groupBy("bucket").agg(F.count("*").alias("n")).collect()}
_n_lv, _n_st = _bk.get("leaver", 0), _bk.get("stayer", 0)

_frames = []
for _d in _DIMS + ["high_potential"]:
    _lv = (matched.filter(F.col("bucket") == "leaver").groupBy(F.col(_d).cast("string").alias("segment_value"))
           .agg(F.count("*").alias("leavers_n")))
    _st = (matched.filter(F.col("bucket") == "stayer").groupBy(F.col(_d).cast("string").alias("segment_value"))
           .agg(F.count("*").alias("stayers_n")))
    _frames.append(_lv.join(_st, "segment_value", "outer")
                   .withColumn("segment_dim", F.lit(_d))
                   .withColumn("leavers_n", F.coalesce(F.col("leavers_n"), F.lit(0)))
                   .withColumn("stayers_n", F.coalesce(F.col("stayers_n"), F.lit(0))))
l7 = _frames[0]
for _f in _frames[1:]:
    l7 = l7.unionByName(_f)
l7 = (l7.withColumn("pct_leavers", F.round(100.0 * F.col("leavers_n") / _n_lv, 2))
      .withColumn("pct_stayers", F.round(100.0 * F.col("stayers_n") / _n_st, 2))
      .withColumn("ratio", F.round(F.col("pct_leavers") / F.col("pct_stayers"), 2))
      .select("segment_dim", "segment_value", "pct_leavers", "pct_stayers", "ratio",
              "leavers_n", "stayers_n")
      .orderBy("segment_dim", F.desc("ratio")))
T("L7 - LEAVERS vs STAYERS, bank-wide | matched clients only (leavers n = " + str(_n_lv) +
  ", stayers n = " + str(_n_st) + ") | ratio > 1 = over-represented among LEAVERS", l7)

# %% [20] L8 - CARDS, per campaign: leavers vs that campaign's OWN mailed base. This is what
# separates "this campaign LOSES young single-product clients" from "this campaign MAILS them".
# carries EVERY band L8/L9/cards_cube consume - a short select here surfaces 200 lines later as
# "Column 'age_band' does not exist", so the list is the union of what all three cells reference.
_CARDS_CARRY = ["CLNT_NO", "bucket", "ucp_matched", "AGE", "TENURE_RBC_YEARS", "prod_cnt",
                "PROF_TOT_ANNUAL", "age_band", "tenure_band", "prod_band", "tibc_mix",
                "prof_quintile", "high_potential", BAND_STAMP]
_missing = [c for c in _CARDS_CARRY if c not in banded.columns]
assert not _missing, "banded is missing " + str(_missing) + " - available: " + str(banded.columns)

cards_send = stage("cards_send",
                   lambda: (senders_cards_raw.join(banded.select(*_CARDS_CARRY), "CLNT_NO", "inner")
                            .filter(F.col("ucp_matched"))),
                   requires=_CARDS_CARRY + ["mne"])


def _side(df, label):
    return (df.groupBy("mne")
            .agg(F.count("*").alias(label + "_clients"),
                 F.expr("percentile_approx(AGE, 0.5)").alias(label + "_median_age"),
                 F.expr("percentile_approx(TENURE_RBC_YEARS, 0.5)").alias(label + "_median_tenure"),
                 F.expr("percentile_approx(prod_cnt, 0.5)").alias(label + "_median_prod"),
                 F.expr("percentile_approx(PROF_TOT_ANNUAL, 0.5)").alias(label + "_median_prof"),
                 F.round(100.0 * F.avg(F.when(F.col("prod_band") == "1", 1.0).otherwise(0.0)), 1)
                 .alias(label + "_pct_single_product"),
                 F.round(100.0 * F.avg(F.col("high_potential").cast("double")), 1)
                 .alias(label + "_pct_high_potential")))


l8 = (_side(cards_send, "mailed")
      .join(_side(cards_send.filter(F.col("bucket") == "leaver"), "leaver"), "mne", "left")
      .join(senders_mne.select("mne", "senders"), "mne", "left")
      .withColumn("unsub_per_1000",
                  F.when(F.col("senders") > 0,
                         F.round(1000.0 * F.col("leaver_clients") / F.col("senders"), 2)))
      .withColumn("delta_median_age", F.col("leaver_median_age") - F.col("mailed_median_age"))
      .withColumn("delta_median_tenure", F.col("leaver_median_tenure") - F.col("mailed_median_tenure"))
      .withColumn("delta_median_prod", F.col("leaver_median_prod") - F.col("mailed_median_prod"))
      .withColumn("delta_pct_single_product",
                  F.round(F.col("leaver_pct_single_product") - F.col("mailed_pct_single_product"), 1))
      .orderBy(F.desc("senders")))
T("L8 - CARDS campaigns: LEAVERS vs that campaign's OWN mailed base | matched clients only | a "
  "positive delta_pct_single_product means the campaign loses single-product clients at a higher "
  "rate than it mails them - the campaign's own contribution, not its targeting", l8)

# %% [20b] L9 - THE L7 VIEW, PER CARDS CAMPAIGN. The bank-wide ratio chart is the strongest thing
# in this analysis; this is the same comparison inside each cards campaign. It CANNOT come from the
# main cube: there, stayers carry no MNE (only leavers do), so per-campaign leaver-vs-stayer has to
# be built from cards_send, which is client x MNE with the bucket attached. Cards only - that is the
# tier that pulled client x MNE.
_CARD_DIMS = ["age_band", "tenure_band", "prod_band", "prof_quintile", "high_potential"]

_l9_parts = []
for _d in _CARD_DIMS:
    _lv = (cards_send.filter(F.col("bucket") == "leaver")
           .groupBy("mne", F.col(_d).cast("string").alias("segment_value"))
           .agg(F.count("*").alias("leavers_n")))
    _st = (cards_send.filter(F.col("bucket") == "stayer")
           .groupBy("mne", F.col(_d).cast("string").alias("segment_value"))
           .agg(F.count("*").alias("stayers_n")))
    _l9_parts.append(_lv.join(_st, ["mne", "segment_value"], "outer")
                     .withColumn("segment_dim", F.lit(_d)))
l9 = _l9_parts[0]
for _p in _l9_parts[1:]:
    l9 = l9.unionByName(_p)

# denominators are per campaign, so each MNE's percentages sum to 100 within a dimension
_tot = (cards_send.filter(F.col("bucket").isin("leaver", "stayer"))
        .groupBy("mne", "bucket").agg(F.count("*").alias("n"))
        .groupBy("mne")
        .agg(F.sum(F.when(F.col("bucket") == "leaver", F.col("n")).otherwise(0)).alias("mne_leavers"),
             F.sum(F.when(F.col("bucket") == "stayer", F.col("n")).otherwise(0)).alias("mne_stayers")))

l9 = (l9.join(_tot, "mne", "left")
      .withColumn("leavers_n", F.coalesce(F.col("leavers_n"), F.lit(0)))
      .withColumn("stayers_n", F.coalesce(F.col("stayers_n"), F.lit(0)))
      .withColumn("pct_leavers", F.round(100.0 * F.col("leavers_n") / F.col("mne_leavers"), 2))
      .withColumn("pct_stayers", F.round(100.0 * F.col("stayers_n") / F.col("mne_stayers"), 2))
      .withColumn("ratio", F.when(F.col("pct_stayers") > 0,
                                  F.round(F.col("pct_leavers") / F.col("pct_stayers"), 2)))
      .select("mne", "segment_dim", "segment_value", "pct_leavers", "pct_stayers", "ratio",
              "leavers_n", "stayers_n", "mne_leavers", "mne_stayers")
      .orderBy("mne", "segment_dim", F.desc("ratio")))

T("L9 - LEAVERS vs STAYERS, PER CARDS CAMPAIGN | same comparison as L7, inside each campaign | "
  "matched clients only | ratio > 1 = over-represented among that campaign's leavers | top 40 rows "
  "shown, the CSV has all of them", l9.filter(F.col("stayers_n") > 50).limit(40))

# %% [20d] L10 - CONTACT VOLUME. The confound under the headline.
#
# L7 says the young and the new leave more: age <25 at 1.33, tenure 0-2 at 1.25. That reading assumes
# the two groups were mailed the same way. If young and new clients simply receive more email, those
# ratios are a contact-intensity artifact wearing an age label.
#
# Two blocks, and the second only matters if the first says yes:
#   (a) were leavers mailed harder than stayers?
#   (b) if so, does the age/tenure signal SURVIVE inside a contact band?
# If the ratios flatten toward 1.00 once contact is held fixed, contact was the story. If they hold
# inside every band, contact is not the explanation and L7 stands on its own.
if "contact_band" not in matched.columns:
    print("L10 skipped - matched has no contact columns. Re-run [1b] and [6].")
else:
    l10a = (matched.groupBy("bucket").agg(
        F.count("*").alias("clients"),
        F.round(F.avg("n_send_events"), 2).alias("mean_sends"),
        F.expr("percentile_approx(n_send_events, 0.5)").alias("median_sends"),
        F.expr("percentile_approx(n_send_events, 0.9)").alias("p90_sends"),
        # LABEL FOLLOWS THE COLUMN. This used to read mean_distinct_campaigns off n_treatments,
        # which counts deployment WAVES - see [6]. The alias is now built from BREADTH_LABEL so it
        # cannot claim "campaigns" when the landing only supports "deployment waves".
        *([F.round(F.avg("breadth_lower"), 2).alias("mean_distinct_" + BREADTH_LABEL + "_min"),
           F.round(F.avg("breadth_upper"), 2).alias("mean_distinct_" + BREADTH_LABEL + "_max")]
          if BREADTH_COL else [])))
    T("L10a - CONTACT VOLUME by bucket | matched clients | if leavers sit well above stayers, every "
      "ratio in L7 is partly a contact effect", l10a)

    # the L7 ratios, re-cut inside each contact band
    _lv10 = (matched.filter(F.col("bucket") == "leaver").groupBy("contact_band")
             .agg(F.count("*").alias("_lv")))
    _st10 = (matched.filter(F.col("bucket") == "stayer").groupBy("contact_band")
             .agg(F.count("*").alias("_st")))
    _parts10 = []
    for _d in ["age_band", "tenure_band"]:
        _a = (matched.filter(F.col("bucket") == "leaver")
              .groupBy("contact_band", F.col(_d).alias("segment_value"))
              .agg(F.count("*").alias("leavers_n")))
        _b = (matched.filter(F.col("bucket") == "stayer")
              .groupBy("contact_band", F.col(_d).alias("segment_value"))
              .agg(F.count("*").alias("stayers_n")))
        _parts10.append(_a.join(_b, ["contact_band", "segment_value"], "outer")
                        .withColumn("segment_dim", F.lit(_d)))
    l10b = _parts10[0]
    for _p in _parts10[1:]:
        l10b = l10b.unionByName(_p)
    l10b = (l10b.join(_lv10, "contact_band", "left").join(_st10, "contact_band", "left")
            .withColumn("leavers_n", F.coalesce(F.col("leavers_n"), F.lit(0)))
            .withColumn("stayers_n", F.coalesce(F.col("stayers_n"), F.lit(0)))
            .withColumn("pct_leavers", F.round(100.0 * F.col("leavers_n") / F.col("_lv"), 2))
            .withColumn("pct_stayers", F.round(100.0 * F.col("stayers_n") / F.col("_st"), 2))
            .withColumn("ratio", F.when(F.col("pct_stayers") > 0,
                                        F.round(F.col("pct_leavers") / F.col("pct_stayers"), 2)))
            .select("contact_band", "segment_dim", "segment_value", "pct_leavers", "pct_stayers",
                    "ratio", "leavers_n", "stayers_n")
            .orderBy("segment_dim", "contact_band", F.desc("ratio")))
    T("L10b - THE L7 RATIOS, HELD AT CONSTANT CONTACT | compare a segment's ratio ACROSS bands. "
      "Flat across bands means contact is not the explanation; collapsing toward 1.00 means it is",
      l10b.filter(F.col("stayers_n") > 500))

# %% [20e] L11 - ENGAGEMENT. Losing the listeners, or keeping the deaf?
#
# An unsubscribe from someone who never opened an email is list hygiene. An unsubscribe from someone
# who opened and clicked is a client telling you something. They are not the same loss and they must
# not share a number. This splits the leaver population by what they did before leaving.
if "engagement" not in matched.columns:
    print("L11 skipped - matched has no engagement columns. Re-run [1b] and [6].")
else:
    l11 = (matched.groupBy("engagement", "bucket").agg(
        F.count("*").alias("clients"),
        F.expr("percentile_approx(AGE, 0.5)").alias("median_age"),
        F.expr("percentile_approx(TENURE_RBC_YEARS, 0.5)").alias("median_tenure"),
        F.expr("percentile_approx(prod_cnt, 0.5)").alias("median_prod"),
        F.expr("percentile_approx(PROF_TOT_ANNUAL, 0.5)").alias("median_prof"),
        F.round(100.0 * F.avg(F.when(F.col("prof_quintile") == "5", 1.0).otherwise(0.0)), 1)
        .alias("pct_top_prof_quintile")))
    _tot11 = matched.groupBy("bucket").agg(F.count("*").alias("_n"))
    l11 = (l11.join(_tot11, "bucket", "left")
           .withColumn("pct_of_bucket", F.round(100.0 * F.col("clients") / F.col("_n"), 2))
           .drop("_n").orderBy("bucket", "engagement"))
    T("L11 - ENGAGEMENT before leaving | compare the LEAVER rows against the STAYER rows at the same "
      "engagement level, and compare 'clicked' leavers against 'never_opened' leavers - if the "
      "engaged leaver is worth more, that is the expensive segment", l11)

    _eng_ratio = (l11.filter(F.col("bucket").isin("leaver", "stayer"))
                  .groupBy("engagement")
                  .pivot("bucket", ["leaver", "stayer"]).agg(F.first("pct_of_bucket"))
                  .withColumn("ratio", F.when(F.col("stayer") > 0,
                                              F.round(F.col("leaver") / F.col("stayer"), 2)))
                  .orderBy(F.desc("ratio")))
    T("L11b - engagement mix, leavers vs stayers | ratio > 1 = over-represented among LEAVERS",
      _eng_ratio)

# %% [20f] L12 - THE ALREADY-OUT POPULATION. 427,079 clients, 6.8x the leaver count, never analysed.
#
# Mailed inside the window despite having unsubscribed BEFORE it. Three questions the earlier version
# could not answer because prior_unsubs was one bare column: who are they, which campaigns mailed
# them, and how long after their unsubscribe.
_ao_profile = (matched.filter(F.col("bucket") == "already_out").groupBy("age_band")
               .agg(F.count("*").alias("clients")).orderBy("age_band"))
T("L12a - already_out by age | these clients are in the mailed population but were never reachable; "
  "they are excluded from the stayer pool on purpose", _ao_profile)

if "days_since_prior_unsub" not in matched.columns:
    print("L12b/c skipped - matched has no prior-unsub date. Re-run [1b] and [8].")
else:
    l12b = (matched.filter(F.col("bucket") == "already_out")
            .withColumn("lag_band",
                        F.when(F.col("days_since_prior_unsub") <= 30, "0-30 days")
                         .when(F.col("days_since_prior_unsub") <= 90, "31-90 days")
                         .when(F.col("days_since_prior_unsub") <= 180, "91-180 days")
                         .when(F.col("days_since_prior_unsub") <= 365, "181-365 days")
                         .otherwise("over a year"))
            .groupBy("lag_band").agg(
                F.count("*").alias("clients"),
                F.expr("percentile_approx(days_since_prior_unsub, 0.5)").alias("median_days"),
                F.round(F.avg("n_send_events"), 2).alias("mean_sends_after_unsub")
                if "n_send_events" in matched.columns
                else F.lit(None).cast("double").alias("mean_sends_after_unsub"))
            .orderBy("median_days"))
    T("L12b - HOW LONG after the unsubscribe were they mailed | a suppression list that works has "
      "almost nobody past 30 days; a latency problem and a consent gap look different here", l12b)

    l12c = (matched.filter(F.col("bucket") == "already_out")
            .groupBy(F.col("last_prior_unsub_mne").alias("left_via_mne"))
            .agg(F.count("*").alias("clients_mailed_after_unsub"),
                 F.expr("percentile_approx(days_since_prior_unsub, 0.5)").alias("median_days_after"))
            .orderBy(F.desc("clients_mailed_after_unsub")))
    T("L12c - WHICH campaign they left through, before being mailed again | this names where the "
      "suppression is not holding", l12c.limit(25))

# %% [20g] L13 - CAMPAIGN CO-OCCURRENCE. Brief item 3c, verbatim: "once we've determined which
# campaigns drive the highest unsubs, are certain campaigns causing unsubscribes alone, or do
# unsubscribes spike when specific campaigns run together."
#
# The test is a comparison, not a ranking. For every pair (A,B):
#   solo A  = clients mailed by A          -> unsub rate
#   solo B  = clients mailed by B          -> unsub rate
#   pair AB = clients mailed by BOTH       -> unsub rate
# If pair_rate is materially above BOTH solo rates, the combination is doing something neither
# campaign does alone. If it sits between them, it is composition - the pair population is just the
# heavier-mailed clients, and the pair is not the cause.
#
# SOURCE, and it decides what the answer can say:
#   senders_wide  - bank-wide, ~200 mnemonics, leavers whole + 1-in-10 stayers. Cards vs the bank.
#   senders_cards - fallback, 7 cards mnemonics. Cards vs cards only. Cannot answer "how does cards
#                   compare with other mnemonics in the bank", which is the point of the question.
#
# WEIGHTING. Leavers are a census (weight 1); non-leavers arrive through MOD(CLNT_NO,SAMPLE_MOD) and
# are weighted back up by SAMPLE_MOD. Weighting the leaver side too would inflate the numerator and
# hand back the sampling rate as a finding. Rates below use weighted denominators and RAW leaver
# counts, so a rate is (real leavers) / (estimated mailed) - the only combination that is not a
# sample artifact. Raw counts are printed alongside so nothing is hidden behind a weight.
_WIDE_SRC = senders_wide_raw if HAVE_WIDE else (
    senders_cards_raw.select("CLNT_NO", "mne") if senders_cards_raw is not None else None)
_WIDE_BASIS = "BANK_WIDE_SAMPLED" if HAVE_WIDE else "CARDS_ONLY_CENSUS"
_WIDE_W = SAMPLE_MOD if HAVE_WIDE else 1

if _WIDE_SRC is None:
    print("L13 skipped - neither senders_wide nor senders_cards is available.")
else:
    print("L13 basis:", _WIDE_BASIS, "| stayer weight =", _WIDE_W)
    _pairbase = (_WIDE_SRC.join(banded.select("CLNT_NO", "bucket"), "CLNT_NO", "inner")
                 .withColumn("w", F.when(F.col("bucket") == "leaver", F.lit(1))
                             .otherwise(F.lit(_WIDE_W)))
                 .withColumn("is_lv", (F.col("bucket") == "leaver").cast("int")))

    # EXPLOSION GUARD. A self-join on client turns n campaigns into n*(n-1)/2 pairs: 9 campaigns is
    # 36 rows, 50 campaigns is 1,225. A handful of very broadly-mailed clients would dominate the
    # shuffle. Capped - and the cap is REPORTED, because a silent cap reads as full coverage.
    MAX_MNE_PER_CLIENT = 25
    _cnt = _pairbase.groupBy("CLNT_NO").agg(F.count("*").alias("n_mne"))
    _over = _cnt.filter(F.col("n_mne") > MAX_MNE_PER_CLIENT)
    _n_over = _over.count()
    if _n_over:
        _over_lv = (_over.join(_pairbase.filter(F.col("is_lv") == 1).select("CLNT_NO").distinct(),
                               "CLNT_NO", "inner").count())
        print("!! PAIR CAP: " + str(_n_over) + " clients hold more than " + str(MAX_MNE_PER_CLIENT) +
              " campaigns and are EXCLUDED from pair rows (" + str(_over_lv) + " of them leavers).")
        print("   They remain in the solo rows. Raise MAX_MNE_PER_CLIENT to include them.")
    _keep = _cnt.filter(F.col("n_mne") <= MAX_MNE_PER_CLIENT).select("CLNT_NO")

    # SOLO - one row per campaign, over the same population the pairs are drawn from, so a pair rate
    # and a solo rate are comparable by construction. Using L6's senders here instead would compare
    # a sampled numerator against a census denominator.
    _solo = (_pairbase.groupBy("mne").agg(
        F.sum("w").alias("clients_est"),
        F.count("*").alias("clients_raw"),
        F.sum("is_lv").alias("leavers_raw"))
        .withColumn("mne_b", F.lit(""))
        .withColumnRenamed("mne", "mne_a"))

    _p = _pairbase.join(_keep, "CLNT_NO", "inner")
    _pa = _p.select("CLNT_NO", F.col("mne").alias("mne_a"), "w", "is_lv")
    _pb = _p.select("CLNT_NO", F.col("mne").alias("mne_b"))
    _pairs = (_pa.join(_pb, "CLNT_NO", "inner").filter(F.col("mne_a") < F.col("mne_b"))
              .groupBy("mne_a", "mne_b").agg(
                  F.sum("w").alias("clients_est"),
                  F.count("*").alias("clients_raw"),
                  F.sum("is_lv").alias("leavers_raw")))

    l13 = (_solo.unionByName(_pairs)
           .withColumn("basis", F.lit(_WIDE_BASIS))
           .withColumn("unsub_per_1000_est",
                       F.when(F.col("clients_est") > 0,
                              F.round(1000.0 * F.col("leavers_raw") / F.col("clients_est"), 2)))
           .select("basis", "mne_a", "mne_b", "clients_est", "clients_raw", "leavers_raw",
                   "unsub_per_1000_est"))

    # THE COMPARISON the brief actually asks for: pair rate against the HIGHER of its two solo rates.
    # Against the higher, not the average - a pair that beats only the weaker campaign has told you
    # nothing except which of the two is worse.
    _s = l13.filter(F.col("mne_b") == "").select(
        F.col("mne_a").alias("_m"), F.col("unsub_per_1000_est").alias("_solo_rate"))
    l13_pairs = (l13.filter(F.col("mne_b") != "")
                 .join(_s.withColumnRenamed("_m", "mne_a").withColumnRenamed("_solo_rate", "rate_a"),
                       "mne_a", "left")
                 .join(_s.withColumnRenamed("_m", "mne_b").withColumnRenamed("_solo_rate", "rate_b"),
                       "mne_b", "left")
                 .withColumn("rate_worse_solo", F.greatest("rate_a", "rate_b"))
                 .withColumn("lift_vs_worse_solo",
                             F.when(F.col("rate_worse_solo") > 0,
                                    F.round(F.col("unsub_per_1000_est") /
                                            F.col("rate_worse_solo"), 2)))
                 .select("mne_a", "mne_b", "clients_est", "leavers_raw", "unsub_per_1000_est",
                         "rate_a", "rate_b", "rate_worse_solo", "lift_vs_worse_solo"))

    # MIN_PAIR_LEAVERS is a precision floor, not a filter on the finding: a pair with 3 leavers can
    # show any lift at all and none of it means anything. Everything survives into the CSV.
    MIN_PAIR_LEAVERS = 30
    T("L13 - CAMPAIGN PAIRS, top 25 by lift over the worse of the two solo rates | basis " +
      _WIDE_BASIS + " | lift > 1 means the COMBINATION loses clients faster than either campaign "
      "alone | pairs with fewer than " + str(MIN_PAIR_LEAVERS) + " leavers hidden here, kept in the "
      "csv",
      l13_pairs.filter(F.col("leavers_raw") >= MIN_PAIR_LEAVERS)
      .orderBy(F.desc("lift_vs_worse_solo")).limit(25))
    T("L13b - CARDS pairs specifically (either side is a cards campaign)",
      l13_pairs.filter(F.col("mne_a").isin(*sorted(CARDS_MNES)) |
                       F.col("mne_b").isin(*sorted(CARDS_MNES)))
      .filter(F.col("leavers_raw") >= MIN_PAIR_LEAVERS)
      .orderBy(F.desc("lift_vs_worse_solo")).limit(25))

# %% [20c] CARDS CUBE - the per-campaign pivot source. Same idea as the main cube but at client x
# MNE grain, so bucket and campaign coexist on a row and a pivot can slice leaver-vs-stayer WITHIN
# a campaign. This is the file to open if the question is "which campaign loses which kind of client".
cards_cube = (cards_send.groupBy("mne", "bucket", "age_band", "tenure_band", "prod_band",
                                 "tibc_mix", "prof_quintile", "high_potential")
              .agg(F.count("*").alias("clients"),
                   F.round(F.avg("AGE"), 2).alias("mean_age"),
                   F.round(F.avg("TENURE_RBC_YEARS"), 2).alias("mean_tenure"),
                   F.round(F.avg("prod_cnt"), 2).alias("mean_prod"),
                   F.round(F.avg("PROF_TOT_ANNUAL"), 2).alias("mean_prof")))

cards_cube.write.mode("overwrite").parquet(BASE + "cards_cube")
cards_cube = spark.read.parquet(BASE + "cards_cube")
print("CARDS CUBE:", cards_cube.count(), "rows ->", BASE + "cards_cube")

# %% [21] # THE PIVOT CUBE - counts and means only. No explode, no stack, no percentile sketches.
#
# History, so nobody rebuilds the thing that failed: this cell first looped in Python over every
# MNE x role x metric and unioned ~2,400 one-row frames (driver OOM). Then it melted 76M rows and
# ran percentile_approx per group (driver OOM, four times). Percentiles over a 10M-client bank-wide
# block are what cost the money - count() and avg() are single-pass and cheap. Bands give the shape
# of the distribution anyway: a pivot on the quintile/band columns IS the percentile view.
#
# GRAIN: one row per campaign x program x bucket x age_band x tenure_band x prod_band x tibc_mix x
# prof_quintile x high_potential, with the client count and the mean of each of the four measures.
# bucket is a COLUMN (leaver / stayer / already_out), so leaver-vs-stayer is a pivot filter rather
# than a separate export. Matched clients only - unmatched have no bands.
cube = (matched.groupBy("mne", "program", "bucket", "age_band", "tenure_band", "prod_band",
                        "tibc_mix", "prof_quintile", "high_potential")
        .agg(F.count("*").alias("clients"),
             F.round(F.avg("AGE"), 2).alias("mean_age"),
             F.round(F.avg("TENURE_RBC_YEARS"), 2).alias("mean_tenure"),
             F.round(F.avg("prod_cnt"), 2).alias("mean_prod"),
             F.round(F.avg("PROF_TOT_ANNUAL"), 2).alias("mean_prof"))
        .join(senders_mne.select("mne", "senders"), "mne", "left")
        .join(_leavers_by_mne.select("mne", "unsubs"), "mne", "left")
        .withColumn("unsub_per_1000",
                    F.when(F.col("senders") > 0,
                           F.round(1000.0 * F.col("unsubs") / F.col("senders"), 2))))

cube.write.mode("overwrite").parquet(BASE + "cube")
cube = spark.read.parquet(BASE + "cube")
_n_cube = cube.count()
print("CUBE:", _n_cube, "rows ->", BASE + "cube")
assert _n_cube > 0, "cube landed empty - investigate before trusting anything downstream"

cube.coalesce(1).write.mode("overwrite").option("header", True).csv(BASE + "cube_csv")
assert spark.read.option("header", True).csv(BASE + "cube_csv").count() == _n_cube,     "cube_csv readback mismatch"
print("cube csv ->", BASE + "cube_csv", "- readback confirmed.")

# The four on-screen tables, written out as well. Each is a few hundred rows at most; they are the
# analysis as you read it, so they belong in the deliverable and not only in the notebook.
for _nm, _df in [("l1_where_by_mne", _leaver_profile(banded, ["mne", "program"])),
                 ("l6_campaign_summary", l6),
                 ("l7_leaver_vs_stayer", l7),
                 ("l8_cards_mailed_vs_leaver", l8),
                 ("l9_per_campaign_ratios", l9),
                 ("cards_cube", cards_cube)]:
    _df.coalesce(1).write.mode("overwrite").option("header", True).csv(BASE + "csv_" + _nm)
    print("  csv_" + _nm, "->", spark.read.option("header", True).csv(BASE + "csv_" + _nm).count(),
          "rows, readback confirmed")

# %% [21b] THE TWO DELIVERABLE CSVs.
#
# Ten csv exports answered ten questions and made the reader hold ten files. There are only TWO
# irreducible grains here, and everything above is one of them sliced differently:
#
#   A  unsub_segments   - a row is a SEGMENT.  Campaign is a scope COLUMN.   Answers WHO leaves.
#   B  unsub_campaigns  - a row is a CAMPAIGN (or a pair). No client attributes. Answers HOW MANY.
#
# A pivot on A regenerates L1, L7, L8, L9, L10b, L11 and L12. B carries L6, cadence and L13. The
# L-tables above stay on screen because that is where the analysis is read; these two files are what
# leaves the notebook. Counts only - every rate in A is the reader's to compute from two columns, so
# no denominator can be lost on the way to a slide.
#
# basis is on every row and it is not decoration. CENSUS rows count every client. SAMPLED rows come
# through MOD(CLNT_NO,SAMPLE_MOD) with leavers whole, so their client counts are ESTIMATES and their
# leaver counts are real. Mixing the two in one pivot without reading basis will produce a number
# that is wrong by SAMPLE_MOD and looks perfectly reasonable.
_SEG_DIMS = [d for d in (_DIMS + ["high_potential", "contact_band", "engagement"])
             if d in matched.columns]
print("segment dims:", _SEG_DIMS)


def _seg_rows(df, scope_type, scope_col, basis, weight):
    """One frame per dim, unioned. scope_col=None puts the whole frame under a single scope value.

    Dims are intersected with the frame's OWN columns. cards_send carries only _CARDS_CARRY, so it
    has no contact_band or engagement - asking for them would raise 200 lines from here. A dim that
    a frame cannot supply is absent from that scope's rows, not an error, and it is printed so the
    absence is visible rather than inferred from a gap in a pivot.
    """
    _out = []
    _use = [d for d in _SEG_DIMS if d in df.columns]
    _skip = [d for d in _SEG_DIMS if d not in df.columns]
    if _skip:
        print("  " + scope_type + "/" + basis + ": no", _skip, "on this frame - dims omitted")
    for _d in _use:
        _keys = ([scope_col] if scope_col else []) + ["bucket"]
        _g = (df.groupBy(*(_keys + [F.col(_d).cast("string").alias("segment_value")]))
              .agg(F.count("*").alias("clients")))
        if scope_col:
            _g = _g.withColumnRenamed(scope_col, "scope_value")
        else:
            _g = _g.withColumn("scope_value", F.lit("ALL"))
        _out.append(_g.withColumn("segment_dim", F.lit(_d))
                    .withColumn("scope_type", F.lit(scope_type))
                    .withColumn("basis", F.lit(basis))
                    .withColumn("clients_est",
                                F.when(F.col("bucket") == "leaver", F.col("clients"))
                                 .otherwise(F.col("clients") * F.lit(weight)))
                    .select("scope_type", "scope_value", "basis", "bucket", "segment_dim",
                            "segment_value", "clients", "clients_est"))
    _u = _out[0]
    for _f in _out[1:]:
        _u = _u.unionByName(_f)
    return _u


_seg_parts = [_seg_rows(matched, "BANK", None, "CENSUS", 1),
              _seg_rows(cards_send, "CAMPAIGN", "mne", "CARDS_CENSUS", 1)]
if HAVE_WIDE:
    _wide_banded = (senders_wide_raw.join(banded.filter(F.col("ucp_matched")), "CLNT_NO", "inner"))
    _seg_parts.append(_seg_rows(_wide_banded, "CAMPAIGN", "mne", "BANK_WIDE_SAMPLED", SAMPLE_MOD))

unsub_segments = _seg_parts[0]
for _p in _seg_parts[1:]:
    unsub_segments = unsub_segments.unionByName(_p)

# CADENCE -> a word. 92 days in the window, so 13+ waves is weekly, 6-12 fortnightly, 2-5 monthly.
# The raw n_deployments and send_days stay in the file: the label is a convenience, not the evidence.
_WIN_DAYS = 92
if HAVE_CADENCE:
    _cad = (add_mne_program(cadence_mne, mne_col="mne").drop("program")
            .withColumn("cadence",
                        F.when(F.col("n_deployments") >= 13, "weekly_or_more")
                         .when(F.col("n_deployments") >= 6, "fortnightly")
                         .when(F.col("n_deployments") >= 2, "monthly")
                         .otherwise("single_send"))
            .withColumn("mean_days_between_waves",
                        F.when(F.col("n_deployments") > 1,
                               F.round(F.lit(_WIN_DAYS) / F.col("n_deployments"), 1)))
            .select("mne", "n_deployments", "send_days", "first_send_dt", "last_send_dt",
                    "cadence", "mean_days_between_waves"))
else:
    _cad = None

# SOLO rows carry the census figures from L6 - senders and attributed unsubs are exact, no sample in
# them. Pair rows can only come from the sampled wide pull. Both live here because a solo campaign is
# the degenerate case of a pair, and splitting them would put brief 1a and brief 3c in two files that
# the reader has to join by hand.
_camp_solo = (senders_mne.select("mne", "program", "senders")
              .join(_leavers_by_mne.select("mne", F.col("unsubs").alias("unsubs_attributed")),
                    "mne", "left")
              .withColumn("mne_a", F.col("mne")).withColumn("mne_b", F.lit(""))
              .withColumn("basis", F.lit("CENSUS")))
if _cad is not None:
    _camp_solo = _camp_solo.join(_cad, "mne", "left")

# The solo row carries BOTH readings side by side: the census senders/unsubs from L6, and the same
# campaign as L13 measured it on the sampled pool. If they disagree by much more than sampling
# noise, the sample is not representative and every pair rate built on it is suspect. That check is
# only possible because both sit on one row.
if _WIDE_SRC is not None:
    _camp_solo = _camp_solo.join(
        l13.filter(F.col("mne_b") == "").select(
            F.col("mne_a").alias("mne"), "clients_est", "clients_raw", "leavers_raw",
            "unsub_per_1000_est"), "mne", "left")
_camp_solo = _camp_solo.drop("mne")

# Explicit column list, not allowMissingColumns: that argument needs Spark 3.1 and this file has
# never asserted a Spark version. Aligning by hand costs four lines and cannot surprise anyone.
_CAMP_COLS = ["basis", "mne_a", "mne_b", "program", "senders", "unsubs_attributed",
              "clients_est", "clients_raw", "leavers_raw", "unsub_per_1000_est"]
if _cad is not None:
    _CAMP_COLS += ["n_deployments", "send_days", "first_send_dt", "last_send_dt", "cadence",
                   "mean_days_between_waves"]


def _align(df):
    return df.select(*[F.col(c) if c in df.columns else F.lit(None).alias(c) for c in _CAMP_COLS])


if _WIDE_SRC is not None:
    unsub_campaigns = _align(_camp_solo).unionByName(_align(l13.filter(F.col("mne_b") != "")))
else:
    unsub_campaigns = _align(_camp_solo)

for _nm, _df in [("unsub_segments", unsub_segments), ("unsub_campaigns", unsub_campaigns)]:
    _df.coalesce(1).write.mode("overwrite").option("header", True).csv(BASE + _nm)
    _n = spark.read.option("header", True).csv(BASE + _nm).count()
    assert _n > 0, _nm + " landed empty"
    print("  " + _nm, "->", _n, "rows, readback confirmed")

T("DELIVERABLE A - unsub_segments, first 20 rows (pivot this: scope_type x bucket x segment_dim)",
  unsub_segments.orderBy("scope_type", "scope_value", "segment_dim", "segment_value").limit(20))
T("DELIVERABLE B - unsub_campaigns, cards solo rows",
  unsub_campaigns.filter((F.col("mne_b") == "") & F.col("mne_a").isin(*sorted(CARDS_MNES)))
  .orderBy(F.desc("senders")))

print("")
print("To fetch for Excel, from a TERMINAL (not this kernel):")
print("  THE TWO DELIVERABLES:")
for _nm in ["unsub_segments", "unsub_campaigns"]:
    print("  hdfs dfs -getmerge /user/427966379/unsub_value_museum/" + _nm + " " + _nm + ".csv")
print("  BACKING DETAIL (only if a number is challenged):")
for _nm in ["cube_csv", "csv_l1_where_by_mne", "csv_l6_campaign_summary",
            "csv_l7_leaver_vs_stayer", "csv_l8_cards_mailed_vs_leaver",
            "csv_l9_per_campaign_ratios", "csv_cards_cube"]:
    print("  hdfs dfs -getmerge /user/427966379/unsub_value_museum/" + _nm + " " + _nm + ".csv")

# %% [22] SAVE - client_spine. THE durable artifact: every table above re-derives from it without
# touching EDW or UCP again.
_SPINE_CORE = ["CLNT_NO", "bucket", "mne", "program", "TREATMENT_ID", "unsub_tm",
               "ucp_matched", "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT",
               "B_TOT_CNT", "C_TOT_CNT", "PROF_TOT_ANNUAL", "prod_cnt", "prod_band",
               "tibc_mix", "tenure_band", "age_band", "prof_quintile", "high_potential"]
# contact, engagement and already-out detail ride along when the pull carried them, so every table
# above can be re-derived from this one file without going back to EDW.
_SPINE_EXTRA = [c for c in ["n_send_events", "n_mnes", "n_treatments", "breadth_upper",
                            "breadth_lower", "breadth_band", "n_opens", "n_clicks", "opened",
                            "clicked", "contact_band", "engagement", "last_prior_unsub_dt",
                            "last_prior_unsub_mne", "n_prior_unsub_events",
                            "days_since_prior_unsub"] if c in banded.columns]
client_spine = banded.select(*(_SPINE_CORE + _SPINE_EXTRA))
print("client_spine carries", len(_SPINE_CORE), "core columns +", len(_SPINE_EXTRA), "extra:",
      _SPINE_EXTRA or "(none - pull predates contact/engagement)")
_n_cs = client_spine.count()
client_spine.write.mode("overwrite").parquet(BASE + "client_spine")
assert spark.read.parquet(BASE + "client_spine").count() == _n_cs, "client_spine readback mismatch"
print("client_spine saved to", BASE + "client_spine", "-", _n_cs, "rows, readback confirmed.")

# %% [23] ONE-SCREEN SUMMARY
display(Markdown("## UNSUB VALUE MUSEUM - " + WIN_START + " to " + WIN_END + " - SUMMARY"))
T("SUMMARY - sizes", pd.DataFrame([
    {"figure": "Clients mailed in window (bank-wide)", "value": _n_mailed},
    {"figure": "Leavers (unsubscribed in window)", "value": _n_lv},
    {"figure": "Stayers (no unsub, none prior)", "value": _n_st},
    {"figure": "Already out (mailed despite an earlier unsub - the leak)",
     "value": int(_bucket_counts.loc[_bucket_counts["bucket"] == "already_out", "count"].sum())},
    {"figure": "UCP matched", "value": _n_matched},
]))
T("SUMMARY - strongest leaver-vs-stayer signals (|ratio| furthest from 1)",
  l7.filter(F.col("stayers_n") > 100).orderBy(F.desc("ratio")).limit(8))

display(Markdown("**CAVEATS**"))
for _c in [
    "NO RATE except unsub_per_1000 in L6/L8. Everything else is a count, a median or a share.",
    "LAST-TOUCH ATTRIBUTION: the MNE on a leaver is whichever campaign's email carried the click. "
    "A client on many lists who unsubscribes once is attributed to one campaign; heavy senders "
    "structurally inherit blame they may not have earned.",
    "OVERLAPPING DENOMINATORS in L6 and L8: a client mailed by several campaigns sits in several "
    "rows. Read down a column, never across - the columns do not sum to the bank total.",
    "PROF_TOT_ANNUAL is CURRENT-YEAR CONTRIBUTION, not LTV. Proven 2026-07-27: medians rise "
    "-25.53 -> 35.96 -> 111.54 -> 257.70 -> 555.93 across tenure bands. Label it 'annual "
    "profitability' on any slide. A client flagged low-value is partly just young.",
    ("PER-CAMPAIGN PROFILES: cards campaigns are a CENSUS (senders_cards). Non-cards campaigns come "
     "from senders_wide, which is leavers-whole plus 1-in-" + str(SAMPLE_MOD) + " stayers, so their "
     "client counts are ESTIMATES (clients_est) and their leaver counts are real. Read the `basis` "
     "column before comparing a cards row with a non-cards row."
     if HAVE_WIDE else
     "PER-CAMPAIGN SENDER AND STAYER PROFILES ARE CARDS-ONLY. senders_wide was not pulled, so "
     "non-cards campaigns carry counts and a rate but null sender/stayer stats, and 3c compares "
     "cards against cards only."),
    ("CAMPAIGN BREADTH is counted as " + BREADTH_LABEL.upper() + ", from column " +
     str(BREADTH_COL) + ". It is reported as a RANGE (breadth_lower..breadth_upper) because the "
     "pull is monthly and distinct counts do not sum across months: the lower bound is one month's "
     "max, the upper is the sum. breadth_band uses the LOWER bound." +
     ("" if BREADTH_IS_CAMPAIGNS else " THIS LANDING COUNTS DEPLOYMENT WAVES, NOT CAMPAIGNS - "
      "brief item 3b needs n_mnes; re-run [6].")),
    "L13 CO-OCCURRENCE IS A COMPARISON, NOT AN ATTRIBUTION. A pair rate above both solo rates says "
    "the combination loses clients faster than either campaign alone; it does not say the pair "
    "CAUSED it. Clients mailed by two campaigns are mailed more, and contact volume is the standing "
    "confound - check the pair against L10 before calling a combination the cause.",
    "CLIENTS ACQUIRED DURING THE WINDOW cannot exist in the " + UCP_ANCHOR + " snapshot and come "
    "back unmatched. Counted in M0a, never dropped silently.",
    "BAND CUT POINTS and the high_potential thresholds (age<" + str(HP_AGE) + ", tenure<=" +
    str(HP_TENURE) + ", products<=" + str(HP_PRODS) + ") are our parameters, not standards.",
]:
    display(Markdown("- " + _c))

# %% [24] CLEANUP - OPT-IN. The raw pulls are scratch; client_spine and summary are the artifacts.
# Left False by default: this file is run top to bottom, and firing cleanup mid-iteration would
# force a re-pull on the next run.
RUN_CLEANUP = False

if not RUN_CLEANUP:
    print("CLEANUP skipped (RUN_CLEANUP = False) - raw pulls retained for restartability.")
else:
    for _art in ("client_spine", "summary"):
        assert landed(_art) and spark.read.parquet(BASE + _art).count() > 0, \
            _art + " is not safely landed - refusing to delete any raw pull."
    _raw = (["unsubs_3m/" + m[0] for m in WIN_MONTHS] +
            ["senders_base/" + m[0] for m in WIN_MONTHS] +
            ["senders_cards/" + m[0] for m in WIN_MONTHS] +
            ["senders_by_mne", "prior_unsubs"])
    for _p in _raw:
        get_ipython().system("hdfs dfs -rm -r -f /user/427966379/unsub_value_museum/" + _p)
    T("CLEANUP - what survived", pd.DataFrame(
        [{"dataset": a, "status": "KEPT" if landed(a) else "MISSING - INVESTIGATE"}
         for a in ("client_spine", "summary", "summary_csv", "ucp_spine")] +
        [{"dataset": p, "status": "REMOVED" if not landed(p) else "STILL PRESENT"} for p in _raw]))
    print("housekeeping complete - raw pulls removed, durable outputs verified intact.")

# OPEN QUESTIONS (2026-07-27, appended post-run, no code changed)
#
# ANCHOR RULE ANDRE SPECIFIED: per-client UCP anchoring - each client's UCP snapshot taken at the
#   month-end matching THEIR OWN unsub date, not a date shared across clients.
#
# WHAT WAS IMPLEMENTED INSTEAD: ONE common anchor, UCP_ANCHOR = "2026-02-28", used for every client
#   regardless of bucket (leaver/already_out/stayer). See cell [1]/[12] above.
#
# WHY: stayers have no unsub event to anchor to. Per-client anchoring would put leavers and stayers
#   at different points in their own timelines, biasing the L7/L8 leaver-vs-stayer comparisons.
#   2026-02-28 is also the last month-end strictly before the Mar-May send window, so nothing in it
#   can be a consequence of the treatment. The 12-month version of this file DID try per-client
#   anchoring - it fanned out to 30 UCP partitions and got the session killed by YARN (2026-07-27).
#
# STATUS: Andre's ruling 2026-07-27 - accepted FOR THIS RUN ONLY. Staleness is ~2 months and the
#   four measured fields (age, tenure, product count, annual profitability) move slowly at that
#   horizon. He was explicit that this deviated from his stated instruction and was under-flagged
#   when it happened (buried in a design comment, not called out prominently).
#
# REVISIT ON NEXT ITERATION: either implement true per-client unsub-date anchoring, or get explicit
#   sign-off from Andre to keep the common anchor permanently. Do not silently carry this forward.
