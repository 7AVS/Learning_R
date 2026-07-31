# =============================================================================
# ONE CELL. Paste the whole file into one notebook cell and press run once.
#
# This is [2f] and nothing else - the DFP card-state pull, the only cell that
# still owes you data. Everything else already landed on HDFS and is not
# touched here. Nothing in this file re-pulls the cohort, the mnemonics or the
# 18-bite prior-unsub cell.
#
# WHAT IT DOES
#   installs teradatasql, asks for your password, pulls DLY_FULL_PORTFOLIO in
#   10 bites, lands each bite as it returns, rolls up to one row per client,
#   writes dfp_card_state to HDFS.
#
# IF IT DIES PART WAY, RUN IT AGAIN. Completed bites print CACHED and cost
# seconds. Only the bites that never finished go back to EDW.
#
# WHEN IT FINISHES you are done with the pull. Go to [A1] in
# unsub_before_after_jul25.py (line 550) - it reads HDFS and needs no password.
# =============================================================================

get_ipython().system("./environment/bin/python -m pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")

import getpass, time, warnings
import pandas as pd
import teradatasql

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")
if not hasattr(pd.DataFrame, "iteritems"):
    pd.DataFrame.iteritems = pd.DataFrame.items

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

OUT      = "hdfs:///user/427966379/unsub_value_jul25/"
PULL_OUT = OUT + "cache/"

# --- helpers, copied verbatim from [1] so this file needs nothing else -------
def key_pd(sr, label=""):
    n = pd.to_numeric(sr, errors="coerce")
    bad = int(n.isna().sum())
    if bad:
        print("   key_pd(%s): %s of %s CLNT_NO are null or non-numeric - dropped"
              % (label, f"{bad:,}", f"{len(n):,}"))
    return n.round(0).astype("Int64").astype("string")

def land(pdf, name):
    path = PULL_OUT + name
    spark.createDataFrame(pdf).coalesce(8).write.mode("overwrite").option("header", True).csv(path)
    n = spark.read.option("header", True).csv(path).count()
    assert n == len(pdf), "%s readback mismatch: wrote %d read %d" % (name, len(pdf), n)
    print("LANDED %-14s %s rows -> %s" % (name, f"{len(pdf):,}", path))

def cached(name):
    path = PULL_OUT + name
    try:
        df = spark.read.option("header", True).csv(path)
        n = df.count()
        if n > 0:
            print("CACHED %-14s %s rows <- %s" % (name, f"{n:,}", path))
            return df.toPandas()
    except Exception:
        pass
    return None

# --- what already landed, before you spend a password -----------------------
print("\nOn HDFS already:")
for _n in ["cohort_raw", "unsub_mne", "prior_unsub", "dfp_card_state"]:
    try:
        print("   %-16s %s rows" % (_n, f"{spark.read.option('header',True).csv(PULL_OUT+_n).count():,}"))
    except Exception:
        print("   %-16s MISSING" % _n)

if cached("dfp_card_state") is not None:
    print("\ndfp_card_state is already on HDFS. Nothing to pull. Go to [A1].")
else:
    username = input("Enter your username: ")
    password = getpass.getpass("Enter your password: ")
    EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com", user=username,
                              password=password, logmech="LDAP")

    def edw_pd(sql, chunksize=1_000_000):
        parts, n, t0 = [], 0, time.time()
        for c in pd.read_sql(sql, EDW, chunksize=chunksize):
            parts.append(c); n += len(c)
            print("   ...", f"{n:,}", "rows pulled,", int(time.time() - t0), "s elapsed", flush=True)
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    cur = EDW.cursor(); cur.execute("SELECT USER, CURRENT_TIMESTAMP")
    print("EDW round-trip returned:", cur.fetchall()); cur.close()

    # --- the pull -----------------------------------------------------------
    # status is one-way: once an account goes non-OPEN it stays non-OPEN, so the account's FINAL state
    # is MAX(CASE ...) over a plain GROUP BY. No window function, no DISTINCT across the table.
    DFP_TMPL = """
    SELECT clnt_no,
           COUNT(DISTINCT acct_no) AS n_accts,
           MAX(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS any_open,
           MAX(CASE WHEN status = 'VOL'  THEN 1 ELSE 0 END) AS ever_voluntary,
           MAX(CASE WHEN status = 'WOFF' THEN 1 ELSE 0 END) AS ever_writeoff,
           MAX(CASE WHEN status = 'BKPT' THEN 1 ELSE 0 END) AS ever_bankrupt,
           MIN(acct_cls_dt) AS first_close_dt,
           MAX(acct_cls_dt) AS last_close_dt
    FROM D3CV12A.DLY_FULL_PORTFOLIO
    WHERE dt_record_ext >= DATE '2025-07-01' AND dt_record_ext < DATE '2026-07-01'
      AND ABS(clnt_no) MOD 10 = %d
    GROUP BY clnt_no
    """

    def _dfp_bite(i):
        path = PULL_OUT + "dfp_bite_%d" % i
        try:
            hit = spark.read.option("header", True).csv(path)
            n = hit.count()
            if n > 0:
                print("dfp bite %d/10 CACHED %s rows" % (i + 1, f"{n:,}"), flush=True)
                return hit.toPandas()
        except Exception:
            pass
        print("dfp bite %d/10 pulling" % (i + 1), flush=True)
        x = edw_pd(DFP_TMPL % i)
        assert len(x) > 0, "dfp bite %d returned zero rows" % i
        # Teradata titles an unaliased column with its DDL casing, so SELECT clnt_no comes back as
        # CLNT_NO. Lower-casing on arrival is what stops KeyError: 'clnt_no'.
        x.columns = [c.strip().lower() for c in x.columns]
        spark.createDataFrame(
            x.astype(str).replace({"NaT": "", "nan": "", "None": ""})
        ).coalesce(8).write.mode("overwrite").option("header", True).csv(path)
        print("   bite %d landed -> %s" % (i + 1, path), flush=True)
        return x

    dfp = pd.concat([_dfp_bite(_i) for _i in range(10)], ignore_index=True)
    dfp.columns = [c.strip().lower() for c in dfp.columns]
    dfp["clnt_key"] = key_pd(dfp["clnt_no"], "dfp")
    dfp = dfp[dfp["clnt_key"].notna()].copy()
    dfp["clnt_key"] = dfp["clnt_key"].astype(str)
    for _c in ["n_accts", "any_open", "ever_voluntary", "ever_writeoff", "ever_bankrupt"]:
        dfp[_c] = pd.to_numeric(dfp[_c], errors="coerce").fillna(0).astype("int32")
    for _c in ["first_close_dt", "last_close_dt"]:
        dfp[_c] = pd.to_datetime(dfp[_c], errors="coerce").dt.strftime("%Y-%m-%d")

    # one mutually exclusive state per client. Any open card wins - a client with three closed cards
    # and one open one has not attrited.
    dfp["card_state"] = "closed_other"
    dfp.loc[dfp["ever_bankrupt"] == 1, "card_state"] = "bankrupt"
    dfp.loc[dfp["ever_writeoff"] == 1, "card_state"] = "written_off"
    dfp.loc[dfp["ever_voluntary"] == 1, "card_state"] = "closed_voluntary"
    dfp.loc[dfp["any_open"] == 1, "card_state"] = "open"

    dfp = dfp[["clnt_key", "card_state", "n_accts", "first_close_dt", "last_close_dt"]]
    land(dfp, "dfp_card_state")
    print(dfp["card_state"].value_counts().rename_axis("card_state").reset_index(name="clients"))
    print("\nDONE. dfp_card_state is on HDFS. Next: [A1] in unsub_before_after_jul25.py, line 550.")
    print("Clients in the cohort with NO row here hold no card at all - a third state, not the same")
    print("as having left. [A1] onward keeps them separate.")
