# %% [markdown]
# # 39 — probe: do switches get written TOGETHER? (write bundles by timestamp)
#
# Pack 38 [4] showed the LOB codes moving in lockstep (~213-239K each, near-identical
# per-writer volumes) — the hypothesis is one action writing MANY preferences at once.
# Test: rows written for the same client at the same CHG_TMSTMP = one bundle.
# 18-month frame, 5002 writes, DDWV01.CPC_RB_PREF.
#
# CAVEAT — current-state table: a preference later re-written leaves its old bundle, so
# bundle sizes are FLOORS. Same-timestamp = same system action; a true trigger/dependency
# (A causes B later) would show as ordered writes apart in time, not bundles.

# %% [0] connect + proof round-trip
try:
    import teradatasql
except ImportError:
    get_ipython().system("pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")
    import teradatasql
import getpass
import pandas as pd

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")

EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com", user=username,
                          password=password, logmech="LDAP")

def edw_pd(sql, chunksize=1_000_000):
    cur = EDW.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    parts = []
    while True:
        rows = cur.fetchmany(chunksize)
        if not rows:
            break
        parts.append(pd.DataFrame(rows, columns=cols))
    cur.close()
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=cols)

display(edw_pd("SELECT USER AS usr, SESSION AS sess, CURRENT_TIMESTAMP AS ts"))

# %% [1] Raw look - all 5002 rows for 5 clients who wrote 1012 in the frame
# Eyeball first: do their other preferences carry the SAME timestamp?
display(edw_pd("""
SELECT p.CLNT_NO, p.PREF_ID, p.CLNT_CONSENT_TYP, p.CHG_TMSTMP, p.APP_SYS_CD
FROM DDWV01.CPC_RB_PREF p
JOIN (
    SELECT CLNT_NO
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-02-01'
    SAMPLE 5
) s ON s.CLNT_NO = p.CLNT_NO
WHERE p.CLNT_CONSENT_TYP = 5002
ORDER BY p.CLNT_NO, p.CHG_TMSTMP, p.PREF_ID
"""))

# %% [2] Bundle sizes - how many preferences share one (client, timestamp) write event
bs = edw_pd("""
SELECT bundle_size, COUNT(*) AS n_write_events
FROM (
    SELECT CLNT_NO, CHG_TMSTMP, COUNT(*) AS bundle_size
    FROM DDWV01.CPC_RB_PREF
    WHERE CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-02-01'
    GROUP BY 1, 2
) t
GROUP BY 1 ORDER BY 1
""")
bs["share_pct"] = (bs["n_write_events"] / bs["n_write_events"].sum() * 100).round(1)
display(bs)

# %% [3] Bundle size by writer system - who writes one switch, who writes them all
bw = edw_pd("""
SELECT APP_SYS_CD,
       CASE WHEN bundle_size = 1  THEN '1_single'
            WHEN bundle_size = 2  THEN '2_pair'
            WHEN bundle_size <= 5 THEN '3_to_5'
            WHEN bundle_size <= 10 THEN '6_to_10'
            ELSE                       '11_plus' END AS bundle_bucket,
       COUNT(*) AS n_write_events
FROM (
    SELECT CLNT_NO, CHG_TMSTMP, APP_SYS_CD, COUNT(*) AS bundle_size
    FROM DDWV01.CPC_RB_PREF
    WHERE CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-02-01'
    GROUP BY 1, 2, 3
) t
GROUP BY 1, 2
""")
pv = bw.pivot_table(index="APP_SYS_CD", columns="bundle_bucket",
                    values="n_write_events", aggfunc="sum", fill_value=0)
pv["TOTAL"] = pv.sum(axis=1)
display(pv.sort_values("TOTAL", ascending=False).head(12))

# %% [4] What travels WITH a 1012 write - same client, same timestamp
# For every 1012 write event: which other preferences were written in the same instant.
co = edw_pd("""
SELECT p2.PREF_ID AS co_written_pref, COUNT(*) AS n_together
FROM DDWV01.CPC_RB_PREF p1
JOIN DDWV01.CPC_RB_PREF p2
  ON  p2.CLNT_NO    = p1.CLNT_NO
  AND p2.CHG_TMSTMP = p1.CHG_TMSTMP
  AND p2.PREF_ID   <> 1012
  AND p2.CLNT_CONSENT_TYP = 5002
WHERE p1.PREF_ID = 1012 AND p1.CLNT_CONSENT_TYP = 5002
  AND p1.CHG_TMSTMP >= DATE '2025-02-01'
GROUP BY 1 ORDER BY 2 DESC
""")
n_1012 = edw_pd("""
SELECT COUNT(*) AS n FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002 AND CHG_TMSTMP >= DATE '2025-02-01'
""")["n"].iloc[0]
co["pct_of_1012_writes"] = (100.0 * co["n_together"] / n_1012).round(1)
print(f"--- of {n_1012:,} 1012 write events, what else was written at the exact same instant ---")
display(co.head(25))
