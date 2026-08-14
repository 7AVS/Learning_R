# %% [markdown]
# # 36 — clone of the production Borealis CPC_CHANNELS_CD rule
#
# Reproduces the production contactability logic (transcribed to
# `references/cpc_channels_cd_borealis_rule.md`) so we can audit it against our own
# counts. Per preference: FALSE (do not contact) when consent = 5002, OR when consent
# = 5003 AND EMP_ID is a real value (not NULL, not the two dummy codes). ELSE TRUE.
# Source tables exactly as production: `DDWV01.RB_CLNT_DLY` (latest snapshot within
# 5 days) LEFT JOIN `DG6V01.CPC_CLNT_PREF_CHC` on CLNT_NO.
#
# Open question this pack must settle (2026-08-14): is 5003 a dormant default, or do
# rows TRANSITION into 5003 as consent withdrawals ([4])? And does EMP_ID mean
# "client is an employee" or "employee who keyed the change"? Descriptive only.

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

# %% [P1] Sample rows — CPC_CLNT_PREF_CHC, all columns (grain + does it carry EMP_ID / a change timestamp?)
pd.set_option("display.max_colwidth", 120)
print("--- DG6V01.CPC_CLNT_PREF_CHC, 10 raw rows, every column ---")
display(edw_pd("SELECT * FROM DG6V01.CPC_CLNT_PREF_CHC SAMPLE 10"))

# %% [1] Faithful clone — CPC_CHANNELS_CD string for 10 random clients (production shape)
# Same CASE per preference as production, same MIN() collapse, same snapshot filter.
# Driver restricted to 10 sampled clients FIRST so this stays light.
PREFS = [(1007, "Banking - Direct Mail"),    (1008, "Banking - Telephone"),
         (1009, "Banking - RBC Online"),     (1012, "Banking - E-Mail"),
         (1013, "Banking - Face to Face"),   (1037, "Direct Investing - Direct Mail"),
         (1038, "Direct Investing - Telephone"), (1039, "Direct Investing - DI Online"),
         (1040, "Direct Investing - E-Mail"), (1041, "Direct Investing - Face to Face"),
         (1048, "Banking - ATM")]

BLOCK = """MIN(CASE
    WHEN B.PREF_ID = {pid}
         AND (B.CLNT_CONSENT_TYP = 5002
              OR (B.CLNT_CONSENT_TYP = 5003
                  AND B.EMP_ID NOT IN (999999999999999, 999999999)
                  AND B.EMP_ID IS NOT NULL)) THEN '{name}:FALSE, '
    ELSE '{name}:TRUE, '
END)"""
blocks = ",\n".join(BLOCK.format(pid=p, name=n) for p, n in PREFS)

display(edw_pd(f"""
WITH pick AS (
    SELECT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.RB_CLNT_DLY WHERE SNAP_DT > DATE-5)
    SAMPLE 10
)
SELECT A.CLNT_NO,
       CONCAT('[', {blocks}, ']') AS CPC_CHANNELS_CD
FROM pick A
LEFT JOIN DG6V01.CPC_CLNT_PREF_CHC B ON A.CLNT_NO = B.CLNT_NO
GROUP BY 1
"""))

# %% [2] Reason buckets per preference — who is FALSE and WHY (whole CHC table, one scan)
# The Borealis FALSE decomposed: explicit 5002 vs the 5003+real-EMP_ID leg, plus the
# TRUE-side masses. 1002 and 1014 included for comparison even though the production
# rule does not use them.
rb = edw_pd("""
SELECT PREF_ID,
       CASE WHEN CLNT_CONSENT_TYP = 5002                              THEN '1_FALSE_explicit_no_5002'
            WHEN CLNT_CONSENT_TYP = 5003
                 AND EMP_ID IS NOT NULL
                 AND EMP_ID NOT IN (999999999999999, 999999999)       THEN '2_FALSE_5003_real_emp_id'
            WHEN CLNT_CONSENT_TYP = 5003                              THEN '3_TRUE_5003_null_or_dummy_emp'
            ELSE                                                           '4_TRUE_other_consent_typ' END AS bucket,
       COUNT(*)                 AS n_rows,
       COUNT(DISTINCT CLNT_NO)  AS n_clients
FROM DG6V01.CPC_CLNT_PREF_CHC
WHERE PREF_ID IN (1002, 1007, 1008, 1009, 1012, 1013, 1014, 1037, 1038, 1039, 1040, 1041, 1048)
GROUP BY 1, 2
ORDER BY 1, 2
""")
display(rb.pivot_table(index="PREF_ID", columns="bucket", values="n_clients",
                       aggfunc="sum", fill_value=0))

# %% [3] The no-row mass — clients in the snapshot with NO CHC rows at all (default TRUE everywhere)
print("--- production default: no preference rows -> contactable on every channel ---")
display(edw_pd("""
SELECT COUNT(*)                                            AS n_clients_snapshot,
       SUM(CASE WHEN B.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS n_with_chc_rows,
       SUM(CASE WHEN B.CLNT_NO IS NULL     THEN 1 ELSE 0 END) AS n_no_chc_rows_default_true
FROM DDWV01.RB_CLNT_DLY A
LEFT JOIN (SELECT DISTINCT CLNT_NO FROM DG6V01.CPC_CLNT_PREF_CHC) B
       ON B.CLNT_NO = A.CLNT_NO
WHERE A.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.RB_CLNT_DLY WHERE SNAP_DT > DATE-5)
"""))

# %% [4] THE DECIDER — fresh writes INTO 5003, by EMP_ID class (1002 / 1012 / 1014)
# If 5003 is a dormant default, this is ~empty. If rows transition into 5003 as
# consent withdrawals, these are switch-off EVENTS our 5002-only packs exclude.
# NOTE: assumes CHC carries CHG_TMSTMP - if this errors, take the real change-timestamp
# column name from [P1] and substitute.
SQL_5003_WRITES = """
SELECT CASE WHEN EMP_ID IS NULL                            THEN 'emp_null'
            WHEN EMP_ID IN (999999999999999, 999999999)    THEN 'emp_dummy'
            ELSE                                                'emp_real' END AS emp_class,
       COUNT(*)                AS n_writes_to_5003,
       COUNT(DISTINCT CLNT_NO) AS n_clients
FROM DG6V01.CPC_CLNT_PREF_CHC
WHERE PREF_ID = {pref}
  AND CLNT_CONSENT_TYP = 5003
  AND CHG_TMSTMP >= DATE '2026-05-01'
GROUP BY 1 ORDER BY 2 DESC
"""
for pref in (1002, 1012, 1014):
    print(f"--- PREF_ID {pref}: writes into 5003 since 2026-05-01, by EMP_ID class ---")
    display(edw_pd(SQL_5003_WRITES.format(pref=pref)))
