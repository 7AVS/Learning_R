# %% [markdown]
# # 42 — verification probes: DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL (Email Eligibility Detail)
#
# Candidate base for the emailable-base waterfall (its own doc: "can also be used for a
# waterfall analysis"; catalogued in `references/cidm_email_eligibility.md`). Each cell
# answers ONE question. Companion plain-SQL version: `42_cidm_em_dtl_probes.sql`.

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
pd.set_option("display.max_colwidth", 80)

# %% [1] What does a row look like?
print("--- DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL, 10 raw rows, every column ---")
display(edw_pd("SELECT * FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL SAMPLE 10"))

# %% [2] Freshness + history: is LOAD_DT a daily snapshot, monthly, or one-off?
print("--- overall span ---")
display(edw_pd("""
SELECT MIN(LOAD_DT) AS earliest_load,
       MAX(LOAD_DT) AS latest_load,
       COUNT(DISTINCT LOAD_DT) AS n_loads,
       CAST(COUNT(*) AS BIGINT) AS n_rows_total
FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
"""))
print("--- rows per load (stable ~14-15MM per load = full-base snapshots) ---")
display(edw_pd("""
SELECT LOAD_DT, CAST(COUNT(*) AS BIGINT) AS n_rows,
       COUNT(DISTINCT CLNT_NO) AS n_clients
FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
GROUP BY 1
ORDER BY 1 DESC
"""))

# %% [3] Grain proof on the LATEST load: one row per client, or duplicates?
gr = edw_pd("""
SELECT dup_rows, COUNT(*) AS n_clients
FROM (
    SELECT CLNT_NO, COUNT(*) AS dup_rows
    FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
    WHERE LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
    GROUP BY 1
) t
GROUP BY 1 ORDER BY 1
""")
display(gr)
if len(gr) == 1 and gr["dup_rows"].iloc[0] == 1:
    print("GRAIN OK: one row per client on the latest load.")
else:
    print("WARNING: duplicated CLNT_NO on the latest load - inspect before using.")

# %% [4] The waterfall raw material: indicator combinations on the latest load
# This one output IS the decomposition of why clients are in or out of the base -
# including the kill-file and spam-complaint layers we previously called invisible.
print("--- indicator combinations, client counts, latest load ---")
display(edw_pd("""
SELECT DELIVERABLE_EM_ADDR_IND, CPC1012_IND, EMAIL_KILL_CLNT_IND,
       SPAM_COMPLAINT_EM_IND, VALID_EM_ADDR_IND, EM_ELIGIBLE_IND,
       CAST(COUNT(*) AS BIGINT) AS n_clients
FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
WHERE LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 7 DESC
"""))

# %% [5] The headline: emailable base on the latest load (mock expects ~14-15MM)
display(edw_pd("""
SELECT EM_ELIGIBLE_IND, CAST(COUNT(*) AS BIGINT) AS n_clients
FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
WHERE LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
GROUP BY 1
"""))

# %% [6] Cross-check vs what we know: CPC1012_IND vs standing 1012=No in CHC
print("--- CHC: clients standing at explicit No on 1012 ---")
display(edw_pd("""
SELECT CAST(COUNT(*) AS BIGINT) AS n_standing_1012_no_in_CHC
FROM DG6V01.CPC_CLNT_PREF_CHC
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
"""))
print("--- EM_DTL: CPC1012_IND split on latest load (encoding per [4]) ---")
display(edw_pd("""
SELECT CPC1012_IND, CAST(COUNT(*) AS BIGINT) AS n_clients
FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
WHERE LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
GROUP BY 1
"""))

# %% [7] Equivalence spot-check: CHC vs CPC_RB_PREF (documented mirrors)
display(edw_pd("""
SELECT 'DG6V01.CPC_CLNT_PREF_CHC' AS source_table,
       CAST(COUNT(*) AS BIGINT) AS n_1012_explicit_no
FROM DG6V01.CPC_CLNT_PREF_CHC
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
UNION ALL
SELECT 'DDWV01.CPC_RB_PREF',
       CAST(COUNT(*) AS BIGINT)
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
"""))
