# %% [markdown]
# # 36b — the production Borealis CPC_CHANNELS_CD query, VERBATIM
#
# The exact query from the Borealis business-rules repo (transcribed 2026-08-14 from
# screenshots; see `references/cpc_channels_cd_borealis_rule.md`), longhand, no
# sampling, full portfolio - one row per client in the latest RB_CLNT_DLY snapshot.
# Written blocks byte-shaped like the source, including the original comment header.
#
# WARNING: full-portfolio output (millions of rows). [1] proves it compiles and sizes
# it with a COUNT wrapper; [2] is the raw run - uncomment only in the work env, and
# send the output to a table/extract, not to a notebook display.

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

# %% [V] The verbatim production query
SQL_VERBATIM = """
/*
 * CREATED BY: devillersg
 * DATE CREATED: 2017-04-17
 * UNIQUE KEY: CLNT_NO
 *
 * DESCRIPTION:
 * Client Preferences collects and stores customer consents and preferences relative to information sharing, marketing and service options.
 * There are four groups of Consents & Preferences:
 *      1)Consents for entity marketing and Information Usage
 *      2)Consents for Communication Channels
 *      3)Product preferences
 *      4)Service preferences
 * TRUE means the client does not have a CPC and can be contacted
 *
 * CHANGE LOG:
 * 2017-04-17: gd: created
 * 2023-02-06: Jiaming Yang : Added coalesce statement
 * 2023-02-09: Jiaming Yang : removed coalesce statement
 *
 * 2024-11-18: RBC Borealis: Business rule auto conversion
 */
SELECT A.CLNT_NO,
       CONCAT('[', MIN(CASE
                       WHEN B.PREF_ID = 1007
                            AND (B.CLNT_CONSENT_TYP=5002
                                 OR (B.CLNT_CONSENT_TYP=5003
                                     AND B.EMP_ID NOT IN (999999999999999, 999999999)
                                     AND B.EMP_ID IS NOT NULL)) THEN 'Banking - Direct Mail:FALSE, '
                       ELSE 'Banking - Direct Mail:TRUE, '
                   END), MIN(CASE
                       WHEN B.PREF_ID = 1008
                            AND (B.CLNT_CONSENT_TYP=5002
                                 OR (B.CLNT_CONSENT_TYP=5003
                                     AND B.EMP_ID NOT IN (999999999999999, 999999999)
                                     AND B.EMP_ID IS NOT NULL)) THEN 'Banking - Telephone:FALSE, '
                       ELSE 'Banking - Telephone:TRUE, '
                   END), MIN(CASE
                       WHEN B.PREF_ID = 1009
                            AND (B.CLNT_CONSENT_TYP=5002
                                 OR (B.CLNT_CONSENT_TYP=5003
                                     AND B.EMP_ID NOT IN (999999999999999, 999999999)
                                     AND B.EMP_ID IS NOT NULL)) THEN 'Banking - RBC Online:FALSE, '
                       ELSE 'Banking - RBC Online:TRUE, '
                   END), MIN(CASE
                       WHEN B.PREF_ID = 1012
                            AND (B.CLNT_CONSENT_TYP=5002
                                 OR (B.CLNT_CONSENT_TYP=5003
                                     AND B.EMP_ID NOT IN (999999999999999, 999999999)
                                     AND B.EMP_ID IS NOT NULL)) THEN 'Banking - E-Mail:FALSE, '
                       ELSE 'Banking - E-Mail:TRUE, '
                   END), MIN(CASE
                       WHEN B.PREF_ID = 1013
                            AND (B.CLNT_CONSENT_TYP=5002
                                 OR (B.CLNT_CONSENT_TYP=5003
                                     AND B.EMP_ID NOT IN (999999999999999, 999999999)
                                     AND B.EMP_ID IS NOT NULL)) THEN 'Banking - Face to Face:FALSE, '
                       ELSE 'Banking - Face to Face:TRUE, '
                   END), MIN(CASE
                       WHEN B.PREF_ID = 1037
                            AND (B.CLNT_CONSENT_TYP=5002
                                 OR (B.CLNT_CONSENT_TYP=5003
                                     AND B.EMP_ID NOT IN (999999999999999, 999999999)
                                     AND B.EMP_ID IS NOT NULL)) THEN 'Direct Investing - Direct Mail:FALSE, '
                       ELSE 'Direct Investing - Direct Mail:TRUE, '
                   END), MIN(CASE
                       WHEN B.PREF_ID = 1038
                            AND (B.CLNT_CONSENT_TYP=5002
                                 OR (B.CLNT_CONSENT_TYP=5003
                                     AND B.EMP_ID NOT IN (999999999999999, 999999999)
                                     AND B.EMP_ID IS NOT NULL)) THEN 'Direct Investing - Telephone:FALSE, '
                       ELSE 'Direct Investing - Telephone:TRUE, '
                   END), MIN(CASE
                       WHEN B.PREF_ID = 1039
                            AND (B.CLNT_CONSENT_TYP=5002
                                 OR (B.CLNT_CONSENT_TYP=5003
                                     AND B.EMP_ID NOT IN (999999999999999, 999999999)
                                     AND B.EMP_ID IS NOT NULL)) THEN 'Direct Investing - DI Online:FALSE, '
                       ELSE 'Direct Investing - DI Online:TRUE, '
                   END), MIN(CASE
                       WHEN B.PREF_ID = 1040
                            AND (B.CLNT_CONSENT_TYP=5002
                                 OR (B.CLNT_CONSENT_TYP=5003
                                     AND B.EMP_ID NOT IN (999999999999999, 999999999)
                                     AND B.EMP_ID IS NOT NULL)) THEN 'Direct Investing - E-Mail:FALSE, '
                       ELSE 'Direct Investing - E-Mail:TRUE, '
                   END), MIN(CASE
                       WHEN B.PREF_ID = 1041
                            AND (B.CLNT_CONSENT_TYP=5002
                                 OR (B.CLNT_CONSENT_TYP=5003
                                     AND B.EMP_ID NOT IN (999999999999999, 999999999)
                                     AND B.EMP_ID IS NOT NULL)) THEN 'Direct Investing - Face to Face:FALSE, '
                       ELSE 'Direct Investing - Face to Face:TRUE, '
                   END), MIN(CASE
                       WHEN B.PREF_ID = 1048
                            AND (B.CLNT_CONSENT_TYP=5002
                                 OR (B.CLNT_CONSENT_TYP=5003
                                     AND B.EMP_ID NOT IN (999999999999999, 999999999)
                                     AND B.EMP_ID IS NOT NULL)) THEN 'Banking - ATM:FALSE'
                       ELSE 'Banking - ATM:TRUE'
                   END), ']') AS CPC_CHANNELS_CD
FROM DDWV01.RB_CLNT_DLY AS A
LEFT JOIN DG6V01.CPC_CLNT_PREF_CHC AS B ON A.CLNT_NO = B.CLNT_NO
WHERE A.SNAP_DT = (select max(SNAP_DT) from DDWV01.RB_CLNT_DLY where SNAP_DT > date-5)
GROUP BY 1
"""

# %% [1] Run it - the exact query, full portfolio
df = edw_pd(SQL_VERBATIM)
print(f"{len(df):,} rows")
display(df.head(20))

# %% [2] Summary - contactable vs not, per channel, off the strings [1] pulled
CHANNELS = ["Banking - Direct Mail", "Banking - Telephone", "Banking - RBC Online",
            "Banking - E-Mail", "Banking - Face to Face",
            "Direct Investing - Direct Mail", "Direct Investing - Telephone",
            "Direct Investing - DI Online", "Direct Investing - E-Mail",
            "Direct Investing - Face to Face", "Banking - ATM"]
n = len(df)
rows = []
for ch in CHANNELS:
    n_false = int(df["CPC_CHANNELS_CD"].str.contains(f"{ch}:FALSE", regex=False).sum())
    rows.append({"channel": ch, "n_false_do_not_contact": n_false,
                 "n_true_contactable": n - n_false,
                 "pct_false": round(100.0 * n_false / n, 2)})
summary = pd.DataFrame(rows)
print(f"--- production CPC_CHANNELS_CD, {n:,} clients in snapshot ---")
display(summary)
