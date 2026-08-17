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

# %% [8] The Borealis PRODUCTS rule (cpc_products_cd.sql), cloned - standing view
# Production builds a per-client product-consent string off CPC_CLNT_PREF_CHC with the
# same rule as the channels one: FALSE (do not contact for this product) when 5002, or
# 5003 + real EMP_ID; else TRUE. This cell decomposes their view: per product switch,
# who is FALSE and why, who is TRUE. Their "product attribution" as standing counts.
PROD_DESC = {
    1004: "Accounts and Packages", 1006: "Credit Cards", 1010: "Creditor Insurance",
    1020: "Call for Registered GIC maturity", 1021: "Call for Non-Registered GIC maturity",
    1023: "Investments - Registered", 1024: "Investments - Non-Registered",
    1025: "Loans and Lines of Credit", 1026: "Mortgages", 1027: "Business Deposit Accounts",
    1028: "Creditor Insurance BLIP", 1030: "Cash Management Services", 1031: "Leasing",
    1034: "Client Card", 1044: "Travel Health Insurance",
}
pr = edw_pd("""
SELECT PREF_ID,
       CASE WHEN CLNT_CONSENT_TYP = 5002                            THEN 'FALSE - explicit No (5002)'
            WHEN CLNT_CONSENT_TYP = 5003
                 AND EMP_ID IS NOT NULL
                 AND EMP_ID NOT IN (999999999999999, 999999999)     THEN 'FALSE - blank + real EMP_ID'
            WHEN CLNT_CONSENT_TYP = 5003                            THEN 'TRUE - blank (contactable default)'
            ELSE                                                         'TRUE - other consent value' END AS borealis_reading,
       COUNT(DISTINCT CLNT_NO) AS n_clients
FROM DG6V01.CPC_CLNT_PREF_CHC
WHERE PREF_ID IN (1004,1006,1010,1020,1021,1023,1024,1025,1026,1027,1028,1030,1031,1034,1044)
GROUP BY 1, 2
""")
pr.insert(1, "product_switch", [PROD_DESC.get(p, "??") for p in pr["PREF_ID"]])
pv8 = pr.pivot_table(index=["PREF_ID", "product_switch"], columns="borealis_reading",
                     values="n_clients", aggfunc="sum", fill_value=0)
pv8["TOTAL rows"] = pv8.sum(axis=1)
print("Per product switch: how production's rule reads the standing book (clients per bucket).")
print("Rule: FALSE = do-not-contact for that product; blank = contactable unless employee:")
display(pv8)

# %% [9] Product switches - FLOW: monthly writes to No (who actually opts out per product)
# Flow from the mirror table (CPC_RB_PREF - proven equivalent in [7], has the write
# timestamp). Direct product opt-outs are mostly branch bundles (pack 39), so read
# these volumes as consent erosion per product, NOT product-specific client choices.
fl = edw_pd("""
SELECT TRIM(EXTRACT(YEAR FROM CAST(CHG_TMSTMP AS DATE))) || '-' ||
         TRIM(CASE WHEN EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)) < 10 THEN '0' ELSE '' END) ||
         TRIM(EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)))  AS chg_month,
       PREF_ID,
       COUNT(*) AS n_writes_to_no
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID IN (1004,1006,1010,1020,1021,1023,1024,1025,1026,1027,1028,1030,1031,1034,1044)
  AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2025-02-01'
GROUP BY 1, 2
""")
fl["product_switch"] = [f"{p} {PROD_DESC.get(p, '??')}" for p in fl["PREF_ID"]]
pv9 = fl.pivot_table(index="chg_month", columns="product_switch",
                     values="n_writes_to_no", aggfunc="sum", fill_value=0)
pv9["TOTAL"] = pv9.sum(axis=1)
pv9.loc["TOTAL"] = pv9.sum(axis=0)
print("Monthly switch-offs per product preference, 18-month frame:")
display(pv9)
