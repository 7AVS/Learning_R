# %% [markdown]
# # 39 — do consent switches get changed TOGETHER, or one at a time?
#
# A **change moment** = all the preference rows one client got written at the exact same
# timestamp. One moment = one action (a branch visit, a phone call, an unsubscribe click).
# If a moment touches 20 switches, that was one "opt out of everything" action — not 20
# separate decisions. 18-month frame, switches set to No (5002), DDWV01.CPC_RB_PREF.
# Current-state caveat: later re-writes shrink old moments, so sizes are floors.

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

# %% [D] Decodes used by every cell below
PREF_DESC = {
    1002: "?? email-consent switch, no verified decode",
    1004: "Accounts & Packages", 1006: "Credit Cards",
    1007: "Banking - Direct Mail", 1008: "Banking - Telephone",
    1009: "Banking - RBC Online", 1010: "Creditor Insurance",
    1012: "Banking - E-Mail (CASL)", 1013: "Banking - Face to Face",
    1014: "Share-for-Marketing (CIDM audience selector)",
    1016: "?? no verified decode", 1020: "?? no verified decode",
    1021: "?? no verified decode", 1022: "?? no verified decode",
    1023: "Investments - Registered", 1024: "Investments - Non-Registered",
    1025: "Loans & Lines of Credit", 1026: "Mortgages",
    1027: "?? no verified decode", 1028: "?? no verified decode",
    1030: "?? no verified decode", 1031: "?? no verified decode",
    1034: "?? no verified decode", 1042: "?? no verified decode",
    1044: "Travel Health Insurance", 1045: "E-Newsletter - Banking",
    1046: "E-Newsletter - Rewards", 1048: "Banking - ATM",
}
SYS_DESC = {
    7001: "Sales Platform (branch staff)", 7002: "DI Client Source",
    7003: "Royal Direct / Client View (contact centre)", 7004: "Online Banking",
    7005: "Service Platform", 7006: "RBC Banking (STaR UI, batch/purge)",
    7016: "RBC.COM", 7020: "Exact Target (email ESP - the unsubscribe page)",
    7027: "D&H", 7033: "?? NOT in dictionary", 7053: "?? NOT in dictionary",
    7999: "Default Application System", 99999: "batch update (SRF consolidation)",
}

# %% [1] Five real examples - one client's rows, so a change moment is visible on screen
print("Below: every switch-to-No row for 5 clients who switched off 1012.")
print("Rows sharing one CHG_TMSTMP = written by ONE action. Different timestamps = separate actions.")
ex = edw_pd("""
SELECT p.CLNT_NO, p.CHG_TMSTMP, p.PREF_ID, p.APP_SYS_CD
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
""")
ex["switch"] = [f"{p} = {PREF_DESC.get(p, '??')}" for p in ex["PREF_ID"]]
ex["written_by"] = [f"{s} = {SYS_DESC.get(s, '?? not in dictionary')}" for s in ex["APP_SYS_CD"]]
display(ex[["CLNT_NO", "CHG_TMSTMP", "switch", "written_by"]])

# %% [2] One action changes HOW MANY switches?
bs = edw_pd("""
SELECT bundle_size, COUNT(*) AS n_moments
FROM (
    SELECT CLNT_NO, CHG_TMSTMP, COUNT(*) AS bundle_size
    FROM DDWV01.CPC_RB_PREF
    WHERE CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-02-01'
    GROUP BY 1, 2
) t
GROUP BY 1
""")
def size_label(n):
    if n == 1:  return "1 switch - one specific choice"
    if n == 2:  return "2 switches - e.g. the unsub page's two radios"
    if n <= 13: return "3-13 switches - partial bundles"
    if n <= 18: return "14-18 switches - most of the book"
    if n <= 21: return "19-21 switches - OPT OUT OF EVERYTHING"
    return "22+ switches"
bs["what_one_action_changed"] = [size_label(n) for n in bs["bundle_size"]]
out = bs.groupby("what_one_action_changed", as_index=False)["n_moments"].sum()
out["pct_of_all_actions"] = (out["n_moments"] / out["n_moments"].sum() * 100).round(1)
out = out.sort_values("n_moments", ascending=False).rename(
    columns={"n_moments": "n_actions_18mo"})
print("Each row = client-actions in 18 months, grouped by how many switches the ONE action set to No:")
display(out)

# %% [3] WHO changes one switch, WHO flips everything
bw = edw_pd("""
SELECT APP_SYS_CD, bundle_size, COUNT(*) AS n_moments
FROM (
    SELECT CLNT_NO, CHG_TMSTMP, APP_SYS_CD, COUNT(*) AS bundle_size
    FROM DDWV01.CPC_RB_PREF
    WHERE CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-02-01'
    GROUP BY 1, 2, 3
) t
GROUP BY 1, 2
""")
bw["system"] = [f"{s} = {SYS_DESC.get(s, '?? not in dictionary')}" for s in bw["APP_SYS_CD"]]
bw["action_type"] = ["changed 1 switch" if n == 1 else
                     "changed 2 switches" if n == 2 else
                     "changed 3-10" if n <= 10 else
                     "changed 11+ (mass opt-out)" for n in bw["bundle_size"]]
pv = bw.pivot_table(index="system", columns="action_type", values="n_moments",
                    aggfunc="sum", fill_value=0)
pv["TOTAL actions"] = pv.sum(axis=1)
pv["% mass opt-out"] = (100 * pv.get("changed 11+ (mass opt-out)", 0) / pv["TOTAL actions"]).round(1)
print("Each row = one writing system; columns = what its actions typically did:")
display(pv.sort_values("TOTAL actions", ascending=False).head(12))

# %% [4] When 1012 (email consent) goes to No - what ELSE the same action switched off
n_1012 = edw_pd("""
SELECT COUNT(*) AS n FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002 AND CHG_TMSTMP >= DATE '2025-02-01'
""")["n"].iloc[0]
co = edw_pd("""
SELECT p2.PREF_ID AS co_pref, COUNT(*) AS n_together
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
co.insert(1, "which_is", [PREF_DESC.get(p, "?? not in decode list") for p in co["co_pref"]])
co = co.rename(columns={"co_pref": "switch_also_set_to_No",
                        "n_together": "n_times_in_same_action"})
co["pct_of_all_1012_switch_offs"] = (100.0 * co["n_times_in_same_action"] / n_1012).round(1)
print(f"1012 (Banking E-Mail) was switched off {n_1012:,} times in 18 months.")
print("Each row = another switch, and how often the SAME action turned it off too.")
print("High % = 1012 usually falls as part of a bulk opt-out, not alone:")
display(co.head(25))
