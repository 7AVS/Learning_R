# %% [markdown]
# # 39 — when one gate closes, do others follow? (bundles vs cascade)
#
# Two hypotheses, both open:
# - **Bundle (no cascade):** one writer writes many gates in the SAME instant. Nothing
#   propagates - it was one action.
# - **Cascade:** closing one gate TRIGGERS other gates to close AFTER it (seconds to
#   days later, possibly by a different system). Implies a hierarchy and a starter gate.
#
# Distinguishing signal = time + writer. Same instant + same writer = bundle. Lagged
# follow-up, especially machine-written, = cascade; whichever gate is consistently
# FIRST is the starter. No anchor gate is privileged - every gate is tested as a
# potential starter.
#
# Frame: 18 months (>= 2025-02-01), switches to No (5002), DDWV01.CPC_RB_PREF.
# Current-state caveat: only the LAST write per (client, gate) survives, so both
# bundles and cascades are FLOORS - anything later overwritten is invisible.

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

# %% [D] Decodes used by every cell below
PREF_DESC = {
    1002: "Entity Level Marketing - RBC",
    1004: "Accounts & Packages", 1006: "Credit Cards",
    1007: "Banking - Direct Mail", 1008: "Banking - Telephone",
    1009: "Banking - RBC Online", 1010: "Creditor Insurance",
    1012: "Banking - E-Mail (CASL)", 1013: "Banking - Face to Face",
    1014: "Info Share for Marketing - Banking (CIDM audience selector)",
    1015: "Info Share for Marketing - Service (MASTER, closes first)", 1016: "Entity Level Marketing - Credit Bureau",
    1020: "?? no verified decode", 1021: "?? no verified decode",
    1022: "?? no verified decode", 1023: "Investments - Registered",
    1024: "Investments - Non-Registered", 1025: "Loans & Lines of Credit",
    1026: "Mortgages", 1027: "?? no verified decode", 1028: "?? no verified decode",
    1030: "?? no verified decode", 1031: "?? no verified decode",
    1034: "?? no verified decode", 1036: "Info Share for Marketing - Online Personalization (MASTER, closes first)",
    1042: "?? no verified decode", 1044: "Travel Health Insurance",
    1045: "E-Newsletter - Banking", 1046: "E-Newsletter - Rewards", 1048: "Banking - ATM",
}
SYS_DESC = {
    7001: "Sales Platform (branch staff)", 7002: "DI Client Source",
    7003: "Royal Direct / Client View (contact centre)", 7004: "Online Banking",
    7005: "Service Platform", 7006: "RBC Banking (STaR UI, batch/purge)",
    7007: "RBC Express", 7008: "DS Client Source", 7009: "BridgeTrack", 7010: "CASPER",
    7012: "Retail Banking Investment System F200", 7013: "Retail Banking Investment System 5G10",
    7014: "Term Investment System 4V00", 7015: "SAP / RCT-LINX desktop", 7016: "RBC.COM",
    7017: "D&H/AMIA/CMG (telemarketer)", 7018: "CART", 7019: "IRIS",
    7020: "Exact Target (email ESP - the unsubscribe page)", 7021: "TSYS",
    7022: "RD Fulfillment", 7023: "Assisted Multi Product Application",
    7024: "VOX (telemarketing vendor)", 7025: "ZEDD telemarketing / CASL Tool",
    7026: "APAC (telemarketing vendor)", 7027: "D&H", 7028: "CPC-CA (MCA)", 7029: "RCL TPA",
    7030: "GISP (WM) / ADHOC Data Source", 7033: "?? NOT in dictionary",
    7053: "?? NOT in dictionary", 7999: "Default Application System",
    99999: "batch update (SRF consolidation)",
}

# %% [1] Five real examples - one client's rows, so bundles and lags are visible on screen
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

# %% [4] THE CASCADE TEST - after one gate closes, does another close LATER?
# Every gate is a candidate starter. For each client: take each switch-to-No write
# (the anchor), and look for OTHER gates written to No AFTER it, up to 7 days later.
# Same instant is EXCLUDED here - that is the bundle, already measured in [2]/[3].
# A cascade shows as volume at short lags; hierarchy = who is consistently first.
casc = edw_pd("""
SELECT a.PREF_ID  AS gate_closed_first,
       b.PREF_ID  AS gate_closed_after,
       CASE WHEN (b.CHG_TMSTMP - a.CHG_TMSTMP) DAY(4) TO SECOND <= INTERVAL '0 00:01:00' DAY TO SECOND
                 THEN '1_within_a_minute'
            WHEN (b.CHG_TMSTMP - a.CHG_TMSTMP) DAY(4) TO SECOND <= INTERVAL '0 01:00:00' DAY TO SECOND
                 THEN '2_within_an_hour'
            WHEN CAST(b.CHG_TMSTMP AS DATE) = CAST(a.CHG_TMSTMP AS DATE)
                 THEN '3_same_day'
            WHEN CAST(b.CHG_TMSTMP AS DATE) = CAST(a.CHG_TMSTMP AS DATE) + 1
                 THEN '4_next_day'
            ELSE      '5_within_a_week' END AS how_long_after,
       b.APP_SYS_CD AS follow_up_written_by,
       COUNT(*)     AS n_clients
FROM DDWV01.CPC_RB_PREF a
JOIN DDWV01.CPC_RB_PREF b
  ON  b.CLNT_NO = a.CLNT_NO
  AND b.PREF_ID <> a.PREF_ID
  AND b.CLNT_CONSENT_TYP = 5002
  AND b.CHG_TMSTMP > a.CHG_TMSTMP                              -- strictly AFTER: bundles excluded
  AND CAST(b.CHG_TMSTMP AS DATE) <= CAST(a.CHG_TMSTMP AS DATE) + 7
WHERE a.CLNT_CONSENT_TYP = 5002
  AND a.CHG_TMSTMP >= DATE '2025-02-01'
GROUP BY 1, 2, 3, 4
""")
casc["first_gate"]  = [f"{p} = {PREF_DESC.get(p, '??')}" for p in casc["gate_closed_first"]]
casc["later_gate"]  = [f"{p} = {PREF_DESC.get(p, '??')}" for p in casc["gate_closed_after"]]
casc["written_by"]  = [f"{s} = {SYS_DESC.get(s, '??')}" for s in casc["follow_up_written_by"]]

print("TABLE 1 - is there ANY cascade? Follow-up closings by lag (all gate pairs pooled):")
t1 = casc.groupby("how_long_after", as_index=False)["n_clients"].sum()
t1["pct"] = (t1["n_clients"] / t1["n_clients"].sum() * 100).round(1)
display(t1)

print("TABLE 2 - the top starter->follower pairs at lags of ONE HOUR OR LESS")
print("(machine-speed follow-ups; a hierarchy would live here):")
fast = casc[casc["how_long_after"].isin(["1_within_a_minute", "2_within_an_hour"])]
t2 = (fast.groupby(["first_gate", "later_gate", "written_by"], as_index=False)["n_clients"].sum()
          .sort_values("n_clients", ascending=False))
display(t2.head(20))

print("TABLE 3 - who is consistently FIRST (starter) vs LATER (receiver), fast lags only:")
starters  = fast.groupby("first_gate", as_index=False)["n_clients"].sum().rename(
    columns={"first_gate": "gate", "n_clients": "n_times_it_closed_FIRST"})
receivers = fast.groupby("later_gate", as_index=False)["n_clients"].sum().rename(
    columns={"later_gate": "gate", "n_clients": "n_times_it_closed_AFTER"})
t3 = starters.merge(receivers, on="gate", how="outer").fillna(0)
t3["starter_ratio"] = (t3["n_times_it_closed_FIRST"] /
                       (t3["n_times_it_closed_FIRST"] + t3["n_times_it_closed_AFTER"]).clip(lower=1)).round(2)
print("starter_ratio near 1 = this gate starts cascades; near 0 = it receives them:")
display(t3.sort_values("n_times_it_closed_FIRST", ascending=False).head(25))
