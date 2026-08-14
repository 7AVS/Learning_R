# %% [markdown]
# # 37 — probe: what does the ESP (APP_SYS_CD 7020) actually write into CPC?
#
# Standalone, two questions, 18-month frame (>= 2025-02-01), all off DDWV01.CPC_RB_PREF:
#
# 1. **Footprint** — which PREF_IDs does 7020 write to No? The SFMC blueprint
#    (`unsub_tracking/sfmc_unsub_blueprint_notes.md`) says the unsubscribe page can only
#    write 1012 + ONE LOB code from a closed list. Any other code with volume = an
#    undocumented 7020 path.
# 2. **Fingerprint** — what hour do 7020 writes land? Documented pipe is one automation:
#    FTP backfeed 3:30 AM Mon-Sat, 9:00 AM Sunday. Spikes at those hours = the page is
#    the sole source. Volume elsewhere = a second Exact Target pipe (e.g. the 2:00 AM
#    prospect backfeed) — or CHG_TMSTMP carries client-action time, not batch time.
#
# Descriptive only.

# %% [0] connect + proof round-trip
try:
    import teradatasql
except ImportError:
    get_ipython().system("pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")
    import teradatasql
import getpass
import pandas as pd
import matplotlib.pyplot as plt

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

# %% [1] Footprint - every PREF_ID that 7020 wrote to No, vs the documented page list
ACCEPTED = {1012: "Banking - Email (CASL, mandatory radio)", 1004: "Accounts & Packages",
            1006: "Credit Cards", 1010: "Creditor Insurance", 1023: "Investments - Registered",
            1024: "Investments - Non-Registered", 1025: "Loans & Lines of Credit",
            1026: "Mortgages", 1044: "Travel Health Insurance", 1045: "E-Newsletter - Banking",
            1046: "E-Newsletter - Rewards", 1002: "(old form list only)"}
fp = edw_pd("""
SELECT PREF_ID, COUNT(*) AS n_writes_to_no
FROM DDWV01.CPC_RB_PREF
WHERE APP_SYS_CD = 7020
  AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2025-02-01'
GROUP BY 1 ORDER BY 2 DESC
""")
fp.insert(1, "on_unsub_page", [ACCEPTED.get(p, "?? NOT on the documented page") for p in fp["PREF_ID"]])
fp["share_pct"] = (fp["n_writes_to_no"] / fp["n_writes_to_no"].sum() * 100).round(1)
display(fp)

# %% [2] Fingerprint - hour-of-day of 7020 writes, Sunday split from Mon-Sat
hr = edw_pd("""
SELECT EXTRACT(HOUR FROM CHG_TMSTMP)                                    AS hr,
       SUM(CASE WHEN TD_DAY_OF_WEEK(CAST(CHG_TMSTMP AS DATE)) = 1
                THEN 1 ELSE 0 END)                                      AS n_sunday,
       SUM(CASE WHEN TD_DAY_OF_WEEK(CAST(CHG_TMSTMP AS DATE)) <> 1
                THEN 1 ELSE 0 END)                                      AS n_mon_sat
FROM DDWV01.CPC_RB_PREF
WHERE APP_SYS_CD = 7020
  AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2025-02-01'
GROUP BY 1 ORDER BY 1
""")
display(hr)
fig, ax = plt.subplots(figsize=(11, 4))
ax.bar(hr["hr"] - 0.2, hr["n_mon_sat"], width=0.4, label="Mon-Sat (documented batch 3:30 AM)", color="#2a78d6")
ax.bar(hr["hr"] + 0.2, hr["n_sunday"], width=0.4, label="Sunday (documented batch 9:00 AM)", color="#e08214")
ax.set_xlabel("hour of CHG_TMSTMP"); ax.set_ylabel("writes")
ax.set_xticks(range(0, 24))
ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v)}"))
ax.set_title("7020 writes by hour - does everything land at the documented batch times?",
             fontweight="bold")
ax.legend(); ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()
