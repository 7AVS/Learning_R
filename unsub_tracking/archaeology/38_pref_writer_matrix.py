# %% [markdown]
# # 38 — probe: which system writes which preference
#
# One question: for every PREF_ID, how many writes to No (5002), by which APP_SYS_CD —
# both decoded. 18-month frame (>= 2025-02-01), DDWV01.CPC_RB_PREF, all preferences,
# all writer systems. (Drop the 5002 filter in the SQL to see all consent values.)

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

# %% [1] Decodes
# Preferences - from the Borealis production rule (channel names), the SFMC unsubscribe
# page list (LOB names), and the EDW dictionary/operational notes. '??' = no verified decode.
PREF_DESC = {
    1002: "?? no verified decode (tracked email-consent switch)",
    1004: "Accounts & Packages",
    1006: "Credit Cards",
    1007: "Banking - Direct Mail",
    1008: "Banking - Telephone",
    1009: "Banking - RBC Online",
    1010: "Creditor Insurance",
    1012: "Banking - E-Mail (CASL)",
    1013: "Banking - Face to Face",
    1014: "Share-for-Marketing (CIDM audience selector; dict: cross-entity sharing)",
    1015: "?? no verified decode (blank=No family, per dictionary)",
    1023: "Investments - Registered",
    1024: "Investments - Non-Registered",
    1025: "Loans & Lines of Credit",
    1026: "Mortgages",
    1037: "Direct Investing - Direct Mail",
    1038: "Direct Investing - Telephone",
    1039: "Direct Investing - DI Online",
    1040: "Direct Investing - E-Mail",
    1041: "Direct Investing - Face to Face",
    1044: "Travel Health Insurance",
    1045: "E-Newsletter - Banking",
    1046: "E-Newsletter - Rewards",
    1048: "Banking - ATM",
}
# Writer systems - schemas/cpc_rb_pref_log_schema.md (official dictionary 2026-08-13)
SYS_DESC = {
    7001: "Sales Platform (branch/service delivery staff)", 7002: "DI Client Source",
    7003: "Royal Direct / Client View (contact centre)", 7004: "Online Banking",
    7005: "Service Platform", 7006: "RBC Banking (STaR UI, batch/purge)",
    7007: "RBC Express", 7008: "DS Client Source", 7009: "BridgeTrack", 7010: "CASPER",
    7012: "Retail Banking Investment System F200", 7013: "Retail Banking Investment System 5G10",
    7014: "Term Investment System 4V00", 7015: "SAP / RCT-LINX desktop", 7016: "RBC.COM",
    7017: "D&H/AMIA/CMG (telemarketer)", 7018: "CART", 7019: "IRIS",
    7020: "Exact Target (email ESP)", 7021: "TSYS", 7022: "RD Fulfillment",
    7023: "Assisted Multi Product Application", 7024: "VOX (telemarketing vendor)",
    7025: "ZEDD telemarketing / CASL Tool (context-dep.)", 7026: "APAC (telemarketing vendor)",
    7027: "D&H", 7028: "CPC-CA (MCA)", 7029: "RCL TPA",
    7030: "GISP (WM) / ADHOC Data Source (context-dep.)", 7999: "Default Application System",
    99999: "batch update (SRF consolidation)",
}

# %% [2] PREF_ID x writer system - writes to No, decoded, sorted by volume
px = edw_pd("""
SELECT PREF_ID, APP_SYS_CD, COUNT(*) AS n_writes_to_no
FROM DDWV01.CPC_RB_PREF
WHERE CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2025-02-01'
GROUP BY 1, 2
ORDER BY 3 DESC
""")
px.insert(1, "preference", [PREF_DESC.get(p, "?? not in decode list") for p in px["PREF_ID"]])
px.insert(3, "written_by", [SYS_DESC.get(s, "?? not in dictionary") for s in px["APP_SYS_CD"]])
px["share_pct"] = (px["n_writes_to_no"] / px["n_writes_to_no"].sum() * 100).round(2)
pd.set_option("display.max_rows", 300)
display(px)

# %% [3] Same data as a matrix - preferences down, top-10 writers across, rest pooled
top_writers = px.groupby("APP_SYS_CD")["n_writes_to_no"].sum().nlargest(10).index
px["writer_col"] = [f"{s} {SYS_DESC.get(s, '??')[:22]}" if s in top_writers else "OTHER"
                    for s in px["APP_SYS_CD"]]
px["pref_row"] = [f"{p} {PREF_DESC.get(p, '??')[:30]}" for p in px["PREF_ID"]]
mat = px.pivot_table(index="pref_row", columns="writer_col", values="n_writes_to_no",
                     aggfunc="sum", fill_value=0)
mat["TOTAL"] = mat.sum(axis=1)
mat = mat.sort_values("TOTAL", ascending=False)
mat.loc["TOTAL"] = mat.sum(axis=0)
display(mat)
