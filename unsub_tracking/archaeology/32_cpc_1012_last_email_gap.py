# %% [markdown]
# # 32 v2 — CPC 1012 flips (from CPC_RB_PREF) → last email decision before the flip
#
# **v2 (2026-08-13): rebuilt on DDWV01.CPC_RB_PREF (current state).** Pack 33 EDA showed
# CPC_RB_PREF_LOG captures ~1% of 1012 No's (34,150 ever vs 3,258,923 standing) — every
# v1 volume was wrong. Here a "flip" = a client whose CURRENT standing is 1012=No, dated
# by CHG_TMSTMP (when that standing was last set). Known limit: clients who flipped No
# and later re-consented are invisible here (cross-check = MTHLY month-pair transitions).
#
# **Descriptive only — no claims.** Nearest prior email DECISION record (not delivered
# email) per flip. Emails are frequent, so nearby decisions are expected by chance;
# nothing here attributes any flip to any email, in either direction.
#
# All outputs inline. No files written. Connection = same as unsub_unified.

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

TD_HOST = "Teradata-dns-sysa.fg.rbc.com"
EDW = teradatasql.connect(host=TD_HOST, user=username, password=password, logmech="LDAP")

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

# %% [C] WINDOWS - one convention, used by every cell below
WIN_FLOOR  = "2025-02-01"   # flips window: last 18 months, pinned
LOOK_FLOOR = "2024-02-01"   # history lookback: 12 months BEFORE the window opens,
                            # so early-window flips have real lookback and
                            # 'no_*_found' means none in 12-30 months, not an artifact

# %% [P1] PROBE — 1012 consent mix on CPC_RB_PREF (expect ~3.26M at No, per pack 33)
display(edw_pd("""
SELECT CLNT_CONSENT_TYP, CAST(COUNT(*) AS BIGINT) AS n_clients
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012
GROUP BY 1 ORDER BY 2 DESC
"""))

# %% [P2] PROBE — raw rows: 10 clients standing at 1012=No, all columns
pd.set_option("display.max_colwidth", 120)
display(edw_pd("""
SELECT *
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
ORDER BY CHG_TMSTMP DESC
SAMPLE 10
"""))

# %% [P3] PROBE — the attribution, raw: 20 matched flip↔last-email pairs on screen
# One row = one client: when their standing became No, the closest email decision
# before it, and the gap. This is the join everything below aggregates.
display(edw_pd("""
WITH flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012                 -- email consent switch
      AND CLNT_CONSENT_TYP = 5002        -- standing = No
      AND CHG_TMSTMP >= DATE '2025-02-01'   -- pinned window
),
last_email AS (
    SELECT f.CLNT_NO, f.flip_dt, t.TREATMT_STRT_DT AS email_dt,
           SUBSTR(t.TACTIC_ID, 8, 3) AS mne, t.TACTIC_ID
    FROM flips f
    JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
      ON  t.CLNT_NO = f.CLNT_NO
      AND t.TREATMT_STRT_DT <= f.flip_dt           -- only emails BEFORE the flip
      AND t.TREATMT_STRT_DT >= DATE '2024-02-01'   -- lookback: 12mo before window
      AND ( SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
            OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
    -- one row per client: the email CLOSEST to the flip
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY t.TREATMT_STRT_DT DESC) = 1
)
SELECT CLNT_NO, flip_dt, email_dt, flip_dt - email_dt AS gap_days, mne, TACTIC_ID
FROM last_email
SAMPLE 20
"""))

# %% [1] Q0 — monthly arrivals into No (the volume story, now from the right table)
SQL_TOTALS = """
SELECT TRIM(EXTRACT(YEAR FROM CAST(CHG_TMSTMP AS DATE))) || '-' ||
         TRIM(CASE WHEN EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)) < 10 THEN '0' ELSE '' END) ||
         TRIM(EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)))  AS chg_month,
       COUNT(*)                                              AS n_clients
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2025-02-01'   -- 18-month window, pinned
GROUP BY 1
ORDER BY 1
"""
tot = edw_pd(SQL_TOTALS)
display(tot)

fig, ax = plt.subplots(figsize=(12, 4.5))
ax.bar(tot["chg_month"], tot["n_clients"], color="#2a78d6")
for x, v in zip(tot["chg_month"], tot["n_clients"]):
    ax.text(x, v, f"{int(v):,}", ha="center", va="bottom", fontsize=7.5, rotation=90)
ax.set_ylabel("clients")
ax.set_title(f"1012 standing became No, by month of last change (CPC_RB_PREF) · 18mo total: {tot['n_clients'].sum():,}",
             fontweight="bold")
ax.tick_params(axis="x", rotation=45)
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# %% [2] Q1 — flip_month x gap_bucket (flips pinned >= 2025-02-01)
SQL_BUCKETS = """
WITH flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012
      AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-02-01'   -- pinned window (reruns reproduce exactly)
),
last_email AS (
    SELECT f.CLNT_NO, f.flip_dt, t.TREATMT_STRT_DT AS email_dt
    FROM flips f
    JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
      ON  t.CLNT_NO = f.CLNT_NO
      AND t.TREATMT_STRT_DT <= f.flip_dt
      AND t.TREATMT_STRT_DT >= DATE '2024-02-01'   -- lookback: 12mo before window
      AND ( SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
            OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY t.TREATMT_STRT_DT DESC) = 1
),
gapped AS (
    SELECT f.CLNT_NO,
           TRIM(EXTRACT(YEAR FROM f.flip_dt)) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM f.flip_dt) < 10 THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM f.flip_dt))              AS flip_month,
           f.flip_dt - em.email_dt                            AS gap_days
    FROM flips f
    LEFT JOIN last_email em ON em.CLNT_NO = f.CLNT_NO   -- LEFT: keep clients with no email (gap NULL)
)
SELECT flip_month,
       CASE WHEN gap_days IS NULL   THEN '6_no_email_found'
            WHEN gap_days <= 1      THEN '1_same_or_next_day'
            WHEN gap_days <= 7      THEN '2_within_week'
            WHEN gap_days <= 30     THEN '3_within_month'
            WHEN gap_days <= 90     THEN '4_within_quarter'
            ELSE                         '5_over_90_days' END AS gap_bucket,
       COUNT(*)                                               AS n_clients
FROM gapped
GROUP BY 1, 2
ORDER BY 1, 2
"""
bk = edw_pd(SQL_BUCKETS)
_piv = bk.pivot_table(index="flip_month", columns="gap_bucket",
                      values="n_clients", aggfunc="sum", fill_value=0)
_piv["TOTAL"] = _piv.sum(axis=1)
_piv.loc["TOTAL"] = _piv.sum(axis=0)
display(_piv)

# %% [3] rollup + meeting bar
roll = bk.groupby("gap_bucket", as_index=False)["n_clients"].sum()
roll["share_pct"] = (roll["n_clients"] / roll["n_clients"].sum() * 100).round(1)
# TOTAL row: all clients who flipped 1012 to No in the frame - the headline denominator
total_row = pd.DataFrame([{"gap_bucket": "TOTAL", "n_clients": roll["n_clients"].sum(),
                           "share_pct": 100.0}])
display(pd.concat([roll, total_row], ignore_index=True))

order = ["1_same_or_next_day", "2_within_week", "3_within_month",
         "4_within_quarter", "5_over_90_days", "6_no_email_found"]
labels = ["same/next day", "2-7 days", "8-30 days", "31-90 days", ">90 days", "no email found"]
r = roll.set_index("gap_bucket").reindex(order)
fig, ax = plt.subplots(figsize=(10, 5))
ax.barh(labels[::-1], r["n_clients"][::-1].fillna(0), color="#2a78d6")
for i, v in enumerate(r["n_clients"][::-1]):
    if pd.notna(v):
        ax.text(v, i, f" {int(v):,} ({r['share_pct'][::-1].iloc[i]}%)", va="center", fontsize=10)
ax.set_xlabel("clients")
ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax.set_title("Where the last email decision sits relative to the 1012 change (CPC_RB_PREF flips)",
             fontweight="bold")
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# %% [4] day-level gaps 0-90 + histogram
SQL_DAYS = """
WITH flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-02-01'
),
last_email AS (
    SELECT f.CLNT_NO, f.flip_dt, t.TREATMT_STRT_DT AS email_dt
    FROM flips f
    JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
      ON  t.CLNT_NO = f.CLNT_NO
      AND t.TREATMT_STRT_DT <= f.flip_dt
      AND t.TREATMT_STRT_DT >= DATE '2024-02-01'   -- lookback: 12mo before window
      AND ( SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
            OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY t.TREATMT_STRT_DT DESC) = 1
)
SELECT f.flip_dt - em.email_dt AS gap_days,
       COUNT(*)                AS n_clients
FROM flips f
JOIN last_email em ON em.CLNT_NO = f.CLNT_NO
WHERE f.flip_dt - em.email_dt <= 90
GROUP BY 1
ORDER BY 1
"""
dy = edw_pd(SQL_DAYS)
display(dy.head(15))

fig, ax = plt.subplots(figsize=(12, 5))
ax.bar(dy["gap_days"], dy["n_clients"], width=0.9, color="#2a78d6")
d01 = dy[dy["gap_days"] <= 1]["n_clients"].sum()
ax.annotate(f"day 0-1: {d01:,}", xy=(1, dy[dy["gap_days"] <= 1]["n_clients"].max() if d01 else 0),
            xytext=(8, dy["n_clients"].max() * 0.9), fontsize=11, fontweight="bold",
            arrowprops=dict(arrowstyle="->", color="#52514e"))
ax.set_xlabel("days from last email decision to the 1012 change")
ax.set_ylabel("clients")
ax.set_title("1012 standing became No (pinned window) — days since last email decision\n"
             "Descriptive timing only: nearest prior decision record; no attribution implied.",
             fontweight="bold")
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# %% [5] campaign (MNE) mix of the matched last email
SQL_MNE = """
WITH flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-02-01'
),
last_email AS (
    SELECT f.CLNT_NO, f.flip_dt, t.TREATMT_STRT_DT AS email_dt,
           SUBSTR(t.TACTIC_ID, 8, 3) AS mne
    FROM flips f
    JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
      ON  t.CLNT_NO = f.CLNT_NO
      AND t.TREATMT_STRT_DT <= f.flip_dt
      AND t.TREATMT_STRT_DT >= DATE '2024-02-01'   -- lookback: 12mo before window
      AND ( SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
            OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY t.TREATMT_STRT_DT DESC) = 1
)
SELECT em.mne,
       COUNT(*)                                                     AS n_clients,
       SUM(CASE WHEN f.flip_dt - em.email_dt <= 7 THEN 1 ELSE 0 END) AS n_within_week
FROM flips f
JOIN last_email em ON em.CLNT_NO = f.CLNT_NO
GROUP BY 1
ORDER BY 2 DESC
"""
mne = edw_pd(SQL_MNE)
display(mne.head(20))

# %% [6] writers of the standing No's (CPC_RB_PREF) — who set the current state
wr2 = edw_pd("""
SELECT APP_SYS_CD, CAST(COUNT(*) AS BIGINT) AS n_clients
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
GROUP BY 1 ORDER BY 2 DESC
""")
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
wr2["system"] = [SYS_DESC.get(c, "?? not in dictionary") for c in wr2["APP_SYS_CD"]]
wr2["share_pct"] = (wr2["n_clients"] / wr2["n_clients"].sum() * 100).round(1)
display(wr2)

# %% [7] the other side of the equation — distinct clients emailed per month (unchanged from v1)
SQL_EMAILED = """
SELECT TRIM(EXTRACT(YEAR FROM TREATMT_STRT_DT)) || '-' ||
         TRIM(CASE WHEN EXTRACT(MONTH FROM TREATMT_STRT_DT) < 10 THEN '0' ELSE '' END) ||
         TRIM(EXTRACT(MONTH FROM TREATMT_STRT_DT))  AS email_month,
       CAST(COUNT(*) AS BIGINT)  AS n_email_decisions,
       COUNT(DISTINCT CLNT_NO)   AS n_clients_emailed   -- one per client per month
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE TREATMT_STRT_DT >= DATE '2025-02-01'   -- pinned window, same frame as the flips
  AND ( SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
        OR UPPER(COALESCE(ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
GROUP BY 1
ORDER BY 1
"""
emd = edw_pd(SQL_EMAILED)

# unsub side of the same months - vendor feedback disposition 4. EVENT-only (no MASTER
# join, so it stays light): grain = email address (consumer_id_hashed), which runs
# ~1.1 addresses per client - close enough for a monthly rate, stated on the chart.
SQL_UNSUBS = """
SELECT TRIM(EXTRACT(YEAR FROM disposition_dt_tm)) || '-' ||
         TRIM(CASE WHEN EXTRACT(MONTH FROM disposition_dt_tm) < 10 THEN '0' ELSE '' END) ||
         TRIM(EXTRACT(MONTH FROM disposition_dt_tm))  AS email_month,
       COUNT(*)                            AS n_unsub_events,
       COUNT(DISTINCT consumer_id_hashed)  AS n_unsubscribers   -- one per address per month
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_cd = 4
  AND disposition_dt_tm >= DATE '2025-02-01'   -- same pinned window
GROUP BY 1
ORDER BY 1
"""
uns = edw_pd(SQL_UNSUBS)
emd = emd.merge(uns, on="email_month", how="left")
emd["unsub_rate_pct"] = (100.0 * emd["n_unsubscribers"] / emd["n_clients_emailed"]).round(3)
display(emd)   # email_month | decisions | clients emailed | unsub events | unsubscribers | rate

fig, ax = plt.subplots(figsize=(12, 4.5))
ax.bar(emd["email_month"], emd["n_clients_emailed"], color="#2a78d6")
for x, v in zip(emd["email_month"], emd["n_clients_emailed"]):
    ax.text(x, v, f"{v/1e6:.1f}M", ha="center", va="bottom", fontsize=8)
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v/1e6:.0f}M"))
ax.set_ylabel("distinct clients (millions)")
ax.set_title("Clients with at least one email decision, per month (each client counted once)",
             fontweight="bold")
ax.tick_params(axis="x", rotation=45)
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

fig, ax = plt.subplots(figsize=(12, 4.5))
ax.bar(emd["email_month"], emd["unsub_rate_pct"], color="#2a78d6")
for x, v in zip(emd["email_month"], emd["unsub_rate_pct"]):
    if pd.notna(v):
        ax.text(x, v, f"{v:.2f}%", ha="center", va="bottom", fontsize=8)
ax.set_ylabel("unsub rate (%)")
ax.set_title("Unsubscribers per client emailed, by month (deduplicated)\n"
             "unsub side = email-address grain (~1.1 addresses/client) - approximate rate",
             fontweight="bold")
ax.tick_params(axis="x", rotation=45)
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# %% [8] WIDER LENS — 1002 / 1012 / 1014: monthly arrivals into No (CPC_RB_PREF)
# Same pinned window. 1014 caveat: blank(5003) also means No on that switch - this
# counts explicit 5002 writes only, so 1014 is a floor.
sw = edw_pd("""
SELECT PREF_ID,
       TRIM(EXTRACT(YEAR FROM CAST(CHG_TMSTMP AS DATE))) || '-' ||
         TRIM(CASE WHEN EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)) < 10 THEN '0' ELSE '' END) ||
         TRIM(EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)))  AS chg_month,
       COUNT(*) AS n_clients
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID IN (1002, 1012, 1014)
  AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2025-02-01'
GROUP BY 1, 2
ORDER BY 1, 2
""")
_p = sw.pivot_table(index="chg_month", columns="PREF_ID", values="n_clients", aggfunc="sum", fill_value=0)
_p["TOTAL"] = _p.sum(axis=1)
display(_p)

fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
for ax, pid, lbl in zip(axes, [1002, 1012, 1014],
                        ["1002 (entity Do-Not-Solicit)", "1012 (Banking E-Mail consent)",
                         "1014 (Share for Marketing - explicit No only, blank also = No)"]):
    d = sw[sw["PREF_ID"] == pid]
    ax.bar(d["chg_month"], d["n_clients"], color="#2a78d6")
    ax.set_title(f"{lbl} — arrivals into No per month · total {d['n_clients'].sum():,}",
                 fontweight="bold", fontsize=11, loc="left")
    ax.set_ylabel("clients")
    ax.spines[["top", "right"]].set_visible(False)
axes[-1].tick_params(axis="x", rotation=45)
plt.tight_layout(); plt.show()

# %% [9] WIDER LENS — writers of the standing No's, per switch
wr3 = edw_pd("""
SELECT PREF_ID, APP_SYS_CD, CAST(COUNT(*) AS BIGINT) AS n_clients
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID IN (1002, 1012, 1014) AND CLNT_CONSENT_TYP = 5002
GROUP BY 1, 2
""")
for pid in [1002, 1012, 1014]:
    print(f"--- PREF_ID {pid}: writers of the standing No's ---")
    p = wr3[wr3["PREF_ID"] == pid][["APP_SYS_CD", "n_clients"]].sort_values("n_clients", ascending=False).copy()
    p["system"] = [SYS_DESC.get(c, "?? not in dictionary") for c in p["APP_SYS_CD"]]
    p["share_pct"] = (p["n_clients"] / p["n_clients"].sum() * 100).round(1)
    display(p.head(12))
