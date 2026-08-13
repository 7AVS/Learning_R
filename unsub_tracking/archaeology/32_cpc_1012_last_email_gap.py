# %% [markdown]
# # 32 — CPC 1012 flips → last email decision before the flip
#
# **Question (skeptics' framing, inverted):** take every client whose most recent
# Banking E-Mail consent (PREF_ID 1012) changed to **No** in the past 18 months.
# For each, find the **last email decision** in tactic history **before** that change.
# How close are they?
#
# **Descriptive only — no claims.** This measures the proximity of the nearest
# prior email DECISION record (not delivered email) to each flip. Emails are
# frequent, so nearby decisions are expected by chance; nothing here attributes
# any flip to any email, in either direction.
#
# All outputs inline. No files written. Connection = same as unsub_unified.

# %% [0] connect (same idiom as unsub_unified) + proof round-trip
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
    # cursor-based (pack 01 idiom): same DataFrame, no pandas DBAPI warning
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

# %% [P1] PROBE — what does PREF_ID actually contain? (type/format trap)
# Expect: 1012 present with material volume. If 1012 is missing but a padded/char
# variant appears, our numeric filter is silently matching nothing.
display(edw_pd("""
SELECT PREF_ID, COUNT(*) AS n_rows, COUNT(DISTINCT CLNT_NO) AS n_clients,
       MIN(CAST(CHG_TMSTMP AS DATE)) AS first_dt, MAX(CAST(CHG_TMSTMP AS DATE)) AS last_dt
FROM DDWV01.CPC_RB_PREF_LOG
WHERE CHG_TMSTMP >= DATE '2024-01-01'
GROUP BY 1 ORDER BY 2 DESC
"""))

# %% [P2] PROBE — consent-type mix for 1012 specifically
# Expect: 5001/5002/5003 style codes. If "No" lives under a different code than 5002,
# our volume is wrong. Blank(5003)=Yes for 1012, so 5002 is the only "No" we want.
display(edw_pd("""
SELECT CLNT_CONSENT_TYP, COUNT(*) AS n_rows, COUNT(DISTINCT CLNT_NO) AS n_clients
FROM DDWV01.CPC_RB_PREF_LOG
WHERE PREF_ID = 1012 AND CHG_TMSTMP >= DATE '2024-01-01'
GROUP BY 1 ORDER BY 2 DESC
"""))

# %% [P3] PROBE — raw eyeball: 10 actual 1012->5002 rows, no transformation
# Look at CHG_TMSTMP format, CLNT_NO length/leading zeros, APP_SYS_CD (who writes these).
display(edw_pd("""
SELECT TOP 10 CLNT_NO, PREF_ID, CLNT_CONSENT_TYP, CHG_TMSTMP, APP_SYS_CD, SYS_FUNC_CD
FROM DDWV01.CPC_RB_PREF_LOG
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= ADD_MONTHS(CURRENT_DATE, -18)
ORDER BY CHG_TMSTMP DESC
"""))

# %% [P4] PROBE — join-key integrity: do flip clients exist in the tactic table AT ALL?
# The killer check. If CLNT_NO formats mismatch (char vs numeric, padding), the join
# silently returns nothing and "no email found" is an artifact.
# Expect: pct_any_tactic HIGH (most bank clients get decisioned for something).
# If it is near zero -> formats differ -> STOP, fix the join before trusting anything.
display(edw_pd("""
WITH flips AS (
    SELECT CLNT_NO
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= ADD_MONTHS(CURRENT_DATE, -18)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
)
SELECT COUNT(DISTINCT f.CLNT_NO)  AS n_flip_clients,
       COUNT(DISTINCT t.CLNT_NO)  AS n_with_any_tactic,   -- matched clients only (NULLs drop out)
       CAST(100.0 * COUNT(DISTINCT t.CLNT_NO) / NULLIF(COUNT(DISTINCT f.CLNT_NO),0)
            AS DECIMAL(5,1))      AS pct_any_tactic
FROM flips f
LEFT JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
  ON  t.CLNT_NO = f.CLNT_NO
  AND t.TREATMT_STRT_DT >= DATE '2024-01-01'   -- any channel, any decision
"""))

# %% [P5] PROBE — is the EM filter catching a sane share of decisions?
# For flip clients' tactic rows: how many match the EM (email) pattern vs total?
# Expect: a material share EM. If pct_em is ~0, the channel filter is the artifact
# behind "no email found", not reality.
display(edw_pd("""
WITH flips AS (
    SELECT CLNT_NO
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= ADD_MONTHS(CURRENT_DATE, -18)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
)
SELECT COUNT(*) AS n_tactic_rows,
       SUM(CASE WHEN SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
                  OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%'
                THEN 1 ELSE 0 END) AS n_em_rows,
       CAST(100.0 * SUM(CASE WHEN SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
                  OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%'
                THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0) AS DECIMAL(5,1)) AS pct_em
FROM flips f
JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
  ON t.CLNT_NO = f.CLNT_NO AND t.TREATMT_STRT_DT >= DATE '2024-01-01'
"""))

# %% [1] Q0 — the story opener: monthly volume of 1012 changes to No
# Simplest possible count, no QUALIFY: every 1012->No change event in the window.
# n_change_events = rows in the log; n_clients = distinct clients that month.
# (The gap analysis below then keeps ONE row per client - their most recent flip -
#  so its per-month numbers sit slightly below n_clients here. That is expected.)
SQL_TOTALS = """
SELECT TRIM(EXTRACT(YEAR FROM CAST(CHG_TMSTMP AS DATE))) || '-' ||
         TRIM(CASE WHEN EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)) < 10 THEN '0' ELSE '' END) ||
         TRIM(EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)))  AS chg_month,
       COUNT(*)                                              AS n_change_events,
       COUNT(DISTINCT CLNT_NO)                               AS n_clients
FROM DDWV01.CPC_RB_PREF_LOG
WHERE PREF_ID = 1012
  AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2024-01-01'   -- full data floor: whole log on screen
GROUP BY 1                              -- (gap analysis below still uses last 18 months)
ORDER BY 1
"""
tot = edw_pd(SQL_TOTALS)
display(tot)

fig, ax = plt.subplots(figsize=(12, 4.5))
ax.bar(tot["chg_month"], tot["n_clients"], color="#2a78d6")
for x, v in zip(tot["chg_month"], tot["n_clients"]):
    ax.text(x, v, f"{int(v):,}", ha="center", va="bottom", fontsize=8.5)
ax.set_ylabel("distinct clients")
ax.set_title(f"1012 (Banking E-Mail consent) changed to No — clients per month since 2024 · total {tot['n_clients'].sum():,}",
             fontweight="bold")
ax.tick_params(axis="x", rotation=45)
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# %% [2] Q1 — flip_month x gap_bucket
# flips      : most recent 1012 change-to-No per client, past 18 months (5002 = No)
# last_email : last email decision on/before the flip date (EM per channel_codes; floor 2024-01-01)
# gapped     : one row per flipping client; gap NULL = no email decision found since floor
SQL_BUCKETS = """
WITH flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1012                 -- email consent switch
      AND CLNT_CONSENT_TYP = 5002        -- flipped to No
      AND CHG_TMSTMP >= ADD_MONTHS(CURRENT_DATE, -18)   -- last 18 months
    -- one row per client: their most recent flip
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
),
last_email AS (
    SELECT f.CLNT_NO, f.flip_dt, t.TREATMT_STRT_DT AS email_dt
    FROM flips f
    JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
      ON  t.CLNT_NO = f.CLNT_NO
      AND t.TREATMT_STRT_DT <= f.flip_dt           -- only emails BEFORE the flip
      AND t.TREATMT_STRT_DT >= DATE '2024-01-01'   -- data floor
      AND ( SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
            OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )   -- EM = email channel
    -- one row per client: the email CLOSEST to the flip
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY t.TREATMT_STRT_DT DESC) = 1
),
gapped AS (
    SELECT f.CLNT_NO,
           TRIM(EXTRACT(YEAR FROM f.flip_dt)) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM f.flip_dt) < 10 THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM f.flip_dt))              AS flip_month,
           f.flip_dt - em.email_dt                            AS gap_days   -- days between them
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
_piv["TOTAL"] = _piv.sum(axis=1)           # row total per month
_piv.loc["TOTAL"] = _piv.sum(axis=0)       # column totals at the bottom
display(_piv)

# %% [3] rollup + meeting bar — the decision view
roll = bk.groupby("gap_bucket", as_index=False)["n_clients"].sum()
roll["share_pct"] = (roll["n_clients"] / roll["n_clients"].sum() * 100).round(1)
display(roll)

order = ["1_same_or_next_day", "2_within_week", "3_within_month",
         "4_within_quarter", "5_over_90_days", "6_no_email_found"]
labels = ["same/next day", "2-7 days", "8-30 days", "31-90 days", ">90 days", "no email found"]
r = roll.set_index("gap_bucket").reindex(order)
fig, ax = plt.subplots(figsize=(10, 5))
ax.barh(labels[::-1], r["n_clients"][::-1], color="#2a78d6")
for i, v in enumerate(r["n_clients"][::-1]):
    if pd.notna(v):
        ax.text(v, i, f" {int(v):,} ({r['share_pct'][::-1].iloc[i]}%)", va="center", fontsize=10)
ax.set_xlabel("clients")
ax.set_title("Where the last email decision sits relative to the 1012 change", fontweight="bold")
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# %% [4] Q2 — day-level gaps 0-90 + histogram (the chart that decides it)
SQL_DAYS = """
WITH flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1012                 -- email consent switch
      AND CLNT_CONSENT_TYP = 5002        -- flipped to No
      AND CHG_TMSTMP >= ADD_MONTHS(CURRENT_DATE, -18)   -- last 18 months
    -- one row per client: their most recent flip
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
),
last_email AS (
    SELECT f.CLNT_NO, f.flip_dt, t.TREATMT_STRT_DT AS email_dt
    FROM flips f
    JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
      ON  t.CLNT_NO = f.CLNT_NO
      AND t.TREATMT_STRT_DT <= f.flip_dt           -- only emails BEFORE the flip
      AND t.TREATMT_STRT_DT >= DATE '2024-01-01'   -- data floor
      AND ( SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
            OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )   -- EM = email channel
    -- one row per client: the email CLOSEST to the flip
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY t.TREATMT_STRT_DT DESC) = 1
)
SELECT f.flip_dt - em.email_dt AS gap_days,   -- days between email and flip
       COUNT(*)                AS n_clients
FROM flips f
JOIN last_email em ON em.CLNT_NO = f.CLNT_NO
WHERE f.flip_dt - em.email_dt <= 90           -- chart window: first 90 days only
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
ax.set_title("Most recent 1012 change to No (18 mo) — days since last email decision\n"
             "Descriptive timing only: nearest prior decision record; no attribution implied.",
             fontweight="bold")
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# %% [5] what WAS that last email? campaign (MNE) mix of the matched decisions
# Same two CTEs; instead of the gap we show which campaign the last email belonged to.
SQL_MNE = """
WITH flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1012                 -- email consent switch
      AND CLNT_CONSENT_TYP = 5002        -- flipped to No
      AND CHG_TMSTMP >= ADD_MONTHS(CURRENT_DATE, -18)   -- last 18 months
    -- one row per client: their most recent flip
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
),
last_email AS (
    SELECT f.CLNT_NO, f.flip_dt, t.TREATMT_STRT_DT AS email_dt,
           SUBSTR(t.TACTIC_ID, 8, 3) AS mne
    FROM flips f
    JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
      ON  t.CLNT_NO = f.CLNT_NO
      AND t.TREATMT_STRT_DT <= f.flip_dt           -- only emails BEFORE the flip
      AND t.TREATMT_STRT_DT >= DATE '2024-01-01'   -- data floor
      AND ( SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
            OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )   -- EM = email channel
    -- one row per client: the email CLOSEST to the flip
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY t.TREATMT_STRT_DT DESC) = 1
)
SELECT em.mne,
       COUNT(*)                                              AS n_clients,
       SUM(CASE WHEN f.flip_dt - em.email_dt <= 7 THEN 1 ELSE 0 END) AS n_within_week
FROM flips f
JOIN last_email em ON em.CLNT_NO = f.CLNT_NO
GROUP BY 1
ORDER BY 2 DESC
"""
mne = edw_pd(SQL_MNE)
display(mne.head(20))

# %% [6] WIDER LENS — 1002 / 1012 / 1014: monthly flips to No, side by side
# Same chart as Q0, one panel per switch. Kept at the bottom so the 1012 story stays focused.
# 1002 = entity Do-Not-Solicit; 1012 = Banking E-Mail consent; 1014 = decisioning-read switch.
SQL_3SW = """
SELECT PREF_ID,
       TRIM(EXTRACT(YEAR FROM CAST(CHG_TMSTMP AS DATE))) || '-' ||
         TRIM(CASE WHEN EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)) < 10 THEN '0' ELSE '' END) ||
         TRIM(EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)))  AS chg_month,
       COUNT(*)                 AS n_change_events,
       COUNT(DISTINCT CLNT_NO)  AS n_clients
FROM DDWV01.CPC_RB_PREF_LOG
WHERE PREF_ID IN (1002, 1012, 1014)
  AND CLNT_CONSENT_TYP = 5002                       -- changed to No
  AND CHG_TMSTMP >= DATE '2024-01-01'               -- full data floor
GROUP BY 1, 2
ORDER BY 1, 2
"""
sw = edw_pd(SQL_3SW)
display(sw.pivot_table(index="chg_month", columns="PREF_ID",
                       values="n_clients", aggfunc="sum", fill_value=0))

fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
for ax, pid in zip(axes, [1002, 1012, 1014]):
    d = sw[sw["PREF_ID"] == pid]
    ax.bar(d["chg_month"], d["n_clients"], color="#2a78d6")
    ax.set_title(f"{pid} changed to No — clients per month · total {d['n_clients'].sum():,}",
                 fontweight="bold", fontsize=11, loc="left")
    ax.set_ylabel("clients")
    ax.spines[["top", "right"]].set_visible(False)
axes[-1].tick_params(axis="x", rotation=45)
plt.tight_layout(); plt.show()

# %% [7] WHO WRITES EACH SWITCH — APP_SYS_CD by PREF_ID and direction, since 2024
# Which systems feed these switches. Yes(5001) vs No(5002) split shows what each
# system is doing (opt-in capture vs opt-out writes).
SQL_WRITERS = """
SELECT PREF_ID, APP_SYS_CD, CLNT_CONSENT_TYP,
       COUNT(*)                 AS n_rows,
       COUNT(DISTINCT CLNT_NO)  AS n_clients
FROM DDWV01.CPC_RB_PREF_LOG
WHERE PREF_ID IN (1002, 1012, 1014)
  AND CHG_TMSTMP >= DATE '2024-01-01'
GROUP BY 1, 2, 3
ORDER BY 1, 4 DESC
"""
wr = edw_pd(SQL_WRITERS)

# APP_SYS_CD decode — schemas/cpc_rb_pref_log_schema.md (dictionary pics 2026-07-15)
SYS_DESC = {
    7001: "Sales Platform / branch staff",
    7002: "DI staff",
    7003: "Royal Direct contact centre",
    7004: "Online Banking",
    7005: "Service Platform",
    7006: "RBC Banking internal/batch (STAR UI, purge)",
    7009: "Bridgetrack/Sapient",
    7015: "SAP (RCT/LINUX)",
    7016: "RBC.COM",
    7017: "telemarketing vendor",
    7020: "Exact Target (email ESP)",
    7021: "TSYS",
    7024: "telemarketing vendor",
    7025: "telemarketing vendor",
    7026: "telemarketing vendor",
    7999: "default",
    99999: "batch SRF consolidation",
}

for pid in [1002, 1012, 1014]:
    print(f"--- PREF_ID {pid}: rows by writing system x consent value ---")
    p = (wr[wr["PREF_ID"] == pid]
         .pivot_table(index="APP_SYS_CD", columns="CLNT_CONSENT_TYP",
                      values="n_rows", aggfunc="sum", fill_value=0))
    p["TOTAL"] = p.sum(axis=1)
    p.insert(0, "system", [SYS_DESC.get(c, "?? not in dictionary") for c in p.index])
    display(p.sort_values("TOTAL", ascending=False))
