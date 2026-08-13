# %% [markdown]
# # 32 — CPC 1012 flips → last email decision before the flip
#
# **Question (skeptics' framing, inverted):** take every client whose most recent
# Banking E-Mail consent (PREF_ID 1012) changed to **No** in the past 18 months.
# For each, find the **last email decision** in tactic history **before** that change.
# How close are they?
#
# **Read rule:** emails are frequent, so a nearby email is expected by chance.
# If email clicks drive 1012 flips, **day 0–1 towers** over everything.
# A flat spread across 0–30+ days = contact-cadence coincidence, not attribution.
#
# All outputs inline. No files written. Connection = same as unsub_unified.

# %% [0] connect (same idiom as unsub_unified) + proof round-trip
get_ipython().system("./environment/bin/python -m pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")
import getpass
import pandas as pd
import matplotlib.pyplot as plt
import teradatasql

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")

TD_HOST = "Teradata-dns-sysa.fg.rbc.com"
EDW = teradatasql.connect(host=TD_HOST, user=username, password=password, logmech="LDAP")

def edw_pd(sql, chunksize=1_000_000):
    parts, n = [], 0
    for c in pd.read_sql(sql, EDW, chunksize=chunksize):
        parts.append(c); n += len(c)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

display(edw_pd("SELECT USER AS usr, SESSION AS sess, CURRENT_TIMESTAMP AS ts"))

# %% [1] Q1 — flip_month x gap_bucket
# flips      : most recent 1012 change-to-No per client, past 18 months (5002 = No)
# last_email : last email decision on/before the flip date (EM per channel_codes; floor 2024-01-01)
# gapped     : one row per flipping client; gap NULL = no email decision found since floor
SQL_BUCKETS = """
WITH flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1012
      AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= ADD_MONTHS(CURRENT_DATE, -18)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
),
last_email AS (
    SELECT f.CLNT_NO, f.flip_dt, t.TREATMT_STRT_DT AS email_dt
    FROM flips f
    JOIN DTZV01.TACTIC_EVNT_IP_AR_H60M t
      ON  t.CLNT_NO = f.CLNT_NO
      AND t.TREATMT_STRT_DT <= f.flip_dt
      AND t.TREATMT_STRT_DT >= DATE '2024-01-01'
      AND ( SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
            OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY t.TREATMT_STRT_DT DESC) = 1
),
gapped AS (
    SELECT f.CLNT_NO,
           TRIM(EXTRACT(YEAR FROM f.flip_dt)) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM f.flip_dt) < 10 THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM f.flip_dt))              AS flip_month,
           f.flip_dt - le.email_dt                            AS gap_days
    FROM flips f
    LEFT JOIN last_email le ON le.CLNT_NO = f.CLNT_NO
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
display(bk.pivot_table(index="flip_month", columns="gap_bucket",
                       values="n_clients", aggfunc="sum", fill_value=0))

# %% [2] rollup + meeting bar — the decision view
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

# %% [3] Q2 — day-level gaps 0-90 + histogram (the chart that decides it)
SQL_DAYS = """
WITH flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1012
      AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= ADD_MONTHS(CURRENT_DATE, -18)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
),
last_email AS (
    SELECT f.CLNT_NO, f.flip_dt, t.TREATMT_STRT_DT AS email_dt
    FROM flips f
    JOIN DTZV01.TACTIC_EVNT_IP_AR_H60M t
      ON  t.CLNT_NO = f.CLNT_NO
      AND t.TREATMT_STRT_DT <= f.flip_dt
      AND t.TREATMT_STRT_DT >= DATE '2024-01-01'
      AND ( SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
            OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY t.TREATMT_STRT_DT DESC) = 1
)
SELECT f.flip_dt - le.email_dt AS gap_days,
       COUNT(*)                AS n_clients
FROM flips f
JOIN last_email le ON le.CLNT_NO = f.CLNT_NO
WHERE f.flip_dt - le.email_dt <= 90
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
             "If email clicks drove the change, day 0-1 towers. Flat = contact-cadence coincidence.",
             fontweight="bold")
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()
