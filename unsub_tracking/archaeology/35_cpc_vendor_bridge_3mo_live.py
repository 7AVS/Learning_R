# %% [markdown]
# # 35 — the bridge, live on EDW, 3-month slice (audit / show-people version)
#
# Same question pack 34b answers at full scale off HDFS — but here everything runs
# **directly on the source tables**, in plain SQL, on a slice small enough that the
# workload manager leaves it alone:
#
# - **Flips**: clients whose Banking E-Mail consent (PREF_ID 1012) stands at No,
#   last changed **on/after 2026-05-01** — from `DDWV01.CPC_RB_PREF` (current state).
# - **Unsubs**: one-click unsubscribe events (disposition 4) **on/after 2026-04-01**
#   (one month earlier, so early-window flips still have lookback) — from
#   `DTZV01.VENDOR_FEEDBACK_EVENT`, resolved to client numbers via
#   `DTZV01.VENDOR_FEEDBACK_MASTER`.
#
# Every cell = one visible SQL, one idea. Raw rows shown before any aggregation.
# **Descriptive only — no claims.** Proximity, not attribution, in either direction.

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

# %% [P1] Sample rows — vendor feedback event table (10 unsubscribe events)
pd.set_option("display.max_colwidth", 120)
print("--- DTZV01.VENDOR_FEEDBACK_EVENT, disposition_cd = 4 (unsubscribe), May window ---")
display(edw_pd("""
SELECT consumer_id_hashed, TREATMENT_ID, disposition_cd, disposition_dt_tm
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_cd = 4                              -- 4 = unsubscribe
  AND disposition_dt_tm >= DATE '2026-04-01'          -- slice floor
ORDER BY disposition_dt_tm DESC
SAMPLE 10
"""))

# %% [P2] Sample rows — CPC preference table (10 clients, 1012 standing = No)
print("--- DDWV01.CPC_RB_PREF, PREF_ID 1012 (Banking E-Mail consent), standing = No ---")
display(edw_pd("""
SELECT CLNT_NO, PREF_ID, CLNT_CONSENT_TYP, CHG_TMSTMP, APP_SYS_CD
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012                                  -- email consent switch
  AND CLNT_CONSENT_TYP = 5002                         -- standing = No
  AND CHG_TMSTMP >= DATE '2026-05-01'                 -- changed in the slice window
ORDER BY CHG_TMSTMP DESC
SAMPLE 10
"""))

# %% [1] Monthly volumes — both tables
print("--- unsubscribe events + clients per month (vendor side, resolved to CLNT_NO) ---")
display(edw_pd("""
SELECT TRIM(EXTRACT(YEAR FROM e.disposition_dt_tm)) || '-' ||
         TRIM(CASE WHEN EXTRACT(MONTH FROM e.disposition_dt_tm) < 10 THEN '0' ELSE '' END) ||
         TRIM(EXTRACT(MONTH FROM e.disposition_dt_tm))  AS mth,
       COUNT(*)                   AS n_unsub_events,
       COUNT(DISTINCT m.CLNT_NO)  AS n_clients          -- same resolution as the bridge below
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON  m.consumer_id_hashed = e.consumer_id_hashed      -- vendor id -> client number
  AND m.TREATMENT_ID       = e.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2026-04-01'
  AND m.load_tm           >= DATE '2026-03-01'          -- load-stamp margin
GROUP BY 1 ORDER BY 1
"""))
print("--- 1012 switched to No per month (CPC side) ---")
display(edw_pd("""
SELECT TRIM(EXTRACT(YEAR FROM CAST(CHG_TMSTMP AS DATE))) || '-' ||
         TRIM(CASE WHEN EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)) < 10 THEN '0' ELSE '' END) ||
         TRIM(EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)))  AS mth,
       COUNT(*) AS n_clients_to_no
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2026-05-01'
GROUP BY 1 ORDER BY 1
"""))

# %% [2] Gap between the 1012 change and the nearest prior unsubscribe (one query, 4 steps)
SQL_BRIDGE = """
WITH flips AS (
    -- step 1: clients whose 1012 standing became No in the window - one row per client
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2026-05-01'
),
unsubs AS (
    -- step 2: unsubscribe events resolved to client numbers (vendor id -> CLNT_NO)
    SELECT DISTINCT m.CLNT_NO, CAST(e.disposition_dt_tm AS DATE) AS unsub_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2026-04-01'    -- 1 month before the flip window
      AND m.load_tm           >= DATE '2026-03-01'    -- load-stamp margin
),
nearest AS (
    -- step 3: per flipping client, the unsubscribe CLOSEST before the flip
    SELECT f.CLNT_NO, f.flip_dt, u.unsub_dt
    FROM flips f
    JOIN unsubs u
      ON  u.CLNT_NO = f.CLNT_NO
      AND u.unsub_dt <= f.flip_dt
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY u.unsub_dt DESC) = 1
)
-- step 4: how close? every flip lands in exactly one bucket
SELECT CASE WHEN n.unsub_dt IS NULL              THEN '5_no_unsub_found'
            WHEN f.flip_dt - n.unsub_dt <= 1     THEN '1_same_or_next_day'
            WHEN f.flip_dt - n.unsub_dt <= 7     THEN '2_within_week'
            WHEN f.flip_dt - n.unsub_dt <= 30    THEN '3_within_month'
            ELSE                                      '4_over_30_days' END AS gap_bucket,
       COUNT(*) AS n_clients
FROM flips f
LEFT JOIN nearest n ON n.CLNT_NO = f.CLNT_NO      -- LEFT: flips with no unsub stay in
GROUP BY 1 ORDER BY 1
"""
br = edw_pd(SQL_BRIDGE)
br["share_pct"] = (br["n_clients"] / br["n_clients"].sum() * 100).round(1)
display(br)

labels = {"1_same_or_next_day": "same/next day", "2_within_week": "2-7 days",
          "3_within_month": "8-30 days", "4_over_30_days": ">30 days",
          "5_no_unsub_found": "no unsub found"}
r = br.set_index("gap_bucket").reindex(list(labels))
fig, ax = plt.subplots(figsize=(10, 4.5))
ax.barh(list(labels.values())[::-1], r["n_clients"][::-1].fillna(0), color="#2a78d6")
for i, v in enumerate(r["n_clients"][::-1]):
    if pd.notna(v):
        ax.text(v, i, f" {int(v):,} ({r['share_pct'][::-1].iloc[i]}%)", va="center", fontsize=10)
ax.set_xlabel("clients")
ax.set_title("3-month live slice: nearest unsubscribe before each 1012 switch-off",
             fontweight="bold")
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# %% [3] Sample of matched records — full columns from both tables (20 clients)
# Every column straight off the source tables: vendor side (treatment id, when the mail
# went out, when the disposition-4 unsub was captured) and CPC side (the gate, the
# position it was set to, the exact write timestamp, and WHICH SYSTEM wrote it).
display(edw_pd("""
WITH flips AS (
    SELECT CLNT_NO, PREF_ID, CLNT_CONSENT_TYP, CHG_TMSTMP, APP_SYS_CD,
           CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2026-05-01'
),
unsubs AS (
    SELECT DISTINCT m.CLNT_NO, e.consumer_id_hashed, e.TREATMENT_ID,
           e.disposition_dt_tm AS unsub_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2026-04-01'
      AND m.load_tm           >= DATE '2026-03-01'
),
nearest AS (
    SELECT f.CLNT_NO, u.consumer_id_hashed, u.TREATMENT_ID, u.unsub_tm
    FROM flips f
    JOIN unsubs u
      ON  u.CLNT_NO = f.CLNT_NO
      AND CAST(u.unsub_tm AS DATE) <= f.flip_dt
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY u.unsub_tm DESC) = 1
),
mail_sent AS (
    -- the send (disposition 1) of the SAME treatment to the SAME consumer
    SELECT n.CLNT_NO, MIN(e2.disposition_dt_tm) AS mail_sent_tm
    FROM nearest n
    JOIN DTZV01.VENDOR_FEEDBACK_EVENT e2
      ON  e2.consumer_id_hashed = n.consumer_id_hashed
      AND e2.TREATMENT_ID       = n.TREATMENT_ID
      AND e2.disposition_cd     = 1
      AND e2.disposition_dt_tm >= DATE '2026-03-01'
    GROUP BY 1
)
SELECT f.CLNT_NO,
       n.TREATMENT_ID,
       SUBSTR(n.TREATMENT_ID, 8, 3)              AS mne,
       s.mail_sent_tm,                                          -- email went out (disp=1)
       n.unsub_tm,                                              -- unsub captured (disp=4)
       f.CHG_TMSTMP                              AS cpc_write_tm, -- switch written
       f.PREF_ID                                 AS gate,
       f.CLNT_CONSENT_TYP                        AS consent_cd,    -- 5002 = No (POSITION is reserved)
       f.APP_SYS_CD                              AS written_by,   -- WHICH SYSTEM wrote it
       f.flip_dt - CAST(n.unsub_tm AS DATE)      AS gap_days
FROM flips f
JOIN nearest n   ON n.CLNT_NO = f.CLNT_NO
LEFT JOIN mail_sent s ON s.CLNT_NO = f.CLNT_NO
SAMPLE 20
"""))

# %% [5] Writer system distribution — changes with an unsubscribe 0-1 days before vs without
display(edw_pd("""
WITH flips AS (
    SELECT CLNT_NO, APP_SYS_CD, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2026-05-01'
),
unsubs AS (
    SELECT DISTINCT m.CLNT_NO, CAST(e.disposition_dt_tm AS DATE) AS unsub_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2026-04-01'
      AND m.load_tm           >= DATE '2026-03-01'
),
flagged AS (
    SELECT f.CLNT_NO, f.APP_SYS_CD,
           MAX(CASE WHEN f.flip_dt - u.unsub_dt BETWEEN 0 AND 1 THEN 1 ELSE 0 END) AS bridged_1d
    FROM flips f
    LEFT JOIN unsubs u ON u.CLNT_NO = f.CLNT_NO AND u.unsub_dt <= f.flip_dt
    GROUP BY 1, 2
)
SELECT CASE WHEN bridged_1d = 1 THEN '1_unsub_0-1d_before' ELSE '2_no_recent_unsub' END AS grp,
       APP_SYS_CD                                AS written_by,
       COUNT(*)                                  AS n_clients
FROM flagged
GROUP BY 1, 2
ORDER BY 1, 3 DESC
"""))

# %% [4] Bridged clients by campaign mnemonic (unsubscribe 0-1 days before the change)
# One row per client. Several campaigns unsubbed in that same 0-1d window -> 'MULTI'
# (shown as-is, never an arbitrary winner). TREATMENT_ID 'DEFAULT' -> 'UNTAGGED'.
display(edw_pd("""
WITH flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2026-05-01'
),
win_events AS (
    -- ALL unsub events 0-1 days before the flip (the pipe is a next-day batch)
    SELECT DISTINCT f.CLNT_NO,
           -- campaign rows = program emails ONLY: id must start with 7 digits (the date part)
           -- then a 3-letter code. DEFAULT and anything malformed -> UNTAGGED, kept visible.
           CASE WHEN UPPER(m.TREATMENT_ID) = 'DEFAULT'                             THEN 'UNTAGGED'
                WHEN SUBSTR(m.TREATMENT_ID, 1, 7) NOT BETWEEN '0000000' AND '9999999' THEN 'UNTAGGED'
                WHEN UPPER(SUBSTR(m.TREATMENT_ID, 8, 3)) NOT BETWEEN 'AAA' AND 'ZZZ'  THEN 'UNTAGGED'
                ELSE UPPER(SUBSTR(m.TREATMENT_ID, 8, 3)) END AS mne
    FROM flips f
    JOIN DTZV01.VENDOR_FEEDBACK_EVENT e
      ON  e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2026-04-01'
    JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
      AND m.load_tm            >= DATE '2026-03-01'
      AND m.CLNT_NO            = f.CLNT_NO
    WHERE f.flip_dt - CAST(e.disposition_dt_tm AS DATE) BETWEEN 0 AND 1
),
per_client AS (
    SELECT CLNT_NO,
           CASE WHEN COUNT(DISTINCT mne) = 1 THEN MIN(mne) ELSE 'MULTI' END AS attributed_mne
    FROM win_events
    GROUP BY 1
)
SELECT attributed_mne, COUNT(*) AS n_clients
FROM per_client
GROUP BY 1 ORDER BY 2 DESC
"""))
