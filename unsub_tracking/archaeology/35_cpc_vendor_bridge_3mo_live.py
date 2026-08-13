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

# %% [2] Gap between each switch change and the nearest prior unsubscribe (one query, 4 steps)
# Run once per switch: 1012 (Banking E-Mail), then 1002 and 1014. 1014 caveat: blank
# (5003) also reads as No on that switch - explicit 5002 writes only, so 1014 is a floor.
SQL_BRIDGE = """
WITH flips AS (
    -- step 1: clients whose {pref} standing became No in the window - one row per client
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = {pref} AND CLNT_CONSENT_TYP = 5002
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
labels = {"1_same_or_next_day": "same/next day", "2_within_week": "2-7 days",
          "3_within_month": "8-30 days", "4_over_30_days": ">30 days",
          "5_no_unsub_found": "no unsub found"}
for pref in (1012, 1002, 1014):
    br = edw_pd(SQL_BRIDGE.format(pref=pref))
    br["share_pct"] = (br["n_clients"] / br["n_clients"].sum() * 100).round(1)
    print(f"--- PREF_ID {pref}: nearest unsubscribe before each switch-off ---")
    display(br)
    r = br.set_index("gap_bucket").reindex(list(labels))
    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.barh(list(labels.values())[::-1], r["n_clients"][::-1].fillna(0), color="#2a78d6")
    for i, v in enumerate(r["n_clients"][::-1]):
        if pd.notna(v):
            ax.text(v, i, f" {int(v):,} ({r['share_pct'][::-1].iloc[i]}%)", va="center", fontsize=10)
    ax.set_xlabel("clients")
    ax.set_title(f"3-month live slice: nearest unsubscribe before each {pref} switch-off",
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

# %% [4] Writer system of the changes - decoded, with and without a prior unsubscribe
# Run once per switch: 1012, 1002, 1014 (same 1014 explicit-5002 floor caveat as [2]).
SQL_WRITERS = """
WITH flips AS (
    SELECT CLNT_NO, APP_SYS_CD, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = {pref} AND CLNT_CONSENT_TYP = 5002
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
)
SELECT f.APP_SYS_CD,
       COUNT(*) AS n_changes,
       SUM(CASE WHEN u.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS n_with_unsub_0_1d
FROM flips f
LEFT JOIN (
    SELECT DISTINCT f2.CLNT_NO
    FROM flips f2
    JOIN unsubs u2
      ON  u2.CLNT_NO = f2.CLNT_NO
      AND f2.flip_dt - u2.unsub_dt BETWEEN 0 AND 1
) u ON u.CLNT_NO = f.CLNT_NO
GROUP BY 1 ORDER BY 2 DESC
"""
# APP_SYS_CD decode - schemas/cpc_rb_pref_log_schema.md (official dictionary 2026-08-13)
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
for pref in (1012, 1002, 1014):
    wr = edw_pd(SQL_WRITERS.format(pref=pref))
    wr.insert(1, "system", [SYS_DESC.get(c, "?? not in dictionary") for c in wr["APP_SYS_CD"]])
    wr["share_pct"] = (wr["n_changes"] / wr["n_changes"].sum() * 100).round(1)
    print(f"--- PREF_ID {pref}: writer system, n_changes + n_with_unsub_0_1d ---")
    display(wr)

# %% [5] Bridged clients by campaign mnemonic (unsubscribe 0-1 days before the change)
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

# %% [6] ESP-written changes (APP_SYS_CD 7020) - vendor-feedback accounting, no window
# Reverse direction of [4]: start from the 1012 switch-offs WRITTEN BY the email ESP
# itself (7020 = Exact Target). These writes originate inside the email channel, so the
# vendor tables should hold the unsubscribe that caused each one. Lookback widened to
# 2024-01-01 (not the April slice floor) - the question is "does the trail exist at
# all", not "is it inside the window". Every 7020-written flip lands in exactly one
# bucket. Client filter pushed into MASTER first, so the wide scan stays small.
# Run once per switch: 1012, 1002, 1014.
SQL_ESP = """
WITH esp_flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = {pref} AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2026-05-01'
      AND APP_SYS_CD = 7020                             -- written by the ESP itself
),
vf AS (
    -- every vendor identity row for JUST these clients (small driver, wide history)
    SELECT m.CLNT_NO, m.consumer_id_hashed, m.TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
    JOIN esp_flips f ON f.CLNT_NO = m.CLNT_NO
    WHERE m.load_tm >= DATE '2023-10-01'                -- 3-mo load-stamp margin on 2024 floor
    GROUP BY 1, 2, 3
),
unsub4 AS (
    -- ALL their disposition-4 events since 2024, before OR after the write
    SELECT v.CLNT_NO, CAST(e.disposition_dt_tm AS DATE) AS unsub_dt
    FROM vf v
    JOIN DTZV01.VENDOR_FEEDBACK_EVENT e
      ON  e.consumer_id_hashed = v.consumer_id_hashed
      AND e.TREATMENT_ID       = v.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'
),
per_client AS (
    SELECT f.CLNT_NO, f.flip_dt,
           MAX(CASE WHEN u.unsub_dt <= f.flip_dt THEN u.unsub_dt END) AS last_unsub_before,
           MIN(CASE WHEN u.unsub_dt >  f.flip_dt THEN u.unsub_dt END) AS first_unsub_after,
           MAX(CASE WHEN v.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)     AS in_master
    FROM esp_flips f
    LEFT JOIN (SELECT DISTINCT CLNT_NO FROM vf) v ON v.CLNT_NO = f.CLNT_NO
    LEFT JOIN unsub4 u                            ON u.CLNT_NO = f.CLNT_NO
    GROUP BY 1, 2
)
SELECT CASE
         WHEN flip_dt - last_unsub_before <= 1  THEN '1_unsub_0_1d_before_write'
         WHEN flip_dt - last_unsub_before <= 30 THEN '2_unsub_2_30d_before_write'
         WHEN last_unsub_before IS NOT NULL     THEN '3_unsub_over_30d_before_write'
         WHEN first_unsub_after IS NOT NULL     THEN '4_unsub_only_AFTER_write'
         WHEN in_master = 1                     THEN '5_in_vendor_tables_no_unsub_ever'
         ELSE                                        '6_clnt_no_absent_from_master' END AS evidence,
       COUNT(*) AS n_clients
FROM per_client
GROUP BY 1 ORDER BY 1
"""
print("--- (bucket 6 caveat: master rows loaded before 2023-10 are outside this scan) ---")
for pref in (1012, 1002, 1014):
    esp = edw_pd(SQL_ESP.format(pref=pref))
    if len(esp) == 0:
        print(f"--- PREF_ID {pref}: no 7020-written switch-offs in the window ---")
        continue
    esp["share_pct"] = (esp["n_clients"] / esp["n_clients"].sum() * 100).round(1)
    print(f"--- PREF_ID {pref}: 7020-written switch-offs - where is the unsubscribe behind each write? ---")
    display(esp)

# %% [6b] The 7020-written 1012 switch-offs by campaign mnemonic
# Which campaign's mail sits behind each ESP-written switch-off. Attribution = the
# nearest prior unsubscribe (2024 lookback, no window, same frame as [6]); several
# distinct campaigns on that nearest day -> 'MULTI'. DEFAULT/malformed ids -> 'UNTAGGED'
# (same conventions as [5]). Clients with no prior unsub at all -> 'NO_PRIOR_UNSUB',
# so the table sums to the full 7020 count from [4].
m6b = edw_pd("""
WITH esp_flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2026-05-01'
      AND APP_SYS_CD = 7020
),
vf AS (
    SELECT m.CLNT_NO, m.consumer_id_hashed, m.TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
    JOIN esp_flips f ON f.CLNT_NO = m.CLNT_NO
    WHERE m.load_tm >= DATE '2023-10-01'
    GROUP BY 1, 2, 3
),
unsub_ev AS (
    SELECT DISTINCT v.CLNT_NO, CAST(e.disposition_dt_tm AS DATE) AS unsub_dt,
           CASE WHEN UPPER(v.TREATMENT_ID) = 'DEFAULT'                                THEN 'UNTAGGED'
                WHEN SUBSTR(v.TREATMENT_ID, 1, 7) NOT BETWEEN '0000000' AND '9999999' THEN 'UNTAGGED'
                WHEN UPPER(SUBSTR(v.TREATMENT_ID, 8, 3)) NOT BETWEEN 'AAA' AND 'ZZZ'  THEN 'UNTAGGED'
                ELSE UPPER(SUBSTR(v.TREATMENT_ID, 8, 3)) END AS mne
    FROM vf v
    JOIN DTZV01.VENDOR_FEEDBACK_EVENT e
      ON  e.consumer_id_hashed = v.consumer_id_hashed
      AND e.TREATMENT_ID       = v.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'
),
nearest_day AS (
    -- per client, the LAST unsub day on/before the write
    SELECT f.CLNT_NO, MAX(u.unsub_dt) AS nd
    FROM esp_flips f
    JOIN unsub_ev u ON u.CLNT_NO = f.CLNT_NO AND u.unsub_dt <= f.flip_dt
    GROUP BY 1
),
attributed AS (
    -- all unsub events on that nearest day; one campaign -> it, several -> MULTI
    SELECT n.CLNT_NO,
           CASE WHEN COUNT(DISTINCT u.mne) = 1 THEN MIN(u.mne) ELSE 'MULTI' END AS attributed_mne
    FROM nearest_day n
    JOIN unsub_ev u ON u.CLNT_NO = n.CLNT_NO AND u.unsub_dt = n.nd
    GROUP BY 1
)
SELECT COALESCE(a.attributed_mne, 'NO_PRIOR_UNSUB') AS attributed_mne,
       COUNT(*) AS n_clients
FROM esp_flips f
LEFT JOIN attributed a ON a.CLNT_NO = f.CLNT_NO
GROUP BY 1 ORDER BY 2 DESC
""")
m6b["share_pct"] = (m6b["n_clients"] / m6b["n_clients"].sum() * 100).round(1)
print("--- 7020-written 1012 switch-offs: campaign behind the nearest prior unsubscribe ---")
display(m6b)

# %% [7] The untraced ESP writes - raw rows, what little vendor trail exists (15 clients)
# Buckets 5 and 6 from [6]: the ESP wrote the switch, but no unsubscribe event exists
# anywhere since 2024. Per client: the CPC write, how many identity rows MASTER holds,
# how many events of ANY disposition, and the last time any email activity was seen.
display(edw_pd("""
WITH esp_flips AS (
    SELECT CLNT_NO, CHG_TMSTMP, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2026-05-01'
      AND APP_SYS_CD = 7020
),
vf AS (
    SELECT m.CLNT_NO, m.consumer_id_hashed, m.TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
    JOIN esp_flips f ON f.CLNT_NO = m.CLNT_NO
    WHERE m.load_tm >= DATE '2023-10-01'
    GROUP BY 1, 2, 3
),
ev AS (
    SELECT v.CLNT_NO,
           COUNT(*)                                                    AS n_events_any_disp,
           SUM(CASE WHEN e.disposition_cd = 4 THEN 1 ELSE 0 END)       AS n_unsub_events,
           MAX(e.disposition_dt_tm)                                    AS last_event_tm
    FROM vf v
    JOIN DTZV01.VENDOR_FEEDBACK_EVENT e
      ON  e.consumer_id_hashed = v.consumer_id_hashed
      AND e.TREATMENT_ID       = v.TREATMENT_ID
    WHERE e.disposition_dt_tm >= DATE '2024-01-01'
    GROUP BY 1
),
mrows AS (
    SELECT CLNT_NO, COUNT(*) AS n_master_identity_rows
    FROM vf GROUP BY 1
)
SELECT f.CLNT_NO,
       f.CHG_TMSTMP                                   AS cpc_write_tm,
       COALESCE(mr.n_master_identity_rows, 0)         AS n_master_identity_rows,
       COALESCE(ev.n_events_any_disp, 0)              AS n_events_any_disp,
       ev.last_event_tm
FROM esp_flips f
LEFT JOIN mrows mr ON mr.CLNT_NO = f.CLNT_NO
LEFT JOIN ev       ON ev.CLNT_NO = f.CLNT_NO
WHERE COALESCE(ev.n_unsub_events, 0) = 0              -- no disposition-4 anywhere since 2024
SAMPLE 15
"""))

# %% [8] Size of the universe - emails sent vs unsubscribes captured, per month (slice window)
# Both sides straight off DTZV01.VENDOR_FEEDBACK_EVENT, no MASTER join (stays light).
# disposition 1 = send, disposition 4 = unsubscribe. Grain = email address
# (consumer_id_hashed), ~1.1 addresses per client. This is the denominator the bridge
# lives inside: of all mail going out, how much unsubscribing does the vendor table
# even capture - before asking how much of THAT reaches CPC.
uv = edw_pd("""
SELECT TRIM(EXTRACT(YEAR FROM disposition_dt_tm)) || '-' ||
         TRIM(CASE WHEN EXTRACT(MONTH FROM disposition_dt_tm) < 10 THEN '0' ELSE '' END) ||
         TRIM(EXTRACT(MONTH FROM disposition_dt_tm))  AS mth,
       CAST(SUM(CASE WHEN disposition_cd = 1 THEN 1 ELSE 0 END) AS BIGINT) AS n_sends,
       COUNT(DISTINCT CASE WHEN disposition_cd = 1 THEN consumer_id_hashed END) AS n_addresses_mailed,
       SUM(CASE WHEN disposition_cd = 4 THEN 1 ELSE 0 END)                 AS n_unsub_events,
       COUNT(DISTINCT CASE WHEN disposition_cd = 4 THEN consumer_id_hashed END) AS n_addresses_unsubbed
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_cd IN (1, 4)
  AND disposition_dt_tm >= DATE '2026-04-01'          -- same slice floor as the pack
GROUP BY 1 ORDER BY 1
""")
uv["unsub_per_1k_mailed"] = (1000.0 * uv["n_addresses_unsubbed"] / uv["n_addresses_mailed"]).round(2)
display(uv)

fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
axes[0].bar(uv["mth"], uv["n_addresses_mailed"], color="#2a78d6")
axes[0].yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v/1e6:.0f}M"))
axes[0].set_title("addresses mailed (disp 1)", fontweight="bold")
axes[1].bar(uv["mth"], uv["n_addresses_unsubbed"], color="#2a78d6")
axes[1].yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v/1e3:.0f}K"))
axes[1].set_title("addresses unsubscribed (disp 4)", fontweight="bold")
for ax in axes:
    ax.spines[["top", "right"]].set_visible(False)
plt.suptitle("Slice window: mail out vs unsubscribes captured - the universe the bridge lives inside",
             fontweight="bold")
plt.tight_layout(); plt.show()
