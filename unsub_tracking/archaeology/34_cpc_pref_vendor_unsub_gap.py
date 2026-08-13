# %% [markdown]
# # 34 — CPC 1012 flips (CPC_RB_PREF) → nearest prior VENDOR unsub (disposition 4)
#
# Companion to pack 32 v2. Same flips (standing became No, pinned window), but the
# candidate cause is now the **email-channel unsub event** from vendor feedback
# (disposition_cd = 4), not the email decision. Question on screen: when a client's
# 1012 standing turned No, had they clicked unsubscribe shortly before?
#
# Vendor unsub resolution = pack 21b's validated idiom: EVENT (disp=4, bounded) →
# MASTER on (consumer_id_hashed, TREATMENT_ID) with load_tm bounds → DISTINCT CLNT_NO.
# MASTER grain is not 1:1 — DISTINCT dedup is mandatory (canon §20).
#
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

def edw_exec(sql, label=""):
    # for DDL (volatile tables): execute without expecting rows
    cur = EDW.cursor()
    cur.execute(sql)
    cur.close()
    print(f"OK: {label or sql[:60]}")

display(edw_pd("SELECT USER AS usr, SESSION AS sess, CURRENT_TIMESTAMP AS ts"))

# %% [C] WINDOWS - one convention, used by every cell below
WIN_FLOOR  = "2025-02-01"   # flips window: last 18 months, pinned
LOOK_FLOOR = "2024-02-01"   # history lookback: 12 months BEFORE the window opens,
                            # so early-window flips have real lookback and
                            # 'no_*_found' means none in 12-30 months, not an artifact

# %% [1] BUILD vendor-unsub lookup ONCE per session (volatile — the one heavy join)
# EVENT disp=4 since 2024 → MASTER resolve to CLNT_NO (load_tm bounded, 1mo margin).
# Volatile table justified: every cell below reuses it; rerunning this cell in the
# same session raises "already exists" — that is fine, just skip to [2].
try:
    edw_exec("DROP TABLE vt_unsub34", "drop old vt_unsub34")
except Exception:
    pass

edw_exec("""
CREATE VOLATILE TABLE vt_unsub34 AS (
    SELECT DISTINCT m.CLNT_NO, e.disposition_dt_tm AS unsub_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4                         -- unsubscribe event
      AND e.disposition_dt_tm >= DATE '2024-02-01'     -- lookback: 12mo before window
      AND m.load_tm >= DATE '2024-01-01'               -- lookback minus 1mo margin (load stamp)
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS
""", "vt_unsub34 built")
edw_exec("COLLECT STATISTICS ON vt_unsub34 COLUMN (CLNT_NO)", "stats collected")

# %% [P1] PROBE — scale + raw look of the resolved unsub events
display(edw_pd("""
SELECT CAST(COUNT(*) AS BIGINT) AS n_unsub_events,
       COUNT(DISTINCT CLNT_NO)  AS n_clients
FROM vt_unsub34
"""))
display(edw_pd("SELECT * FROM vt_unsub34 SAMPLE 10"))

# %% [P2] PROBE — the attribution, raw: 20 matched flip↔nearest-unsub pairs
display(edw_pd("""
WITH flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-02-01'   -- pinned window
),
nearest AS (
    SELECT f.CLNT_NO, f.flip_dt, CAST(u.unsub_tm AS DATE) AS unsub_dt
    FROM flips f
    JOIN vt_unsub34 u
      ON  u.CLNT_NO = f.CLNT_NO
      AND CAST(u.unsub_tm AS DATE) <= f.flip_dt           -- unsub on/before the flip
    -- one row per client: the unsub CLOSEST to the flip
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY u.unsub_tm DESC) = 1
)
SELECT CLNT_NO, flip_dt, unsub_dt, flip_dt - unsub_dt AS gap_days
FROM nearest
SAMPLE 20
"""))

# %% [2] gap buckets — flip vs nearest prior vendor unsub
SQL_VBUCKETS = """
WITH flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-02-01'
),
nearest AS (
    SELECT f.CLNT_NO, f.flip_dt, CAST(u.unsub_tm AS DATE) AS unsub_dt
    FROM flips f
    JOIN vt_unsub34 u
      ON  u.CLNT_NO = f.CLNT_NO
      AND CAST(u.unsub_tm AS DATE) <= f.flip_dt          -- unsub on/before the flip
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY u.unsub_tm DESC) = 1
),
gapped AS (
    SELECT f.CLNT_NO,
           TRIM(EXTRACT(YEAR FROM f.flip_dt)) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM f.flip_dt) < 10 THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM f.flip_dt))              AS flip_month,
           f.flip_dt - n.unsub_dt                             AS gap_days
    FROM flips f
    LEFT JOIN nearest n ON n.CLNT_NO = f.CLNT_NO   -- LEFT: keep flips with no unsub (gap NULL)
)
SELECT flip_month,
       CASE WHEN gap_days IS NULL   THEN '6_no_unsub_found'
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
vb = edw_pd(SQL_VBUCKETS)
_piv = vb.pivot_table(index="flip_month", columns="gap_bucket",
                      values="n_clients", aggfunc="sum", fill_value=0)
_piv["TOTAL"] = _piv.sum(axis=1)
_piv.loc["TOTAL"] = _piv.sum(axis=0)
display(_piv)

# %% [3] rollup + bar — the decision view
roll = vb.groupby("gap_bucket", as_index=False)["n_clients"].sum()
roll["share_pct"] = (roll["n_clients"] / roll["n_clients"].sum() * 100).round(1)
display(roll)

order = ["1_same_or_next_day", "2_within_week", "3_within_month",
         "4_within_quarter", "5_over_90_days", "6_no_unsub_found"]
labels = ["same/next day", "2-7 days", "8-30 days", "31-90 days", ">90 days", "no unsub found"]
r = roll.set_index("gap_bucket").reindex(order)
fig, ax = plt.subplots(figsize=(10, 5))
ax.barh(labels[::-1], r["n_clients"][::-1].fillna(0), color="#2a78d6")
for i, v in enumerate(r["n_clients"][::-1]):
    if pd.notna(v):
        ax.text(v, i, f" {int(v):,} ({r['share_pct'][::-1].iloc[i]}%)", va="center", fontsize=10)
ax.set_xlabel("clients")
ax.set_title("Where the nearest vendor unsub (disp=4) sits relative to the 1012 change",
             fontweight="bold")
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# %% [4] day-level 0-90 histogram
SQL_VDAYS = """
WITH flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-02-01'
),
nearest AS (
    SELECT f.CLNT_NO, f.flip_dt, CAST(u.unsub_tm AS DATE) AS unsub_dt
    FROM flips f
    JOIN vt_unsub34 u
      ON  u.CLNT_NO = f.CLNT_NO
      AND CAST(u.unsub_tm AS DATE) <= f.flip_dt
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY u.unsub_tm DESC) = 1
)
SELECT f.flip_dt - n.unsub_dt AS gap_days,
       COUNT(*)               AS n_clients
FROM flips f
JOIN nearest n ON n.CLNT_NO = f.CLNT_NO
WHERE f.flip_dt - n.unsub_dt <= 90
GROUP BY 1
ORDER BY 1
"""
vdy = edw_pd(SQL_VDAYS)
display(vdy.head(15))

fig, ax = plt.subplots(figsize=(12, 5))
ax.bar(vdy["gap_days"], vdy["n_clients"], width=0.9, color="#2a78d6")
d01 = vdy[vdy["gap_days"] <= 1]["n_clients"].sum()
ax.annotate(f"day 0-1: {d01:,}", xy=(1, vdy[vdy["gap_days"] <= 1]["n_clients"].max() if d01 else 0),
            xytext=(8, vdy["n_clients"].max() * 0.9), fontsize=11, fontweight="bold",
            arrowprops=dict(arrowstyle="->", color="#52514e"))
ax.set_xlabel("days from vendor unsub to the 1012 change")
ax.set_ylabel("clients")
ax.set_title("1012 standing became No — days since nearest prior vendor unsub (disp=4)\n"
             "Descriptive timing only; no attribution implied.",
             fontweight="bold")
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# %% [5] the reverse direction — of vendor unsubbers, how many flip 1012 within 90 days?
# Complements [2]: same two sources, opposite conditioning. (Caveat: flips visible
# here are arrivals into CURRENT standing only — re-consented clients don't count.)
display(edw_pd("""
WITH first_unsub AS (
    SELECT CLNT_NO, MIN(CAST(unsub_tm AS DATE)) AS first_unsub_dt
    FROM vt_unsub34
    WHERE unsub_tm >= DATE '2025-02-01'   -- same pinned window
    GROUP BY 1
),
flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
)
SELECT COUNT(*)                                            AS n_unsub_clients,
       SUM(CASE WHEN f.flip_dt >= u.first_unsub_dt
                 AND f.flip_dt <  u.first_unsub_dt + 90
                THEN 1 ELSE 0 END)                         AS n_flipped_within_90d,
       CAST(100.0 * SUM(CASE WHEN f.flip_dt >= u.first_unsub_dt
                 AND f.flip_dt <  u.first_unsub_dt + 90
                THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0) AS DECIMAL(5,2)) AS pct_flipped_90d
FROM first_unsub u
LEFT JOIN flips f ON f.CLNT_NO = u.CLNT_NO
"""))
