# %% [markdown]
# 45_audit_queries.py - executable twin of 45_audit_queries.sql
#
# The audit surface behind the contactable-base deck: every query here is a
# byte-identical copy of the .sql source (Q0, coverage probe, Q1-Q5) - no
# rewritten SQL, no "improved" joins. Teradata-direct via a pre-initialized
# EDW connector (no login code in this file - EDW is already a live session
# object in the kernel). Runtime may be long; correctness over speed.
# Built 2026-08-24.
#
# UNIVERSE RULE (Andre 2026-08-21): every query scopes to active personal
# clients - CLNT_TYP_CD=1 on CPC_RB_PREF_MTHLY, AND every CPC/event read is
# merged to RB_CLNT_DLY CLNT_STS='A' at the EQUIVALENT DATE (month-end
# grain): a month-end standing count uses that month-end's status snapshot,
# an event uses its own month's month-end, the waterfall checks BOTH
# anchors. Never one fixed snapshot across a time series. "Personal" is on
# every number in this file.
#
# The commented-out Spark appendix (A1/A2, UCP-on-parquet) at the bottom of
# the .sql source is skipped here - those only run on the Lumina/YARN kernel.

# %% [0] Setup - imports, EDW helper, house palette
import pandas as pd
import matplotlib.pyplot as plt
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', 100)


def edw_query(sql, desc=""):
    """Run SQL via EDW cursor, return DataFrame. Shows timing."""
    t0 = time.time()
    if desc:
        print(f"  [{desc}] executing...", end=" ", flush=True)
    cursor = EDW.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    cursor.close()
    elapsed = time.time() - t0
    print(f"{len(rows):,} rows in {elapsed:.0f}s")
    return pd.DataFrame(rows, columns=cols)


# house palette (spotlight_deck.py convention: restrained, not a rainbow)
C_THEN = "#003168"    # navy - primary / "then" series, solid bars
C_LINE = "#B00020"    # red - highlight / departure emphasis
C_MUTE = "#9AA7B4"    # grey - muted / secondary / "other" bucket
C_GOLD = "#C49102"    # amber - tertiary segment
C_GREEN = "#4C8C4A"   # green - additions / subscribers
C_STEEL = "#6F90AC"   # steel blue - bar-chart fill
C_SAND = "#D9C9A3"    # sand - the SF-open dangling segment (appears twice, same color both times)


def style_ax(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


print("Setup complete.")


# %% [0b] connect + proof round-trip (standard cell - same as packs 32/33/unsub_unified)
try:
    import teradatasql
except ImportError:
    get_ipython().system("pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")
    import teradatasql
import getpass

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")

TD_HOST = "Teradata-dns-sysa.fg.rbc.com"
EDW = teradatasql.connect(host=TD_HOST, user=username, password=password, logmech="LDAP")

_cur = EDW.cursor()
_cur.execute("SELECT 1")
print("EDW round-trip returned:", _cur.fetchall())
_cur.close()


# %% [1] Q0 - universe codes probe (informational). Only CLNT_STS='A' feeds the
# status CTEs used in Q1/Q2/Q3 (act, u_a, u_b); CLNT_TYP plays no role there -
# personal-vs-non comes from CLNT_TYP_CD=1 on CPC_RB_PREF_MTHLY instead. If
# row_count <> distinct_clnt_count the table is not client-grain at a snapshot.

sql_q0 = """
SELECT CLNT_TYP, CLNT_STS,
       COUNT(*) AS row_count,
       COUNT(DISTINCT CLNT_NO) AS distinct_clnt_count
FROM DDWV01.RB_CLNT_DLY
WHERE SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.RB_CLNT_DLY WHERE SNAP_DT >= DATE - 7)
GROUP BY CLNT_TYP, CLNT_STS
ORDER BY CLNT_TYP, CLNT_STS;
"""
df_q0 = edw_query(sql_q0, "Q0")
display(df_q0)


# %% [1b] Coverage probe - RUN ONCE BEFORE Q1/Q2/Q3 (now load-bearing): the
# date-matched joins need EVERY month-end 2024-01-31 .. 2026-07-31 present in
# RB_CLNT_DLY. If the daily table keeps only a rolling window, history joins
# silently empty out. Expect ~31 month-end rows, each in the tens of millions.

sql_coverage = """
SELECT SNAP_DT, COUNT(*) AS n
FROM DDWV01.RB_CLNT_DLY
WHERE SNAP_DT >= DATE '2024-01-31' AND EXTRACT(DAY FROM SNAP_DT + 1) = 1
GROUP BY 1 ORDER BY 1;   -- expect ~31 month-end rows, each in the tens of millions
"""
df_coverage = edw_query(sql_coverage, "coverage")
display(df_coverage)


# %% [2] Q1 - monthly vendor activity x MNE since 2024-01, sends and unsubs
# side by side (clnt_no grain). unsub_clients = first unsub OF THE MONTH per
# client; first_unsub_clients = first unsub of the WHOLE WINDOW per client
# (window sum = distinct unsubscribing clients).

sql_q1 = """
WITH act AS (
    -- ACTIVE spine, date-matched (Andre 2026-08-21): one row per month-end x
    -- active client. Each event below joins to the status snapshot of ITS OWN
    -- month - never one fixed snapshot across the series. Month-end-only rows
    -- kept via EXTRACT (SNAP_DT+1 = day 1 <=> SNAP_DT is a month-end); if this
    -- predicate defeats partition pruning and the scan crawls, swap it for an
    -- explicit SNAP_DT IN (...) list of month-ends.
    SELECT SNAP_DT, CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE CLNT_STS = 'A'   -- Q0 2026-08-21: A=14.86M, I=14.07M, null=463K (CLNT_TYP=1); quoted - CHAR column
      AND SNAP_DT >= DATE '2024-01-31'
      AND EXTRACT(DAY FROM SNAP_DT + 1) = 1
      AND CLNT_TYP = 1   -- PERSONAL only (Andre 2026-08-21: every query, every number).
                         -- Source: RB_CLNT_DLY type code; Q0 shows type 1 = 29.4M
                         -- (personal-scale) vs type 2 = 3.9M. CPC-side queries use
                         -- CLNT_TYP_CD = 1 on CPC_RB_PREF_MTHLY for the same cut.
),
base AS (
    SELECT m.CLNT_NO,
           e.disposition_cd,
           e.disposition_dt_tm AS dt,
           e.TREATMENT_ID,
           SUBSTR(e.TREATMENT_ID, 8, 3) AS mne,
           TRIM(EXTRACT(YEAR FROM e.disposition_dt_tm)) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM e.disposition_dt_tm) < 10
                       THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM e.disposition_dt_tm))       AS evt_month
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2023274' AND '2026212'
                  AND CLNT_NO IS NOT NULL) m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    -- active check at the event's own month-end: the client must be 'A' in the
    -- month the send/unsub happened, not merely today
    INNER JOIN act ON act.CLNT_NO = m.CLNT_NO
                  AND act.SNAP_DT = ADD_MONTHS(CAST(e.disposition_dt_tm AS DATE)
                                               - EXTRACT(DAY FROM e.disposition_dt_tm) + 1, 1) - 1
    WHERE e.disposition_cd IN (1, 4)
      AND e.disposition_dt_tm >= DATE '2024-01-01'
      AND e.disposition_dt_tm <  DATE '2026-08-01'
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
),
sends AS (
    SELECT evt_month, mne, CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS sent_clients
    FROM base
    WHERE disposition_cd = 1
    GROUP BY 1, 2
),
unsub_ranked AS (
    -- TWO ranks per unsub row (Andre 2026-08-24):
    --   rn_month  - first per client per MONTH  -> monthly view (a client counts
    --               once each month they unsub; repeats across months reappear)
    --   rn_window - first per client EVER       -> unique view (client counts
    --               once in the whole window, in the month + MNE of their first
    --               unsub; window sum = unique unsubscribing clients)
    SELECT evt_month, CLNT_NO, mne,
           ROW_NUMBER() OVER (PARTITION BY CLNT_NO, evt_month
                              ORDER BY dt ASC, mne ASC, TREATMENT_ID ASC) AS rn_month,
           ROW_NUMBER() OVER (PARTITION BY CLNT_NO
                              ORDER BY dt ASC, mne ASC, TREATMENT_ID ASC) AS rn_window
    FROM base
    WHERE disposition_cd = 4
),
unsubs AS (
    SELECT evt_month, mne,
           CAST(SUM(CASE WHEN rn_month  = 1 THEN 1 ELSE 0 END) AS BIGINT) AS unsub_clients,
           CAST(SUM(CASE WHEN rn_window = 1 THEN 1 ELSE 0 END) AS BIGINT) AS first_unsub_clients
    FROM unsub_ranked
    GROUP BY 1, 2
)
SELECT COALESCE(s.evt_month, un.evt_month) AS evt_month,
       COALESCE(s.mne, un.mne)             AS mne,
       COALESCE(s.sent_clients, 0)         AS sent_clients,
       COALESCE(un.unsub_clients, 0)       AS unsub_clients,
       COALESCE(un.first_unsub_clients, 0) AS first_unsub_clients
FROM sends s
FULL OUTER JOIN unsubs un
  ON un.evt_month = s.evt_month AND un.mne = s.mne
ORDER BY 1, 2;
"""
df_q1 = edw_query(sql_q1, "Q1")
display(df_q1.head(20))


# %% [2a] Q1 deck table - monthly totals across MNEs, and the chart comparing
# the two client counts the query exposes: repeat-eligible monthly unsubs vs
# unique first-of-window unsubs.

monthly_totals = (df_q1.groupby("evt_month", as_index=False)
                   .agg(sent_clients=("sent_clients", "sum"),
                        unsub_clients=("unsub_clients", "sum"),
                        first_unsub_clients=("first_unsub_clients", "sum"))
                   .sort_values("evt_month").reset_index(drop=True))
display(monthly_totals)

total_unsub_client_months = int(monthly_totals["unsub_clients"].sum())
total_first_unsub_clients = int(monthly_totals["first_unsub_clients"].sum())

fig, ax = plt.subplots(figsize=(12, 5))
x = range(len(monthly_totals))
w = 0.38
ax.bar([i - w / 2 for i in x], monthly_totals["unsub_clients"], width=w,
       color=C_LINE, label="unsub_clients (monthly, repeat months count)")
ax.bar([i + w / 2 for i in x], monthly_totals["first_unsub_clients"], width=w,
       color=C_THEN, label="first_unsub_clients (unique, window-wide)")
ax.set_xticks(list(x))
ax.set_xticklabels(monthly_totals["evt_month"], rotation=45, fontsize=8)
ax.set_ylabel("clients")
ax.legend(loc="upper left", fontsize=9, frameon=False)
style_ax(ax)
ax.set_title(f"{total_unsub_client_months:,} client-months vs {total_first_unsub_clients:,} "
             f"unique clients - monthly unsub vs first-unsub-of-window",
             fontweight="bold", loc="left")
plt.tight_layout(); plt.show()


# %% [2b] Q1 - top-10 MNE by first_unsub_clients over the whole window (unique
# clients; multi-MNE clients counted once, under their first unsub's MNE).

top10_mne = (df_q1.groupby("mne", as_index=False)["first_unsub_clients"].sum()
             .sort_values("first_unsub_clients", ascending=False)
             .head(10).reset_index(drop=True))
display(top10_mne)

fig, ax = plt.subplots(figsize=(9, 5))
yy = list(range(len(top10_mne)))[::-1]
ax.barh(yy, top10_mne["first_unsub_clients"], color=C_STEEL)
for yi, v in zip(yy, top10_mne["first_unsub_clients"]):
    ax.text(v, yi, f" {v:,.0f}", va="center", fontsize=9)
ax.set_yticks(yy)
ax.set_yticklabels(top10_mne["mne"])
ax.set_xlabel("first_unsub_clients (unique clients, window total)")
style_ax(ax)
ax.set_title("Top-10 MNE by first-unsub clients, 2024-01 -> 2026-07",
             fontweight="bold", loc="left")
plt.tight_layout(); plt.show()


# %% [3] Q2 - CPC 1012 monthly x writer: standing (stock) and writes (flow)
# side by side. Chart splits n_writes_to_no by writer: 7020 (SFMC email
# backfeed) vs every other APP_SYS_CD grouped as 'Other'.

sql_q2 = """
WITH act AS (
    -- ACTIVE spine, date-matched (Andre 2026-08-21): one row per month-end x
    -- active client. Standing joins at its OWN MTH_END_DT; writes join at the
    -- month-end of the write's month - never one fixed snapshot across the
    -- series. Swap the EXTRACT month-end predicate for an explicit
    -- SNAP_DT IN (...) list if partition pruning suffers.
    SELECT SNAP_DT, CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE CLNT_STS = 'A'   -- Q0 2026-08-21: A=14.86M, I=14.07M, null=463K (CLNT_TYP=1); quoted - CHAR column
      AND SNAP_DT >= DATE '2024-01-31'
      AND EXTRACT(DAY FROM SNAP_DT + 1) = 1
      AND CLNT_TYP = 1   -- PERSONAL only (Andre 2026-08-21: every query, every number).
                         -- Source: RB_CLNT_DLY type code; Q0 shows type 1 = 29.4M
                         -- (personal-scale) vs type 2 = 3.9M. CPC-side queries use
                         -- CLNT_TYP_CD = 1 on CPC_RB_PREF_MTHLY for the same cut.
),
standing AS (
    SELECT TRIM(EXTRACT(YEAR FROM MTH_END_DT)) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM MTH_END_DT) < 10 THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM MTH_END_DT))                AS mth,
           APP_SYS_CD,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5001 THEN 1 ELSE 0 END) AS BIGINT) AS n_5001_yes,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS BIGINT) AS n_5002_no,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5003 THEN 1 ELSE 0 END) AS BIGINT) AS n_5003_blank,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5004 THEN 1 ELSE 0 END) AS BIGINT) AS n_5004_yes_credit_bureau,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP NOT IN (5001, 5002, 5003, 5004)
                         THEN 1 ELSE 0 END) AS BIGINT)                              AS n_other_value
    FROM DDWV01.CPC_RB_PREF_MTHLY p
    -- active check at the SAME month-end the standing is measured at
    INNER JOIN act ON act.CLNT_NO = p.CLNT_NO
                  AND act.SNAP_DT = p.MTH_END_DT
    WHERE p.PREF_ID = 1012
      AND p.MTH_END_DT >= DATE '2024-01-31'
      AND p.CLNT_TYP_CD = 1
    GROUP BY 1, 2
),
writes AS (
    -- ALL writes that month by target value (not just 5002 - Andre 2026-08-20:
    -- yes-writes expose bulk subscribe events, blank-writes expose resolutions)
    SELECT TRIM(EXTRACT(YEAR FROM CAST(CHG_TMSTMP AS DATE))) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)) < 10
                       THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)))  AS mth,
           APP_SYS_CD,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5001 THEN 1 ELSE 0 END) AS BIGINT) AS n_writes_to_yes,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS BIGINT) AS n_writes_to_no,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5003 THEN 1 ELSE 0 END) AS BIGINT) AS n_writes_to_blank,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5004 THEN 1 ELSE 0 END) AS BIGINT) AS n_writes_to_5004,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP NOT IN (5001, 5002, 5003, 5004)
                         THEN 1 ELSE 0 END) AS BIGINT)                              AS n_writes_to_other
    FROM DDWV01.CPC_RB_PREF w
    -- active check at the month-end of the WRITE's month (RB_CLNT_DLY is the
    -- status source; write-day precision would need daily snapshots verified)
    INNER JOIN act ON act.CLNT_NO = w.CLNT_NO
                  AND act.SNAP_DT = ADD_MONTHS(CAST(w.CHG_TMSTMP AS DATE)
                                               - EXTRACT(DAY FROM w.CHG_TMSTMP) + 1, 1) - 1
    WHERE w.PREF_ID = 1012
      AND w.CHG_TMSTMP >= DATE '2024-01-01'
      AND w.CHG_TMSTMP <  DATE '2026-08-01'  -- file cutoff; also keeps the partial
                                             -- current month from silently dropping
                                             -- on a not-yet-existing month-end snapshot
    GROUP BY 1, 2
)
SELECT COALESCE(s.mth, w.mth)                 AS mth,
       COALESCE(s.APP_SYS_CD, w.APP_SYS_CD)   AS app_sys_cd,
       COALESCE(s.n_5001_yes, 0)              AS n_5001_yes,
       COALESCE(s.n_5002_no, 0)               AS n_5002_no,
       COALESCE(s.n_5003_blank, 0)            AS n_5003_blank,
       COALESCE(s.n_5004_yes_credit_bureau, 0) AS n_5004_yes_credit_bureau,
       COALESCE(s.n_other_value, 0)           AS n_other_value,
       COALESCE(w.n_writes_to_yes, 0)         AS n_writes_to_yes,
       COALESCE(w.n_writes_to_no, 0)          AS n_writes_to_no,
       COALESCE(w.n_writes_to_blank, 0)       AS n_writes_to_blank,
       COALESCE(w.n_writes_to_5004, 0)        AS n_writes_to_5004,
       COALESCE(w.n_writes_to_other, 0)       AS n_writes_to_other
FROM standing s
FULL OUTER JOIN writes w
  ON w.mth = s.mth AND w.APP_SYS_CD = s.APP_SYS_CD
ORDER BY 1, 2;
"""
df_q2 = edw_query(sql_q2, "Q2")
display(df_q2.head(20))

writes_by_writer = df_q2.copy()
writes_by_writer["writer"] = writes_by_writer["app_sys_cd"].apply(
    lambda c: "7020 (email backfeed)" if pd.notnull(c) and int(c) == 7020 else "Other")
writes_piv = (writes_by_writer.groupby(["mth", "writer"], as_index=False)["n_writes_to_no"].sum()
              .pivot(index="mth", columns="writer", values="n_writes_to_no")
              .fillna(0).sort_index())
display(writes_piv)

mar_jul_2026 = [m for m in writes_piv.index if "2026-03" <= m <= "2026-07"]
collapse_sum = int(writes_piv.loc[mar_jul_2026, "7020 (email backfeed)"].sum()) \
    if "7020 (email backfeed)" in writes_piv.columns else 0

fig, ax = plt.subplots(figsize=(12, 5))
x = range(len(writes_piv))
bottom = [0.0] * len(writes_piv)
for writer, color in [("7020 (email backfeed)", C_LINE), ("Other", C_MUTE)]:
    if writer in writes_piv.columns:
        vals = writes_piv[writer].tolist()
        ax.bar(x, vals, bottom=bottom, width=0.65, color=color, label=writer)
        bottom = [a + b for a, b in zip(bottom, vals)]
ax.set_xticks(list(x))
ax.set_xticklabels(writes_piv.index, rotation=45, fontsize=8)
ax.set_ylabel("n_writes_to_no")
ax.legend(loc="upper right", fontsize=9, frameon=False)
style_ax(ax)
ax.set_title(f"CPC 1012 writes to No by writer - 7020 collapses to {collapse_sum:,} "
             f"over Mar-Jul 2026", fontweight="bold", loc="left")
plt.tight_layout(); plt.show()


# %% [4] Q3 - THE WATERFALL, Aug-2024 -> Jul-2026 (status checked at BOTH
# anchors). Single-row result; every number in the chart below is read off
# this row, nothing hardcoded.

sql_q3 = """
WITH u_a AS (
    -- Universe at the START anchor: CLNT_STS is CHAR, 'A' = active; status
    -- date matches the CPC anchor MTH_END_DT on the Aug-24 side (`a` below).
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2024-08-31'
      AND CLNT_STS = 'A'   -- Q0 2026-08-21: A=14.86M, I=14.07M, null=463K (CLNT_TYP=1); quoted — CHAR column
),
u_b AS (
    -- Universe at the END anchor: CLNT_STS is CHAR, 'A' = active; status
    -- date matches the CPC anchor MTH_END_DT on the Jul-26 side (`b` below).
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2026-07-31'
      AND CLNT_STS = 'A'   -- Q0 2026-08-21: A=14.86M, I=14.07M, null=463K (CLNT_TYP=1); quoted — CHAR column
),
a AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_a
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2024-08-31'
      AND CLNT_TYP_CD = 1
),
b AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b, APP_SYS_CD
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2026-07-31'
      AND CLNT_TYP_CD = 1
),
v AS (
    SELECT DISTINCT m.CLNT_NO
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2024153' AND '2026212'
                  AND CLNT_NO IS NOT NULL) m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-09-01'
      AND e.disposition_dt_tm <  DATE '2026-08-01'
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
),
j AS (
    -- One row per client appearing in `a` and/or `b`. active_a/active_b are
    -- looked up per-anchor against u_a/u_b (no single inner-joined universe
    -- anymore - a client's book status can differ across the two dates).
    -- writer_b is APP_SYS_CD from `b` - the system that wrote the Jul-26 row.
    SELECT COALESCE(a.CLNT_NO, b.CLNT_NO)                        AS CLNT_NO,
           a.cons_a,
           b.cons_b,
           b.APP_SYS_CD                                          AS writer_b,
           CASE WHEN ua.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END    AS active_a,
           CASE WHEN ub.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END    AS active_b,
           CASE WHEN v.CLNT_NO  IS NOT NULL THEN 1 ELSE 0 END    AS vendor_unsub
    FROM a
    FULL OUTER JOIN b ON a.CLNT_NO = b.CLNT_NO
    LEFT JOIN u_a ua ON ua.CLNT_NO = COALESCE(a.CLNT_NO, b.CLNT_NO)
    LEFT JOIN u_b ub ON ub.CLNT_NO = COALESCE(a.CLNT_NO, b.CLNT_NO)
    LEFT JOIN v       ON v.CLNT_NO  = COALESCE(a.CLNT_NO, b.CLNT_NO)
)
SELECT
    -- start bar: cons=5001 AND active, at the Aug-24 anchor
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS start_5001_aug24,

    -- additions: wasn't (5001 AND active) at the start, but is at the end -
    -- covers both newly-consented clients and previously-inactive clients
    -- who came back active while already sitting on 5001. Written as an
    -- explicit disjunction (not NOT(...)) so a NULL cons_a (client absent
    -- from `a` entirely) reads as "not in book", not as unknown/excluded.
    CAST(SUM(CASE WHEN (cons_a IS NULL OR cons_a <> 5001 OR active_a = 0)
                   AND cons_b = 5001 AND active_b = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS new_5001,

    -- 1. left the active book - checked first, takes precedence over consent
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND active_b = 0
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_inactive,

    -- 2. closed by CPC/email backfeed (7020), no Salesforce record
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND active_b = 1 AND cons_b = 5002
                   AND writer_b = 7020 AND vendor_unsub = 0
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_email_cpc,

    -- 3. closed AND has a Salesforce unsub record, any writer
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND active_b = 1 AND cons_b = 5002 AND vendor_unsub = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_overlap,

    -- 4. closed by a writer other than 7020, no Salesforce record.
    -- COALESCE(writer_b, -1) so a NULL writer_b lands here (not silently
    -- dropped) - this keeps segs 2/3/4 a full partition of the
    -- active_b=1, cons_b=5002 space regardless of writer nullability.
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND active_b = 1 AND cons_b = 5002
                   AND COALESCE(writer_b, -1) <> 7020 AND vendor_unsub = 0
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_ac_branch,

    -- 5. still active, but landed on a consent value that's neither
    -- 5001 nor 5002 (blank/other/NULL) - no consent value silently vanishes
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND active_b = 1
                   AND (cons_b IS NULL OR cons_b NOT IN (5001, 5002))
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS left_other,

    -- inside end_5001_jul26 (still 5001 AND active on both sides) but carries
    -- a Salesforce unsub record - a contactable adjustment, NOT a departure
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND cons_b = 5001 AND active_b = 1 AND vendor_unsub = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_email_sf_open,

    -- end bar (official): cons=5001 AND active, at the Jul-26 anchor
    CAST(SUM(CASE WHEN cons_b = 5001 AND active_b = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS end_5001_jul26,

    -- end bar (contactable): official end minus the still-5001/active
    -- clients that carry a Salesforce unsub record
    CAST((SUM(CASE WHEN cons_b = 5001 AND active_b = 1 THEN 1 ELSE 0 END)
        - SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                     AND cons_b = 5001 AND active_b = 1 AND vendor_unsub = 1
                    THEN 1 ELSE 0 END)) AS BIGINT)                           AS end_contactable
FROM j;
"""
df_q3 = edw_query(sql_q3, "Q3")
display(df_q3.T.rename(columns={0: "value"}))

r = df_q3.iloc[0]
start_v = float(r["start_5001_aug24"])
new_v = float(r["new_5001"])
seg_inactive = float(r["seg_inactive"])
seg_email_cpc = float(r["seg_email_cpc"])
seg_overlap = float(r["seg_overlap"])
seg_ac_branch = float(r["seg_ac_branch"])
left_other = float(r["left_other"])
seg_email_sf_open = float(r["seg_email_sf_open"])
end_official = float(r["end_5001_jul26"])
end_contactable = float(r["end_contactable"])
combined_branch = seg_ac_branch + left_other  # "Ac/branch" column carries left_other too

# identity: start + new - (5 true CPC departures + the SF-open adjustment) = end_contactable
departures_to_contactable = (seg_inactive + seg_email_cpc + seg_overlap +
                              seg_ac_branch + left_other + seg_email_sf_open)
identity_ok = abs((start_v + new_v - departures_to_contactable) - end_contactable) < 1
print(f"Identity check (start + new - all six departure/adjustment segments = "
      f"end_contactable): {'HOLDS' if identity_ok else 'BROKEN - investigate before using'}")
print(f"end_official ({end_official:,.0f}) - seg_email_sf_open ({seg_email_sf_open:,.0f}) "
      f"= {end_official - seg_email_sf_open:,.0f} vs end_contactable ({end_contactable:,.0f})")

# deck-ready chart-feed - exact stacked-column-waterfall layout (categories in
# rows, one column per chart series; blanks are 0 so Excel/matplotlib treat
# them as no-bar)
chart_feed = pd.DataFrame([
    {"Column1": "Aug'24", "Base": 0.0, "Solid": start_v, "Subscribers": 0.0,
     "Inactive": 0.0, "Ac/branch": 0.0, "Email SF": 0.0,
     "Overlap SF & CPC": 0.0, "Email CPC": 0.0, "Unsub SF -> Open CPC": 0.0},
    {"Column1": "subscriber", "Base": start_v, "Solid": 0.0, "Subscribers": new_v,
     "Inactive": 0.0, "Ac/branch": 0.0, "Email SF": 0.0,
     "Overlap SF & CPC": 0.0, "Email CPC": 0.0, "Unsub SF -> Open CPC": 0.0},
    {"Column1": "unsubscribe", "Base": end_contactable, "Solid": 0.0, "Subscribers": 0.0,
     "Inactive": seg_inactive, "Ac/branch": combined_branch, "Email SF": seg_email_sf_open,
     "Overlap SF & CPC": seg_overlap, "Email CPC": seg_email_cpc, "Unsub SF -> Open CPC": 0.0},
    {"Column1": "Jul'26", "Base": 0.0, "Solid": end_contactable, "Subscribers": 0.0,
     "Inactive": 0.0, "Ac/branch": 0.0, "Email SF": 0.0,
     "Overlap SF & CPC": 0.0, "Email CPC": 0.0, "Unsub SF -> Open CPC": seg_email_sf_open},
])
display(chart_feed)

# the waterfall chart itself, built from the same r / chart_feed values
M = 1e6
fig, ax = plt.subplots(figsize=(11.5, 6))
lo = min(start_v, end_contactable) / M * 0.93

ax.bar(0, start_v / M - lo, bottom=lo, width=0.6, color=C_THEN, zorder=3)
ax.text(0, start_v / M + 0.05, f"{start_v / M:.2f}", ha="center", fontsize=11, fontweight="bold")

ax.bar(1, new_v / M, bottom=start_v / M, width=0.6, color=C_GREEN, zorder=3)
ax.text(1, (start_v + new_v) / M + 0.05, f"+{new_v / M:.2f}", ha="center", fontsize=11, fontweight="bold")
top_after_new = (start_v + new_v) / M

segments = [
    ("Inactive", seg_inactive, C_MUTE),
    ("Ac/branch (+ left/other)", combined_branch, C_STEEL),
    ("Email SF", seg_email_sf_open, C_SAND),
    ("Overlap SF & CPC", seg_overlap, C_GOLD),
    ("Email CPC", seg_email_cpc, C_LINE),
]
base = top_after_new
for lbl, v, c in segments:
    h = v / M
    ax.bar(2, -h, bottom=base, width=0.6, color=c, zorder=3, edgecolor="white",
           linewidth=1.2, label=lbl)
    if h > 0.01:
        ax.text(2, base - h / 2, f"-{h:.2f}", ha="center", va="center", fontsize=9)
    base -= h
ax.text(2, top_after_new + 0.05, f"-{(top_after_new - base):.2f}", ha="center",
        fontsize=11, fontweight="bold")

ax.bar(3, end_contactable / M - lo, bottom=lo, width=0.6, color=C_THEN, zorder=3)
ax.text(3, end_contactable / M + 0.05, f"{end_contactable / M:.2f} contactable", ha="center",
        fontsize=10, fontweight="bold")
ax.bar(3, seg_email_sf_open / M, bottom=end_contactable / M, width=0.6, color=C_SAND, zorder=3,
       edgecolor="white", linewidth=1.2,
       label="Unsub SF -> Open CPC (still official, not contactable)")
ax.text(3, (end_contactable + seg_email_sf_open) / M + 0.05,
        f"{(end_contactable + seg_email_sf_open) / M:.2f} official", ha="center", fontsize=10,
        fontweight="bold")

ax.set_xticks([0, 1, 2, 3])
ax.set_xticklabels(["Aug'24", "Subscribers", "Unsubscribe\n(5 segments)", "Jul'26"], fontsize=10)
ax.set_ylabel("# clients (M)")
ax.set_ylim(lo, top_after_new * 1.05)
style_ax(ax)
ax.legend(loc="upper left", fontsize=8, frameon=False)
ax.set_title(f"Subscriber book bridge - Aug'24 {start_v / M:.2f}M -> Jul'26 "
             f"{end_contactable / M:.2f}M contactable "
             f"({(end_contactable + seg_email_sf_open) / M:.2f}M official)",
             fontweight="bold", loc="left")
plt.tight_layout(); plt.show()


# %% [5] Q4 - SF-open blind-spot split by MNE: what did those clients actually
# unsub from, while 1012 (CPC) stayed open the whole time?

sql_q4 = """
WITH u_a AS (
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2024-08-31'
      AND CLNT_STS = 'A'
),
u_b AS (
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2026-07-31'
      AND CLNT_STS = 'A'
),
a AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_a
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2024-08-31'
      AND CLNT_TYP_CD = 1
),
b AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2026-07-31'
      AND CLNT_TYP_CD = 1
),
vm AS (
    -- one row per client: MNE of the FIRST disposition-4 in the window
    -- (locked EVENT+MASTER merge, identical filters to Q3's v)
    SELECT CLNT_NO, mne
    FROM (
        SELECT m.CLNT_NO,
               SUBSTR(e.TREATMENT_ID, 8, 3) AS mne,
               ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO
                                  ORDER BY e.disposition_dt_tm ASC,
                                           e.TREATMENT_ID ASC) AS rn
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2024153' AND '2026212'
                      AND CLNT_NO IS NOT NULL) m
          ON  m.consumer_id_hashed = e.consumer_id_hashed
          AND m.TREATMENT_ID       = e.TREATMENT_ID
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= DATE '2024-09-01'
          AND e.disposition_dt_tm <  DATE '2026-08-01'
          AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
          AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    ) t
    WHERE rn = 1
)
SELECT vm.mne,
       CAST(COUNT(*) AS BIGINT) AS n_clients
FROM a
INNER JOIN b   ON b.CLNT_NO   = a.CLNT_NO
INNER JOIN u_a ON u_a.CLNT_NO = a.CLNT_NO
INNER JOIN u_b ON u_b.CLNT_NO = a.CLNT_NO
INNER JOIN vm  ON vm.CLNT_NO  = a.CLNT_NO
WHERE a.cons_a = 5001
  AND b.cons_b = 5001
GROUP BY vm.mne
ORDER BY n_clients DESC;
"""
df_q4 = edw_query(sql_q4, "Q4")
top15_q4 = df_q4.sort_values("n_clients", ascending=False).head(15).reset_index(drop=True)
display(top15_q4)

total_q4 = int(df_q4["n_clients"].sum())
top1_share_pct = (top15_q4["n_clients"].iloc[0] / total_q4 * 100) if total_q4 else 0.0

fig, ax = plt.subplots(figsize=(9, 6))
yy = list(range(len(top15_q4)))[::-1]
ax.barh(yy, top15_q4["n_clients"], color=C_STEEL)
for yi, v in zip(yy, top15_q4["n_clients"]):
    ax.text(v, yi, f" {v:,.0f}", va="center", fontsize=9)
ax.set_yticks(yy)
ax.set_yticklabels(top15_q4["mne"])
ax.set_xlabel("n_clients")
style_ax(ax)
ax.set_title(f"Top-15 MNE, SF-open blind spot - top MNE is {top1_share_pct:.0f}% of "
             f"{total_q4:,} total", fontweight="bold", loc="left")
plt.tight_layout(); plt.show()


# %% [6] Q5 - did the blind-spot clients ever see the preference page? Split by
# send family (Loyalty vs Other) x page outcome (up to 3 outcomes each).

sql_q5 = """
WITH u_a AS (
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2024-08-31'
      AND CLNT_STS = 'A'
),
u_b AS (
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2026-07-31'
      AND CLNT_STS = 'A'
),
a AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_a
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2024-08-31'
      AND CLNT_TYP_CD = 1
),
b AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2026-07-31'
      AND CLNT_TYP_CD = 1
),
vm AS (
    -- first SF unsub per client: its date AND its sending campaign MNE
    SELECT CLNT_NO, unsub_dt, mne
    FROM (
        SELECT m.CLNT_NO,
               CAST(e.disposition_dt_tm AS DATE)  AS unsub_dt,
               SUBSTR(e.TREATMENT_ID, 8, 3)       AS mne,
               ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO
                                  ORDER BY e.disposition_dt_tm ASC,
                                           e.TREATMENT_ID ASC) AS rn
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2024153' AND '2026212'
                      AND CLNT_NO IS NOT NULL) m
          ON  m.consumer_id_hashed = e.consumer_id_hashed
          AND m.TREATMENT_ID       = e.TREATMENT_ID
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= DATE '2024-09-01'
          AND e.disposition_dt_tm <  DATE '2026-08-01'
          AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
          AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    ) t
    WHERE rn = 1
),
cohort AS (
    SELECT vm.CLNT_NO, vm.unsub_dt, vm.mne
    FROM a
    INNER JOIN b   ON b.CLNT_NO   = a.CLNT_NO
    INNER JOIN u_a ON u_a.CLNT_NO = a.CLNT_NO
    INNER JOIN u_b ON u_b.CLNT_NO = a.CLNT_NO
    INNER JOIN vm  ON vm.CLNT_NO  = a.CLNT_NO
    WHERE a.cons_a = 5001
      AND b.cons_b = 5001
),
wr AS (
    -- one row per cohort client who has at least one opt-out write (any gate)
    -- within [-1, +14] days of their first unsub; did any of them hit 1012?
    SELECT c.CLNT_NO,
           MAX(CASE WHEN w.PREF_ID = 1012 THEN 1 ELSE 0 END) AS wrote_1012,
           MAX(CASE WHEN w.PREF_ID <> 1012 THEN 1 ELSE 0 END) AS wrote_other
    FROM cohort c
    INNER JOIN DDWV01.CPC_RB_PREF w
      ON  w.CLNT_NO = c.CLNT_NO
      AND w.CLNT_CONSENT_TYP = 5002
      AND CAST(w.CHG_TMSTMP AS DATE) >= c.unsub_dt - 1
      AND CAST(w.CHG_TMSTMP AS DATE) <= c.unsub_dt + 14
    WHERE w.CHG_TMSTMP >= DATE '2024-08-01'   -- scan floor; window starts 2024-09
    GROUP BY c.CLNT_NO
)
SELECT CASE WHEN c.mne IN ('VRE', 'VME', 'VRG')   -- EDIT ME: extend from mapping Mne.csv
            THEN 'Loyalty send' ELSE 'Other send' END          AS send_family,
       CASE WHEN wr.CLNT_NO IS NULL               THEN '1. no write on ANY gate - never saw the page'
            WHEN wr.wrote_1012 = 1                THEN '2. wrote 1012 in-window - late/reverted edge'
            ELSE                                       '3. wrote OTHER gate only - chose a different gate on the page'
       END                                                     AS page_outcome,
       CAST(COUNT(*) AS BIGINT)                                AS n_clients,
       CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(5,1)) AS pct
FROM cohort c
LEFT JOIN wr ON wr.CLNT_NO = c.CLNT_NO
GROUP BY 1, 2
ORDER BY 1, 2;
"""
df_q5 = edw_query(sql_q5, "Q5")
display(df_q5)

no_write_mask = df_q5["page_outcome"].astype(str).str.startswith("1.")
no_write_pct_combined = float(pd.to_numeric(df_q5.loc[no_write_mask, "pct"]).sum())

top6 = df_q5.reset_index(drop=True)
labels = top6["send_family"].astype(str) + " - " + top6["page_outcome"].astype(str)

fig, ax = plt.subplots(figsize=(10, 5.5))
yy = list(range(len(top6)))[::-1]
ax.barh(yy, top6["n_clients"], color=C_STEEL)
for yi, n, p in zip(yy, top6["n_clients"], top6["pct"]):
    ax.text(n, yi, f" {n:,.0f} ({float(p):.1f}%)", va="center", fontsize=9)
ax.set_yticks(yy)
ax.set_yticklabels(labels, fontsize=9)
ax.set_xlabel("n_clients")
style_ax(ax)
ax.set_title(f"Blind-spot page behavior - {no_write_pct_combined:.1f}% never wrote on ANY "
             f"gate (both send families combined)", fontweight="bold", loc="left")
plt.tight_layout(); plt.show()
