# %% [0] Setup — Vendor Feedback (Email) EDA config + EDW helper
# Source query pack: unsub_tracking/01_vendor_feedback_eda.sql (Q0-Q5), queries below are verbatim from that file.
# Tables: DTZV01.VENDOR_FEEDBACK_MASTER (email send master) + DTZV01.VENDOR_FEEDBACK_EVENT (disposition events).
# Engine: Teradata-direct via pre-initialized EDW connector — no login/connection code here.
# disposition_cd (confirmed AUH Phase 1): 1=sent 2=opened 3=clicked 4=unsubscribed 5=hardbounce 6=complaint.
# Corrected after first run (2026-07-14): MASTER has NO SEND_DT — send timing comes from the decisioning
#   table (TACTIC_EVNT_IP_AR) via m.TREATMENT_ID = t.TACTIC_ID + m.CLNT_NO = t.CLNT_NO.
#   FEEDBACK_ID does NOT exist — only MASTER<->EVENT join path is consumer_id_hashed + TREATMENT_ID.
# Fan-out caveat: a joined count ABOVE the no-join denominator means duplicate keys on the
#   MASTER side — that's a grain finding, not noise.

import pandas as pd
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', 100)

# ── Editable window boundaries (edit here only — f-strung into queries below) ──
WIN_START = '2026-04-01'        # Q3/Q4 recent window start
WIN_END = '2026-07-01'          # Q3/Q4 recent window end (exclusive)
TREND_START = '2024-01-01'      # Q1c monthly trend (TREATMT_STRT_DT)
HIST_START = '2024-01-01'       # history floor for EVENT scans (data reaches ~2018; pre-2024 excluded)
TRAILING_START = '2025-07-01'   # Q5 trailing 12 months start
TRAILING_END = '2026-07-01'     # Q5 trailing 12 months end (exclusive)


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


print("Setup complete.")
print(f"Windows: recent=[{WIN_START}, {WIN_END}) | trend_start={TREND_START} | trailing=[{TRAILING_START}, {TRAILING_END})")


# %% [1] Q0a — MASTER full column catalog (TOP 5 sample rows)

sql = """
SELECT TOP 5 * FROM DTZV01.VENDOR_FEEDBACK_MASTER
"""
df_q0a = edw_query(sql, "Q0a")
print(df_q0a.to_string(index=False))

cols_master = df_q0a.columns.tolist()
cols_master_upper = [c.upper() for c in cols_master]
expected_master = ['TREATMENT_ID', 'CLNT_NO', 'CONSUMER_ID_HASHED']  # SEND_DT/FEEDBACK_ID confirmed absent 2026-07-14
missing_master = [c for c in expected_master if c not in cols_master_upper]
print(f"\nQ0a proves: full column list for VENDOR_FEEDBACK_MASTER ({len(cols_master)} columns): {cols_master}")
if missing_master:
    print(f"  WARNING: expected repo-confirmed columns not found: {missing_master} — check for renamed/aliased columns above.")
else:
    print(f"  Repo-confirmed columns present: {expected_master}")


# %% [2] Q0b — EVENT full column catalog (TOP 5 sample rows)

sql = """
SELECT TOP 5 * FROM DTZV01.VENDOR_FEEDBACK_EVENT
"""
df_q0b = edw_query(sql, "Q0b")
print(df_q0b.to_string(index=False))

cols_event = df_q0b.columns.tolist()
cols_event_upper = [c.upper() for c in cols_event]
expected_event = ['DISPOSITION_CD', 'DISPOSITION_DT_TM', 'CONSUMER_ID_HASHED', 'TREATMENT_ID']  # EVENT_TYPE does not exist; 9-col catalog in schemas/vendor_feedback_tables_schema.md
missing_event = [c for c in expected_event if c not in cols_event_upper]
print(f"\nQ0b proves: full column list for VENDOR_FEEDBACK_EVENT ({len(cols_event)} columns): {cols_event}")
if missing_event:
    print(f"  WARNING: expected repo-confirmed columns not found: {missing_event} — check for renamed/aliased columns above.")
else:
    print(f"  Repo-confirmed columns present: {expected_event}")


# %% [3] Q1a — MASTER volume, cardinality (no date range — MASTER has no send-date column)

sql = """
SELECT
    CAST(COUNT(*) AS BIGINT)        AS master_rows,
    COUNT(DISTINCT CLNT_NO)         AS distinct_clients,
    COUNT(DISTINCT TREATMENT_ID)    AS distinct_treatments
FROM DTZV01.VENDOR_FEEDBACK_MASTER
"""
df_q1a = edw_query(sql, "Q1a")
print(df_q1a.to_string(index=False))

r = df_q1a.iloc[0]
q1a_master_rows = int(r['master_rows'])
print(f"\nQ1a proves: MASTER has {q1a_master_rows:,} rows, {r['distinct_clients']:,} distinct clients, "
      f"{r['distinct_treatments']:,} distinct treatments. Send timing comes via the decisioning join (Q1b/Q1c).")


# %% [4] Q1b — MASTER -> decisioning join coverage (TREATMENT_ID = TACTIC_ID + CLNT_NO)
# EXISTS avoids fan-out from multi-wave (CLNT_NO, TACTIC_ID) duplicates.
# Using DG6V01.TACTIC_EVNT_IP_AR_HIST; alternative: DTZV01.TACTIC_EVNT_IP_AR_H60M.

sql = """
SELECT
    CAST(COUNT(*) AS BIGINT)        AS master_rows_with_decis_match
FROM DTZV01.VENDOR_FEEDBACK_MASTER m
WHERE EXISTS (
    SELECT 1
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TACTIC_ID = m.TREATMENT_ID
      AND t.CLNT_NO   = m.CLNT_NO
)
"""
df_q1b = edw_query(sql, "Q1b")
print(df_q1b.to_string(index=False))

q1b_matched = int(df_q1b.iloc[0]['master_rows_with_decis_match'])
if 'q1a_master_rows' in globals():
    pct = (q1b_matched / q1a_master_rows * 100) if q1a_master_rows else 0
    print(f"\nQ1b proves: {q1b_matched:,} of {q1a_master_rows:,} MASTER rows ({pct:.1f}%) resolve to a decisioning "
          f"record — this is the send-timing coverage ceiling for the whole build.")
    if pct < 95:
        print(f"  WARNING: coverage below 95% — investigate which treatments/periods fail to match before designing on top.")
else:
    print(f"\nQ1b: {q1b_matched:,} MASTER rows resolve to a decisioning record. (Run Q1a cell for the coverage %.)")


# %% [5] Q1c — send volume by month of TREATMT_STRT_DT, last ~24 months (via decisioning)
# Joined-row counts can inherit multi-wave duplicates; distinct_clients is fan-out-safe.

sql = f"""
SELECT
    EXTRACT(YEAR FROM t.TREATMT_STRT_DT) * 100
      + EXTRACT(MONTH FROM t.TREATMT_STRT_DT) AS send_month_yyyymm,
    CAST(COUNT(*) AS BIGINT)        AS joined_rows,
    COUNT(DISTINCT m.CLNT_NO)       AS distinct_clients
FROM DTZV01.VENDOR_FEEDBACK_MASTER m
INNER JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
    ON  t.TACTIC_ID = m.TREATMENT_ID
    AND t.CLNT_NO   = m.CLNT_NO
WHERE t.TREATMT_STRT_DT >= DATE '{TREND_START}'
GROUP BY 1
ORDER BY 1
"""
df_q1c = edw_query(sql, "Q1c")
print(df_q1c.to_string(index=False))

print(f"\nQ1c proves: monthly send volume (decisioning wave date) since {TREND_START} ({len(df_q1c)} months returned).")
if len(df_q1c) > 0:
    lo, hi = df_q1c['joined_rows'].min(), df_q1c['joined_rows'].max()
    print(f"  Row range across months: {lo:,} to {hi:,} — a sharp drop mid-window flags a retention cutoff, not a real volume dip.")


# %% [5] Q2a — EVENT disposition_cd distribution, 2024+

sql = f"""
SELECT
    disposition_cd,
    CAST(COUNT(*) AS BIGINT)        AS event_rows
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_dt_tm >= DATE '{HIST_START}'
GROUP BY disposition_cd
ORDER BY event_rows DESC
"""
df_q2a = edw_query(sql, "Q2a")
print(df_q2a.to_string(index=False))

known_codes = {1, 2, 3, 4, 5, 6}
codes_numeric = pd.to_numeric(df_q2a['disposition_cd'], errors='coerce')
if codes_numeric.isna().any():
    print(f"  WARNING: non-numeric disposition_cd values: {df_q2a.loc[codes_numeric.isna(), 'disposition_cd'].tolist()}")
seen_codes = set(codes_numeric.dropna().astype(int).tolist())
unknown_codes = seen_codes - known_codes
total_events = df_q2a['event_rows'].sum()
print(f"\nQ2a proves: disposition_cd distribution since {HIST_START} ({total_events:,} total rows).")
if unknown_codes:
    print(f"  WARNING: unrecognized disposition_cd values present: {unknown_codes} — not in confirmed set {known_codes}, chase down.")
else:
    print(f"  All disposition_cd values fall within confirmed set {known_codes} (1=sent 2=opened 3=clicked 4=unsub 5=hardbounce 6=complaint).")


# %% [6] Q2b — EVENT disposition_cd by year, 2024+

sql = f"""
SELECT
    EXTRACT(YEAR FROM disposition_dt_tm)   AS disposition_year,
    disposition_cd,
    CAST(COUNT(*) AS BIGINT)               AS event_rows
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_dt_tm >= DATE '{HIST_START}'
GROUP BY 1, 2
ORDER BY 1, 2
"""
df_q2b = edw_query(sql, "Q2b")
print(df_q2b.to_string(index=False))

print(f"\nQ2b proves: disposition_cd mix by year since {HIST_START} "
      f"({df_q2b['disposition_year'].min()} to {df_q2b['disposition_year'].max()}).")


# %% [7] Q3a — EVENT total rows in window (join-reconciliation denominator)

sql = f"""
SELECT
    COUNT(*)                        AS event_rows_window
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_dt_tm >= DATE '{WIN_START}'
  AND disposition_dt_tm <  DATE '{WIN_END}'
"""
df_q3a = edw_query(sql, "Q3a")
print(df_q3a.to_string(index=False))

q3a_rows = int(df_q3a.iloc[0]['event_rows_window'])
print(f"\nQ3a proves: {q3a_rows:,} EVENT rows in window [{WIN_START}, {WIN_END}) — denominator for Q3b/Q3c match rates.")


# %% [9] Q3c — EVENT rows matched to MASTER via consumer_id_hashed + TREATMENT_ID
# (Former Q3b FEEDBACK_ID join removed — column does not exist; this is the only join path.)

sql = f"""
SELECT
    COUNT(*)                        AS event_rows_matched_consumer_treatment
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  e.consumer_id_hashed = m.consumer_id_hashed
    AND e.TREATMENT_ID       = m.TREATMENT_ID
WHERE e.disposition_dt_tm >= DATE '{WIN_START}'
  AND e.disposition_dt_tm <  DATE '{WIN_END}'
"""
df_q3c = edw_query(sql, "Q3c")
print(df_q3c.to_string(index=False))

q3c_rows = int(df_q3c.iloc[0]['event_rows_matched_consumer_treatment'])
if 'q3a_rows' in globals():
    pct_c = (q3c_rows / q3a_rows * 100) if q3a_rows else 0
    print(f"\nQ3c proves: consumer_id_hashed+TREATMENT_ID join matched {q3c_rows:,} of {q3a_rows:,} window rows ({pct_c:.1f}%).")
    if q3c_rows > q3a_rows:
        print(f"  WARNING: matched count ABOVE Q3a denominator ({q3c_rows:,} > {q3a_rows:,}) — join fan-out via "
              f"consumer_id_hashed+TREATMENT_ID.")
else:
    print(f"\nQ3c: consumer_id_hashed+TREATMENT_ID join matched {q3c_rows:,} window rows. (Run Q3a cell for the denominator comparison.)")


# %% [10] Q4a — Unsub (disposition_cd=4) key coverage, EVENT alone (no join, fan-out-safe)

sql = f"""
SELECT
    COUNT(*)                        AS unsub_rows_total,
    COUNT(TREATMENT_ID)             AS unsub_rows_with_treatment_id,
    COUNT(consumer_id_hashed)       AS unsub_rows_with_consumer_id
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_cd = 4
  AND disposition_dt_tm >= DATE '{WIN_START}'
  AND disposition_dt_tm <  DATE '{WIN_END}'
"""
df_q4a = edw_query(sql, "Q4a")
print(df_q4a.to_string(index=False))

r = df_q4a.iloc[0]
q4a_total = int(r['unsub_rows_total'])
print(f"\nQ4a proves: {q4a_total:,} unsub events (disposition_cd=4) in window, no join — true code-4 row count, fan-out-safe.")
if q4a_total:
    print(f"  With TREATMENT_ID: {r['unsub_rows_with_treatment_id']:,} ({r['unsub_rows_with_treatment_id']/q4a_total*100:.1f}%) | "
          f"With consumer_id_hashed: {r['unsub_rows_with_consumer_id']:,} ({r['unsub_rows_with_consumer_id']/q4a_total*100:.1f}%)")


# %% [11] Q4b — Unsub attribution resolved to MASTER / CLNT_NO (consumer+treatment path)

sql = f"""
SELECT
    COUNT(*)                        AS unsub_rows_joined,
    COUNT(DISTINCT m.CLNT_NO)       AS distinct_clients_via_master
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  e.consumer_id_hashed = m.consumer_id_hashed
    AND e.TREATMENT_ID       = m.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '{WIN_START}'
  AND e.disposition_dt_tm <  DATE '{WIN_END}'
"""
df_q4b = edw_query(sql, "Q4b")
print(df_q4b.to_string(index=False))

r = df_q4b.iloc[0]
q4b_joined = int(r['unsub_rows_joined'])
print(f"\nQ4b proves: {q4b_joined:,} unsub events resolve to MASTER, covering {r['distinct_clients_via_master']:,} distinct clients.")
if 'q4a_total' in globals():
    print(f"  Q4a (no-join) total = {q4a_total:,} vs Q4b (joined) = {q4b_joined:,}.")
    if q4b_joined > q4a_total:
        print(f"  WARNING: joined count ABOVE Q4a's no-join total ({q4b_joined:,} > {q4a_total:,}) — MASTER-side fan-out "
              f"(duplicate consumer_id_hashed+TREATMENT_ID keys) is inflating unsub attribution.")
    else:
        print(f"  Joined count at or below Q4a total — no evidence of fan-out on this path.")
else:
    print(f"  (Run Q4a cell for the no-join fan-out comparison.)")


# %% [12] Q5 — Unsubs by campaign MNE, trailing 12 months

sql = f"""
SELECT
    EXTRACT(YEAR FROM e.disposition_dt_tm) * 100
      + EXTRACT(MONTH FROM e.disposition_dt_tm) AS unsub_month_yyyymm,
    SUBSTR(m.TREATMENT_ID, 8, 3)    AS mne,
    COUNT(*)                        AS unsub_rows,
    COUNT(DISTINCT m.CLNT_NO)       AS distinct_clients
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  e.consumer_id_hashed = m.consumer_id_hashed
    AND e.TREATMENT_ID       = m.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '{TRAILING_START}'
  AND e.disposition_dt_tm <  DATE '{TRAILING_END}'
GROUP BY 1, 2
ORDER BY 1, unsub_rows DESC
"""
df_q5 = edw_query(sql, "Q5")
print(df_q5.to_string(index=False))

print(f"\nQ5 proves: unsub volume by campaign MNE, trailing 12 months [{TRAILING_START}, {TRAILING_END}) "
      f"— {len(df_q5)} month x MNE rows.")
if len(df_q5) > 0:
    top_mne = df_q5.groupby('mne')['unsub_rows'].sum().sort_values(ascending=False)
    print(f"  Top MNE by total unsub_rows: {top_mne.index[0]} ({top_mne.iloc[0]:,}). Row counts inherit any Q4b "
          f"fan-out — distinct_clients is the fan-out-safe figure.")
