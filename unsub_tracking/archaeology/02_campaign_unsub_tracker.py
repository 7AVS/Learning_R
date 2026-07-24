# %% [0] Setup — Campaign Unsub Tracker (MNE x month x disposition, 2024+)
# EVENT alone — no join, no fan-out; MNE = SUBSTR(TREATMENT_ID, 8, 3), consumer identity on EVENT.
# disposition_cd: 1=sent 2=opened 3=clicked 4=unsubscribed 5=hardbounce 6=complaint.
# Engine: Teradata-direct via pre-initialized EDW connector.
# Schema canon: schemas/vendor_feedback_tables_schema.md

import pandas as pd
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

HIST_START = '2024-01-01'   # history floor


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


print(f"Setup complete. History floor: {HIST_START}")


# %% [1] T1 — the tracker: MNE x month x disposition_cd (long format, counts only)

sql = f"""
SELECT
    SUBSTR(TREATMENT_ID, 8, 3)      AS mne,
    EXTRACT(YEAR FROM disposition_dt_tm) * 100
      + EXTRACT(MONTH FROM disposition_dt_tm) AS event_month_yyyymm,
    disposition_cd,
    CAST(COUNT(*) AS BIGINT)        AS event_rows,
    COUNT(DISTINCT consumer_id_hashed) AS distinct_consumers
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_dt_tm >= DATE '{HIST_START}'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""
df_tracker = edw_query(sql, "T1")

n_mne = df_tracker['mne'].nunique()
months = sorted(df_tracker['event_month_yyyymm'].unique())
print(f"\nT1: {len(df_tracker):,} MNE x month x disposition rows | {n_mne} distinct MNEs | "
      f"months {months[0]} to {months[-1]}")
print(f"MNEs found: {sorted(df_tracker['mne'].unique())}")


# %% [2] View — unsubs (code 4) pivot: month x MNE

unsub = df_tracker[pd.to_numeric(df_tracker['disposition_cd'], errors='coerce') == 4]
pv_unsub = unsub.pivot_table(index='event_month_yyyymm', columns='mne',
                             values='event_rows', aggfunc='sum', fill_value=0)
print("Unsub events (disposition_cd=4), month x MNE:")
print(pv_unsub.to_string())

tot = unsub.groupby('mne')['event_rows'].sum().sort_values(ascending=False)
print(f"\nTotal unsubs by MNE since {HIST_START}:")
print(tot.to_string())


# %% [3] View — full disposition mix per MNE (totals since HIST_START)

pv_mix = df_tracker.pivot_table(index='mne', columns='disposition_cd',
                                values='event_rows', aggfunc='sum', fill_value=0)
print(f"Disposition mix by MNE since {HIST_START} (1=sent 2=open 3=click 4=unsub 5=hardbounce 6=complaint):")
print(pv_mix.to_string())
print("\nDivide col 4 by col 1 per MNE for unsub-per-sent — done here, not in SQL.")


# %% [4] T2 — guard: events with NULL/short TREATMENT_ID (fall out of the MNE cut)

sql = f"""
SELECT
    EXTRACT(YEAR FROM disposition_dt_tm) * 100
      + EXTRACT(MONTH FROM disposition_dt_tm) AS event_month_yyyymm,
    disposition_cd,
    CAST(COUNT(*) AS BIGINT)        AS unattributed_rows
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_dt_tm >= DATE '{HIST_START}'
  AND (TREATMENT_ID IS NULL OR CHARACTER_LENGTH(TRIM(TREATMENT_ID)) < 10)
GROUP BY 1, 2
ORDER BY 1, 2
"""
df_guard = edw_query(sql, "T2")

if len(df_guard) == 0:
    print("T2: zero rows with NULL/short TREATMENT_ID — every event carries a full campaign id. Tracker is complete.")
else:
    print(df_guard.to_string(index=False))
    n_bad = df_guard['unattributed_rows'].sum()
    print(f"\nWARNING: {n_bad:,} event rows since {HIST_START} have no usable TREATMENT_ID — "
          f"the tracker undercounts by this much; check which disposition codes they carry.")
