# %% [1] Configuration
# IMT Pipeline — EDW Only (Teradata via Trino)
# Replicates SAS proc sql pipeline using EDW cursor pattern
# Produces vintage curves in VVD v3 format + email metrics

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import base64
import os
import warnings
warnings.filterwarnings('ignore')

# ── Campaign parameters ──
IMT_MNES = ["IRI", "IPC"]
ACTION_GROUP = "TG4"
CONTROL_GROUP = "TG7"
DATA_END_DATE = "2026-03-01"
TACTIC_DATE_PREFIX = "2022"  # substr(tactic_id,1,7) >= this (year filter)
EXCLUDED_TACTIC = "20221891RI"  # known bad tactic_id

# ── Teradata/Trino table references ──
TACTIC_TABLE = "DT3V01.TACTIC_EVNT_IP_AR_H60M"
# Alternative: DT3V01.TACTIC_EVNT_IP_AR_HIST
SEG_TABLE = "DG6V01.CLNT_DERIV_DTA_HIST"
EVENT_TABLE = "DDNV01.EXT_CDP_CHNL_EVNT"
EMAIL_MASTER_TABLE = "DTZV01.VENDOR_FEEDBACK_MASTER"
EMAIL_EVENT_TABLE = "DTZV01.VENDOR_FEEDBACK_EVENT"

# ── Success event filters ──
ACTVY_TYP_CD_FILTER = "031"          # International Money Transfer
CHNL_TYP_CD_OLB = "034"              # Online Banking
CHNL_TYP_CD_MB = "021"               # Mobile Apps
SRC_DTA_STORE_CD_FILTER = ["139", "140"]
EVENT_START_DATE = "2021-11-10"

# ── Measurement ──
MAX_DAYS = 90
HDFS_OUTPUT = "/user/427966379/eda_output"

print("Configuration loaded.")
print(f"Campaigns: {IMT_MNES}")
print(f"Action: {ACTION_GROUP}, Control: {CONTROL_GROUP}")
print(f"Tables: {TACTIC_TABLE}, {EVENT_TABLE}")

# %% [2] Experiment Population — Tactic + Segmentation from EDW
# Mirrors SAS Part 1: TACTIC table with LEFT JOIN to segmentation

tactic_sql = f"""
SELECT DISTINCT
    A.CLNT_NO,
    A.TACTIC_ID,
    A.RPT_GRP_CD,
    A.TACTIC_CELL_CD,
    A.TST_GRP_CD,
    A.ADDNL_DECSN_DATA1,
    A.TACTIC_DECSN_VRB_INFO,
    A.TREATMT_MN,
    A.TREATMT_STRT_DT,
    A.TREATMT_END_DT,
    SUBSTR(A.TACTIC_ID, 8, 3) AS MNE,

    CASE
        WHEN SEG.CLNT_STRTGY_SEG_CD = 'NI'      THEN 'Newcomer'
        WHEN SEG.CLNT_STRTGY_SEG_CD = 'NGEN_NS'  THEN 'N-Gen Non-Student'
        WHEN SEG.CLNT_STRTGY_SEG_CD = 'YOUTH'    THEN 'Youth'
        WHEN SEG.CLNT_STRTGY_SEG_CD = 'NGEN_ST'  THEN 'Student'
        ELSE 'Mass'
    END AS SEGMENT,

    CASE
        WHEN SEG.NEW_IMGRNT_CD = 'PERM' THEN 'Permanent Resident'
        WHEN SEG.NEW_IMGRNT_CD = 'STDYX' THEN 'Foreign Student'
        WHEN SEG.NEW_IMGRNT_CD = 'TPWK'  THEN 'Temporary Worker'
        WHEN SEG.NEW_IMGRNT_CD = 'OTHER' THEN 'Other'
        ELSE NULL
    END AS NEWCOMER_SEGMENT,

    CASE
        WHEN TRIM(SEG.CLNT_CATG_SEG_CD) = 'NEW' THEN 'NEW'
        WHEN TRIM(SEG.CLNT_CATG_SEG_CD) IN ('NEWISH', 'EXIST', 'RETURN') THEN 'EXISTING'
        ELSE 'OTHER'
    END AS NEW_EXISTING

FROM {TACTIC_TABLE} A
LEFT JOIN {SEG_TABLE} SEG
    ON A.CLNT_NO = SEG.CLNT_NO
    AND SEG.MTH_END_DT = DATE_TRUNC('month', A.TREATMT_STRT_DT) - INTERVAL '1' DAY
WHERE SUBSTR(A.TACTIC_ID, 8, 3) IN ('IRI', 'IPC')
    AND A.TACTIC_ID <> '{EXCLUDED_TACTIC}'
ORDER BY A.CLNT_NO, A.TREATMT_STRT_DT
"""

print("Querying tactic population from EDW...")
print(f"Table: {TACTIC_TABLE} LEFT JOIN {SEG_TABLE}")
cursor = EDW.cursor()
cursor.execute(tactic_sql)
tactic_rows = cursor.fetchall()
tactic_cols = [desc[0] for desc in cursor.description]
cursor.close()

tactic_df = pd.DataFrame(tactic_rows, columns=tactic_cols)

# Normalize CLNT_NO: strip leading zeros
tactic_df['CLNT_NO'] = tactic_df['CLNT_NO'].astype(str).str.lstrip('0')

# Derive cohort (yyyy-MM from treatment start)
tactic_df['TREATMT_STRT_DT'] = pd.to_datetime(tactic_df['TREATMT_STRT_DT'])
tactic_df['TREATMT_END_DT'] = pd.to_datetime(tactic_df['TREATMT_END_DT'])
tactic_df['COHORT'] = tactic_df['TREATMT_STRT_DT'].dt.strftime('%Y-%m')
tactic_df['WINDOW_DAYS'] = (tactic_df['TREATMT_END_DT'] - tactic_df['TREATMT_STRT_DT']).dt.days

# mo_dt: treatment start minus day-of-month (first of month)
tactic_df['MO_DT'] = tactic_df['TREATMT_STRT_DT'].values.astype('datetime64[M]')

# Filter to test/control groups
tactic_df = tactic_df[tactic_df['TST_GRP_CD'].isin([ACTION_GROUP, CONTROL_GROUP])].copy()

# Duplicate check
dup_counts = tactic_df.groupby(['CLNT_NO', 'TACTIC_ID']).size().reset_index(name='DUP')
dup_counts = dup_counts[dup_counts['DUP'] > 1]

print(f"\n{'='*60}")
print(f"Tactic population: {len(tactic_df):,} rows")
print(f"Unique clients:    {tactic_df['CLNT_NO'].nunique():,}")
print(f"Unique tactics:    {tactic_df['TACTIC_ID'].nunique():,}")
print(f"Duplicate keys:    {len(dup_counts):,}")
print(f"Date range:        {tactic_df['TREATMT_STRT_DT'].min()} to {tactic_df['TREATMT_STRT_DT'].max()}")
print(f"\nBy MNE:")
print(tactic_df.groupby('MNE').agg(
    clients=('CLNT_NO', 'nunique'),
    rows=('CLNT_NO', 'count')
).to_string())
print(f"\nBy TST_GRP_CD:")
print(tactic_df.groupby('TST_GRP_CD').agg(
    clients=('CLNT_NO', 'nunique'),
    rows=('CLNT_NO', 'count')
).to_string())
print(f"\nBy SEGMENT:")
print(tactic_df['SEGMENT'].value_counts().to_string())
print(f"\nBy COHORT (top 10):")
print(tactic_df['COHORT'].value_counts().head(10).to_string())

# %% [3] Success Events — IMT metrics at 30/60/90 day windows
# Mirrors SAS Part 2: JOIN tactic to events, compute windowed metrics
# Does the heavy join in Trino SQL, pulls aggregated result (1 row per client×tactic)

success_sql = f"""
SELECT
    a.CLNT_NO,
    a.TACTIC_ID,

    /* ── Period anchoring (SAS-equivalent) ── */
    FLOOR(DATE_DIFF('day', DATE '2021-11-01', MIN(b.CAPTR_DT)) / 30) + 1 AS PERIOD_ANCHORED_SUCCESS,

    /* ── Overall IMT counts ── */
    COUNT(DISTINCT b.EVNT_ID) AS IMT,
    COUNT(DISTINCT CASE WHEN DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 30 THEN b.EVNT_ID END) AS IMT_30,
    COUNT(DISTINCT CASE WHEN DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 60 THEN b.EVNT_ID END) AS IMT_60,
    COUNT(DISTINCT CASE WHEN DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 90 THEN b.EVNT_ID END) AS IMT_90,

    /* ── Dollar amounts ── */
    SUM(b.EVNT_AMT_CAD) AS AMT_CAD,
    SUM(CASE WHEN DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 30 THEN b.EVNT_AMT_CAD ELSE 0 END) AS AMT_30,
    SUM(CASE WHEN DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 60 THEN b.EVNT_AMT_CAD ELSE 0 END) AS AMT_60,
    SUM(CASE WHEN DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 90 THEN b.EVNT_AMT_CAD ELSE 0 END) AS AMT_90,

    /* ── OLB (Online Banking, CHNL_TYP_CD = '034') ── */
    COUNT(DISTINCT CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_OLB}' THEN b.EVNT_ID END) AS IMT_OL,
    COUNT(DISTINCT CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_OLB}' AND DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 30 THEN b.EVNT_ID END) AS IMT_OL_30,
    COUNT(DISTINCT CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_OLB}' AND DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 60 THEN b.EVNT_ID END) AS IMT_OL_60,
    COUNT(DISTINCT CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_OLB}' AND DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 90 THEN b.EVNT_ID END) AS IMT_OL_90,
    SUM(CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_OLB}' THEN b.EVNT_AMT_CAD ELSE 0 END) AS AMT_OL,
    SUM(CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_OLB}' AND DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 30 THEN b.EVNT_AMT_CAD ELSE 0 END) AS AMT_OL_30,
    SUM(CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_OLB}' AND DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 60 THEN b.EVNT_AMT_CAD ELSE 0 END) AS AMT_OL_60,
    SUM(CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_OLB}' AND DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 90 THEN b.EVNT_AMT_CAD ELSE 0 END) AS AMT_OL_90,

    /* ── MB (Mobile, CHNL_TYP_CD = '021') ── */
    COUNT(DISTINCT CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_MB}' THEN b.EVNT_ID END) AS IMT_MB,
    COUNT(DISTINCT CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_MB}' AND DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 30 THEN b.EVNT_ID END) AS IMT_MB_30,
    COUNT(DISTINCT CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_MB}' AND DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 60 THEN b.EVNT_ID END) AS IMT_MB_60,
    COUNT(DISTINCT CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_MB}' AND DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 90 THEN b.EVNT_ID END) AS IMT_MB_90,
    SUM(CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_MB}' THEN b.EVNT_AMT_CAD ELSE 0 END) AS AMT_MB,
    SUM(CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_MB}' AND DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 30 THEN b.EVNT_AMT_CAD ELSE 0 END) AS AMT_MB_30,
    SUM(CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_MB}' AND DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 60 THEN b.EVNT_AMT_CAD ELSE 0 END) AS AMT_MB_60,
    SUM(CASE WHEN b.CHNL_TYP_CD = '{CHNL_TYP_CD_MB}' AND DATE_DIFF('day', a.TREATMT_STRT_DT, b.CAPTR_DT) <= 90 THEN b.EVNT_AMT_CAD ELSE 0 END) AS AMT_MB_90,

    /* ── Per-client aggregates ── */
    COUNT(DISTINCT b.EVNT_ID) AS EVNT_ID_PER_CLNT,
    COUNT(DISTINCT b.AR_ID) AS ACCT_PER_CLNT,

    /* ── Vintage curve inputs ── */
    MIN(b.CAPTR_DT) AS FIRST_SUCCESS_DATE

FROM {TACTIC_TABLE} a
LEFT JOIN {EVENT_TABLE} b
    ON a.CLNT_NO = b.CLNT_NO
    AND b.CAPTR_DT BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    AND b.ACTVY_TYP_CD = '{ACTVY_TYP_CD_FILTER}'
    AND b.CHNL_TYP_CD IN ('{CHNL_TYP_CD_OLB}', '{CHNL_TYP_CD_MB}')
    AND b.SRC_DTA_STORE_CD IN ('139', '140')
    AND b.CAPTR_DT >= DATE '{EVENT_START_DATE}'
WHERE SUBSTR(a.TACTIC_ID, 8, 3) IN ('IRI', 'IPC')
    AND a.TACTIC_ID <> '{EXCLUDED_TACTIC}'
    AND a.TST_GRP_CD IN ('{ACTION_GROUP}', '{CONTROL_GROUP}')
GROUP BY a.CLNT_NO, a.TACTIC_ID
"""

print("Querying success events from EDW (this may take several minutes)...")
print(f"Join: {TACTIC_TABLE} × {EVENT_TABLE}")
print(f"Filters: ACTVY_TYP_CD={ACTVY_TYP_CD_FILTER}, CHNL_TYP_CD IN ({CHNL_TYP_CD_OLB},{CHNL_TYP_CD_MB})")

cursor = EDW.cursor()
cursor.execute(success_sql)
success_rows = cursor.fetchall()
success_cols = [desc[0] for desc in cursor.description]
cursor.close()

success_df = pd.DataFrame(success_rows, columns=success_cols)
success_df['CLNT_NO'] = success_df['CLNT_NO'].astype(str).str.lstrip('0')

print(f"\n{'='*60}")
print(f"Success result: {len(success_df):,} rows")
print(f"Clients with IMT > 0: {(success_df['IMT'] > 0).sum():,}")
print(f"Clients with no IMT:  {(success_df['IMT'] == 0).sum():,} (or NULL)")
print(f"\nIMT counts summary:")
for col in ['IMT', 'IMT_30', 'IMT_60', 'IMT_90', 'IMT_OL', 'IMT_MB']:
    if col in success_df.columns:
        vals = pd.to_numeric(success_df[col], errors='coerce').fillna(0)
        print(f"  {col:12s}: mean={vals.mean():.3f}, max={vals.max():.0f}, >0={( vals > 0).sum():,}")
print(f"\nAmount summary:")
for col in ['AMT_CAD', 'AMT_30', 'AMT_60', 'AMT_90']:
    if col in success_df.columns:
        vals = pd.to_numeric(success_df[col], errors='coerce').fillna(0)
        print(f"  {col:12s}: mean=${vals.mean():,.2f}, total=${vals.sum():,.2f}")

# %% [4] Merge Tactic Population with Success Metrics

# Merge on CLNT_NO + TACTIC_ID
result_df = tactic_df.merge(success_df, on=['CLNT_NO', 'TACTIC_ID'], how='left')

# Fill NULLs for clients with no success events
imt_cols = [c for c in result_df.columns if c.startswith(('IMT', 'AMT_', 'EVNT_ID_PER', 'ACCT_PER'))]
for col in imt_cols:
    result_df[col] = pd.to_numeric(result_df[col], errors='coerce').fillna(0)

# Derive success flag and days-to-success
result_df['FIRST_SUCCESS_DATE'] = pd.to_datetime(result_df['FIRST_SUCCESS_DATE'], errors='coerce')
result_df['DAYS_TO_SUCCESS'] = (result_df['FIRST_SUCCESS_DATE'] - result_df['TREATMT_STRT_DT']).dt.days
result_df['SUCCESS_FLAG'] = (result_df['IMT'] > 0).astype(int)

# Period anchoring (SAS: floor((CAPTR_DT - 01NOV2021) / 30) + 1)
result_df['PERIOD_ANCHORED_TACTIC'] = (
    (result_df['TREATMT_STRT_DT'] - pd.Timestamp('2021-11-01')).dt.days // 30 + 1
)

print(f"{'='*60}")
print(f"Merged result: {len(result_df):,} rows")
print(f"  Success (IMT > 0):   {result_df['SUCCESS_FLAG'].sum():,} ({result_df['SUCCESS_FLAG'].mean()*100:.2f}%)")
print(f"  No success:          {(result_df['SUCCESS_FLAG'] == 0).sum():,}")
print(f"\nBy MNE × TST_GRP_CD:")
pivot = result_df.groupby(['MNE', 'TST_GRP_CD']).agg(
    clients=('CLNT_NO', 'nunique'),
    success=('SUCCESS_FLAG', 'sum'),
    rate=('SUCCESS_FLAG', 'mean'),
    avg_imt=('IMT', 'mean'),
    avg_amt=('AMT_CAD', 'mean')
).round(4)
print(pivot.to_string())
print(f"\nDays-to-success distribution (successful clients):")
succ = result_df[result_df['SUCCESS_FLAG'] == 1]['DAYS_TO_SUCCESS']
if len(succ) > 0:
    print(f"  Min: {succ.min():.0f}, Median: {succ.median():.0f}, Mean: {succ.mean():.1f}, Max: {succ.max():.0f}")

# %% [5] Email Metrics — from Vendor Feedback tables
# Same logic as imt_pipeline.py Cell 4b
# Query ALL tactic IDs — join key is TREATMENT_ID = TACTIC_ID, no channel pre-filter needed

# Query ALL tactic IDs — join key is TREATMENT_ID = TACTIC_ID, no channel pre-filter needed
# (TACTIC_CELL_CD may be empty for IMT campaigns; ADDNL_DECSN_DATA1 has channel info
#  but the vendor feedback tables already filter to email-only dispositions)
email_tactic_ids = result_df['TACTIC_ID'].unique().tolist()

print(f"Querying email metrics for {len(email_tactic_ids)} unique tactic IDs...")

if len(email_tactic_ids) > 0:
    # Query in batches of 50
    BATCH_SIZE = 50
    email_results = []

    for i in range(0, len(email_tactic_ids), BATCH_SIZE):
        batch = email_tactic_ids[i:i+BATCH_SIZE]
        tactic_list = ",".join([f"'{t}'" for t in batch])

        email_sql = f"""
        SELECT
            FM.CLNT_NO,
            FM.TREATMENT_ID,
            MAX(CASE WHEN FE.DISPOSITION_CD = 1 THEN 1 ELSE 0 END) AS EMAIL_SENT,
            MAX(CASE WHEN FE.DISPOSITION_CD = 2 THEN 1 ELSE 0 END) AS EMAIL_OPENED,
            MAX(CASE WHEN FE.DISPOSITION_CD = 3 THEN 1 ELSE 0 END) AS EMAIL_CLICKED,
            MAX(CASE WHEN FE.DISPOSITION_CD = 4 THEN 1 ELSE 0 END) AS EMAIL_UNSUBSCRIBED,
            MAX(CASE WHEN FE.DISPOSITION_CD = 1 THEN CAST(FE.DISPOSITION_DT_TM AS DATE) END) AS EMAIL_SENT_DT,
            MAX(CASE WHEN FE.DISPOSITION_CD = 2 THEN CAST(FE.DISPOSITION_DT_TM AS DATE) END) AS EMAIL_OPENED_DT,
            MAX(CASE WHEN FE.DISPOSITION_CD = 3 THEN CAST(FE.DISPOSITION_DT_TM AS DATE) END) AS EMAIL_CLICKED_DT,
            MAX(CASE WHEN FE.DISPOSITION_CD = 4 THEN CAST(FE.DISPOSITION_DT_TM AS DATE) END) AS EMAIL_UNSUBSCRIBED_DT
        FROM {EMAIL_MASTER_TABLE} FM
        INNER JOIN {EMAIL_EVENT_TABLE} FE
            ON FM.CONSUMER_ID_HASHED = FE.CONSUMER_ID_HASHED
            AND FM.TREATMENT_ID = FE.TREATMENT_ID
        WHERE FM.TREATMENT_ID IN ({tactic_list})
        GROUP BY FM.CLNT_NO, FM.TREATMENT_ID
        """

        cursor = EDW.cursor()
        cursor.execute(email_sql)
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        cursor.close()

        if rows:
            batch_df = pd.DataFrame(rows, columns=cols)
            email_results.append(batch_df)
        print(f"  Batch {i//BATCH_SIZE + 1}/{(len(email_tactic_ids)-1)//BATCH_SIZE + 1}: {len(rows)} rows")

    if email_results:
        email_df = pd.concat(email_results, ignore_index=True)
        email_df['CLNT_NO'] = email_df['CLNT_NO'].astype(str).str.lstrip('0')

        # Merge to result
        result_df = result_df.merge(
            email_df.rename(columns={'TREATMENT_ID': 'TACTIC_ID'}),
            on=['CLNT_NO', 'TACTIC_ID'],
            how='left'
        )
        for ec in ['EMAIL_SENT', 'EMAIL_OPENED', 'EMAIL_CLICKED', 'EMAIL_UNSUBSCRIBED']:
            result_df[ec] = result_df[ec].fillna(0).astype(int)

        print(f"\nEmail metrics merged: {len(email_df):,} client-tactic pairs")
        print(f"  Sent: {result_df['EMAIL_SENT'].sum():,}")
        print(f"  Opened: {result_df['EMAIL_OPENED'].sum():,}")
        print(f"  Clicked: {result_df['EMAIL_CLICKED'].sum():,}")
        print(f"  Unsubscribed: {result_df['EMAIL_UNSUBSCRIBED'].sum():,}")
    else:
        print("No email engagement data found.")
        for ec in ['EMAIL_SENT', 'EMAIL_OPENED', 'EMAIL_CLICKED', 'EMAIL_UNSUBSCRIBED']:
            result_df[ec] = 0
        for ec in ['EMAIL_SENT_DT', 'EMAIL_OPENED_DT', 'EMAIL_CLICKED_DT', 'EMAIL_UNSUBSCRIBED_DT']:
            result_df[ec] = pd.NaT
else:
    print("No tactic IDs found — skipping email metrics.")
    for ec in ['EMAIL_SENT', 'EMAIL_OPENED', 'EMAIL_CLICKED', 'EMAIL_UNSUBSCRIBED']:
        result_df[ec] = 0
    for ec in ['EMAIL_SENT_DT', 'EMAIL_OPENED_DT', 'EMAIL_CLICKED_DT', 'EMAIL_UNSUBSCRIBED_DT']:
        result_df[ec] = pd.NaT

# %% [6] Population Summary & Sample Ratio Mismatch (SRM) Check

print("="*60)
print("POPULATION SUMMARY")
print("="*60)

for mne in IMT_MNES:
    mne_data = result_df[result_df['MNE'] == mne]
    print(f"\n{'─'*40}")
    print(f"MNE: {mne} — {len(mne_data):,} rows, {mne_data['CLNT_NO'].nunique():,} unique clients")

    for grp in [ACTION_GROUP, CONTROL_GROUP]:
        g = mne_data[mne_data['TST_GRP_CD'] == grp]
        print(f"\n  {grp}: {len(g):,} clients")
        print(f"    Success rate:  {g['SUCCESS_FLAG'].mean()*100:.2f}%")
        print(f"    Avg IMT count: {g['IMT'].mean():.3f}")
        print(f"    Avg AMT_CAD:   ${g['AMT_CAD'].mean():,.2f}")
        if 'EMAIL_SENT' in g.columns:
            print(f"    Email sent:    {g['EMAIL_SENT'].sum():,} ({g['EMAIL_SENT'].mean()*100:.1f}%)")
            print(f"    Email opened:  {g['EMAIL_OPENED'].sum():,} ({g['EMAIL_OPENED'].mean()*100:.1f}%)")

    # SRM check (chi-square)
    from scipy import stats
    action_n = len(mne_data[mne_data['TST_GRP_CD'] == ACTION_GROUP])
    control_n = len(mne_data[mne_data['TST_GRP_CD'] == CONTROL_GROUP])
    total = action_n + control_n
    if total > 0:
        expected_ratio = action_n / total
        chi2, p_val = stats.chisquare([action_n, control_n])
        print(f"\n  SRM: Action={action_n:,}, Control={control_n:,}, Ratio={expected_ratio:.4f}")
        print(f"  Chi-sq={chi2:.2f}, p-value={p_val:.4f}", end="")
        print("  ⚠ POSSIBLE SRM" if p_val < 0.01 else "  ✓ OK")

print(f"\n{'='*60}")
print("SEGMENT DISTRIBUTION")
for col in ['SEGMENT', 'NEWCOMER_SEGMENT', 'NEW_EXISTING']:
    if col in result_df.columns:
        print(f"\n{col}:")
        print(result_df.groupby(['MNE', col]).size().unstack(fill_value=0).to_string())

# %% [7] Vintage Curves — VVD v3 format
# MNE | COHORT | TST_GRP_CD | RPT_GRP_CD | METRIC | DAY | WINDOW_DAYS | CLIENT_CNT | SUCCESS_CNT | RATE

vintage_rows = []

# Group keys
group_cols = ['MNE', 'COHORT', 'TST_GRP_CD', 'RPT_GRP_CD']

for keys, grp in result_df.groupby(group_cols):
    mne, cohort, tst, rpt = keys
    client_cnt = len(grp)
    window_days = int(grp['WINDOW_DAYS'].median())

    # ── IMT Success vintage curve ──
    successful = grp[grp['SUCCESS_FLAG'] == 1]['DAYS_TO_SUCCESS'].dropna()
    for day in range(MAX_DAYS + 1):
        success_cnt = int((successful <= day).sum())
        rate = round(success_cnt / client_cnt * 100, 2) if client_cnt > 0 else 0.0
        vintage_rows.append({
            'MNE': mne, 'COHORT': cohort, 'TST_GRP_CD': tst, 'RPT_GRP_CD': rpt,
            'METRIC': 'imt_success', 'DAY': day, 'WINDOW_DAYS': window_days,
            'CLIENT_CNT': client_cnt, 'SUCCESS_CNT': success_cnt, 'RATE': rate
        })

    # ── Email vintage curves (if email data exists) ──
    email_metrics = {
        'email_sent': ('EMAIL_SENT', 'EMAIL_SENT_DT'),
        'email_open': ('EMAIL_OPENED', 'EMAIL_OPENED_DT'),
        'email_click': ('EMAIL_CLICKED', 'EMAIL_CLICKED_DT'),
        'email_unsub': ('EMAIL_UNSUBSCRIBED', 'EMAIL_UNSUBSCRIBED_DT'),
    }
    for metric_name, (flag_col, date_col) in email_metrics.items():
        if flag_col in grp.columns and date_col in grp.columns:
            em_grp = grp[grp[flag_col] == 1].copy()
            if len(em_grp) > 0:
                em_grp[date_col] = pd.to_datetime(em_grp[date_col], errors='coerce')
                em_grp['EM_DAYS'] = (em_grp[date_col] - em_grp['TREATMT_STRT_DT']).dt.days
                em_days = em_grp['EM_DAYS'].dropna()
                em_days = em_days[(em_days >= 0) & (em_days <= MAX_DAYS)]
            else:
                em_days = pd.Series(dtype=float)

            for day in range(MAX_DAYS + 1):
                success_cnt = int((em_days <= day).sum())
                rate = round(success_cnt / client_cnt * 100, 2) if client_cnt > 0 else 0.0
                vintage_rows.append({
                    'MNE': mne, 'COHORT': cohort, 'TST_GRP_CD': tst, 'RPT_GRP_CD': rpt,
                    'METRIC': metric_name, 'DAY': day, 'WINDOW_DAYS': window_days,
                    'CLIENT_CNT': client_cnt, 'SUCCESS_CNT': success_cnt, 'RATE': rate
                })

vintage_df = pd.DataFrame(vintage_rows)

print(f"{'='*60}")
print(f"Vintage curves: {len(vintage_df):,} rows")
print(f"  Metrics: {vintage_df['METRIC'].unique().tolist()}")
print(f"  Groups:  {vintage_df.groupby(group_cols).ngroups}")
print(f"\nDay-90 summary by MNE × TST_GRP_CD × METRIC:")
day90 = vintage_df[vintage_df['DAY'] == 90]
print(day90.groupby(['MNE', 'TST_GRP_CD', 'METRIC']).agg(
    client_cnt=('CLIENT_CNT', 'sum'),
    success_cnt=('SUCCESS_CNT', 'sum'),
    avg_rate=('RATE', 'mean')
).round(2).to_string())

# %% [8] Lift & Statistical Significance

from scipy import stats

print("="*60)
print("LIFT ANALYSIS (Action vs Control)")
print("="*60)

lift_rows = []
for mne in IMT_MNES:
    mne_data = result_df[result_df['MNE'] == mne]
    action = mne_data[mne_data['TST_GRP_CD'] == ACTION_GROUP]
    control = mne_data[mne_data['TST_GRP_CD'] == CONTROL_GROUP]

    if len(action) == 0 or len(control) == 0:
        print(f"\n{mne}: Missing action or control group, skipping.")
        continue

    metrics = {
        'SUCCESS_RATE': ('SUCCESS_FLAG', 'mean'),
        'AVG_IMT_COUNT': ('IMT', 'mean'),
        'AVG_AMT_CAD': ('AMT_CAD', 'mean'),
        'IMT_30_RATE': ('IMT_30', lambda x: (x > 0).mean()),
        'IMT_60_RATE': ('IMT_60', lambda x: (x > 0).mean()),
        'IMT_90_RATE': ('IMT_90', lambda x: (x > 0).mean()),
    }
    if 'EMAIL_OPENED' in result_df.columns:
        metrics['EMAIL_OPEN_RATE'] = ('EMAIL_OPENED', 'mean')
        metrics['EMAIL_CLICK_RATE'] = ('EMAIL_CLICKED', 'mean')

    print(f"\n{'─'*40}")
    print(f"MNE: {mne}")
    print(f"  Action (n={len(action):,})  vs  Control (n={len(control):,})")
    print(f"  {'Metric':<20s} {'Action':>10s} {'Control':>10s} {'Lift':>10s} {'p-value':>10s}")

    for metric_name, (col, agg_func) in metrics.items():
        if col not in action.columns:
            continue
        a_val = agg_func(action[col]) if callable(agg_func) else action[col].agg(agg_func)
        c_val = agg_func(control[col]) if callable(agg_func) else control[col].agg(agg_func)
        lift = ((a_val - c_val) / c_val * 100) if c_val != 0 else float('inf')

        # Two-proportion z-test for rates, t-test for means
        if 'RATE' in metric_name:
            # z-test for proportions
            a_n, c_n = len(action), len(control)
            a_s = int(a_val * a_n)
            c_s = int(c_val * c_n)
            p_pool = (a_s + c_s) / (a_n + c_n) if (a_n + c_n) > 0 else 0
            se = np.sqrt(p_pool * (1 - p_pool) * (1/a_n + 1/c_n)) if p_pool > 0 and p_pool < 1 else 1
            z = (a_val - c_val) / se if se > 0 else 0
            p_val = 2 * (1 - stats.norm.cdf(abs(z)))
        else:
            t_stat, p_val = stats.ttest_ind(
                pd.to_numeric(action[col], errors='coerce').fillna(0),
                pd.to_numeric(control[col], errors='coerce').fillna(0),
                equal_var=False
            )

        sig = "***" if p_val < 0.001 else "**" if p_val < 0.01 else "*" if p_val < 0.05 else ""
        print(f"  {metric_name:<20s} {a_val:>10.4f} {c_val:>10.4f} {lift:>+9.1f}% {p_val:>9.4f} {sig}")

        lift_rows.append({
            'MNE': mne, 'METRIC': metric_name,
            'ACTION_VAL': a_val, 'CONTROL_VAL': c_val,
            'LIFT_PCT': lift, 'P_VALUE': p_val
        })

lift_df = pd.DataFrame(lift_rows)

# %% [9] Export — CSV Download Link + HDFS Backup

from IPython.display import display, HTML

# ── Vintage curves CSV ──
csv_data = vintage_df.to_csv(index=False)
size_mb = len(csv_data.encode('utf-8')) / (1024 * 1024)
print(f"Vintage curves CSV: {size_mb:.2f} MB, {len(vintage_df):,} rows")

if size_mb <= 50:
    b64 = base64.b64encode(csv_data.encode()).decode()
    filename = "imt_edw_vintage_curves.csv"
    link = f'<a download="{filename}" href="data:text/csv;base64,{b64}" target="_blank" style="font-size:16px; padding:10px 20px; background:#264f78; color:white; text-decoration:none; border-radius:4px;">Download {filename} ({size_mb:.1f} MB)</a>'
    display(HTML(link))
else:
    print(f"WARNING: CSV too large for browser download ({size_mb:.1f} MB). Use HDFS backup.")

# ── Lift summary CSV ──
if len(lift_df) > 0:
    lift_csv = lift_df.to_csv(index=False)
    lift_b64 = base64.b64encode(lift_csv.encode()).decode()
    lift_link = f'<a download="imt_edw_lift_summary.csv" href="data:text/csv;base64,{lift_b64}" target="_blank" style="font-size:16px; padding:10px 20px; background:#264f78; color:white; text-decoration:none; border-radius:4px;">Download imt_edw_lift_summary.csv</a>'
    display(HTML(lift_link))

# ── Client-level detail CSV ──
detail_cols = ['CLNT_NO', 'TACTIC_ID', 'MNE', 'COHORT', 'TST_GRP_CD', 'RPT_GRP_CD',
               'SEGMENT', 'NEWCOMER_SEGMENT', 'NEW_EXISTING', 'TREATMT_STRT_DT', 'TREATMT_END_DT',
               'WINDOW_DAYS', 'SUCCESS_FLAG', 'DAYS_TO_SUCCESS', 'FIRST_SUCCESS_DATE',
               'IMT', 'IMT_30', 'IMT_60', 'IMT_90',
               'AMT_CAD', 'AMT_30', 'AMT_60', 'AMT_90',
               'IMT_OL', 'IMT_OL_30', 'IMT_OL_60', 'IMT_OL_90',
               'IMT_MB', 'IMT_MB_30', 'IMT_MB_60', 'IMT_MB_90',
               'AMT_OL', 'AMT_OL_30', 'AMT_OL_60', 'AMT_OL_90',
               'AMT_MB', 'AMT_MB_30', 'AMT_MB_60', 'AMT_MB_90',
               'EMAIL_SENT', 'EMAIL_OPENED', 'EMAIL_CLICKED', 'EMAIL_UNSUBSCRIBED']
detail_cols = [c for c in detail_cols if c in result_df.columns]
detail_csv = result_df[detail_cols].to_csv(index=False)
detail_size = len(detail_csv.encode('utf-8')) / (1024 * 1024)
if detail_size <= 50:
    detail_b64 = base64.b64encode(detail_csv.encode()).decode()
    detail_link = f'<a download="imt_edw_client_detail.csv" href="data:text/csv;base64,{detail_b64}" target="_blank" style="font-size:16px; padding:10px 20px; background:#264f78; color:white; text-decoration:none; border-radius:4px;">Download imt_edw_client_detail.csv ({detail_size:.1f} MB)</a>'
    display(HTML(detail_link))

# ── HDFS backup ──
try:
    local_path = "/tmp/imt_edw_vintage_curves.csv"
    vintage_df.to_csv(local_path, index=False)
    os.system(f"hdfs dfs -mkdir -p {HDFS_OUTPUT}")
    os.system(f"hdfs dfs -put -f {local_path} {HDFS_OUTPUT}/imt_edw_vintage_curves.csv")
    print(f"\nHDFS backup: {HDFS_OUTPUT}/imt_edw_vintage_curves.csv")

    local_detail = "/tmp/imt_edw_client_detail.csv"
    result_df[detail_cols].to_csv(local_detail, index=False)
    os.system(f"hdfs dfs -put -f {local_detail} {HDFS_OUTPUT}/imt_edw_client_detail.csv")
    print(f"HDFS backup: {HDFS_OUTPUT}/imt_edw_client_detail.csv")
except Exception as e:
    print(f"HDFS backup failed: {e}")

# %% [10] Mega Summary Output

print("="*70)
print("IMT PIPELINE — EDW ONLY — SUMMARY")
print("="*70)

print(f"\nData source:  EDW (Teradata via Trino)")
print(f"Tables:       {TACTIC_TABLE} × {EVENT_TABLE}")
print(f"Segmentation: {SEG_TABLE}")
print(f"Email:        {EMAIL_MASTER_TABLE} × {EMAIL_EVENT_TABLE}")
print(f"Campaigns:    {IMT_MNES}")
print(f"Groups:       Action={ACTION_GROUP}, Control={CONTROL_GROUP}")
print(f"Window:       0-{MAX_DAYS} days")

print(f"\n{'─'*70}")
print(f"POPULATION")
print(f"  Total rows:     {len(result_df):,}")
print(f"  Unique clients: {result_df['CLNT_NO'].nunique():,}")
print(f"  Date range:     {result_df['TREATMT_STRT_DT'].min().date()} to {result_df['TREATMT_STRT_DT'].max().date()}")
print(f"  Cohorts:        {result_df['COHORT'].nunique()}")

print(f"\n{'─'*70}")
print(f"SUCCESS RATES (0-90 days)")
for mne in IMT_MNES:
    m = result_df[result_df['MNE'] == mne]
    a = m[m['TST_GRP_CD'] == ACTION_GROUP]['SUCCESS_FLAG']
    c = m[m['TST_GRP_CD'] == CONTROL_GROUP]['SUCCESS_FLAG']
    a_rate = a.mean() * 100 if len(a) > 0 else 0
    c_rate = c.mean() * 100 if len(c) > 0 else 0
    lift = ((a_rate - c_rate) / c_rate * 100) if c_rate > 0 else 0
    print(f"  {mne}: Action={a_rate:.2f}% (n={len(a):,}), Control={c_rate:.2f}% (n={len(c):,}), Lift={lift:+.1f}%")

print(f"\n{'─'*70}")
print(f"CHANNEL BREAKDOWN (0-90 days, Action group)")
for mne in IMT_MNES:
    a = result_df[(result_df['MNE'] == mne) & (result_df['TST_GRP_CD'] == ACTION_GROUP)]
    if len(a) > 0:
        ol_rate = (a['IMT_OL_90'] > 0).mean() * 100
        mb_rate = (a['IMT_MB_90'] > 0).mean() * 100
        print(f"  {mne}: OLB={ol_rate:.2f}%, Mobile={mb_rate:.2f}%")

print(f"\n{'─'*70}")
print(f"EMAIL ENGAGEMENT")
if 'EMAIL_SENT' in result_df.columns:
    em = result_df[result_df['EMAIL_SENT'] == 1]
    if len(em) > 0:
        print(f"  Sent:         {len(em):,}")
        print(f"  Open rate:    {em['EMAIL_OPENED'].mean()*100:.1f}%")
        print(f"  Click rate:   {em['EMAIL_CLICKED'].mean()*100:.1f}%")
        print(f"  Unsub rate:   {em['EMAIL_UNSUBSCRIBED'].mean()*100:.1f}%")
    else:
        print("  No email data.")

print(f"\n{'─'*70}")
print(f"VINTAGE CURVES: {len(vintage_df):,} data points")
print(f"OUTPUTS:")
print(f"  1. imt_edw_vintage_curves.csv  (download link above)")
print(f"  2. imt_edw_client_detail.csv   (download link above)")
print(f"  3. imt_edw_lift_summary.csv    (download link above)")
print(f"  4. HDFS: {HDFS_OUTPUT}/imt_edw_*.csv")
print("="*70)
