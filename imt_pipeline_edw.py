# %% [1] Configuration
# IMT Pipeline — EDW Only (Teradata via Trino) — OPTIMIZED
# No cross-table SQL joins. Each table queried independently, joined in pandas.

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import base64
import os
import time
import warnings
warnings.filterwarnings('ignore')

# ── Campaign parameters ──
IMT_MNES = ["IRI", "IPC"]
ACTION_GROUP = "TG4"
CONTROL_GROUP = "TG7"
EXCLUDED_TACTIC = "20221891RI"

# ── Table references (Teradata via Trino) ──
TACTIC_TABLE = "DT3V01.TACTIC_EVNT_IP_AR_H60M"
SEG_TABLE = "DG6V01.CLNT_DERIV_DTA_HIST"
EVENT_TABLE = "DDNV01.EXT_CDP_CHNL_EVNT"
EMAIL_MASTER = "DTZV01.VENDOR_FEEDBACK_MASTER"
EMAIL_EVENT = "DTZV01.VENDOR_FEEDBACK_EVENT"

# ── Event filters ──
ACTVY_FILTER = "031"       # International Money Transfer
CHNL_OLB = "034"           # Online Banking
CHNL_MB = "021"            # Mobile Apps
SRC_FILTERS = ("139", "140")
EVENT_START = "2021-11-10"

# ── Measurement ──
# IRI is trigger-based (30-day window), IPC is 90-day window
MNE_WINDOWS = {"IRI": 30, "IPC": 90}
MAX_DAYS = max(MNE_WINDOWS.values())  # 90, used for data pull range
HDFS_OUTPUT = "/user/427966379/eda_output"

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

print("Configuration loaded.")
print(f"Campaigns: {IMT_MNES} | Action: {ACTION_GROUP}, Control: {CONTROL_GROUP}")


# %% [2] Tactic Population — NO JOINS, simple filter
# Pull raw tactics, filter/transform in pandas

print("=" * 60)
print("PULLING TACTIC POPULATION")

# Push ALL filters to SQL to minimize spool:
# - TST_GRP_CD filter (eliminates most rows)
# - LIKE instead of SUBSTR (better index usage in Teradata)
# - Date filter to reduce scan
tactic_df = edw_query(f"""
    SELECT
        CLNT_NO, TACTIC_ID, RPT_GRP_CD, TACTIC_CELL_CD, TST_GRP_CD,
        ADDNL_DECSN_DATA1, TACTIC_DECSN_VRB_INFO, TREATMT_MN,
        TREATMT_STRT_DT, TREATMT_END_DT
    FROM {TACTIC_TABLE}
    WHERE (TACTIC_ID LIKE '_______IRI%' OR TACTIC_ID LIKE '_______IPC%')
        AND TST_GRP_CD IN ('{ACTION_GROUP}', '{CONTROL_GROUP}')
        AND TACTIC_ID <> '{EXCLUDED_TACTIC}'
        AND TREATMT_STRT_DT >= DATE '2025-01-01'
""", "tactics")

# ── Transform in pandas ──
tactic_df['CLNT_NO'] = tactic_df['CLNT_NO'].astype(str).str.strip().str.lstrip('0')
tactic_df['TREATMT_STRT_DT'] = pd.to_datetime(tactic_df['TREATMT_STRT_DT'])
tactic_df['TREATMT_END_DT'] = pd.to_datetime(tactic_df['TREATMT_END_DT'])
tactic_df['MNE'] = tactic_df['TACTIC_ID'].str[7:10]
tactic_df['COHORT'] = tactic_df['TREATMT_STRT_DT'].dt.strftime('%Y-%m')
tactic_df['WINDOW_DAYS'] = (tactic_df['TREATMT_END_DT'] - tactic_df['TREATMT_STRT_DT']).dt.days
tactic_df['MO_DT'] = tactic_df['TREATMT_STRT_DT'].values.astype('datetime64[M]')

# TST_GRP_CD already filtered in SQL. Dedup only.
tactic_df.drop_duplicates(subset=['CLNT_NO', 'TACTIC_ID'], inplace=True)

print(f"\nTactic population: {len(tactic_df):,} rows")
print(f"  Clients: {tactic_df['CLNT_NO'].nunique():,}")
print(f"  Tactics: {tactic_df['TACTIC_ID'].nunique():,}")
print(f"  Date range: {tactic_df['TREATMT_STRT_DT'].min().date()} to {tactic_df['TREATMT_STRT_DT'].max().date()}")
print(f"\n  By MNE × TST_GRP_CD:")
print(tactic_df.groupby(['MNE', 'TST_GRP_CD']).agg(
    n=('CLNT_NO', 'count'), clients=('CLNT_NO', 'nunique')
).to_string())


# %% [3] Success Events — independent pull, NO SQL JOIN
# Pull IMT events with tight date range from tactic dates, filter to our clients in pandas

print("=" * 60)
print("PULLING IMT SUCCESS EVENTS")

min_dt = tactic_df['TREATMT_STRT_DT'].min()
max_dt = tactic_df['TREATMT_END_DT'].max()
print(f"  Date window: {min_dt.date()} to {max_dt.date()}")

# Batch by QUARTER to avoid spool overflow on the events table
# Each quarter query scans ~3 months of data = manageable spool
client_set = set(tactic_df['CLNT_NO'].unique())
event_chunks = []

# Build quarterly date ranges from min_dt to max_dt
q_starts = pd.date_range(
    start=min_dt.to_period('Q').start_time,
    end=max_dt,
    freq='QS'
)

for qs in q_starts:
    qe = qs + pd.offsets.QuarterEnd(0)
    qs_str = qs.strftime('%Y-%m-%d')
    qe_str = qe.strftime('%Y-%m-%d')

    chunk = edw_query(f"""
        SELECT
            CLNT_NO, CAPTR_DT, EVNT_ID, AR_ID, EVNT_AMT_CAD, CHNL_TYP_CD
        FROM {EVENT_TABLE}
        WHERE ACTVY_TYP_CD = '{ACTVY_FILTER}'
            AND CHNL_TYP_CD IN ('{CHNL_OLB}', '{CHNL_MB}')
            AND SRC_DTA_STORE_CD IN ('{SRC_FILTERS[0]}', '{SRC_FILTERS[1]}')
            AND CAPTR_DT >= DATE '{qs_str}'
            AND CAPTR_DT <= DATE '{qe_str}'
    """, f"events {qs_str[:7]}")

    if len(chunk) > 0:
        chunk['CLNT_NO'] = chunk['CLNT_NO'].astype(str).str.strip().str.lstrip('0')
        chunk = chunk[chunk['CLNT_NO'].isin(client_set)]
        if len(chunk) > 0:
            event_chunks.append(chunk)

if event_chunks:
    events_df = pd.concat(event_chunks, ignore_index=True)
else:
    events_df = pd.DataFrame(columns=['CLNT_NO', 'CAPTR_DT', 'EVNT_ID', 'AR_ID', 'EVNT_AMT_CAD', 'CHNL_TYP_CD'])

events_df['CAPTR_DT'] = pd.to_datetime(events_df['CAPTR_DT'])
events_df['EVNT_AMT_CAD'] = pd.to_numeric(events_df['EVNT_AMT_CAD'], errors='coerce').fillna(0)
print(f"\n  Total matched events: {len(events_df):,} for {events_df['CLNT_NO'].nunique():,} clients")


# %% [4] Join + Compute Metrics (all in pandas)

print("=" * 60)
print("JOINING & COMPUTING METRICS")

# Merge events to tactics on CLNT_NO
merged = events_df.merge(
    tactic_df[['CLNT_NO', 'TACTIC_ID', 'TREATMT_STRT_DT', 'TREATMT_END_DT']],
    on='CLNT_NO'
)
# Filter: event within treatment window
matched = merged[
    (merged['CAPTR_DT'] >= merged['TREATMT_STRT_DT']) &
    (merged['CAPTR_DT'] <= merged['TREATMT_END_DT'])
].copy()
matched['DAYS'] = (matched['CAPTR_DT'] - matched['TREATMT_STRT_DT']).dt.days
matched['MNE'] = matched['TACTIC_ID'].str[7:10]
matched['MAX_WINDOW'] = matched['MNE'].map(MNE_WINDOWS).fillna(MAX_DAYS)
matched = matched[matched['DAYS'] <= matched['MAX_WINDOW']].copy()
print(f"  Matched events: {len(matched):,}")

# Aggregate per client × tactic
def agg_success(g):
    """Compute all success metrics for one client×tactic group."""
    d = {}
    d['IMT'] = g['EVNT_ID'].nunique()
    d['FIRST_SUCCESS_DATE'] = g['CAPTR_DT'].min()
    d['AMT_CAD'] = g['EVNT_AMT_CAD'].sum()

    for window in [30, 60, 90]:
        w = g[g['DAYS'] <= window]
        d[f'IMT_{window}'] = w['EVNT_ID'].nunique()
        d[f'AMT_{window}'] = w['EVNT_AMT_CAD'].sum()

    # OLB (034)
    olb = g[g['CHNL_TYP_CD'] == CHNL_OLB]
    d['IMT_OL'] = olb['EVNT_ID'].nunique()
    d['AMT_OL'] = olb['EVNT_AMT_CAD'].sum()
    for window in [30, 60, 90]:
        w = olb[olb['DAYS'] <= window]
        d[f'IMT_OL_{window}'] = w['EVNT_ID'].nunique()
        d[f'AMT_OL_{window}'] = w['EVNT_AMT_CAD'].sum()

    # MB (021)
    mb = g[g['CHNL_TYP_CD'] == CHNL_MB]
    d['IMT_MB'] = mb['EVNT_ID'].nunique()
    d['AMT_MB'] = mb['EVNT_AMT_CAD'].sum()
    for window in [30, 60, 90]:
        w = mb[mb['DAYS'] <= window]
        d[f'IMT_MB_{window}'] = w['EVNT_ID'].nunique()
        d[f'AMT_MB_{window}'] = w['EVNT_AMT_CAD'].sum()

    d['EVNT_ID_PER_CLNT'] = g['EVNT_ID'].nunique()
    d['ACCT_PER_CLNT'] = g['AR_ID'].nunique()
    return pd.Series(d)

if len(matched) > 0:
    print("  Aggregating per client × tactic (this may take a minute)...")
    success_df = matched.groupby(['CLNT_NO', 'TACTIC_ID']).apply(agg_success).reset_index()
else:
    # Empty success DataFrame with correct columns
    metric_cols = ['IMT', 'FIRST_SUCCESS_DATE', 'AMT_CAD']
    for w in [30, 60, 90]:
        metric_cols += [f'IMT_{w}', f'AMT_{w}']
    for ch in ['OL', 'MB']:
        metric_cols += [f'IMT_{ch}', f'AMT_{ch}']
        for w in [30, 60, 90]:
            metric_cols += [f'IMT_{ch}_{w}', f'AMT_{ch}_{w}']
    metric_cols += ['EVNT_ID_PER_CLNT', 'ACCT_PER_CLNT']
    success_df = pd.DataFrame(columns=['CLNT_NO', 'TACTIC_ID'] + metric_cols)

# Merge back to full tactic population
result_df = tactic_df.merge(success_df, on=['CLNT_NO', 'TACTIC_ID'], how='left')

# Fill NULLs
num_cols = [c for c in result_df.columns if c.startswith(('IMT', 'AMT_', 'EVNT_ID_PER', 'ACCT_PER'))]
for c in num_cols:
    result_df[c] = pd.to_numeric(result_df[c], errors='coerce').fillna(0)

result_df['FIRST_SUCCESS_DATE'] = pd.to_datetime(result_df['FIRST_SUCCESS_DATE'], errors='coerce')
result_df['DAYS_TO_SUCCESS'] = (result_df['FIRST_SUCCESS_DATE'] - result_df['TREATMT_STRT_DT']).dt.days
result_df['SUCCESS_FLAG'] = (result_df['IMT'] > 0).astype(int)

print(f"\nResult: {len(result_df):,} rows")
print(f"  Success: {result_df['SUCCESS_FLAG'].sum():,} ({result_df['SUCCESS_FLAG'].mean()*100:.2f}%)")
print(f"\n  By MNE × TST_GRP_CD:")
print(result_df.groupby(['MNE', 'TST_GRP_CD']).agg(
    n=('CLNT_NO', 'count'),
    success=('SUCCESS_FLAG', 'sum'),
    rate=('SUCCESS_FLAG', 'mean'),
    avg_imt=('IMT', 'mean'),
    avg_amt=('AMT_CAD', 'mean')
).round(4).to_string())


# %% [5] Segmentation Enrichment — separate query, batched by month

print("=" * 60)
print("SEGMENTATION ENRICHMENT")

# Get distinct (CLNT_NO, month_before_treatment) pairs
result_df['SEG_MTH'] = (result_df['TREATMT_STRT_DT'].dt.to_period('M') - 1).dt.to_timestamp() + pd.offsets.MonthEnd(0)
seg_months = sorted(result_df['SEG_MTH'].dropna().unique())
print(f"  Segmentation months to query: {len(seg_months)}")

seg_results = []
for mth in seg_months:
    mth_str = pd.Timestamp(mth).strftime('%Y-%m-%d')
    clients_in_month = result_df[result_df['SEG_MTH'] == mth]['CLNT_NO'].unique()
    if len(clients_in_month) == 0:
        continue

    # Batch clients in chunks of 1000 for the IN clause
    for i in range(0, len(clients_in_month), 1000):
        batch = clients_in_month[i:i+1000]
        clnt_in = ",".join(f"'{c}'" for c in batch)
        seg_sql = f"""
            SELECT CLNT_NO, CLNT_STRTGY_SEG_CD, NEW_IMGRNT_CD, CLNT_CATG_SEG_CD
            FROM {SEG_TABLE}
            WHERE MTH_END_DT = DATE '{mth_str}'
                AND CLNT_NO IN ({clnt_in})
        """
        try:
            batch_df = edw_query(seg_sql, f"seg {mth_str} batch {i//1000+1}")
            batch_df['SEG_MTH'] = pd.Timestamp(mth)
            seg_results.append(batch_df)
        except Exception as e:
            print(f"  WARNING: seg query failed for {mth_str}: {e}")

if seg_results:
    seg_df = pd.concat(seg_results, ignore_index=True)
    seg_df['CLNT_NO'] = seg_df['CLNT_NO'].astype(str).str.strip().str.lstrip('0')

    # Map segment codes to labels
    seg_map = {'NI': 'Newcomer', 'NGEN_NS': 'N-Gen Non-Student',
               'YOUTH': 'Youth', 'NGEN_ST': 'Student'}
    seg_df['SEGMENT'] = seg_df['CLNT_STRTGY_SEG_CD'].map(seg_map).fillna('Mass')

    imm_map = {'PERM': 'Permanent Resident', 'STDYX': 'Foreign Student',
               'TPWK': 'Temporary Worker', 'OTHER': 'Other'}
    seg_df['NEWCOMER_SEGMENT'] = seg_df['NEW_IMGRNT_CD'].map(imm_map)

    new_existing_map = {'NEW': 'NEW', 'NEWISH': 'EXISTING', 'EXIST': 'EXISTING', 'RETURN': 'EXISTING'}
    seg_df['NEW_EXISTING'] = seg_df['CLNT_CATG_SEG_CD'].str.strip().map(new_existing_map).fillna('OTHER')

    # Merge to result
    result_df = result_df.merge(
        seg_df[['CLNT_NO', 'SEG_MTH', 'SEGMENT', 'NEWCOMER_SEGMENT', 'NEW_EXISTING']],
        on=['CLNT_NO', 'SEG_MTH'], how='left'
    )
    result_df['SEGMENT'] = result_df['SEGMENT'].fillna('Unknown')
    print(f"\nSegmentation merged: {seg_df['CLNT_NO'].nunique():,} clients enriched")
    print(result_df['SEGMENT'].value_counts().to_string())
else:
    print("  No segmentation data retrieved. Adding placeholder columns.")
    result_df['SEGMENT'] = 'Unknown'
    result_df['NEWCOMER_SEGMENT'] = None
    result_df['NEW_EXISTING'] = 'Unknown'


# %% [6] Email Metrics — query ALL tactic IDs against vendor feedback

print("=" * 60)
print("EMAIL METRICS")

email_tactic_ids = result_df['TACTIC_ID'].unique().tolist()
print(f"  Querying {len(email_tactic_ids)} tactic IDs against vendor feedback...")
print(f"  Sample tactic IDs: {email_tactic_ids[:5]}")

# Diagnostic: check if vendor feedback has ANY data for our tactics
if len(email_tactic_ids) > 0:
    diag_list = ",".join(f"'{t}'" for t in email_tactic_ids[:10])
    try:
        diag_df = edw_query(f"""
            SELECT COUNT(*) AS cnt, COUNT(DISTINCT TREATMENT_ID) AS tactic_cnt
            FROM {EMAIL_MASTER}
            WHERE TREATMENT_ID IN ({diag_list})
        """, "email diagnostic")
        cnt, tcnt = diag_df.iloc[0]['cnt'], diag_df.iloc[0]['tactic_cnt']
        print(f"  DIAGNOSTIC: vendor_feedback has {cnt} rows for first 10 tactics ({tcnt} matched)")
        if cnt == 0:
            print("  WARNING: No vendor feedback data found. TREATMENT_ID may not match TACTIC_ID.")
            print(f"  Try: SELECT DISTINCT TREATMENT_ID FROM {EMAIL_MASTER} WHERE TREATMENT_ID LIKE '%IRI%' OR TREATMENT_ID LIKE '%IPC%' LIMIT 10")
    except Exception as e:
        print(f"  DIAGNOSTIC query failed: {e}")

BATCH_SIZE = 50
email_results = []

for i in range(0, len(email_tactic_ids), BATCH_SIZE):
    batch = email_tactic_ids[i:i+BATCH_SIZE]
    tactic_list = ",".join(f"'{t}'" for t in batch)

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
    FROM {EMAIL_MASTER} FM
    INNER JOIN {EMAIL_EVENT} FE
        ON FM.CONSUMER_ID_HASHED = FE.CONSUMER_ID_HASHED
        AND FM.TREATMENT_ID = FE.TREATMENT_ID
    WHERE FM.TREATMENT_ID IN ({tactic_list})
    GROUP BY FM.CLNT_NO, FM.TREATMENT_ID
    """

    try:
        batch_df = edw_query(email_sql, f"email batch {i//BATCH_SIZE+1}/{(len(email_tactic_ids)-1)//BATCH_SIZE+1}")
        if len(batch_df) > 0:
            email_results.append(batch_df)
    except Exception as e:
        print(f"  WARNING: email batch failed: {e}")

if email_results:
    email_df = pd.concat(email_results, ignore_index=True)
    email_df['CLNT_NO'] = email_df['CLNT_NO'].astype(str).str.strip().str.lstrip('0')
    email_df.rename(columns={'TREATMENT_ID': 'TACTIC_ID'}, inplace=True)

    result_df = result_df.merge(email_df, on=['CLNT_NO', 'TACTIC_ID'], how='left')
    for ec in ['EMAIL_SENT', 'EMAIL_OPENED', 'EMAIL_CLICKED', 'EMAIL_UNSUBSCRIBED']:
        result_df[ec] = result_df[ec].fillna(0).astype(int)

    sent = result_df['EMAIL_SENT'].sum()
    print(f"\n  Email: {sent:,} sent, {result_df['EMAIL_OPENED'].sum():,} opened, "
          f"{result_df['EMAIL_CLICKED'].sum():,} clicked")
else:
    print("  No email data found.")
    for ec in ['EMAIL_SENT', 'EMAIL_OPENED', 'EMAIL_CLICKED', 'EMAIL_UNSUBSCRIBED']:
        result_df[ec] = 0
    for ec in ['EMAIL_SENT_DT', 'EMAIL_OPENED_DT', 'EMAIL_CLICKED_DT', 'EMAIL_UNSUBSCRIBED_DT']:
        result_df[ec] = pd.NaT


# %% [7] Population Summary & SRM

print("=" * 60)
print("POPULATION SUMMARY")

for mne in IMT_MNES:
    m = result_df[result_df['MNE'] == mne]
    print(f"\n{'─'*40}")
    print(f"MNE: {mne} — {len(m):,} rows, {m['CLNT_NO'].nunique():,} clients")

    for grp in [ACTION_GROUP, CONTROL_GROUP]:
        g = m[m['TST_GRP_CD'] == grp]
        print(f"  {grp}: {len(g):,} clients, window={MNE_WINDOWS.get(mne, MAX_DAYS)}d, "
              f"success={g['SUCCESS_FLAG'].mean()*100:.2f}%, "
              f"avg_imt={g['IMT'].mean():.3f}, avg_amt=${g['AMT_CAD'].mean():,.2f}")
        if 'EMAIL_SENT' in g.columns:
            es = g['EMAIL_SENT'].sum()
            if es > 0:
                print(f"    email: sent={es:,}, opened={g['EMAIL_OPENED'].sum():,}, "
                      f"clicked={g['EMAIL_CLICKED'].sum():,}")

    # SRM
    from scipy import stats
    a_n = len(m[m['TST_GRP_CD'] == ACTION_GROUP])
    c_n = len(m[m['TST_GRP_CD'] == CONTROL_GROUP])
    if a_n + c_n > 0:
        chi2, p = stats.chisquare([a_n, c_n])
        status = "POSSIBLE SRM" if p < 0.01 else "OK"
        print(f"  SRM: {a_n:,} vs {c_n:,}, ratio={a_n/(a_n+c_n):.3f}, p={p:.4f} [{status}]")


# %% [8] Vintage Curves — VVD v3 format

print("=" * 60)
print("BUILDING VINTAGE CURVES")

vintage_rows = []
group_cols = ['MNE', 'COHORT', 'TST_GRP_CD', 'RPT_GRP_CD']

for keys, grp in result_df.groupby(group_cols):
    mne, cohort, tst, rpt = keys
    n = len(grp)
    wd = int(grp['WINDOW_DAYS'].median())
    mne_max = MNE_WINDOWS.get(mne, MAX_DAYS)  # IRI=30, IPC=90

    # IMT success curve
    succ_days = grp[grp['SUCCESS_FLAG'] == 1]['DAYS_TO_SUCCESS'].dropna()
    for day in range(mne_max + 1):
        s = int((succ_days <= day).sum())
        vintage_rows.append({
            'MNE': mne, 'COHORT': cohort, 'TST_GRP_CD': tst, 'RPT_GRP_CD': rpt,
            'METRIC': 'imt_success', 'DAY': day, 'WINDOW_DAYS': wd,
            'CLIENT_CNT': n, 'SUCCESS_CNT': s, 'RATE': round(s / n * 100, 2) if n > 0 else 0
        })

    # Email curves
    email_metrics = {
        'email_sent': ('EMAIL_SENT', 'EMAIL_SENT_DT'),
        'email_open': ('EMAIL_OPENED', 'EMAIL_OPENED_DT'),
        'email_click': ('EMAIL_CLICKED', 'EMAIL_CLICKED_DT'),
        'email_unsub': ('EMAIL_UNSUBSCRIBED', 'EMAIL_UNSUBSCRIBED_DT'),
    }
    for metric_name, (flag_col, date_col) in email_metrics.items():
        if flag_col not in grp.columns or date_col not in grp.columns:
            continue
        em = grp[grp[flag_col] == 1].copy()
        if len(em) > 0:
            em[date_col] = pd.to_datetime(em[date_col], errors='coerce')
            em_days = ((em[date_col] - em['TREATMT_STRT_DT']).dt.days).dropna()
            em_days = em_days[(em_days >= 0) & (em_days <= mne_max)]
        else:
            em_days = pd.Series(dtype=float)

        for day in range(mne_max + 1):
            s = int((em_days <= day).sum())
            vintage_rows.append({
                'MNE': mne, 'COHORT': cohort, 'TST_GRP_CD': tst, 'RPT_GRP_CD': rpt,
                'METRIC': metric_name, 'DAY': day, 'WINDOW_DAYS': wd,
                'CLIENT_CNT': n, 'SUCCESS_CNT': s, 'RATE': round(s / n * 100, 2) if n > 0 else 0
            })

vintage_df = pd.DataFrame(vintage_rows)
print(f"Vintage curves: {len(vintage_df):,} rows, metrics: {vintage_df['METRIC'].unique().tolist()}")

# Final-day summary (IRI@30, IPC@90)
vintage_df['_mne_max'] = vintage_df['MNE'].map(MNE_WINDOWS).fillna(MAX_DAYS).astype(int)
d_final = vintage_df[vintage_df['DAY'] == vintage_df['_mne_max']]
vintage_df.drop(columns='_mne_max', inplace=True)
print(f"\nFinal-day rates (IRI@30d, IPC@90d):")
print(d_final.groupby(['MNE', 'TST_GRP_CD', 'METRIC']).agg(
    clients=('CLIENT_CNT', 'sum'), rate=('RATE', 'mean')
).round(2).to_string())


# %% [9] Lift & Significance

from scipy import stats

print("=" * 60)
print("LIFT ANALYSIS")

lift_rows = []
for mne in IMT_MNES:
    m = result_df[result_df['MNE'] == mne]
    a = m[m['TST_GRP_CD'] == ACTION_GROUP]
    c = m[m['TST_GRP_CD'] == CONTROL_GROUP]
    if len(a) == 0 or len(c) == 0:
        continue

    print(f"\n{mne}: Action(n={len(a):,}) vs Control(n={len(c):,})")
    print(f"  {'Metric':<20s} {'Action':>10s} {'Control':>10s} {'Lift':>10s} {'p':>8s}")

    metrics = [
        ('SUCCESS_RATE', 'SUCCESS_FLAG', 'mean'),
        ('AVG_IMT', 'IMT', 'mean'),
        ('AVG_AMT', 'AMT_CAD', 'mean'),
        ('IMT_30_RATE', 'IMT_30', lambda x: (x > 0).mean()),
        ('IMT_60_RATE', 'IMT_60', lambda x: (x > 0).mean()),
        ('IMT_90_RATE', 'IMT_90', lambda x: (x > 0).mean()),
    ]
    if 'EMAIL_OPENED' in a.columns:
        metrics.append(('EMAIL_OPEN', 'EMAIL_OPENED', 'mean'))
        metrics.append(('EMAIL_CLICK', 'EMAIL_CLICKED', 'mean'))

    for name, col, func in metrics:
        if col not in a.columns:
            continue
        av = func(a[col]) if callable(func) else a[col].agg(func)
        cv = func(c[col]) if callable(func) else c[col].agg(func)
        lift = ((av - cv) / cv * 100) if cv != 0 else 0

        if 'RATE' in name or name.startswith('EMAIL'):
            an, cn = len(a), len(c)
            a_s, c_s = int(av * an), int(cv * cn)
            pp = (a_s + c_s) / (an + cn) if (an + cn) > 0 else 0
            se = np.sqrt(pp * (1 - pp) * (1/an + 1/cn)) if 0 < pp < 1 else 1
            z = (av - cv) / se if se > 0 else 0
            pv = 2 * (1 - stats.norm.cdf(abs(z)))
        else:
            _, pv = stats.ttest_ind(
                pd.to_numeric(a[col], errors='coerce').fillna(0),
                pd.to_numeric(c[col], errors='coerce').fillna(0),
                equal_var=False
            )

        sig = "***" if pv < 0.001 else "**" if pv < 0.01 else "*" if pv < 0.05 else ""
        print(f"  {name:<20s} {av:>10.4f} {cv:>10.4f} {lift:>+9.1f}% {pv:>8.4f} {sig}")
        lift_rows.append({'MNE': mne, 'METRIC': name, 'ACTION': av, 'CONTROL': cv, 'LIFT': lift, 'P': pv})

lift_df = pd.DataFrame(lift_rows)


# %% [10] Export — Download Links + HDFS Backup

from IPython.display import display, HTML

print("=" * 60)
print("EXPORT")

def make_download_link(df, filename):
    csv = df.to_csv(index=False)
    mb = len(csv.encode()) / (1024 * 1024)
    if mb > 50:
        print(f"  {filename}: {mb:.1f} MB — too large for browser download")
        return
    b64 = base64.b64encode(csv.encode()).decode()
    link = (f'<a download="{filename}" href="data:text/csv;base64,{b64}" '
            f'target="_blank" style="font-size:16px;padding:10px 20px;background:#264f78;'
            f'color:white;text-decoration:none;border-radius:4px;">'
            f'Download {filename} ({mb:.1f} MB)</a>')
    display(HTML(link))
    print(f"  {filename}: {mb:.1f} MB, {len(df):,} rows")

# Vintage curves
make_download_link(vintage_df, "imt_edw_vintage_curves.csv")

# Client detail
detail_cols = [c for c in ['CLNT_NO', 'TACTIC_ID', 'MNE', 'COHORT', 'TST_GRP_CD', 'RPT_GRP_CD',
    'SEGMENT', 'NEWCOMER_SEGMENT', 'NEW_EXISTING', 'TREATMT_STRT_DT', 'TREATMT_END_DT',
    'WINDOW_DAYS', 'SUCCESS_FLAG', 'DAYS_TO_SUCCESS', 'FIRST_SUCCESS_DATE',
    'IMT', 'IMT_30', 'IMT_60', 'IMT_90', 'AMT_CAD', 'AMT_30', 'AMT_60', 'AMT_90',
    'IMT_OL', 'IMT_OL_30', 'IMT_OL_60', 'IMT_OL_90', 'AMT_OL', 'AMT_OL_30', 'AMT_OL_60', 'AMT_OL_90',
    'IMT_MB', 'IMT_MB_30', 'IMT_MB_60', 'IMT_MB_90', 'AMT_MB', 'AMT_MB_30', 'AMT_MB_60', 'AMT_MB_90',
    'EMAIL_SENT', 'EMAIL_OPENED', 'EMAIL_CLICKED', 'EMAIL_UNSUBSCRIBED'
] if c in result_df.columns]
make_download_link(result_df[detail_cols], "imt_edw_client_detail.csv")

# Lift
if len(lift_df) > 0:
    make_download_link(lift_df, "imt_edw_lift_summary.csv")

# HDFS backup
try:
    for fname, df in [("imt_edw_vintage_curves.csv", vintage_df),
                       ("imt_edw_client_detail.csv", result_df[detail_cols])]:
        local = f"/tmp/{fname}"
        df.to_csv(local, index=False)
        os.system(f"hdfs dfs -mkdir -p {HDFS_OUTPUT}")
        os.system(f"hdfs dfs -put -f {local} {HDFS_OUTPUT}/{fname}")
        print(f"  HDFS: {HDFS_OUTPUT}/{fname}")
except Exception as e:
    print(f"  HDFS backup failed: {e}")

print("\n" + "=" * 60)
print("DONE.")
