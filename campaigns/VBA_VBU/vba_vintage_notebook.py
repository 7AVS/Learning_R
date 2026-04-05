import pandas as pd
import time


def edw_query(sql, desc=""):
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


# ------------------------------------------------------------
# Cell 1 — Base data (population + Casper + SCOT responses)
#
# Single SQL query that pulls all individual-level response data.
# Reused by Cell 2 (summary) and Cell 3 (vintage curves) in pandas.
# ------------------------------------------------------------

sql_base = """
WITH vba_pop AS (
    SELECT DISTINCT
        E.clnt_no,
        CAST(E.tactic_id AS VARCHAR(50))           AS tactic_id,
        E.treatmt_strt_dt                          AS Treat_Start_DT,
        COALESCE(E.treatmt_end_dt, E.treatmt_strt_dt) AS Treat_End_DT,
        E.tst_grp_cd
    FROM DG6V01.tactic_evnt_ip_ar_hist E
    WHERE E.treatmt_strt_dt >= DATE '2025-11-01'
        AND SUBSTR(E.tactic_id, 8, 3) = 'VBA'
        AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
),
casper_apps AS (
    SELECT
        vba.tactic_id,
        vba.clnt_no,
        CASE WHEN p3c.Status = 'A' THEN p3c.acct_no END AS visa_acct_no,
        CASE WHEN p3c.Status = 'A' THEN 1 ELSE 0 END    AS visa_app_approved,
        CAST(p3c.app_rcv_dt AS DATE)                     AS visa_response_dt,
        'Casper' AS response_source
    FROM vba_pop vba
    INNER JOIN D3CV12A.appl_fact_dly p3c
        ON vba.clnt_no = p3c.bus_clnt_no
    WHERE p3c.app_rcv_dt BETWEEN vba.Treat_Start_DT AND vba.Treat_End_DT
        AND p3c.Status IN ('A','D','O')
        AND p3c.PROD_APPRVD IN ('B','E')
        AND (p3c.Cell_Code IS NULL OR p3c.Cell_Code NOT IN ('PATACT','GV0320'))
        AND p3c.CR_LMT_CHG_IND = 'N'
        AND p3c.visa_prod_cd NOT IN ('CCL','BXX')
),
scot_apps_raw AS (
    SELECT
        CAST(creditapplication_borrowers_borrowersrfnumber AS INTEGER) AS clnt_no,
        MAX(
            CASE
                WHEN creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid IS NOT NULL
                THEN TRY_CAST(
                    creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid AS BIGINT
                )
            END
        )                                                              AS visa_acct_no,
        MIN(CAST(creditapplication_createddatetime AS DATE))           AS visa_response_dt,
        CASE
            WHEN MAX(CASE WHEN creditapplication_creditapplicationstatuscode = 'FULFILLED' THEN 1 ELSE 0 END) = 1
            THEN 1 ELSE 0
        END                                                            AS visa_app_approved
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot
    WHERE creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory = 'CREDIT_CARD'
    GROUP BY CAST(creditapplication_borrowers_borrowersrfnumber AS INTEGER)
),
scot_apps AS (
    SELECT
        vba.tactic_id,
        vba.clnt_no,
        scot.visa_acct_no,
        scot.visa_app_approved,
        scot.visa_response_dt,
        'Scott' AS response_source
    FROM vba_pop vba
    INNER JOIN scot_apps_raw scot
        ON vba.clnt_no = scot.clnt_no
    WHERE scot.visa_response_dt BETWEEN vba.Treat_Start_DT AND vba.Treat_End_DT
),
responses AS (
    SELECT tactic_id, clnt_no, visa_acct_no, visa_app_approved,
           CAST(visa_response_dt AS DATE) AS visa_response_dt, response_source
    FROM casper_apps
    UNION ALL
    SELECT tactic_id, clnt_no, visa_acct_no, visa_app_approved,
           CAST(visa_response_dt AS DATE) AS visa_response_dt, response_source
    FROM scot_apps
)
SELECT
    r.tactic_id,
    r.clnt_no,
    r.visa_acct_no,
    r.visa_app_approved,
    r.visa_response_dt,
    r.response_source,
    v.Treat_Start_DT,
    v.Treat_End_DT,
    v.tst_grp_cd,
    SUBSTR(r.tactic_id, 8, 3) AS mne
FROM responses r
INNER JOIN vba_pop v
    ON r.tactic_id = v.tactic_id
    AND r.clnt_no = v.clnt_no
"""

df_base = edw_query(sql_base, desc="VBA Base Data")
print(f"Columns: {list(df_base.columns)}")
print(df_base.head(10))

# Also pull population counts (needed for denominators)
sql_pop = """
SELECT DISTINCT
    CAST(E.tactic_id AS VARCHAR(50))           AS tactic_id,
    E.clnt_no,
    E.treatmt_strt_dt                          AS Treat_Start_DT,
    COALESCE(E.treatmt_end_dt, E.treatmt_strt_dt) AS Treat_End_DT,
    E.tst_grp_cd,
    SUBSTR(E.tactic_id, 8, 3)                 AS mne
FROM DG6V01.tactic_evnt_ip_ar_hist E
WHERE E.treatmt_strt_dt >= DATE '2025-11-01'
    AND SUBSTR(E.tactic_id, 8, 3) = 'VBA'
    AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
"""

df_pop = edw_query(sql_pop, desc="VBA Population")
print(f"Population: {len(df_pop):,} rows, {df_pop['clnt_no'].nunique():,} unique clients")


# ------------------------------------------------------------
# Cell 2 — Summary (leads, responders, rates by tactic, tst_grp_cd)
# ------------------------------------------------------------

# Leads per (tactic_id, tst_grp_cd)
leads = df_pop.groupby(['tactic_id', 'tst_grp_cd', 'mne']).agg(
    Treat_Start_DT=('Treat_Start_DT', 'min'),
    leads=('clnt_no', 'nunique')
).reset_index()

# Approved responses only
approved = df_base[df_base['visa_app_approved'] == 1].copy()

# Responders by source
casper_resp = approved[approved['response_source'] == 'Casper'].groupby(
    ['tactic_id', 'tst_grp_cd']
)['clnt_no'].nunique().reset_index(name='successes_casper')

scott_resp = approved[approved['response_source'] == 'Scott'].groupby(
    ['tactic_id', 'tst_grp_cd']
)['clnt_no'].nunique().reset_index(name='successes_scott')

any_resp = approved.groupby(
    ['tactic_id', 'tst_grp_cd']
)['clnt_no'].nunique().reset_index(name='successes_any')

# Merge
df_summary = leads.merge(casper_resp, on=['tactic_id', 'tst_grp_cd'], how='left') \
                   .merge(scott_resp, on=['tactic_id', 'tst_grp_cd'], how='left') \
                   .merge(any_resp, on=['tactic_id', 'tst_grp_cd'], how='left')

df_summary = df_summary.fillna(0)
for col in ['successes_casper', 'successes_scott', 'successes_any']:
    df_summary[col] = df_summary[col].astype(int)

df_summary['rate_casper'] = round(df_summary['successes_casper'] / df_summary['leads'] * 100, 2)
df_summary['rate_scott'] = round(df_summary['successes_scott'] / df_summary['leads'] * 100, 2)
df_summary['rate_any'] = round(df_summary['successes_any'] / df_summary['leads'] * 100, 2)

df_summary = df_summary.sort_values(['tactic_id', 'tst_grp_cd'])
print(df_summary)


# ------------------------------------------------------------
# Cell 3 — Vintage Curves (0-90 days)
# ------------------------------------------------------------

# Earliest approved response per client, per source
approved = df_base[df_base['visa_app_approved'] == 1].copy()
approved['Treat_Start_DT'] = pd.to_datetime(approved['Treat_Start_DT'])
approved['visa_response_dt'] = pd.to_datetime(approved['visa_response_dt'])
approved['vintage'] = (approved['visa_response_dt'] - approved['Treat_Start_DT']).dt.days
approved = approved[(approved['vintage'] >= 0) & (approved['vintage'] <= 90)]

# Earliest response per (clnt_no, tactic_id, response_source)
earliest = approved.sort_values('visa_response_dt').drop_duplicates(
    subset=['clnt_no', 'tactic_id', 'response_source'], keep='first'
)

# Daily counts by (mne, tst_grp_cd, Treat_Start_DT, Treat_End_DT, vintage, response_source)
daily = earliest.groupby(
    ['mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT', 'vintage', 'response_source']
)['clnt_no'].nunique().reset_index(name='daily_count')

# Pivot sources into columns
daily_primary = daily[daily['response_source'] == 'Casper'].rename(
    columns={'daily_count': 'success_daily_primary'}
).drop(columns='response_source')

daily_secondary = daily[daily['response_source'] == 'Scott'].rename(
    columns={'daily_count': 'success_daily_secondary'}
).drop(columns='response_source')

# Scaffold: 0-90 for each cohort
pop_cohort = df_pop.copy()
pop_cohort['Treat_Start_DT'] = pd.to_datetime(pop_cohort['Treat_Start_DT'])
pop_cohort['Treat_End_DT'] = pd.to_datetime(pop_cohort['Treat_End_DT'])

cohort = pop_cohort.groupby(['mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT']).agg(
    leads=('clnt_no', 'nunique')
).reset_index()

import numpy as np
scaffold_rows = []
for _, row in cohort.iterrows():
    for v in range(91):
        scaffold_rows.append({
            'mne': row['mne'],
            'tst_grp_cd': row['tst_grp_cd'],
            'Treat_Start_DT': row['Treat_Start_DT'],
            'Treat_End_DT': row['Treat_End_DT'],
            'leads': row['leads'],
            'vintage': v
        })
scaffold = pd.DataFrame(scaffold_rows)

# Merge daily counts onto scaffold
df_vintage = scaffold.merge(
    daily_primary,
    on=['mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT', 'vintage'],
    how='left'
).merge(
    daily_secondary,
    on=['mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT', 'vintage'],
    how='left'
)

df_vintage['success_daily_primary'] = df_vintage['success_daily_primary'].fillna(0).astype(int)
df_vintage['success_daily_secondary'] = df_vintage['success_daily_secondary'].fillna(0).astype(int)

# Cumulative
df_vintage = df_vintage.sort_values(['mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT', 'vintage'])
df_vintage['success_cum_primary'] = df_vintage.groupby(
    ['mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT']
)['success_daily_primary'].cumsum()
df_vintage['success_cum_secondary'] = df_vintage.groupby(
    ['mne', 'tst_grp_cd', 'Treat_Start_DT', 'Treat_End_DT']
)['success_daily_secondary'].cumsum()

print(df_vintage)


# ------------------------------------------------------------
# Cell 4 — Export to CSV
# ------------------------------------------------------------

df_summary.to_csv('vba_vintage_summary.csv', index=False)
print(f"Summary exported: vba_vintage_summary.csv ({len(df_summary)} rows)")

df_vintage.to_csv('vba_vintage_curves.csv', index=False)
print(f"Vintage curves exported: vba_vintage_curves.csv ({len(df_vintage)} rows)")
