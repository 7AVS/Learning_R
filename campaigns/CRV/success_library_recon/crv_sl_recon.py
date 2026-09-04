# CRV (installments) x Success Library reconciliation, step 1 of 5.
# Runs 01_cohort_arm_summary.sql via Starburst/Trino federation (dg6v01 <-> edl0_im).

# %% [0] Connection - EDL = Trino (copied from unsub_tracking/museum/cpc_reservoir_extract.py cell [1], Teradata half removed: this query is fully federated through Starburst)
import getpass
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")

TRINO_HOST = "strplvaexh0001.fg.rbc.com"     # letter l confirmed by DNS; digit-1 spelling does not resolve

# verify=False is the platform norm (your working cell; verified TLS tested once 2026-07-24 - corp cert not in trust store)
import urllib3, warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")
EDL = connect(host=TRINO_HOST, port=8443, catalog="edl0_im", user=username,
              auth=BasicAuthentication(username, password), http_scheme="https", verify=False)

# PROOF, not prints: round-trip the connection and show what the SERVER returned
tcur = EDL.cursor()
tcur.execute("SELECT 1")
print("EDL round-trip (trino) returned:", tcur.fetchall())
tcur.close()

# %% [1] Parameters - the only values substituted into the SQL below
MNE               = 'CRV'                       # campaign mnemonic: substr(tactic_id, 8, 3)
COHORT_START      = '2026-01-01'                 # date floor, applied to both tactic and event tables
EVENT_CD          = 'p_card_installmt_purch'      # Success Library event code counted as conversion
CONTROL_TST_GRP   = 'TG8'                        # tst_grp_cd value that marks Control (else Action)
OUT_DIR           = 'campaigns/CRV/success_library_recon/output'  # relative output folder

import os
os.makedirs(OUT_DIR, exist_ok=True)

# %% [2] SQL_01 - 01_cohort_arm_summary.sql, verbatim CTE structure, semicolon stripped for Trino
SQL_01 = f"""
-- 01_cohort_arm_summary.sql
-- CRV (installments) x Success Library reconciliation, step 1 of 5.
-- ENGINE   : Starburst/Trino (federated dg6v01 <-> edl0_im).
-- Output   : one row per cohort_month x arm: treated accounts, accounts with >=1
--            installment-purchase event in the Success Library. Counts only.
-- Arms     : DG6V01.TACTIC_EVNT_IP_AR_HIST, substr(tactic_id,8,3)='CRV',
--            tst_grp_cd='TG8' -> Control else Action
--            (source: campaigns/CRV/vintage_reconciliation/crv_vintage_v2_production.sql).
-- Cohort   : month of treatmt_eff_dt (effective date; treatmt_strt_dt misclassifies ~1.7%).
--            Same account in one (cohort_month, arm) cell across waves is collapsed with
--            MIN(treatmt_eff_dt)/MAX(treatmt_end_dt). Counted again in other cohorts.
-- Success  : edl0_im.prod_zp10_prod_staging.measurement_events_v2,
--            event_cd='p_card_installmt_purch', event_date inside the account's own
--            [treatmt_eff_dt, treatmt_end_dt] window (canonical CRV rule, no fixed N days).
-- Key      : CAST(visa_acct_no AS DECIMAL(38,0)) = CAST(acct_no AS DECIMAL(38,0)).
-- Guard    : population and arm come only from the tactic table.
-- Run date : not yet executed.

WITH

-- CRV deployments from 2026-01-01, collapsed to one row per account x cohort x arm
tactic_cohort AS (
    SELECT
        visa_acct_no,
        date_trunc('month', treatmt_eff_dt)                             AS cohort_month,
        CASE WHEN tst_grp_cd = '{CONTROL_TST_GRP}' THEN 'Control' ELSE 'Action' END     AS arm,
        MIN(treatmt_eff_dt)                                             AS acct_treat_dt,
        MAX(treatmt_end_dt)                                             AS acct_treat_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = '{MNE}'
      AND treatmt_eff_dt >= DATE '{COHORT_START}'
      AND treatmt_eff_dt IS NOT NULL
    GROUP BY
        visa_acct_no,
        date_trunc('month', treatmt_eff_dt),
        CASE WHEN tst_grp_cd = '{CONTROL_TST_GRP}' THEN 'Control' ELSE 'Action' END
),

-- one row per cohort_month x arm: treated population + the cell's own treat-date bounds
cohort_arm_cells AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT visa_acct_no) AS treated_accts,
        MIN(acct_treat_dt)           AS first_treat_dt,
        MAX(acct_treat_dt)           AS last_treat_dt
    FROM tactic_cohort
    GROUP BY cohort_month, arm
),

-- Success Library hits inside each account's own [treatmt_eff_dt, treatmt_end_dt] window.
-- Partition keys (event_cd, event_date) filtered before the join; account key normalized
-- via widening DECIMAL(38,0) cast on both sides (join-condition predicates don't push
-- down, so the constant event_date floor is kept alongside the dynamic window bound).
success_events AS (
    SELECT
        t.visa_acct_no,
        t.cohort_month,
        t.arm
    FROM tactic_cohort t
    JOIN edl0_im.prod_zp10_prod_staging.measurement_events_v2 m
        ON CAST(t.visa_acct_no AS DECIMAL(38,0)) = CAST(m.acct_no AS DECIMAL(38,0))
    WHERE m.event_cd = '{EVENT_CD}'
      AND m.event_date >= DATE '{COHORT_START}'
      AND m.event_date BETWEEN t.acct_treat_dt AND t.acct_treat_end_dt
),

-- accounts with >=1 success, per cohort_month x arm
success_cells AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT visa_acct_no) AS success_accts
    FROM success_events
    GROUP BY cohort_month, arm
)

SELECT
    c.cohort_month,
    c.arm,
    c.treated_accts,
    COALESCE(s.success_accts, 0) AS success_accts,
    c.first_treat_dt,
    c.last_treat_dt
FROM cohort_arm_cells c
LEFT JOIN success_cells s
    ON  s.cohort_month = c.cohort_month
    AND s.arm          = c.arm
ORDER BY c.cohort_month, c.arm
"""

# %% [3] Run SQL_01, display in full, save to OUT_DIR, then prove the result against COHORT_START
pd.set_option("display.max_rows", 100)
pd.options.display.float_format = "{:.0f}".format  # no scientific notation on any id-like float

df = pd.read_sql(SQL_01, EDL)
print(f"rows returned: {len(df):,}")
display(df)

out_path = os.path.join(OUT_DIR, "01_cohort_arm_summary.csv")
df.to_csv(out_path, index=False)
print("saved ->", out_path)

# PROOF, not a print: fail loudly if the cohort floor or population is wrong
assert len(df) > 0, "SQL_01 returned zero rows - stop and investigate before proceeding"
cohort_min = pd.to_datetime(df["cohort_month"]).min()
cohort_max = pd.to_datetime(df["cohort_month"]).max()
assert cohort_min >= pd.to_datetime(COHORT_START), \
    f"cohort_month {cohort_min} is before the COHORT_START floor {COHORT_START}"
print(f"Expected cohorts from {COHORT_START}, got {cohort_min.date()} to {cohort_max.date()}, {len(df)} rows.")

# %% [4] Sanity - treated/success counts per arm, summed across cohorts (counts only, no rates)
# compare to the dashboard totals for the same months
arm_totals = df.groupby("arm", as_index=False)[["treated_accts", "success_accts"]].sum()
display(arm_totals)
