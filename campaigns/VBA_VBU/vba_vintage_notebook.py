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
# Connector Validation
#
# Testing whether the EDW.cursor() connection can reach both
# Teradata (EDW) tables and EDL (Starburst/Trino) tables
# through the same cursor. If both pass, the VBA vintage CTE
# chain can be written as a single unified query. If EDL fails,
# we split into two queries and merge in pandas.
# ------------------------------------------------------------

# Test 1 — Teradata (EDW): tactic history table, VBA rows
sql_edw = """
SELECT *
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE SUBSTR(TACTIC_ID, 8, 3) = 'VBA'
LIMIT 5
"""

try:
    df_edw = edw_query(sql_edw, desc="EDW / Teradata")
    print("PASS — EDW cursor reached Teradata")
    print(df_edw)
except Exception as e:
    print(f"FAIL — EDW cursor could not reach Teradata: {e}")


# Test 2 — EDL: SCOT credit application snapshot (via EDW cursor)
# Fields from original SAS transcription (vba_success_original_sas.sql)
sql_edl = """
SELECT
    creditapplication_borrowers_borrowersrfnumber,
    creditapplication_creditapplicationstatuscode,
    creditapplication_createddatetime,
    creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot
WHERE creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory IN ('CREDIT_CARD')
LIMIT 5
"""

try:
    df_edl = edw_query(sql_edl, desc="EDL / Starburst")
    print("PASS — EDW cursor reached EDL")
    print(df_edl)
except Exception as e:
    print(f"FAIL — EDW cursor could not reach EDL: {e}")


# ------------------------------------------------------------
# Result
#
# - Both queries returned rows: unified CTE approach works.
#   The VBA vintage chain can be written as a single query
#   joining Teradata and EDL tables through one cursor.
# - EDL query failed: need separate queries — pull Teradata
#   (Casper primary) and EDL (SCOT secondary) independently,
#   then merge on clnt_no in pandas.
# ------------------------------------------------------------


# ------------------------------------------------------------
# Cell 2 — VBA Vintage Summary
# ------------------------------------------------------------

sql_summary = """
WITH vba_pop AS (
    SELECT DISTINCT
        E.clnt_no,
        CAST(E.tactic_id AS VARCHAR(50))           AS tactic_id,
        E.treatmt_strt_dt                          AS Treat_Start_DT,
        E.treatmt_end_dt                           AS Treat_End_DT,
        E.tst_grp_cd
    FROM DG6V01.tactic_evnt_ip_ar_hist E
    WHERE E.treatmt_strt_dt >= DATE '2025-11-01'
        AND SUBSTR(E.tactic_id, 8, 3) = 'VBA'
        AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
),
casper_apps AS (
    SELECT
        vba.clnt_no,
        vba.tactic_id,
        CASE WHEN p3c.Status IN ('A') THEN p3c.acct_no ELSE NULL END AS visa_acct_no,
        CASE WHEN p3c.Status IN ('A') THEN 1 ELSE 0 END   AS visa_app_approved,
        p3c.app_rcv_dt                                     AS visa_response_dt
    FROM vba_pop vba
    INNER JOIN d3cv12a.appl_fact_dly p3c
        ON vba.clnt_no = p3c.bus_clnt_no
    WHERE
        p3c.app_rcv_dt BETWEEN vba.Treat_Start_DT AND vba.Treat_End_DT
        AND (p3c.Status IN ('D', 'O') OR p3c.Status IN ('A'))
        AND p3c.PROD_APPRVD IN ('B', 'E')
        AND (p3c.Cell_Code IS NULL OR p3c.Cell_Code <> 'PATACT')
        AND p3c.CR_LMT_CHG_IND = 'N'
        AND p3c.visa_prod_cd NOT IN ('CCL', 'BXX')
        AND (p3c.Cell_Code IS NULL OR p3c.Cell_Code <> 'GV0320')
),
scot_apps_raw AS (
    SELECT
        CAST(creditapplication_borrowers_borrowersrfnumber AS INTEGER) AS clnt_no,
        CAST(creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid AS INTEGER) AS visa_acct_no,
        MIN(CAST(creditapplication_createddatetime AS DATE))           AS visa_response_dt,
        MAX(CASE
            WHEN creditapplication_creditapplicationstatuscode IN ('FULFILLED') THEN 1 ELSE 0
        END)                                                           AS visa_app_approved
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot
    WHERE creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory IN ('CREDIT_CARD')
    GROUP BY 1, 2
),
scot_apps AS (
    SELECT
        vba.clnt_no,
        vba.tactic_id,
        scot.visa_acct_no,
        scot.visa_app_approved,
        scot.visa_response_dt
    FROM vba_pop vba
    INNER JOIN scot_apps_raw scot
        ON vba.clnt_no = scot.clnt_no
    WHERE scot.visa_response_dt BETWEEN vba.Treat_Start_DT AND vba.Treat_End_DT
),
responders_primary AS (
    SELECT DISTINCT clnt_no, tactic_id FROM casper_apps WHERE visa_app_approved = 1
),
responders_secondary AS (
    SELECT DISTINCT clnt_no, tactic_id FROM scot_apps WHERE visa_app_approved = 1
)
SELECT
    SUBSTR(b.tactic_id, 8, 3)                     AS MNE,
    b.tst_grp_cd,
    b.Treat_Start_DT,
    CASE
        WHEN MONTH(b.Treat_Start_DT) IN (11, 12) THEN CAST(YEAR(b.Treat_Start_DT) + 1 AS VARCHAR(4))
        ELSE CAST(YEAR(b.Treat_Start_DT) AS VARCHAR(4))
    END
    || CASE
        WHEN MONTH(b.Treat_Start_DT) IN (11, 12, 1) THEN 'Q1'
        WHEN MONTH(b.Treat_Start_DT) IN (2, 3, 4)    THEN 'Q2'
        WHEN MONTH(b.Treat_Start_DT) IN (5, 6, 7)    THEN 'Q3'
        WHEN MONTH(b.Treat_Start_DT) IN (8, 9, 10)   THEN 'Q4'
        ELSE 'error'
    END                                            AS yearqtr,
    COUNT(DISTINCT b.clnt_no)                      AS leads,
    COUNT(DISTINCT rp.clnt_no)                     AS primary_responders,
    COUNT(DISTINCT rs.clnt_no)                     AS secondary_responders,
    CASE WHEN COUNT(DISTINCT b.clnt_no) = 0 THEN 0
        ELSE CAST(COUNT(DISTINCT rp.clnt_no) AS DECIMAL(18,6))
             / CAST(COUNT(DISTINCT b.clnt_no) AS DECIMAL(18,6))
    END                                            AS primary_rate,
    CASE WHEN COUNT(DISTINCT b.clnt_no) = 0 THEN 0
        ELSE CAST(COUNT(DISTINCT rs.clnt_no) AS DECIMAL(18,6))
             / CAST(COUNT(DISTINCT b.clnt_no) AS DECIMAL(18,6))
    END                                            AS secondary_rate
FROM vba_pop b
LEFT JOIN responders_primary rp
    ON rp.clnt_no = b.clnt_no AND rp.tactic_id = b.tactic_id
LEFT JOIN responders_secondary rs
    ON rs.clnt_no = b.clnt_no AND rs.tactic_id = b.tactic_id
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
"""

try:
    df_summary = edw_query(sql_summary, desc="VBA Summary")
    print(df_summary)
except Exception as e:
    print(f"Summary query failed: {e}")


# ------------------------------------------------------------
# Cell 3 — VBA Vintage Curves (0-90 days)
# ------------------------------------------------------------

sql_vintage = """
WITH vba_pop AS (
    SELECT DISTINCT
        E.clnt_no,
        CAST(E.tactic_id AS VARCHAR(50))           AS tactic_id,
        E.treatmt_strt_dt                          AS Treat_Start_DT,
        E.treatmt_end_dt                           AS Treat_End_DT,
        E.tst_grp_cd
    FROM DG6V01.tactic_evnt_ip_ar_hist E
    WHERE E.treatmt_strt_dt >= DATE '2025-11-01'
        AND SUBSTR(E.tactic_id, 8, 3) = 'VBA'
        AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
),
casper_apps AS (
    SELECT
        vba.clnt_no,
        vba.tactic_id,
        vba.Treat_Start_DT,
        vba.Treat_End_DT,
        CASE WHEN p3c.Status IN ('A') THEN p3c.acct_no ELSE NULL END AS visa_acct_no,
        CASE WHEN p3c.Status IN ('A') THEN 1 ELSE 0 END   AS visa_app_approved,
        p3c.app_rcv_dt                                     AS visa_response_dt
    FROM vba_pop vba
    INNER JOIN d3cv12a.appl_fact_dly p3c
        ON vba.clnt_no = p3c.bus_clnt_no
    WHERE
        p3c.app_rcv_dt BETWEEN vba.Treat_Start_DT AND vba.Treat_End_DT
        AND (p3c.Status IN ('D', 'O') OR p3c.Status IN ('A'))
        AND p3c.PROD_APPRVD IN ('B', 'E')
        AND (p3c.Cell_Code IS NULL OR p3c.Cell_Code <> 'PATACT')
        AND p3c.CR_LMT_CHG_IND = 'N'
        AND p3c.visa_prod_cd NOT IN ('CCL', 'BXX')
        AND (p3c.Cell_Code IS NULL OR p3c.Cell_Code <> 'GV0320')
),
scot_apps_raw AS (
    SELECT
        CAST(creditapplication_borrowers_borrowersrfnumber AS INTEGER) AS clnt_no,
        CAST(creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid AS INTEGER) AS visa_acct_no,
        MIN(CAST(creditapplication_createddatetime AS DATE))           AS visa_response_dt,
        MAX(CASE
            WHEN creditapplication_creditapplicationstatuscode IN ('FULFILLED') THEN 1 ELSE 0
        END)                                                           AS visa_app_approved
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot
    WHERE creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory IN ('CREDIT_CARD')
    GROUP BY 1, 2
),
scot_apps AS (
    SELECT
        vba.clnt_no,
        vba.tactic_id,
        vba.Treat_Start_DT,
        vba.Treat_End_DT,
        scot.visa_acct_no,
        scot.visa_app_approved,
        scot.visa_response_dt
    FROM vba_pop vba
    INNER JOIN scot_apps_raw scot
        ON vba.clnt_no = scot.clnt_no
    WHERE scot.visa_response_dt BETWEEN vba.Treat_Start_DT AND vba.Treat_End_DT
),
earliest_primary_by_client AS (
    SELECT
        clnt_no,
        tactic_id,
        Treat_Start_DT,
        Treat_End_DT,
        MIN(visa_response_dt)                      AS first_response_dt
    FROM casper_apps
    WHERE visa_app_approved = 1
    GROUP BY 1, 2, 3, 4
),
earliest_secondary_by_client AS (
    SELECT
        clnt_no,
        tactic_id,
        Treat_Start_DT,
        Treat_End_DT,
        MIN(visa_response_dt)                      AS first_response_dt
    FROM scot_apps
    WHERE visa_app_approved = 1
    GROUP BY 1, 2, 3, 4
),
vintages_primary AS (
    SELECT
        clnt_no,
        tactic_id,
        Treat_Start_DT,
        Treat_End_DT,
        CASE
            WHEN first_response_dt < Treat_Start_DT THEN 0
            ELSE DATE_DIFF('day', Treat_Start_DT, first_response_dt)
        END                                        AS vintage
    FROM earliest_primary_by_client
    WHERE CASE
            WHEN first_response_dt < Treat_Start_DT THEN 0
            ELSE DATE_DIFF('day', Treat_Start_DT, first_response_dt)
        END BETWEEN 0 AND 90
),
vintages_secondary AS (
    SELECT
        clnt_no,
        tactic_id,
        Treat_Start_DT,
        Treat_End_DT,
        CASE
            WHEN first_response_dt < Treat_Start_DT THEN 0
            ELSE DATE_DIFF('day', Treat_Start_DT, first_response_dt)
        END                                        AS vintage
    FROM earliest_secondary_by_client
    WHERE CASE
            WHEN first_response_dt < Treat_Start_DT THEN 0
            ELSE DATE_DIFF('day', Treat_Start_DT, first_response_dt)
        END BETWEEN 0 AND 90
),
cohort AS (
    SELECT
        SUBSTR(b.tactic_id, 8, 3)                 AS mne,
        b.Treat_Start_DT,
        b.Treat_End_DT,
        COUNT(DISTINCT b.clnt_no)                  AS leads
    FROM vba_pop b
    GROUP BY 1, 2, 3
),
successes_primary AS (
    SELECT
        SUBSTR(v.tactic_id, 8, 3)                 AS mne,
        v.Treat_Start_DT,
        v.Treat_End_DT,
        v.vintage,
        COUNT(DISTINCT v.clnt_no)                  AS success_daily_primary
    FROM vintages_primary v
    GROUP BY 1, 2, 3, 4
),
successes_secondary AS (
    SELECT
        SUBSTR(v.tactic_id, 8, 3)                 AS mne,
        v.Treat_Start_DT,
        v.Treat_End_DT,
        v.vintage,
        COUNT(DISTINCT v.clnt_no)                  AS success_daily_secondary
    FROM vintages_secondary v
    GROUP BY 1, 2, 3, 4
),
scaffold AS (
    SELECT
        c.mne,
        c.Treat_Start_DT,
        c.Treat_End_DT,
        c.leads,
        t.vintage
    FROM cohort c
    CROSS JOIN UNNEST(SEQUENCE(0, 90)) AS t(vintage)
)
SELECT
    s.mne,
    s.Treat_Start_DT,
    s.Treat_End_DT,
    s.vintage,
    s.leads,
    COALESCE(p.success_daily_primary, 0)           AS success_daily_primary,
    SUM(COALESCE(p.success_daily_primary, 0)) OVER (
        PARTITION BY s.mne, s.Treat_Start_DT, s.Treat_End_DT
        ORDER BY s.vintage
        ROWS UNBOUNDED PRECEDING
    )                                              AS success_cum_primary,
    COALESCE(sc.success_daily_secondary, 0)        AS success_daily_secondary,
    SUM(COALESCE(sc.success_daily_secondary, 0)) OVER (
        PARTITION BY s.mne, s.Treat_Start_DT, s.Treat_End_DT
        ORDER BY s.vintage
        ROWS UNBOUNDED PRECEDING
    )                                              AS success_cum_secondary
FROM scaffold s
LEFT JOIN successes_primary p
    ON p.mne = s.mne
    AND p.Treat_Start_DT = s.Treat_Start_DT
    AND p.Treat_End_DT = s.Treat_End_DT
    AND p.vintage = s.vintage
LEFT JOIN successes_secondary sc
    ON sc.mne = s.mne
    AND sc.Treat_Start_DT = s.Treat_Start_DT
    AND sc.Treat_End_DT = s.Treat_End_DT
    AND sc.vintage = s.vintage
ORDER BY s.mne, s.Treat_Start_DT, s.Treat_End_DT, s.vintage
"""

try:
    df_vintage = edw_query(sql_vintage, desc="VBA Vintage Curves")
    print(df_vintage)
except Exception as e:
    print(f"Vintage curves query failed: {e}")


# ------------------------------------------------------------
# Cell 4 — Export to CSV
# ------------------------------------------------------------

try:
    df_summary.to_csv('vba_vintage_summary.csv', index=False)
    print(f"Summary exported: vba_vintage_summary.csv ({len(df_summary)} rows)")
except Exception as e:
    print(f"Summary CSV export failed: {e}")

try:
    df_vintage.to_csv('vba_vintage_curves.csv', index=False)
    print(f"Vintage curves exported: vba_vintage_curves.csv ({len(df_vintage)} rows)")
except Exception as e:
    print(f"Vintage curves CSV export failed: {e}")
