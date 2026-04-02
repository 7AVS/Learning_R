-- =============================================================================
-- VBA Campaign — Vintage Curves (0-90 days)
-- =============================================================================
--
-- Purpose:
--   Daily vintage curves showing credit card application success rates over
--   a 0-90 day window post-deployment. VBA success = new credit card
--   application approved (NOT product changes like VBU).
--
-- Adapted from: VBU Validation workbook vintage structure (Daniel Chin)
-- Success logic from: VBA Campaign Success SAS code (transcribed)
--
-- VBA vs VBU — DIFFERENT success definitions:
--   VBU: product CHANGE on existing account (dly_full_portfolio → AIB target)
--   VBA: new credit card APPLICATION approved (Casper + SCOT)
--   DO NOT use dly_full_portfolio, AIB, or prior_target for VBA.
--
-- Tables:
--   DG6V01.tactic_evnt_ip_ar_hist — tactic population
--   p3c.appl_fact_dly — Casper: credit card application fact (Teradata)
--   edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot — SCOT
--   SYS_CALENDAR.CALENDAR — system calendar for zero-fill scaffold
--
-- Success definition (from VBA SAS code):
--   Casper: Status IN ('D','O','A'), PROD_APPRVD IN ('B','E'),
--           CR_LMT_CHG_IND = 'N', exclude CCL/BXX, exclude PATACT/GV0320
--           Approved = Status 'A'
--   SCOT:   productcategory = 'CREDIT_CARD', statuscode = 'FULFILLED'
--   Combined: visa_app_approved = 1 from either source
--   Response date: earliest visa_Response_Dt
--   Window: response date BETWEEN treatmt_strt_dt AND treatmt_end_dt
--
-- Vintage = days from treatmt_strt_dt to earliest visa_Response_Dt (0-90)
--
-- Run in: Teradata (uses SYS_CALENDAR). See notes for Starburst adaptation.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 1: Summary — Leads, Responders, Response Rates by Fiscal Quarter
-- ---------------------------------------------------------------------------

WITH vba_pop AS (
    -- VBA population from tactic event history
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
    -- Credit card applications from Casper (p3c.appl_fact_dly)
    SELECT
        vba.clnt_no,
        vba.tactic_id,
        CASE WHEN p3c.Status IN ('A') THEN p3c.acct_no ELSE NULL END AS visa_acct_no,
        CASE WHEN p3c.Status IN ('A') THEN 1 ELSE 0 END   AS visa_app_approved,
        p3c.app_rcv_dt                                     AS visa_response_dt
    FROM vba_pop vba
    INNER JOIN p3c.appl_fact_dly p3c
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
    -- Credit card applications from SCOT (one row per client)
    SELECT
        CAST(creditapplication_borrowers_borrowersrfnumber AS INTEGER) AS clnt_no,
        MAX(CASE
            WHEN creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid IS NOT NULL
            THEN CAST(creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid AS INTEGER)
            ELSE NULL
        END)                                                           AS visa_acct_no,
        MIN(CAST(creditapplication_createddatetime AS DATE))           AS visa_response_dt,
        MAX(CASE
            WHEN creditapplication_creditapplicationstatuscode IN ('FULFILLED') THEN 1 ELSE 0
        END)                                                           AS visa_app_approved
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot
    WHERE creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory IN ('CREDIT_CARD')
    GROUP BY 1
),
scot_apps AS (
    -- SCOT applications joined to VBA population (within treatment window)
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
all_apps AS (
    -- Combine both sources
    SELECT clnt_no, tactic_id, visa_acct_no, visa_app_approved, visa_response_dt FROM casper_apps
    UNION ALL
    SELECT clnt_no, tactic_id, visa_acct_no, visa_app_approved, visa_response_dt FROM scot_apps
),
client_success AS (
    -- Deduplicate: one row per client+tactic, best outcome
    SELECT
        clnt_no,
        tactic_id,
        MAX(visa_app_approved)                                     AS visa_app_approved,
        MIN(CASE WHEN visa_app_approved = 1 THEN visa_response_dt END) AS earliest_response_dt
    FROM all_apps
    GROUP BY clnt_no, tactic_id
),
responders_any AS (
    -- Any application (approved or not) within window
    SELECT DISTINCT clnt_no, tactic_id FROM all_apps
),
responders_approved AS (
    -- Approved applications only
    SELECT DISTINCT clnt_no, tactic_id FROM client_success WHERE visa_app_approved = 1
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
    COUNT(DISTINCT ra.clnt_no)                     AS apps_any,
    COUNT(DISTINCT rp.clnt_no)                     AS apps_approved,
    CASE WHEN COUNT(DISTINCT b.clnt_no) = 0 THEN 0
        ELSE CAST(COUNT(DISTINCT ra.clnt_no) AS DECIMAL(18,6))
             / CAST(COUNT(DISTINCT b.clnt_no) AS DECIMAL(18,6))
    END                                            AS app_rate_any,
    CASE WHEN COUNT(DISTINCT b.clnt_no) = 0 THEN 0
        ELSE CAST(COUNT(DISTINCT rp.clnt_no) AS DECIMAL(18,6))
             / CAST(COUNT(DISTINCT b.clnt_no) AS DECIMAL(18,6))
    END                                            AS approval_rate
FROM vba_pop b
LEFT JOIN responders_any ra
    ON ra.clnt_no = b.clnt_no AND ra.tactic_id = b.tactic_id
LEFT JOIN responders_approved rp
    ON rp.clnt_no = b.clnt_no AND rp.tactic_id = b.tactic_id
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;


-- ---------------------------------------------------------------------------
-- QUERY 2: Vintage Curves — Daily + Cumulative (0-90 days)
-- ---------------------------------------------------------------------------
-- Vintage = days from treatment start to application response date
-- "Any" = any application received; "Approved" = application approved
-- ---------------------------------------------------------------------------

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
        CASE WHEN p3c.Status IN ('A') THEN 1 ELSE 0 END   AS visa_app_approved,
        p3c.app_rcv_dt                                     AS visa_response_dt
    FROM vba_pop vba
    INNER JOIN p3c.appl_fact_dly p3c
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
        MIN(CAST(creditapplication_createddatetime AS DATE))           AS visa_response_dt,
        MAX(CASE
            WHEN creditapplication_creditapplicationstatuscode IN ('FULFILLED') THEN 1 ELSE 0
        END)                                                           AS visa_app_approved
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot
    WHERE creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory IN ('CREDIT_CARD')
    GROUP BY 1
),
scot_apps AS (
    SELECT
        vba.clnt_no,
        vba.tactic_id,
        vba.Treat_Start_DT,
        vba.Treat_End_DT,
        scot.visa_app_approved,
        scot.visa_response_dt
    FROM vba_pop vba
    INNER JOIN scot_apps_raw scot
        ON vba.clnt_no = scot.clnt_no
    WHERE scot.visa_response_dt BETWEEN vba.Treat_Start_DT AND vba.Treat_End_DT
),
all_apps AS (
    SELECT clnt_no, tactic_id, Treat_Start_DT, Treat_End_DT, visa_app_approved, visa_response_dt FROM casper_apps
    UNION ALL
    SELECT clnt_no, tactic_id, Treat_Start_DT, Treat_End_DT, visa_app_approved, visa_response_dt FROM scot_apps
),
earliest_any_by_client AS (
    -- Earliest application (any status) per client
    SELECT
        clnt_no,
        tactic_id,
        Treat_Start_DT,
        Treat_End_DT,
        MIN(visa_response_dt)                      AS first_app_dt
    FROM all_apps
    GROUP BY 1, 2, 3, 4
),
earliest_approved_by_client AS (
    -- Earliest APPROVED application per client
    SELECT
        clnt_no,
        tactic_id,
        Treat_Start_DT,
        Treat_End_DT,
        MIN(visa_response_dt)                      AS first_app_dt
    FROM all_apps
    WHERE visa_app_approved = 1
    GROUP BY 1, 2, 3, 4
),
vintages_any AS (
    -- Vintage in days for ANY application; keep 0..90
    SELECT
        clnt_no,
        tactic_id,
        Treat_Start_DT,
        Treat_End_DT,
        CAST(CASE
            WHEN first_app_dt < Treat_Start_DT THEN 0
            ELSE first_app_dt - Treat_Start_DT
        END AS INTEGER)                            AS vintage
    FROM earliest_any_by_client
    WHERE CAST(CASE
            WHEN first_app_dt < Treat_Start_DT THEN 0
            ELSE first_app_dt - Treat_Start_DT
        END AS INTEGER) BETWEEN 0 AND 90
),
vintages_approved AS (
    -- Vintage in days for APPROVED applications; keep 0..90
    SELECT
        clnt_no,
        tactic_id,
        Treat_Start_DT,
        Treat_End_DT,
        CAST(CASE
            WHEN first_app_dt < Treat_Start_DT THEN 0
            ELSE first_app_dt - Treat_Start_DT
        END AS INTEGER)                            AS vintage
    FROM earliest_approved_by_client
    WHERE CAST(CASE
            WHEN first_app_dt < Treat_Start_DT THEN 0
            ELSE first_app_dt - Treat_Start_DT
        END AS INTEGER) BETWEEN 0 AND 90
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
successes_any AS (
    SELECT
        SUBSTR(v.tactic_id, 8, 3)                 AS mne,
        v.Treat_Start_DT,
        v.Treat_End_DT,
        v.vintage,
        COUNT(DISTINCT v.clnt_no)                  AS apps_daily_any
    FROM vintages_any v
    GROUP BY 1, 2, 3, 4
),
successes_approved AS (
    SELECT
        SUBSTR(v.tactic_id, 8, 3)                 AS mne,
        v.Treat_Start_DT,
        v.Treat_End_DT,
        v.vintage,
        COUNT(DISTINCT v.clnt_no)                  AS apps_daily_approved
    FROM vintages_approved v
    GROUP BY 1, 2, 3, 4
),
scaffold AS (
    -- Zero-fill vintages 0 to min(90, treatment_end+5)
    SELECT
        c.mne,
        c.Treat_Start_DT,
        c.Treat_End_DT,
        c.leads,
        CAST(cal.calendar_date - c.Treat_Start_DT AS INTEGER) AS vintage
    FROM cohort c
    JOIN SYS_CALENDAR.CALENDAR cal
        ON cal.calendar_date BETWEEN c.Treat_Start_DT
            AND CASE
                WHEN (c.Treat_Start_DT + INTERVAL '90' DAY) <= (c.Treat_End_DT + INTERVAL '5' DAY)
                THEN (c.Treat_Start_DT + INTERVAL '90' DAY)
                ELSE (c.Treat_End_DT + INTERVAL '5' DAY)
            END
)
SELECT
    s.mne,
    s.Treat_Start_DT,
    s.Treat_End_DT,
    s.vintage,
    s.leads,
    COALESCE(a.apps_daily_any, 0)                  AS apps_daily_any,
    SUM(COALESCE(a.apps_daily_any, 0)) OVER (
        PARTITION BY s.mne, s.Treat_Start_DT, s.Treat_End_DT
        ORDER BY s.vintage
        ROWS UNBOUNDED PRECEDING
    )                                              AS apps_cum_any,
    COALESCE(p.apps_daily_approved, 0)             AS apps_daily_approved,
    SUM(COALESCE(p.apps_daily_approved, 0)) OVER (
        PARTITION BY s.mne, s.Treat_Start_DT, s.Treat_End_DT
        ORDER BY s.vintage
        ROWS UNBOUNDED PRECEDING
    )                                              AS apps_cum_approved
FROM scaffold s
LEFT JOIN successes_any a
    ON a.mne = s.mne
    AND a.Treat_Start_DT = s.Treat_Start_DT
    AND a.Treat_End_DT = s.Treat_End_DT
    AND a.vintage = s.vintage
LEFT JOIN successes_approved p
    ON p.mne = s.mne
    AND p.Treat_Start_DT = s.Treat_Start_DT
    AND p.Treat_End_DT = s.Treat_End_DT
    AND p.vintage = s.vintage
ORDER BY s.mne, s.Treat_Start_DT, s.Treat_End_DT, s.vintage;


-- ---------------------------------------------------------------------------
-- NOTES
-- ---------------------------------------------------------------------------
-- 1. SYS_CALENDAR.CALENDAR is Teradata-specific. For Starburst/Trino:
--    CROSS JOIN UNNEST(SEQUENCE(...)) AS t(calendar_date)
--
-- 2. DATE arithmetic in Trino: use DATE_DIFF('day', start, end)
--
-- 3. p3c.appl_fact_dly may need catalog prefix in Starburst.
--
-- 4. "Any" = any credit card application received (approved or not)
--    "Approved" = application with visa_app_approved = 1
--    These replace VBU's "any product change" / "primary (AIB)" categories.
--
-- 5. The SCOT table (tsz_00222) uses long JSON-flattened column names.
--    These are correct as written.
--
-- 6. Duplicates between Casper and SCOT are handled by earliest_*_by_client
--    CTEs (MIN date per client).
-- ---------------------------------------------------------------------------
