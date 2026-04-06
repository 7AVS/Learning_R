-- =============================================================================
-- VBA Campaign — Vintage Curves (0-90 days)
-- =============================================================================
--
-- Purpose:
--   Daily vintage curves showing credit card application rates over a 0-90 day
--   window post-deployment. Casper and SCOT are UNIONED and deduplicated —
--   each client counts once, attributed to whichever source approved first.
--
-- Success definition (from VBA SAS code):
--   Primary (Casper — d3cv12a.appl_fact_dly):
--     Status IN ('D','O','A'), PROD_APPRVD IN ('B','E'), CR_LMT_CHG_IND = 'N'
--     Exclude visa_prod_cd IN ('CCL','BXX'), exclude Cell_Code IN ('PATACT','GV0320')
--     Approved = Status = 'A'
--   Secondary (SCOT — tsz_00222 joined to tactic population):
--     productcategory = 'CREDIT_CARD', statuscode = 'FULFILLED'
--
-- Sources:
--   Casper (d3cv12a.appl_fact_dly) — EDW application fact table
--   SCOT (tsz_00222) — Visa SCOT response, joined to population
--   Both UNIONED, deduped to earliest approved per client. Same logic in
--   summary and vintage curves so volumes match.
--
-- Tables:
--   DG6V01.tactic_evnt_ip_ar_hist — tactic population
--   d3cv12a.appl_fact_dly — Casper: credit card application fact (Teradata)
--   edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot — SCOT
--
-- Vintage = days from treatmt_strt_dt to earliest visa_Response_Dt (0-90)
--
-- Run in: Starburst/Trino
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
    -- PRIMARY source: credit card applications from Casper (d3cv12a.appl_fact_dly)
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
    -- SECONDARY source: credit card applications from SCOT (one row per client)
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
    -- SECONDARY source joined to VBA population (within treatment window)
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
all_responses AS (
    -- Union Casper + SCOT into one set
    SELECT clnt_no, tactic_id, visa_app_approved, visa_response_dt, 'Casper' AS response_source
    FROM casper_apps
    UNION ALL
    SELECT clnt_no, tactic_id, visa_app_approved, visa_response_dt, 'Scott' AS response_source
    FROM scot_apps
),
success AS (
    -- Dedup: one row per client — earliest approved response wins
    SELECT *
    FROM (
        SELECT
            clnt_no,
            tactic_id,
            visa_response_dt,
            response_source,
            ROW_NUMBER() OVER (PARTITION BY tactic_id, clnt_no ORDER BY visa_response_dt ASC) AS rn
        FROM all_responses
        WHERE visa_app_approved = 1
    )
    WHERE rn = 1
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
    COUNT(DISTINCT s.clnt_no)                      AS successes_any,
    COUNT(DISTINCT CASE WHEN s.response_source = 'Casper' THEN s.clnt_no END) AS successes_casper,
    COUNT(DISTINCT CASE WHEN s.response_source = 'Scott'  THEN s.clnt_no END) AS successes_scott,
    CASE WHEN COUNT(DISTINCT b.clnt_no) = 0 THEN 0
        ELSE CAST(COUNT(DISTINCT s.clnt_no) AS DECIMAL(18,6))
             / CAST(COUNT(DISTINCT b.clnt_no) AS DECIMAL(18,6))
    END                                            AS response_rate
FROM vba_pop b
LEFT JOIN success s
    ON s.clnt_no = b.clnt_no AND s.tactic_id = b.tactic_id
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;

-- @@SPLIT@@

-- ---------------------------------------------------------------------------
-- QUERY 2: Vintage Curves — Daily + Cumulative (0-90 days)
-- ---------------------------------------------------------------------------
-- Vintage = days from treatment start to application response date
-- PRIMARY  = Casper (d3cv12a.appl_fact_dly) — EDW source
-- SECONDARY = SCOT (tsz_00222) — Visa SCOT response source
-- Both track approved applications only (visa_app_approved = 1)
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
    -- PRIMARY source: Casper application fact
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
    -- SECONDARY source: SCOT credit application snapshot (one row per client)
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
    -- SECONDARY source joined to VBA population (within treatment window)
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
all_responses AS (
    -- Union Casper + SCOT into one set with response_source label
    SELECT clnt_no, tactic_id, Treat_Start_DT, Treat_End_DT,
           visa_app_approved, visa_response_dt, 'Casper' AS response_source
    FROM casper_apps
    UNION ALL
    SELECT clnt_no, tactic_id, Treat_Start_DT, Treat_End_DT,
           visa_app_approved, visa_response_dt, 'Scott' AS response_source
    FROM scot_apps
),
success AS (
    -- Dedup: one row per client — earliest approved response wins
    SELECT *
    FROM (
        SELECT
            clnt_no,
            tactic_id,
            Treat_Start_DT,
            Treat_End_DT,
            visa_response_dt,
            response_source,
            ROW_NUMBER() OVER (PARTITION BY tactic_id, clnt_no ORDER BY visa_response_dt ASC) AS rn
        FROM all_responses
        WHERE visa_app_approved = 1
    )
    WHERE rn = 1
),
vintages AS (
    -- Vintage in days; clamp negatives to 0; keep 0..90
    SELECT
        clnt_no,
        tactic_id,
        Treat_Start_DT,
        Treat_End_DT,
        CASE
            WHEN visa_response_dt < Treat_Start_DT THEN 0
            ELSE DATE_DIFF('day', Treat_Start_DT, visa_response_dt)
        END                                        AS vintage
    FROM success
    WHERE CASE
            WHEN visa_response_dt < Treat_Start_DT THEN 0
            ELSE DATE_DIFF('day', Treat_Start_DT, visa_response_dt)
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
successes_daily AS (
    -- Daily approved count per vintage day (deduped across sources)
    SELECT
        SUBSTR(v.tactic_id, 8, 3)                 AS mne,
        v.Treat_Start_DT,
        v.Treat_End_DT,
        v.vintage,
        COUNT(DISTINCT v.clnt_no)                  AS success_daily
    FROM vintages v
    GROUP BY 1, 2, 3, 4
),
scaffold AS (
    -- Zero-fill vintages 0 to 90
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
    COALESCE(d.success_daily, 0)                   AS success_daily,
    SUM(COALESCE(d.success_daily, 0)) OVER (
        PARTITION BY s.mne, s.Treat_Start_DT, s.Treat_End_DT
        ORDER BY s.vintage
        ROWS UNBOUNDED PRECEDING
    )                                              AS success_cum
FROM scaffold s
LEFT JOIN successes_daily d
    ON d.mne = s.mne
    AND d.Treat_Start_DT = s.Treat_Start_DT
    AND d.Treat_End_DT = s.Treat_End_DT
    AND d.vintage = s.vintage
ORDER BY s.mne, s.Treat_Start_DT, s.Treat_End_DT, s.vintage;


-- ---------------------------------------------------------------------------
-- NOTES
-- ---------------------------------------------------------------------------
-- 1. Uses Trino DATE_DIFF('day', start, end) for vintage day calculation.
--
-- 2. d3cv12a.appl_fact_dly may need catalog prefix in Starburst.
--
-- 3. Casper and SCOT are UNIONED and deduped — each client counts once,
--    attributed to whichever source approved earliest. Summary and vintage
--    curves use identical dedup logic so day-90 cumulative = summary total.
--
-- 4. The SCOT table (tsz_00222) uses long JSON-flattened column names.
--    These are correct as written.
-- ---------------------------------------------------------------------------
