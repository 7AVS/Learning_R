-- =============================================================================
-- VBA Campaign — Vintage Curves (0-90 days)
-- =============================================================================
--
-- Purpose:
--   Daily vintage curves showing credit card application rates over a 0-90 day
--   window post-deployment, tracked SEPARATELY by data source so you can
--   compare signal quality between Casper and SCOT.
--
-- Success definition (from VBA SAS code):
--   Primary (Casper — d3cv12a.appl_fact_dly):
--     Status IN ('D','O','A'), PROD_APPRVD IN ('B','E'), CR_LMT_CHG_IND = 'N'
--     Exclude visa_prod_cd IN ('CCL','BXX'), exclude Cell_Code IN ('PATACT','GV0320')
--     Approved = Status = 'A'
--   Secondary (SCOT — tsz_00222 joined to tactic population):
--     productcategory = 'CREDIT_CARD', statuscode = 'FULFILLED'
--
-- Two tiers — same metric, different sources:
--   PRIMARY   = Casper (d3cv12a.appl_fact_dly) — EDW application fact table
--   SECONDARY = SCOT (tsz_00222) — Visa SCOT response, joined to population
--   These are kept separate throughout. No UNION between them.
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
responders_primary AS (
    -- PRIMARY (Casper): approved applications only
    SELECT DISTINCT clnt_no, tactic_id FROM casper_apps WHERE visa_app_approved = 1
),
responders_secondary AS (
    -- SECONDARY (SCOT): approved applications only
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
earliest_primary_by_client AS (
    -- PRIMARY (Casper): earliest approved response per client
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
    -- SECONDARY (SCOT): earliest approved response per client
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
    -- Vintage in days for PRIMARY (Casper) approved; keep 0..90
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
    -- Vintage in days for SECONDARY (SCOT) approved; keep 0..90
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
    -- Daily approved count from PRIMARY (Casper) per vintage day
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
    -- Daily approved count from SECONDARY (SCOT) per vintage day
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
ORDER BY s.mne, s.Treat_Start_DT, s.Treat_End_DT, s.vintage;


-- ---------------------------------------------------------------------------
-- NOTES
-- ---------------------------------------------------------------------------
-- 1. Uses Trino DATE_DIFF('day', start, end) for vintage day calculation.
--
-- 2. d3cv12a.appl_fact_dly may need catalog prefix in Starburst.
--
-- 3. PRIMARY = Casper (d3cv12a.appl_fact_dly) — EDW application fact table.
--    SECONDARY = SCOT (tsz_00222) — Visa SCOT response joined to population.
--    Both are separate throughout — no UNION between them. The vintage curves
--    let you compare which source detects applications earlier or more completely.
--
-- 4. The SCOT table (tsz_00222) uses long JSON-flattened column names.
--    These are correct as written.
--
-- 5. Both vintage tiers filter visa_app_approved = 1 (approved only).
--    Overlap between sources is expected — a client can appear in both.
-- ---------------------------------------------------------------------------
