-- =============================================================================
-- VBA Campaign — Success Measurement (Production)
-- =============================================================================
--
-- Purpose:
--   Unified CTE query measuring VBA campaign success (credit card application
--   approvals) by combining two data sources: Casper/EDW application fact and
--   SCOT credit application snapshot.
--
-- Original: SAS code with temp tables. Rewritten as single CTE for Starburst.
--
-- Tables:
--   1. DG6V01.TACTIC_EVNT_IP_AR_HIST — tactic event history (VBA leads)
--   2. p3c.appl_fact_dly — credit card application fact daily (Casper/EDW)
--   3. edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot — SCOT
--
-- Success definition:
--   visa_app_approved = 1 (FULFILLED status in SCOT, or Status 'A' in Casper)
--   Response window: between treatmt_strt_dt and treatmt_end_dt
--
-- MNE filter: SUBSTR(TACTIC_ID, 8, 3) IN ('VBA', 'VBU')
-- Standard exclusions: MNE not in (PER, COL, MCR, OPP), SUBSTR(TACTIC_ID, 8, 1) <> 'J'
--
-- Test/Control: derived from TREATMENT_ID
--   Control = ends in '_C' or contains 'CTRL'
--   Test = everything else
--
-- Fiscal year: FY starts November
--   Q1 = Nov/Dec/Jan, Q2 = Feb/Mar/Apr, Q3 = May/Jun/Jul, Q4 = Aug/Sep/Oct
--
-- Run in: Starburst (Trino-compatible SQL)
-- =============================================================================


-- ---------------------------------------------------------------------------
-- FULL VINTAGE WITH SUCCESS — All Metrics in One Query
-- ---------------------------------------------------------------------------

WITH vba_leads AS (
    -- CTE 1: VBA/VBU deployed population from tactic event history
    SELECT
        CLNT_NO,
        TACTIC_ID,
        SUBSTR(TACTIC_ID, 8, 3)                                   AS mne,
        TST_GRP_CD,
        RPT_GRP_CD,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        -- Test vs Control from TST_GRP_CD
        -- (Original SAS used TREATMENT_ID ending in '_C' or containing 'CTRL')
        -- Adjust this logic once we see actual TST_GRP_CD values for VBA
        TST_GRP_CD                                                 AS test_group_raw,
        -- Fiscal quarter derivation (FY starts November)
        CASE
            WHEN MONTH(TREATMT_STRT_DT) IN (11, 12) THEN
                'FY' || CAST(YEAR(TREATMT_STRT_DT) + 1 AS VARCHAR) || '-Q1'
            WHEN MONTH(TREATMT_STRT_DT) IN (1)      THEN
                'FY' || CAST(YEAR(TREATMT_STRT_DT) AS VARCHAR) || '-Q1'
            WHEN MONTH(TREATMT_STRT_DT) IN (2, 3, 4) THEN
                'FY' || CAST(YEAR(TREATMT_STRT_DT) AS VARCHAR) || '-Q2'
            WHEN MONTH(TREATMT_STRT_DT) IN (5, 6, 7) THEN
                'FY' || CAST(YEAR(TREATMT_STRT_DT) AS VARCHAR) || '-Q3'
            WHEN MONTH(TREATMT_STRT_DT) IN (8, 9, 10) THEN
                'FY' || CAST(YEAR(TREATMT_STRT_DT) AS VARCHAR) || '-Q4'
        END                                                        AS fiscal_qtr
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE
        SUBSTR(TACTIC_ID, 8, 3) IN ('VBA', 'VBU')
        AND TREATMT_STRT_DT >= DATE '2025-11-01'
        -- Standard exclusions (Daniel's framework)
        AND SUBSTR(TACTIC_ID, 8, 3) NOT IN ('PER', 'COL', 'MCR', 'OPP')
        AND SUBSTR(TACTIC_ID, 8, 1) <> 'J'
),

casper_response AS (
    -- CTE 2: Credit card applications from Casper/EDW (p3c.appl_fact_dly)
    -- Success = Status 'A' (approved)
    -- Filters: new accounts only (not limit changes), exclude CCL/BXX, exclude PATACT/GV0320
    SELECT
        vba.CLNT_NO,
        vba.TACTIC_ID,
        CASE WHEN p3c.Status IN ('A') THEN p3c.acct_no ELSE NULL END  AS visa_acct_no,
        CASE WHEN p3c.Status IN ('A') THEN 1 ELSE 0 END               AS visa_app_approved,
        p3c.app_rcv_dt                                                 AS visa_response_dt
    FROM vba_leads vba
    INNER JOIN p3c.appl_fact_dly p3c
        ON vba.CLNT_NO = p3c.bus_clnt_no
    WHERE
        p3c.app_rcv_dt BETWEEN vba.TREATMT_STRT_DT AND vba.TREATMT_END_DT
        AND (p3c.Status IN ('D', 'O') OR p3c.Status IN ('A'))
        AND p3c.PROD_APPRVD IN ('B', 'E')
        AND (p3c.Cell_Code IS NULL OR p3c.Cell_Code <> 'PATACT')
        -- New accounts only, not credit limit changes
        AND p3c.CR_LMT_CHG_IND = 'N'
        -- CCL is a credit line not a card, BXX not VBA
        AND p3c.visa_prod_cd NOT IN ('CCL', 'BXX')
        -- Exclude ASC GV0320
        AND (p3c.Cell_Code IS NULL OR p3c.Cell_Code <> 'GV0320')
),

scot_response AS (
    -- CTE 3: Credit card applications from SCOT (credit application snapshot)
    -- Success = creditapplicationstatuscode = 'FULFILLED'
    SELECT
        CAST(creditapplication_borrowers_borrowersrfnumber AS INTEGER)  AS clnt_no,
        MAX(CASE
            WHEN creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid IS NOT NULL
            THEN CAST(creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid AS INTEGER)
            ELSE NULL
        END)                                                           AS visa_acct_no,
        MIN(CAST(creditapplication_createddatetime AS DATE))           AS visa_response_dt,
        MAX(CASE
            WHEN creditapplication_creditapplicationstatuscode IN ('FULFILLED')
            THEN 1 ELSE 0
        END)                                                           AS visa_app_approved
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot
    WHERE
        creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory IN ('CREDIT_CARD')
    GROUP BY 1
),

scot_joined AS (
    -- CTE 4: Join SCOT responses to VBA tactic population
    -- Window: response date between treatment start and end
    SELECT
        vba.CLNT_NO,
        vba.TACTIC_ID,
        scot.visa_acct_no,
        scot.visa_app_approved,
        scot.visa_response_dt
    FROM vba_leads vba
    INNER JOIN scot_response scot
        ON vba.CLNT_NO = scot.clnt_no
    WHERE
        scot.visa_response_dt BETWEEN vba.TREATMT_STRT_DT AND vba.TREATMT_END_DT
),

all_responses AS (
    -- CTE 5: Combine both sources (replaces SAS data...set stack)
    SELECT CLNT_NO, TACTIC_ID, visa_acct_no, visa_app_approved, visa_response_dt
    FROM casper_response
    UNION ALL
    SELECT CLNT_NO, TACTIC_ID, visa_acct_no, visa_app_approved, visa_response_dt
    FROM scot_joined
),

client_success AS (
    -- CTE 6: Deduplicate — one row per client with earliest response
    -- Success = any approved application
    SELECT
        CLNT_NO,
        TACTIC_ID,
        MAX(visa_app_approved)                                     AS visa_app_approved,
        MIN(CASE WHEN visa_app_approved = 1 THEN visa_response_dt END) AS earliest_response_dt
    FROM all_responses
    GROUP BY CLNT_NO, TACTIC_ID
)

-- Final output: vintage by MNE, fiscal quarter, test group
SELECT
    vba.mne,
    vba.fiscal_qtr,
    vba.test_group_raw                                             AS test_group,
    COUNT(DISTINCT vba.CLNT_NO)                                    AS leads,
    COUNT(DISTINCT CASE WHEN s.visa_app_approved = 1
                        THEN s.CLNT_NO END)                        AS responders,
    ROUND(
        CAST(COUNT(DISTINCT CASE WHEN s.visa_app_approved = 1
                                 THEN s.CLNT_NO END) AS DOUBLE)
        / NULLIF(COUNT(DISTINCT vba.CLNT_NO), 0) * 100,
        2
    )                                                              AS response_rate_pct
FROM vba_leads vba
LEFT JOIN client_success s
    ON vba.CLNT_NO = s.CLNT_NO
    AND vba.TACTIC_ID = s.TACTIC_ID
GROUP BY
    vba.mne,
    vba.fiscal_qtr,
    vba.test_group_raw
ORDER BY
    vba.mne,
    vba.fiscal_qtr,
    test_group;


-- ---------------------------------------------------------------------------
-- NOTES
-- ---------------------------------------------------------------------------
-- 1. The p3c.appl_fact_dly table path may need catalog prefix in Starburst.
--    If it doesn't resolve, check what catalog Casper/P3C tables live under.
--
-- 2. The SCOT table (tsz_00222) uses extremely long column names from the
--    JSON-flattened credit application schema. These are correct as-is.
--
-- 3. Test/Control: the original SAS code derived T/C from TREATMENT_ID
--    (ending in '_C' or containing 'CTRL'). This rewrite uses TST_GRP_CD
--    from the tactic event table. Verify the mapping matches after first run.
--
-- 4. The UNION ALL in all_responses may produce duplicates if the same
--    client appears in both Casper and SCOT with different approval status.
--    client_success CTE handles this by taking MAX(visa_app_approved).
--
-- 5. To add monthly trend: replace fiscal_qtr with
--    CAST(YEAR(TREATMT_STRT_DT) AS VARCHAR) || '-' ||
--    LPAD(CAST(MONTH(TREATMT_STRT_DT) AS VARCHAR), 2, '0')
--    in the GROUP BY and SELECT.
-- ---------------------------------------------------------------------------
