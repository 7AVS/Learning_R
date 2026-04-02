-- =============================================================================
-- VBA Campaign Success — Original SAS Code (Transcribed)
-- =============================================================================
--
-- Source: VBA Campaign Success SAS code, shared via screenshots 2026-04-02
-- Transcribed: 2026-04-02
-- Platform: SAS with embedded Teradata + Trino SQL (pass-through)
--
-- This is the REFERENCE copy of the original SAS pipeline.
-- The Starburst CTE rewrite is in vba_success_measurement.sql.
--
-- Pipeline:
--   1. vba_leads_tactic = tactic event table filtered for VBA
--   2. vba_response_applfact = Casper (p3c.appl_fact_dly) card applications
--   3. VBA_SCOT_Response1 = SCOT credit application snapshot
--   4. Visa_SCOT_Response = SCOT joined to tactic population
--   5. VBA_Response = UNION (SAS data...set) of both sources
--   6. Success: visa_app_approved = 1, response date = earliest visa_Response_Dt
--
-- =============================================================================


-- ---------------------------------------------------------------------------
-- STEP 1: Cards app from Casper (Teradata pass-through)
-- ---------------------------------------------------------------------------
-- SAS: create table vba_response_applfact as
--      select * from connection to teradata (...)

-- VBA_LEADS_TACTIC is assumed to be a pre-built table from tactic event
-- history, filtered for VBA campaigns. Contains:
--   tactic_id, clnt_no, treatmt_strt_dt, treatmt_end_dt

SELECT
    vba.tactic_id,
    vba.clnt_no,
    CASE WHEN p3c.appl_fact_dly.Status IN ('A')
         THEN p3c.appl_fact_dly.acct_no ELSE NULL END     AS visa_acct_no,
    CASE WHEN p3c.appl_fact_dly.Status IN ('A')
         THEN 1 ELSE 0 END                                AS visa_App_Approved,
    p3c.appl_fact_dly.app_rcv_dt                           AS visa_Response_Dt
FROM vba_leads_tactic AS vba
INNER JOIN p3c.appl_fact_dly AS appl_fact_dly
    ON vba.clnt_no = appl_fact_dly.bus_clnt_no
WHERE
    p3c.appl_fact_dly.app_rcv_dt BETWEEN vba.treatmt_strt_dt AND vba.treatmt_end_dt
    AND (p3c.APPL_FACT_DLY.status IN ('D', 'O') OR p3c.APPL_FACT_DLY.status IN ('A'))
    AND p3c.appl_fact_dly.PROD_APPRVD IN ('B', 'E')
    AND (p3c.appl_fact_dly.Cell_Code IS NULL OR p3c.appl_fact_dly.Cell_Code <> 'PATACT')
    -- Actual NEW ACCT app — not just a credit limit change
    AND p3c.appl_fact_dly.CR_LMT_CHG_IND = 'N'
    -- CCL is a credit line not a card; BXX not belong to VBA
    AND p3c.appl_fact_dly.visa_prod_cd NOT IN ('CCL', 'BXX')
    -- April 16 2020 — Excluding ASC GV0320
    AND Cell_Code NOT IN ('GV0320');


-- ---------------------------------------------------------------------------
-- STEP 2: Cards app from SCOT (Trino pass-through)
-- ---------------------------------------------------------------------------
-- SAS: create table VBA_SCOT_Response1 as
--      SELECT * from connection to trino (...)

SELECT *
FROM (
    SELECT
        CAST(creditapplication_borrowers_borrowersrfnumber AS INT)    AS clnt_no,
        MAX(CASE
            WHEN creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid IS NULL THEN NULL
            ELSE CAST(creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid AS INT)
        END)                                                         AS visa_acct_no,
        MIN(CAST(creditapplication_createddatetime AS DATE))         AS visa_Response_Dt,
        MAX(CASE
            WHEN creditapplication_creditapplicationstatuscode IN ('FULFILLED') THEN 1 ELSE 0
        END)                                                         AS visa_App_Approved
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot
    WHERE creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory IN ('CREDIT_CARD')
    GROUP BY 1, 2
) scot;


-- ---------------------------------------------------------------------------
-- STEP 3: Join SCOT to VBA tactic population
-- ---------------------------------------------------------------------------
-- SAS: create table Visa_SCOT_Response as select ...

SELECT
    visa.tactic_id,
    visa.clnt_no,
    scot.visa_acct_no,
    scot.visa_App_Approved,
    scot.visa_Response_Dt
FROM vba_clnt_tactic AS vba   /* VBA leads from tactic history */
INNER JOIN VBA_SCOT_Response1 AS scot
    ON vba.clnt_no = scot.clnt_no
    AND scot.visa_Response_Dt BETWEEN vba.treatmt_strt_dt AND vba.treatmt_end_dt;


-- ---------------------------------------------------------------------------
-- STEP 4: Combine both sources (SAS data...set = UNION)
-- ---------------------------------------------------------------------------
-- SAS:
--   data VBA_Response;
--   set vba_response_applfact
--       Visa_SCOT_Response;
--   run;

-- Equivalent SQL:
-- SELECT * FROM vba_response_applfact
-- UNION ALL
-- SELECT * FROM Visa_SCOT_Response;


-- ---------------------------------------------------------------------------
-- SUCCESS DEFINITION
-- ---------------------------------------------------------------------------
-- Success: visa_app_approved = 1
-- Response date: earliest visa_Response_Dt
-- ---------------------------------------------------------------------------
