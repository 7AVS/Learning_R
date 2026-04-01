-- =============================================================================
-- AUH (Authorized Users) — Campaign Performance Tracking
-- =============================================================================
--
-- Purpose:
--   Daily/weekly tracking of AUH Phase 1 response rates and email metrics.
--   These are the production queries Daniel's team uses for reporting.
--
-- Campaign:
--   Authorized User Acquisition — Phase 1 (Non-Rewards)
--   Tactic ID:  2026042AUH
--   MNE:        AUH
--   Treatment:  2026-02-12 to 2026-03-12
--
-- Source:
--   Transcribed from AUH ResponseRates_Tracking.xlsx (Daniel Chin)
--   SharePoint > NBA M&A > Pods > Pod of Gold > Cards > Cards Authorized User
--
-- Tables:
--   1. DG6V01.TACTIC_EVNT_IP_AR_HIST     (tactic population)
--   2. D3CV12A.ACCT_CRD_OWN_DLY_DELTA    (success — auth user card opening)
--   3. DTZV01.VENDOR_FEEDBACK_MASTER      (email delivery)
--   4. DTZV01.VENDOR_FEEDBACK_EVENT       (email engagement)
--
-- Test Groups:
--   NRGA   = Non-Reward Web Visits (treatment)
--   NRGA_C = Non-Reward Web Visits (control)
--   NRR    = Non-Reward Random (treatment)
--   NRR_C  = Non-Reward Random (control)
--   NRS    = Non-Reward Model (treatment)
--   NRS_C  = Non-Reward Model (control)
--
-- =============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 1: Population by Test Group
-- ---------------------------------------------------------------------------
-- Purpose: Count leads per test group for the denominator.
-- Output:  TST_GRP_CD, lead count
-- ---------------------------------------------------------------------------

SELECT
    TST_GRP_CD,
    COUNT(*)                             AS lead_cnts
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE
    TACTIC_ID = '2026042AUH'
GROUP BY
    TST_GRP_CD
ORDER BY TST_GRP_CD;


-- ---------------------------------------------------------------------------
-- QUERY 2: Response Rates — All Products (Generic)
-- ---------------------------------------------------------------------------
-- Purpose: Count leads and responses per test group (any auth user add).
-- Success: LEFT JOIN to card ownership delta table
--          - CHG_DT = '9999/12/31'  (current record, not historical)
--          - RELATIONSHIP_CD = 'Z'  (authorized user relationship)
--          - card_sts IN ('A', '')  (active card)
--          - CAPTR_DT > DATE '2026-02-12' (after treatment start)
-- Key:    TACTIC_EVNT_ID = account number (cast to decimal for join)
--         prod_cd = SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 3) = card product
-- ---------------------------------------------------------------------------

SELECT
    a.TST_GRP_CD,
    COUNT(*)                             AS lead_cnts,
    SUM(CASE WHEN b.acct_no IS NOT NULL
             THEN 1 ELSE 0 END)         AS resp_cnts
FROM (
    SELECT
        TST_GRP_CD,
        SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 3)  AS prod_cd,
        CAST(TACTIC_EVNT_ID AS DECIMAL(20,0))   AS acct_no
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE
        TACTIC_ID = '2026042AUH'
) a
LEFT JOIN D3CV12A.ACCT_CRD_OWN_DLY_DELTA b
    ON  a.acct_no          = b.acct_no
    AND b.CHG_DT           = '9999/12/31'
    AND b.RELATIONSHIP_CD  = 'Z'
    AND b.card_sts        IN ('A', '')
    AND b.CAPTR_DT         > DATE '2026-02-12'
GROUP BY
    a.TST_GRP_CD
ORDER BY a.TST_GRP_CD;


-- ---------------------------------------------------------------------------
-- QUERY 3: Response Rates — Target Products Only
-- ---------------------------------------------------------------------------
-- Purpose: Same as Query 2 but filtered to target card products.
-- Products: PLT, CLO, MC1, MCP, VPR (the 5 non-rewards cards in scope)
-- Note:   This is the stricter success definition — only counts auth user
--         adds on the specific card products targeted by the campaign.
-- ---------------------------------------------------------------------------

SELECT
    a.TST_GRP_CD,
    COUNT(*)                             AS lead_cnts,
    SUM(CASE WHEN b.acct_no IS NOT NULL
             THEN 1 ELSE 0 END)         AS resp_cnts
FROM (
    SELECT
        TST_GRP_CD,
        SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 3)  AS prod_cd,
        CAST(TACTIC_EVNT_ID AS DECIMAL(20,0))   AS acct_no
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE
        TACTIC_ID = '2026042AUH'
        AND SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 3) IN ('PLT', 'CLO', 'MC1', 'MCP', 'VPR')
) a
LEFT JOIN D3CV12A.ACCT_CRD_OWN_DLY_DELTA b
    ON  a.acct_no          = b.acct_no
    AND b.CHG_DT           = '9999/12/31'
    AND b.RELATIONSHIP_CD  = 'Z'
    AND b.card_sts        IN ('A', '')
    AND b.CAPTR_DT         > DATE '2026-02-12'
GROUP BY
    a.TST_GRP_CD
ORDER BY a.TST_GRP_CD;


-- ---------------------------------------------------------------------------
-- QUERY 4: Daily Response Vintage (for trend chart)
-- ---------------------------------------------------------------------------
-- Purpose: Daily cumulative response rates by test group.
--          This produces the Control vs Test line chart.
-- Output:  TST_GRP_CD, CAPTR_DT, lead_cnts, resp_cnts
-- ---------------------------------------------------------------------------

SELECT
    a.TST_GRP_CD,
    b.CAPTR_DT,
    COUNT(*)                             AS lead_cnts,
    SUM(CASE WHEN b.acct_no IS NOT NULL
             THEN 1 ELSE 0 END)         AS resp_cnts
FROM (
    SELECT
        TST_GRP_CD,
        SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 3)  AS prod_cd,
        CAST(TACTIC_EVNT_ID AS DECIMAL(20,0))   AS acct_no
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE
        TACTIC_ID = '2026042AUH'
        AND SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 3) IN ('PLT', 'CLO', 'MC1', 'MCP', 'VPR')
) a
LEFT JOIN D3CV12A.ACCT_CRD_OWN_DLY_DELTA b
    ON  a.acct_no          = b.acct_no
    AND b.CHG_DT           = '9999/12/31'
    AND b.RELATIONSHIP_CD  = 'Z'
    AND b.card_sts        IN ('A', '')
    AND b.CAPTR_DT         > DATE '2026-02-12'
GROUP BY
    a.TST_GRP_CD,
    b.CAPTR_DT
ORDER BY a.TST_GRP_CD, b.CAPTR_DT;


-- ---------------------------------------------------------------------------
-- QUERY 5: Email Metrics
-- ---------------------------------------------------------------------------
-- Purpose: Email delivery and engagement by test group.
-- Disposition codes:
--   1 = sent
--   2 = opened
--   3 = clicked
--   4 = unsubscribed
--   5 = hard bounce
--   6 = complaint
-- Join path:
--   FEEDBACK_MASTER.TREATMENT_ID = TACTIC.TACTIC_ID
--   FEEDBACK_MASTER.CLNT_NO      = TACTIC.CLNT_NO
--   FEEDBACK_EVENT joins on consumer_id_hashed + TREATMENT_ID
-- ---------------------------------------------------------------------------

SELECT
    t.TACTIC_ID,
    t.TREATMT_STRT_DT,
    t.TREATMT_END_DT,
    t.TST_GRP_CD,
    SUM(CASE WHEN e.disposition_cd = 1
             THEN 1 ELSE 0 END)         AS email_sent,
    SUM(CASE WHEN e.disposition_cd = 2
             THEN 1 ELSE 0 END)         AS email_opened,
    SUM(CASE WHEN e.disposition_cd = 3
             THEN 1 ELSE 0 END)         AS email_clicked,
    SUM(CASE WHEN e.disposition_cd = 4
             THEN 1 ELSE 0 END)         AS email_unsubscribed,
    SUM(CASE WHEN e.disposition_cd = 5
             THEN 1 ELSE 0 END)         AS email_hardbounce,
    SUM(CASE WHEN e.disposition_cd = 6
             THEN 1 ELSE 0 END)         AS email_complaint
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  m.TREATMENT_ID     = t.TACTIC_ID
    AND m.CLNT_NO          = t.CLNT_NO
INNER JOIN DTZV01.VENDOR_FEEDBACK_EVENT e
    ON  e.consumer_id_hashed = m.consumer_id_hashed
    AND e.TREATMENT_ID       = m.TREATMENT_ID
WHERE
    SUBSTR(t.TACTIC_ID, 8, 3) = 'AUH'
GROUP BY
    t.TACTIC_ID,
    t.TREATMT_STRT_DT,
    t.TREATMT_END_DT,
    t.TST_GRP_CD
ORDER BY t.TST_GRP_CD;
