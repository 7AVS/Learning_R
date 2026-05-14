-- AUH Interim Measurement — Phase 1 + Phase 2 (all waves)
-- All AUH tactic_ids captured via SUBSTR. Raw code columns preserved.
--
-- TEMP COLUMN — NEEDS EXPLICIT VALIDATION:
--   ac_temp = 'Control' when TST_GRP_CD ends in '_C', else 'Action'.
--   Daniel's Phase 1 convention; assumed to hold for Phase 2 but Robin's
--   email did not explicitly confirm '_C' = Control. Treat as working
--   label only.
--
-- Channel derived via SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 8) per Andre.
-- Acquired product and card BIN (first 6 of CARD_NO) pulled from success
-- table. Join is on acct_no only (NOT a.prod_cd = b.prod_cd) so we can
-- see when offered_prod != acquired_prod.


-- M1: Cohort-grain summary with two-tier success counts
-- leads               = distinct cohort accounts in the group
-- clients_with_au_add = distinct cohort accounts with >=1 AU add post-treatment
-- au_adds_total       = count of AU add events (one account can contribute >1)
SELECT
    a.TACTIC_ID,
    a.TREATMT_STRT_DT,
    a.TREATMT_END_DT,
    a.TREATMT_MN,
    a.TST_GRP_CD,
    a.ac_temp,
    a.RPT_GRP_CD,
    a.channel,
    a.offered_prod,
    COUNT(DISTINCT a.acct_no)                                              AS leads,
    COUNT(DISTINCT CASE WHEN b.acct_no IS NOT NULL THEN a.acct_no END)     AS clients_with_au_add,
    SUM(CASE WHEN b.acct_no IS NOT NULL THEN 1 ELSE 0 END)                 AS au_adds_total
FROM (
    SELECT
        TACTIC_ID,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TREATMT_MN,
        TST_GRP_CD,
        RPT_GRP_CD,
        SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 8)              AS channel,
        SUBSTR(TACTIC_DECISN_VRB_INFO,  21,  3)             AS offered_prod,
        CAST(TACTIC_EVNT_ID AS DECIMAL(20,0))               AS acct_no,
        CASE WHEN TRIM(TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END AS ac_temp
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
) a
LEFT JOIN D3CV12A.ACCT_CRD_OWN_DLY_DELTA b
       ON  a.acct_no         = b.acct_no
       AND b.CHG_DT          = DATE '9999-12-31'
       AND b.RELATIONSHIP_CD = '2'
       AND b.card_sts IN ('A', '')
       AND b.CAPTR_DT        > a.TREATMT_STRT_DT
GROUP BY
    a.TACTIC_ID, a.TREATMT_STRT_DT, a.TREATMT_END_DT,
    a.TREATMT_MN, a.TST_GRP_CD, a.ac_temp, a.RPT_GRP_CD,
    a.channel, a.offered_prod
ORDER BY a.TACTIC_ID, a.ac_temp, a.TST_GRP_CD, a.RPT_GRP_CD;


-- M2: Acquired-product detail (success events only)
-- For each cohort group, breaks down successes by acquired product
-- and card BIN (first 6 of CARD_NO). Use to compare offered vs acquired
-- product and to map BIN -> product name in Excel.
SELECT
    a.TACTIC_ID,
    a.TREATMT_STRT_DT,
    a.TREATMT_MN,
    a.TST_GRP_CD,
    a.ac_temp,
    a.RPT_GRP_CD,
    a.channel,
    a.offered_prod,
    b.prod_cd                            AS acquired_prod,
    SUBSTR(b.card_no, 1, 6)              AS card_bin,
    COUNT(*)                             AS au_adds_total
FROM (
    SELECT
        TACTIC_ID,
        TREATMT_STRT_DT,
        TREATMT_MN,
        TST_GRP_CD,
        RPT_GRP_CD,
        SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 8)              AS channel,
        SUBSTR(TACTIC_DECISN_VRB_INFO,  21,  3)             AS offered_prod,
        CAST(TACTIC_EVNT_ID AS DECIMAL(20,0))               AS acct_no,
        CASE WHEN TRIM(TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END AS ac_temp
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
) a
INNER JOIN D3CV12A.ACCT_CRD_OWN_DLY_DELTA b
       ON  a.acct_no         = b.acct_no
       AND b.CHG_DT          = DATE '9999-12-31'
       AND b.RELATIONSHIP_CD = '2'
       AND b.card_sts IN ('A', '')
       AND b.CAPTR_DT        > a.TREATMT_STRT_DT
GROUP BY
    a.TACTIC_ID, a.TREATMT_STRT_DT,
    a.TREATMT_MN, a.TST_GRP_CD, a.ac_temp, a.RPT_GRP_CD,
    a.channel, a.offered_prod,
    b.prod_cd, SUBSTR(b.card_no, 1, 6)
ORDER BY a.TACTIC_ID, a.ac_temp, a.TST_GRP_CD, au_adds_total DESC;
