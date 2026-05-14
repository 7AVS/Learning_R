-- AUH Interim Measurement — Phase 1 + Phase 2 (all waves)
-- All AUH tactic_ids captured via SUBSTR. All raw code columns preserved.
-- No derived Action/Control or arm-type labels — no confirmed cell-code lookup exists for AUH.
-- Labeling happens after empirical profiling, not in this SQL.


-- Q1: Code interaction profile (no success join) — distinct codes per wave
SELECT
    TACTIC_ID,
    TREATMT_STRT_DT,
    TREATMT_END_DT,
    TREATMT_MN,
    TST_GRP_CD,
    RPT_GRP_CD,
    TACTIC_CELL_CD,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 3)   AS prod_cd_extracted,
    COUNT(*)                                AS leads
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
GROUP BY
    TACTIC_ID, TREATMT_STRT_DT, TREATMT_END_DT,
    TREATMT_MN, TST_GRP_CD, RPT_GRP_CD, TACTIC_CELL_CD,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 3)
ORDER BY TACTIC_ID, TREATMT_MN, TST_GRP_CD, RPT_GRP_CD;


-- Q2: Per-arm rollup with success — leads and AU adds by raw TREATMT_MN
SELECT
    a.TACTIC_ID,
    a.TREATMT_STRT_DT,
    a.TREATMT_MN,
    COUNT(*)                                                        AS leads,
    SUM(CASE WHEN b.acct_no IS NOT NULL THEN 1 ELSE 0 END)          AS au_adds
FROM (
    SELECT
        TACTIC_ID,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TREATMT_MN,
        SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 3)        AS prod_cd,
        CAST(TACTIC_EVNT_ID AS DECIMAL(20,0))        AS acct_no
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
) a
LEFT JOIN D3CV12A.ACCT_CRD_OWN_DLY_DELTA b
       ON  a.acct_no         = b.acct_no
       AND a.prod_cd         = b.prod_cd
       AND b.CHG_DT          = DATE '9999-12-31'
       AND b.RELATIONSHIP_CD = '2'
       AND b.card_sts IN ('A', '')
       AND b.CAPTR_DT        > a.TREATMT_STRT_DT
GROUP BY
    a.TACTIC_ID, a.TREATMT_STRT_DT, a.TREATMT_MN
ORDER BY a.TACTIC_ID, a.TREATMT_MN;


-- Q3: Full-grain rollup with success — leads and AU adds by all raw codes
SELECT
    a.TACTIC_ID,
    a.TREATMT_STRT_DT,
    a.TREATMT_MN,
    a.TST_GRP_CD,
    a.RPT_GRP_CD,
    a.TACTIC_CELL_CD,
    a.prod_cd,
    COUNT(*)                                                        AS leads,
    SUM(CASE WHEN b.acct_no IS NOT NULL THEN 1 ELSE 0 END)          AS au_adds
FROM (
    SELECT
        TACTIC_ID,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TREATMT_MN,
        TST_GRP_CD,
        RPT_GRP_CD,
        TACTIC_CELL_CD,
        SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 3)        AS prod_cd,
        CAST(TACTIC_EVNT_ID AS DECIMAL(20,0))        AS acct_no
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
) a
LEFT JOIN D3CV12A.ACCT_CRD_OWN_DLY_DELTA b
       ON  a.acct_no         = b.acct_no
       AND a.prod_cd         = b.prod_cd
       AND b.CHG_DT          = DATE '9999-12-31'
       AND b.RELATIONSHIP_CD = '2'
       AND b.card_sts IN ('A', '')
       AND b.CAPTR_DT        > a.TREATMT_STRT_DT
GROUP BY
    a.TACTIC_ID, a.TREATMT_STRT_DT, a.TREATMT_MN,
    a.TST_GRP_CD, a.RPT_GRP_CD, a.TACTIC_CELL_CD, a.prod_cd
ORDER BY a.TACTIC_ID, a.TREATMT_MN, a.TST_GRP_CD, a.RPT_GRP_CD;


-- Q4: Daily vintage — leads and AU adds per arm per CAPTR_DT
SELECT
    a.TACTIC_ID,
    a.TREATMT_STRT_DT,
    a.TREATMT_MN,
    b.CAPTR_DT,
    COUNT(*)                                                        AS leads,
    SUM(CASE WHEN b.acct_no IS NOT NULL THEN 1 ELSE 0 END)          AS au_adds
FROM (
    SELECT
        TACTIC_ID,
        TREATMT_STRT_DT,
        TREATMT_MN,
        SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 3)        AS prod_cd,
        CAST(TACTIC_EVNT_ID AS DECIMAL(20,0))        AS acct_no
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
) a
LEFT JOIN D3CV12A.ACCT_CRD_OWN_DLY_DELTA b
       ON  a.acct_no         = b.acct_no
       AND a.prod_cd         = b.prod_cd
       AND b.CHG_DT          = DATE '9999-12-31'
       AND b.RELATIONSHIP_CD = '2'
       AND b.card_sts IN ('A', '')
       AND b.CAPTR_DT        > a.TREATMT_STRT_DT
GROUP BY a.TACTIC_ID, a.TREATMT_STRT_DT, a.TREATMT_MN, b.CAPTR_DT
ORDER BY a.TACTIC_ID, a.TREATMT_MN, b.CAPTR_DT;
