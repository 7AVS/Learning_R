-- AUH Interim Measurement — Phase 1 + Phase 2 (all waves)
-- All AUH tactic_ids captured via SUBSTR. All codes preserved.
-- Lift slicer = TREATMT_MN (PAUHNM% = Control). Sub-segment grain via TST_GRP_CD x RPT_GRP_CD empirical profile.


-- Q1: Code interaction profile (no success join) — find the grain
SELECT
    TACTIC_ID,
    TREATMT_STRT_DT,
    TREATMT_END_DT,
    TREATMT_MN,
    CASE WHEN TREATMT_MN LIKE 'PAUHNM%' THEN 'Control' ELSE 'Action' END  AS control_grp,
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


-- Q2a: Sample raw TACTIC_DECISN_VRB_INFO strings (NO GROUP BY)
-- The string is a packed list; tail positions are client-unique numerics.
-- Never GROUP BY the full string — only header positions are categorical.
SELECT TOP 200
    TACTIC_ID,
    TREATMT_MN,
    TST_GRP_CD,
    TACTIC_CELL_CD,
    TACTIC_DECISN_VRB_INFO
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
ORDER BY TACTIC_ID, TREATMT_MN, TST_GRP_CD;


-- Q2b: Categorical-only profile of VRB_INFO header positions
-- Adjust SUBSTR positions once Q2a reveals the layout.
SELECT
    TACTIC_ID,
    TREATMT_MN,
    TST_GRP_CD,
    SUBSTR(TACTIC_DECISN_VRB_INFO,  1, 10)   AS vrb_tactic_id,   -- expect = TACTIC_ID
    SUBSTR(TACTIC_DECISN_VRB_INFO, 12,  8)   AS vrb_model_code,  -- AUHQUTV8 / AUHABMKN / ...
    SUBSTR(TACTIC_DECISN_VRB_INFO, 21,  5)   AS vrb_prod_seg,    -- IAV / MC1 / RNMAV / CLO / ...
    COUNT(*) AS leads
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
GROUP BY
    TACTIC_ID, TREATMT_MN, TST_GRP_CD,
    SUBSTR(TACTIC_DECISN_VRB_INFO,  1, 10),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 12,  8),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 21,  5)
ORDER BY TACTIC_ID, TREATMT_MN, TST_GRP_CD;


-- Q3: Action vs Control rollup with success — top-line lift per wave
SELECT
    a.TACTIC_ID,
    a.TREATMT_STRT_DT,
    a.control_grp,
    a.TREATMT_MN,
    COUNT(*)                                                        AS leads,
    SUM(CASE WHEN b.acct_no IS NOT NULL THEN 1 ELSE 0 END)          AS au_adds
FROM (
    SELECT
        TACTIC_ID,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TREATMT_MN,
        CASE WHEN TREATMT_MN LIKE 'PAUHNM%' THEN 'Control' ELSE 'Action' END  AS control_grp,
        SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 3)        AS prod_cd,
        CAST(TACTIC_EVNT_ID AS DECIMAL(20,0))        AS acct_no
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
) a
LEFT JOIN D3CV12A.ACCT_CRD_OWN_DLY_DELTA b
       ON  a.acct_no         = b.acct_no
       AND a.prod_cd         = b.prod_cd
       AND b.CHG_DT          = DATE '9999-12-31'
       AND b.RELATIONSHIP_CD = 'Z'          -- verify: 'Z' (Daniel doc) vs '2' (screenshot)
       AND (b.card_sts = 'A' OR b.card_sts IS NULL)
       AND b.CAPTR_DT        > a.TREATMT_STRT_DT
GROUP BY
    a.TACTIC_ID, a.TREATMT_STRT_DT, a.control_grp, a.TREATMT_MN
ORDER BY a.TACTIC_ID, a.control_grp, a.TREATMT_MN;


-- Q4: Full-grain rollup with success — Action/Control by sub-segment codes
SELECT
    a.TACTIC_ID,
    a.TREATMT_STRT_DT,
    a.control_grp,
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
        CASE WHEN TREATMT_MN LIKE 'PAUHNM%' THEN 'Control' ELSE 'Action' END  AS control_grp,
        SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 3)        AS prod_cd,
        CAST(TACTIC_EVNT_ID AS DECIMAL(20,0))        AS acct_no
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
) a
LEFT JOIN D3CV12A.ACCT_CRD_OWN_DLY_DELTA b
       ON  a.acct_no         = b.acct_no
       AND a.prod_cd         = b.prod_cd
       AND b.CHG_DT          = DATE '9999-12-31'
       AND b.RELATIONSHIP_CD = 'Z'
       AND (b.card_sts = 'A' OR b.card_sts IS NULL)
       AND b.CAPTR_DT        > a.TREATMT_STRT_DT
GROUP BY
    a.TACTIC_ID, a.TREATMT_STRT_DT, a.control_grp, a.TREATMT_MN,
    a.TST_GRP_CD, a.RPT_GRP_CD, a.TACTIC_CELL_CD, a.prod_cd
ORDER BY a.TACTIC_ID, a.control_grp, a.TREATMT_MN, a.TST_GRP_CD, a.RPT_GRP_CD;


-- Q5: Daily vintage — for the rate-curve chart (Action vs Control per wave)
SELECT
    a.TACTIC_ID,
    a.TREATMT_STRT_DT,
    a.control_grp,
    b.CAPTR_DT,
    COUNT(*)                                                        AS leads,
    SUM(CASE WHEN b.acct_no IS NOT NULL THEN 1 ELSE 0 END)          AS au_adds
FROM (
    SELECT
        TACTIC_ID,
        TREATMT_STRT_DT,
        TREATMT_MN,
        CASE WHEN TREATMT_MN LIKE 'PAUHNM%' THEN 'Control' ELSE 'Action' END  AS control_grp,
        SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 3)        AS prod_cd,
        CAST(TACTIC_EVNT_ID AS DECIMAL(20,0))        AS acct_no
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
) a
LEFT JOIN D3CV12A.ACCT_CRD_OWN_DLY_DELTA b
       ON  a.acct_no         = b.acct_no
       AND a.prod_cd         = b.prod_cd
       AND b.CHG_DT          = DATE '9999-12-31'
       AND b.RELATIONSHIP_CD = 'Z'
       AND (b.card_sts = 'A' OR b.card_sts IS NULL)
       AND b.CAPTR_DT        > a.TREATMT_STRT_DT
GROUP BY a.TACTIC_ID, a.TREATMT_STRT_DT, a.control_grp, b.CAPTR_DT
ORDER BY a.TACTIC_ID, a.control_grp, b.CAPTR_DT;
