-- ============================================================================
-- PCQ experiment: population per arm, per deployment, with the date range.
-- ENGINE: Teradata-direct    TABLE: DTZV01.TACTIC_EVNT_IP_AR_H60M
-- Returns: deployment | strt_dt | end_dt | arm | clients
-- ============================================================================

SELECT
      x.tactic_id                                                AS deployment
    , MIN(x.strt_dt)                                             AS strt_dt
    , MAX(x.end_dt)                                              AS end_dt
    , x.tst_grp_cd                                               AS arm
    , COUNT(*)                                                   AS clients
FROM (
    SELECT
          TACTIC_ID                          AS tactic_id
        , TRIM(TST_GRP_CD)                   AS tst_grp_cd
        , CLNT_NO                            AS clnt_no
        , MIN(TREATMT_STRT_DT)               AS strt_dt
        , MAX(TREATMT_END_DT)                AS end_dt
    FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
      AND TREATMT_STRT_DT >= DATE '2026-01-01'
    GROUP BY TACTIC_ID, TRIM(TST_GRP_CD), CLNT_NO
) x
GROUP BY x.tactic_id, x.tst_grp_cd
ORDER BY 2, 4
;
