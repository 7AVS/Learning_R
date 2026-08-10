-- ============================================================================
-- PCQ Sales Modal: Champion vs Challenger size, per deployment, with the range.
-- ENGINE: Teradata-direct    TABLE: DTZV01.TACTIC_EVNT_IP_AR_H60M
-- Returns: deployment | strt_dt | end_dt | grp | clients   (~6-8 rows)
--
-- Modal Sales arms are EXACTLY these three codes. PCQ's other NG3_* codes
-- (NG3_PROP, NG3_NTC, NG3_A2C) are the rest of the campaign, NOT the modal
-- experiment - do not match on LIKE 'NG3%'.
--   NG3_CHMP              -> Champion
--   NG3_CHLN, NG3_CHLG    -> Challenger  (pooled)
-- ============================================================================

SELECT
      x.tactic_id                                                AS deployment
    , MIN(x.strt_dt)                                             AS strt_dt
    , MAX(x.end_dt)                                              AS end_dt
    , x.grp                                                      AS grp
    , COUNT(*)                                                   AS clients
FROM (
    SELECT
          TACTIC_ID                          AS tactic_id
        , CLNT_NO                            AS clnt_no
        , CAST(CASE WHEN TRIM(TST_GRP_CD) = 'NG3_CHMP' THEN 'Champion'
                    ELSE 'Challenger' END AS VARCHAR(20))        AS grp
        , MIN(TREATMT_STRT_DT)               AS strt_dt
        , MAX(TREATMT_END_DT)                AS end_dt
    FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
      AND TREATMT_STRT_DT >= DATE '2026-01-01'
      AND TRIM(TST_GRP_CD) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
    GROUP BY TACTIC_ID, CLNT_NO,
             CAST(CASE WHEN TRIM(TST_GRP_CD) = 'NG3_CHMP' THEN 'Champion'
                       ELSE 'Challenger' END AS VARCHAR(20))
) x
GROUP BY x.tactic_id, x.grp
ORDER BY 2, 4
;
