-- ============================================================================
-- VBA + VBU: one row per deployment. Action vs Control size, range, Q3 flag.
-- ENGINE: Teradata-direct   TABLE: DTZV01.TACTIC_EVNT_IP_AR_H60M
-- Deployments starting 2026-03-01 onward.
-- ============================================================================

SELECT
      SUBSTR(x.tactic_id, 8, 3)                                  AS mne
    , x.tactic_id                                                AS deployment
    , MIN(x.strt_dt)                                             AS strt_dt
    , MAX(x.end_dt)                                              AS end_dt
    , SUM(CASE WHEN x.arm = 'Action'  THEN 1 ELSE 0 END)         AS action_clients
    , SUM(CASE WHEN x.arm = 'Control' THEN 1 ELSE 0 END)         AS control_clients
    , CASE WHEN MAX(x.end_dt) >= DATE '2026-05-01'
            AND MAX(x.end_dt) <  DATE '2026-08-01'
           THEN 'Q3' ELSE '' END                                 AS in_q3
FROM (
    SELECT
          TACTIC_ID                          AS tactic_id
        , CLNT_NO                            AS clnt_no
        , CAST(CASE WHEN SUBSTR(TRIM(TST_GRP_CD), 1, 1) = 'C' THEN 'Control'
                    ELSE 'Action' END AS VARCHAR(10))            AS arm
        , MIN(TREATMT_STRT_DT)               AS strt_dt
        , MAX(TREATMT_END_DT)                AS end_dt
    FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
    WHERE SUBSTR(TACTIC_ID, 8, 3) IN ('VBA', 'VBU')
      AND TREATMT_STRT_DT >= DATE '2026-03-01'
    GROUP BY TACTIC_ID, CLNT_NO,
             CAST(CASE WHEN SUBSTR(TRIM(TST_GRP_CD), 1, 1) = 'C' THEN 'Control'
                       ELSE 'Action' END AS VARCHAR(10))
) x
GROUP BY SUBSTR(x.tactic_id, 8, 3), x.tactic_id
ORDER BY 1, 3
;
