-- ============================================================================
-- VBA + VBU: arm codes, population, deployments and range. Tactic table only.
-- ENGINE: Teradata-direct   TABLE: DTZV01.TACTIC_EVNT_IP_AR_H60M
-- Scope: deployments starting 2026-03-01 onward.
-- Returns: mne | deployment | strt_dt | end_dt | arm_code | arm_guess | clients | in_q3
--
-- arm_code  = TST_GRP_CD exactly as it is on the row. No mapping assumed.
-- arm_guess = LEFT char C/T convention the pp_ files currently use. If arm_code
--             shows a different scheme, arm_guess is wrong and the vintage
--             files need correcting.
-- in_q3     = end date falls 2026-05-01..2026-07-31. Display only, not a filter.
-- ============================================================================

SELECT
      SUBSTR(x.tactic_id, 8, 3)                                  AS mne
    , x.tactic_id                                                AS deployment
    , MIN(x.strt_dt)                                             AS strt_dt
    , MAX(x.end_dt)                                              AS end_dt
    , x.arm_code
    , CASE WHEN SUBSTR(x.arm_code, 1, 1) = 'C' THEN 'Control'
           WHEN SUBSTR(x.arm_code, 1, 1) = 'T' THEN 'Action'
           ELSE 'unmapped' END                                   AS arm_guess
    , COUNT(*)                                                   AS clients
    , CASE WHEN MAX(x.end_dt) >= DATE '2026-05-01'
            AND MAX(x.end_dt) <  DATE '2026-08-01'
           THEN 'Q3' ELSE '' END                                 AS in_q3
FROM (
    SELECT
          TACTIC_ID                          AS tactic_id
        , TRIM(TST_GRP_CD)                   AS arm_code
        , CLNT_NO                            AS clnt_no
        , MIN(TREATMT_STRT_DT)               AS strt_dt
        , MAX(TREATMT_END_DT)                AS end_dt
    FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
    WHERE SUBSTR(TACTIC_ID, 8, 3) IN ('VBA', 'VBU')
      AND TREATMT_STRT_DT >= DATE '2026-03-01'
    GROUP BY TACTIC_ID, TRIM(TST_GRP_CD), CLNT_NO
) x
GROUP BY SUBSTR(x.tactic_id, 8, 3), x.tactic_id, x.arm_code
ORDER BY 1, 3, 5
;
