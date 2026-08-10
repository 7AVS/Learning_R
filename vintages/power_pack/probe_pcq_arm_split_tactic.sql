-- ============================================================================
-- PROBE: PCQ arm split read from the TACTIC EVENT table, not the curated set
-- ENGINE: Teradata-direct
-- Table:  DTZV01.TACTIC_EVNT_IP_AR_H60M
--         (if this returns nothing, retry against DG6V01.TACTIC_EVNT_IP_AR_HIST.
--          Both variants exist; H60M is the one that returned PCQ rows 2026-08-10.)
--
-- REWRITTEN 2026-08-10 after Teradata error 3504 "Selected non-aggregated value
-- must be part of the associated group". Three things changed so it cannot fire:
--   1. GROUP BY lists the full expressions, never ordinals.
--   2. ORDER BY uses positional numbers only, never aliases or bare columns.
--   3. Every block pre-aggregates to one row per (wave, client) in an inner
--      query, so the outer query never needs more than one COUNT(DISTINCT).
--      Teradata also restricts multiple DISTINCT aggregates in one SELECT;
--      pre-aggregating avoids that too.
--
-- WHY THIS FILE: the curated run (probe_pcq_arm_split.sql) showed PCQ is a
-- clean 50/50 and that Modal Sales exists on only three waves starting
-- 2026-06-01. This reproduces that read from raw tactic history.
--
-- CURATED vs TACTIC column mapping:
--   test_group_latest   ->  TST_GRP_CD
--   treatmt_start_dt    ->  TREATMT_STRT_DT
--   treatmt_end_dt      ->  TREATMT_END_DT
--   tpa_ita = 'TPA'     ->  no equivalent. TPA/ITA is a curated field. Scope is
--                           SUBSTR(TACTIC_ID,8,3)='PCQ' instead, so tactic-side
--                           counts may run slightly high if non-TPA rows exist.
--   (none)              ->  SUBSTR(TACTIC_DECISN_VRB_INFO,121,30) LIKE '%MS%'
--                           = Modal Sales delivery marker
--                           (campaigns/sales_modal/pcq/pcq_ms_vs_benchmark.sql:31)
--
-- [VERIFY] whether TST_GRP_CD carries the same NG3_CHMP / NG3_CHLN / NG3_CHLG
-- strings as curated test_group_latest. Block A does NOT assume it does.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- BLOCK A - what arm codes actually exist on the tactic table, per PCQ wave.
-- No assumption about NG3_*. Expect ~15-40 rows.
-- Curated result to tie against:
--     2026-06-01 -> 2026-07-10   CHLG 3,886   CHLN 63,995   CHMP 67,625
--     2026-06-09 -> 2026-08-07   CHLG 2,229   CHLN 69,576   CHMP 71,769
--     2026-07-14 -> 2026-09-04   CHLG 2,925   CHLN 64,127   CHMP 67,308
-- ----------------------------------------------------------------------------
SELECT
      x.tactic_id
    , x.tst_grp_cd
    , MIN(x.strt_dt)                                             AS strt_dt
    , MAX(x.end_dt)                                              AS end_dt
    , CASE WHEN MAX(x.end_dt) >= DATE '2026-05-01'
            AND MAX(x.end_dt) <  DATE '2026-08-01'
           THEN 'IN Q3' ELSE 'CUT' END                           AS window_verdict
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
ORDER BY 3, 2
;


-- ----------------------------------------------------------------------------
-- BLOCK B - the Modal Sales marker, independent of TST_GRP_CD.
-- Which waves carried MS, and how many clients. Expect ~20-40 rows.
-- Scan down to the first row with carries_experiment = 'MODAL SALES' = onset.
-- ----------------------------------------------------------------------------
SELECT
      y.tactic_id
    , MIN(y.strt_dt)                                             AS strt_dt
    , MAX(y.end_dt)                                              AS end_dt
    , CASE WHEN MAX(y.end_dt) >= DATE '2026-05-01'
            AND MAX(y.end_dt) <  DATE '2026-08-01'
           THEN 'IN Q3' ELSE 'CUT' END                           AS window_verdict
    , COUNT(*)                                                   AS clients_total
    , SUM(y.is_ms)                                               AS clients_ms
    , CASE WHEN SUM(y.is_ms) > 0 THEN 'MODAL SALES' ELSE ' ' END AS carries_experiment
FROM (
    SELECT
          TACTIC_ID              AS tactic_id
        , CLNT_NO                AS clnt_no
        , MIN(TREATMT_STRT_DT)   AS strt_dt
        , MAX(TREATMT_END_DT)    AS end_dt
        , MAX(CASE WHEN SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
                   THEN 1 ELSE 0 END)                            AS is_ms
    FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
      AND TREATMT_STRT_DT >= DATE '2026-01-01'
    GROUP BY TACTIC_ID, CLNT_NO
) y
GROUP BY y.tactic_id
ORDER BY 2
;


-- ----------------------------------------------------------------------------
-- BLOCK C - arm split rolled to cohort month, tactic-side.
-- Curated Block B returned:
--     2026-06  Challenger 139,686  Champion 139,394
--     2026-07  Challenger  67,052  Champion  67,308
--
-- The CASE assumes TST_GRP_CD carries the NG3_* strings. If Block A shows
-- different codes, EDIT THIS CASE before reading the numbers.
-- ----------------------------------------------------------------------------
SELECT
      z.cohort_month
    , z.grp
    , COUNT(*)                                                   AS clients
FROM (
    SELECT
          CAST(
            CAST(EXTRACT(YEAR FROM TREATMT_STRT_DT) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM TREATMT_STRT_DT) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM TREATMT_STRT_DT) AS VARCHAR(2))
          AS VARCHAR(7))                                         AS cohort_month
        , CAST(
            CASE WHEN TRIM(TST_GRP_CD) = 'NG3_CHMP'               THEN 'Champion'
                 WHEN TRIM(TST_GRP_CD) IN ('NG3_CHLN','NG3_CHLG') THEN 'Challenger'
                 ELSE 'OTHER - see Block A'
            END AS VARCHAR(30))                                  AS grp
        , CLNT_NO                                                AS clnt_no
    FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
      AND TREATMT_STRT_DT >= DATE '2026-01-01'
    GROUP BY
          CAST(
            CAST(EXTRACT(YEAR FROM TREATMT_STRT_DT) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM TREATMT_STRT_DT) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM TREATMT_STRT_DT) AS VARCHAR(2))
          AS VARCHAR(7))
        , CAST(
            CASE WHEN TRIM(TST_GRP_CD) = 'NG3_CHMP'               THEN 'Champion'
                 WHEN TRIM(TST_GRP_CD) IN ('NG3_CHLN','NG3_CHLG') THEN 'Challenger'
                 ELSE 'OTHER - see Block A'
            END AS VARCHAR(30))
        , CLNT_NO
) z
GROUP BY z.cohort_month, z.grp
ORDER BY 1, 2
;
