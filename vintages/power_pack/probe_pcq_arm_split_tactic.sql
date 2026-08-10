-- ============================================================================
-- PROBE: PCQ arm split read from the TACTIC EVENT table, not the curated set
-- ENGINE: Teradata-direct
-- Table:  DTZV01.TACTIC_EVNT_IP_AR_H60M
--         (if this returns nothing, retry against DG6V01.TACTIC_EVNT_IP_AR_HIST
--           both variants exist; H60M is the one that returned PCQ rows on
--          2026-08-10)
--
-- WHY: the curated run (probe_pcq_arm_split.sql) showed PCQ is a clean 50/50
--      and that Modal Sales exists on only three waves starting 2026-06-01.
--      This reproduces the same read from raw tactic history so the curated
--      table is not the single source of truth.
--
-- WHAT DIFFERS FROM THE CURATED VERSION:
--   curated                      tactic table
--   -------                      ------------
--   test_group_latest            TST_GRP_CD
--   treatmt_start_dt             TREATMT_STRT_DT
--   treatmt_end_dt               TREATMT_END_DT
--   tpa_ita = 'TPA'              no equivalent  TPA/ITA is a curated field.
--                                Population is scoped by SUBSTR(TACTIC_ID,8,3)='PCQ'.
--   (no equivalent)              SUBSTR(TACTIC_DECISN_VRB_INFO,121,30) LIKE '%MS%'
--                                = the Modal Sales delivery marker
--                                (campaigns/sales_modal/pcq/pcq_ms_vs_benchmark.sql:31)
--
--   [VERIFY] whether TST_GRP_CD on the tactic table carries the same
--   NG3_CHMP / NG3_CHLN / NG3_CHLG strings as curated test_group_latest.
--   Block A does NOT assume it does  it lists whatever codes are actually
--   there. Read Block A before trusting any mapping.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- BLOCK A - what arm codes actually exist on the tactic table for PCQ, per wave.
-- No assumption about NG3_*. Expect ~15-40 rows.
-- Compare against the curated result:
--     2026-06-01 -> 2026-07-10   CHLG 3,886   CHLN 63,995   CHMP 67,625
--     2026-06-09 -> 2026-08-07   CHLG 2,229   CHLN 69,576   CHMP 71,769
--     2026-07-14 -> 2026-09-04   CHLG 2,925   CHLN 64,127   CHMP 67,308
-- ----------------------------------------------------------------------------
SELECT
      TACTIC_ID
    , TRIM(TST_GRP_CD)                                           AS tst_grp_cd
    , MIN(TREATMT_STRT_DT)                                       AS strt_dt
    , MAX(TREATMT_END_DT)                                        AS end_dt
    , CASE WHEN MAX(TREATMT_END_DT) >= DATE '2026-05-01'
            AND MAX(TREATMT_END_DT) <  DATE '2026-08-01'
           THEN 'IN Q3' ELSE 'CUT' END                           AS window_verdict
    , COUNT(DISTINCT CLNT_NO)                                    AS clients
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
  AND TREATMT_STRT_DT >= DATE '2026-01-01'
GROUP BY 1, 2
ORDER BY strt_dt, tst_grp_cd
;


-- ----------------------------------------------------------------------------
-- BLOCK B - the Modal Sales marker, independent of TST_GRP_CD.
-- Shows which waves carried MS and how many clients, so experiment onset is
-- readable straight off the tactic table. Expect ~20-40 rows.
-- ----------------------------------------------------------------------------
SELECT
      TACTIC_ID
    , MIN(TREATMT_STRT_DT)                                       AS strt_dt
    , MAX(TREATMT_END_DT)                                        AS end_dt
    , CASE WHEN MAX(TREATMT_END_DT) >= DATE '2026-05-01'
            AND MAX(TREATMT_END_DT) <  DATE '2026-08-01'
           THEN 'IN Q3' ELSE 'CUT' END                           AS window_verdict
    , COUNT(DISTINCT CLNT_NO)                                    AS clients_total
    , COUNT(DISTINCT CASE WHEN SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
                          THEN CLNT_NO END)                      AS clients_ms
    , CASE WHEN COUNT(DISTINCT CASE WHEN SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
                                    THEN CLNT_NO END) > 0
           THEN 'MODAL SALES' ELSE '' END                        AS carries_experiment
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
  AND TREATMT_STRT_DT >= DATE '2026-01-01'
GROUP BY 1
ORDER BY strt_dt
;


-- ----------------------------------------------------------------------------
-- BLOCK C - the arm split rolled to cohort month, tactic-side.
-- Direct comparison against the curated Block B, which returned:
--     2026-06  Challenger 139,686  Champion 139,394
--     2026-07  Challenger  67,052  Champion  67,308
--
-- The CASE below assumes TST_GRP_CD carries the NG3_* strings. If Block A
-- shows different codes, EDIT THIS CASE to match what is really there before
-- reading the numbers.
-- ----------------------------------------------------------------------------
SELECT
      CAST(EXTRACT(YEAR FROM TREATMT_STRT_DT) AS VARCHAR(4)) || '-' ||
      CASE WHEN EXTRACT(MONTH FROM TREATMT_STRT_DT) < 10 THEN '0' ELSE '' END ||
      CAST(EXTRACT(MONTH FROM TREATMT_STRT_DT) AS VARCHAR(2))    AS cohort_month
    , CASE WHEN TRIM(TST_GRP_CD) = 'NG3_CHMP'               THEN 'Champion'
           WHEN TRIM(TST_GRP_CD) IN ('NG3_CHLN','NG3_CHLG') THEN 'Challenger'
           ELSE 'OTHER  see Block A'                       END AS grp
    , COUNT(DISTINCT CLNT_NO)                                    AS clients
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
  AND TREATMT_STRT_DT >= DATE '2026-01-01'
GROUP BY 1, 2
ORDER BY 1, 2
;
