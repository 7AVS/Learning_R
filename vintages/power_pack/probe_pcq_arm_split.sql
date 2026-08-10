-- ============================================================================
-- PROBE: is the Q3 <<WINDOW>> filter cutting the PCQ Champion arm?
-- ENGINE: Teradata-direct
-- Table:  DL_MR_PROD.cards_tpa_pcq_decision_resp
--
-- SYMPTOMS (Andre, 2026-08-10, running pp_pcq_sales_modal.sql):
--   1. Champion and Challenger should be a ~50/50 split. Champion is far behind.
--   2. Only a May cohort comes back; March/April/June are missing.
--
-- HYPOTHESIS:
--   pp_pcq_sales_modal.sql filters on treatmt_end_dt (the Q3 window) at the
--   DEPLOYMENT level. If the Champion wave and the Challenger wave carry
--   DIFFERENT treatmt_end_dt values, the window keeps one arm and drops the
--   other. That would explain both symptoms at once, and it would mean the
--   randomisation is being silently destroyed by the scoping filter.
--
--   An experiment's arms must be selected TOGETHER. Filtering arms by a
--   deployment-level date is the error.
--
-- HOW TO READ BLOCK A:
--   NG3_CHMP rows showing 'CUT' while NG3_CHLN/NG3_CHLG show 'IN Q3'
--       -> HYPOTHESIS CONFIRMED. The window is slicing the control arm out.
--   All three codes showing the same verdict per wave
--       -> hypothesis dead, the imbalance is somewhere else; run Block B.
--   NG3_* codes present only on May-starting waves
--       -> the single May cohort is CORRECT: Modal Sales started in May.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- BLOCK A - THE ANSWER. Every (test group x wave) with its window verdict.
-- Expect ~10-30 rows.
-- ----------------------------------------------------------------------------
SELECT
      TRIM(test_group_latest)                                    AS test_group
    , treatmt_start_dt
    , treatmt_end_dt
    , CASE WHEN treatmt_end_dt >= DATE '2026-05-01'
            AND treatmt_end_dt <  DATE '2026-08-01'
           THEN 'IN Q3' ELSE 'CUT' END                           AS window_verdict
    , COUNT(DISTINCT clnt_no)                                    AS clients
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
WHERE decsn_year       = 2026
  AND tpa_ita          = 'TPA'
  AND treatmt_start_dt >= DATE '2026-01-01'
  AND TRIM(test_group_latest) IN ('NG3_CHMP','NG3_CHLN','NG3_CHLG')
GROUP BY 1, 2, 3
ORDER BY 2, 1
;


-- ----------------------------------------------------------------------------
-- BLOCK B - the split as it stands, ignoring the window entirely.
-- This is what the arm balance SHOULD look like. Expect ~8 rows.
-- If Champion is ~50% here but not in the curve, the window is the cause.
-- If Champion is already short here, the imbalance predates my filter and
-- lives in the source data or the test_group_latest column itself.
-- ----------------------------------------------------------------------------
SELECT
      CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) || '-' ||
      CASE WHEN EXTRACT(MONTH FROM treatmt_start_dt) < 10 THEN '0' ELSE '' END ||
      CAST(EXTRACT(MONTH FROM treatmt_start_dt) AS VARCHAR(2))   AS cohort_month
    , CASE WHEN TRIM(test_group_latest) = 'NG3_CHMP'                THEN 'Champion'
           WHEN TRIM(test_group_latest) IN ('NG3_CHLN','NG3_CHLG')  THEN 'Challenger'
      END                                                        AS grp
    , COUNT(DISTINCT clnt_no)                                    AS clients
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
WHERE decsn_year       = 2026
  AND tpa_ita          = 'TPA'
  AND treatmt_start_dt >= DATE '2026-01-01'
  AND TRIM(test_group_latest) IN ('NG3_CHMP','NG3_CHLN','NG3_CHLG')
GROUP BY 1, 2
ORDER BY 1, 2
;


-- ----------------------------------------------------------------------------
-- BLOCK C - does the first-touch dedup drop one arm?
-- pp_pcq_sales_modal.sql keeps one row per (clnt_no, cohort_month) on earliest
-- treatmt_start_dt. If a client appears as Champion in one wave and Challenger
-- in a later wave that same month, dedup keeps the earlier one - and if that
-- skews one way, it shifts the split.
-- Expect a small number, ideally 0.
-- ----------------------------------------------------------------------------
SELECT
      cohort_month
    , COUNT(*)                                                   AS clients_in_both_arms
FROM (
    SELECT
          clnt_no
        , CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) || '-' ||
          CASE WHEN EXTRACT(MONTH FROM treatmt_start_dt) < 10 THEN '0' ELSE '' END ||
          CAST(EXTRACT(MONTH FROM treatmt_start_dt) AS VARCHAR(2)) AS cohort_month
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE decsn_year       = 2026
      AND tpa_ita          = 'TPA'
      AND treatmt_start_dt >= DATE '2026-01-01'
      AND TRIM(test_group_latest) IN ('NG3_CHMP','NG3_CHLN','NG3_CHLG')
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT CASE WHEN TRIM(test_group_latest) = 'NG3_CHMP'
                               THEN 'Champion' ELSE 'Challenger' END) > 1
) x
GROUP BY 1
ORDER BY 1
;
