-- ============================================================================
-- Why does pp_pcq_sales_modal.sql produce a 2026-05 cohort and a non-50/50
-- split, when both the curated table and the tactic table say PCQ Modal Sales
-- is June/July only and 50/50?
--
-- This is the vintage's OWN population + cohort + grp logic, copied exactly,
-- stopping before the spine and the success join. Whatever it returns is what
-- the curve is built on.
--
-- ENGINE: Teradata-direct   TABLE: dl_mr_prod.cards_tpa_pcq_decision_resp
--
-- EXPECTED (from the tactic table, confirmed 2026-08-10):
--     2026-06   Champion 139,394   Challenger 139,686
--     2026-07   Champion  98,186   Challenger  98,248
--   and with the <<WINDOW>> applied, only deployment 2026152PCQ survives
--   (ends 2026-07-10), so 2026-06 alone, ~67,625 / ~67,882.
--
-- HOW TO READ IT:
--   Block 1 returns 2026-06 only, ~50/50   -> population is fine, the bug is
--                                             downstream in the spine/join.
--   Block 1 returns a 2026-05 row          -> the bug is in the population,
--                                             and Block 2 shows which
--                                             deployment is dragging it in.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- BLOCK 1 - the cells the curve uses. Expect 2 rows.
-- ----------------------------------------------------------------------------
SELECT
      c.cohort_month
    , c.grp
    , COUNT(*)                                                   AS base
FROM (
    SELECT
          clnt_no
        , CAST(
            CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM treatmt_start_dt) < 10 THEN '0' ELSE '' END ||
            TRIM(CAST(EXTRACT(MONTH FROM treatmt_start_dt) AS VARCHAR(2)))
          AS VARCHAR(7))                                         AS cohort_month
        , CAST(CASE WHEN TRIM(test_group_latest) = 'NG3_CHMP' THEN 'Champion'
                    ELSE 'Challenger' END AS VARCHAR(20))        AS grp
    FROM dl_mr_prod.cards_tpa_pcq_decision_resp
    WHERE decsn_year       = 2026
      AND tpa_ita          = 'TPA'
      AND treatmt_start_dt >= DATE '2026-01-01'
      AND treatmt_end_dt   >= DATE '2026-05-01'
      AND treatmt_end_dt   <  DATE '2026-08-01'
      AND TRIM(test_group_latest) IN ('NG3_CHMP','NG3_CHLN','NG3_CHLG')
    GROUP BY
          clnt_no
        , CAST(
            CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM treatmt_start_dt) < 10 THEN '0' ELSE '' END ||
            TRIM(CAST(EXTRACT(MONTH FROM treatmt_start_dt) AS VARCHAR(2)))
          AS VARCHAR(7))
        , CAST(CASE WHEN TRIM(test_group_latest) = 'NG3_CHMP' THEN 'Champion'
                    ELSE 'Challenger' END AS VARCHAR(20))
) c
GROUP BY c.cohort_month, c.grp
ORDER BY 1, 2
;


-- ----------------------------------------------------------------------------
-- BLOCK 2 - same population, but showing the raw dates per deployment.
-- If a 2026-05 cohort exists, this names the deployment producing it and
-- shows whether the curated treatmt_start_dt disagrees with the tactic
-- table's TREATMT_STRT_DT for the same wave. Expect a handful of rows.
-- ----------------------------------------------------------------------------
SELECT
      tactic_id
    , MIN(treatmt_start_dt)                                      AS min_start
    , MAX(treatmt_start_dt)                                      AS max_start
    , MIN(treatmt_end_dt)                                        AS min_end
    , MAX(treatmt_end_dt)                                        AS max_end
    , TRIM(test_group_latest)                                    AS test_group_latest
    , COUNT(DISTINCT clnt_no)                                    AS clients
FROM dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE decsn_year       = 2026
  AND tpa_ita          = 'TPA'
  AND treatmt_start_dt >= DATE '2026-01-01'
  AND treatmt_end_dt   >= DATE '2026-05-01'
  AND treatmt_end_dt   <  DATE '2026-08-01'
  AND TRIM(test_group_latest) IN ('NG3_CHMP','NG3_CHLN','NG3_CHLG')
GROUP BY tactic_id, TRIM(test_group_latest)
ORDER BY 2, 6
;
