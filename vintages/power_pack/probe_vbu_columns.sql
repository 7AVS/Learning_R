-- ============================================================================
-- Why does pp_vbu_campaign.sql return zero rows?
-- ENGINE: Teradata-direct   TABLE: dl_mr_prod.cards_bizups_vbu_descresp_clnt
--
-- Two suspects, both mine:
--   1. The arm derivation was a GUESS. The schema doc documents no convention
--      for turning test_group / control into Action/Control. If it yields no
--      'Control' rows, the file's WHERE grp IS NOT NULL drops everything.
--   2. The seed filter added 2026-08-10 requires a deployment to have
--      Control clients > 0. Combined with (1), that excludes every deployment.
--
-- Run BLOCK 1 first - it shows the real column names and values.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- BLOCK 1 - what is actually on the table. 5 rows, all columns.
-- If this errors on the table name, that is the answer.
-- ----------------------------------------------------------------------------
SELECT TOP 5 *
FROM dl_mr_prod.cards_bizups_vbu_descresp_clnt
;


-- ----------------------------------------------------------------------------
-- BLOCK 2 - the arm columns, per deployment. Expect ~10-30 rows.
-- Shows whether test_group / control are populated at all, and what they hold.
-- If either column name errors, it does not exist and the vintage is wrong.
-- ----------------------------------------------------------------------------
SELECT
      tactic_id
    , TRIM(test_group)                                           AS test_group
    , TRIM(control)                                              AS control_col
    , COUNT(DISTINCT clnt_no)                                    AS clients
FROM dl_mr_prod.cards_bizups_vbu_descresp_clnt
WHERE response_start >= DATE '2026-03-01'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;


-- ----------------------------------------------------------------------------
-- BLOCK 3 - does the table have any rows in our window at all?
-- Separates "arm derivation broken" from "population empty". 1 row.
-- ----------------------------------------------------------------------------
SELECT
      COUNT(*)                                                   AS rows_
    , COUNT(DISTINCT clnt_no)                                    AS clients
    , COUNT(DISTINCT tactic_id)                                  AS deployments
    , MIN(response_start)                                        AS min_start
    , MAX(response_start)                                        AS max_start
    , MIN(response_end)                                          AS min_end
    , MAX(response_end)                                          AS max_end
    , SUM(CASE WHEN responder_targetproduct = 1 THEN 1 ELSE 0 END) AS responders
    , COUNT(dt_prod_change_client)                               AS change_dates_populated
FROM dl_mr_prod.cards_bizups_vbu_descresp_clnt
WHERE response_start >= DATE '2026-01-01'
;
