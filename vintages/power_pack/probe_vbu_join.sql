-- ============================================================================
-- Does the curated VBU table join to the tactic table at all?
-- ENGINE: Teradata-direct
--
-- pp_vbu_campaign.sql joins
--     dl_mr_prod.cards_bizups_vbu_descresp_clnt  ON (tactic_id, clnt_no)
--     DTZV01.TACTIC_EVNT_IP_AR_H60M
-- and returns zero rows. The population on both sides is known to be non-empty,
-- so the join is the prime suspect: if the tactic_id strings differ in format,
-- or clnt_no differs in type, the join matches NOTHING and the output is blank.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- BLOCK 1 - the deployment ids on each side, side by side. ~18 rows.
-- If the same id appears twice (once per src) the ids match.
-- If curated ids never appear under 'tactic', the join can never work.
-- ----------------------------------------------------------------------------
SELECT CAST('curated' AS VARCHAR(10))                            AS src
     , TRIM(tactic_id)                                           AS tactic_id
     , COUNT(DISTINCT clnt_no)                                   AS clients
FROM dl_mr_prod.cards_bizups_vbu_descresp_clnt
WHERE response_start >= DATE '2026-01-01'
GROUP BY 1, 2

UNION ALL

SELECT CAST('tactic'  AS VARCHAR(10))                            AS src
     , TRIM(TACTIC_ID)                                           AS tactic_id
     , COUNT(DISTINCT CLNT_NO)                                   AS clients
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE SUBSTR(TACTIC_ID, 8, 3) = 'VBU'
  AND TREATMT_STRT_DT >= DATE '2026-01-01'
GROUP BY 1, 2

ORDER BY 2, 1
;


-- ----------------------------------------------------------------------------
-- BLOCK 2 - does the join actually match? 1 row.
-- matched_clients = 0 confirms the join is the bug.
-- ----------------------------------------------------------------------------
SELECT
      COUNT(*)                                                   AS matched_rows
    , COUNT(DISTINCT c.clnt_no)                                  AS matched_clients
    , COUNT(DISTINCT c.tactic_id)                                AS matched_deployments
FROM dl_mr_prod.cards_bizups_vbu_descresp_clnt c
INNER JOIN DTZV01.TACTIC_EVNT_IP_AR_H60M t
    ON  TRIM(t.TACTIC_ID) = TRIM(c.tactic_id)
    AND t.CLNT_NO         = c.clnt_no
WHERE c.response_start >= DATE '2026-01-01'
  AND SUBSTR(t.TACTIC_ID, 8, 3) = 'VBU'
;


-- ----------------------------------------------------------------------------
-- BLOCK 3 - same join with both client keys normalised through DECIMAL(38,0).
-- The repo has a documented trap where one side is numeric and the other a
-- zero-padded varchar, and a raw compare matches nothing
-- (references/vintage_datalab_method.md, CRV join-key trap).
-- If Block 2 returns 0 and this returns rows, that is the fix. 1 row.
-- ----------------------------------------------------------------------------
SELECT
      COUNT(*)                                                   AS matched_rows
    , COUNT(DISTINCT c.clnt_no)                                  AS matched_clients
    , COUNT(DISTINCT c.tactic_id)                                AS matched_deployments
FROM dl_mr_prod.cards_bizups_vbu_descresp_clnt c
INNER JOIN DTZV01.TACTIC_EVNT_IP_AR_H60M t
    ON  TRIM(t.TACTIC_ID) = TRIM(c.tactic_id)
    AND CAST(t.CLNT_NO AS DECIMAL(38,0)) = CAST(c.clnt_no AS DECIMAL(38,0))
WHERE c.response_start >= DATE '2026-01-01'
  AND SUBSTR(t.TACTIC_ID, 8, 3) = 'VBU'
;


-- ----------------------------------------------------------------------------
-- BLOCK 4 - how many curated deployments survive the Q3 <<WINDOW>> at all?
-- If this is 0, the window is the bug, not the join. ~7 rows.
-- ----------------------------------------------------------------------------
SELECT
      TRIM(tactic_id)                                            AS tactic_id
    , MIN(response_start)                                        AS min_start
    , MAX(response_end)                                          AS max_end
    , CASE WHEN MAX(response_end) >= DATE '2026-05-01'
            AND MAX(response_end) <  DATE '2026-08-01'
           THEN 'IN Q3' ELSE 'CUT' END                           AS window_verdict
    , COUNT(DISTINCT clnt_no)                                    AS clients
FROM dl_mr_prod.cards_bizups_vbu_descresp_clnt
WHERE response_start >= DATE '2026-01-01'
GROUP BY 1
ORDER BY 2
;
