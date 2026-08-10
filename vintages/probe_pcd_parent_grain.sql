-- ============================================================================
-- PROBE: is cards_pcd_ongoing_decis_resp.tactic_id_parent 1:1 with a
--        deployment, or does it group several child deployments?
-- ENGINE: Teradata-direct (no catalog prefix)
-- Table:  dl_mr_prod.cards_pcd_ongoing_decis_resp
--
-- WHY THIS MATTERS:
--   pcd_vintage_monthly.sql L52 (and _quarterly L59) filter:
--       WHERE tactic_id_parent = '2026111PCD'
--   The tactic table shows EIGHT PCD deployments ending in Q3, totalling
--   2,244,854 client-rows. '2026111PCD' is one of them, at 513,325.
--
--   If tactic_id_parent is 1:1 with a deployment -> the PCD Q3 curve is
--   built on ~23% of volume and needs to be widened.
--   If it groups children  -> the single filter may already cover the 8.
--
-- CIRCUMSTANTIAL EVIDENCE FOR 1:1 (not proof, hence this probe):
--   - click_classification_diagnostic.sql:61 filters
--       tactic_id_parent IN ('2026111PCD','2026125PCD')
--     Both strings appear as TACTIC_IDs in DTZV01.TACTIC_EVNT_IP_AR_H60M.
--   - This table carries NO plain tactic_id column - only the _parent one.
--   - PLI names its equivalent parent_tactic_id; TPA names its tactic_id.
--     "Parent" reads as a naming habit, not a hierarchy.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- BLOCK A - THE ANSWER. Distinct tactic_id_parent values whose response_end
-- lands in Q3. Expect <= 10 rows.
--
-- READ IT LIKE THIS:
--   ~8 rows, values matching the 8 TACTIC_IDs from probe_tactic_ids_q3
--       -> 1:1 CONFIRMED. The vintage filter is too narrow. Widen it.
--   1 row ('2026111PCD') carrying ~2.2M clients
--       -> grouping CONFIRMED. The filter is fine as-is. No change needed.
-- ----------------------------------------------------------------------------
SELECT
      tactic_id_parent
    , COUNT(*)                                                   AS rows_
    , COUNT(DISTINCT clnt_no)                                    AS clients
    , MIN(response_start)                                        AS min_response_start
    , MAX(response_start)                                        AS max_response_start
    , MIN(response_end)                                          AS min_response_end
    , MAX(response_end)                                          AS max_response_end
FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
WHERE response_start >= DATE '2024-01-01'           -- 2024 data floor
  AND response_end   >= DATE '2026-05-01'           -- Q3 start
  AND response_end   <  DATE '2026-08-01'           -- Q3 end (incl thru 7/31)
GROUP BY 1
ORDER BY clients DESC
;


-- ----------------------------------------------------------------------------
-- BLOCK B - CROSS-CHECK against the tactic table's own numbers.
-- probe_tactic_ids_q3 Block A reported, for TACTIC_ID = '2026111PCD':
--       513,325 clients, strt 2026-04-21, end 2026-06-19
-- If this block returns ~513K clients and those same two dates, the curated
-- table's tactic_id_parent IS the deployment id. That is the cleanest single
-- proof available. A far larger number means it groups.
-- ----------------------------------------------------------------------------
SELECT
      COUNT(*)                                                   AS rows_
    , COUNT(DISTINCT clnt_no)                                    AS clients
    , MIN(response_start)                                        AS min_response_start
    , MAX(response_start)                                        AS max_response_start
    , MIN(response_end)                                          AS min_response_end
    , MAX(response_end)                                          AS max_response_end
FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
;
