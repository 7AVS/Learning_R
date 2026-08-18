-- ENGINE: Teradata-direct
-- Reconciliation check for vbu_vintage_datalab.sql — no spine, 2-4 rows, print only.
-- Source: DL_MR_PROD.cards_bizups_vbu_descresp_clnt (bare schema, same table the
--   vintage build reads).
--
-- DASHBOARD TARGET (vintages/power_pack/pp_vbu_campaign_v2.sql header, lines 84-100 —
--   "RECONCILIATION TARGET" block, deployment Treat_Start_DT 2026-04-13 ->
--   Treat_End_DT 2026-06-30, cohort_month 2026-04):
--     Action   base 28,821 leads   /  471 target-product responders (1.63%)
--     Control  base 11,133 leads   /    5 target-product responders (0.04%)
--   Same numbers appear a second time in v2's "RECONCILIATION -- gap measured
--   2026-08-10" block, labelled there as the dashboard's own figures (as opposed to
--   v2's pre-fix rebuild, which fell short by 42 Action / 5 Control).
--
-- WHAT THIS CHECKS: does DL_MR_PROD.cards_bizups_vbu_descresp_clnt reproduce
--   28,821 / 471 (Action) and 11,133 / 5 (Control) directly, using its own
--   `control` arm field and `responder_targetproduct` flag, no join, no rebuild?
--   `response_start` on this table's population is a daily grain (P1: values run
--   2022-11-14 -> 2026-08-14, one row-cluster per day), while v2's Treat_Start_DT
--   is described as a single deployment date (2026-04-13) with an 11-week response
--   window (-> 2026-06-30). Two queries below cover both readings: exact single day,
--   and the day range through end of month in case the deployment actually spans
--   several `response_start` days on this table.
--
-- ALSO reports `any` responders (responder_anyproduct) alongside `target` for a data
--   point on how much wider the "any product change" definition runs vs the
--   dashboard's presumed target-product-only figure.
--
-- IF THIS MATCHES: the curated table IS the dashboard's source, and
--   vbu_vintage_datalab.sql inherits that reconciliation for free — no further
--   validation needed on population or success detection, only on the horizon/spine
--   mechanics already covered by vba_vintage_datalab.sql's proven pattern.
-- IF THIS DOES NOT MATCH: the curated table's `control`/`responder_targetproduct`
--   diverge from the dashboard's own population or detection logic, and
--   vbu_vintage_datalab.sql's numbers need a caveat before they go in front of
--   anyone — do not silently trust the curve shape.

-- Check 1: exact single day, response_start = 2026-04-13.
SELECT
    control                                                                  AS arm
  , COUNT(DISTINCT clnt_no)                                                  AS leads
  , COUNT(DISTINCT CASE WHEN responder_targetproduct = 1
                        THEN clnt_no END)                                    AS target_responders
  , COUNT(DISTINCT CASE WHEN responder_anyproduct = 1
                        THEN clnt_no END)                                    AS any_responders
  , MIN(response_end)                                                        AS min_response_end
  , MAX(response_end)                                                        AS max_response_end
FROM DL_MR_PROD.cards_bizups_vbu_descresp_clnt
WHERE response_start = DATE '2026-04-13'
GROUP BY 1
ORDER BY 1;

-- Check 2: same, but the wave may span several days of response_start through
-- end of month. Pick whichever of check 1 / check 2 lands on 28,821 / 11,133.
SELECT
    control                                                                  AS arm
  , COUNT(DISTINCT clnt_no)                                                  AS leads
  , COUNT(DISTINCT CASE WHEN responder_targetproduct = 1
                        THEN clnt_no END)                                    AS target_responders
  , COUNT(DISTINCT CASE WHEN responder_anyproduct = 1
                        THEN clnt_no END)                                    AS any_responders
  , MIN(response_end)                                                        AS min_response_end
  , MAX(response_end)                                                        AS max_response_end
FROM DL_MR_PROD.cards_bizups_vbu_descresp_clnt
WHERE response_start BETWEEN DATE '2026-04-13' AND DATE '2026-04-30'
GROUP BY 1
ORDER BY 1;
