-- ============================================================================
-- PROBE: deployments per MNE ending in a date window
-- ENGINE: Teradata-direct  (TOP not LIMIT, DATE literals, no catalog prefix)
-- Table:  DTZV01.TACTIC_EVNT_IP_AR_H60M   (60-month rolling tactic event)
-- Grain:  TACTIC_ID = one deployment/wave  -> COUNT(DISTINCT TACTIC_ID)
-- MNE:    SUBSTR(TACTIC_ID, 8, 3)   (positions 8-10, repo-wide convention)
--
-- ASSUMPTION (flagged, change if wrong): window year = 2026.
--   "May 1 - Jul 31" read as 2026-05-01 .. 2026-07-31 inclusive.
--   To change the year, edit the 4 date literals marked <<WINDOW>> below.
--
-- CAVEAT: TREATMT_END_DT is documented-verified (UNSUB_TRACKING_KNOWLEDGE.md
--   :142-144) but I have not seen it used in a WHERE clause anywhere in the
--   repo. BLOCK 0 proves it exists and is populated BEFORE any count is
--   trusted. Do not skip Block 0.
--
-- CAVEAT: a second table variant exists in the wild -
--   DG6V01.TACTIC_EVNT_IP_AR_HIST. If H60M returns nothing, retry Block 0
--   against that one before concluding "no deployments".
-- ============================================================================


-- ----------------------------------------------------------------------------
-- BLOCK 0 - PROVE THE COLUMN. Run this alone first. 1 row.
-- Expect: end_dt_populated close to total_rows, and max_end_dt >= 2026-07-31.
-- If end_dt_populated is 0 or tiny -> STOP. TREATMT_END_DT is not usable and
-- the whole probe needs a different anchor. Do not run Blocks 1-3.
-- ----------------------------------------------------------------------------
SELECT
      COUNT(*)                                                   AS total_rows
    , COUNT(TREATMT_END_DT)                                      AS end_dt_populated
    , COUNT(*) - COUNT(TREATMT_END_DT)                           AS end_dt_null
    , MIN(TREATMT_STRT_DT)                                       AS min_start_dt
    , MAX(TREATMT_STRT_DT)                                       AS max_start_dt
    , MIN(TREATMT_END_DT)                                        AS min_end_dt
    , MAX(TREATMT_END_DT)                                        AS max_end_dt
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE TREATMT_STRT_DT >= DATE '2024-01-01'          -- 2024 data floor (hard rule)
  AND TREATMT_STRT_DT <  DATE '2026-08-01'          -- <<WINDOW>> prune upper bound
  AND SUBSTR(TACTIC_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH','VBA','VBU')
;


-- ----------------------------------------------------------------------------
-- BLOCK 1 - THE ANSWER. Deployments per MNE ending in the window. 7 rows max.
-- ----------------------------------------------------------------------------
SELECT
      SUBSTR(TACTIC_ID, 8, 3)                                    AS mne
    , COUNT(DISTINCT TACTIC_ID)                                  AS deployments
    , COUNT(DISTINCT CLNT_NO)                                    AS clients
    , MIN(TREATMT_STRT_DT)                                       AS earliest_start
    , MAX(TREATMT_STRT_DT)                                       AS latest_start
    , MIN(TREATMT_END_DT)                                        AS earliest_end
    , MAX(TREATMT_END_DT)                                        AS latest_end
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE TREATMT_STRT_DT >= DATE '2024-01-01'          -- 2024 data floor + pruning
  AND TREATMT_STRT_DT <  DATE '2026-08-01'          -- <<WINDOW>> a deployment
                                                    -- ending in the window
                                                    -- cannot start after it
  AND TREATMT_END_DT  >= DATE '2026-05-01'          -- <<WINDOW>> start
  AND TREATMT_END_DT  <  DATE '2026-08-01'          -- <<WINDOW>> end (excl 8/1
                                                    -- = incl through 7/31)
  AND SUBSTR(TACTIC_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH','VBA','VBU')
GROUP BY 1
ORDER BY deployments DESC
;


-- ----------------------------------------------------------------------------
-- BLOCK 2 - Same counts split by end month. 21 rows max.
-- Tells you WHERE in the window the deployments land - a curve anchored on a
-- month with 1 deployment is not the same product as one with 12.
-- ----------------------------------------------------------------------------
SELECT
      SUBSTR(TACTIC_ID, 8, 3)                                    AS mne
    , CAST(TREATMT_END_DT AS DATE FORMAT 'YYYY-MM')              AS end_month
    , COUNT(DISTINCT TACTIC_ID)                                  AS deployments
    , COUNT(DISTINCT CLNT_NO)                                    AS clients
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE TREATMT_STRT_DT >= DATE '2024-01-01'
  AND TREATMT_STRT_DT <  DATE '2026-08-01'          -- <<WINDOW>>
  AND TREATMT_END_DT  >= DATE '2026-05-01'          -- <<WINDOW>>
  AND TREATMT_END_DT  <  DATE '2026-08-01'          -- <<WINDOW>>
  AND SUBSTR(TACTIC_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH','VBA','VBU')
GROUP BY 1, 2
ORDER BY 1, 2
;


-- ----------------------------------------------------------------------------
-- BLOCK 3 - THE BLIND SPOT. Deployments that STARTED before Aug 1 but have NO
-- end date (always-on / open-ended). These are invisible to Blocks 1-2 and
-- would otherwise be silently dropped from any "ended in window" count.
-- 7 rows max. If a campaign shows a large number here, an end-date-anchored
-- window is the wrong lens for it.
-- ----------------------------------------------------------------------------
SELECT
      SUBSTR(TACTIC_ID, 8, 3)                                    AS mne
    , COUNT(DISTINCT TACTIC_ID)                                  AS open_ended_deployments
    , COUNT(DISTINCT CLNT_NO)                                    AS clients
    , MIN(TREATMT_STRT_DT)                                       AS earliest_start
    , MAX(TREATMT_STRT_DT)                                       AS latest_start
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE TREATMT_STRT_DT >= DATE '2024-01-01'
  AND TREATMT_STRT_DT <  DATE '2026-08-01'          -- <<WINDOW>>
  AND TREATMT_END_DT IS NULL
  AND SUBSTR(TACTIC_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH','VBA','VBU')
GROUP BY 1
ORDER BY open_ended_deployments DESC
;
