-- ============================================================================
-- PROBE: actual TACTIC_IDs per MNE ending in Q3 (2026-05-01 .. 2026-07-31)
-- ENGINE: Teradata-direct
-- Purpose: PCD and AUH vintage curves hard-code their tactic IDs:
--            pcd_vintage_*.sql  ->  tactic_id_parent = '2026111PCD'
--            auh_vintage_*.sql  ->  tactic_id IN ('2026042AUH','2026119AUH')
--          This lists the IDs that ACTUALLY ended in Q3 so those hard-coded
--          lists can be checked instead of assumed.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- BLOCK A - PCD and AUH only. Expect ~8 PCD rows + ~2 AUH rows.
-- Compare the auh ids against ('2026042AUH','2026119AUH') by eye.
-- Compare the pcd ids against parent '2026111PCD' - note PCD's curve keys on
-- tactic_id_parent, which may differ from tactic_id; if these ids do not
-- contain '2026111', the parent linkage needs checking separately.
-- ----------------------------------------------------------------------------
SELECT
      SUBSTR(TACTIC_ID, 8, 3)                                    AS mne
    , TACTIC_ID
    , MIN(TREATMT_STRT_DT)                                       AS strt_dt
    , MAX(TREATMT_END_DT)                                        AS end_dt
    , COUNT(DISTINCT CLNT_NO)                                    AS clients
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE TREATMT_STRT_DT >= DATE '2024-01-01'          -- 2024 data floor
  AND TREATMT_STRT_DT <  DATE '2026-08-01'
  AND TREATMT_END_DT  >= DATE '2026-05-01'          -- Q3 start
  AND TREATMT_END_DT  <  DATE '2026-08-01'          -- Q3 end (incl thru 7/31)
  AND SUBSTR(TACTIC_ID, 8, 3) IN ('PCD','AUH')
GROUP BY 1, 2
ORDER BY 1, 4
;


-- ----------------------------------------------------------------------------
-- BLOCK B - OPTIONAL. All 7 MNEs, ids only, no client counts.
-- 115 rows. Run only if you want the full Q3 deployment roster on file.
-- ----------------------------------------------------------------------------
SELECT
      SUBSTR(TACTIC_ID, 8, 3)                                    AS mne
    , TACTIC_ID
    , MIN(TREATMT_STRT_DT)                                       AS strt_dt
    , MAX(TREATMT_END_DT)                                        AS end_dt
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE TREATMT_STRT_DT >= DATE '2024-01-01'
  AND TREATMT_STRT_DT <  DATE '2026-08-01'
  AND TREATMT_END_DT  >= DATE '2026-05-01'
  AND TREATMT_END_DT  <  DATE '2026-08-01'
  AND SUBSTR(TACTIC_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH','VBA','VBU')
GROUP BY 1, 2
ORDER BY 1, 3
;
