-- O2P async: tactic ID -> deployment cycle mapping + per-cycle volumes
-- Feeds the cadence/volume [ASK] in o2p_async_holdout_doe_draft.md (Section 4).
-- Engine: Starburst federation (Trino syntax).
SELECT tactic_id,
       TRIM(tst_grp_cd) AS tg,
       MIN(treatmt_strt_dt) AS first_start,
       MAX(treatmt_strt_dt) AS last_start,
       COUNT(DISTINCT clnt_no) AS clients,
       -- Mobile = MB (Andre 2026-09-04). '%IM%' is a CRV backend quirk, NOT mobile —
       -- the legacy O2P trackers used it by mistake. Two MB variants counted so the
       -- reconciliation can settle which column carries the flag for O2P.
       COUNT(DISTINCT CASE WHEN TRIM(tactic_cell_cd) LIKE '%MB%' THEN clnt_no END) AS mobile_cellcd_mb,
       COUNT(DISTINCT CASE WHEN SUBSTR(tactic_decisn_vrb_info, 121, 30) LIKE '%MB%' THEN clnt_no END) AS mobile_vrb_mb,
       COUNT(DISTINCT CASE WHEN TRIM(tactic_cell_cd) LIKE '%IM%' THEN clnt_no END) AS im_cells_crv_quirk
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE tactic_id IN ('2026099O2P', '2026126O2P', '2026132O2P')
GROUP BY 1, 2
ORDER BY 1, 2
