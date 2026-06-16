-- PCQ Modal Sales (MS) — Step 1 discovery on the tactic event table
-- Engine: Teradata-direct (matches the async tactic-event trackers)
-- Table: DG6V01.TACTIC_EVNT_IP_AR_HIST  (event grain: 1 row per impression/contact)
--
-- Goal of this step (confirm BEFORE building the two-hop):
--   1. Where does the "MS" channel actually sit in TACTIC_DECISN_VRB_INFO?
--      (mobile "MB" sits at chars 121-30 — confirm MS is the same segment)
--   2. Which PCQ TACTIC_IDs exist on/after Jun 1, and is MS one deployment or several
--   3. The distinct TST_GRP_CD (test group) values present
--   4. The real column names for impressions / score (Query 0)
-- TACTIC_ID positions 8-10 = MNE (campaign), e.g. '2026xxxPCQ'.


-- ============================================================================
-- QUERY 0: Column inventory — find the impressions / score / key columns
-- ============================================================================
SELECT ColumnName, ColumnType, ColumnLength
FROM DBC.ColumnsV
WHERE DatabaseName = 'DG6V01'
  AND TableName    = 'TACTIC_EVNT_IP_AR_HIST'
ORDER BY ColumnId;


-- ============================================================================
-- QUERY 1: PCQ Jun-1+ deployments — channel segment, MS flag, test groups
--   Shows the raw 121-30 channel slice so we can SEE the channel vocabulary
--   (MB, MS, ...) and verify MS's position before trusting the %MS% test.
--   Cross-check: ms_anywhere flags MS appearing ANYWHERE in the string, so a
--   mismatch between ms_seg_121_30 and ms_anywhere tells us the position is wrong.
-- ============================================================================
SELECT
    TACTIC_ID,
    SUBSTRING(TACTIC_ID, 8, 3)                                   AS mne,
    MIN(EVNT_STRT_DT)                                            AS first_evnt_dt,
    MAX(EVNT_STRT_DT)                                            AS last_evnt_dt,
    TST_GRP_CD,
    TRIM(SUBSTRING(TACTIC_DECISN_VRB_INFO, 121, 30))            AS chnl_seg_121_30,
    CASE WHEN SUBSTRING(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
         THEN 1 ELSE 0 END                                       AS ms_seg_121_30,
    CASE WHEN TACTIC_DECISN_VRB_INFO LIKE '%MS%'
         THEN 1 ELSE 0 END                                       AS ms_anywhere,
    COUNT(*)                                                     AS event_rows,
    COUNT(DISTINCT CLNT_NO)                                      AS clients
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE SUBSTRING(TACTIC_ID, 8, 3) = 'PCQ'
  AND EVNT_STRT_DT >= DATE '2026-06-01'
GROUP BY
    TACTIC_ID,
    SUBSTRING(TACTIC_ID, 8, 3),
    TST_GRP_CD,
    TRIM(SUBSTRING(TACTIC_DECISN_VRB_INFO, 121, 30)),
    CASE WHEN SUBSTRING(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%' THEN 1 ELSE 0 END,
    CASE WHEN TACTIC_DECISN_VRB_INFO LIKE '%MS%' THEN 1 ELSE 0 END
ORDER BY
    TACTIC_ID,
    ms_seg_121_30 DESC,
    TST_GRP_CD,
    chnl_seg_121_30;
