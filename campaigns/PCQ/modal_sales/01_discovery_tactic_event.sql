-- PCQ Modal Sales (MS) — Step 1 discovery on the tactic event table
-- Engine: Teradata-direct (matches the async tactic-event trackers)
-- Table: DG6V01.TACTIC_EVNT_IP_AR_HIST  (event grain: 1 row per impression/contact)
--
-- We do NOT have a schema for this table — run QUERY 0 first to confirm column
-- names (especially impressions / score). QUERY 1 uses only columns the async
-- trackers already proved exist, plus EVNT_STRT_DT / TST_GRP_CD; if either name
-- is off, swap it using QUERY 0's output.
--
-- Goal: confirm (1) MS is detectable at TACTIC_DECISN_VRB_INFO chars 121-30,
-- (2) which PCQ TACTIC_IDs run on/after Jun 1 and whether MS is one or many,
-- (3) the TST_GRP_CD values present.
-- TACTIC_ID positions 8-10 = MNE (campaign), e.g. '2026xxxPCQ'.


-- ============================================================================
-- QUERY 0: Column inventory — RUN FIRST (find impressions / score / key names)
-- ============================================================================
SELECT ColumnName, ColumnType, ColumnLength
FROM DBC.ColumnsV
WHERE DatabaseName = 'DG6V01'
  AND TableName    = 'TACTIC_EVNT_IP_AR_HIST'
ORDER BY ColumnId;


-- ============================================================================
-- QUERY 1: PCQ Jun-1+ — MS vs non-MS, by tactic and test group
--   Binary MS flag only (channel sits at chars 121-30, same slice as mobile MB).
-- ============================================================================
SELECT
    TACTIC_ID,
    SUBSTRING(TACTIC_ID, 8, 3)                                  AS mne,
    TST_GRP_CD,
    CASE WHEN SUBSTRING(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
         THEN 'MS' ELSE 'non-MS' END                            AS ms_flag,
    MIN(EVNT_STRT_DT)                                           AS first_evnt_dt,
    MAX(EVNT_STRT_DT)                                           AS last_evnt_dt,
    COUNT(*)                                                    AS event_rows,
    COUNT(DISTINCT CLNT_NO)                                     AS clients
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE SUBSTRING(TACTIC_ID, 8, 3) = 'PCQ'
  AND EVNT_STRT_DT >= DATE '2026-06-01'
GROUP BY
    TACTIC_ID,
    SUBSTRING(TACTIC_ID, 8, 3),
    TST_GRP_CD,
    CASE WHEN SUBSTRING(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
         THEN 'MS' ELSE 'non-MS' END
ORDER BY TACTIC_ID, ms_flag DESC, TST_GRP_CD;
