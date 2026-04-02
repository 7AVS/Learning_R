-- =============================================================================
-- PCD Tactic Event — TACTIC_DECISN_VRB_INFO Field Mapping
-- =============================================================================
--
-- Purpose:
--   Reverse-engineer the byte positions within TACTIC_DECISN_VRB_INFO
--   for PCD tactic records. Find where strategy_seg_cd and other fields
--   are packed within this 150-byte string.
--
-- Known positions (from AUH):
--   Position 21, length 3 = product code (PLT, CLO, MC1, MCP, VPR)
--   All other positions = unknown for PCD
--
-- Approach:
--   1. Pull raw TACTIC_DECISN_VRB_INFO for PCD records
--   2. Cross-reference with PCD decision/response table (which has
--      strategy_seg_cd as a dedicated column)
--   3. Find where strategy_seg_cd value appears in the 150-byte string
--
-- Tables:
--   DTZV01.TACTIC_EVNT_IP_AR_H60M (tactic event)
--   dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp (PCD decision/response)
--
-- =============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 1: Raw TACTIC_DECISN_VRB_INFO for PCD records
-- ---------------------------------------------------------------------------
-- Pull the full verbose info string for PCD tactic events.
-- Look at the raw values to understand the structure.
-- Also extract known position 21-23 (product code) as a sanity check.
-- ---------------------------------------------------------------------------

SELECT
    TACTIC_ID,
    TST_GRP_CD,
    RPT_GRP_CD,
    TACTIC_CELL_CD,
    TACTIC_DECISN_VRB_INFO,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 1, 10)   AS pos_01_10,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 11, 10)  AS pos_11_20,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 10)  AS pos_21_30,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 31, 10)  AS pos_31_40,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 41, 10)  AS pos_41_50,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 51, 10)  AS pos_51_60,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 61, 10)  AS pos_61_70,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 71, 10)  AS pos_71_80,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 81, 10)  AS pos_81_90,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 91, 10)  AS pos_91_100,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 101, 10) AS pos_101_110,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 111, 10) AS pos_111_120,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 10) AS pos_121_130,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 131, 10) AS pos_131_140,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 141, 10) AS pos_141_150,
    TREATMT_STRT_DT,
    COUNT(DISTINCT CLNT_NO)                  AS unique_clients
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE
    SUBSTR(TACTIC_ID, 8, 3) = 'PCD'
    AND TREATMT_STRT_DT >= DATE '2025-01-01'
GROUP BY
    TACTIC_ID,
    TST_GRP_CD,
    RPT_GRP_CD,
    TACTIC_CELL_CD,
    TACTIC_DECISN_VRB_INFO,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 1, 10),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 11, 10),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 10),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 31, 10),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 41, 10),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 51, 10),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 61, 10),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 71, 10),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 81, 10),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 91, 10),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 101, 10),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 111, 10),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 10),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 131, 10),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 141, 10),
    TREATMT_STRT_DT
ORDER BY TREATMT_STRT_DT DESC, unique_clients DESC;


-- ---------------------------------------------------------------------------
-- QUERY 2: Cross-reference with PCD decision/response table
-- ---------------------------------------------------------------------------
-- Join tactic event to PCD decision/response on CLNT_NO to find where
-- strategy_seg_cd appears within TACTIC_DECISN_VRB_INFO.
--
-- Pull both the raw verbose string and the dedicated strategy columns
-- side by side so we can visually locate the strategy code.
-- ---------------------------------------------------------------------------

SELECT
    t.TACTIC_ID,
    t.TST_GRP_CD,
    t.RPT_GRP_CD,
    t.TACTIC_DECISN_VRB_INFO,
    d.strategy_seg_cd,
    d.strtgy_seg_desc,
    d.act_ctl_seg,
    d.test_value,
    d.test_description,
    d.mnemonic,
    d.tactic_id_parent,
    -- Check if strategy_seg_cd appears anywhere in the verbose string
    POSITION(TRIM(d.strategy_seg_cd) IN t.TACTIC_DECISN_VRB_INFO) AS strategy_position,
    -- Extract known chunks for comparison
    SUBSTR(TACTIC_DECISN_VRB_INFO, 1, 8)    AS pos_01_08,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 9, 8)    AS pos_09_16,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 17, 8)   AS pos_17_24,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 25, 8)   AS pos_25_32,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 33, 8)   AS pos_33_40,
    SUBSTR(TACTIC_DECISN_VRB_INFO, 41, 8)   AS pos_41_48,
    COUNT(DISTINCT t.CLNT_NO)                AS matched_clients
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M t
INNER JOIN dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp d
    ON t.CLNT_NO = d.clnt_no
WHERE
    SUBSTR(t.TACTIC_ID, 8, 3) = 'PCD'
    AND t.TREATMT_STRT_DT >= DATE '2025-01-01'
    AND d.response_start >= DATE '2025-01-01'
GROUP BY
    t.TACTIC_ID,
    t.TST_GRP_CD,
    t.RPT_GRP_CD,
    t.TACTIC_DECISN_VRB_INFO,
    d.strategy_seg_cd,
    d.strtgy_seg_desc,
    d.act_ctl_seg,
    d.test_value,
    d.test_description,
    d.mnemonic,
    d.tactic_id_parent,
    POSITION(TRIM(d.strategy_seg_cd) IN t.TACTIC_DECISN_VRB_INFO),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 1, 8),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 9, 8),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 17, 8),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 25, 8),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 33, 8),
    SUBSTR(TACTIC_DECISN_VRB_INFO, 41, 8)
ORDER BY matched_clients DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- QUERY 3: Also pull ADDNL_DECISN_DATA1 for PCD
-- ---------------------------------------------------------------------------
-- The other verbose field — may contain channel info or other useful data.
-- Same approach: slice into chunks and inspect.
-- ---------------------------------------------------------------------------

SELECT
    TACTIC_ID,
    TST_GRP_CD,
    RPT_GRP_CD,
    ADDNL_DECISN_DATA1,
    SUBSTR(ADDNL_DECISN_DATA1, 1, 10)   AS adnl_pos_01_10,
    SUBSTR(ADDNL_DECISN_DATA1, 11, 10)  AS adnl_pos_11_20,
    SUBSTR(ADDNL_DECISN_DATA1, 21, 10)  AS adnl_pos_21_30,
    SUBSTR(ADDNL_DECISN_DATA1, 31, 10)  AS adnl_pos_31_40,
    SUBSTR(ADDNL_DECISN_DATA1, 41, 10)  AS adnl_pos_41_50,
    TREATMT_STRT_DT,
    COUNT(DISTINCT CLNT_NO)              AS unique_clients
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE
    SUBSTR(TACTIC_ID, 8, 3) = 'PCD'
    AND TREATMT_STRT_DT >= DATE '2025-01-01'
GROUP BY
    TACTIC_ID,
    TST_GRP_CD,
    RPT_GRP_CD,
    ADDNL_DECISN_DATA1,
    SUBSTR(ADDNL_DECISN_DATA1, 1, 10),
    SUBSTR(ADDNL_DECISN_DATA1, 11, 10),
    SUBSTR(ADDNL_DECISN_DATA1, 21, 10),
    SUBSTR(ADDNL_DECISN_DATA1, 31, 10),
    SUBSTR(ADDNL_DECISN_DATA1, 41, 10),
    TREATMT_STRT_DT
ORDER BY TREATMT_STRT_DT DESC, unique_clients DESC;


-- ---------------------------------------------------------------------------
-- NOTES
-- ---------------------------------------------------------------------------
-- After running these queries:
-- 1. Query 1 output: Look at the 10-byte chunks to see the structure
-- 2. Query 2 output: The strategy_position column tells you EXACTLY where
--    strategy_seg_cd sits in TACTIC_DECISN_VRB_INFO (byte position)
-- 3. Query 3 output: Map what's in ADDNL_DECISN_DATA1
-- 4. Document all findings in ga4_ecommerce_field_mapping.sql or a new
--    dedicated tactic field mapping reference file
--
-- Once we know the byte position of strategy_seg_cd, we can:
--   - Add SUBSTR(TACTIC_DECISN_VRB_INFO, <pos>, <len>) to the production
--     tracker CTE to filter by specific experiment
--   - Use this instead of (or in addition to) the mnemonic filter
-- ---------------------------------------------------------------------------
