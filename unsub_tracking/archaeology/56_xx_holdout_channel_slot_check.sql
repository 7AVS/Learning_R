-- 56: Quick check — is "channel slot = XX" the holdout flag? (Cards MNEs, Teradata-direct)
-- 2026-09-04: holdout/control cells carry no channel label (slot reads XX); action cells
-- carry the channel code (EM, MB, ...). If true, the channel slot is a universal arm flag and
-- TST_GRP_CD is never needed. Two blocks, run in order, both feed the same record-and-compare step.
-- Scope: CRV, PCL, PCQ, PCD, AUH, TREATMT_STRT_DT >= 2024-01-01. Grain = (CLNT_NO, TACTIC_ID).
-- =============================================================================

-- ===== BLOCK 1: what raw channel-slot values does each Cards MNE use? =====
-- QUESTION: per MNE, what are the top 5 raw values of the two channel-slot fields, and does XX
--   appear as one of them?
-- ROWS: <=25 (5 MNEs x top 5 value pairs)
-- GOOD LOOKS LIKE: every MNE shows an EM-bearing value AND an XX value; the XX rows carry
--   several test-group codes (holdouts exist inside many cells, not one)
-- WHAT TO DO WITH IT: record which raw values carry XX

SELECT
    SUBSTR(t.TACTIC_ID, 8, 3)                                       AS mne,
    TRIM(SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30))                 AS chnl_slot_vrb_info_121,
    TRIM(UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')))                 AS chnl_slot_addnl_data1,
    CAST(COUNT(*) AS BIGINT)                                        AS decisions,
    CAST(COUNT(DISTINCT t.CLNT_NO) AS BIGINT)                       AS distinct_clients,
    CAST(COUNT(DISTINCT TRIM(t.TST_GRP_CD)) AS BIGINT)              AS distinct_test_group_codes
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
  AND SUBSTR(t.TACTIC_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH')
GROUP BY 1, 2, 3
QUALIFY ROW_NUMBER() OVER (PARTITION BY SUBSTR(t.TACTIC_ID, 8, 3) ORDER BY COUNT(*) DESC) <= 5
ORDER BY 1, 4 DESC;


-- ===== BLOCK 2: do XX decisions ever get an email sent? =====
-- QUESTION: of decisions whose channel slot contains XX, how many reach the vendor send chain and
--   how many were actually sent (disposition_cd = 1)?
-- ROWS: 5 (one per MNE)
-- GOOD LOOKS LIKE: decisions_sent_disposition1 at or near zero for every MNE. Any MNE with real
--   sends under XX means XX is not (only) the holdout marker there.
-- WHAT TO DO WITH IT: record the result

WITH xx_decis AS (
    SELECT DISTINCT
        t.CLNT_NO,
        t.TACTIC_ID,
        SUBSTR(t.TACTIC_ID, 8, 3) AS mne
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2025-01-01'  -- 2024-01-01 full pull exhausted spool on 2026-09-04 (Pack 54); widen only after this pass completes
      AND SUBSTR(t.TACTIC_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH')
      AND (   TRIM(SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30)) LIKE '%XX%'
           OR TRIM(UPPER(COALESCE(t.ADDNL_DECISN_DATA1, ''))) LIKE '%XX%' )
),
-- v3.1 (matches Pack 54's spool fix): INNER JOIN to a derived distinct-tactic-id table
-- instead of an IN-subquery — the IN-subquery forces a dedupe-and-compare against every
-- MASTER/EVENT row scanned and burns spool at this population size.
in_master AS (
    SELECT DISTINCT m.TREATMENT_ID, m.CLNT_NO, m.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
    INNER JOIN (SELECT DISTINCT TACTIC_ID FROM xx_decis) x
        ON x.TACTIC_ID = m.TREATMENT_ID
    WHERE m.CLNT_NO IS NOT NULL
),
sent_events AS (
    SELECT DISTINCT e.TREATMENT_ID, e.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT TACTIC_ID FROM xx_decis) x
        ON x.TACTIC_ID = e.TREATMENT_ID
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2025-01-01'  -- 2024-01-01 full pull exhausted spool on 2026-09-04 (Pack 54); widen only after this pass completes
),
flags AS (
    SELECT
        d.mne, d.CLNT_NO, d.TACTIC_ID,
        MAX(CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)            AS f_in_master,
        MAX(CASE WHEN s.consumer_id_hashed IS NOT NULL THEN 1 ELSE 0 END) AS f_sent
    FROM xx_decis d
    LEFT JOIN in_master m
        ON  m.TREATMENT_ID = d.TACTIC_ID AND m.CLNT_NO = d.CLNT_NO
    LEFT JOIN sent_events s
        ON  s.TREATMENT_ID = d.TACTIC_ID AND s.consumer_id_hashed = m.consumer_id_hashed
    GROUP BY 1, 2, 3
)
SELECT
    mne,
    CAST(COUNT(*) AS BIGINT)                AS decisions_xx,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients_xx,
    CAST(SUM(f_in_master) AS BIGINT)        AS decisions_in_master,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent_disposition1
FROM flags
GROUP BY 1
ORDER BY 1;
