-- 54: Email-decisioned send funnel — Cards MNEs only (CRV, PCL, PCQ, PCD, AUH)
-- v3 2026-09-04: arms from channel slot (XX = holdout), TG-code mapping removed.
-- v3.1 2026-09-04: spool fix — volatile tables live in spool; Step A restricted to two arms
-- + window param, tactic-id driver table for MASTER/EVENT joins.
-- ENGINE: Teradata-direct.
-- PURPOSE: of EMAIL_ACTION and HOLDOUT_XX decisions on these five Cards MNEs, how many reach
-- the vendor send chain vs how many are actually sent (HOLDOUT_XX sends should be ~zero).
-- RUN ORDER: Step 0 (reset) -> Step A (volatile, two arms only) -> Step A2 (tactic-id driver
-- table) -> SIZE CHECK -> Step B (volatile MASTER match) -> Step C (volatile SENT match) ->
-- Block 0 (action-vs-holdout proof) -> Block 1 (EMAIL_ACTION funnel) -> Block 2 (HOLDOUT_XX
-- funnel) -> Block 3 (full cube).
-- Bank-wide was attempted 2026-09-04 and ran out of spool; Cards-only by design. The
-- all-arms, full-2024 version of Step A also ran out of spool at 110M rows (2026-09-04) —
-- fixed here by restricting to two arms and swapping every downstream IN-subquery for a
-- join against a small tactic-id driver table.
-- Grain = (CLNT_NO, TACTIC_ID) throughout — one row per decision, not per client.
-- Counts only, no rates. VOLATILE TABLEs persist in-session — Step 0 resets them.
-- =============================================================================

-- STEP 0 removed 2026-09-04: no DROP statements in packs. If you rerun a pack in the same
-- session and hit 'table already exists', run 00_reset_volatiles.sql first.


-- ===== PARAMETER BLOCK 1: MNE SCOPE + WINDOW START =====
-- MNE scope — Cards personal MNEs, exactly as Pack 17 scoped them:
-- archaeology/17_em_decision_vendor_coverage.sql -> SUBSTR(t.TACTIC_ID,8,3) IN ('CRV','PCL','PCQ','PCD','AUH')
-- Window start — DATE '2025-01-01' for this pass. 2024-01-01 full pull = 110M rows and
-- exhausted spool on 2026-09-04; widen only after this pass completes successfully.
-- Both values are written directly into Step A (and the window into Step C) below — Teradata
-- has no shared variable across statements in a plain SQL script, so edit both places together
-- if either parameter changes; this block is the documentation copy, not a live variable.


-- ===== STEP A: VOLATILE — EMAIL_ACTION + HOLDOUT_XX decisions for the Cards MNEs =====
-- DROP TABLE vt_em_decis_cards;  -- also see STEP 0; kept here too in case this step alone reruns
--
-- v3.1: restricted to the two arms this file measures (2024-01-01, all-arms pull was 110M rows
-- and exhausted spool). Raw chnl_slot_vrb/chnl_slot_addnl columns are dropped — that raw-value
-- profiling now lives in Pack 56 Block 1. CASE lives in a derived table; the outer query
-- filters on its result so the arm logic is written once.

CREATE VOLATILE TABLE vt_em_decis_cards AS (
    SELECT
        CLNT_NO,
        TACTIC_ID,
        mne,
        cohort_yyyymm,
        arm_from_channel
    FROM (
        SELECT DISTINCT
            t.CLNT_NO,
            t.TACTIC_ID,
            SUBSTR(t.TACTIC_ID, 8, 3)                                AS mne,
            EXTRACT(YEAR FROM t.TREATMT_STRT_DT) * 100
              + EXTRACT(MONTH FROM t.TREATMT_STRT_DT)                AS cohort_yyyymm,
            CASE
                -- 2026-09-04: holdout = channel slot XX; action = channel code; no TG codes
                WHEN SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
                  OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%'
                    THEN 'EMAIL_ACTION'
                WHEN SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%XX%'
                  OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%XX%'
                    THEN 'HOLDOUT_XX'
                WHEN TRIM(SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30)) = ''
                 AND TRIM(UPPER(COALESCE(t.ADDNL_DECISN_DATA1, ''))) = ''
                    THEN 'NO_CHANNEL_BLANK'
                ELSE 'OTHER_CHANNEL'
            END                                                      AS arm_from_channel
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
        WHERE t.TREATMT_STRT_DT >= DATE '2025-01-01'                          -- PARAMETER BLOCK 1: window start
          AND SUBSTR(t.TACTIC_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH')     -- PARAMETER BLOCK 1: MNE scope
    ) tagged
    WHERE arm_from_channel IN ('EMAIL_ACTION', 'HOLDOUT_XX')
) WITH DATA
PRIMARY INDEX (TACTIC_ID, CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_em_decis_cards COLUMN (TACTIC_ID, CLNT_NO);


-- ===== STEP A2: VOLATILE — distinct tactic ids from Step A (join driver for Steps B/C) =====
-- DROP TABLE vt_tactic_ids;  -- also see STEP 0

CREATE VOLATILE TABLE vt_tactic_ids AS (
    SELECT DISTINCT TACTIC_ID FROM vt_em_decis_cards
) WITH DATA
PRIMARY INDEX (TACTIC_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_tactic_ids COLUMN (TACTIC_ID);


-- ===== SIZE CHECK: how big is the population before we join it? =====
-- QUESTION: how big is the population before we join it?
-- ROWS: 3
-- GOOD LOOKS LIKE: a few tens of millions at most
-- WHAT TO DO WITH IT: record the result

SELECT
    CAST(arm_from_channel AS VARCHAR(20))     AS arm_from_channel,
    CAST(COUNT(*) AS BIGINT)                  AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT)   AS distinct_clients,
    CAST(COUNT(DISTINCT TACTIC_ID) AS BIGINT) AS distinct_tactics
FROM vt_em_decis_cards
GROUP BY 1
UNION ALL
SELECT
    CAST('TOTAL' AS VARCHAR(20))              AS arm_from_channel,
    CAST(COUNT(*) AS BIGINT)                  AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT)   AS distinct_clients,
    CAST(COUNT(DISTINCT TACTIC_ID) AS BIGINT) AS distinct_tactics
FROM vt_em_decis_cards
ORDER BY 1;


-- ===== STEP B: VOLATILE — MASTER match, restricted via join to Step A2's tactic ids =====
-- DROP TABLE vt_master_cards;  -- also see STEP 0
-- v3.1: INNER JOIN to vt_tactic_ids instead of an IN-subquery. The IN-subquery forced a
-- 110M-row dedupe-and-compare against every MASTER row scanned and burned spool; the join
-- lets the optimizer use vt_tactic_ids' own (small, stats-collected) footprint instead.

CREATE VOLATILE TABLE vt_master_cards AS (
    SELECT DISTINCT
        m.TREATMENT_ID,
        m.CLNT_NO,
        m.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
    INNER JOIN vt_tactic_ids x
        ON x.TACTIC_ID = m.TREATMENT_ID
    WHERE m.CLNT_NO IS NOT NULL
) WITH DATA
PRIMARY INDEX (TREATMENT_ID, CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_master_cards COLUMN (TREATMENT_ID, CLNT_NO);


-- ===== STEP C: VOLATILE — SENT (disposition_cd=1) match, restricted the same way =====
-- DROP TABLE vt_sent_cards;  -- also see STEP 0

CREATE VOLATILE TABLE vt_sent_cards AS (
    SELECT DISTINCT
        e.TREATMENT_ID,
        e.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN vt_tactic_ids x
        ON x.TACTIC_ID = e.TREATMENT_ID
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2025-01-01'  -- PARAMETER BLOCK 1: window start
) WITH DATA
PRIMARY INDEX (TREATMENT_ID, consumer_id_hashed)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_sent_cards COLUMN (TREATMENT_ID, consumer_id_hashed);


-- ===== BLOCK 0: does the channel slot separate action from holdout? =====
-- QUESTION: does the channel slot separate action from holdout, per MNE?
-- ROWS: 10 (5 Cards MNEs x 2 arms)
-- GOOD LOOKS LIKE: HOLDOUT_XX rows show sends at or near zero; EMAIL_ACTION sends close to
--   in_master
-- WHAT TO DO WITH IT: record the result

WITH flags AS (
    SELECT
        d.mne,
        d.arm_from_channel,
        d.CLNT_NO,
        d.TACTIC_ID,
        MAX(CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)            AS f_in_master,
        MAX(CASE WHEN s.consumer_id_hashed IS NOT NULL THEN 1 ELSE 0 END) AS f_sent
    FROM vt_em_decis_cards d
    LEFT JOIN vt_master_cards m
        ON  m.TREATMENT_ID = d.TACTIC_ID
        AND m.CLNT_NO       = d.CLNT_NO
    LEFT JOIN vt_sent_cards s
        ON  s.TREATMENT_ID       = d.TACTIC_ID
        AND s.consumer_id_hashed = m.consumer_id_hashed
    GROUP BY 1, 2, 3, 4
)
SELECT
    mne,
    arm_from_channel,
    CAST(COUNT(*) AS BIGINT)                AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_in_master) AS BIGINT)        AS decisions_in_master,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent_disposition1
FROM flags
GROUP BY 1, 2
ORDER BY 1, 2;


-- ===== BLOCK 1: funnel by MNE — EMAIL_ACTION rows only, plus one TOTAL row =====
-- QUESTION: of email-action decisions on these five MNEs, how many show up in the vendor
--   send chain, and how many were actually sent?
-- ROWS: 6 (5 Cards MNEs + 1 TOTAL row)
-- GOOD LOOKS LIKE: decisions_sent_disposition1 close to decisions_in_master — Pack 17 found
--   these nearly equal for these same five MNEs
-- WHAT TO DO WITH IT: record the result

WITH flags AS (
    SELECT
        d.mne,
        d.CLNT_NO,
        d.TACTIC_ID,
        MAX(CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)            AS f_in_master,
        MAX(CASE WHEN s.consumer_id_hashed IS NOT NULL THEN 1 ELSE 0 END) AS f_sent
    FROM vt_em_decis_cards d
    LEFT JOIN vt_master_cards m
        ON  m.TREATMENT_ID = d.TACTIC_ID
        AND m.CLNT_NO       = d.CLNT_NO
    LEFT JOIN vt_sent_cards s
        ON  s.TREATMENT_ID       = d.TACTIC_ID
        AND s.consumer_id_hashed = m.consumer_id_hashed
    WHERE d.arm_from_channel = 'EMAIL_ACTION'
    GROUP BY 1, 2, 3
)
SELECT
    CAST(mne AS VARCHAR(20))                AS mne,
    CAST(COUNT(*) AS BIGINT)                AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_in_master) AS BIGINT)        AS decisions_in_master,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent_disposition1
FROM flags
GROUP BY 1
UNION ALL
SELECT
    CAST('TOTAL' AS VARCHAR(20))            AS mne,
    CAST(COUNT(*) AS BIGINT)                AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_in_master) AS BIGINT)        AS decisions_in_master,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent_disposition1
FROM flags
ORDER BY 1;


-- ===== BLOCK 2: funnel by MNE — HOLDOUT_XX rows only, plus one TOTAL row =====
-- QUESTION: of holdout (channel slot XX) decisions on these five MNEs, how many show up in
--   the vendor send chain, and how many were actually sent?
-- ROWS: 6 (5 Cards MNEs + 1 TOTAL row)
-- GOOD LOOKS LIKE: decisions_sent_disposition1 near zero — holdout cells should not receive
--   the email
-- WHAT TO DO WITH IT: record the result

WITH flags AS (
    SELECT
        d.mne,
        d.CLNT_NO,
        d.TACTIC_ID,
        MAX(CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)            AS f_in_master,
        MAX(CASE WHEN s.consumer_id_hashed IS NOT NULL THEN 1 ELSE 0 END) AS f_sent
    FROM vt_em_decis_cards d
    LEFT JOIN vt_master_cards m
        ON  m.TREATMENT_ID = d.TACTIC_ID
        AND m.CLNT_NO       = d.CLNT_NO
    LEFT JOIN vt_sent_cards s
        ON  s.TREATMENT_ID       = d.TACTIC_ID
        AND s.consumer_id_hashed = m.consumer_id_hashed
    WHERE d.arm_from_channel = 'HOLDOUT_XX'
    GROUP BY 1, 2, 3
)
SELECT
    CAST(mne AS VARCHAR(20))                AS mne,
    CAST(COUNT(*) AS BIGINT)                AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_in_master) AS BIGINT)        AS decisions_in_master,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent_disposition1
FROM flags
GROUP BY 1
UNION ALL
SELECT
    CAST('TOTAL' AS VARCHAR(20))            AS mne,
    CAST(COUNT(*) AS BIGINT)                AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_in_master) AS BIGINT)        AS decisions_in_master,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent_disposition1
FROM flags
ORDER BY 1;


-- ===== BLOCK 3: full cube — MNE x cohort_month x arm_from_channel (both arms) =====
-- QUESTION: same funnel, broken out by MNE x month x channel-slot arm, for self-service
--   slicing
-- ROWS: many (5 MNEs x ~8-12 months x 2 arms — roughly 60-120)
-- GOOD LOOKS LIKE: EMAIL_ACTION and HOLDOUT_XX both show continuous monthly coverage per
--   MNE; HOLDOUT_XX sends stay near zero every month, not just on average
-- WHAT TO DO WITH IT: save to Excel, do not paste

WITH flags AS (
    SELECT
        d.mne,
        d.cohort_yyyymm,
        d.arm_from_channel,
        d.CLNT_NO,
        d.TACTIC_ID,
        MAX(CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)            AS f_in_master,
        MAX(CASE WHEN s.consumer_id_hashed IS NOT NULL THEN 1 ELSE 0 END) AS f_sent
    FROM vt_em_decis_cards d
    LEFT JOIN vt_master_cards m
        ON  m.TREATMENT_ID = d.TACTIC_ID
        AND m.CLNT_NO       = d.CLNT_NO
    LEFT JOIN vt_sent_cards s
        ON  s.TREATMENT_ID       = d.TACTIC_ID
        AND s.consumer_id_hashed = m.consumer_id_hashed
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    mne,
    cohort_yyyymm,
    arm_from_channel,
    CAST(COUNT(*) AS BIGINT)                AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_in_master) AS BIGINT)        AS decisions_in_master,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent_disposition1
FROM flags
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
