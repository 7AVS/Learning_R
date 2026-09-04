-- 54: Email-decisioned send funnel — Cards MNEs only (CRV, PCL, PCQ, PCD, AUH)
-- v3 2026-09-04: arms from channel slot (XX = holdout), TG-code mapping removed per Andre.
-- ENGINE: Teradata-direct.
-- PURPOSE: of ALL decisions on these five Cards MNEs since 2024-01-01, tag each by its
-- channel slot (EM = email action, XX = holdout/no-contact, blank, or other) and see how
-- many action-tagged decisions reach the vendor send chain vs how many holdout-tagged ones
-- do (should be ~zero sent).
-- RUN ORDER: Step A (volatile decisioned pop, channel-tagged) -> Step B (volatile MASTER
-- match) -> Step C (volatile SENT match) -> Block 0 (channel-slot proof) -> Block 1
-- (EMAIL_ACTION funnel by MNE) -> Block 2 (HOLDOUT_XX funnel by MNE) -> Block 3 (full cube).
-- Bank-wide was attempted 2026-09-04 and ran out of spool; Cards-only by design.
-- Grain = (CLNT_NO, TACTIC_ID) throughout — one row per decision, not per client.
-- Counts only, no rates. VOLATILE TABLEs persist in-session — each step's DROP line is
-- commented at the top of the step; uncomment and run it alone if a prior run failed midway.
-- =============================================================================

-- ===== PARAMETER BLOCK 1: MNE SCOPE (from Pack 17 — do not guess, do not add MNEs here) =====
-- Cards personal MNEs, exactly as Pack 17 scoped them:
-- archaeology/17_em_decision_vendor_coverage.sql -> SUBSTR(t.TACTIC_ID,8,3) IN ('CRV','PCL','PCQ','PCD','AUH')
-- The list is written directly into Step A's WHERE clause below. Edit it in ONE place — Step A —
-- if the scope ever changes; this block is the documentation copy, not a live variable.


-- ===== STEP A: VOLATILE — ALL decisions for the Cards MNEs, tagged by channel slot =====
-- DROP TABLE vt_em_decis_cards;  -- uncomment + run alone if rerunning after a failed pass
--
-- No channel filter in the WHERE clause (v3 change) — every decision on these five MNEs is
-- pulled, then classified below. XX-match uses LIKE '%XX%', the same substring style as the
-- documented EM rule (UNSUB_TRACKING_KNOWLEDGE.md §6: `SUBSTR(...,121,30) LIKE '%EM%'`), not
-- exact equality — see the report for the evidence this choice is based on.

CREATE VOLATILE TABLE vt_em_decis_cards AS (
    SELECT DISTINCT
        t.CLNT_NO,
        t.TACTIC_ID,
        SUBSTR(t.TACTIC_ID, 8, 3)                                    AS mne,
        EXTRACT(YEAR FROM t.TREATMT_STRT_DT) * 100
          + EXTRACT(MONTH FROM t.TREATMT_STRT_DT)                    AS cohort_yyyymm,
        TRIM(SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30))              AS chnl_slot_vrb,
        TRIM(UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')))              AS chnl_slot_addnl,
        CASE
            -- Andre 2026-09-04: holdout = channel slot XX; action = channel code; no TG codes
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
        END                                                          AS arm_from_channel
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
      AND SUBSTR(t.TACTIC_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH')  -- PARAMETER BLOCK 1
) WITH DATA
PRIMARY INDEX (TACTIC_ID, CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_em_decis_cards COLUMN (TACTIC_ID, CLNT_NO);


-- ===== STEP B: VOLATILE — MASTER match, restricted to Step A's tactic ids =====
-- DROP TABLE vt_master_cards;  -- uncomment + run alone if rerunning after a failed pass

CREATE VOLATILE TABLE vt_master_cards AS (
    SELECT DISTINCT
        m.TREATMENT_ID,
        m.CLNT_NO,
        m.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
    WHERE m.CLNT_NO IS NOT NULL
      AND m.TREATMENT_ID IN (SELECT TACTIC_ID FROM vt_em_decis_cards)
) WITH DATA
PRIMARY INDEX (TREATMENT_ID, CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_master_cards COLUMN (TREATMENT_ID, CLNT_NO);


-- ===== STEP C: VOLATILE — SENT (disposition_cd=1) match, restricted the same way =====
-- DROP TABLE vt_sent_cards;  -- uncomment + run alone if rerunning after a failed pass

CREATE VOLATILE TABLE vt_sent_cards AS (
    SELECT DISTINCT
        e.TREATMENT_ID,
        e.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2024-01-01'
      AND e.TREATMENT_ID IN (SELECT TACTIC_ID FROM vt_em_decis_cards)
) WITH DATA
PRIMARY INDEX (TREATMENT_ID, consumer_id_hashed)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_sent_cards COLUMN (TREATMENT_ID, consumer_id_hashed);


-- ===== BLOCK 0: does the channel slot separate action from holdout? =====
-- QUESTION: Does the channel slot separate action from holdout? Which raw values does each
--   Cards MNE use?
-- ROWS: <=30 (5 Cards MNEs x top 6 combinations each)
-- GOOD LOOKS LIKE: HOLDOUT_XX rows have decisions_sent_disposition1 at or near zero;
--   EMAIL_ACTION rows have sends close to decisions_in_master
-- WHAT TO DO WITH IT: paste to Claude

WITH flags AS (
    SELECT
        d.mne,
        d.arm_from_channel,
        d.chnl_slot_vrb,
        d.chnl_slot_addnl,
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
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
    mne,
    arm_from_channel,
    chnl_slot_vrb,
    chnl_slot_addnl,
    CAST(COUNT(*) AS BIGINT)                AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_in_master) AS BIGINT)        AS decisions_in_master,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent_disposition1
FROM flags
GROUP BY 1, 2, 3, 4
QUALIFY ROW_NUMBER() OVER (PARTITION BY mne ORDER BY COUNT(*) DESC) <= 6
ORDER BY mne, decisions DESC;


-- ===== BLOCK 1: funnel by MNE — EMAIL_ACTION rows only, plus one TOTAL row =====
-- QUESTION: of email-action decisions on these five MNEs, how many show up in the vendor
--   send chain, and how many were actually sent?
-- ROWS: 6 (5 Cards MNEs + 1 TOTAL row)
-- GOOD LOOKS LIKE: decisions_sent_disposition1 close to decisions_in_master — Pack 17 found
--   these nearly equal for these same five MNEs
-- WHAT TO DO WITH IT: paste to Claude

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
-- WHAT TO DO WITH IT: paste to Claude

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


-- ===== BLOCK 3: full cube — MNE x cohort_month x arm_from_channel (all four arms) =====
-- QUESTION: same funnel, broken out by MNE x month x channel-slot arm, for self-service
--   slicing across all four arm categories (EMAIL_ACTION, HOLDOUT_XX, NO_CHANNEL_BLANK,
--   OTHER_CHANNEL)
-- ROWS: many (5 MNEs x ~20 months x up to 4 arms — roughly 200-400)
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
