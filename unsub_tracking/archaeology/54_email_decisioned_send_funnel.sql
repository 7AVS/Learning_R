-- 54: Email-decisioned send funnel — Cards MNEs only (CRV, PCL, PCQ, PCD, AUH)
-- ENGINE: Teradata-direct.
-- PURPOSE: of clients decisioned into email and not in a control cell, how many appear in
-- the vendor send chain, and how many were actually sent — scoped to the five Cards MNEs
-- (list taken from Pack 17, archaeology/17_em_decision_vendor_coverage.sql).
-- RUN ORDER: Step A (volatile decisioned pop) -> Step B (volatile MASTER match) ->
-- Step C (volatile SENT match) -> Block 0 (arm profiling) -> Block 1 (NON_CONTROL funnel by
-- MNE) -> Block 2 (CONTROL funnel by MNE) -> Block 3 (full MNE x month x arm cube).
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

-- ===== PARAMETER BLOCK 2: CONTROL ARM MAPPING — FILL FROM BLOCK 0 OUTPUT =====
-- No suffix guessing (bank-wide Pack 54 v1 tried a '%C'/'%T' suffix rule and Block 0 showed it
-- was 100% wrong for these MNEs). Every (mne, test_group_code) pair confirmed CONTROL from
-- Block 0's output gets its own WHEN line below; everything else defaults to NON_CONTROL until
-- filled in. This exact CASE expression is copy-pasted into Blocks 1, 2 and 3 below — edit it
-- here, then paste the edited version into all three places (Teradata CTEs don't share across
-- statements, so there is no single point of definition to change instead).
--
-- CASE
--     -- FILL FROM BLOCK 0 OUTPUT: one WHEN per (mne, test_group_code) confirmed CONTROL, e.g.:
--     -- WHEN d.mne = 'CRV' AND d.test_group_code = 'CODE_HERE' THEN 'CONTROL'
--     ELSE 'NON_CONTROL'
-- END AS arm_group


-- ===== STEP A: VOLATILE — email-decisioned Cards population =====
-- DROP TABLE vt_em_decis_cards;  -- uncomment + run alone if rerunning after a failed pass

CREATE VOLATILE TABLE vt_em_decis_cards AS (
    SELECT DISTINCT
        t.CLNT_NO,
        t.TACTIC_ID,
        SUBSTR(t.TACTIC_ID, 8, 3)                        AS mne,
        EXTRACT(YEAR FROM t.TREATMT_STRT_DT) * 100
          + EXTRACT(MONTH FROM t.TREATMT_STRT_DT)         AS cohort_yyyymm,
        TRIM(t.TST_GRP_CD)                                AS test_group_code
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
      AND SUBSTR(t.TACTIC_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH')  -- PARAMETER BLOCK 1
      AND (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
           OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
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


-- ===== BLOCK 0: top 5 TST_GRP_CD codes per Cards MNE, by decision volume =====
-- QUESTION: what are the top 5 TST_GRP_CD codes by decision volume for each Cards MNE, and
--   how big is each one relative to that MNE's total decision volume?
-- ROWS: 25 (5 Cards MNEs x top 5 codes each). NOT the full code list — first pass showed
--   PCD/PCL/AUH/PCQ each carry dozens of distinct codes (81/53/42/40; CRV only 3), and codes
--   read as plain numbered test groups (TG1, TG4, ...) with no C/T suffix to guess from, so a
--   full dump is unreadable. Top 5 by volume is where the real cells are; re-run without the
--   QUALIFY cap if a specific MNE needs its long tail checked.
-- GOOD LOOKS LIKE: one of each MNE's top codes reads as a clear holdout/no-contact cell (a
--   TG code with meaningfully lower volume than its siblings, or one that never repeats
--   across cohorts the way action codes do)
-- WHAT TO DO WITH IT: paste to Claude — used to fill PARAMETER BLOCK 2 above

SELECT
    mne,
    test_group_code,
    CAST(COUNT(*) AS BIGINT)                              AS decisions_email,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT)                AS distinct_clients,
    CAST(SUM(COUNT(*)) OVER (PARTITION BY mne) AS BIGINT)  AS mne_total_decisions
FROM vt_em_decis_cards
GROUP BY mne, test_group_code
QUALIFY ROW_NUMBER() OVER (PARTITION BY mne ORDER BY COUNT(*) DESC) <= 5
ORDER BY mne, decisions_email DESC;


-- ===== BLOCK 1: funnel by MNE only — NON_CONTROL rows, plus one TOTAL row =====
-- QUESTION: of clients decisioned to email and NOT flagged control, how many show up in the
--   vendor send chain, and how many were actually sent?
-- ROWS: 6 (5 Cards MNEs + 1 TOTAL row)
-- GOOD LOOKS LIKE: decisions_sent_disposition1 close to decisions_in_master — Pack 17 found
--   these nearly equal for these same five MNEs
-- WHAT TO DO WITH IT: paste to Claude

WITH arm_tagged AS (
    SELECT
        d.mne,
        d.CLNT_NO,
        d.TACTIC_ID,
        CASE
            -- FILL FROM BLOCK 0 OUTPUT: one WHEN per (mne, test_group_code) confirmed CONTROL, e.g.:
            -- WHEN d.mne = 'CRV' AND d.test_group_code = 'CODE_HERE' THEN 'CONTROL'
            ELSE 'NON_CONTROL'
        END AS arm_group
    FROM vt_em_decis_cards d
),
flags AS (
    SELECT
        a.mne,
        a.arm_group,
        a.CLNT_NO,
        a.TACTIC_ID,
        MAX(CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)            AS f_in_master,
        MAX(CASE WHEN s.consumer_id_hashed IS NOT NULL THEN 1 ELSE 0 END) AS f_sent
    FROM arm_tagged a
    LEFT JOIN vt_master_cards m
        ON  m.TREATMENT_ID = a.TACTIC_ID
        AND m.CLNT_NO       = a.CLNT_NO
    LEFT JOIN vt_sent_cards s
        ON  s.TREATMENT_ID       = a.TACTIC_ID
        AND s.consumer_id_hashed = m.consumer_id_hashed
    GROUP BY 1, 2, 3, 4
)
SELECT
    CAST(mne AS VARCHAR(20))                AS mne,
    CAST(COUNT(*) AS BIGINT)                AS decisions_email,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_in_master) AS BIGINT)        AS decisions_in_master,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent_disposition1
FROM flags
WHERE arm_group = 'NON_CONTROL'
GROUP BY 1
UNION ALL
SELECT
    CAST('TOTAL' AS VARCHAR(20))            AS mne,
    CAST(COUNT(*) AS BIGINT)                AS decisions_email,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_in_master) AS BIGINT)        AS decisions_in_master,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent_disposition1
FROM flags
WHERE arm_group = 'NON_CONTROL'
ORDER BY 1;


-- ===== BLOCK 2: same four counts, CONTROL rows by MNE =====
-- QUESTION: of clients decisioned to email and flagged CONTROL, how many show up in the
--   vendor send chain, and how many were actually sent?
-- ROWS: expect <=6 (one row per Cards MNE with a filled control code — zero rows until
--   PARAMETER BLOCK 2 is filled in, since everything defaults to NON_CONTROL until then)
-- GOOD LOOKS LIKE: decisions_sent_disposition1 near zero — control cells should not receive
--   the email
-- WHAT TO DO WITH IT: paste to Claude

WITH arm_tagged AS (
    SELECT
        d.mne,
        d.CLNT_NO,
        d.TACTIC_ID,
        CASE
            -- FILL FROM BLOCK 0 OUTPUT: one WHEN per (mne, test_group_code) confirmed CONTROL, e.g.:
            -- WHEN d.mne = 'CRV' AND d.test_group_code = 'CODE_HERE' THEN 'CONTROL'
            ELSE 'NON_CONTROL'
        END AS arm_group
    FROM vt_em_decis_cards d
),
flags AS (
    SELECT
        a.mne,
        a.arm_group,
        a.CLNT_NO,
        a.TACTIC_ID,
        MAX(CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)            AS f_in_master,
        MAX(CASE WHEN s.consumer_id_hashed IS NOT NULL THEN 1 ELSE 0 END) AS f_sent
    FROM arm_tagged a
    LEFT JOIN vt_master_cards m
        ON  m.TREATMENT_ID = a.TACTIC_ID
        AND m.CLNT_NO       = a.CLNT_NO
    LEFT JOIN vt_sent_cards s
        ON  s.TREATMENT_ID       = a.TACTIC_ID
        AND s.consumer_id_hashed = m.consumer_id_hashed
    GROUP BY 1, 2, 3, 4
)
SELECT
    mne,
    CAST(COUNT(*) AS BIGINT)                AS decisions_email,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_in_master) AS BIGINT)        AS decisions_in_master,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent_disposition1
FROM flags
WHERE arm_group = 'CONTROL'
GROUP BY 1
ORDER BY 1;


-- ===== BLOCK 3: full cube — MNE x cohort_month x arm (NON_CONTROL and CONTROL) =====
-- QUESTION: same funnel, broken out by MNE x month x arm, for self-service slicing
-- ROWS: many (5 MNEs x ~20 months x up to 2 arm groups — roughly 150-200)
-- GOOD LOOKS LIKE: continuous monthly coverage per MNE in both arm groups once PARAMETER
--   BLOCK 2 is filled in — gaps or a permanently-empty CONTROL arm mean the mapping is
--   still wrong or incomplete for that MNE
-- WHAT TO DO WITH IT: save to Excel, do not paste

WITH arm_tagged AS (
    SELECT
        d.mne,
        d.cohort_yyyymm,
        d.CLNT_NO,
        d.TACTIC_ID,
        CASE
            -- FILL FROM BLOCK 0 OUTPUT: one WHEN per (mne, test_group_code) confirmed CONTROL, e.g.:
            -- WHEN d.mne = 'CRV' AND d.test_group_code = 'CODE_HERE' THEN 'CONTROL'
            ELSE 'NON_CONTROL'
        END AS arm_group
    FROM vt_em_decis_cards d
),
flags AS (
    SELECT
        a.mne,
        a.cohort_yyyymm,
        a.arm_group,
        a.CLNT_NO,
        a.TACTIC_ID,
        MAX(CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)            AS f_in_master,
        MAX(CASE WHEN s.consumer_id_hashed IS NOT NULL THEN 1 ELSE 0 END) AS f_sent
    FROM arm_tagged a
    LEFT JOIN vt_master_cards m
        ON  m.TREATMENT_ID = a.TACTIC_ID
        AND m.CLNT_NO       = a.CLNT_NO
    LEFT JOIN vt_sent_cards s
        ON  s.TREATMENT_ID       = a.TACTIC_ID
        AND s.consumer_id_hashed = m.consumer_id_hashed
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    mne,
    cohort_yyyymm,
    arm_group,
    CAST(COUNT(*) AS BIGINT)                AS decisions_email,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_in_master) AS BIGINT)        AS decisions_in_master,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent_disposition1
FROM flags
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
