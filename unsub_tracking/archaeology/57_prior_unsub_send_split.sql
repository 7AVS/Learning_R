-- 57: Does a prior SFMC unsubscribe stop the email? (Cards MNEs, Teradata-direct)
-- Built on Pack 54 v3.1's confirmed result: EMAIL_ACTION decisions since 2025-01-01,
-- in_master == sent in every row. This file asks the next question: among those
-- EMAIL_ACTION decisions, does a client who already unsubscribed (any program, any prior
-- treatment, bank-wide) still get sent to? If PRIOR_UNSUB sends are near zero, SFMC honours
-- its own unsub list and "unreachable" = PRIOR_UNSUB decisions / all decisions. If not, SFMC
-- re-mails unsubscribed clients and the unsub list isn't a suppression gate.
-- RUN ORDER: Step 0 (reset) -> Step A (EMAIL_ACTION decisions) -> Step A2 (tactic-id driver)
-- -> Step B (sent) -> Step C (first-ever unsub per client, bank-wide) -> Step D (zero-send
-- months) -> ZERO-SEND MONTHS output -> Block 1 (excl. zero-send months) -> Block 2 (all
-- months) -> Block 3 (cube).
-- Grain = (CLNT_NO, TACTIC_ID). Counts only, no rates. Volatile tables live in spool
-- (Pack 54's 110M-row lesson) — Step 0 resets them, every downstream join uses the
-- tactic-id driver table, not an IN-subquery.
-- =============================================================================

-- ===== STEP 0: RESET — drop volatile tables from any previous attempt =====
-- Errors "does not exist" on a first run are expected here; keep going.
DROP TABLE vt_em_decis57;
DROP TABLE vt_tactic_ids57;
DROP TABLE vt_sent57;
DROP TABLE vt_first_unsub57;
DROP TABLE vt_zero_send_months57;


-- ===== PARAMETER BLOCK: MNE SCOPE + TWO WINDOWS =====
-- MNE scope — Cards personal MNEs, exactly as Pack 17 scoped them:
-- archaeology/17_em_decision_vendor_coverage.sql -> SUBSTR(t.TACTIC_ID,8,3) IN ('CRV','PCL','PCQ','PCD','AUH')
-- DECISION window start — DATE '2025-01-01', used in Step A. Same spool-safe floor as Pack 54
-- v3.1 (the 2024-01-01 full pull hit 110M rows and exhausted spool on 2026-09-04).
-- UNSUB lookback floor — DATE '2024-01-01', used in Step C ONLY, ON PURPOSE. This file needs
-- unsubscribes that happened BEFORE the 2025-01-01 decisions, so the lookback has to start
-- earlier than the decision window — narrowing it to 2025-01-01 would silently erase every
-- unsub that could actually explain a 2025 non-send.
-- Both values are written directly into the relevant step below — no shared variable across
-- statements in a plain Teradata script; edit in place if either changes.


-- ===== STEP A: VOLATILE — EMAIL_ACTION decisions, Cards MNEs, 2025-01-01+ =====
-- DROP TABLE vt_em_decis57;  -- also see STEP 0
-- Only the EM branch of Pack 54's channel CASE is needed here (HOLDOUT_XX is out of scope
-- for this question — holdouts never send regardless of unsub history, already proven).

CREATE VOLATILE TABLE vt_em_decis57 AS (
    SELECT DISTINCT
        t.CLNT_NO,
        t.TACTIC_ID,
        SUBSTR(t.TACTIC_ID, 8, 3)                        AS mne,
        EXTRACT(YEAR FROM t.TREATMT_STRT_DT) * 100
          + EXTRACT(MONTH FROM t.TREATMT_STRT_DT)         AS cohort_yyyymm,
        t.TREATMT_STRT_DT
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2025-01-01'                          -- PARAMETER BLOCK: decision window start
      AND SUBSTR(t.TACTIC_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH')     -- PARAMETER BLOCK: MNE scope
      AND (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
           OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
) WITH DATA
PRIMARY INDEX (TACTIC_ID, CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_em_decis57 COLUMN (TACTIC_ID, CLNT_NO);


-- ===== STEP A2: VOLATILE — distinct tactic ids from Step A (join driver) =====
-- DROP TABLE vt_tactic_ids57;  -- also see STEP 0

CREATE VOLATILE TABLE vt_tactic_ids57 AS (
    SELECT DISTINCT TACTIC_ID FROM vt_em_decis57
) WITH DATA
PRIMARY INDEX (TACTIC_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_tactic_ids57 COLUMN (TACTIC_ID);


-- ===== STEP B: VOLATILE — sent (disposition_cd=1), restricted via the tactic-id driver =====
-- DROP TABLE vt_sent57;  -- also see STEP 0
-- Pack 54 v3.1 proved in_master == sent for this population, so one table (sent) suffices —
-- no separate MASTER-presence table needed here.

CREATE VOLATILE TABLE vt_sent57 AS (
    SELECT DISTINCT
        m.CLNT_NO,
        m.TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
    INNER JOIN vt_tactic_ids57 x
        ON x.TACTIC_ID = m.TREATMENT_ID
    INNER JOIN DTZV01.VENDOR_FEEDBACK_EVENT e
        ON  e.TREATMENT_ID       = m.TREATMENT_ID
        AND e.consumer_id_hashed = m.consumer_id_hashed
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2025-01-01'  -- PARAMETER BLOCK: decision window start
) WITH DATA
PRIMARY INDEX (TREATMENT_ID, CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_sent57 COLUMN (TREATMENT_ID, CLNT_NO);


-- ===== STEP C: VOLATILE — first-ever unsub per client, bank-wide, 2024-01-01+ =====
-- DROP TABLE vt_first_unsub57;  -- also see STEP 0
-- Bank-wide on purpose — an unsub from ANY program counts against reachability, not just a
-- Cards one. Junk TREATMENT_IDs (vendor residue, §20.12) excluded. Expected size ~650K
-- clients (§0 of this file, "649,885 distinct unsub clients since 2024-01-01").

CREATE VOLATILE TABLE vt_first_unsub57 AS (
    SELECT
        m.CLNT_NO,
        MIN(e.disposition_dt_tm) AS first_unsub_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.TREATMENT_ID       = e.TREATMENT_ID
        AND m.consumer_id_hashed = e.consumer_id_hashed
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'   -- PARAMETER BLOCK: unsub lookback floor (intentionally earlier than the decision window)
      AND e.TREATMENT_ID NOT IN ('DEFAULT', 'CABVRSN1')
      AND m.CLNT_NO IS NOT NULL
    GROUP BY m.CLNT_NO
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_first_unsub57 COLUMN (CLNT_NO);


-- ===== STEP D: VOLATILE — mne x cohort_yyyymm pairs with ZERO sends (operational gaps) =====
-- DROP TABLE vt_zero_send_months57;  -- also see STEP 0
-- Pack 54 v3.1's cube showed whole months with zero sends (CRV 202602-202605, PCL 202607) —
-- an operational gap, not client-level suppression. Exclude these before reading Block 1's
-- prior-unsub split, or a zero-send month with zero PRIOR_UNSUB clients in it will make the
-- split look cleaner than it is.

CREATE VOLATILE TABLE vt_zero_send_months57 AS (
    SELECT
        d.mne,
        d.cohort_yyyymm
    FROM vt_em_decis57 d
    LEFT JOIN vt_sent57 s
        ON  s.TREATMENT_ID = d.TACTIC_ID
        AND s.CLNT_NO       = d.CLNT_NO
    GROUP BY d.mne, d.cohort_yyyymm
    HAVING SUM(CASE WHEN s.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) = 0
) WITH DATA
PRIMARY INDEX (mne, cohort_yyyymm)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_zero_send_months57 COLUMN (mne, cohort_yyyymm);


-- ===== ZERO-SEND MONTHS: mne x cohort_yyyymm pairs excluded from Block 1 =====
-- QUESTION: which mne x month combinations had zero sends at all among EMAIL_ACTION decisions?
-- ROWS: ~5
-- GOOD LOOKS LIKE: CRV 202602-202605 and PCL 202607 appear, matching Pack 54 v3.1's cube
-- WHAT TO DO WITH IT: paste to Claude

SELECT mne, cohort_yyyymm
FROM vt_zero_send_months57
ORDER BY 1, 2;


-- ===== BLOCK 1: mne x prior-unsub flag, EXCLUDING zero-send months =====
-- QUESTION: among email-action decisions in months that sent at all, does a prior SFMC
--   unsubscribe (any program, before this decision) change whether the email is sent?
-- ROWS: 12 (5 Cards MNEs x 2 flags + 1 TOTAL row per flag)
-- GOOD LOOKS LIKE: PRIOR_UNSUB sends far below NO_PRIOR_UNSUB. If PRIOR_UNSUB sends are near
--   zero, SFMC honours its own list and the unreachable share = PRIOR_UNSUB decisions / all
--   decisions. If PRIOR_UNSUB sends are high, SFMC re-mails unsubscribed clients.
-- WHAT TO DO WITH IT: paste to Claude

WITH decis_flagged AS (
    SELECT
        d.mne,
        d.cohort_yyyymm,
        d.CLNT_NO,
        d.TACTIC_ID,
        CASE
            WHEN u.first_unsub_dt_tm IS NOT NULL
             AND u.first_unsub_dt_tm < CAST(d.TREATMT_STRT_DT AS TIMESTAMP(0))
                THEN 'PRIOR_UNSUB'
            ELSE 'NO_PRIOR_UNSUB'
        END AS prior_unsub_flag
    FROM vt_em_decis57 d
    LEFT JOIN vt_first_unsub57 u
        ON u.CLNT_NO = d.CLNT_NO
),
sent_flagged AS (
    SELECT
        df.mne,
        df.cohort_yyyymm,
        df.prior_unsub_flag,
        df.CLNT_NO,
        df.TACTIC_ID,
        MAX(CASE WHEN s.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS f_sent
    FROM decis_flagged df
    LEFT JOIN vt_sent57 s
        ON  s.TREATMENT_ID = df.TACTIC_ID
        AND s.CLNT_NO       = df.CLNT_NO
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    CAST(mne AS VARCHAR(20))              AS mne,
    CAST(prior_unsub_flag AS VARCHAR(20)) AS prior_unsub_flag,
    CAST(COUNT(*) AS BIGINT)                AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent
FROM sent_flagged sf
WHERE NOT EXISTS (
    SELECT 1 FROM vt_zero_send_months57 z
    WHERE z.mne = sf.mne AND z.cohort_yyyymm = sf.cohort_yyyymm
)
GROUP BY mne, prior_unsub_flag
UNION ALL
SELECT
    CAST('TOTAL' AS VARCHAR(20))          AS mne,
    CAST(prior_unsub_flag AS VARCHAR(20)) AS prior_unsub_flag,
    CAST(COUNT(*) AS BIGINT)                AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent
FROM sent_flagged sf
WHERE NOT EXISTS (
    SELECT 1 FROM vt_zero_send_months57 z
    WHERE z.mne = sf.mne AND z.cohort_yyyymm = sf.cohort_yyyymm
)
GROUP BY prior_unsub_flag
ORDER BY 1, 2;


-- ===== BLOCK 2: mne x prior-unsub flag, ALL MONTHS (shows how much the gaps distort it) =====
-- QUESTION: same split with the zero-send months left in — how much does the operational
--   gap change the read versus Block 1?
-- ROWS: 12 (5 Cards MNEs x 2 flags + 1 TOTAL row per flag)
-- GOOD LOOKS LIKE: broadly similar shape to Block 1; a big divergence means the zero-send
--   months carry a disproportionate share of one flag
-- WHAT TO DO WITH IT: paste to Claude

WITH decis_flagged AS (
    SELECT
        d.mne,
        d.cohort_yyyymm,
        d.CLNT_NO,
        d.TACTIC_ID,
        CASE
            WHEN u.first_unsub_dt_tm IS NOT NULL
             AND u.first_unsub_dt_tm < CAST(d.TREATMT_STRT_DT AS TIMESTAMP(0))
                THEN 'PRIOR_UNSUB'
            ELSE 'NO_PRIOR_UNSUB'
        END AS prior_unsub_flag
    FROM vt_em_decis57 d
    LEFT JOIN vt_first_unsub57 u
        ON u.CLNT_NO = d.CLNT_NO
),
sent_flagged AS (
    SELECT
        df.mne,
        df.cohort_yyyymm,
        df.prior_unsub_flag,
        df.CLNT_NO,
        df.TACTIC_ID,
        MAX(CASE WHEN s.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS f_sent
    FROM decis_flagged df
    LEFT JOIN vt_sent57 s
        ON  s.TREATMENT_ID = df.TACTIC_ID
        AND s.CLNT_NO       = df.CLNT_NO
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    CAST(mne AS VARCHAR(20))              AS mne,
    CAST(prior_unsub_flag AS VARCHAR(20)) AS prior_unsub_flag,
    CAST(COUNT(*) AS BIGINT)                AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent
FROM sent_flagged sf
GROUP BY mne, prior_unsub_flag
UNION ALL
SELECT
    CAST('TOTAL' AS VARCHAR(20))          AS mne,
    CAST(prior_unsub_flag AS VARCHAR(20)) AS prior_unsub_flag,
    CAST(COUNT(*) AS BIGINT)                AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent
FROM sent_flagged sf
GROUP BY prior_unsub_flag
ORDER BY 1, 2;


-- ===== BLOCK 3: full cube — mne x cohort_yyyymm x prior-unsub flag, all months =====
-- QUESTION: same split, broken out by month, for self-service slicing (identifies exactly
--   which months/MNEs the zero-send gap sits in and whether PRIOR_UNSUB sends spike anywhere)
-- ROWS: many (5 MNEs x ~8-12 months x 2 flags — roughly 80-120)
-- GOOD LOOKS LIKE: PRIOR_UNSUB sends stay low every month, not just on average; the zero-send
--   months from the block above show 0 decisions_sent for BOTH flags, not just one
-- WHAT TO DO WITH IT: save to Excel, do not paste

WITH decis_flagged AS (
    SELECT
        d.mne,
        d.cohort_yyyymm,
        d.CLNT_NO,
        d.TACTIC_ID,
        CASE
            WHEN u.first_unsub_dt_tm IS NOT NULL
             AND u.first_unsub_dt_tm < CAST(d.TREATMT_STRT_DT AS TIMESTAMP(0))
                THEN 'PRIOR_UNSUB'
            ELSE 'NO_PRIOR_UNSUB'
        END AS prior_unsub_flag
    FROM vt_em_decis57 d
    LEFT JOIN vt_first_unsub57 u
        ON u.CLNT_NO = d.CLNT_NO
),
sent_flagged AS (
    SELECT
        df.mne,
        df.cohort_yyyymm,
        df.prior_unsub_flag,
        df.CLNT_NO,
        df.TACTIC_ID,
        MAX(CASE WHEN s.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS f_sent
    FROM decis_flagged df
    LEFT JOIN vt_sent57 s
        ON  s.TREATMENT_ID = df.TACTIC_ID
        AND s.CLNT_NO       = df.CLNT_NO
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    mne,
    cohort_yyyymm,
    prior_unsub_flag,
    CAST(COUNT(*) AS BIGINT)                AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent
FROM sent_flagged
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
