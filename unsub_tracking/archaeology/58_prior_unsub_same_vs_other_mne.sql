-- 58: When a prior-unsubscribed client is still sent to, was the unsub SAME program or OTHER? (Cards MNEs, Teradata-direct)
-- Built on Pack 57's finding: prior unsub (any program, bank-wide) drops send rate from 94%
-- to 55%, not to zero. This file splits WHY: was the earlier unsub on the SAME MNE (a
-- compliance question — the program is re-mailing someone who opted out of it specifically)
-- or only on OTHER MNEs (legitimate — a program-specific opt-out doesn't have to silence a
-- different program)?
-- RUN ORDER: Step 0 (reset, incl. Pack 54 and Pack 57 leftovers) -> Step A (EMAIL_ACTION
-- decisions) -> Step A2 (tactic-id driver) -> Step B1 (MASTER match) -> Step B2 (SENT
-- events) -> Step B3 (sent = join B1+B2, drop B1/B2) -> Step C58 (first unsub PER MNE per
-- client, bank-wide) -> Step D (zero-send months) -> ZERO-SEND MONTHS output -> Block 1
-- (mne x flag) -> Block 2 (TOTAL by flag) -> Block 3 (days-since-unsub buckets, SAME_MNE
-- sent only) -> Block 4 (cube).
-- Volatile tables persist for the session and consume spool (Pack 54/57 lesson) — this file
-- builds its own 58-suffixed tables from scratch rather than reusing 57's, since Andre may
-- reconnect between sessions and 57's tables may already be gone.
-- Grain = (CLNT_NO, TACTIC_ID). Counts only, no rates.
-- =============================================================================

-- STEP 0 removed 2026-09-04: no DROP statements in packs. If you rerun a pack in the same
-- session and hit 'table already exists', run 00_reset_volatiles.sql first.


-- ===== PARAMETER BLOCK: MNE SCOPE + TWO WINDOWS (same as Pack 57) =====
-- MNE scope — Cards personal MNEs, exactly as Pack 17 scoped them:
-- archaeology/17_em_decision_vendor_coverage.sql -> SUBSTR(t.TACTIC_ID,8,3) IN ('CRV','PCL','PCQ','PCD','AUH')
-- DECISION window start — DATE '2025-01-01', used in Step A and Step B2.
-- UNSUB lookback floor — DATE '2024-01-01', used in Step C58 ONLY, ON PURPOSE — needs unsubs
-- from BEFORE the 2025-01-01 decision window, so it has to start earlier than that window.
-- Both values are written directly into the relevant step below — no shared variable across
-- statements in a plain Teradata script; edit in place if either changes.


-- ===== STEP A: VOLATILE — EMAIL_ACTION decisions, Cards MNEs, 2025-01-01+ =====
-- DROP TABLE vt_em_decis58;  -- also see STEP 0

CREATE VOLATILE TABLE vt_em_decis58 AS (
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

COLLECT STATISTICS ON vt_em_decis58 COLUMN (TACTIC_ID, CLNT_NO);


-- ===== STEP A2: VOLATILE — distinct tactic ids from Step A (join driver) =====
-- DROP TABLE vt_tactic_ids58;  -- also see STEP 0

CREATE VOLATILE TABLE vt_tactic_ids58 AS (
    SELECT DISTINCT TACTIC_ID FROM vt_em_decis58
) WITH DATA
PRIMARY INDEX (TACTIC_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_tactic_ids58 COLUMN (TACTIC_ID);


-- ===== STEP B1: VOLATILE — MASTER match, restricted via the tactic-id driver =====
-- DROP TABLE vt_master58;  -- also see STEP 0

CREATE VOLATILE TABLE vt_master58 AS (
    SELECT DISTINCT
        m.TREATMENT_ID,
        m.CLNT_NO,
        m.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
    INNER JOIN vt_tactic_ids58 x
        ON x.TACTIC_ID = m.TREATMENT_ID
    WHERE m.CLNT_NO IS NOT NULL
) WITH DATA
PRIMARY INDEX (TREATMENT_ID, CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_master58 COLUMN (TREATMENT_ID, CLNT_NO);


-- ===== STEP B2: VOLATILE — SENT (disposition_cd=1) events, restricted via the tactic-id driver =====
-- DROP TABLE vt_sent_evt58;  -- also see STEP 0

CREATE VOLATILE TABLE vt_sent_evt58 AS (
    SELECT DISTINCT
        e.TREATMENT_ID,
        e.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN vt_tactic_ids58 x
        ON x.TACTIC_ID = e.TREATMENT_ID
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2025-01-01'  -- PARAMETER BLOCK: decision window start
) WITH DATA
PRIMARY INDEX (TREATMENT_ID, consumer_id_hashed)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_sent_evt58 COLUMN (TREATMENT_ID, consumer_id_hashed);


-- ===== STEP B3: VOLATILE — sent, two-way join of B1+B2 (spool-safe pattern from Pack 54/57) =====
-- DROP TABLE vt_sent58;  -- also see STEP 0

CREATE VOLATILE TABLE vt_sent58 AS (
    SELECT DISTINCT
        m.CLNT_NO,
        m.TREATMENT_ID
    FROM vt_master58 m
    INNER JOIN vt_sent_evt58 s
        ON  s.TREATMENT_ID       = m.TREATMENT_ID
        AND s.consumer_id_hashed = m.consumer_id_hashed
) WITH DATA
PRIMARY INDEX (TREATMENT_ID, CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_sent58 COLUMN (TREATMENT_ID, CLNT_NO);

-- Drop the two intermediate tables now that vt_sent58 is built — frees their spool for
-- Step C58/D and the blocks below.
DROP TABLE vt_master58;
DROP TABLE vt_sent_evt58;


-- ===== STEP C58: VOLATILE — first-ever unsub PER CLIENT PER MNE, bank-wide, 2024-01-01+ =====
-- DROP TABLE vt_first_unsub_by_mne58;  -- also see STEP 0
-- Same population and floor as Pack 57's vt_first_unsub57, but grouped one level finer — by
-- (CLNT_NO, unsub_mne) instead of CLNT_NO alone — so same-program vs other-program can be
-- told apart downstream. Junk TREATMENT_IDs (vendor residue, §20.12) excluded.

CREATE VOLATILE TABLE vt_first_unsub_by_mne58 AS (
    SELECT
        m.CLNT_NO,
        SUBSTR(e.TREATMENT_ID, 8, 3)  AS unsub_mne,
        MIN(e.disposition_dt_tm)      AS first_unsub_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.TREATMENT_ID       = e.TREATMENT_ID
        AND m.consumer_id_hashed = e.consumer_id_hashed
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'   -- PARAMETER BLOCK: unsub lookback floor
      AND e.TREATMENT_ID NOT IN ('DEFAULT', 'CABVRSN1')
      AND m.CLNT_NO IS NOT NULL
    GROUP BY m.CLNT_NO, SUBSTR(e.TREATMENT_ID, 8, 3)
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_first_unsub_by_mne58 COLUMN (CLNT_NO);
COLLECT STATISTICS ON vt_first_unsub_by_mne58 COLUMN (CLNT_NO, unsub_mne);


-- ===== STEP D: VOLATILE — mne x cohort_yyyymm pairs with ZERO sends (operational gaps) =====
-- DROP TABLE vt_zero_send_months58;  -- also see STEP 0
-- Same scope as Pack 57 (identical Step A population) — expect the same result: CRV
-- 202502-202505 and PCL 202607.

CREATE VOLATILE TABLE vt_zero_send_months58 AS (
    SELECT
        d.mne,
        d.cohort_yyyymm
    FROM vt_em_decis58 d
    LEFT JOIN vt_sent58 s
        ON  s.TREATMENT_ID = d.TACTIC_ID
        AND s.CLNT_NO       = d.CLNT_NO
    GROUP BY d.mne, d.cohort_yyyymm
    HAVING SUM(CASE WHEN s.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) = 0
) WITH DATA
PRIMARY INDEX (mne, cohort_yyyymm)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_zero_send_months58 COLUMN (mne, cohort_yyyymm);


-- ===== ZERO-SEND MONTHS: mne x cohort_yyyymm pairs excluded from Block 1/2 =====
-- QUESTION: which mne x month combinations had zero sends at all among EMAIL_ACTION decisions?
-- ROWS: ~5
-- GOOD LOOKS LIKE: matches Pack 57 — CRV 202502-202505 and PCL 202607
-- WHAT TO DO WITH IT: paste to Claude

SELECT mne, cohort_yyyymm
FROM vt_zero_send_months58
ORDER BY 1, 2;


-- ===== BLOCK 1: mne x prior-unsub flag (same-program vs other-program split), zero-send months EXCLUDED =====
-- QUESTION: of email-action decisions, does it matter whether the prior unsub was on THIS
--   program or a DIFFERENT one?
-- ROWS: 15 (5 Cards MNEs x 3 flags)
-- GOOD LOOKS LIKE: PRIOR_UNSUB_SAME_MNE sends near zero (the unsub is honoured for its own
--   program); PRIOR_UNSUB_OTHER_MNE_ONLY sends near the NO_PRIOR rate (program-specific
--   opt-out). If SAME_MNE sends are substantial, the program re-mails clients who
--   unsubscribed from it.
-- WHAT TO DO WITH IT: paste to Claude

WITH same_mne_unsub AS (
    SELECT CLNT_NO, unsub_mne, first_unsub_dt_tm
    FROM vt_first_unsub_by_mne58
),
any_mne_unsub AS (
    SELECT CLNT_NO, MIN(first_unsub_dt_tm) AS first_unsub_any_mne
    FROM vt_first_unsub_by_mne58
    GROUP BY CLNT_NO
),
decis_flagged AS (
    SELECT
        d.mne,
        d.cohort_yyyymm,
        d.CLNT_NO,
        d.TACTIC_ID,
        CASE
            WHEN sm.first_unsub_dt_tm IS NOT NULL
             AND sm.first_unsub_dt_tm < CAST(d.TREATMT_STRT_DT AS TIMESTAMP(0))
                THEN 'PRIOR_UNSUB_SAME_MNE'
            WHEN am.first_unsub_any_mne IS NOT NULL
             AND am.first_unsub_any_mne < CAST(d.TREATMT_STRT_DT AS TIMESTAMP(0))
                THEN 'PRIOR_UNSUB_OTHER_MNE_ONLY'
            ELSE 'NO_PRIOR_UNSUB'
        END AS prior_unsub_flag
    FROM vt_em_decis58 d
    LEFT JOIN same_mne_unsub sm
        ON  sm.CLNT_NO   = d.CLNT_NO
        AND sm.unsub_mne = d.mne
    LEFT JOIN any_mne_unsub am
        ON am.CLNT_NO = d.CLNT_NO
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
    LEFT JOIN vt_sent58 s
        ON  s.TREATMENT_ID = df.TACTIC_ID
        AND s.CLNT_NO       = df.CLNT_NO
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    mne,
    prior_unsub_flag,
    CAST(COUNT(*) AS BIGINT)                AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent
FROM sent_flagged sf
WHERE NOT EXISTS (
    SELECT 1 FROM vt_zero_send_months58 z
    WHERE z.mne = sf.mne AND z.cohort_yyyymm = sf.cohort_yyyymm
)
GROUP BY mne, prior_unsub_flag
ORDER BY 1, 2;


-- ===== BLOCK 2: TOTAL by flag (all Cards MNEs pooled), zero-send months EXCLUDED =====
-- QUESTION: bank-wide across these five MNEs, how does send behave for each of the three flags?
-- ROWS: 3
-- GOOD LOOKS LIKE: same shape as Block 1's per-MNE rows, summed
-- WHAT TO DO WITH IT: paste to Claude

WITH same_mne_unsub AS (
    SELECT CLNT_NO, unsub_mne, first_unsub_dt_tm
    FROM vt_first_unsub_by_mne58
),
any_mne_unsub AS (
    SELECT CLNT_NO, MIN(first_unsub_dt_tm) AS first_unsub_any_mne
    FROM vt_first_unsub_by_mne58
    GROUP BY CLNT_NO
),
decis_flagged AS (
    SELECT
        d.mne,
        d.cohort_yyyymm,
        d.CLNT_NO,
        d.TACTIC_ID,
        CASE
            WHEN sm.first_unsub_dt_tm IS NOT NULL
             AND sm.first_unsub_dt_tm < CAST(d.TREATMT_STRT_DT AS TIMESTAMP(0))
                THEN 'PRIOR_UNSUB_SAME_MNE'
            WHEN am.first_unsub_any_mne IS NOT NULL
             AND am.first_unsub_any_mne < CAST(d.TREATMT_STRT_DT AS TIMESTAMP(0))
                THEN 'PRIOR_UNSUB_OTHER_MNE_ONLY'
            ELSE 'NO_PRIOR_UNSUB'
        END AS prior_unsub_flag
    FROM vt_em_decis58 d
    LEFT JOIN same_mne_unsub sm
        ON  sm.CLNT_NO   = d.CLNT_NO
        AND sm.unsub_mne = d.mne
    LEFT JOIN any_mne_unsub am
        ON am.CLNT_NO = d.CLNT_NO
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
    LEFT JOIN vt_sent58 s
        ON  s.TREATMENT_ID = df.TACTIC_ID
        AND s.CLNT_NO       = df.CLNT_NO
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    prior_unsub_flag,
    CAST(COUNT(*) AS BIGINT)                AS decisions,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent
FROM sent_flagged sf
WHERE NOT EXISTS (
    SELECT 1 FROM vt_zero_send_months58 z
    WHERE z.mne = sf.mne AND z.cohort_yyyymm = sf.cohort_yyyymm
)
GROUP BY prior_unsub_flag
ORDER BY 1;


-- ===== BLOCK 3: days between same-MNE unsub and decision, PRIOR_UNSUB_SAME_MNE decisions that WERE sent =====
-- QUESTION: are same-program re-sends concentrated right after the unsub (batch lag) or long
--   after (re-consent / list reset)?
-- ROWS: <=25 (5 Cards MNEs x 5 day-buckets)
-- GOOD LOOKS LIKE: if re-sends cluster in 0-7/8-30, it's a suppression-list update lag, not a
--   standing failure to honour the unsub; heavy weight in 91-365/365+ points at a stale or
--   reset suppression list instead
-- WHAT TO DO WITH IT: paste to Claude

WITH same_mne_unsub AS (
    SELECT CLNT_NO, unsub_mne, first_unsub_dt_tm
    FROM vt_first_unsub_by_mne58
),
decis_same AS (
    SELECT
        d.mne,
        d.CLNT_NO,
        d.TACTIC_ID,
        d.TREATMT_STRT_DT - CAST(sm.first_unsub_dt_tm AS DATE) AS days_since_same_mne_unsub
    FROM vt_em_decis58 d
    INNER JOIN same_mne_unsub sm
        ON  sm.CLNT_NO   = d.CLNT_NO
        AND sm.unsub_mne = d.mne
    WHERE sm.first_unsub_dt_tm < CAST(d.TREATMT_STRT_DT AS TIMESTAMP(0))
),
sent_same AS (
    SELECT
        ds.mne,
        ds.CLNT_NO,
        ds.TACTIC_ID,
        ds.days_since_same_mne_unsub,
        MAX(CASE WHEN s.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS f_sent
    FROM decis_same ds
    LEFT JOIN vt_sent58 s
        ON  s.TREATMENT_ID = ds.TACTIC_ID
        AND s.CLNT_NO       = ds.CLNT_NO
    GROUP BY 1, 2, 3, 4
)
SELECT
    mne,
    CASE
        WHEN days_since_same_mne_unsub BETWEEN 0 AND 7    THEN '0-7'
        WHEN days_since_same_mne_unsub BETWEEN 8 AND 30   THEN '8-30'
        WHEN days_since_same_mne_unsub BETWEEN 31 AND 90  THEN '31-90'
        WHEN days_since_same_mne_unsub BETWEEN 91 AND 365 THEN '91-365'
        ELSE '365+'
    END                                      AS days_since_unsub_bucket,
    CAST(COUNT(*) AS BIGINT)                 AS decisions_sent
FROM sent_same
WHERE f_sent = 1
GROUP BY
    mne,
    CASE
        WHEN days_since_same_mne_unsub BETWEEN 0 AND 7    THEN '0-7'
        WHEN days_since_same_mne_unsub BETWEEN 8 AND 30   THEN '8-30'
        WHEN days_since_same_mne_unsub BETWEEN 31 AND 90  THEN '31-90'
        WHEN days_since_same_mne_unsub BETWEEN 91 AND 365 THEN '91-365'
        ELSE '365+'
    END
ORDER BY
    mne,
    CASE
        WHEN days_since_unsub_bucket = '0-7'    THEN 1
        WHEN days_since_unsub_bucket = '8-30'   THEN 2
        WHEN days_since_unsub_bucket = '31-90'  THEN 3
        WHEN days_since_unsub_bucket = '91-365' THEN 4
        ELSE 5
    END;


-- ===== BLOCK 4: full cube — mne x cohort_yyyymm x prior-unsub flag, all months =====
-- QUESTION: same three-way split, broken out by month, for self-service slicing
-- ROWS: many (5 MNEs x ~8-12 months x 3 flags — roughly 120-180)
-- GOOD LOOKS LIKE: PRIOR_UNSUB_SAME_MNE sends stay low every month, not just on average
-- WHAT TO DO WITH IT: save to Excel, do not paste

WITH same_mne_unsub AS (
    SELECT CLNT_NO, unsub_mne, first_unsub_dt_tm
    FROM vt_first_unsub_by_mne58
),
any_mne_unsub AS (
    SELECT CLNT_NO, MIN(first_unsub_dt_tm) AS first_unsub_any_mne
    FROM vt_first_unsub_by_mne58
    GROUP BY CLNT_NO
),
decis_flagged AS (
    SELECT
        d.mne,
        d.cohort_yyyymm,
        d.CLNT_NO,
        d.TACTIC_ID,
        CASE
            WHEN sm.first_unsub_dt_tm IS NOT NULL
             AND sm.first_unsub_dt_tm < CAST(d.TREATMT_STRT_DT AS TIMESTAMP(0))
                THEN 'PRIOR_UNSUB_SAME_MNE'
            WHEN am.first_unsub_any_mne IS NOT NULL
             AND am.first_unsub_any_mne < CAST(d.TREATMT_STRT_DT AS TIMESTAMP(0))
                THEN 'PRIOR_UNSUB_OTHER_MNE_ONLY'
            ELSE 'NO_PRIOR_UNSUB'
        END AS prior_unsub_flag
    FROM vt_em_decis58 d
    LEFT JOIN same_mne_unsub sm
        ON  sm.CLNT_NO   = d.CLNT_NO
        AND sm.unsub_mne = d.mne
    LEFT JOIN any_mne_unsub am
        ON am.CLNT_NO = d.CLNT_NO
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
    LEFT JOIN vt_sent58 s
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
