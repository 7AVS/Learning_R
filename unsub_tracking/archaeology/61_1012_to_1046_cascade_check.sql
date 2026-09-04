-- 61: 1012(email/7020) -> 1046(Avion) cascade check (Teradata-direct)
-- QUESTION: when a client revokes RBC-wide email consent (1012->5002) via the email page
--   (APP_SYS_CD 7020), does CPC also write a 1046 (Avion program) closure for the same client
--   at/near the same time? A director's 80/20 Avion-only-vs-RBC-wide split starts from clients
--   WITH a 1046 closure and assumes the cascade always fires. This tests that assumption both
--   directions: forward (1012 -> nearest 1046) and reverse (1046/7020 -> nearest 1012).
-- WINDOW: 1012 side = 2025-05-01..2026-06-25 (director's extract). 1046 side built wider
--   (2025-04-01..2026-07-31) to catch cascades landing just outside the 1012 window.
-- WRITER/ORIGIN COLUMN: APP_SYS_CD on DDWV01.CPC_RB_PREF_LOG itself - schemas/cpc_rb_pref_log_schema.md
--   line 24 confirms the log carries it (S0 screenshot + dictionary); no fallback to CPC_RB_PREF needed.
-- RUN ORDER: Block 1 (forward cascade, paste) -> Block 2 (reverse cascade, paste) -> Block 3 (monthly
--   denominators, paste). Grain = CLNT_NO. Counts only.
-- =============================================================================

-- No DROP / volatile tables in this file (2026-09-04): both sources are small, built as CTEs in each block.

-- ===== PARAMETER BLOCK: WINDOWS =====
-- WIN_START        = DATE '2025-05-01'  - 1012 side, matches the director's extract.
-- WIN_END_EXCL     = DATE '2026-06-26'  - 1012 side, exclusive (covers through 2026-06-25).
-- WIDE_START       = DATE '2025-04-01'  - 1046 side, one month earlier to catch early cascades.
-- WIDE_END_EXCL    = DATE '2026-08-01'  - 1046 side, one month later to catch late cascades.
-- No shared variables across statements in a plain Teradata script - edit all four literals
-- in place (Step A, Step B, and the re-bound of vt_1046_61 in Blocks 2/3) if the window changes.


-- ===== BLOCK 1: forward cascade — 1012/7020 clients, nearest 1046 (any origin), by timing bucket =====
-- QUESTION: for clients who revoke via the RBC-wide email page, does a 1046 (Avion) closure
--   show up at or near the same time - or not at all?
-- ROWS: 7 (6 categories + TOTAL)
-- GOOD LOOKS LIKE: if NEVER is large, choosing RBC-wide does not write 1046 and any split that
--   starts from 1046 closures undercounts RBC-wide.
-- WHAT TO DO WITH IT: paste to Claude
-- BUCKET DEFINITION: one row per client (client's earliest 1012/7020 write in window). Nearest
--   1046 = MIN(1046 write_dt - 1012 write_dt) among 1046 writes ON OR AFTER the 1012 date
--   (the cascade candidate); if none on/after, check for any 1046 write BEFORE the 1012 date
--   (already closed earlier, not a cascade result) -> BEFORE_THE_1012; if no 1046 write at
--   all (either side) -> NEVER.

WITH vt_1012_email61 AS (
    SELECT DISTINCT
        CLNT_NO,
        CAST(CHG_TMSTMP AS DATE) AS write_dt
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1012
      AND CLNT_CONSENT_TYP = 5002
      AND APP_SYS_CD = 7020
      AND CHG_TMSTMP >= DATE '2025-05-01'   -- PARAMETER BLOCK: WIN_START
      AND CHG_TMSTMP <  DATE '2026-06-26'   -- PARAMETER BLOCK: WIN_END_EXCL
),
vt_1046_61 AS (
    SELECT
        CLNT_NO,
        CAST(CHG_TMSTMP AS DATE)                                        AS write_dt,
        MAX(CASE WHEN APP_SYS_CD = 7020 THEN 1 ELSE 0 END)              AS is_email_origin
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1046
      AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-04-01'   -- PARAMETER BLOCK: WIDE_START
      AND CHG_TMSTMP <  DATE '2026-08-01'   -- PARAMETER BLOCK: WIDE_END_EXCL
    GROUP BY CLNT_NO, CAST(CHG_TMSTMP AS DATE)
),
first_1012 AS (
    SELECT CLNT_NO, MIN(write_dt) AS write_dt
    FROM vt_1012_email61
    GROUP BY CLNT_NO
),
fwd AS (
    SELECT f.CLNT_NO, MIN(n.write_dt - f.write_dt) AS fwd_diff
    FROM first_1012 f
    INNER JOIN vt_1046_61 n
        ON  n.CLNT_NO  = f.CLNT_NO
        AND n.write_dt >= f.write_dt
    GROUP BY f.CLNT_NO
),
bwd AS (
    SELECT DISTINCT f.CLNT_NO
    FROM first_1012 f
    INNER JOIN vt_1046_61 n
        ON  n.CLNT_NO  = f.CLNT_NO
        AND n.write_dt <  f.write_dt
),
classified AS (
    SELECT f.CLNT_NO,
           CASE
             WHEN fwd.fwd_diff = 0                       THEN 'SAME_DAY'
             WHEN fwd.fwd_diff = 1                        THEN 'WITHIN_1_DAY'
             WHEN fwd.fwd_diff BETWEEN 2 AND 14           THEN 'WITHIN_14_DAYS'
             WHEN fwd.fwd_diff > 14                       THEN 'LATER_THAN_14_DAYS'
             WHEN fwd.fwd_diff IS NULL AND bwd.CLNT_NO IS NOT NULL THEN 'BEFORE_THE_1012'
             ELSE 'NEVER'
           END AS cascade_bucket
    FROM first_1012 f
    LEFT JOIN fwd ON fwd.CLNT_NO = f.CLNT_NO
    LEFT JOIN bwd ON bwd.CLNT_NO = f.CLNT_NO
)
SELECT CAST(cascade_bucket AS VARCHAR(20)) AS cascade_bucket,
       CAST(COUNT(*) AS BIGINT)            AS clients
FROM classified
GROUP BY 1
UNION ALL
SELECT CAST('TOTAL' AS VARCHAR(20)),
       CAST(COUNT(*) AS BIGINT)
FROM classified
ORDER BY 1;


-- ===== BLOCK 2: reverse cascade — 1046/7020 closures, nearest 1012 (any origin), by timing bucket =====
-- QUESTION: reproduce the director's 80/20 from our side - of clients who closed 1046 via the
--   email page, how many also have a 1012 revocation nearby?
-- ROWS: 5 (4 categories + TOTAL)
-- GOOD LOOKS LIKE: NEVER ~= 80%, SAME_DAY ~= 19-20% (director's split, seen from the CPC side)
-- WHAT TO DO WITH IT: paste to Claude
-- BUCKET DEFINITION: one row per client (client's earliest 1046/7020 write in the PARAMETER
--   window). Nearest 1012 = MIN(ABS(1012 write_dt - 1046 write_dt)) in EITHER direction
--   (this check is symmetry, not causal ordering); diff > 14 days folds into NEVER along with
--   clients with no 1012 write at all - both mean "no meaningful association."

WITH vt_1012_email61 AS (
    SELECT DISTINCT
        CLNT_NO,
        CAST(CHG_TMSTMP AS DATE) AS write_dt
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1012
      AND CLNT_CONSENT_TYP = 5002
      AND APP_SYS_CD = 7020
      AND CHG_TMSTMP >= DATE '2025-05-01'   -- PARAMETER BLOCK: WIN_START
      AND CHG_TMSTMP <  DATE '2026-06-26'   -- PARAMETER BLOCK: WIN_END_EXCL
),
vt_1046_61 AS (
    SELECT
        CLNT_NO,
        CAST(CHG_TMSTMP AS DATE)                                        AS write_dt,
        MAX(CASE WHEN APP_SYS_CD = 7020 THEN 1 ELSE 0 END)              AS is_email_origin
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1046
      AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-04-01'   -- PARAMETER BLOCK: WIDE_START
      AND CHG_TMSTMP <  DATE '2026-08-01'   -- PARAMETER BLOCK: WIDE_END_EXCL
    GROUP BY CLNT_NO, CAST(CHG_TMSTMP AS DATE)
),
first_1046_email AS (
    SELECT CLNT_NO, MIN(write_dt) AS write_dt
    FROM vt_1046_61
    WHERE is_email_origin = 1
      AND write_dt >= DATE '2025-05-01'   -- PARAMETER BLOCK: WIN_START
      AND write_dt <  DATE '2026-06-26'   -- PARAMETER BLOCK: WIN_END_EXCL
    GROUP BY CLNT_NO
),
nearest AS (
    SELECT f.CLNT_NO, MIN(ABS(w.write_dt - f.write_dt)) AS abs_diff
    FROM first_1046_email f
    INNER JOIN vt_1012_email61 w
        ON w.CLNT_NO = f.CLNT_NO
    GROUP BY f.CLNT_NO
),
classified AS (
    SELECT f.CLNT_NO,
           CASE
             WHEN n.abs_diff = 0                THEN 'SAME_DAY'
             WHEN n.abs_diff = 1                 THEN 'WITHIN_1_DAY'
             WHEN n.abs_diff BETWEEN 2 AND 14     THEN 'WITHIN_14_DAYS'
             ELSE 'NEVER'
           END AS cascade_bucket
    FROM first_1046_email f
    LEFT JOIN nearest n ON n.CLNT_NO = f.CLNT_NO
)
SELECT CAST(cascade_bucket AS VARCHAR(20)) AS cascade_bucket,
       CAST(COUNT(*) AS BIGINT)            AS clients
FROM classified
GROUP BY 1
UNION ALL
SELECT CAST('TOTAL' AS VARCHAR(20)),
       CAST(COUNT(*) AS BIGINT)
FROM classified
ORDER BY 1;


-- ===== BLOCK 3: monthly denominators — 7020 1012 revocations vs 7020 1046 closures vs both same day =====
-- QUESTION: month by month, how do the two "unsub via email page" denominators compare, and how
--   often do they land on the same calendar day for the same client?
-- ROWS: <=15 (14 months in the PARAMETER window)
-- GOOD LOOKS LIKE: clnt_both_same_day tracks Block 1's SAME_DAY total when rolled up; a big gap
--   between clnt_7020_1012 and clnt_both_same_day confirms Block 1's NEVER share is not a
--   one-off month.
-- WHAT TO DO WITH IT: paste to Claude

WITH vt_1012_email61 AS (
    SELECT DISTINCT
        CLNT_NO,
        CAST(CHG_TMSTMP AS DATE) AS write_dt
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1012
      AND CLNT_CONSENT_TYP = 5002
      AND APP_SYS_CD = 7020
      AND CHG_TMSTMP >= DATE '2025-05-01'   -- PARAMETER BLOCK: WIN_START
      AND CHG_TMSTMP <  DATE '2026-06-26'   -- PARAMETER BLOCK: WIN_END_EXCL
),
vt_1046_61 AS (
    SELECT
        CLNT_NO,
        CAST(CHG_TMSTMP AS DATE)                                        AS write_dt,
        MAX(CASE WHEN APP_SYS_CD = 7020 THEN 1 ELSE 0 END)              AS is_email_origin
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1046
      AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-04-01'   -- PARAMETER BLOCK: WIDE_START
      AND CHG_TMSTMP <  DATE '2026-08-01'   -- PARAMETER BLOCK: WIDE_END_EXCL
    GROUP BY CLNT_NO, CAST(CHG_TMSTMP AS DATE)
),
d1012 AS (
    SELECT CLNT_NO, write_dt,
           TRIM(EXTRACT(YEAR FROM write_dt)) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM write_dt) < 10 THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM write_dt)) AS cohort_yyyymm
    FROM vt_1012_email61
),
d1046 AS (
    SELECT CLNT_NO, write_dt,
           TRIM(EXTRACT(YEAR FROM write_dt)) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM write_dt) < 10 THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM write_dt)) AS cohort_yyyymm
    FROM vt_1046_61
    WHERE is_email_origin = 1
      AND write_dt >= DATE '2025-05-01'   -- PARAMETER BLOCK: WIN_START
      AND write_dt <  DATE '2026-06-26'   -- PARAMETER BLOCK: WIN_END_EXCL
),
c1012 AS (
    SELECT cohort_yyyymm, CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS clnt_7020_1012
    FROM d1012
    GROUP BY 1
),
c1046 AS (
    SELECT cohort_yyyymm, CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS clnt_7020_1046
    FROM d1046
    GROUP BY 1
),
cboth AS (
    SELECT a.cohort_yyyymm, CAST(COUNT(DISTINCT a.CLNT_NO) AS BIGINT) AS clnt_both_same_day
    FROM d1012 a
    INNER JOIN d1046 b
        ON  b.CLNT_NO  = a.CLNT_NO
        AND b.write_dt = a.write_dt
    GROUP BY 1
)
SELECT CAST(COALESCE(c1012.cohort_yyyymm, c1046.cohort_yyyymm) AS VARCHAR(10)) AS cohort_yyyymm,
       COALESCE(c1012.clnt_7020_1012, 0)     AS clnt_7020_1012,
       COALESCE(c1046.clnt_7020_1046, 0)     AS clnt_7020_1046,
       COALESCE(cboth.clnt_both_same_day, 0) AS clnt_both_same_day
FROM c1012
FULL OUTER JOIN c1046 ON c1046.cohort_yyyymm = c1012.cohort_yyyymm
LEFT JOIN cboth        ON cboth.cohort_yyyymm = COALESCE(c1012.cohort_yyyymm, c1046.cohort_yyyymm)
ORDER BY 1;
