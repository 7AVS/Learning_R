-- 63: does a 7020 (email page) 1012 revocation trickle down to ANY other consent? (Teradata-direct)
-- QUESTION: for clients who revoked RBC-wide email consent (1012 -> 5002) via the email page,
--   was ANY other PREF_ID written to 5002 the same day (any origin)? Pack 61 tested 1046 only.
-- ENGINE: Teradata-direct, pure CTEs, no volatile tables, no DROPs. Source = DDWV01.CPC_RB_PREF
--   (NEVER the LOG). Window = director's extract, 2025-05-01..2026-06-25. Grain = CLNT_NO.
-- RUN ORDER: Block 1 -> Block 2. Both feed the same record-and-compare step. Counts only.
-- COMPANION TO pack 61 v2 (50,660 7020 1012 revocations; 92% never get a 1046).
-- =============================================================================

-- ===== BLOCK 1: how many 1012 revocations carry ANY other same-day closure =====
-- QUESTION: of the 7020 1012 revocations, how many have at least one other PREF_ID closed
--   (5002) on the same calendar day, and how many closed preferences do they carry?
-- ROWS: <=8 (buckets of other-pref count: 0, 1, 2, 3-5, 6+) + TOTAL
-- GOOD LOOKS LIKE: if the 0 bucket holds ~90%+, 1012 is written alone and there is no
--   trickle-down to any program consent. If most rows sit in 3-5 or 6+, the cascade exists
--   and pack 61's 1046 result is Avion-specific.
-- WHAT TO DO WITH IT: record the result

WITH rev_1012 AS (
    SELECT CLNT_NO, MIN(CAST(CHG_TMSTMP AS DATE)) AS rev_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012
      AND CLNT_CONSENT_TYP = 5002
      AND APP_SYS_CD = 7020
      AND CHG_TMSTMP >= DATE '2025-05-01'   -- PARAMETER: window start
      AND CHG_TMSTMP <  DATE '2026-06-26'   -- PARAMETER: window end (exclusive)
    GROUP BY CLNT_NO
),
other_same_day AS (
    SELECT r.CLNT_NO, COUNT(DISTINCT p.PREF_ID) AS n_other_prefs_closed
    FROM rev_1012 r
    LEFT JOIN DDWV01.CPC_RB_PREF p
        ON  p.CLNT_NO = r.CLNT_NO
        AND p.PREF_ID <> 1012
        AND p.CLNT_CONSENT_TYP = 5002
        AND CAST(p.CHG_TMSTMP AS DATE) = r.rev_dt
    GROUP BY r.CLNT_NO
),
bucketed AS (
    SELECT CLNT_NO,
           CASE WHEN n_other_prefs_closed = 0 THEN '0_NONE'
                WHEN n_other_prefs_closed = 1 THEN '1'
                WHEN n_other_prefs_closed = 2 THEN '2'
                WHEN n_other_prefs_closed BETWEEN 3 AND 5 THEN '3_TO_5'
                ELSE '6_PLUS' END AS other_prefs_closed_same_day
    FROM other_same_day
)
SELECT CAST(other_prefs_closed_same_day AS VARCHAR(20)) AS other_prefs_closed_same_day,
       CAST(COUNT(*) AS BIGINT)                          AS clients
FROM bucketed
GROUP BY 1
UNION ALL
SELECT CAST('TOTAL' AS VARCHAR(20)), CAST(COUNT(*) AS BIGINT)
FROM bucketed
ORDER BY 1;


-- ===== BLOCK 2: which other preferences get closed alongside a 7020 1012 revocation =====
-- QUESTION: when something else IS closed the same day, which PREF_IDs are they, and from
--   which origin (APP_SYS_CD) - the email page itself (7020) or another system?
-- ROWS: <=15 (top 15 PREF_ID x APP_SYS_CD pairs by clients)
-- GOOD LOOKS LIKE: 1046 appears with ~3,293 clients (pack 61 v2 same-day figure) as a
--   consistency check. Anything above it is a program consent the page DOES cascade to.
-- WHAT TO DO WITH IT: record the result

WITH rev_1012 AS (
    SELECT CLNT_NO, MIN(CAST(CHG_TMSTMP AS DATE)) AS rev_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012
      AND CLNT_CONSENT_TYP = 5002
      AND APP_SYS_CD = 7020
      AND CHG_TMSTMP >= DATE '2025-05-01'   -- PARAMETER: window start
      AND CHG_TMSTMP <  DATE '2026-06-26'   -- PARAMETER: window end (exclusive)
    GROUP BY CLNT_NO
)
SELECT TOP 15
    p.PREF_ID,
    p.APP_SYS_CD,
    CAST(COUNT(DISTINCT p.CLNT_NO) AS BIGINT) AS clients
FROM rev_1012 r
INNER JOIN DDWV01.CPC_RB_PREF p
    ON  p.CLNT_NO = r.CLNT_NO
    AND p.PREF_ID <> 1012
    AND p.CLNT_CONSENT_TYP = 5002
    AND CAST(p.CHG_TMSTMP AS DATE) = r.rev_dt
GROUP BY 1, 2
ORDER BY 3 DESC;
