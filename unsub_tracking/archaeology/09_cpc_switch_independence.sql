-- =============================================================================
-- CPC switch independence — are switches written in bundles? do they contradict?
-- =============================================================================
-- Two decisions:
--   I1: do preference changes arrive as multi-switch bundles (form saves), and
--       which switches travel together with which values?
--   I2: does the CURRENT state contain "contradictions" (1002=No but explicit
--       Yes elsewhere)? Large count = switches stored fully independently, only
--       enforcement reconciles; ~zero = something cascades writes.
-- Output contract: I1a = 3 rows, I1b <= 20 rows, I2 <= 16 rows.
-- ENGINE: Teradata-direct.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- I1a: bundle size — how many switches change in one save (3 rows)
-- ---------------------------------------------------------------------------
-- Bundle = same client, same CHG_TMSTMP (microsecond identity = one transaction).
-- 2024+ window, March 2024 excluded (HSBC migration load would swamp it).
-- ---------------------------------------------------------------------------

WITH ev AS (
    SELECT CLNT_NO, CHG_TMSTMP, COUNT(*) AS switches_in_bundle
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE CHG_TMSTMP >= DATE '2024-01-01'
      AND NOT (CHG_TMSTMP >= DATE '2024-03-01' AND CHG_TMSTMP < DATE '2024-04-01')
    GROUP BY 1, 2
)
SELECT
    CASE WHEN switches_in_bundle = 1 THEN '1 (single-switch change)'
         WHEN switches_in_bundle BETWEEN 2 AND 5 THEN '2-5 (partial bundle)'
         ELSE '6+ (full form save)' END AS bundle_size,
    CAST(COUNT(*) AS BIGINT)            AS bundles,
    COUNT(DISTINCT CLNT_NO)             AS distinct_clients
FROM ev
GROUP BY 1
ORDER BY 1;


-- ---------------------------------------------------------------------------
-- I1b: which switches travel together, with which values (<= 20 rows)
-- ---------------------------------------------------------------------------
-- Same-timestamp pairs (PREF_ID a < b so each pair counts once). If 1002/5002
-- pairs dominate with channel/product 5002s, opt-outs cascade in practice;
-- if pairs are mostly 5001/5003 onboarding combos, bundles are form captures.
-- ---------------------------------------------------------------------------

SELECT TOP 20
    a.PREF_ID           AS pref_a,
    a.CLNT_CONSENT_TYP  AS consent_a,
    b.PREF_ID           AS pref_b,
    b.CLNT_CONSENT_TYP  AS consent_b,
    CAST(COUNT(*) AS BIGINT) AS pair_events,
    COUNT(DISTINCT a.CLNT_NO) AS distinct_clients
FROM DDWV01.CPC_RB_PREF_LOG a
INNER JOIN DDWV01.CPC_RB_PREF_LOG b
    ON  b.CLNT_NO    = a.CLNT_NO
    AND b.CHG_TMSTMP = a.CHG_TMSTMP
    AND b.PREF_ID    > a.PREF_ID
WHERE a.CHG_TMSTMP >= DATE '2024-01-01'
  AND NOT (a.CHG_TMSTMP >= DATE '2024-03-01' AND a.CHG_TMSTMP < DATE '2024-04-01')
GROUP BY 1, 2, 3, 4
ORDER BY pair_events DESC;


-- ---------------------------------------------------------------------------
-- I2: contradiction census — current state (<= 16 rows)
-- ---------------------------------------------------------------------------
-- Latest state per (client, pref) over 1002 + Banking channels + product prefs.
-- Read: rows with state_1002 = 5002 AND any_explicit_yes_other = 1 are the
-- "contradictions" — client is entity-opted-out yet holds explicit Yes switches
-- underneath. Big count = independent storage (enforcement must reconcile);
-- ~zero = opt-out cascades to the other switches at write time.
-- ---------------------------------------------------------------------------

WITH latest AS (
    SELECT
        CLNT_NO,
        PREF_ID,
        CLNT_CONSENT_TYP,
        ROW_NUMBER() OVER (PARTITION BY CLNT_NO, PREF_ID
                           ORDER BY CHG_TMSTMP DESC) AS rn
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID IN (1002,
                      1007, 1008, 1009, 1012, 1013, 1048,      -- Banking channels
                      1004, 1006, 1010, 1023, 1024, 1025, 1026, 1044)  -- product prefs
),
flags AS (
    SELECT
        CLNT_NO,
        MAX(CASE WHEN PREF_ID = 1002 THEN CLNT_CONSENT_TYP END)                    AS state_1002,
        MAX(CASE WHEN PREF_ID <> 1002 AND CLNT_CONSENT_TYP = 5001 THEN 1 ELSE 0 END) AS any_explicit_yes_other,
        MAX(CASE WHEN PREF_ID <> 1002 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS any_explicit_no_other
    FROM latest
    WHERE rn = 1
    GROUP BY 1
)
SELECT
    state_1002,                          -- NULL = never set (blank default)
    any_explicit_yes_other,
    any_explicit_no_other,
    CAST(COUNT(*) AS BIGINT)  AS clients
FROM flags
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
