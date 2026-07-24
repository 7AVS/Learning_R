-- =============================================================================
-- CPC master cube [IN-ENV EXTRACT — pivot base, NOT for screenshots]
-- =============================================================================
-- The full picture in one extract: switch x position x writing system x
-- save-shape. Pivot in-environment (rows: PREF_ID; columns: consent/system;
-- filters: bundle size — whatever the question needs).
-- Dimensions: PREF_ID, CLNT_CONSENT_TYP, APP_SYS_CD, bundle_size bucket.
-- Grain note: change_rows counts log rows; distinct_clients dedups per cell.
-- Window: 2024+ organic (HSBC 2024-03 excluded). For all-time, drop the two
-- CHG_TMSTMP predicates (and expect the HSBC block to appear).
-- Expected size: a few thousand sparse rows.
-- ENGINE: Teradata-direct.
-- =============================================================================

WITH ev AS (
    SELECT
        CLNT_NO,
        CHG_TMSTMP,
        COUNT(*) AS switches_in_bundle
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE CHG_TMSTMP >= DATE '2024-01-01'
      AND NOT (CHG_TMSTMP >= DATE '2024-03-01' AND CHG_TMSTMP < DATE '2024-04-01')
    GROUP BY 1, 2
)
SELECT
    l.PREF_ID,
    l.CLNT_CONSENT_TYP,
    l.APP_SYS_CD,
    CASE WHEN ev.switches_in_bundle = 1 THEN '1'
         WHEN ev.switches_in_bundle BETWEEN 2 AND 5 THEN '2-5'
         ELSE '6+' END          AS bundle_size,
    CAST(COUNT(*) AS BIGINT)    AS change_rows,
    COUNT(DISTINCT l.CLNT_NO)   AS distinct_clients
FROM DDWV01.CPC_RB_PREF_LOG l
INNER JOIN ev
    ON  ev.CLNT_NO    = l.CLNT_NO
    AND ev.CHG_TMSTMP = l.CHG_TMSTMP
WHERE l.CHG_TMSTMP >= DATE '2024-01-01'
  AND NOT (l.CHG_TMSTMP >= DATE '2024-03-01' AND l.CHG_TMSTMP < DATE '2024-04-01')
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;
