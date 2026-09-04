-- 65: tracking the CPC backfeed catch-up runs (Teradata-direct, pure CTEs, no volatile tables)
-- QUESTION: since the gap was confirmed to MarTech, are catch-up loads landing in CPC, how big are they,
--   and how far behind the Salesforce unsub do they run? A self-test unsub on 2026-08-05 appeared in CPC
--   on 2026-09-02 at an hour that does not match the usual 7020 batch (about 04:24).
-- SOURCE: DDWV01.CPC_RB_PREF (never the LOG). Window from 2026-07-01. Grain = client x write date.
-- RUN ORDER: Block 1 -> Block 2. Counts only.
-- =============================================================================

-- ===== BLOCK 1: 1012 revocations by write day and hour, all origins, since 2026-07-01 =====
-- QUESTION: on which days and hours do consent revocations land, and which origin writes them?
--   The regular 7020 backfeed sits in one early-morning hour; anything else is a separate load.
-- ROWS: <= 20 (largest day-hour-origin cells)
-- GOOD LOOKS LIKE: a steady 7020 row per day at the usual hour, plus a few large off-hour rows
--   after mid-August 2026 = the catch-up runs. Their client counts size each run.
-- WHAT TO DO WITH IT: record; compare the off-hour dates with the dates the fix was announced

SELECT TOP 20
    CAST(CHG_TMSTMP AS DATE)                                   AS write_dt,
    EXTRACT(HOUR FROM CHG_TMSTMP)                              AS write_hour,
    APP_SYS_CD,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT)                    AS clients
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012
  AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2026-07-01'   -- PARAMETER: tracking window start
GROUP BY 1, 2, 3
ORDER BY 4 DESC;


-- ===== BLOCK 2: lag from the Salesforce unsub to the CPC write, 7020 writes since 2026-08-01 =====
-- QUESTION: for each 7020 revocation since August, how long after the client's most recent Salesforce
--   unsub (disposition_cd = 4) did it land? A 15-60 day mass that was absent before is the catch-up.
-- ROWS: <= 8 (lag buckets + TOTAL)
-- GOOD LOOKS LIKE: before the fix, nearly everything is NO_SF_UNSUB (path 2, the form). If the fix
--   drains path 1, the 15-30 and 31-60 day buckets fill up.
-- WHAT TO DO WITH IT: record; rerun monthly while the fix is in flight

WITH cpc_w AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS write_dt, EXTRACT(HOUR FROM CHG_TMSTMP) AS write_hour
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012
      AND CLNT_CONSENT_TYP = 5002
      AND APP_SYS_CD = 7020
      AND CHG_TMSTMP >= DATE '2026-08-01'   -- PARAMETER: fix window start
),
sf_u AS (
    SELECT m.CLNT_NO, CAST(e.disposition_dt_tm AS DATE) AS unsub_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER WHERE CLNT_NO IS NOT NULL) m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2026-01-01'
      AND e.TREATMENT_ID NOT IN ('DEFAULT', 'CABVRSN1')
),
lagged AS (
    SELECT w.CLNT_NO, w.write_dt, w.write_hour,
           MAX(CASE WHEN u.unsub_dt <= w.write_dt THEN u.unsub_dt END) AS last_unsub_before
    FROM cpc_w w
    LEFT JOIN sf_u u ON u.CLNT_NO = w.CLNT_NO
    GROUP BY w.CLNT_NO, w.write_dt, w.write_hour
),
bucketed AS (
    SELECT CLNT_NO, write_dt,
           CASE WHEN write_hour = 4 THEN 'REGULAR_HOUR' ELSE 'OFF_HOUR' END AS batch_type  -- 2026-09-04: regular 7020 batch is hour 4; the 2026-09-02 catch-up ran at hour 5,
           CASE WHEN last_unsub_before IS NULL                   THEN 'NO_SF_UNSUB'
                WHEN write_dt - last_unsub_before <= 1           THEN 'LAG_0_1_DAY'
                WHEN write_dt - last_unsub_before <= 14          THEN 'LAG_2_14_DAYS'
                WHEN write_dt - last_unsub_before <= 30          THEN 'LAG_15_30_DAYS'
                WHEN write_dt - last_unsub_before <= 60          THEN 'LAG_31_60_DAYS'
                ELSE 'LAG_61_PLUS_DAYS' END AS lag_bucket
    FROM lagged
)
SELECT CAST(batch_type AS VARCHAR(15)) AS batch_type,
       CAST(lag_bucket AS VARCHAR(20)) AS lag_bucket,
       CAST(COUNT(*) AS BIGINT)        AS clients
FROM bucketed
GROUP BY 1, 2
UNION ALL
SELECT CAST('TOTAL' AS VARCHAR(15)), CAST('ALL' AS VARCHAR(20)), CAST(COUNT(*) AS BIGINT)
FROM bucketed
ORDER BY 1, 2;
