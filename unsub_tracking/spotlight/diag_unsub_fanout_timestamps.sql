-- diag_unsub_fanout_timestamps.sql — ONE DECISION: when a client shows
-- unsubs on 2+ deployments the same day, is that ONE mechanical write
-- (fan-out) or SEPARATE client actions?
-- Teradata-direct. Window: Jan-Apr 2026.
--
-- Distinct-timestamp counting is NOT enough (a batch writer can stagger
-- rows by milliseconds), so this buckets the TIME SPAN between the
-- first and last unsub row of the client-day:
--   0s / under 1 min        => batch-like: mechanical fan-out
--   1-60 min / over 1 hour  => human-like: separate deliberate actions
-- Read cross-campaign vs single-campaign rows separately: fan-out that
-- stays within a campaign's own waves only inflates event counts;
-- fan-out that crosses campaigns breaks per-campaign attribution.

SELECT
    CASE WHEN n_mne >= 2 THEN 'cross-campaign day'
         ELSE 'single-campaign day' END AS day_type,
    CASE WHEN span_sec = 0       THEN '1: 0s single moment'
         WHEN span_sec < 60      THEN '2: under 1 min'
         WHEN span_sec < 3600    THEN '3: 1-60 min'
         ELSE                         '4: over 1 hour' END AS spread,
    COUNT(*) AS client_days,
    SUM(n_treat) AS unsub_rows
FROM (
    SELECT consumer_id_hashed,
           CAST(disposition_dt_tm AS DATE) AS d,
           COUNT(DISTINCT SUBSTR(TREATMENT_ID, 8, 3)) AS n_mne,
           COUNT(DISTINCT TREATMENT_ID) AS n_treat,
           MAX(EXTRACT(HOUR FROM disposition_dt_tm) * 3600
             + EXTRACT(MINUTE FROM disposition_dt_tm) * 60
             + EXTRACT(SECOND FROM disposition_dt_tm))
         - MIN(EXTRACT(HOUR FROM disposition_dt_tm) * 3600
             + EXTRACT(MINUTE FROM disposition_dt_tm) * 60
             + EXTRACT(SECOND FROM disposition_dt_tm)) AS span_sec
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '2026-01-01'
      AND disposition_dt_tm <  DATE '2026-05-01'
      AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
      AND SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT TREATMENT_ID) >= 2
) t
GROUP BY 1, 2
ORDER BY 1, 2;
