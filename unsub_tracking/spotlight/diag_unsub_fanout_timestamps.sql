-- diag_unsub_fanout_timestamps.sql — ONE DECISION: when a client shows
-- unsubs on 2+ deployments the same day, is that ONE write moment
-- (mechanical fan-out) or SEPARATE moments (separate client actions)?
-- Teradata-direct. Window: Jan-Apr 2026.
-- Read: ts_pattern 'one write moment' dominating => fan-out, our
-- repeat-unsub interpretation is wrong. 'each its own moment' => real
-- separate actions. If single-campaign days ALSO all collapse to one
-- moment, disposition_dt_tm may be a batch stamp and this test is void.

SELECT
    CASE WHEN n_mne >= 2 THEN 'cross-campaign day'
         ELSE 'single-campaign day' END AS day_type,
    CASE WHEN n_ts = 1 THEN 'one write moment'
         WHEN n_ts = n_treat THEN 'each its own moment'
         ELSE 'mixed' END AS ts_pattern,
    COUNT(*) AS client_days,
    SUM(n_treat) AS unsub_rows
FROM (
    SELECT consumer_id_hashed,
           CAST(disposition_dt_tm AS DATE) AS d,
           COUNT(DISTINCT SUBSTR(TREATMENT_ID, 8, 3)) AS n_mne,
           COUNT(DISTINCT TREATMENT_ID) AS n_treat,
           COUNT(DISTINCT disposition_dt_tm) AS n_ts
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
