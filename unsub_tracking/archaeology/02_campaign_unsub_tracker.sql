-- =============================================================================
-- Campaign Unsub Tracker — MNE x month x disposition, 2024+
-- =============================================================================
-- One long-format extract: every campaign (MNE = SUBSTR(TREATMENT_ID, 8, 3)),
-- by month, by disposition_cd (1=sent 2=opened 3=clicked 4=unsub 5=hardbounce
-- 6=complaint). EVENT alone — no join, no fan-out risk; MNE and consumer
-- identity live on EVENT itself (see schemas/vendor_feedback_tables_schema.md).
-- Counts only; rates/pivots downstream.
-- ENGINE: Teradata-direct.
-- =============================================================================

-- T1: the tracker
SELECT
    SUBSTR(TREATMENT_ID, 8, 3)      AS mne,
    EXTRACT(YEAR FROM disposition_dt_tm) * 100
      + EXTRACT(MONTH FROM disposition_dt_tm) AS event_month_yyyymm,
    disposition_cd,
    CAST(COUNT(*) AS BIGINT)        AS event_rows,
    COUNT(DISTINCT consumer_id_hashed) AS distinct_consumers
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_dt_tm >= DATE '2024-01-01'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- T2: guard — rows with NULL/short TREATMENT_ID that fall out of the MNE cut
SELECT
    EXTRACT(YEAR FROM disposition_dt_tm) * 100
      + EXTRACT(MONTH FROM disposition_dt_tm) AS event_month_yyyymm,
    disposition_cd,
    CAST(COUNT(*) AS BIGINT)        AS unattributed_rows
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_dt_tm >= DATE '2024-01-01'
  AND (TREATMENT_ID IS NULL OR CHARACTER_LENGTH(TRIM(TREATMENT_ID)) < 10)
GROUP BY 1, 2
ORDER BY 1, 2;
