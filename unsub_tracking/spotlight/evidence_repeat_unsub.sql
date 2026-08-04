-- evidence_repeat_unsub.sql - SIMPLE EVIDENCE: same client, 2+ unsub events,
-- different campaigns (MNE), different TREATMENT_IDs, same calendar month.
-- Teradata-direct. Window: Jan-Apr 2026 (Window A). Two statements:
-- [1] how many such clients exist; [2] ten real examples with full trails.

-- [1] COUNT: clients with unsubs on 2+ different campaigns within one calendar month
WITH ek AS (
    SELECT consumer_id_hashed, TREATMENT_ID,
           SUBSTR(TREATMENT_ID, 8, 3) AS mne,
           CAST(disposition_dt_tm AS DATE) AS unsub_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '2026-01-01'
      AND disposition_dt_tm <  DATE '2026-05-01'
      AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
      AND SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    GROUP BY 1, 2, 3, 4
),
j AS (
    SELECT m.CLNT_NO, ek.consumer_id_hashed, ek.TREATMENT_ID, ek.mne, ek.unsub_dt
    FROM ek
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE load_tm >= DATE '2025-10-01'
                  AND CLNT_NO IS NOT NULL) m
      ON m.consumer_id_hashed = ek.consumer_id_hashed
     AND m.TREATMENT_ID = ek.TREATMENT_ID
)
SELECT ym, COUNT(*) AS clients_multi_campaign_same_month
FROM (
    SELECT CLNT_NO,
           EXTRACT(YEAR FROM unsub_dt) * 100 + EXTRACT(MONTH FROM unsub_dt) AS ym
    FROM j
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT mne) >= 2
) t
GROUP BY 1
ORDER BY 1;

-- [2] TEN REAL TRAILS: same CLNT_NO, different MNE, different TREATMENT_ID,
-- (usually different consumer_id_hashed = different address), same month.
WITH ek AS (
    SELECT consumer_id_hashed, TREATMENT_ID,
           SUBSTR(TREATMENT_ID, 8, 3) AS mne,
           CAST(disposition_dt_tm AS DATE) AS unsub_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '2026-01-01'
      AND disposition_dt_tm <  DATE '2026-05-01'
      AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
      AND SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    GROUP BY 1, 2, 3, 4
),
j AS (
    SELECT m.CLNT_NO, ek.consumer_id_hashed, ek.TREATMENT_ID, ek.mne, ek.unsub_dt
    FROM ek
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE load_tm >= DATE '2025-10-01'
                  AND CLNT_NO IS NOT NULL) m
      ON m.consumer_id_hashed = ek.consumer_id_hashed
     AND m.TREATMENT_ID = ek.TREATMENT_ID
),
multi AS (
    SELECT CLNT_NO,
           EXTRACT(YEAR FROM unsub_dt) * 100 + EXTRACT(MONTH FROM unsub_dt) AS ym
    FROM j
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT mne) >= 2
),
pick AS (
    SELECT CLNT_NO, ym FROM multi SAMPLE 10
)
SELECT j.CLNT_NO,
       j.mne,
       j.TREATMENT_ID,
       j.consumer_id_hashed,
       j.unsub_dt
FROM j
INNER JOIN pick p
   ON p.CLNT_NO = j.CLNT_NO
  AND p.ym = EXTRACT(YEAR FROM j.unsub_dt) * 100 + EXTRACT(MONTH FROM j.unsub_dt)
ORDER BY j.CLNT_NO, j.unsub_dt;
