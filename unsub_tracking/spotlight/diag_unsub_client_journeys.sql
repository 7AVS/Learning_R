-- diag_unsub_client_journeys.sql — FOLLOW THE CLIENT: for 5 sampled
-- clients with same-day unsubs on 2+ campaigns, pull their FULL event
-- journey (every disposition, every treatment) and watch the mechanism.
-- Journey log: 1 sent -> 2 opened -> 3 clicked -> 4/5/6 outcome, one row
-- per stage per treatment.
-- Read: unsubbed treatment shows open/click just before its disposition-4
-- row => real per-email click. Disposition 4 on a treatment with no sent/
-- open activity => mechanical write. Teradata-direct.

WITH multi AS (
    SELECT consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '2026-01-01'
      AND disposition_dt_tm <  DATE '2026-05-01'
      AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
      AND SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    GROUP BY 1, CAST(disposition_dt_tm AS DATE)
    HAVING COUNT(DISTINCT SUBSTR(TREATMENT_ID, 8, 3)) >= 2
),
pick AS (
    SELECT DISTINCT consumer_id_hashed FROM multi SAMPLE 5
)
SELECT e.consumer_id_hashed,
       e.TREATMENT_ID,
       SUBSTR(e.TREATMENT_ID, 8, 3) AS mne,
       e.disposition_cd,
       e.disposition_dt_tm
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN pick p
   ON p.consumer_id_hashed = e.consumer_id_hashed
WHERE e.disposition_dt_tm >= DATE '2025-10-01'
  AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
  AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
ORDER BY e.consumer_id_hashed, e.disposition_dt_tm, e.TREATMENT_ID;
