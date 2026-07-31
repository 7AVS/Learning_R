-- preflight4.sql
-- Q12 showed 3,029,598 (client, treatment) pairs with sends more than 30 days apart. That
-- contradicts the structure of a TACTIC_ID, which encodes YYYY + Julian day + program - one day,
-- one program, so one client cannot have two sends under it a month apart.
--
-- P7 showed the table carries at least three shapes of TREATMENT_ID:
--   2024313BBP   date-encoded: year 2024, Julian 313, program BBP. SUBSTR(x,8,3) = 'BBP'.
--   CABVRSN1     not date-encoded
--   DEFAULT      not an identifier at all
--
-- If non-dated ids like DEFAULT carry a whole year of email under one key, that explains the
-- entire >30-day bucket without any real campaign sending twice.
-- ENGINE: Teradata-direct. Every query <= 20 rows.


-- =========================================================================================
-- Q15. THE TEST. Split the multi-send pairs by whether TREATMENT_ID is date-encoded.
-- Date-encoded = 10 chars, first 4 numeric (the year).
-- If the >30-day mass sits almost entirely in NOT_DATED, real campaigns behave exactly as
-- Andre described and only the junk ids misbehave.
-- =========================================================================================
SELECT CASE WHEN CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
             AND SUBSTR(TRIM(TREATMENT_ID), 1, 4) BETWEEN '2020' AND '2030'
            THEN 'DATED (tactic id)' ELSE 'NOT DATED (junk/vendor id)' END AS id_shape,
       CASE WHEN span_days = 0   THEN '00 same day'
            WHEN span_days <= 7  THEN '01 1-7 days'
            WHEN span_days <= 30 THEN '02 8-30 days'
            ELSE                      '03 over 30 days' END AS spread,
       COUNT(*) AS client_treatment_pairs
FROM (
    SELECT consumer_id_hashed, TREATMENT_ID,
           (MAX(CAST(disposition_dt_tm AS DATE))
          - MIN(CAST(disposition_dt_tm AS DATE))) AS span_days
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '2025-08-01'
      AND disposition_dt_tm <  DATE '2026-08-01'
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT CAST(disposition_dt_tm AS DATE)) > 1
) t
GROUP BY 1, 2
ORDER BY 1, 2;


-- =========================================================================================
-- Q16. How much of the whole window is logged under non-dated ids? If it is a large share,
-- every "unsubs by campaign" number is really "unsubs by campaign, plus one giant unattributed
-- bucket" - and that bucket needs naming before anything ships.
-- =========================================================================================
SELECT CASE WHEN CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
             AND SUBSTR(TRIM(TREATMENT_ID), 1, 4) BETWEEN '2020' AND '2030'
            THEN 'DATED (tactic id)' ELSE 'NOT DATED (junk/vendor id)' END AS id_shape,
       COUNT(DISTINCT TREATMENT_ID)        AS distinct_ids,
       COUNT(*)                            AS send_rows,
       COUNT(DISTINCT consumer_id_hashed)  AS distinct_clients
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_cd = 1
  AND disposition_dt_tm >= DATE '2025-08-01'
  AND disposition_dt_tm <  DATE '2026-08-01'
GROUP BY 1;


-- =========================================================================================
-- Q17. Name the biggest non-dated ids. These are what SUBSTR(TREATMENT_ID,8,3) turns into a
-- meaningless MNE. Worth knowing what they actually are before excluding or keeping them.
-- =========================================================================================
SELECT TOP 15
       TREATMENT_ID,
       SUBSTR(TRIM(TREATMENT_ID), 8, 3)   AS mne_substring_yields,
       COUNT(*)                           AS send_rows,
       COUNT(DISTINCT consumer_id_hashed) AS distinct_clients
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_cd = 1
  AND disposition_dt_tm >= DATE '2025-08-01'
  AND disposition_dt_tm <  DATE '2026-08-01'
  AND NOT (CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
           AND SUBSTR(TRIM(TREATMENT_ID), 1, 4) BETWEEN '2020' AND '2030')
GROUP BY 1, 2
ORDER BY send_rows DESC;


-- =========================================================================================
-- Q18. For DATED ids only - does a client ever legitimately get two sends on different days
-- under one tactic id? If this comes back near zero, Andre's rule holds exactly and the day-grain
-- collapse in spotlight.py is unnecessary for real campaigns (harmless, but unnecessary).
-- =========================================================================================
SELECT COUNT(*) AS dated_pairs_with_multiple_send_days
FROM (
    SELECT consumer_id_hashed, TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '2025-08-01'
      AND disposition_dt_tm <  DATE '2026-08-01'
      AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
      AND SUBSTR(TRIM(TREATMENT_ID), 1, 4) BETWEEN '2020' AND '2030'
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT CAST(disposition_dt_tm AS DATE)) > 1
) t;
