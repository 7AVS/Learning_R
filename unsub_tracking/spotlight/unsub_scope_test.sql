-- unsub_scope_test.sql
-- Is disposition_cd=4 a PER-LIST unsubscribe or a GLOBAL opt-out?
--
-- v1 failed two ways and both were design errors, not data problems:
--   Q2 blew spool - it self-joined the whole bank-wide mailed base. The question only concerns
--      clients who actually unsubscribed (~320K), not everyone who was mailed (10M+).
--   Q3 threw 3706 - malformed Teradata interval cast. It never needed interval arithmetic:
--      "are these timestamps identical" is COUNT(DISTINCT ts), no subtraction.
--
-- ENGINE: Teradata-direct (DTZV01.*, Teradata syntax, no catalog prefix).
-- Every query returns <= 20 rows.
--
-- RERUN NOTE: volatile tables persist for the session. If a rerun says "table already exists",
-- run the two DROP statements below first. They are commented out so a first run does not fail.
--
-- DROP TABLE vt_unsub_clients;
-- DROP TABLE vt_unsub_sends;


-- ============================================================================================
-- SETUP. Volatile tables + stats. Required, not optional: the self-join in Q2 is exactly the
-- unconstrained-product-join shape TDWM blocks, and COLLECT STATISTICS is what stops it.
-- Window is 3 months on purpose - the MECHANISM does not need 12 months to reveal itself, and a
-- narrow window is what keeps this inside spool.
-- ============================================================================================
CREATE VOLATILE TABLE vt_unsub_clients AS (
    SELECT consumer_id_hashed,
           TREATMENT_ID,
           MIN(disposition_dt_tm)             AS unsub_tm,
           MIN(CAST(disposition_dt_tm AS DATE)) AS unsub_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '2026-04-01'
      AND disposition_dt_tm <  DATE '2026-07-01'
    GROUP BY 1, 2
) WITH DATA PRIMARY INDEX (consumer_id_hashed) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (consumer_id_hashed) ON vt_unsub_clients;
COLLECT STATISTICS COLUMN (consumer_id_hashed, TREATMENT_ID) ON vt_unsub_clients;

-- Sends to those same clients only. Joining to the volatile table first is what keeps this small.
CREATE VOLATILE TABLE vt_unsub_sends AS (
    SELECT e.consumer_id_hashed,
           e.TREATMENT_ID,
           MIN(CAST(e.disposition_dt_tm AS DATE)) AS send_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    JOIN (SELECT DISTINCT consumer_id_hashed FROM vt_unsub_clients) u
      ON u.consumer_id_hashed = e.consumer_id_hashed
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2026-03-01'   -- 30 days of run-up before the unsub window
      AND e.disposition_dt_tm <  DATE '2026-07-01'
    GROUP BY 1, 2
) WITH DATA PRIMARY INDEX (consumer_id_hashed) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (consumer_id_hashed) ON vt_unsub_sends;
COLLECT STATISTICS COLUMN (consumer_id_hashed, TREATMENT_ID) ON vt_unsub_sends;


-- ============================================================================================
-- Q3 FIRST - it is the cheapest and the most decisive.
-- For clients with unsub events on 2+ treatments: how many DISTINCT timestamps are there?
--
--   n_treatments = 3, distinct_ts = 1  -> one action written to three treatments. GLOBAL.
--   n_treatments = 3, distinct_ts = 3  -> three separate events. PER-LIST.
--
-- No interval arithmetic. Expect: <= 12 rows.
-- ============================================================================================
SELECT n_treatments,
       distinct_ts,
       CASE WHEN distinct_ts = 1            THEN 'GLOBAL - one timestamp, many treatments'
            WHEN distinct_ts = n_treatments THEN 'PER-LIST - one timestamp each'
            ELSE                                 'PARTIAL - some share a timestamp' END AS reading,
       COUNT(*) AS clients
FROM (
    SELECT consumer_id_hashed,
           COUNT(DISTINCT TREATMENT_ID) AS n_treatments,
           COUNT(DISTINCT unsub_tm)     AS distinct_ts
    FROM vt_unsub_clients
    GROUP BY 1
    HAVING COUNT(DISTINCT TREATMENT_ID) >= 2
) x
GROUP BY 1, 2, 3
ORDER BY clients DESC;


-- ============================================================================================
-- Q3b. How many unsubscribers have more than one treatment at all? Context for Q3 - if almost
-- nobody does, the whole question is a rounding error and per-campaign attribution is safe.
-- Expect: <= 10 rows.
-- ============================================================================================
SELECT CASE WHEN n_treatments = 1 THEN '1 treatment'
            WHEN n_treatments = 2 THEN '2 treatments'
            WHEN n_treatments <= 4 THEN '3-4 treatments'
            WHEN n_treatments <= 9 THEN '5-9 treatments'
            ELSE '10+ treatments' END AS treatments_with_unsub,
       COUNT(*) AS clients
FROM (
    SELECT consumer_id_hashed, COUNT(DISTINCT TREATMENT_ID) AS n_treatments
    FROM vt_unsub_clients
    GROUP BY 1
) y
GROUP BY 1
ORDER BY 1;


-- ============================================================================================
-- Q2 REBUILT. Of the campaigns sent to a client in the 30 days before their unsub, how many
-- carry the unsub? Now bounded to unsubscribers only, so it fits in spool.
--
--   unsub_treatments = 1              -> PER-LIST
--   unsub_treatments = sent_in_window -> GLOBAL
--
-- Expect: <= 20 rows.
-- ============================================================================================
SELECT sent_in_window,
       unsub_treatments,
       CASE WHEN unsub_treatments = 1              THEN 'PER-LIST'
            WHEN unsub_treatments = sent_in_window THEN 'GLOBAL'
            ELSE                                        'PARTIAL' END AS reading,
       COUNT(*) AS clients
FROM (
    SELECT c.consumer_id_hashed,
           COUNT(DISTINCT s.TREATMENT_ID) AS sent_in_window,
           COUNT(DISTINCT CASE WHEN u2.TREATMENT_ID IS NOT NULL
                               THEN s.TREATMENT_ID END) AS unsub_treatments
    FROM (SELECT consumer_id_hashed, MIN(unsub_dt) AS first_unsub_dt
          FROM vt_unsub_clients GROUP BY 1) c
    JOIN vt_unsub_sends s
      ON s.consumer_id_hashed = c.consumer_id_hashed
     AND s.send_dt <= c.first_unsub_dt
     AND s.send_dt >= c.first_unsub_dt - 30       -- 30-day attribution window
    LEFT JOIN vt_unsub_clients u2
      ON u2.consumer_id_hashed = s.consumer_id_hashed
     AND u2.TREATMENT_ID = s.TREATMENT_ID
    GROUP BY 1
) z
WHERE unsub_treatments > 0
GROUP BY 1, 2, 3
ORDER BY clients DESC;


-- ============================================================================================
-- Q1. Send-to-unsub lag. Sets the attribution window with evidence instead of a guess.
-- Bounded to unsubscribers via the volatile tables, so it will not spool.
-- Expect: <= 10 rows.
-- ============================================================================================
SELECT CASE WHEN lag_days <  0  THEN 'negative - investigate'
            WHEN lag_days =  0  THEN '00 same day'
            WHEN lag_days =  1  THEN '01 next day'
            WHEN lag_days <= 3  THEN '02 2-3 days'
            WHEN lag_days <= 7  THEN '03 4-7 days'
            WHEN lag_days <= 14 THEN '04 8-14 days'
            WHEN lag_days <= 30 THEN '05 15-30 days'
            ELSE                     '06 over 30 days' END AS lag_bucket,
       COUNT(*) AS unsub_events
FROM (
    SELECT (u.unsub_dt - s.send_dt) AS lag_days
    FROM vt_unsub_clients u
    JOIN vt_unsub_sends s
      ON s.consumer_id_hashed = u.consumer_id_hashed
     AND s.TREATMENT_ID = u.TREATMENT_ID
) l
GROUP BY 1
ORDER BY 1;


-- ============================================================================================
-- HOW TO READ IT
--
-- Q3 distinct_ts = 1 dominant
--     -> One opt-out written against every open treatment. Per-campaign unsub attribution is not
--        real. Spotlight 1's per-campaign numbers are "whose email was the last straw", never
--        "which campaign causes unsubs", and every outcome metric goes client-level.
--
-- Q3 distinct_ts = n_treatments dominant
--     -> Genuinely separate events. Per-campaign attribution stands, last-touch within the window
--        is the right rule.
--
-- Q3b mostly "1 treatment"
--     -> The ambiguity affects a small tail. Attribute per-campaign and note the exception.
--
-- Q1 tells you where to cut the window. If the mass is inside 7 days, 30 is already generous.
-- ============================================================================================
