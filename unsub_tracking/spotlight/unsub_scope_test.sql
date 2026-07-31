-- unsub_scope_test.sql
-- Settles one question: is disposition_cd=4 a PER-LIST unsubscribe or a GLOBAL opt-out?
--
-- Everything downstream depends on the answer. If it is global, "unsubs by campaign" means
-- "whose email was the last straw", the campaign that mails most tops any raw count mechanically,
-- and outcome metrics must be client-level. If it is per-list, per-campaign attribution is real.
--
-- No prior result is assumed. Pack 20's "86% same-day" is not evidence of mechanism - same-day is
-- equally consistent with a client clicking two emails in one sitting. This tests the mechanism.
--
-- ENGINE: Teradata-direct (DTZV01.*, no catalog prefix, Teradata syntax).
-- Every query returns <= 25 rows. Run them one at a time, in order.
--
-- COLUMNS USED - all confirmed in UNSUB_TRACKING_KNOWLEDGE.md:117,130,145,151-165:
--   DTZV01.VENDOR_FEEDBACK_EVENT : consumer_id_hashed, TREATMENT_ID, disposition_cd,
--                                  disposition_dt_tm   (1=sent, 4=unsubscribed)
--   DTZV01.VENDOR_FEEDBACK_MASTER: consumer_id_hashed, TREATMENT_ID, CLNT_NO, load_tm
-- No treatment end-date column is assumed to exist. The deployment window is DERIVED from the
-- sends, which is exact per campaign and needs no lookup.


-- ============================================================================================
-- Q1. How long after a send does an unsubscribe actually happen?
-- A TREATMENT_ID is one deployment wave with one send date, so there is no campaign window to
-- derive - the only window that matters is the response lag. Measure it instead of guessing 30
-- or 90 days.
-- Read: find the bucket where the mass stops. That is the attribution window, evidenced.
-- Expect: <= 10 rows.
-- ============================================================================================
WITH snd AS (
    SELECT consumer_id_hashed, TREATMENT_ID,
           MIN(CAST(disposition_dt_tm AS DATE)) AS send_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '2025-08-01'
    GROUP BY 1, 2
),
uns AS (
    SELECT consumer_id_hashed, TREATMENT_ID,
           MIN(CAST(disposition_dt_tm AS DATE)) AS unsub_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '2025-08-01'
    GROUP BY 1, 2
),
lag AS (
    SELECT (u.unsub_dt - s.send_dt) AS lag_days
    FROM uns u
    JOIN snd s ON s.consumer_id_hashed = u.consumer_id_hashed
              AND s.TREATMENT_ID = u.TREATMENT_ID
)
SELECT CASE WHEN lag_days <  0  THEN 'negative - unsub before send, investigate'
            WHEN lag_days =  0  THEN '00 same day'
            WHEN lag_days =  1  THEN '01 next day'
            WHEN lag_days <= 3  THEN '02 2-3 days'
            WHEN lag_days <= 7  THEN '03 4-7 days'
            WHEN lag_days <= 14 THEN '04 8-14 days'
            WHEN lag_days <= 30 THEN '05 15-30 days'
            WHEN lag_days <= 60 THEN '06 31-60 days'
            WHEN lag_days <= 90 THEN '07 61-90 days'
            ELSE                     '08 over 90 days' END AS lag_bucket,
       COUNT(*) AS unsub_events
FROM lag
GROUP BY 1
ORDER BY 1;


-- ============================================================================================
-- Q2. THE TEST. Among clients exposed to 2+ campaigns whose send windows OVERLAP, does the
-- unsubscribe land on ONE treatment or on ALL of them?
--
--   unsub_treatments = 1              -> PER-LIST. Attribution to a campaign is real.
--   unsub_treatments = overlap_count  -> GLOBAL. One click, logged against every open treatment.
--   in between                        -> mixed; read the spread before concluding anything.
--
-- Expect: <= 20 rows. The shape of the distribution IS the answer.
-- ============================================================================================
WITH sent AS (              -- one row per client x treatment they were sent, with its send date
    SELECT e.consumer_id_hashed, e.TREATMENT_ID,
           MIN(CAST(e.disposition_dt_tm AS DATE)) AS send_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2025-08-01'
    GROUP BY 1, 2
),
unsub AS (             -- one row per client x treatment they unsubscribed on
    SELECT DISTINCT e.consumer_id_hashed, e.TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2025-08-01'
),
pairs AS (             -- treatments sent to the same client CLOSE TOGETHER IN TIME. A treatment
                       -- is one wave on one date, so "overlapping exposure" means the sends land
                       -- within RESPONSE_DAYS of each other - both emails were live in the inbox
                       -- at the same time and either could have been the one clicked.
    SELECT s1.consumer_id_hashed,
           s1.TREATMENT_ID
    FROM sent s1
    JOIN sent s2 ON s2.consumer_id_hashed = s1.consumer_id_hashed
                AND s2.TREATMENT_ID <> s1.TREATMENT_ID
    WHERE ABS(s1.send_dt - s2.send_dt) <= 30       -- RESPONSE_DAYS; set from Q1's result
    GROUP BY 1, 2
),
per_client AS (
    SELECT p.consumer_id_hashed,
           COUNT(DISTINCT p.TREATMENT_ID)                                  AS overlap_count,
           COUNT(DISTINCT CASE WHEN u.TREATMENT_ID IS NOT NULL
                               THEN p.TREATMENT_ID END)                    AS unsub_treatments
    FROM pairs p
    LEFT JOIN unsub u ON u.consumer_id_hashed = p.consumer_id_hashed
                     AND u.TREATMENT_ID = p.TREATMENT_ID
    GROUP BY 1
)
SELECT TOP 20
       overlap_count,
       unsub_treatments,
       CASE WHEN unsub_treatments = 0                THEN 'no unsub'
            WHEN unsub_treatments = 1                THEN 'PER-LIST (one treatment)'
            WHEN unsub_treatments = overlap_count    THEN 'GLOBAL (all overlapping treatments)'
            ELSE 'PARTIAL' END                       AS reading,
       COUNT(*)                                      AS clients
FROM per_client
WHERE unsub_treatments > 0
GROUP BY 1, 2, 3
ORDER BY clients DESC;


-- ============================================================================================
-- Q3. Machine or human? For clients with 2+ unsub events, how far apart are they in TIME.
--   0 seconds        -> one system action fanned out across treatments. GLOBAL, mechanically.
--   seconds/minutes  -> still one action, batch-processed
--   hours/days       -> a person clicking unsubscribe more than once. PER-LIST behaviour.
--
-- This is the diagnostic that separates the two explanations "same day" cannot.
-- Expect: <= 10 rows.
-- ============================================================================================
WITH u AS (
    SELECT consumer_id_hashed,
           TREATMENT_ID,
           MIN(disposition_dt_tm) AS unsub_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '2025-08-01'
    GROUP BY 1, 2
),
spread AS (
    SELECT consumer_id_hashed,
           COUNT(DISTINCT TREATMENT_ID)                                   AS n_treatments,
           CAST((MAX(unsub_tm) - MIN(unsub_tm)) SECOND(4) AS INTEGER)     AS spread_seconds
    FROM u
    GROUP BY 1
    HAVING COUNT(DISTINCT TREATMENT_ID) >= 2
)
SELECT CASE WHEN spread_seconds = 0            THEN '0 - identical timestamp'
            WHEN spread_seconds <= 60          THEN '1-60 sec'
            WHEN spread_seconds <= 3600        THEN '1-60 min'
            WHEN spread_seconds <= 86400       THEN '1-24 hours'
            ELSE 'over a day' END              AS unsub_spread,
       COUNT(*)                                AS clients,
       MIN(n_treatments)                       AS min_treatments,
       MAX(n_treatments)                       AS max_treatments
FROM spread
GROUP BY 1
ORDER BY clients DESC;


-- ============================================================================================
-- HOW TO READ THE SET
--
-- Q2 GLOBAL-dominant + Q3 concentrated at 0 seconds
--     -> one opt-out, fanned out by the system. Per-campaign unsub attribution is not real, and
--        every Spotlight 1 per-campaign number has to be worded as "last straw", not "cause".
--        Outcome metrics must be client-level.
--
-- Q2 PER-LIST-dominant + Q3 spread over hours/days
--     -> clients are genuinely unsubscribing per campaign. Per-campaign attribution stands as is.
--
-- Mixed
--     -> both mechanisms exist. Split them: treat 0-second fan-outs as one client-level event and
--        keep the spread-out ones as genuine per-campaign unsubscribes.
--
-- Q1 sets the attribution window for both spotlights. A treatment is one wave on one date,
-- so there is no campaign duration to derive - only the response lag. Take the bucket where
-- the mass stops and use that number, rather than defending a guessed 30 or 90.
-- ============================================================================================
