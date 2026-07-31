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
-- Q1. Derive each campaign's real deployment window from its own sends.
-- Replaces the hardcoded 90-day guess: some campaigns run 5 days, some 90.
-- Expect: 25 rows. Read window_days - if it varies widely, a fixed window was always wrong.
-- ============================================================================================
SELECT TOP 25
       SUBSTR(TREATMENT_ID, 8, 3)                       AS mne,
       TREATMENT_ID,
       CAST(MIN(disposition_dt_tm) AS DATE)             AS first_send_dt,
       CAST(MAX(disposition_dt_tm) AS DATE)             AS last_send_dt,
       (CAST(MAX(disposition_dt_tm) AS DATE)
        - CAST(MIN(disposition_dt_tm) AS DATE))         AS window_days,
       COUNT(DISTINCT consumer_id_hashed)               AS clients_sent
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_cd = 1
  AND disposition_dt_tm >= DATE '2025-08-01'
GROUP BY 1, 2
ORDER BY clients_sent DESC;


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
WITH tw AS (           -- derived deployment window per treatment
    SELECT TREATMENT_ID,
           MIN(disposition_dt_tm) AS t_start,
           MAX(disposition_dt_tm) AS t_end
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 1
      AND disposition_dt_tm >= DATE '2025-08-01'
    GROUP BY 1
),
sent AS (              -- one row per client x treatment they were sent
    SELECT DISTINCT e.consumer_id_hashed, e.TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2025-08-01'
),
unsub AS (             -- one row per client x treatment they unsubscribed on
    SELECT DISTINCT e.consumer_id_hashed, e.TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2025-08-01'
),
pairs AS (             -- client's treatments whose windows overlap another of their treatments
    SELECT s1.consumer_id_hashed,
           s1.TREATMENT_ID
    FROM sent s1
    JOIN tw w1 ON w1.TREATMENT_ID = s1.TREATMENT_ID
    JOIN sent s2 ON s2.consumer_id_hashed = s1.consumer_id_hashed
                AND s2.TREATMENT_ID <> s1.TREATMENT_ID
    JOIN tw w2 ON w2.TREATMENT_ID = s2.TREATMENT_ID
    WHERE w1.t_start <= w2.t_end
      AND w2.t_start <= w1.t_end          -- windows genuinely overlap in time
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
-- Q1 feeds Spotlight 2 separately: use each campaign's DERIVED window instead of a hardcoded
-- 90 days, so a campaign that ran 5 days is not judged on a 90-day response window.
-- ============================================================================================
