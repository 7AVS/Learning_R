-- preflight3.sql
-- Before deduping anything: understand WHAT the duplication is.
--
-- preflight2 proved MASTER holds 1.123 rows per (consumer_id_hashed, TREATMENT_ID) and that 97.5%
-- of those keys map to a single client. It never asked WHY the extra rows exist. A vendor feed
-- duplicating on purpose is more likely than a badly designed table, and if the differing column
-- carries meaning - a send attempt, a status, a channel, a retry - then SELECT DISTINCT on three
-- columns throws that meaning away silently.
--
-- Same question for the 8,084,229 (client, treatment) pairs with multiple cd=1 timestamps.
-- "Retries of the same email" was an assumption, not a finding.
--
-- Run Q9 first - everything else depends on knowing the column list.
-- ENGINE: Teradata-direct.


-- =========================================================================================
-- Q9. What columns does MASTER actually have?
-- UNSUB_TRACKING_KNOWLEDGE.md:135 lists 29, sourced from a screenshot, never verified live.
-- =========================================================================================
HELP TABLE DTZV01.VENDOR_FEEDBACK_MASTER;


-- =========================================================================================
-- Q10. Look at real duplicate rows, whole. Pick one heavily duplicated key that HAS a client
-- (the null-client monsters are a separate problem) and read every column side by side.
-- Whatever differs between these rows IS the duplication.
-- =========================================================================================
SELECT TOP 20 *
FROM DTZV01.VENDOR_FEEDBACK_MASTER
WHERE (consumer_id_hashed, TREATMENT_ID) IN (
    SELECT consumer_id_hashed, TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_MASTER
    WHERE load_tm >= DATE '2025-05-01'
      AND CLNT_NO IS NOT NULL
    GROUP BY 1, 2
    HAVING COUNT(*) BETWEEN 3 AND 6
    QUALIFY ROW_NUMBER() OVER (ORDER BY consumer_id_hashed) = 1
)
ORDER BY load_tm;


-- =========================================================================================
-- Q11. Which columns VARY within a duplicate group, and which are constant?
-- This is the answer in one table. A column that varies is the reason the row exists; a column
-- that never varies is safe to carry through a dedup.
--
-- Fill in the column names from Q9's output before running - the ones below are the only ones
-- confirmed in the repo, so this is a starting set, not the full picture.
-- =========================================================================================
SELECT SUM(CASE WHEN d_clnt    > 1 THEN 1 ELSE 0 END) AS groups_varying_clnt_no,
       SUM(CASE WHEN d_load    > 1 THEN 1 ELSE 0 END) AS groups_varying_load_tm,
       COUNT(*)                                       AS duplicate_groups
FROM (
    SELECT consumer_id_hashed, TREATMENT_ID,
           COUNT(DISTINCT CLNT_NO) AS d_clnt,
           COUNT(DISTINCT load_tm) AS d_load
    FROM DTZV01.VENDOR_FEEDBACK_MASTER
    WHERE load_tm >= DATE '2025-05-01'
      AND CLNT_NO IS NOT NULL
    GROUP BY 1, 2
    HAVING COUNT(*) > 1
) g;


-- =========================================================================================
-- Q12. Same question for the multiple cd=1 rows. Are they retries, or different sends?
-- Read the gap between the timestamps. Minutes/hours apart on the same day looks like a retry
-- or a batch artifact. Weeks apart means the campaign genuinely mailed the client twice under
-- one TREATMENT_ID, and collapsing them would delete real emails.
-- =========================================================================================
SELECT CASE WHEN span_days = 0  THEN '00 same day'
            WHEN span_days = 1  THEN '01 next day'
            WHEN span_days <= 7 THEN '02 2-7 days'
            WHEN span_days <= 30 THEN '03 8-30 days'
            ELSE                     '04 over 30 days' END AS spread_between_sends,
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
    HAVING COUNT(DISTINCT disposition_dt_tm) > 1
) t
GROUP BY 1
ORDER BY 1;


-- =========================================================================================
-- Q13. RE-MEASURE the "178,184 dropped unsubs" properly.
-- The original number counted joined ROWS without deduping MASTER, so it carried the same
-- 1.123x fan-out it was meant to warn about, and it counted EVENTS rather than CLIENTS.
-- Anchor for judging the result: ~319,733 distinct unsubscribers bank-wide over 12 months
-- (RESULTS_CATALOG.md), and 0.2-0.6% unsub rate per deployment.
-- =========================================================================================
SELECT COUNT(*)                     AS distinct_unsub_events_dropped,
       COUNT(DISTINCT m.CLNT_NO)    AS distinct_clients_dropped
FROM (SELECT consumer_id_hashed, TREATMENT_ID, MIN(disposition_dt_tm) AS unsub_tm
      FROM DTZV01.VENDOR_FEEDBACK_EVENT
      WHERE disposition_cd = 4
        AND disposition_dt_tm >= DATE '2025-08-01'
        AND disposition_dt_tm <  DATE '2026-08-01'
      GROUP BY 1, 2) e
JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
      FROM DTZV01.VENDOR_FEEDBACK_MASTER
      WHERE CLNT_NO IS NOT NULL) m
  ON m.consumer_id_hashed = e.consumer_id_hashed
 AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE NOT EXISTS (
    SELECT 1 FROM DTZV01.VENDOR_FEEDBACK_MASTER m2
    WHERE m2.consumer_id_hashed = e.consumer_id_hashed
      AND m2.TREATMENT_ID = e.TREATMENT_ID
      AND m2.load_tm >= DATE '2025-08-01');


-- =========================================================================================
-- Q14. SANITY ANCHOR. Total distinct unsubscribers and unsub events in the window, deduped.
-- Andre's domain read: 0.2-0.6% unsub rate per deployment, under ~5,000 even at peak. Whatever
-- Q13 returns has to sit sensibly inside these totals or the measurement is wrong, not the data.
-- =========================================================================================
SELECT COUNT(*)                  AS distinct_unsub_events,
       COUNT(DISTINCT CLNT_NO)   AS distinct_unsub_clients
FROM (SELECT DISTINCT e.consumer_id_hashed, e.TREATMENT_ID, m.CLNT_NO
      FROM DTZV01.VENDOR_FEEDBACK_EVENT e
      JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
            FROM DTZV01.VENDOR_FEEDBACK_MASTER
            WHERE load_tm >= DATE '2025-05-01'
              AND CLNT_NO IS NOT NULL) m
        ON m.consumer_id_hashed = e.consumer_id_hashed
       AND m.TREATMENT_ID = e.TREATMENT_ID
      WHERE e.disposition_cd = 4
        AND e.disposition_dt_tm >= DATE '2025-08-01'
        AND e.disposition_dt_tm <  DATE '2026-08-01') x;
