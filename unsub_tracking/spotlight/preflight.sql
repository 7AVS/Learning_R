-- preflight.sql
-- Four one-line probes. Run these BEFORE the full spotlight pull. Together they take a few minutes
-- and settle assumptions that every number in spotlight.py currently rests on.
-- ENGINE: Teradata-direct.


-- =========================================================================================
-- P1. MASTER GRAIN. The single biggest unknown.
-- spotlight.py joins EVENT -> MASTER on (consumer_id_hashed, TREATMENT_ID) with no dedup.
-- UNSUB_TRACKING_KNOWLEDGE.md:137 says the grain is "believed one row per client x email send -
-- grain NOT yet verified". If MASTER carries reloads, every COUNT and SUM in all three pulls is
-- inflated, and inflated differently per pull.
--
-- max_rows_per_key = 1  -> safe, nothing to change.
-- max_rows_per_key > 1  -> wrap MASTER in SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
--                          in all three pulls before running anything.
-- =========================================================================================
SELECT MAX(k) AS max_rows_per_key, COUNT(*) AS keys_checked
FROM (SELECT consumer_id_hashed, TREATMENT_ID, COUNT(*) AS k
      FROM DTZV01.VENDOR_FEEDBACK_MASTER
      WHERE load_tm >= DATE '2025-05-01'
      GROUP BY 1, 2) t;


-- =========================================================================================
-- P2. HOW MANY UNSUBS THE OLD ZERO-MARGIN FILTER WAS DELETING.
-- MASTER is filtered on load_tm, a RECORD-LOAD timestamp, not a send date
-- (archaeology/18_vendor_retention_probe.sql:5). With the floor set equal to the event floor, an
-- unsub inside the window whose MASTER row loaded earlier is removed by the INNER JOIN.
-- 28.5% of unsubs lag their send by more than 30 days (RUN_2026-07-31_scope_test.sql), so this
-- lands hardest on the earliest months - exactly the ones q_trend reports.
--
-- This counts what a zero-margin floor would have dropped. spotlight.py now uses MASTER_FLOOR
-- 2025-05-01 (3-month lookback, matching pack 19, pack 20 and museum/20_lookback_cards.sql).
-- If this number is large, that fix mattered.
-- =========================================================================================
SELECT COUNT(*) AS unsub_events_dropped_by_zero_margin
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
  ON m.consumer_id_hashed = e.consumer_id_hashed
 AND m.TREATMENT_ID = e.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2025-08-01'
  AND e.disposition_dt_tm <  DATE '2026-08-01'
  AND m.load_tm < DATE '2025-08-01';


-- =========================================================================================
-- P3. ARE SENDS BEING DOUBLE COUNTED.
-- n_emails_* sums cd=1 rows. If a treatment logs more than one cd=1 row per client (retries,
-- reloads), every email-volume number is inflated. UNSUB_TRACKING_KNOWLEDGE.md warns against
-- counting raw rows without collapsing to send-journey grain first.
--
-- 0 -> one send row per client x treatment, counts are clean.
-- >0 -> collapse to (client, treatment) before summing.
-- =========================================================================================
SELECT COUNT(*) AS client_treatments_with_multiple_send_rows
FROM (SELECT consumer_id_hashed, TREATMENT_ID
      FROM DTZV01.VENDOR_FEEDBACK_EVENT
      WHERE disposition_cd = 1
        AND disposition_dt_tm >= DATE '2025-08-01'
        AND disposition_dt_tm <  DATE '2026-08-01'
      GROUP BY 1, 2
      HAVING COUNT(DISTINCT disposition_dt_tm) > 1) t;


-- =========================================================================================
-- P4. NEGATIVE OR NULL CLNT_NO.
-- Teradata MOD(-7, 10) = -7, which matches no bite in range(10), and MOD(NULL, 10) matches
-- nothing. Such clients would be silently dropped from the bitten pulls while still appearing in
-- the unbitten one, so the cubes would not reconcile.
-- spotlight.py now uses MOD(ABS(m.CLNT_NO), ...). This measures whether that mattered.
-- =========================================================================================
SELECT SUM(CASE WHEN CLNT_NO IS NULL THEN 1 ELSE 0 END) AS null_clnt_no,
       SUM(CASE WHEN CLNT_NO < 0    THEN 1 ELSE 0 END) AS negative_clnt_no,
       COUNT(*)                                        AS rows_checked
FROM DTZV01.VENDOR_FEEDBACK_MASTER
WHERE load_tm >= DATE '2025-05-01';
