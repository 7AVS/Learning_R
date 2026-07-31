-- preflight2.sql
-- P1 failed: MASTER holds up to 95,014 rows for a single (consumer_id_hashed, TREATMENT_ID),
-- averaging 1.123 rows per key across 385,665,736 keys.
--
-- Two different problems hide behind that one number, and they need different fixes:
--   the ~12% average fan-out  -> inflates every COUNT and SUM in all three pulls
--   the 95,014-row key        -> almost certainly a null or sentinel id, not a real client
--
-- These four queries decide which fix is correct. Each returns <= 20 rows.
-- ENGINE: Teradata-direct.


-- =========================================================================================
-- P5. Shape of the fan-out. Is it a long tail on a few bad keys, or is everything duplicated?
-- Determines whether a DISTINCT is a cheap correction or a structural redesign.
-- =========================================================================================
SELECT CASE WHEN k = 1      THEN '1 row  (clean)'
            WHEN k = 2      THEN '2 rows'
            WHEN k <= 5     THEN '3-5 rows'
            WHEN k <= 20    THEN '6-20 rows'
            WHEN k <= 100   THEN '21-100 rows'
            ELSE                 'over 100 rows' END AS rows_per_key,
       COUNT(*) AS keys,
       SUM(k)   AS total_rows
FROM (SELECT consumer_id_hashed, TREATMENT_ID, COUNT(*) AS k
      FROM DTZV01.VENDOR_FEEDBACK_MASTER
      WHERE load_tm >= DATE '2025-05-01'
      GROUP BY 1, 2) t
GROUP BY 1
ORDER BY 1;


-- =========================================================================================
-- P6. THE DECIDING QUERY. Do duplicate rows carry the SAME client or DIFFERENT clients?
--
--   distinct_clients = 1  -> pure duplication. SELECT DISTINCT consumer_id_hashed, TREATMENT_ID,
--                            CLNT_NO on the MASTER side fixes it completely.
--   distinct_clients > 1  -> one email maps to several client numbers. Attribution is genuinely
--                            ambiguous and a DISTINCT would keep every one of them, still
--                            inflating counts. That needs a decision, not a code fix.
-- =========================================================================================
SELECT CASE WHEN c = 1    THEN '1 client  (pure duplication)'
            WHEN c = 2    THEN '2 clients (AMBIGUOUS)'
            WHEN c <= 5   THEN '3-5 clients (AMBIGUOUS)'
            ELSE               'over 5 clients (AMBIGUOUS)' END AS distinct_clients_per_key,
       COUNT(*) AS keys
FROM (SELECT consumer_id_hashed, TREATMENT_ID, COUNT(DISTINCT CLNT_NO) AS c
      FROM DTZV01.VENDOR_FEEDBACK_MASTER
      WHERE load_tm >= DATE '2025-05-01'
      GROUP BY 1, 2) t
GROUP BY 1
ORDER BY 1;


-- =========================================================================================
-- P7. Is the 95,014-row key a real client or a sentinel? Look at the worst offenders directly.
-- A blank, all-zero or obviously-placeholder consumer_id_hashed means these rows should be
-- excluded outright rather than deduped.
-- =========================================================================================
SELECT TOP 10
       consumer_id_hashed,
       TREATMENT_ID,
       COUNT(*)                 AS rows_for_key,
       COUNT(DISTINCT CLNT_NO)  AS distinct_clients,
       MIN(CLNT_NO)             AS min_clnt,
       MAX(CLNT_NO)             AS max_clnt
FROM DTZV01.VENDOR_FEEDBACK_MASTER
WHERE load_tm >= DATE '2025-05-01'
GROUP BY 1, 2
ORDER BY rows_for_key DESC;


-- =========================================================================================
-- P8. What the DISTINCT actually buys. Row count before and after deduping the MASTER side.
-- If deduped_keys equals P1's 385,665,736 and the row count drops to match, the correction is
-- clean and quantified.
-- =========================================================================================
SELECT COUNT(*) AS raw_rows,
       COUNT(DISTINCT consumer_id_hashed || '|' || TREATMENT_ID) AS distinct_keys
FROM DTZV01.VENDOR_FEEDBACK_MASTER
WHERE load_tm >= DATE '2025-05-01'
  AND consumer_id_hashed IS NOT NULL
  AND CLNT_NO IS NOT NULL;
