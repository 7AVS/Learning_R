-- 30: RAW DUMP for a handful of clients. Vendor feedback + CPC, exactly as stored.
-- ENGINE: Teradata-direct.
-- No derived columns. No decodes. No labels. No joins in the output. Three SELECT * blocks.
--
-- HOW TO USE
--   STEP 1  run the picker, copy 10-20 client numbers out of it, throw the rest of its output away
--   STEP 2  paste those client numbers into the INSERT at the top of step 2, run the three dumps
--
-- STEP 1 is the only place anything is computed. STEP 2 is raw.


-- ############################################################################
-- STEP 1 — PICKER. January 2026 unsubs. Output is two lists of client numbers.
-- ############################################################################

CREATE VOLATILE TABLE vt_jan_unsub AS (
    SELECT DISTINCT
        m.CLNT_NO,
        CAST(e.disposition_dt_tm AS DATE) AS unsub_dt,
        SUBSTR(e.TREATMENT_ID, 8, 3)      AS mne
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
        AND m.load_tm >= DATE '2025-12-01'
        AND m.load_tm <  DATE '2026-03-01'
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2026-01-01'   -- editable: month start
      AND e.disposition_dt_tm <  DATE '2026-02-01'   -- editable: month end
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_jan_unsub COLUMN (CLNT_NO);

-- 1a: MULTI — 2+ campaigns unsubbed on the SAME DAY  <- copy these client numbers
SELECT CLNT_NO
FROM vt_jan_unsub
GROUP BY CLNT_NO, unsub_dt
HAVING COUNT(DISTINCT mne) >= 2
ORDER BY 1
SAMPLE 10;

-- 1b: SINGLE — exactly one campaign unsubbed in the whole month  <- copy these client numbers
SELECT CLNT_NO
FROM vt_jan_unsub
GROUP BY CLNT_NO
HAVING COUNT(DISTINCT mne) = 1
ORDER BY 1
SAMPLE 10;

DROP TABLE vt_jan_unsub;


-- ############################################################################
-- STEP 2 — RAW DUMPS. Paste the client numbers below, once.
-- ############################################################################

CREATE VOLATILE TABLE vt_clients (CLNT_NO INTEGER)
PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

INSERT INTO vt_clients VALUES (142780923);   -- <<< PASTE CLIENT NUMBERS HERE, one INSERT per client
INSERT INTO vt_clients VALUES (144004330);
INSERT INTO vt_clients VALUES (230520538);
INSERT INTO vt_clients VALUES (270607922);
INSERT INTO vt_clients VALUES (281925669);
INSERT INTO vt_clients VALUES (355852781);
INSERT INTO vt_clients VALUES (383716735);

COLLECT STATISTICS ON vt_clients COLUMN (CLNT_NO);


-- 2a: VENDOR_FEEDBACK_MASTER, raw
SELECT m.*
FROM DTZV01.VENDOR_FEEDBACK_MASTER m
WHERE m.CLNT_NO IN (SELECT CLNT_NO FROM vt_clients)
  AND m.load_tm >= DATE '2024-01-01'
ORDER BY m.CLNT_NO, m.load_tm;


-- 2b: VENDOR_FEEDBACK_EVENT, raw. Every disposition, not just unsubs.
--     EVENT has no CLNT_NO, so the client list is resolved to consumer_id_hashed via MASTER.
SELECT e.*
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
WHERE e.consumer_id_hashed IN (
        SELECT m.consumer_id_hashed
        FROM DTZV01.VENDOR_FEEDBACK_MASTER m
        WHERE m.CLNT_NO IN (SELECT CLNT_NO FROM vt_clients)
          AND m.load_tm >= DATE '2024-01-01'
      )
  AND e.disposition_dt_tm >= DATE '2024-01-01'
ORDER BY e.consumer_id_hashed, e.disposition_dt_tm;


-- 2c: CPC_RB_PREF_LOG, raw
SELECT c.*
FROM DDWV01.CPC_RB_PREF_LOG c
WHERE c.CLNT_NO IN (SELECT CLNT_NO FROM vt_clients)
  AND c.CHG_TMSTMP >= DATE '2024-01-01'
ORDER BY c.CLNT_NO, c.CHG_TMSTMP;


DROP TABLE vt_clients;
