-- diag_unsub_self_test.sql — SINGLE-SUBJECT EXPERIMENT: what exactly
-- writes disposition 4? Andre clicked an unsub link (landed on the
-- preference page) WITHOUT submitting. Protocol:
--   Day 0: note exact time of (a) link click, (b) any radio selection,
--          (c) whether Submit was pressed. Do NOT submit yet.
--   Day 1: run both statements (vendor feed lands in daily batches).
--          Expected if signal = submit: a disposition 3 (clicked) on the
--          source email's treatment, NO disposition 4.
--   Day 1: then deliberately submit ONE option, note time + which option.
--   Day 2: rerun statement [2]. Observe: does a 4 appear, on WHICH mne /
--          treatment(s), and does the broad "promotional emails from RBC"
--          option write to multiple lists at once?
-- Optional day 2 extension: check the CPC preference log for the same
-- client number — does the submit write any consent switch? (ties back
-- to the CPC bridge study).
-- Teradata-direct. Replace <MY_CLNT_NO> before running.

-- [1] my consumer hashes (one per email address on file)
SELECT DISTINCT consumer_id_hashed, CLNT_NO
FROM DTZV01.VENDOR_FEEDBACK_MASTER
WHERE CLNT_NO = <MY_CLNT_NO>
  AND load_tm >= DATE '2025-10-01';

-- [2] my full journey, last 21 days, ALL dispositions
SELECT e.consumer_id_hashed,
       e.TREATMENT_ID,
       SUBSTR(e.TREATMENT_ID, 8, 3) AS mne,
       e.disposition_cd,
       e.disposition_dt_tm
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
WHERE e.consumer_id_hashed IN (
        SELECT DISTINCT consumer_id_hashed
        FROM DTZV01.VENDOR_FEEDBACK_MASTER
        WHERE CLNT_NO = <MY_CLNT_NO>
          AND load_tm >= DATE '2025-10-01')
  AND e.disposition_dt_tm >= CURRENT_DATE - 21
ORDER BY e.disposition_dt_tm;
