-- !! READS DDWV01.CPC_RB_PREF_LOG, WHICH IS BROKEN (~1% of writes). DO NOT RUN. Historical only. Andre 2026-09-04 !!
-- 30: 10 clients who unsubscribed 2+ campaigns on the SAME DATE — entire raw dump. Teradata-direct.

-- QUERY 1 — full vendor feedback history for 10 such clients. Self-contained, nothing to paste.
SELECT e.*
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
WHERE e.consumer_id_hashed IN (
    SELECT m2.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m2
    WHERE m2.load_tm >= DATE '2025-12-01'
      AND m2.CLNT_NO IN (
        SELECT CLNT_NO FROM (
            SELECT DISTINCT m.CLNT_NO
            FROM DTZV01.VENDOR_FEEDBACK_EVENT u
            INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
                ON  m.consumer_id_hashed = u.consumer_id_hashed
                AND m.TREATMENT_ID       = u.TREATMENT_ID
                AND m.load_tm >= DATE '2025-12-01'
            WHERE u.disposition_cd = 4
              AND u.disposition_dt_tm >= DATE '2026-01-01'    -- editable: window start
              AND u.disposition_dt_tm <  DATE '2026-02-01'    -- editable: window end
            GROUP BY m.CLNT_NO, CAST(u.disposition_dt_tm AS DATE)
            HAVING COUNT(DISTINCT SUBSTR(u.TREATMENT_ID, 8, 3)) >= 2   -- editable: 2+ campaigns same day
        ) d
        SAMPLE 10                                             -- editable: how many clients
      )
)
ORDER BY e.consumer_id_hashed, e.disposition_dt_tm;


-- QUERY 2 — same clients' raw CPC rows.
-- SAMPLE is random, so paste the CLNT_NOs behind Query 1 or you get different clients.
SELECT c.*
FROM DDWV01.CPC_RB_PREF_LOG c
WHERE c.CLNT_NO IN (000000000, 000000000)                     -- <<< paste here
ORDER BY c.CLNT_NO, c.CHG_TMSTMP;
