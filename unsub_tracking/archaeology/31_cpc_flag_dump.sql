-- 31: 10 clients who flipped 1002/1012/1014 after 2025 — entire raw dump. Teradata-direct.

-- QUERY 1 — full CPC history for 10 such clients. Self-contained, nothing to paste.
SELECT c.*
FROM DDWV01.CPC_RB_PREF_LOG c
WHERE c.CLNT_NO IN (
    SELECT CLNT_NO FROM (
        SELECT DISTINCT CLNT_NO
        FROM DDWV01.CPC_RB_PREF_LOG
        WHERE PREF_ID IN (1002, 1012, 1014)          -- editable: which switches
          AND CLNT_CONSENT_TYP = 5002                -- editable: 5002=No/opted-out, 5001=Yes, 5003=blank
          AND CHG_TMSTMP >= DATE '2025-01-01'        -- editable: after 2025
    ) d
    SAMPLE 10                                        -- editable: how many clients
)
ORDER BY c.CLNT_NO, c.CHG_TMSTMP;


-- QUERY 2 — same clients' raw vendor feedback rows.
-- SAMPLE is random, so paste the CLNT_NOs Query 1 returned or you get different clients.
SELECT e.*
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
WHERE e.consumer_id_hashed IN (
    SELECT consumer_id_hashed FROM DTZV01.VENDOR_FEEDBACK_MASTER
    WHERE CLNT_NO IN (000000000, 000000000)          -- <<< paste here
)
ORDER BY e.consumer_id_hashed, e.disposition_dt_tm;
