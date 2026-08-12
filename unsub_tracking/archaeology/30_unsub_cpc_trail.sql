-- 30: January same-day multi-campaign unsubbers, then their raw rows. Teradata-direct.

-- QUERY 1 — who are they
SELECT
    m.CLNT_NO,
    CAST(e.disposition_dt_tm AS DATE)                 AS unsub_dt,
    COUNT(DISTINCT SUBSTR(e.TREATMENT_ID, 8, 3))      AS n_campaigns
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  m.consumer_id_hashed = e.consumer_id_hashed
    AND m.TREATMENT_ID       = e.TREATMENT_ID
    AND m.load_tm >= DATE '2025-12-01'
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2026-01-01'
  AND e.disposition_dt_tm <  DATE '2026-02-01'
GROUP BY 1, 2
HAVING COUNT(DISTINCT SUBSTR(e.TREATMENT_ID, 8, 3)) >= 2
ORDER BY 3 DESC
SAMPLE 20;


-- QUERY 2 — paste 5-10 of those CLNT_NOs into both IN lists below

-- 2a: their raw vendor feedback rows
SELECT e.*
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
WHERE e.consumer_id_hashed IN (
    SELECT consumer_id_hashed FROM DTZV01.VENDOR_FEEDBACK_MASTER
    WHERE CLNT_NO IN (000000000, 000000000)      -- <<< paste here
)
ORDER BY e.consumer_id_hashed, e.disposition_dt_tm;

-- 2b: their raw CPC rows
SELECT c.*
FROM DDWV01.CPC_RB_PREF_LOG c
WHERE c.CLNT_NO IN (000000000, 000000000)        -- <<< paste here
ORDER BY c.CLNT_NO, c.CHG_TMSTMP;
