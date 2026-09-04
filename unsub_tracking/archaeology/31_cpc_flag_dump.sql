-- !! READS DDWV01.CPC_RB_PREF_LOG, WHICH IS BROKEN (~1% of writes). DO NOT RUN. Historical only. Andre 2026-09-04 !!
-- 31: 10 clients whose CPC says No. Did the vendor feedback table see an unsub before it?
-- Teradata-direct. One query. Right-hand columns NULL = no unsub signal behind the No.

SELECT
    c.CLNT_NO,
    c.PREF_ID,
    c.CLNT_CONSENT_TYP,
    c.APP_SYS_CD,
    c.CHG_TMSTMP,
    e.TREATMENT_ID,
    e.disposition_cd,
    e.disposition_dt_tm
FROM DDWV01.CPC_RB_PREF_LOG c
LEFT JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  m.CLNT_NO = c.CLNT_NO
    AND m.load_tm >= DATE '2024-01-01'
LEFT JOIN DTZV01.VENDOR_FEEDBACK_EVENT e
    ON  e.consumer_id_hashed  = m.consumer_id_hashed
    AND e.TREATMENT_ID        = m.TREATMENT_ID
    AND e.disposition_cd      = 4
    AND e.disposition_dt_tm  <= c.CHG_TMSTMP
WHERE c.PREF_ID IN (1002, 1012, 1014)          -- editable: which switches
  AND c.CLNT_CONSENT_TYP = 5002                -- editable: 5002=No, 5001=Yes, 5003=blank
  AND c.CHG_TMSTMP >= DATE '2025-01-01'        -- editable: after 2025
  AND c.CLNT_NO IN (
        SELECT CLNT_NO FROM (
            SELECT DISTINCT CLNT_NO
            FROM DDWV01.CPC_RB_PREF_LOG
            WHERE PREF_ID IN (1002, 1012, 1014)
              AND CLNT_CONSENT_TYP = 5002
              AND CHG_TMSTMP >= DATE '2025-01-01'
        ) d
        SAMPLE 10                              -- editable: how many clients
      )
ORDER BY c.CLNT_NO, c.CHG_TMSTMP, e.disposition_dt_tm;
