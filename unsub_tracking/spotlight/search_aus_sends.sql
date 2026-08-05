-- search_aus_sends.sql — AUS-MNE sends, deployments May 2026 onward,
-- client number containing 428966379, JOINED to VENDOR_FEEDBACK_EVENT:
-- full journey per treatment (1=sent 2=opened 3=clicked 4=unsub
-- 5=hardbounce 6=complaint). LEFT JOIN so a send with no event rows
-- still shows. No CPC. Teradata-direct.
-- Date filter is on the deployment date inside TREATMENT_ID (yyyy+julian;
-- 2026121 = May 1). load_tm is only a LOAD stamp -> wide margin floor.
-- If TRIM(CLNT_NO) errors on type, swap to
-- CAST(m.CLNT_NO AS VARCHAR(30)) LIKE '%428966379%'.

SELECT m.CLNT_NO,
       m.TREATMENT_ID,
       SUBSTR(m.TREATMENT_ID, 1, 7) AS send_yyyyddd,
       m.email_subj_line,
       m.EMAIL_ADDR,
       e.disposition_cd,
       e.disposition_dt_tm
FROM (SELECT DISTINCT CLNT_NO, consumer_id_hashed, TREATMENT_ID,
             email_subj_line, EMAIL_ADDR
      FROM DTZV01.VENDOR_FEEDBACK_MASTER
      WHERE SUBSTR(TREATMENT_ID, 8, 3) = 'AUS'
        AND SUBSTR(TREATMENT_ID, 1, 7) >= '2026121'
        AND load_tm >= DATE '2026-04-01'
        AND TRIM(CLNT_NO) LIKE '%428966379%') m
LEFT JOIN DTZV01.VENDOR_FEEDBACK_EVENT e
  ON  e.consumer_id_hashed = m.consumer_id_hashed
  AND e.TREATMENT_ID = m.TREATMENT_ID
ORDER BY m.TREATMENT_ID, e.disposition_dt_tm;
