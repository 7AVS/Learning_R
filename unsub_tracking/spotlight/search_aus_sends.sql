-- search_aus_sends.sql — AUS-MNE sends, deployments May 2026 onward,
-- client number containing 428966379. Teradata-direct.
-- Date filter is on the deployment date inside TREATMENT_ID (yyyy+julian;
-- 2026121 = May 1). load_tm is only a LOAD stamp -> wide margin floor.
-- If TRIM(CLNT_NO) errors on type, swap to
-- CAST(m.CLNT_NO AS VARCHAR(30)) LIKE '%428966379%'.

SELECT DISTINCT m.CLNT_NO,
       m.TREATMENT_ID,
       SUBSTR(m.TREATMENT_ID, 1, 7) AS send_yyyyddd,
       m.email_subj_line,
       m.EMAIL_ADDR
FROM DTZV01.VENDOR_FEEDBACK_MASTER m
WHERE SUBSTR(m.TREATMENT_ID, 8, 3) = 'AUS'
  AND SUBSTR(m.TREATMENT_ID, 1, 7) >= '2026121'
  AND m.load_tm >= DATE '2026-04-01'
  AND TRIM(m.CLNT_NO) LIKE '%428966379%'
ORDER BY send_yyyyddd;
