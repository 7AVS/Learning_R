-- verify_cpc_trace_controls.sql — prove the empty CPC result is TRUTH,
-- not a broken query. Teradata-direct. Three controls:

-- [1] TABLE LIVENESS: is CPC_RB_PREF_LOG populated and current?
SELECT COUNT(*) AS rows_2026,
       MIN(CHG_TMSTMP) AS min_tm,
       MAX(CHG_TMSTMP) AS max_tm
FROM DDWV01.CPC_RB_PREF_LOG
WHERE CHG_TMSTMP >= TIMESTAMP '2026-01-01 00:00:00';

-- [2] OUR 7 CLIENTS, NO TIME FLOOR: any CPC row EVER?
SELECT CLNT_NO, COUNT(*) AS rows_ever,
       MIN(CHG_TMSTMP) AS first_tm, MAX(CHG_TMSTMP) AS last_tm
FROM DDWV01.CPC_RB_PREF_LOG
WHERE CLNT_NO IN (142780923,144004330,230520538,270607922,
                  281925669,355852781,383716735)
GROUP BY 1
ORDER BY 1;

-- [3] POSITIVE CONTROL + KEY-FORMAT CHECK: sample real rows, eyeball
-- whether CLNT_NO looks like our vendor-side client numbers (9-digit,
-- no leading zeros / same magnitude).
SELECT TOP 10 CLNT_NO, PREF_ID, CLNT_CONSENT_TYP, APP_SYS_CD, CHG_TMSTMP
FROM DDWV01.CPC_RB_PREF_LOG
WHERE CHG_TMSTMP >= TIMESTAMP '2026-03-01 00:00:00'
ORDER BY CHG_TMSTMP DESC;
