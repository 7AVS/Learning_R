-- cpc_landscape_all_prefs.sql v2 — CPC standalone, RB family, all gates.
-- Semantics honored: LOG is a change log (current state = latest row per
-- CLNT_NO x PREF_ID); states 5001 Yes / 5002 No / 5003 blank-answered;
-- clients with NO row on a gate are the true "never asked" blank — they
-- do NOT appear here, so blank per gate = client base - clients counted.
-- Teradata-direct.

-- [1] STANDING: latest state per client x gate, counted by gate x state.
--    Sanity anchors (Apr 2026): 1002 No ~49K · 1012 No ~33K · 1014 No ~79K.
SELECT PREF_ID, CLNT_CONSENT_TYP, COUNT(*) AS clients
FROM (
    SELECT CLNT_NO, PREF_ID, CLNT_CONSENT_TYP,
           ROW_NUMBER() OVER (PARTITION BY CLNT_NO, PREF_ID
                              ORDER BY CHG_TMSTMP DESC) AS rn
    FROM DDWV01.CPC_RB_PREF_LOG
) t
WHERE rn = 1
GROUP BY 1, 2
ORDER BY 1, 2;

-- [2] FLOW: 2026 explicit-No writes by gate x WRITING SYSTEM.
--    APP_SYS_CD: 7001 branch · 7003 contact centre · 7004 online banking
--    7006 internal batch · 7016 RBC.COM · 7020 Exact Target · 7999 default.
SELECT PREF_ID, APP_SYS_CD,
       COUNT(*) AS no_writes_2026,
       COUNT(DISTINCT CLNT_NO) AS clients_2026
FROM DDWV01.CPC_RB_PREF_LOG
WHERE CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= TIMESTAMP '2026-01-01 00:00:00'
GROUP BY 1, 2
ORDER BY no_writes_2026 DESC;

-- [3] MONTHLY CUBE: new log events per month per gate, 2024+ (long
--     format, pivot in Excel). all_writes = any state; no_writes = 5002.
SELECT EXTRACT(YEAR FROM CHG_TMSTMP) * 100
     + EXTRACT(MONTH FROM CHG_TMSTMP) AS ym,
       PREF_ID,
       COUNT(*) AS all_writes,
       SUM(CASE WHEN CLNT_CONSENT_TYP = 5001 THEN 1 ELSE 0 END) AS yes_writes,
       SUM(CASE WHEN CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS no_writes,
       SUM(CASE WHEN CLNT_CONSENT_TYP = 5003 THEN 1 ELSE 0 END) AS blank_writes,
       SUM(CASE WHEN CLNT_CONSENT_TYP = 5004 THEN 1 ELSE 0 END) AS yes_no_sin_writes,
       COUNT(DISTINCT CLNT_NO) AS clients
FROM DDWV01.CPC_RB_PREF_LOG
WHERE CHG_TMSTMP >= TIMESTAMP '2024-01-01 00:00:00'
GROUP BY 1, 2
ORDER BY 1, 2;
