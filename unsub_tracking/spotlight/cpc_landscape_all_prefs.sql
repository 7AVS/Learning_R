-- cpc_landscape_all_prefs.sql — CPC standalone, ALL 26 RB gates, no
-- vendor-email involvement. Teradata-direct, RB family only.
-- [1] STOCK: current standing per gate — where do explicit No's live,
--     at what scale. Uses the current-state view (assumed grain
--     client x PREF_ID; sanity anchors: 1002 No ~49K, 1012 No ~33K,
--     1014 No ~79K as of Apr 2026 — if far off, grain assumption wrong).
-- [2] FLOW: 2026 opt-out writes (5002) per gate — where clients are
--     actively saying no this year.

-- [1] standing by gate x state
SELECT PREF_ID, CLNT_CONSENT_TYP, COUNT(*) AS clients
FROM DDWV01.CPC_RB_PREF
GROUP BY 1, 2
ORDER BY 1, 2;

-- [2] 2026 explicit-No writes by gate
SELECT PREF_ID,
       COUNT(*) AS no_writes_2026,
       COUNT(DISTINCT CLNT_NO) AS clients_2026
FROM DDWV01.CPC_RB_PREF_LOG
WHERE CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= TIMESTAMP '2026-01-01 00:00:00'
GROUP BY 1
ORDER BY no_writes_2026 DESC;

-- [3] the in-database PREF dictionary (small lookups — full dump)
SELECT * FROM DC0V01.OPTION_PREF_CODE_VAL_DESC ORDER BY 1;

-- [4] the PREF group/type lookup
SELECT * FROM DC0V01.OPTION_PREF_GRP_CODE_TYP ORDER BY 1;
