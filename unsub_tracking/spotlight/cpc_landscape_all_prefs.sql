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

-- [3] MONTHLY CUBE: writes per month x gate x writing system, 2024+,
--     long format for Excel pivot. Mappings embedded (from the CPC
--     dictionary spreadsheet + APP_SYS_CD dictionary page, 2026-08-05).
SELECT EXTRACT(YEAR FROM CHG_TMSTMP) * 100
     + EXTRACT(MONTH FROM CHG_TMSTMP) AS ym,
       PREF_ID,
       CASE PREF_ID
         WHEN 1002 THEN 'Entity: RBC Royal Bank'
         WHEN 1004 THEN 'Product: Accounts & Packages'
         WHEN 1006 THEN 'Product: Credit Cards'
         WHEN 1007 THEN 'Channel: Direct Mail'
         WHEN 1008 THEN 'Channel: Telephone'
         WHEN 1009 THEN 'Channel: RBC Online'
         WHEN 1010 THEN 'Product: Creditor Insurance'
         WHEN 1012 THEN 'Channel: Email'
         WHEN 1013 THEN 'Channel: Face to Face'
         WHEN 1014 THEN 'Usage: Share for Marketing'
         WHEN 1015 THEN 'Usage: Share for Service'
         WHEN 1016 THEN 'Entity: Credit Bureau'
         WHEN 1017 THEN 'Hidden: SIN for Tax'
         WHEN 1018 THEN 'Hidden: Statement-Inserts'
         WHEN 1019 THEN 'Hidden: Statement-Marketing'
         WHEN 1020 THEN 'Service: Maturity Call Reg-Invest'
         WHEN 1021 THEN 'Service: Maturity Call Non-Reg'
         WHEN 1022 THEN 'Service: Maturity Call Mortgage'
         WHEN 1023 THEN 'Product: Investments-Registered'
         WHEN 1024 THEN 'Product: Investments-Non-Reg'
         WHEN 1025 THEN 'Product: Loans & Lines of Credit'
         WHEN 1026 THEN 'Product: Mortgages'
         WHEN 1027 THEN 'Product: Business Deposit Accts'
         WHEN 1028 THEN 'Product: Creditor Insurance BLIP'
         WHEN 1029 THEN 'Service: Maturity Call Loans'
         WHEN 1030 THEN 'Product: Cash Management Svcs'
         WHEN 1031 THEN 'Product: Leasing'
         WHEN 1032 THEN 'Service: Referrals Moneris'
         WHEN 1033 THEN 'Service: Referrals Payroll ADP'
         WHEN 1034 THEN 'Product: Client Cards'
         WHEN 1035 THEN 'Service: Maturity Call Mortgage NP'
         WHEN 1036 THEN 'Usage: Share Online Personalization'
         WHEN 1042 THEN 'Service: Banking Surveys'
         WHEN 1044 THEN 'Product: Travel Health Insurance'
         WHEN 1045 THEN 'Hidden: E-Newsletter Banking'
         WHEN 1046 THEN 'Hidden: E-Newsletter Rewards'
         WHEN 1048 THEN 'Channel: ATM'
         ELSE 'UNMAPPED' END AS pref_nm,
       APP_SYS_CD,
       CASE APP_SYS_CD
         WHEN 7001 THEN 'Sales Platform (branch)'
         WHEN 7002 THEN 'DI Client Source'
         WHEN 7003 THEN 'Royal Direct contact ctr'
         WHEN 7004 THEN 'Online Banking'
         WHEN 7005 THEN 'Service Platform'
         WHEN 7006 THEN 'RBC Banking internal/batch'
         WHEN 7007 THEN 'RBC Express'
         WHEN 7008 THEN 'DS Client Source'
         WHEN 7009 THEN 'BridgeTrack/Sapient'
         WHEN 7010 THEN 'CASPER'
         WHEN 7012 THEN 'Retail Invest F200'
         WHEN 7013 THEN 'Retail Invest 5G10'
         WHEN 7014 THEN 'Term Invest 4V00'
         WHEN 7015 THEN 'SAP / RCT-LINX'
         WHEN 7016 THEN 'RBC.COM'
         WHEN 7017 THEN 'D&H/AMIA/CMG telemktr'
         WHEN 7018 THEN 'CART'
         WHEN 7019 THEN 'IRIS'
         WHEN 7020 THEN 'Exact Target (SFMC)'
         WHEN 7021 THEN 'TSYS'
         WHEN 7022 THEN 'RD Fulfillment'
         WHEN 7023 THEN 'Assisted Multi-Product App'
         WHEN 7024 THEN 'VOX telemktr'
         WHEN 7025 THEN 'ZEDD telemktr / CASL Tool'
         WHEN 7026 THEN 'APAC telemktr'
         WHEN 7027 THEN 'D&H'
         WHEN 7028 THEN 'CPC-CA (MCA)'
         WHEN 7029 THEN 'RCL TPA'
         WHEN 7030 THEN 'GISP (WM) / ADHOC'
         WHEN 7999 THEN 'Default'
         WHEN 99999 THEN 'Batch SRF consolidation'
         ELSE 'UNMAPPED' END AS app_sys_nm,
       COUNT(*) AS all_writes,
       SUM(CASE WHEN CLNT_CONSENT_TYP = 5001 THEN 1 ELSE 0 END) AS yes_writes,
       SUM(CASE WHEN CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS no_writes,
       SUM(CASE WHEN CLNT_CONSENT_TYP = 5003 THEN 1 ELSE 0 END) AS blank_writes,
       SUM(CASE WHEN CLNT_CONSENT_TYP = 5004 THEN 1 ELSE 0 END) AS yes_no_sin_writes,
       COUNT(DISTINCT CLNT_NO) AS clients
FROM DDWV01.CPC_RB_PREF_LOG
WHERE CHG_TMSTMP >= TIMESTAMP '2024-01-01 00:00:00'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 4;
