-- =============================================================================
-- CPC writes by system (APP_SYS_CD overlay) — who writes, in what shape, first
-- =============================================================================
-- APP_SYS_CD decode (schemas/cpc_rb_pref_log_schema.md): 7001 branch staff ·
-- 7003 contact centre · 7004 online banking · 7005 service platform · 7006
-- internal/batch · 7009 Bridgetrack · 7016 RBC.COM · 7017/7024/7025/7026
-- telemarketing · 7020 EXACT TARGET (email ESP) · 99999 SRF batch · 7999 default.
-- Output contract: W1/W3/W4 <= 10 rows, W2 <= 15 rows.
-- ENGINE: Teradata-direct.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- W1: where do changes come from — volume by system, 2024+ ex-HSBC (<=10 rows)
-- ---------------------------------------------------------------------------

SELECT TOP 10
    APP_SYS_CD,
    CAST(COUNT(*) AS BIGINT)   AS change_rows,
    COUNT(DISTINCT CLNT_NO)    AS distinct_clients
FROM DDWV01.CPC_RB_PREF_LOG
WHERE CHG_TMSTMP >= DATE '2024-01-01'
  AND NOT (CHG_TMSTMP >= DATE '2024-03-01' AND CHG_TMSTMP < DATE '2024-04-01')
GROUP BY 1
ORDER BY change_rows DESC;


-- ---------------------------------------------------------------------------
-- W2: bundle shape by system, 2024+ ex-HSBC (<=15 rows)
-- ---------------------------------------------------------------------------
-- Which system produces the full-form (6+) saves vs single-switch changes.
-- mixed_system_bundles > 0 would mean one save-event carries two systems
-- (unexpected — flag it if so).
-- ---------------------------------------------------------------------------

WITH ev AS (
    SELECT
        CLNT_NO,
        CHG_TMSTMP,
        COUNT(*)            AS switches_in_bundle,
        MIN(APP_SYS_CD)     AS sys_min,
        MAX(APP_SYS_CD)     AS sys_max
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE CHG_TMSTMP >= DATE '2024-01-01'
      AND NOT (CHG_TMSTMP >= DATE '2024-03-01' AND CHG_TMSTMP < DATE '2024-04-01')
    GROUP BY 1, 2
)
SELECT TOP 15
    sys_max                     AS app_sys_cd,
    CASE WHEN switches_in_bundle = 1 THEN '1'
         WHEN switches_in_bundle BETWEEN 2 AND 5 THEN '2-5'
         ELSE '6+' END          AS bundle_size,
    CAST(COUNT(*) AS BIGINT)    AS bundles,
    CAST(SUM(CASE WHEN sys_min <> sys_max THEN 1 ELSE 0 END) AS BIGINT) AS mixed_system_bundles
FROM ev
GROUP BY 1, 2
ORDER BY bundles DESC;


-- ---------------------------------------------------------------------------
-- W3: first-touch system — which system writes a client's FIRST-ever row (<=10)
-- ---------------------------------------------------------------------------
-- The empirical "where do clients enter the consent system" (onboarding proxy).
-- All-time by construction.
-- ---------------------------------------------------------------------------

WITH first_row AS (
    SELECT
        CLNT_NO,
        APP_SYS_CD,
        ROW_NUMBER() OVER (PARTITION BY CLNT_NO
                           ORDER BY CHG_TMSTMP ASC) AS rn
    FROM DDWV01.CPC_RB_PREF_LOG
)
SELECT TOP 10
    APP_SYS_CD,
    COUNT(*)                   AS clients_first_touched_here
FROM first_row
WHERE rn = 1
GROUP BY 1
ORDER BY clients_first_touched_here DESC;


-- ---------------------------------------------------------------------------
-- W4: what does Exact Target (7020, the email ESP) actually write? (<=10 rows)
-- ---------------------------------------------------------------------------
-- Closes the ESP-pipe question: the registered email-platform writer exists,
-- unsubs don't arrive through it — so what does? Expect e-newsletter prefs
-- (1045/1046/1047) or nothing recent.
-- ---------------------------------------------------------------------------

SELECT TOP 10
    PREF_ID,
    CLNT_CONSENT_TYP,
    CAST(COUNT(*) AS BIGINT)   AS change_rows,
    MIN(CHG_TMSTMP)            AS earliest,
    MAX(CHG_TMSTMP)            AS latest
FROM DDWV01.CPC_RB_PREF_LOG
WHERE APP_SYS_CD = 7020
GROUP BY 1, 2
ORDER BY change_rows DESC;
