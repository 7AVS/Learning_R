-- =============================================================================
-- CPC opt-out — STOCK (how many lost right now) + FLOW (new opt-outs by month)
-- =============================================================================
-- Interpretation rules (schemas/cpc_rb_pref_log_schema.md):
--   Rows are change events in EITHER direction (5001=Yes, 5002=No, 5003=blank).
--   Current stance = LATEST row per (client, pref). Absence = blank default
--   (YES for most prefs; NO for 1014/1015). Opted-out = latest row is 5002.
-- Scope: PREF_ID 1002 (RBC Royal Bank entity do-not-solicit) and 1014 (Banking
-- Share for Marketing) — the two candidate "out of marketing" codes.
-- Output contract: Q1 = 2 rows, Q2 <= ~20 rows.
-- ENGINE: Teradata-direct.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Q1: STOCK — current state per code (2 rows)
-- ---------------------------------------------------------------------------
-- Decision: the "how much of the base is lost TODAY" level. For 1014 remember
-- blank = NO, so clients_explicit_yes is the reachable base there, and
-- clients absent from this table entirely are also NO for 1014.
-- ---------------------------------------------------------------------------

WITH latest AS (
    SELECT
        CLNT_NO,
        PREF_ID,
        CLNT_CONSENT_TYP,
        ROW_NUMBER() OVER (PARTITION BY CLNT_NO, PREF_ID
                           ORDER BY CHG_TMSTMP DESC) AS rn
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID IN (1002, 1014)
)
SELECT
    PREF_ID,
    SUM(CASE WHEN CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS clients_opted_out_now,
    SUM(CASE WHEN CLNT_CONSENT_TYP = 5001 THEN 1 ELSE 0 END) AS clients_explicit_yes_now,
    SUM(CASE WHEN CLNT_CONSENT_TYP = 5003 THEN 1 ELSE 0 END) AS clients_reset_blank_now,
    CAST(COUNT(*) AS BIGINT)                                 AS clients_with_any_state
FROM latest
WHERE rn = 1
GROUP BY 1
ORDER BY 1;


-- ---------------------------------------------------------------------------
-- Q2: FLOW — new opt-out changes per month, 2024+ (<= ~20 rows)
-- ---------------------------------------------------------------------------
-- Decision: is consent loss accelerating? Counts CHANGES to 5002 (a client
-- flipping twice counts twice — this is event flow, not stock; stock is Q1).
-- ---------------------------------------------------------------------------

SELECT
    EXTRACT(YEAR FROM CHG_TMSTMP) * 100
      + EXTRACT(MONTH FROM CHG_TMSTMP) AS chg_month_yyyymm,
    SUM(CASE WHEN PREF_ID = 1002 THEN 1 ELSE 0 END) AS new_optouts_1002,
    SUM(CASE WHEN PREF_ID = 1014 THEN 1 ELSE 0 END) AS new_optouts_1014
FROM DDWV01.CPC_RB_PREF_LOG
WHERE CHG_TMSTMP >= DATE '2024-01-01'
  AND CLNT_CONSENT_TYP = 5002
  AND PREF_ID IN (1002, 1014)
GROUP BY 1
ORDER BY 1;

-- RESULTS (2026-07-15 run): Q1 stock — 1002: 50,738 out / 1,179,248 yes /
-- 2,912,375 blank; 1014: 82,911 out / 555,115 yes / 3,576,392 blank.
-- Q2 flow — 1002 ~150-350/mo declining; 1014 ~600-800/mo flat; 2024-03 spike
-- ~20.5K on BOTH codes = batch-write artifact, verify via Q3 before publishing.
-- Contrast: email unsubs ~35K clients/MONTH (649,885 / 18.5mo) vs ~200/mo
-- entity opt-outs — the reachability loss lives at the vendor level, ~150x.

-- ---------------------------------------------------------------------------
-- Q3: was the 2024-03 spike a batch write? (<=10 rows)
-- ---------------------------------------------------------------------------
-- Decision: if one APP_SYS_CD owns the March 2024 opt-outs, it's mechanical
-- (migration/backfill) — annotate/exclude it from any trend read.
-- ---------------------------------------------------------------------------

SELECT
    APP_SYS_CD,
    PREF_ID,
    CAST(COUNT(*) AS BIGINT)   AS optout_rows
FROM DDWV01.CPC_RB_PREF_LOG
WHERE CHG_TMSTMP >= DATE '2024-03-01'
  AND CHG_TMSTMP <  DATE '2024-04-01'
  AND CLNT_CONSENT_TYP = 5002
  AND PREF_ID IN (1002, 1014)
GROUP BY 1, 2
ORDER BY optout_rows DESC;
