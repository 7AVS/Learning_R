-- =============================================================================
-- CPC Preference Log — first-look EDA (schema discovery ONLY)
-- =============================================================================
-- DDWV01.CPC_RB_PREF_LOG — client preference (do-not-contact) log. Source of
-- truth all campaigns abide by (per team, 2026-07-15). Known so far:
--   - PREF_ID = preference identifier (data dictionary col); code 1014 with
--     CPC = 'N' means out of ALL marketing for the RBC entity.
--   - Log grain (changes over time) — presumed, NOT verified.
-- Purpose here: discover the real schema before writing anything else. We have
-- ZERO verified columns — do NOT extend this pack until S0 output is reviewed
-- (SEND_DT/FEEDBACK_ID lesson: never query assumed columns).
--
-- Downstream intent (next pack, after S0):
--   1. Unsub -> CPC linkage: code-4 clients whose CPC flag changed within N
--      days (validates unsub counts against the trusted source; gives CPC its
--      "why" + campaign attribution).
--   2. Population lost: active base vs opted-out, trend YoY, by source.
--
-- ENGINE: Teradata-direct.
-- =============================================================================

-- S0a: column catalog + sample values
SELECT TOP 5 * FROM DDWV01.CPC_RB_PREF_LOG;

-- S0b: raw size
SELECT CAST(COUNT(*) AS BIGINT) AS cpc_log_rows
FROM DDWV01.CPC_RB_PREF_LOG;

-- =============================================================================
-- S1+ — written after S0 run + data-dictionary pages (2026-07-15).
-- Confirmed: 91,415,764 rows. Grain = CHANGE LOG (client x pref x change event).
-- Columns used below are confirmed by BOTH the S0 screenshot and the dictionary:
--   CLNT_NO, PREF_ID, CLNT_CONSENT_TYP, CHG_TMSTMP (+ APP_SYS_CD in S5).
-- CLNT_CONSENT_TYP: 5001=Yes, 5002=No, 5003=blank/never answered, 5004=CB yes.
--   BLANK = YES for all prefs EXCEPT 1014/1015 where BLANK = NO (denominator!).
-- PREF_ID (dictionary, exact codes): 1001 DI entity, 1002 RBC Royal Bank entity
--   DNS, 1016 CB; usage: 1014 Share-for-Marketing, 1015 Share-for-Service,
--   1036 Online Personalization, 1057 DI SfM; Banking channels: 1007 DM,
--   1008 phone, 1009 online, 1012 mobile, 1013 F2F, 1048 ATM; DI channels:
--   1037 DM, 1038 phone, 1039 online, 1040 E-MAIL, 1041 F2F; newsletters:
--   1045 Banking, 1046 Rewards, 1047 DI.
-- NOTE: catalog has NO "Banking - E-Mail" channel code — S4 discovers which
--   PREF_ID an email unsub actually flips. Do not hardcode until S4 answers.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- S1: PREF_ID x consent distribution (whole log)
-- ---------------------------------------------------------------------------
-- Proves: which preferences carry volume and their yes/no/blank mix. Expect
-- the dictionary codes; anything outside them = new codes to decode.
-- ---------------------------------------------------------------------------

SELECT
    PREF_ID,
    CLNT_CONSENT_TYP,
    CAST(COUNT(*) AS BIGINT)   AS log_rows,
    COUNT(DISTINCT CLNT_NO)    AS distinct_clients
FROM DDWV01.CPC_RB_PREF_LOG
GROUP BY 1, 2
ORDER BY log_rows DESC;


-- ---------------------------------------------------------------------------
-- S2: grain check — change rows per (CLNT_NO, PREF_ID)
-- ---------------------------------------------------------------------------
-- Proves: it's a true log (many pairs with 2+ rows = clients flipping over
-- time). Determines the latest-state rule (ROW_NUMBER by CHG_TMSTMP DESC).
-- ---------------------------------------------------------------------------

WITH pair_counts AS (
    SELECT CLNT_NO, PREF_ID, COUNT(*) AS rows_per_pair
    FROM DDWV01.CPC_RB_PREF_LOG
    GROUP BY 1, 2
)
SELECT
    CASE WHEN rows_per_pair = 1 THEN '1'
         WHEN rows_per_pair = 2 THEN '2'
         ELSE '3+' END          AS rows_per_pair_bucket,
    CAST(COUNT(*) AS BIGINT)    AS pair_count
FROM pair_counts
GROUP BY 1
ORDER BY 1;


-- ---------------------------------------------------------------------------
-- S2b: preferences per client — confirms table is NOT client-level
-- ---------------------------------------------------------------------------
-- Proves: distribution of distinct PREF_IDs held per client. Expect most
-- clients to carry several (entity + usage + channel + product prefs).
-- ---------------------------------------------------------------------------

WITH prefs_per_client AS (
    SELECT CLNT_NO, COUNT(DISTINCT PREF_ID) AS n_prefs
    FROM DDWV01.CPC_RB_PREF_LOG
    GROUP BY 1
)
SELECT
    CASE WHEN n_prefs = 1 THEN ' 1'
         WHEN n_prefs BETWEEN 2 AND 5 THEN ' 2-5'
         WHEN n_prefs BETWEEN 6 AND 10 THEN ' 6-10'
         ELSE '11+' END          AS prefs_per_client_bucket,
    CAST(COUNT(*) AS BIGINT)     AS client_count
FROM prefs_per_client
GROUP BY 1
ORDER BY 1;


-- ---------------------------------------------------------------------------
-- S3: monthly opt-out trend, 2024+, marketing-relevant prefs (exact IN-list)
-- ---------------------------------------------------------------------------
-- Proves: volume of consent CHANGES to No (5002) per month per pref — the raw
-- material of the "population lost over time" view.
-- ---------------------------------------------------------------------------

SELECT
    EXTRACT(YEAR FROM CHG_TMSTMP) * 100
      + EXTRACT(MONTH FROM CHG_TMSTMP) AS chg_month_yyyymm,
    PREF_ID,
    CAST(COUNT(*) AS BIGINT)   AS optout_change_rows,
    COUNT(DISTINCT CLNT_NO)    AS distinct_clients
FROM DDWV01.CPC_RB_PREF_LOG
WHERE CHG_TMSTMP >= DATE '2024-01-01'
  AND CLNT_CONSENT_TYP = 5002
  AND PREF_ID IN (1001, 1002, 1016, 1014, 1015, 1036, 1057,
                  1007, 1008, 1009, 1012, 1013, 1048,
                  1040, 1045, 1046, 1047)
GROUP BY 1, 2
ORDER BY 1, 2;


-- ---------------------------------------------------------------------------
-- S4: WHICH PREF_ID does an email unsub flip? (empirical code discovery)
-- ---------------------------------------------------------------------------
-- Proves: for clients with an email unsub (disposition_cd=4, 2024+), which
-- preference codes changed within 7 days after the unsub. The dominant
-- PREF_ID(s) here = the code(s) email unsubs write. No Banking-E-Mail code
-- exists in the 2007-vintage catalog — this query is how we learn the truth.
-- Window (7 days) editable; distinct_clients is the honest count.
-- ---------------------------------------------------------------------------

WITH unsub_clients AS (
    SELECT
        m.CLNT_NO,
        MIN(e.disposition_dt_tm) AS first_unsub_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'
    GROUP BY 1
)
SELECT
    c.PREF_ID,
    c.CLNT_CONSENT_TYP,
    CAST(COUNT(*) AS BIGINT)   AS change_rows,
    COUNT(DISTINCT c.CLNT_NO)  AS distinct_clients
FROM unsub_clients u
INNER JOIN DDWV01.CPC_RB_PREF_LOG c
    ON  c.CLNT_NO = u.CLNT_NO
    AND c.CHG_TMSTMP >= u.first_unsub_tm
    AND c.CHG_TMSTMP <  u.first_unsub_tm + INTERVAL '7' DAY
GROUP BY 1, 2
ORDER BY distinct_clients DESC;


-- ---------------------------------------------------------------------------
-- S5: which system writes the unsub-linked changes (APP_SYS_CD)
-- ---------------------------------------------------------------------------
-- Proves: the writing system for changes that follow email unsubs — the
-- "email vendor -> CPC" pipe, useful for defending the linkage.
-- ---------------------------------------------------------------------------

WITH unsub_clients AS (
    SELECT
        m.CLNT_NO,
        MIN(e.disposition_dt_tm) AS first_unsub_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'
    GROUP BY 1
)
SELECT
    c.APP_SYS_CD,
    c.PREF_ID,
    CAST(COUNT(*) AS BIGINT)   AS change_rows,
    COUNT(DISTINCT c.CLNT_NO)  AS distinct_clients
FROM unsub_clients u
INNER JOIN DDWV01.CPC_RB_PREF_LOG c
    ON  c.CLNT_NO = u.CLNT_NO
    AND c.CHG_TMSTMP >= u.first_unsub_tm
    AND c.CHG_TMSTMP <  u.first_unsub_tm + INTERVAL '7' DAY
GROUP BY 1, 2
ORDER BY distinct_clients DESC;
