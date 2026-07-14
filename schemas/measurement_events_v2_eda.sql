-- measurement_events_v2 EDA — event_cd catalog
-- Goal: enumerate every event_cd in the Success Library events table so we can map codes
-- to campaigns (PCD, PCQ, PCL, VBA, VBU) and repeat the CRV-style vintage validation.
-- Engine: Starburst/Trino (edl0_im catalog forces Trino syntax).
-- Only known code so far: 'p_card_installmt_purch' (CRV). No repo catalog exists.

-- ============================================================
-- Q0 — full event_cd catalog
-- Proves: which codes exist, their volume, client/account coverage, and history depth.
-- min/max event_date tells us whether a code covers each campaign's deployment window.
-- ============================================================
SELECT
    event_cd,
    COUNT(*)                    AS row_ct,
    COUNT(DISTINCT clnt_no)     AS clnt_ct,
    COUNT(DISTINCT acct_no)     AS acct_ct,
    MIN(event_date)             AS min_event_date,
    MAX(event_date)             AS max_event_date
FROM edl0_im.prod_zp10_prod_staging.measurement_events_v2
-- if the full-history scan is slow, floor it first and rerun unfloored later:
-- WHERE event_date >= DATE '2024-01-01'
GROUP BY event_cd
ORDER BY row_ct DESC;

-- ============================================================
-- Q1 — event_cd x month grid (recent history)
-- Proves: continuity and recency per code — a code that stopped populating months ago
-- can't validate an in-flight campaign. Counts only; pool downstream.
-- ============================================================
SELECT
    event_cd,
    DATE_TRUNC('month', event_date) AS event_month,
    COUNT(*)                        AS row_ct,
    COUNT(DISTINCT acct_no)         AS acct_ct
FROM edl0_im.prod_zp10_prod_staging.measurement_events_v2
WHERE event_date >= DATE '2025-01-01'
GROUP BY 1, 2
ORDER BY 1, 2;

-- ============================================================
-- Q2 — key format profile per event_cd
-- Proves: how acct_no / clnt_no are populated per code, so we know the join-key
-- normalization needed per campaign. CRV trap: acct_no is zero-padded varchar and
-- only joined after CAST(... AS DECIMAL(38,0)) on both sides.
-- ============================================================
SELECT
    event_cd,
    LENGTH(acct_no)                                 AS acct_no_len,
    COUNT(*)                                        AS row_ct,
    SUM(CASE WHEN acct_no IS NULL THEN 1 ELSE 0 END) AS acct_null_ct,
    SUM(CASE WHEN clnt_no IS NULL THEN 1 ELSE 0 END) AS clnt_null_ct
FROM edl0_im.prod_zp10_prod_staging.measurement_events_v2
WHERE event_date >= DATE '2026-01-01'
GROUP BY 1, 2
ORDER BY 1, 2;
