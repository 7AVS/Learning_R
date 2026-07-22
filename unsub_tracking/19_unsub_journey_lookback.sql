-- =============================================================================
-- Unsub journey lookback — contact history before first-unsub vs a stayed baseline
-- =============================================================================
-- Anatomy of an Unsub, number 2 (journey). Two groups over the SAME spotlight
-- window, symmetric send-anchored indexing:
--   unsub  = first-ever-unsub clients (disposition_cd=4, deduped to FIRST per
--            client, CLNT_NO resolved via MASTER — mirrors 13_unsub_value_spine.sql's
--            S1 spine exactly) whose first-ever unsub landed in the spotlight
--            window. index_dt = their first-unsub disposition_dt_tm.
--   stayed = clients with >=1 send (disposition_cd=1) in the SAME spotlight
--            window who NEVER unsub anywhere in the data. index_dt = their
--            OWN last send in the window.
--
-- Both groups are indexed on a communication-adjacent moment specific to that
-- client — the unsub is itself a reaction to a send, and the baseline's index
-- is its own last send — rather than an arbitrary shared calendar cutoff.
-- Indexing the unsub group on their event while giving the baseline a generic
-- cutoff (e.g. spotlight-quarter-end for everyone) would be a conditioning-bias
-- asymmetry between the two arms; this is why the baseline is NOT indexed on
-- a fixed calendar date.
--
-- Lookback: contacts (sends, disposition_cd=1) and distinct MNEs
-- (SUBSTR(treatment_id,8,3), per knowledge-doc §4/§2) in the 12 months
-- STRICTLY BEFORE index_dt, per client. ALL MNEs at extraction — no campaign
-- filter here; slice by MNE downstream in Excel/pivot, not in this query.
--
-- Spool guard: the base send scan keeps a static 2024-01-01 floor to prune the
-- scan up front; the per-client 12-month cutoff is a dynamic join condition
-- layered ON TOP of that floor, not a substitute for it.
--
-- Output: ONE decision-sized summary, cohort_group x cohort_month (hard rule:
-- cohort_month on every row): n_clients, AVG contacts / AVG distinct MNEs
-- (DECIMAL(10,1) — Teradata-direct, so the 9881 pushdown-ROUND hazard does NOT
-- apply here; that error only fires when Starburst pushes a Teradata-only
-- statement down and wraps the arithmetic in ROUND — there is no Starburst in
-- this path), plus a BANDED distribution of clients per lookback-contact count
-- and per lookback-MNE count (median is read off the bands downstream). Bands
-- are an EDITABLE ASSUMPTION — adjust the CASE WHEN breakpoints in the final
-- SELECT if the shape doesn't fit:
--   contacts: 0 / 1 / 2 / 3-4 / 5-6 / 7-9 / 10-14 / 15+
--   mnes:     0 / 1 / 2 / 3 / 4 / 5+
--
-- CAUTION: this statement touches ONLY DTZV01 tables. Converted 2026-07-22
-- from a Trino draft that called APPROX_PERCENTILE — Teradata has no
-- percentile-approx builtin (error 3706 "Data type lookback_contacts does not
-- match a defined type name" in-env, Teradata's signature error for an unknown
-- function). Banded exact counts replace the percentiles; no accuracy lost —
-- these are exact, not approximated.
--
-- SPOTLIGHT WINDOW (edit all four literals below together; example = Q2 2026):
--   start (inclusive) = DATE '2026-04-01'    end (exclusive) = DATE '2026-07-01'
-- 2024-01-01 floor on unsub history mirrors pack 13's convention — a baseline
-- client whose TRUE first unsub predates 2024-01-01 would be misread here as
-- "never unsubbed." Accepted for consistency with the rest of the repo; note
-- it if precision below the floor ever matters.
-- ENGINE: Teradata-direct.
-- =============================================================================

WITH first_unsub_all AS (          -- mirrors 13_unsub_value_spine.sql S1 exactly
    SELECT
        m.CLNT_NO,
        e.disposition_dt_tm,
        ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO
                           ORDER BY e.disposition_dt_tm ASC) AS rn
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'
),
unsub_cohort AS (                  -- group A: first-ever unsub landed in the spotlight window
    SELECT CLNT_NO, disposition_dt_tm AS index_dt
    FROM first_unsub_all
    WHERE rn = 1
      AND disposition_dt_tm >= DATE '2026-04-01'   -- SPOTLIGHT start (inclusive)
      AND disposition_dt_tm <  DATE '2026-07-01'   -- SPOTLIGHT end (exclusive)
),
any_unsub_client AS (              -- any unsub ever (same floor) -> baseline exclusion set
    SELECT DISTINCT CLNT_NO FROM first_unsub_all
),
sends AS (                         -- disposition_cd=1 = sent; reused for baseline pop + lookback below
    SELECT
        m.CLNT_NO,
        e.TREATMENT_ID,
        e.disposition_dt_tm,
        SUBSTR(e.TREATMENT_ID, 8, 3) AS mne
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2024-01-01'   -- static floor -> spool guard (see header)
),
window_sends AS (                  -- sends landing inside the spotlight window (baseline candidate pool)
    SELECT CLNT_NO, MAX(disposition_dt_tm) AS last_send_dt
    FROM sends
    WHERE disposition_dt_tm >= DATE '2026-04-01'     -- SPOTLIGHT start (inclusive)
      AND disposition_dt_tm <  DATE '2026-07-01'     -- SPOTLIGHT end (exclusive)
    GROUP BY CLNT_NO
),
baseline_cohort AS (               -- group B: sent in window, never unsubbed anywhere -> symmetric send-indexed risk set
    SELECT w.CLNT_NO, w.last_send_dt AS index_dt
    FROM window_sends w
    LEFT JOIN any_unsub_client au ON au.CLNT_NO = w.CLNT_NO
    WHERE au.CLNT_NO IS NULL
),
population AS (
    -- CAST on the first branch's literal: Teradata sizes a UNION ALL string
    -- column from the FIRST SELECT alone ('unsub' = 5 chars would otherwise
    -- truncate 'stayed' = 6 chars to 'staye' — CLAUDE.md hard rule #3).
    SELECT CLNT_NO, index_dt, CAST('unsub'  AS VARCHAR(10)) AS cohort_group FROM unsub_cohort
    UNION ALL
    SELECT CLNT_NO, index_dt, CAST('stayed' AS VARCHAR(10)) AS cohort_group FROM baseline_cohort
),
lookback AS (                      -- 12 months strictly before each client's OWN index date
    SELECT
        p.CLNT_NO,
        p.cohort_group,
        p.index_dt,
        COUNT(DISTINCT s.TREATMENT_ID) AS lookback_contacts,
        COUNT(DISTINCT s.mne)          AS lookback_mnes
    FROM population p
    LEFT JOIN sends s
        ON  s.CLNT_NO = p.CLNT_NO
        AND s.disposition_dt_tm >= ADD_MONTHS(CAST(p.index_dt AS DATE), -12)
        AND s.disposition_dt_tm <  p.index_dt
    GROUP BY 1, 2, 3
)
SELECT
    cohort_group,
    EXTRACT(YEAR FROM index_dt) * 100 + EXTRACT(MONTH FROM index_dt) AS cohort_month,
    COUNT(DISTINCT CLNT_NO)                       AS n_clients,
    CAST(AVG(lookback_contacts) AS DECIMAL(10,1)) AS avg_contacts,
    CAST(AVG(lookback_mnes)     AS DECIMAL(10,1)) AS avg_mnes,
    -- lookback_contacts bands (editable breakpoints — see header)
    SUM(CASE WHEN lookback_contacts = 0             THEN 1 ELSE 0 END) AS contacts_0,
    SUM(CASE WHEN lookback_contacts = 1             THEN 1 ELSE 0 END) AS contacts_1,
    SUM(CASE WHEN lookback_contacts = 2             THEN 1 ELSE 0 END) AS contacts_2,
    SUM(CASE WHEN lookback_contacts BETWEEN 3 AND 4 THEN 1 ELSE 0 END) AS contacts_3_4,
    SUM(CASE WHEN lookback_contacts BETWEEN 5 AND 6 THEN 1 ELSE 0 END) AS contacts_5_6,
    SUM(CASE WHEN lookback_contacts BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS contacts_7_9,
    SUM(CASE WHEN lookback_contacts BETWEEN 10 AND 14 THEN 1 ELSE 0 END) AS contacts_10_14,
    SUM(CASE WHEN lookback_contacts >= 15           THEN 1 ELSE 0 END) AS contacts_15p,
    -- lookback_mnes bands (editable breakpoints — see header)
    SUM(CASE WHEN lookback_mnes = 0                 THEN 1 ELSE 0 END) AS mnes_0,
    SUM(CASE WHEN lookback_mnes = 1                 THEN 1 ELSE 0 END) AS mnes_1,
    SUM(CASE WHEN lookback_mnes = 2                 THEN 1 ELSE 0 END) AS mnes_2,
    SUM(CASE WHEN lookback_mnes = 3                 THEN 1 ELSE 0 END) AS mnes_3,
    SUM(CASE WHEN lookback_mnes = 4                 THEN 1 ELSE 0 END) AS mnes_4,
    SUM(CASE WHEN lookback_mnes >= 5                THEN 1 ELSE 0 END) AS mnes_5p
FROM lookback
GROUP BY 1, 2
ORDER BY 1, 2;
