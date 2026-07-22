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
-- Pushdown guard (canon Federation Perf rule #1): the base send scan keeps a
-- static 2024-01-01 floor (pushes to Teradata); the per-client 12-month cutoff
-- is a dynamic join condition layered ON TOP of that floor, not a substitute
-- for it.
--
-- Output: ONE decision-sized summary, cohort_group x cohort_month (hard rule:
-- cohort_month on every row). Counts + APPROX_PERCENTILE(25/50/75) of counts
-- only — no rate division, no decimal casts (Teradata 9881 ROUND hazard).
--
-- CAUTION: this statement touches ONLY DTZV01 tables (single-source Teradata).
-- APPROX_PERCENTILE is a Trino builtin with no Teradata equivalent. If
-- Starburst fully pushes this down and the call errors, fall back to
-- MIN/MAX/AVG/STDDEV_POP (the same swap PCD's curated-EDA E3/E4 made — see
-- campaigns/PCD/pcd_2026111_curated_eda.sql) or export lookback_contacts /
-- lookback_mnes at client grain and percentile downstream in Excel.
--
-- SPOTLIGHT WINDOW (edit all four literals below together; example = Q2 2026):
--   start (inclusive) = DATE '2026-04-01'    end (exclusive) = DATE '2026-07-01'
-- 2024-01-01 floor on unsub history mirrors pack 13's convention — a baseline
-- client whose TRUE first unsub predates 2024-01-01 would be misread here as
-- "never unsubbed." Accepted for consistency with the rest of the repo; note
-- it if precision below the floor ever matters.
-- ENGINE: Starburst/Trino.
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
      AND e.disposition_dt_tm >= DATE '2024-01-01'   -- static floor -> pushes down (canon rule #1)
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
    SELECT CLNT_NO, index_dt, 'unsub'  AS cohort_group FROM unsub_cohort
    UNION ALL
    SELECT CLNT_NO, index_dt, 'stayed' AS cohort_group FROM baseline_cohort
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
        AND s.disposition_dt_tm >= p.index_dt - INTERVAL '12' MONTH
        AND s.disposition_dt_tm <  p.index_dt
    GROUP BY 1, 2, 3
)
SELECT
    cohort_group,
    EXTRACT(YEAR FROM index_dt) * 100 + EXTRACT(MONTH FROM index_dt) AS cohort_month,
    COUNT(DISTINCT CLNT_NO)                    AS n_clients,
    approx_percentile(lookback_contacts, 0.25) AS contacts_p25,
    approx_percentile(lookback_contacts, 0.50) AS contacts_p50,
    approx_percentile(lookback_contacts, 0.75) AS contacts_p75,
    approx_percentile(lookback_mnes, 0.25)     AS mnes_p25,
    approx_percentile(lookback_mnes, 0.50)     AS mnes_p50,
    approx_percentile(lookback_mnes, 0.75)     AS mnes_p75
FROM lookback
GROUP BY 1, 2
ORDER BY 1, 2;
