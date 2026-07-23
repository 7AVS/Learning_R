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
-- CAUTION: data source is ONLY DTZV01 tables, plus one session-scoped
-- VOLATILE TABLE (vt_unsub_journey_pop, dropped at the end of this script)
-- used purely to break the spool plan — see the v3 note below. Converted
-- 2026-07-22 from a Trino draft that called APPROX_PERCENTILE — Teradata has
-- no percentile-approx builtin (error 3706 "Data type lookback_contacts does
-- not match a defined type name" in-env, Teradata's signature error for an
-- unknown function). Banded exact counts replace the percentiles; no
-- accuracy lost — these are exact, not approximated.
--
-- SPOTLIGHT WINDOW (edit all literals below together; example = Q2 2026):
--   start (inclusive) = DATE '2026-04-01'    end (exclusive) = DATE '2026-07-01'
-- The start/end pair now appears in THREE places after the v3 restructure
-- below (was two in v2) — unsub_cohort, pop_sends, and the 15-month sends
-- bound in the lookback statement. Keep all three pairs in sync when rolling
-- the spotlight window forward.
--
-- 2024-01-01 floor on unsub history (first_unsub_all / any_unsub_client only)
-- mirrors pack 13's convention — a baseline client whose TRUE first unsub
-- predates 2024-01-01 would be misread here as "never unsubbed." Accepted for
-- consistency with the rest of the repo; note it if precision below the
-- floor ever matters. This floor is UNCHANGED by the v3 spool fix — it
-- protects a different invariant (true full-history "never unsubbed"
-- exclusion) than the send scans do, and disposition_cd=4 rows were never
-- the spool problem (a small fraction of EVENT next to disposition_cd=1).
-- ENGINE: Teradata-direct.
--
-- v3 2026-07-22: spool fix — bounded scans, pre-aggregation, baseline SAMPLE
-- 500K (editable). Andre ran v2 Teradata-direct and ran OUT OF SPOOL. Root
-- cause: the stayed baseline (~10M unsampled candidate clients) was being
-- joined directly against a send scan floored only at a flat 2024-01-01 (30
-- months, ~120-180M rows/quarter per pack 18's retention probe) with a
-- per-row dynamic date-range join condition — a big-table x big-table join
-- that forces Teradata to redistribute the ENTIRE send scan by CLNT_NO
-- before it can discard a single non-match. Three changes, in order:
--   1. Bounded scans: every send scan now carries a floor tied to the
--      spotlight window instead of the flat house-wide 2024-01-01 floor.
--      pop_sends (candidate-pool lookup, STEP 1) needs only the 3-month
--      spotlight window itself; the lookback join's sends CTE (STEP 2) needs
--      15 months (spotlight start - 12mo, through spotlight end). Both
--      replace the old 30-month floor-to-today scan with the minimum the
--      analytical design actually requires — same data, no accuracy lost,
--      just no longer scanning months nothing downstream reads.
--   2. Baseline SAMPLE: baseline_sampled applies Teradata's SAMPLE 500000 to
--      the deduped stayed-candidate spine (comment there covers the
--      within-group-only read this creates downstream). unsub_cohort is
--      NEVER sampled — it is the full first-unsub population and is already
--      small.
--   3. Population spine materialized as a VOLATILE TABLE (vt_unsub_journey_pop,
--      ~<1M rows post-sample: unsub_cohort + 500K sampled baseline) WITH
--      COLLECT STATISTICS on the join key, so the optimizer can treat it as
--      the small side of the join (duplicate it across AMPs) instead of
--      redistributing the send scan. STEP 2 additionally pre-narrows the
--      bounded send scan to ONLY population clients (sends_pop, an exact-key
--      inner join) BEFORE the per-client date-range logic runs, per
--      query_engine_guidelines.md's "narrow to the small key set first, then
--      join on exact keys" rule. This loses NO day-level precision (unlike a
--      calendar-month pre-aggregation would have) — it only trims the
--      client set the date-range comparison runs against, not the date grain.
-- =============================================================================

-- Drop residual volatile table if rerunning in the same session:
--   DROP TABLE vt_unsub_journey_pop;

-- =============================================================================
-- STEP 1: population spine — unsub cohort (unsampled) + SAMPLED stayed baseline
-- Materialized as a volatile table so STEP 2's lookback join has a small,
-- stats-collected side to join against instead of redistributing the send
-- scan. PRIMARY INDEX on CLNT_NO = the join key STEP 2 uses.
-- =============================================================================
CREATE VOLATILE TABLE vt_unsub_journey_pop AS (
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
          AND e.disposition_dt_tm >= DATE '2024-01-01'   -- full-history floor, unchanged (see header)
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
    pop_sends AS (                     -- bounded to the spotlight window ONLY (3mo): all this step
                                        -- needs is each candidate's own last send in-window. The
                                        -- 12-month lookback scan lives in STEP 2, not here.
        SELECT
            m.CLNT_NO,
            e.disposition_dt_tm
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
            ON  m.consumer_id_hashed = e.consumer_id_hashed
            AND m.TREATMENT_ID       = e.TREATMENT_ID
        WHERE e.disposition_cd = 1
          AND e.disposition_dt_tm >= DATE '2026-04-01'   -- SPOTLIGHT start (inclusive)
          AND e.disposition_dt_tm <  DATE '2026-07-01'   -- SPOTLIGHT end (exclusive)
    ),
    window_sends AS (
        SELECT CLNT_NO, MAX(disposition_dt_tm) AS last_send_dt
        FROM pop_sends
        GROUP BY CLNT_NO
    ),
    baseline_cohort AS (               -- group B candidates: sent in window, never unsubbed anywhere
        SELECT w.CLNT_NO, w.last_send_dt AS index_dt
        FROM window_sends w
        LEFT JOIN any_unsub_client au ON au.CLNT_NO = w.CLNT_NO
        WHERE au.CLNT_NO IS NULL
    ),
    baseline_sampled AS (              -- SAMPLE the stayed baseline ONLY -- unsub_cohort is never sampled
        -- EDITABLE: sample size. 500K is more than sufficient precision for
        -- banded distribution contrasts against the unsub cohort -- raise or
        -- lower if the downstream read needs finer/coarser resolution.
        -- Consequence: once sampled, 'stayed' n_clients is no longer a true
        -- population count. Read every 'stayed' number downstream as a
        -- SHARE/DISTRIBUTION within its own cohort_month, never as a raw
        -- count compared directly against 'unsub' (which IS a true,
        -- unsampled count).
        SELECT CLNT_NO, index_dt
        FROM baseline_cohort
        SAMPLE 500000
    )
    -- CAST on the first branch's literal: Teradata sizes a UNION ALL string
    -- column from the FIRST SELECT alone ('unsub' = 5 chars would otherwise
    -- truncate 'stayed' = 6 chars to 'staye' — CLAUDE.md hard rule #3).
    SELECT CLNT_NO, index_dt, CAST('unsub'  AS VARCHAR(10)) AS cohort_group FROM unsub_cohort
    UNION ALL
    SELECT CLNT_NO, index_dt, CAST('stayed' AS VARCHAR(10)) AS cohort_group FROM baseline_sampled
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_journey_pop COLUMN (CLNT_NO);


-- =============================================================================
-- STEP 2: 12-month lookback against the (small, sampled) population spine
-- sends is re-declared here (CTEs don't carry across statements — 13/16
-- precedent) bounded to the 15-month span the lookback actually needs
-- (spotlight start - 12mo, through spotlight end), then narrowed to ONLY
-- population clients (sends_pop) BEFORE the per-client date-range logic --
-- narrow-to-small-key-set-first, per query_engine_guidelines.md's Federation
-- Performance rule #2. No precision lost: this trims the CLIENT set the
-- date-range comparison runs against, not the date grain itself.
-- =============================================================================
WITH sends AS (
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
      AND e.disposition_dt_tm >= ADD_MONTHS(DATE '2026-04-01', -12)  -- 12mo before SPOTLIGHT start = 2025-04-01
      AND e.disposition_dt_tm <  DATE '2026-07-01'                   -- SPOTLIGHT end (exclusive)
),
sends_pop AS (                     -- narrow to population clients BEFORE the per-client date range/aggregation
    SELECT s.CLNT_NO, s.TREATMENT_ID, s.disposition_dt_tm, s.mne
    FROM sends s
    INNER JOIN vt_unsub_journey_pop p ON p.CLNT_NO = s.CLNT_NO
),
lookback AS (                      -- 12 months strictly before each client's OWN index date
    SELECT
        p.CLNT_NO,
        p.cohort_group,
        p.index_dt,
        COUNT(DISTINCT s.TREATMENT_ID) AS lookback_contacts,
        COUNT(DISTINCT s.mne)          AS lookback_mnes
    FROM vt_unsub_journey_pop p
    LEFT JOIN sends_pop s
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

DROP TABLE vt_unsub_journey_pop;
