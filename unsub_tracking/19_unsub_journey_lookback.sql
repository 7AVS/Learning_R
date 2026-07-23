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
-- CAUTION: data source is ONLY DTZV01 tables, plus FOUR session-scoped
-- VOLATILE TABLEs (all dropped at the end of this script) that stage the
-- build in small pieces on purpose — see the v4 note below for why one big
-- population-spine statement was replaced with four small ones:
--   vt_unsub_events      (VT1) — all disposition_cd=4 EVENT rows, tiny
--   vt_unsub_resolved    (VT2) — VT1 resolved to CLNT_NO via MASTER, w/ rn
--   vt_baseline_spine    (VT3) — sliced, deduped, never-unsubbed baseline candidates
--   vt_unsub_journey_pop (VT4) — population spine: unsub cohort UNION ALL baseline
-- Converted 2026-07-22 from a Trino draft that called APPROX_PERCENTILE — Teradata has
-- no percentile-approx builtin (error 3706 "Data type lookback_contacts does
-- not match a defined type name" in-env, Teradata's signature error for an
-- unknown function). Banded exact counts replace the percentiles; no
-- accuracy lost — these are exact, not approximated.
--
-- SPOTLIGHT WINDOW (edit all literals below together; example = Q2 2026):
--   start (inclusive) = DATE '2026-04-01'    end (exclusive) = DATE '2026-07-01'
-- The start/end pair now appears in FOUR places after the v4 restage (was two
-- in v2, three in v3): VT3's EVENT send bound, VT3's MASTER load_tm margin
-- (computed off the same two literals via ADD_MONTHS, not retyped), VT4's
-- unsub-cohort filter, and the final lookback's 15-month send bound. Keep all
-- four in sync when rolling the spotlight window forward.
--
-- The 2024-01-01 full-history floor that used to gate the unsub branch (v2/v3,
-- mirroring pack 13's convention) is RETIRED in v4. VT1 now scans
-- disposition_cd=4 truly unbounded — pack 18's retention probe confirmed
-- disposition_cd=4 is a small fraction of EVENT even across the full ~7-year
-- retention window, so the unbounded scan is cheap, and dropping the floor
-- fixes the imprecision the old note flagged (a client whose TRUE first unsub
-- predated 2024-01-01 was previously misread here as "never unsubbed" — that
-- can no longer happen). This floor removal touches ONLY the unsub-resolution
-- branch (VT1/VT2); it was never a spool driver (disposition_cd=4 rows are a
-- small fraction of EVENT next to disposition_cd=1) and is unrelated to the
-- baseline branch's spool fix below.
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
--
-- v4 2026-07-23: staged volatile pipeline — unsub set materialized first,
-- MASTER never redistributed, client-slice sampling moved BEFORE the heavy
-- join (statement-1 spool fix). Andre re-ran v3 Teradata-direct and
-- STATEMENT 1 itself (the single CREATE VOLATILE TABLE vt_unsub_journey_pop)
-- ran out of spool — everything downstream cascaded, unreached. Two root
-- causes inside that one statement:
--   (a) CLNT_NO resolution (first_unsub_all, pop_sends) joined EVENT-derived
--       sets through an UNBOUNDED MASTER (~7 years, billions of rows) with no
--       materialized/stats-collected small side for the optimizer to key off
--       — same redistribute-the-big-side risk as the v2 baseline bug, just
--       on the resolution join instead of the lookback join.
--   (b) the baseline branch (pop_sends -> window_sends) scanned the full
--       ~150M-row in-window send set and GROUPed it down to ~10M clients
--       BEFORE baseline_sampled's SAMPLE 500000 ran — SAMPLE applies to a
--       finished result set, not a pushable predicate, so it cannot be
--       pushed ahead of a GROUP BY the way an ordinary WHERE filter can.
--       Sampling last saved nothing upstream; the full join+aggregate cost
--       was already paid.
-- Fix: split the single statement into four smaller CREATE VOLATILE TABLE
-- stages (vt_unsub_events -> vt_unsub_resolved -> vt_baseline_spine ->
-- vt_unsub_journey_pop), each with its own COLLECT STATISTICS, so every join
-- downstream has a real, stats-backed small side instead of an inferred one:
--   1. vt_unsub_events materializes ONLY the disposition_cd=4 rows first —
--      tiny and unbounded (see the retired-floor note above) — so it can be
--      duplicated across AMPs when it later probes MASTER, instead of
--      forcing MASTER to redistribute.
--   2. vt_baseline_spine replaces the post-aggregation SAMPLE with a
--      deterministic MOD(CLNT_NO, 10) = 0 client slice applied as a
--      single-table WHERE predicate on MASTER — BEFORE the GROUP BY, not
--      after — cutting the join+aggregation volume itself by ~10x instead of
--      trimming only the final row count.
-- Net effect: MASTER is still scanned three times across the file (VT2's
-- resolution join, VT3's baseline join, STEP 5's lookback join) — that isn't
-- new — but every one of those three scans now either probes against a tiny
-- stats-collected table (VT1 in VT2's case, vt_unsub_journey_pop in STEP 5's
-- case) or is itself pre-filtered by the client slice + a load_tm margin
-- (VT3's case) before the expensive part of the plan runs.
-- =============================================================================

-- Drop residual volatile tables if rerunning in the same session (in this
-- order — later stages depend on earlier ones, though Teradata does not
-- enforce that at DROP time):
--   DROP TABLE vt_unsub_journey_pop;
--   DROP TABLE vt_baseline_spine;
--   DROP TABLE vt_unsub_resolved;
--   DROP TABLE vt_unsub_events;

-- =============================================================================
-- STEP 1 / VT1: vt_unsub_events — ALL disposition_cd=4 EVENT rows, unbounded
-- Tiny and cheap (unsub rows are a small fraction of EVENT, per pack 18's
-- retention probe) — materializing this FIRST, with stats, is what lets VT2
-- duplicate it across AMPs against MASTER instead of redistributing MASTER.
-- PRIMARY INDEX = the exact MASTER join key.
-- =============================================================================
CREATE VOLATILE TABLE vt_unsub_events AS (
    SELECT
        e.consumer_id_hashed,
        e.TREATMENT_ID,
        e.disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_cd = 4
    -- no date floor here — see the header's "2024-01-01 floor... RETIRED in
    -- v4" note; true first-ever-unsub semantics need the full history, and
    -- this scan is cheap regardless of span (disposition_cd=4 is a small
    -- slice of the ~80-128M rows/quarter pack 18 measured across all
    -- disposition codes).
) WITH DATA PRIMARY INDEX (consumer_id_hashed, TREATMENT_ID) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_events COLUMN (consumer_id_hashed, TREATMENT_ID);


-- =============================================================================
-- STEP 2 / VT2: vt_unsub_resolved — VT1 resolved to CLNT_NO via MASTER
-- VT1 is tiny + stats-collected, so this join duplicates VT1 across AMPs and
-- streams MASTER past it — MASTER itself is NOT redistributed. rn = 1 is the
-- TRUE first-ever unsub per client (mirrors 13_unsub_value_spine.sql's S1).
-- Every row (not just rn=1) feeds VT3's "ever unsubbed anywhere" exclusion.
-- =============================================================================
CREATE VOLATILE TABLE vt_unsub_resolved AS (
    SELECT
        m.CLNT_NO,
        u.disposition_dt_tm AS first_unsub_tm,
        ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO
                           ORDER BY u.disposition_dt_tm ASC) AS rn
    FROM vt_unsub_events u
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = u.consumer_id_hashed
        AND m.TREATMENT_ID       = u.TREATMENT_ID
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_resolved COLUMN (CLNT_NO);


-- =============================================================================
-- STEP 3 / VT3: vt_baseline_spine — sliced, deduped, never-unsubbed baseline
-- candidates. This is the branch that actually blew the spool (v2's ~10M
-- candidates joined unsampled; v3's SAMPLE ran too late). Fix: a deterministic
-- CLNT_NO slice applied as a single-table WHERE predicate on MASTER, BEFORE
-- the GROUP BY — cuts the join+aggregation volume itself, not just the final
-- row count. MASTER is ALSO bound here by a load_tm +/- 1mo margin around the
-- spotlight window (MASTER has no send-date column —
-- schemas/vendor_feedback_tables_schema.md "Hard facts"; load_tm is a
-- load-time proxy, not a true send timestamp, per pack 18 — the 1mo margin is
-- a buffer against load lag/lead, ASSUMED safe, not verified; revisit if
-- VT3's row count looks off against expectations).
-- =============================================================================
CREATE VOLATILE TABLE vt_baseline_spine AS (
    WITH baseline_sends AS (
        SELECT
            m.CLNT_NO,
            e.disposition_dt_tm
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
            ON  m.consumer_id_hashed = e.consumer_id_hashed
            AND m.TREATMENT_ID       = e.TREATMENT_ID
        WHERE e.disposition_cd = 1
          AND e.disposition_dt_tm >= DATE '2026-04-01'                     -- SPOTLIGHT start (inclusive)
          AND e.disposition_dt_tm <  DATE '2026-07-01'                     -- SPOTLIGHT end (exclusive)
          AND m.load_tm           >= ADD_MONTHS(DATE '2026-04-01', -1)     -- SPOTLIGHT start - 1mo margin
          AND m.load_tm           <  ADD_MONTHS(DATE '2026-07-01',  1)     -- SPOTLIGHT end + 1mo margin
          -- EDITABLE: slice modulus. MOD(CLNT_NO, 10) = 0 keeps ~1-in-10 of
          -- the baseline candidate pool (~10M -> ~1M clients) — raise the
          -- modulus for a smaller/cheaper slice, lower it for finer
          -- resolution. Single-table predicate on MASTER, applied BEFORE the
          -- GROUP BY below — unlike v3's post-aggregation SAMPLE 500000
          -- (which ran AFTER the full 150M-row scan/group and saved nothing
          -- upstream), this narrows the join+aggregation volume itself, not
          -- just the final row count.
          -- Consequence: 'stayed' n_clients is no longer a true population
          -- count once sliced — read every 'stayed' number downstream as a
          -- SHARE/DISTRIBUTION within its own cohort_month at a ~1-in-10
          -- slice fraction, never as a raw count compared directly against
          -- 'unsub' (which IS a true, unsliced count). CLNT_NO is
          -- uncorrelated with client behavior, so a systematic 1-in-10 slice
          -- on the ID itself is statistically equivalent to random sampling
          -- for distributional reads — and, unlike SAMPLE, it's an ordinary
          -- WHERE predicate the optimizer can push ahead of the join/GROUP BY.
          AND MOD(m.CLNT_NO, 10) = 0
    ),
    window_sends AS (
        SELECT CLNT_NO, MAX(disposition_dt_tm) AS last_send_dt
        FROM baseline_sends
        GROUP BY CLNT_NO
    )
    SELECT w.CLNT_NO, w.last_send_dt AS index_dt
    FROM window_sends w
    WHERE NOT EXISTS (
        SELECT 1 FROM vt_unsub_resolved au WHERE au.CLNT_NO = w.CLNT_NO
    )
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_baseline_spine COLUMN (CLNT_NO);


-- =============================================================================
-- STEP 4 / VT4: vt_unsub_journey_pop — population spine: unsub cohort UNION
-- ALL sliced baseline. Same role this table played pre-v4 (the final lookback
-- statement's small, stats-collected join side) — only its construction
-- (now staged, above) changed.
-- =============================================================================
CREATE VOLATILE TABLE vt_unsub_journey_pop AS (
    -- CAST on the first branch's literal: Teradata sizes a UNION ALL string
    -- column from the FIRST SELECT alone ('unsub' = 5 chars would otherwise
    -- truncate 'stayed' = 6 chars to 'staye' — CLAUDE.md hard rule #3).
    SELECT CLNT_NO, first_unsub_tm AS index_dt, CAST('unsub' AS VARCHAR(10)) AS cohort_group
    FROM vt_unsub_resolved
    WHERE rn = 1
      AND first_unsub_tm >= DATE '2026-04-01'   -- SPOTLIGHT start (inclusive)
      AND first_unsub_tm <  DATE '2026-07-01'   -- SPOTLIGHT end (exclusive)
    UNION ALL
    SELECT CLNT_NO, index_dt, CAST('stayed' AS VARCHAR(10)) AS cohort_group
    FROM vt_baseline_spine
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_journey_pop COLUMN (CLNT_NO);


-- =============================================================================
-- STEP 5: 12-month lookback against the (small, stats-collected) population
-- spine. sends is re-declared here (CTEs don't carry across statements —
-- 13/16 precedent) bounded to the 15-month span the lookback actually needs
-- (spotlight start - 12mo, through spotlight end), then narrowed to ONLY
-- population clients (sends_pop) BEFORE the per-client date-range logic --
-- narrow-to-small-key-set-first, per query_engine_guidelines.md's Federation
-- Performance rule #2. No precision lost: this trims the CLIENT set the
-- date-range comparison runs against, not the date grain itself. MASTER here
-- is unbounded again, same as VT2 — safe for the same reason: it's probed by
-- the tiny, stats-collected vt_unsub_journey_pop (via sends_pop's narrowing
-- inner join), not redistributed.
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
DROP TABLE vt_baseline_spine;
DROP TABLE vt_unsub_resolved;
DROP TABLE vt_unsub_events;
