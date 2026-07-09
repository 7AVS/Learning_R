-- >>> SET CURATED CATALOG: cards_crv_install_decis_resp Trino catalog is unconfirmed for CRV.
--     Candidates: dw00_im.dl_mr_prod.cards_crv_install_decis_resp  OR  dw00_jm.dl_mr_prod.cards_crv_install_decis_resp
--     (PCD uses dw00_jm; PLI/PCQ use dw00_im. Swap once here if the first errors.)
--     Every curated-table reference in this file uses dw00_im by default -- if
--     BLOCK 3a errors on catalog/schema not found, find/replace every
--     "dw00_im.dl_mr_prod.cards_crv_install_decis_resp" below to dw00_jm and rerun.
--     (Column names/types for this table -- acct_no, offer_start_date,
--     offer_end_date, action_control, test_group -- ARE confirmed, per
--     schemas/crv_pcl_curated_schemas.md. Only the catalog prefix is open.)

-- crv_edw_join_population_diagnostic.sql
-- PRIMARY PURPOSE (this file): explain WHY the Data Lab curated population
-- (cards_crv_install_decis_resp) and the tactic population
-- (dg6v01.tactic_evnt_ip_ar_hist) have DIFFERENT cohort sizes (distinct
-- account counts) for the CRV campaign. BLOCK 3 (3a-3d) below is the
-- headline -- run it first. Blocks 1 and 2 are retained secondary
-- diagnostics from an earlier pass on this same file (tactic key inspection;
-- join-signal check against the events table) and are kept further down for
-- reference -- they are not this file's current focus.
--
-- Same window everywhere unless a sub-block explicitly says otherwise:
--   tactic side:    treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'
--   Data Lab side:  offer_end_date BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'
-- CRV filter on tactic: substr(tactic_id, 8, 3) = 'CRV' (position 8-10 = MNE).
-- Account grain both sides, keys normalized via CAST(... AS DECIMAL(38,0))
-- (tactic visa_acct_no vs curated acct_no) -- same pattern already validated
-- against measurement_events_v2 in crv_vintage_v2_production.sql.
--
-- Engine: Starburst/Trino (EDW via federation). Trino syntax throughout: no
-- QUALIFY, no TOP (LIMIT instead), date_trunc/date_diff not RIGHT()/date math.
-- Counts + small sample rows only -- no rates, no formatting, nothing written back.
-- Each block below is a standalone, independently-runnable SELECT (or WITH...
-- SELECT) -- run one at a time, in order. CTEs are NOT materialized in Trino,
-- so a CTE referenced twice re-scans its source; most blocks here are already
-- narrowed to a small CRV window, so repeat scans are cheap (BLOCK 3d is the
-- one exception -- flagged there).
--
-- Trino risks flagged up front, apply throughout this file:
--   1. CAST-in-join / CAST-in-filter pushdown: casting a federated column
--      (acct_no, visa_acct_no) to DECIMAL(38,0) inside a predicate or set
--      operation can block Starburst from pushing the filter down to the
--      underlying connector, and will throw a hard error instead of a
--      silent zero-match if the source is actually a padded CHAR/VARCHAR
--      rather than a clean numeric type (needs TRY_CAST(TRIM(col) AS ...)
--      instead of a bare CAST). If any query below errors on the CAST,
--      that failure is itself diagnostic -- it means the two account keys
--      are not obviously the same representation and the formats must be
--      reconciled before any count in this file can be trusted.
--   2. Cross-catalog EXCEPT/INTERSECT: datalab_pop and tactic_pop are each
--      sourced from a DIFFERENT catalog (dw00_im vs dg6v01, both federated
--      through Starburst). Trino has to pull both sides fully into the
--      coordinator/workers to compute EXCEPT/INTERSECT rather than pushing
--      the set operation down to either connector -- expect this to be
--      slower than a single-catalog query, though every window in this
--      file except 3d is small enough that it should still be cheap.

-- ============================================================================
-- BLOCK 3: COHORT-SIZE / POPULATION COMPARISON -- Data Lab vs tactic
-- (THE HEADLINE OF THIS FILE). Account grain, same window both sides unless
-- a sub-block says otherwise. Sub-blocks 3a-3d below, run each standalone,
-- in order.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- BLOCK 3a: HEADLINE COUNTS -- one summary row.
-- datalab_accts / tactic_accts / in_both / only_datalab / only_tactic, all via
-- normalized-numeric set logic (CAST ... AS DECIMAL(38,0) both sides).
--
-- Key-normalization risk (flagged, not assumed away): acct_no on the curated
-- table is a confirmed `integer` type (schemas/crv_pcl_curated_schemas.md),
-- so the CAST here should be low-risk on that side. The tactic-side
-- visa_acct_no format is unconfirmed in a schema doc, but this CAST pattern
-- mirrors the one already validated for visa_acct_no against
-- measurement_events_v2's zero-padded acct_no in crv_vintage_v2_production.sql.
-- If the CAST fails outright, that failure is itself diagnostic: it means the
-- two account keys are not the same identifier space, and only_datalab /
-- only_tactic below would be invalid (not just inflated) until reconciled.
--
-- Filter-difference note: datalab_pop below filters to action_control IN
-- ('Action', 'Control'), while tactic_pop carries EVERY tst_grp_cd on the
-- tactic table with no equivalent filter applied here. If tactic carries
-- additional groups that never map into Action/Control on the curated side
-- (e.g. holdout groups, undeployed test cells), that filter asymmetry alone
-- can explain tactic_accts > datalab_accts -- BLOCK 3c below quantifies this.
-- ----------------------------------------------------------------------------
WITH
datalab_pop AS (
    SELECT DISTINCT CAST(acct_no AS DECIMAL(38,0)) AS acct_key
    FROM dw00_im.dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'
      AND TRIM(action_control) IN ('Action', 'Control')
),
tactic_pop AS (
    SELECT DISTINCT CAST(visa_acct_no AS DECIMAL(38,0)) AS acct_key
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'
),
both_sides AS (
    SELECT acct_key FROM datalab_pop
    INTERSECT
    SELECT acct_key FROM tactic_pop
),
only_datalab AS (
    SELECT acct_key FROM datalab_pop
    EXCEPT
    SELECT acct_key FROM tactic_pop
),
only_tactic AS (
    SELECT acct_key FROM tactic_pop
    EXCEPT
    SELECT acct_key FROM datalab_pop
)
SELECT
    (SELECT COUNT(*) FROM datalab_pop)  AS datalab_accts,
    (SELECT COUNT(*) FROM tactic_pop)   AS tactic_accts,
    (SELECT COUNT(*) FROM both_sides)   AS in_both,
    (SELECT COUNT(*) FROM only_datalab) AS only_datalab,
    (SELECT COUNT(*) FROM only_tactic)  AS only_tactic;


-- ----------------------------------------------------------------------------
-- BLOCK 3b: SAMPLES -- ~15 account keys only in Data Lab, ~15 only in tactic,
-- so Andre can eyeball who's actually missing on each side. Same CTEs as 3a,
-- repeated per half so each runs standalone.
--
-- READ THIS FIRST: if 3a's in_both came back near zero -- not just small, but
-- close to zero relative to BOTH datalab_accts and tactic_accts -- that is
-- NOT evidence of a real population gap. It signals the two account keys are
-- DIFFERENT IDENTIFIER SPACES entirely (wrong grain on one side, or a
-- padding/format mismatch the CAST didn't actually reconcile). In that case
-- only_datalab and only_tactic below will each look close to the full size
-- of their own side -- that pattern is diagnostic of a key mismatch, not a
-- real headcount reconciliation, and the samples below should be read as
-- "these keys don't look like the same kind of thing," not "these accounts
-- are missing."
-- ----------------------------------------------------------------------------

-- 3b-i: sample only_datalab (in Data Lab curated table, not found in tactic)
WITH
datalab_pop AS (
    SELECT DISTINCT CAST(acct_no AS DECIMAL(38,0)) AS acct_key
    FROM dw00_im.dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'
      AND TRIM(action_control) IN ('Action', 'Control')
),
tactic_pop AS (
    SELECT DISTINCT CAST(visa_acct_no AS DECIMAL(38,0)) AS acct_key
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'
),
only_datalab AS (
    SELECT acct_key FROM datalab_pop
    EXCEPT
    SELECT acct_key FROM tactic_pop
)
SELECT acct_key
FROM only_datalab
LIMIT 15;

-- 3b-ii: sample only_tactic (on tactic table, not found in Data Lab curated)
WITH
datalab_pop AS (
    SELECT DISTINCT CAST(acct_no AS DECIMAL(38,0)) AS acct_key
    FROM dw00_im.dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'
      AND TRIM(action_control) IN ('Action', 'Control')
),
tactic_pop AS (
    SELECT DISTINCT CAST(visa_acct_no AS DECIMAL(38,0)) AS acct_key
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'
),
only_tactic AS (
    SELECT acct_key FROM tactic_pop
    EXCEPT
    SELECT acct_key FROM datalab_pop
)
SELECT acct_key
FROM only_tactic
LIMIT 15;


-- ----------------------------------------------------------------------------
-- BLOCK 3c: GROUP-FILTER CHECK (new) -- does tactic carry test groups beyond
-- whatever the curated table's action_control filter keeps? Data Lab's
-- action_control IN ('Action', 'Control') filter (see 3a) has no tst_grp_cd
-- equivalent applied on the tactic side anywhere in this file -- if tactic
-- has groups that never resolve to Action or Control on the curated side,
-- that filter asymmetry alone could explain tactic_accts > datalab_accts.
--
-- IMPORTANT precedent conflict -- flagged, not resolved here: 3c-ii below
-- checks tst_grp_cd NOT IN ('TG4', 'TG8') as a first-pass guess at the
-- "excluded" groups. But the only CONFIRMED CRV arm derivation in this repo
-- (crv_vintage_v2_production.sql, crv_cohort_summary_v2_production.sql) is
-- CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END -- i.e. ONE
-- control group and EVERYTHING else falls into Action, with no third bucket
-- and no TG4 special-case. Under that derivation, a tst_grp_cd-only filter
-- could never exclude any group (the ELSE catches everything), so it would
-- NOT explain a tactic > datalab gap by itself -- unless the curated table's
-- actual action_control column was computed with a stricter rule than that
-- vintage script's simplification (e.g. true un-deployed holdout groups that
-- get dropped before the curated table is built, not merely relabeled).
-- Read 3c-i's actual tst_grp_cd distribution FIRST and decide which premise
-- holds before trusting 3c-ii's fixed group list -- do not treat TG4/TG8 as
-- confirmed for this table.
-- ----------------------------------------------------------------------------

-- 3c-i: distinct tactic accounts by tst_grp_cd, in-window, CRV only
SELECT
    tst_grp_cd,
    COUNT(DISTINCT visa_acct_no) AS distinct_accts
FROM dg6v01.tactic_evnt_ip_ar_hist
WHERE substr(tactic_id, 8, 3) = 'CRV'
  AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'
GROUP BY tst_grp_cd
ORDER BY distinct_accts DESC;

-- 3c-ii: distinct tactic accounts whose tst_grp_cd is OUTSIDE the ('TG4','TG8')
-- guess -- UNCONFIRMED mapping for this table, see the comment above. Re-run
-- with whatever group list 3c-i actually shows if TG4/TG8 doesn't match.
SELECT
    COUNT(DISTINCT visa_acct_no) AS accts_outside_tg4_tg8
FROM dg6v01.tactic_evnt_ip_ar_hist
WHERE substr(tactic_id, 8, 3) = 'CRV'
  AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'
  AND tst_grp_cd NOT IN ('TG4', 'TG8');


-- ----------------------------------------------------------------------------
-- BLOCK 3d: WINDOW-SENSITIVITY CHECK (new) -- same datalab_accts vs
-- tactic_accts comparison as 3a, but with NO date window: all CRV rows in
-- 2026 on the tactic side (substr(tactic_id,8,3)='CRV' only, no
-- treatmt_end_dt filter), and all Data Lab curated rows with
-- offer_start_date >= DATE '2026-01-01' on the curated side -- deliberately
-- offer_start_date here, not offer_end_date, since the point of this block
-- is to test whether 3a's gap is an artifact of comparing offer_end_date to
-- treatmt_end_dt rather than a true population difference. The
-- action_control filter is kept the same as 3a so this isolates the window
-- variable only (3c isolates the group-filter variable).
--
-- Why this matters: if the gap between datalab_accts and tactic_accts shrinks,
-- grows, or flips sign a lot once the date window is removed, that means 3a's
-- gap is largely WINDOW-DEFINITION-driven -- offer_end_date and
-- treatmt_end_dt are selecting genuinely different rows/timing for
-- overlapping populations (e.g. a tactic wave that starts before May but ends
-- inside the window, vs. a curated offer keyed to a different date field) --
-- and is not necessarily evidence of a true population gap between the two
-- source tables. If the gap looks roughly the same with or without the
-- window, that argues the two source tables really do carry different
-- populations, independent of timing.
--
-- Performance note: unlike every other block in this file, this removes the
-- date window entirely on both sides, so it scans a full-year CRV slice
-- rather than a 3-month one -- expect this to be slower than 3a-3c, though
-- still filtered to CRV only, not a full table scan.
-- ----------------------------------------------------------------------------
WITH
datalab_pop_all AS (
    SELECT DISTINCT CAST(acct_no AS DECIMAL(38,0)) AS acct_key
    FROM dw00_im.dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND TRIM(action_control) IN ('Action', 'Control')
),
tactic_pop_all AS (
    SELECT DISTINCT CAST(visa_acct_no AS DECIMAL(38,0)) AS acct_key
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
),
both_sides_all AS (
    SELECT acct_key FROM datalab_pop_all
    INTERSECT
    SELECT acct_key FROM tactic_pop_all
),
only_datalab_all AS (
    SELECT acct_key FROM datalab_pop_all
    EXCEPT
    SELECT acct_key FROM tactic_pop_all
),
only_tactic_all AS (
    SELECT acct_key FROM tactic_pop_all
    EXCEPT
    SELECT acct_key FROM datalab_pop_all
)
SELECT
    (SELECT COUNT(*) FROM datalab_pop_all)  AS datalab_accts_nowindow,
    (SELECT COUNT(*) FROM tactic_pop_all)   AS tactic_accts_nowindow,
    (SELECT COUNT(*) FROM both_sides_all)   AS in_both_nowindow,
    (SELECT COUNT(*) FROM only_datalab_all) AS only_datalab_nowindow,
    (SELECT COUNT(*) FROM only_tactic_all)  AS only_tactic_nowindow;


-- ============================================================================
-- SECONDARY DIAGNOSTICS RETAINED BELOW -- not this file's current primary
-- focus (that's BLOCK 3 above). BLOCK 1 (tactic key inspection) and BLOCK 2
-- (join-signal check) are from an earlier diagnostic pass on whether the
-- PySpark vintage notebook's zero-success finding traces to using
-- TACTIC_EVNT_ID as a client key instead of clnt_no. Unchanged from the
-- prior version of this file; kept here for reference.
-- ============================================================================

-- ============================================================================
-- BLOCK 1a: TACTIC KEY INSPECTION -- raw sample rows
-- Goal: SEE whether clnt_no is actually populated on CRV rows, and eyeball how
-- tactic_evnt_id compares to it and to visa_acct_no. tactic_evnt_id is
-- selected here as written even though it is not a confirmed CRV column in
-- this repo (it IS a confirmed column on this same table for AUH tactic
-- rows) -- if it does not exist for this table/catalog binding, this query
-- will error on an unknown column and that itself answers the question.
-- ============================================================================
SELECT
    tactic_id,
    tactic_evnt_id,
    clnt_no,
    visa_acct_no,
    tst_grp_cd,
    treatmt_strt_dt,
    treatmt_end_dt
FROM dg6v01.tactic_evnt_ip_ar_hist
WHERE substr(tactic_id, 8, 3) = 'CRV'
  AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'
LIMIT 25;

-- ============================================================================
-- BLOCK 1b: TACTIC KEY INSPECTION -- cardinality comparison
-- Goal: does tactic_evnt_id have a wildly different distinct count than
-- clnt_no? total_rows is included as a free reference point -- if
-- distinct_tactic_evnt_id is close to total_rows (near-unique per row) while
-- distinct_clnt_no is much lower (clients can have multiple tactic rows),
-- that is direct evidence tactic_evnt_id is a row/account-level id, not a
-- client key, and the notebook's join is comparing the wrong grain entirely.
-- ============================================================================
SELECT
    COUNT(*)                     AS total_rows,
    COUNT(DISTINCT tactic_evnt_id) AS distinct_tactic_evnt_id,
    COUNT(DISTINCT clnt_no)        AS distinct_clnt_no,
    COUNT(DISTINCT visa_acct_no)   AS distinct_visa_acct_no
FROM dg6v01.tactic_evnt_ip_ar_hist
WHERE substr(tactic_id, 8, 3) = 'CRV'
  AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31';


-- ============================================================================
-- BLOCK 2: JOIN-SIGNAL CHECK -- does success join on the REAL keys detect
-- anything at all? One summary row: n_clients / n_clients_with_success /
-- n_accts / n_accts_with_success. Each side is deduped to a distinct set of
-- keys with >=1 in-window success BEFORE counting (clnt_success / acct_success
-- CTEs), so the final COUNT(*) on those CTEs cannot double-count via fan-out.
-- If n_clients_with_success > 0 AND n_accts_with_success > 0, the EDW-side
-- join works cleanly on both keys and the notebook's TACTIC_EVNT_ID key is
-- the bug, not a defect in the underlying data or event table.
--
-- Trino risk flagged: the clnt_no join below (m.clnt_no = t.clnt_no) uses NO
-- cast, unlike the acct_no join. If measurement_events_v2.clnt_no is a
-- different underlying type than tactic_evnt_ip_ar_hist.clnt_no, Trino will
-- throw a strict-typing error here rather than silently returning zero rows --
-- if that happens, wrap both sides the same way the acct_no join does
-- (CAST(... AS DECIMAL(38,0)) or TRY_CAST/TRIM as needed) and note the type
-- mismatch as its own finding, since neither production CRV Trino file
-- (crv_vintage_v2_production.sql / crv_cohort_summary_v2_production.sql) ever
-- joins on clnt_no -- there is no existing precedent to confirm the type pair.
-- ============================================================================
WITH
tactic_pop AS (
    SELECT
        clnt_no,
        visa_acct_no,
        treatmt_strt_dt,
        treatmt_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'
),

-- clients with >=1 in-window success, joined on the REAL client key
clnt_success AS (
    SELECT DISTINCT t.clnt_no
    FROM tactic_pop t
    JOIN edl0_im.prod_zp10_prod_staging.measurement_events_v2 m
        ON m.clnt_no = t.clnt_no
    WHERE m.event_cd = 'p_card_installmt_purch'
      AND m.event_date BETWEEN t.treatmt_strt_dt AND t.treatmt_end_dt
),

-- accounts with >=1 in-window success, joined on the normalized numeric
-- account key -- same pattern as the validated crv_vintage_v2_production.sql:
-- tactic-side visa_acct_no vs. events-side zero-padded acct_no, both CAST to
-- DECIMAL(38,0) to strip the padding.
acct_success AS (
    SELECT DISTINCT t.visa_acct_no
    FROM tactic_pop t
    JOIN edl0_im.prod_zp10_prod_staging.measurement_events_v2 m
        ON CAST(t.visa_acct_no AS DECIMAL(38,0)) = CAST(m.acct_no AS DECIMAL(38,0))
    WHERE m.event_cd = 'p_card_installmt_purch'
      AND m.event_date BETWEEN t.treatmt_strt_dt AND t.treatmt_end_dt
)

SELECT
    (SELECT COUNT(DISTINCT clnt_no)      FROM tactic_pop)   AS n_clients,
    (SELECT COUNT(*)                     FROM clnt_success) AS n_clients_with_success,
    (SELECT COUNT(DISTINCT visa_acct_no) FROM tactic_pop)   AS n_accts,
    (SELECT COUNT(*)                     FROM acct_success) AS n_accts_with_success;
