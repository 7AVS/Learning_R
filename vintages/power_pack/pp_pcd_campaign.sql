-- pcd_campaign_vintage.sql
-- ============================================================================
-- CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10, `deployment` DROPPED 2026-08-10;
--   `mne` ADDED 2026-08-10 as first column).
--   Exactly 8 columns, this order: mne | cohort_month | segment | grp | vintage_day
--   | base | responders | responders_cum. Counts only, no rates, no strategy/model/product
--   breakouts. mne = CAST('PCD' AS VARCHAR(20)) — campaign mnemonic, or the experiment name
--   where the file measures an experiment. segment =
--   CAST('All' AS VARCHAR(20)) — constant; PCD has no pre-treatment population split above
--   test_groups_period Action/Control. Real segment values exist only for AUH.
--
-- mne distinguishes this file from its experiment sibling (pp_pcd_async.sql), so both
--   can be stacked into one cube safely.
-- SCOPE: **CAMPAIGN** — the whole PCD (Product Card Upgrade) campaign, every
--   deployment/wave, past and future. NO async carve-out. This is the
--   "how did the whole campaign perform" curve, distinct from
--   pcd_experiment_vintage.sql (the async-banner-only experiment curve).
--
-- ENGINE: Trino / Starburst. ALL Power Pack files use ONE engine - EDW tables are reached
--         through Starburst federation. No volatile tables, no QUALIFY, no SYS_CALENDAR.
--
-- CATALOG PREFIX: dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp. Applying the SAME
--   DL_MR_PROD-needs-a-dw00-catalog-prefix rule proven for sibling curated tables in this exact
--   schema (dw00_im.dl_mr_prod.cards_pli_decision_resp, .cards_tpa_pcq_decision_resp,
--   .cards_crv_install_decis_resp, .nbo_vba_rbol_combined — all proven in genuinely Trino-tagged
--   files), with the dw00_jm alias CLAUDE.md itself pins for this specific table (CLAUDE.md line
--   56: "dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp"). [VERIFY CATALOG] — I could not find a
--   file whose OWN header declares Trino/Starburst AND that queries this exact table (the closest
--   hits — pcd_tactic_field_mapping.sql, pcd_async_banner_explore.sql — use this prefix but mix
--   Teradata-only blocks in the same file, one explicitly labelled "Run this in Teradata"). Flag
--   this line first if the query errors on catalog resolution.
-- ----------------------------------------------------------------------------
-- SOURCES
--   dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp   -- curated: population + success + grp (test_groups_period)
--   (DG6V01.TACTIC_EVNT_IP_AR_HIST join REMOVED 2026-08-10 — grp no longer needs it, see GRP below)
-- ----------------------------------------------------------------------------
-- POPULATION FILTER (pattern, not hardcoded IDs — new waves appear automatically):
--   SUBSTR(tactic_id_parent, 8, 3) = 'PCD'
--   (positions 8-10 of a tactic id are the campaign mnemonic — see CLAUDE.md
--   "TACTIC_ID structure". PCD ran 8+ deployments in Q3 2026 alone — this file
--   pools ALL of them into monthly cohorts, which is exactly why dedup matters here.)
-- ----------------------------------------------------------------------------
-- GRP — REDERIVED 2026-08-10 off test_groups_period, a column that lives on the curated
--   row itself (dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp), per Andre's validated working
--   file (PCD_async_vintage.sql, transcribed from screenshots 2026-08-10). This REMOVES the join
--   to DG6V01.TACTIC_EVNT_IP_AR_HIST that the prior version carried solely to pull tst_grp_cd
--   — one fewer join, one fewer table dependency, same first-touch semantics.
--   grp derivation: TRIM(test_groups_period) LIKE '%C' -> 'Control', LIKE '%T' -> 'Action'.
--   Rows matching neither pattern resolve to NULL and are excluded (see wave_arm below).
-- ----------------------------------------------------------------------------
-- DEDUP IDENTIFIER: clnt_no — this file's success/arm joins key on clnt_no throughout.
--
-- *** [NOTE] grp tie-break = FIRST-TOUCH ***
--   If a client appeared twice in one cohort_month with opposing arms, grp comes from their
--   FIRST treatment. Per Andre (2026-08-10) this should never fire: a client already live in a
--   deployment is not re-decisioned until it ends (trigger-style decisioning). Kept as a cheap
--   guard for reminder-style sends inside a deployment. The diagnostic at the bottom of this file
--   confirms it — expect zeros.
--
-- *** DEDUP — one row per (clnt_no, cohort_month), anchored on first wave ***
--   PCD ran 8+ deployments across Q3 2026 alone (see repo CLAUDE.md campaign table); a client
--   easily lands in 2+ waves inside one cohort_month. cohort_first below picks the earliest
--   response_start as Day 0 / grp; success is pooled across EVERY wave the client touched that
--   month (see success_pooled) so a conversion attributed only to a later wave isn't silently
--   dropped, and the client's base count is not inflated by counting them once per wave.
-- ----------------------------------------------------------------------------
-- SUCCESS (ONE metric, per contract rule 4):
--   responder_targetproduct = 1, event date = dt_prod_change, anchor = the pooled cohort's
--   first-touch response_start. This is the curated table's own pre-computed primary success
--   flag — the validated choice per async_banner_vintage_success.sql's header. dt_prod_change is
--   already an absolute date on the curated row, so pooling across the client's waves this month
--   is a plain MIN(dt_prod_change) across those rows, then rebased to the first-touch anchor date.
--   (responder_anyproduct / responder_upgrade_path are secondary metrics — dropped here per
--   contract rule 4, not folded into extra columns.)
-- ----------------------------------------------------------------------------
-- GRAIN: client (clnt_no). COUNT(DISTINCT clnt_no) throughout — never COUNT(*).
-- SPINE: vintage_day 0-60 (PCD canon window, unchanged from prior file). UNNEST(SEQUENCE(0,60)),
--   Trino has no SYS_CALENDAR.
-- FLOOR: every scan >= DATE '2026-01-01' (contract rule 6).
-- [VERIFY]: catalog prefix for cards_pcd_ongoing_decis_resp — see CATALOG PREFIX note above.
--   grp itself reads test_groups_period directly off the curated row — see GRP note above,
--   that part is unchanged/confirmed.
-- ----------------------------------------------------------------------------
-- SCOPE: this file is scoped to deployments ENDING in the quarter window below
--   (population filtered on response_end, confirmed column on the curated table,
--   schemas/pcd_curated_schemas.md #5). cohort_month and day-0 still anchor on
--   response_start (the START column) — unchanged. Retargeting a quarter = editing
--   the two <<WINDOW>> literals below only.
-- ============================================================================

-- ============================================================================
-- QUARTER WINDOW — EDIT THESE TWO DATES TO RETARGET THE PACK
--   Selects deployments whose END date (response_end) falls in the window.
--   Cohort month and day 0 still anchor on response_start (START), not these.
--     Q3 FY2026 = 2026-05-01 .. 2026-07-31
-- ============================================================================
-- WINDOW START : DATE '2026-05-01'
-- WINDOW END   : DATE '2026-07-31'   (inclusive; coded as < DATE '2026-08-01')

WITH
wave_pop AS (   -- one row per (clnt_no, wave), earliest response_start; grp read off that same row
    SELECT
        clnt_no, tactic_id_parent AS deployment, response_start, grp
    FROM (
        SELECT
            clnt_no,
            tactic_id_parent,
            response_start,
            CASE
                WHEN TRIM(test_groups_period) LIKE '%C' THEN CAST('Control' AS VARCHAR(20))
                WHEN TRIM(test_groups_period) LIKE '%T' THEN CAST('Action'  AS VARCHAR(20))
            END                                     AS grp,
            ROW_NUMBER() OVER (
                PARTITION BY clnt_no, tactic_id_parent ORDER BY response_start ASC
            ) AS rn
        FROM dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp
        WHERE SUBSTR(tactic_id_parent, 8, 3) = 'PCD'
          AND response_start >= DATE '2026-01-01'                           -- floor guard
          AND response_end   >= DATE '2026-05-01'                           -- <<WINDOW>>
          AND response_end   <  DATE '2026-08-01'                           -- <<WINDOW>>
    ) ranked
    WHERE rn = 1
),

wave_arm AS (
    SELECT
        wp.clnt_no,
        wp.deployment,
        wp.response_start,
        DATE_TRUNC('month', wp.response_start)  AS cohort_month,
        wp.grp
    FROM wave_pop wp
    WHERE wp.grp IS NOT NULL
),

cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire — see header)
    SELECT clnt_no, cohort_month, grp, response_start AS anchor_dt
    FROM (
        SELECT clnt_no, cohort_month, grp, response_start,
               ROW_NUMBER() OVER (
                   PARTITION BY clnt_no, cohort_month ORDER BY response_start ASC
               ) AS rn
        FROM wave_arm
    ) ranked
    WHERE rn = 1
),

pcd_cells AS (
    SELECT cohort_month, grp, COUNT(DISTINCT clnt_no) AS base
    FROM cohort_first
    GROUP BY cohort_month, grp
),

-- success: primary target-product responders only, restricted to same PCD pattern + floor.
-- dt_prod_change is already an absolute date, so no offset math needed at this grain.
success_events AS (
    SELECT
        clnt_no,
        tactic_id_parent AS deployment,
        dt_prod_change    AS success_dt_abs
    FROM dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE SUBSTR(tactic_id_parent, 8, 3) = 'PCD'
      AND response_start >= DATE '2026-01-01'                               -- floor guard
      AND response_end   >= DATE '2026-05-01'                               -- <<WINDOW>> keeps success scan aligned to selected deployments
      AND response_end   <  DATE '2026-08-01'                               -- <<WINDOW>>
      AND responder_targetproduct = 1
      AND dt_prod_change IS NOT NULL
),

-- pool success across every wave the client touched this cohort_month
success_pooled AS (
    SELECT wa.clnt_no, wa.cohort_month, MIN(se.success_dt_abs) AS success_dt_abs
    FROM success_events se
    INNER JOIN wave_arm wa
        ON wa.clnt_no = se.clnt_no AND wa.deployment = se.deployment
    GROUP BY wa.clnt_no, wa.cohort_month
),

numerator AS (
    SELECT
        cf.cohort_month, cf.grp, cf.clnt_no,
        DATE_DIFF('day', cf.anchor_dt, sp.success_dt_abs) AS vintage_day
    FROM cohort_first cf
    INNER JOIN success_pooled sp
        ON sp.clnt_no = cf.clnt_no AND sp.cohort_month = cf.cohort_month
),

daily_counts AS (
    SELECT cohort_month, grp, vintage_day, COUNT(DISTINCT clnt_no) AS responders
    FROM numerator
    WHERE vintage_day BETWEEN 0 AND 60
    GROUP BY cohort_month, grp, vintage_day
),

-- day spine 0-60
spine AS (
    SELECT s.vintage_day
    FROM UNNEST(SEQUENCE(0, 60)) AS s(vintage_day)
),

dense_grid AS (
    SELECT c.cohort_month, c.grp, c.base, s.vintage_day
    FROM pcd_cells c
    CROSS JOIN spine s
)

SELECT
    -- VARCHAR(20) in EVERY file on purpose: in a Teradata UNION ALL the character
    -- length is fixed by the FIRST SELECT block, so stacking a 3-char 'PCD' block
    -- ahead of 'PCD Sales Modal' would silently truncate the longer labels.
    CAST('PCD' AS VARCHAR(20))         AS mne,
    CAST(SUBSTR(CAST(g.cohort_month AS VARCHAR), 1, 7) AS VARCHAR(7)) AS cohort_month,
    CAST('All' AS VARCHAR(20))         AS segment,
    g.grp,
    g.vintage_day,
    g.base,
    CAST(COALESCE(dc.responders, 0) AS INTEGER) AS responders,
    CAST(SUM(COALESCE(dc.responders, 0)) OVER (
        PARTITION BY g.cohort_month, g.grp
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS INTEGER) AS responders_cum
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.grp           = g.grp
    AND dc.vintage_day   = g.vintage_day
ORDER BY g.cohort_month, g.grp, g.vintage_day;

-- ============================================================================
-- DIAGNOSTIC (commented out): how many clients hit both arms in one month?
-- ============================================================================
-- SELECT cohort_month, COUNT(*) AS conflicted_clients FROM (
--     SELECT clnt_no, cohort_month FROM (
--         SELECT
--             wp.clnt_no,
--             DATE_TRUNC('month', wp.response_start) AS cohort_month,
--             wp.grp
--         FROM (
--             SELECT clnt_no, tactic_id_parent AS deployment, response_start,
--                 CASE WHEN TRIM(test_groups_period) LIKE '%C' THEN CAST('Control' AS VARCHAR(20))
--                      WHEN TRIM(test_groups_period) LIKE '%T' THEN CAST('Action'  AS VARCHAR(20))
--                 END AS grp,
--                 ROW_NUMBER() OVER (PARTITION BY clnt_no, tactic_id_parent ORDER BY response_start ASC) AS rn
--             FROM dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp
--             WHERE SUBSTR(tactic_id_parent, 8, 3) = 'PCD'
--               AND response_start >= DATE '2026-01-01'
--         ) wp
--         WHERE wp.rn = 1 AND wp.grp IS NOT NULL
--     ) raw
--     GROUP BY clnt_no, cohort_month
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x GROUP BY 1 ORDER BY 1;
