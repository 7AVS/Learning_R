-- pcd_experiment_vintage.sql
-- ============================================================================
-- CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10, `deployment` DROPPED 2026-08-10;
--   `mne` ADDED 2026-08-10 as first column).
--   Exactly 8 columns, this order: mne | cohort_month | segment | grp | vintage_day
--   | base | responders | responders_cum. Counts only. mne = CAST('PCD Async' AS VARCHAR(20)) —
--   campaign mnemonic, or the experiment name where the file measures an experiment. segment =
--   CAST('All' AS VARCHAR(20))
--   — constant; this async carve-out has no pre-treatment split above tst_grp_cd Test/Control.
--
-- mne distinguishes this file from its campaign sibling (pp_pcd_campaign.sql), so both
--   can be stacked into one cube safely.
-- SCOPE: **EXPERIMENT** — the async mobile banner carve-out inside PCD, NOT
--   the whole campaign. Companion file pcd_campaign_vintage.sql is the
--   campaign-scope curve.
--
-- *** THIS FILE FIXES A REAL BUG (carried forward from the 8-column version) ***
--   vintages/pcd_vintage_monthly.sql (the file this replaces) filtered
--   tactic_id_parent = '2026111PCD' and applied NO strategy_seg_cd ASYNC
--   filter anywhere. It therefore mixed ASYNC and NON_ASYNC recipients into
--   one curve, AND it only covered 1 of the 2 confirmed async waves. Fixed
--   here: both population conditions below are enforced together.
-- ----------------------------------------------------------------------------
-- Engine: Teradata-direct. SYS_CALENDAR spine + the population/base cells both
--   live in VOLATILE TABLEs with COLLECT STATISTICS before the cross join
--   (TDWM unconstrained-product-join guard). CTEs for everything else.
-- ----------------------------------------------------------------------------
-- SOURCES
--   dl_mr_prod.cards_pcd_ongoing_decis_resp   -- curated: population + success
--   DG6V01.TACTIC_EVNT_IP_AR_HIST              -- tst_grp_cd ONLY (see below)
-- ----------------------------------------------------------------------------
-- POPULATION FILTER — BOTH conditions required:
--   (a) tactic_id_parent IN ('2026111PCD', '2026125PCD')
--   (b) strategy_seg_cd  IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6',
--                             'MFB8UJPY','MFB9BX97','MFB9HYQ7')            -- ASYNC carve-out
-- ----------------------------------------------------------------------------
-- GRP — tst_grp_cd lives on the TACTIC EVENT table, NOT the curated table
--   (verified 2026-08-10 against schemas/pcd_curated_schemas.md). Joined
--   curated -> tactic on (tactic_id = tactic_id_parent, clnt_no = clnt_no).
--   grp derivation: TRIM(tst_grp_cd) LIKE '%C' -> 'Control', LIKE '%T' -> 'Test'.
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
--   Both async waves (2026111PCD, 2026125PCD) can in principle hit the same client in the same
--   cohort_month. cohort_first below picks the earliest response_start as Day 0 / grp; success
--   is pooled across BOTH waves the client touched that month (see success_pooled) so a
--   conversion attributed only to the second wave isn't silently dropped, and the client's base
--   count is not inflated by counting them once per wave.
-- ----------------------------------------------------------------------------
-- SUCCESS (ONE metric, per contract rule 4):
--   responder_targetproduct = 1, event date = dt_prod_change, anchor = the pooled cohort's
--   first-touch response_start — same validated primary success flag as the campaign file,
--   restricted to the same (a)+(b) async population filter. dt_prod_change is an absolute date
--   already on the curated row, so pooling across the client's waves this month is a plain
--   MIN(dt_prod_change) across those rows, then rebased to the first-touch anchor date.
-- ----------------------------------------------------------------------------
-- GRAIN: client (clnt_no). COUNT(DISTINCT clnt_no) throughout.
-- SPINE: vintage_day 0-60 (PCD canon window).
-- FLOOR: every scan >= DATE '2026-01-01' (contract rule 6).
-- [VERIFY]: none open. Both population conditions and the tst_grp_cd source
--   table are confirmed against the repo's validated async files and schema.
-- ----------------------------------------------------------------------------
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_pcd_experiment_cells;
--   DROP TABLE vt_pcd_experiment_spine;
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

-- ============================================================================
-- STEP 1: denominator cells (cohort_month x grp -> base)
-- ============================================================================
CREATE VOLATILE TABLE vt_pcd_experiment_cells AS (
    WITH wave_pop AS (
        SELECT
            clnt_no,
            tactic_id_parent                       AS deployment,
            MIN(response_start)                    AS response_start
        FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
        WHERE tactic_id_parent IN ('2026111PCD', '2026125PCD')
          AND response_start >= DATE '2026-01-01'                           -- floor guard
          AND response_end   >= DATE '2026-05-01'                           -- <<WINDOW>>
          AND response_end   <  DATE '2026-08-01'                           -- <<WINDOW>>
          AND strategy_seg_cd IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6',
                                   'MFB8UJPY','MFB9BX97','MFB9HYQ7')
        GROUP BY clnt_no, tactic_id_parent
    ),
    arm_lookup AS (
        SELECT DISTINCT
            tactic_id,
            clnt_no,
            CASE
                WHEN TRIM(tst_grp_cd) LIKE '%C' THEN CAST('Control' AS VARCHAR(20))
                WHEN TRIM(tst_grp_cd) LIKE '%T' THEN CAST('Test'    AS VARCHAR(20))
            END AS grp
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE tactic_id IN ('2026111PCD', '2026125PCD')
          AND treatmt_strt_dt >= DATE '2026-01-01'
    ),
    wave_arm AS (
        SELECT
            wp.clnt_no,
            wp.response_start,
            CAST(
                CAST(EXTRACT(YEAR FROM wp.response_start) AS VARCHAR(4)) || '-' ||
                CASE WHEN EXTRACT(MONTH FROM wp.response_start) < 10 THEN '0' ELSE '' END ||
                CAST(EXTRACT(MONTH FROM wp.response_start) AS VARCHAR(2))
            AS VARCHAR(7))                          AS cohort_month,
            al.grp
        FROM wave_pop wp
        INNER JOIN arm_lookup al
            ON al.tactic_id = wp.deployment AND al.clnt_no = wp.clnt_no
        WHERE al.grp IS NOT NULL
    ),
    cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire — see header)
        SELECT clnt_no, cohort_month, grp
        FROM wave_arm
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY clnt_no, cohort_month ORDER BY response_start ASC
        ) = 1
    )
    SELECT cohort_month, grp, COUNT(DISTINCT clnt_no) AS base
    FROM cohort_first
    GROUP BY cohort_month, grp
) WITH DATA PRIMARY INDEX (cohort_month, grp) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcd_experiment_cells COLUMN (cohort_month, grp);

-- ============================================================================
-- STEP 2: day spine 0-60
-- ============================================================================
CREATE VOLATILE TABLE vt_pcd_experiment_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 60
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcd_experiment_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
wave_pop AS (
    SELECT
        clnt_no,
        tactic_id_parent                       AS deployment,
        MIN(response_start)                    AS response_start
    FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent IN ('2026111PCD', '2026125PCD')
      AND response_start >= DATE '2026-01-01'                               -- floor guard
      AND response_end   >= DATE '2026-05-01'                               -- <<WINDOW>>
      AND response_end   <  DATE '2026-08-01'                               -- <<WINDOW>>
      AND strategy_seg_cd IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6',
                               'MFB8UJPY','MFB9BX97','MFB9HYQ7')
    GROUP BY clnt_no, tactic_id_parent
),

arm_lookup AS (
    SELECT DISTINCT
        tactic_id,
        clnt_no,
        CASE
            WHEN TRIM(tst_grp_cd) LIKE '%C' THEN CAST('Control' AS VARCHAR(20))
            WHEN TRIM(tst_grp_cd) LIKE '%T' THEN CAST('Test'    AS VARCHAR(20))
        END AS grp
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id IN ('2026111PCD', '2026125PCD')
      AND treatmt_strt_dt >= DATE '2026-01-01'
),

wave_arm AS (
    SELECT
        wp.clnt_no,
        wp.deployment,
        wp.response_start,
        CAST(
            CAST(EXTRACT(YEAR FROM wp.response_start) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM wp.response_start) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM wp.response_start) AS VARCHAR(2))
        AS VARCHAR(7))                          AS cohort_month,
        al.grp
    FROM wave_pop wp
    INNER JOIN arm_lookup al
        ON al.tactic_id = wp.deployment AND al.clnt_no = wp.clnt_no
    WHERE al.grp IS NOT NULL
),

cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire — see header)
    SELECT clnt_no, cohort_month, grp, response_start AS anchor_dt
    FROM wave_arm
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY clnt_no, cohort_month ORDER BY response_start ASC
    ) = 1
),

-- success: primary target-product responders only, restricted to the SAME (a)+(b) async filter.
-- dt_prod_change is already an absolute date, so no offset math needed at this grain.
success_events AS (
    SELECT
        clnt_no,
        tactic_id_parent AS deployment,
        dt_prod_change    AS success_dt_abs
    FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent IN ('2026111PCD', '2026125PCD')
      AND response_start >= DATE '2026-01-01'                               -- floor guard
      AND response_end   >= DATE '2026-05-01'                               -- <<WINDOW>> keeps success scan aligned to selected deployments
      AND response_end   <  DATE '2026-08-01'                               -- <<WINDOW>>
      AND strategy_seg_cd IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6',
                               'MFB8UJPY','MFB9BX97','MFB9HYQ7')
      AND responder_targetproduct = 1
      AND dt_prod_change IS NOT NULL
),

-- pool success across every wave the client touched this cohort_month — join via wave_arm
-- to attach cohort_month, then take the earliest absolute success date for the client-month
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
        CAST(sp.success_dt_abs - cf.anchor_dt AS INTEGER) AS vintage_day
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

dense_grid AS (
    SELECT c.cohort_month, c.grp, c.base, s.vintage_day
    FROM vt_pcd_experiment_cells c
    CROSS JOIN vt_pcd_experiment_spine s
)

SELECT
    -- VARCHAR(20) in EVERY file on purpose: in a Teradata UNION ALL the character
    -- length is fixed by the FIRST SELECT block, so stacking a 3-char 'PCD' block
    -- ahead of 'PCD Sales Modal' would silently truncate the longer labels.
    CAST('PCD Async' AS VARCHAR(20))   AS mne,
    g.cohort_month,
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

DROP TABLE vt_pcd_experiment_cells;
DROP TABLE vt_pcd_experiment_spine;

-- ============================================================================
-- DIAGNOSTIC (commented out): how many clients hit both arms in one month?
-- ============================================================================
-- SELECT cohort_month, COUNT(*) AS conflicted_clients FROM (
--     SELECT clnt_no, cohort_month FROM (
--         SELECT
--             wp.clnt_no,
--             CAST(
--                 CAST(EXTRACT(YEAR FROM wp.response_start) AS VARCHAR(4)) || '-' ||
--                 CASE WHEN EXTRACT(MONTH FROM wp.response_start) < 10 THEN '0' ELSE '' END ||
--                 CAST(EXTRACT(MONTH FROM wp.response_start) AS VARCHAR(2))
--             AS VARCHAR(7)) AS cohort_month,
--             al.grp
--         FROM (
--             SELECT clnt_no, tactic_id_parent AS deployment, MIN(response_start) AS response_start
--             FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
--             WHERE tactic_id_parent IN ('2026111PCD', '2026125PCD')
--               AND response_start >= DATE '2026-01-01'
--               AND strategy_seg_cd IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6',
--                                        'MFB8UJPY','MFB9BX97','MFB9HYQ7')
--             GROUP BY clnt_no, tactic_id_parent
--         ) wp
--         INNER JOIN (
--             SELECT DISTINCT tactic_id, clnt_no,
--                 CASE WHEN TRIM(tst_grp_cd) LIKE '%C' THEN CAST('Control' AS VARCHAR(20))
--                      WHEN TRIM(tst_grp_cd) LIKE '%T' THEN CAST('Test'    AS VARCHAR(20))
--                 END AS grp
--             FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
--             WHERE tactic_id IN ('2026111PCD', '2026125PCD') AND treatmt_strt_dt >= DATE '2026-01-01'
--         ) al ON al.tactic_id = wp.deployment AND al.clnt_no = wp.clnt_no
--         WHERE al.grp IS NOT NULL
--     ) raw
--     GROUP BY clnt_no, cohort_month
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x GROUP BY 1 ORDER BY 1;
