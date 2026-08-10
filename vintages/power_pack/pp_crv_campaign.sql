-- crv_campaign_vintage.sql
-- Contract  : vintages/OUTPUT_CONTRACT.md (locked 2026-08-10, `deployment` DROPPED 2026-08-10).
--             7 columns only: cohort_month, segment, grp, vintage_day, base,
--             responders, responders_cum.
-- segment   : CAST('All' AS VARCHAR(20)) — constant. CRV has no pre-treatment population split
--             above Action/Control; segment carries real values only for AUH.
-- Campaign  : CRV (Credit Card Installment Plan) — CAMPAIGN scope
-- Engine    : Teradata-direct. SYS_CALENDAR spine in a VOLATILE TABLE + COLLECT STATISTICS
--             before the cross join (TDWM product-join guard). CTEs for everything else.
-- Source    : DL_MR_PROD.cards_crv_install_decis_resp (population/arm/window) +
--             DL_MR_PROD.cards_crv_install_details (RAW success — migrated off the curated
--             responder flag per Andre's direction, 2026-07-22)
-- Scope     : source table is already CRV-scoped — no additional population filter.
-- DEDUP IDENTIFIER: acct_no — this file's success join keys on acct_no throughout, so acct_no
--             is what first-touch dedup below is keyed on.
-- ----------------------------------------------------------------------------
-- *** deployment DROPPED, 2026-08-10 *** — was offer_start_date (CRV has no tactic_id). CRV
--   routinely runs 30+ offers inside one cohort month (58 in Q3) and Andre will not build a
--   curve per offer. Curve grain is now COHORT MONTH. Dropping deployment is CLEAN here: there
--   is no separate identity concept lost (offer_start_date was standing in for a wave id, not a
--   real key), but it does mean multiple offers to the SAME acct_no within one cohort_month must
--   now be deduped or base/responders double-count. See DEDUP below — this is the real work.
-- ----------------------------------------------------------------------------
-- *** [NOTE] grp tie-break = FIRST-TOUCH ***
--   If a client appeared twice in one cohort_month with opposing arms, grp comes from their
--   FIRST treatment. Per Andre (2026-08-10) this should never fire: a client already live in a
--   deployment is not re-decisioned until it ends (trigger-style decisioning). Kept as a cheap
--   guard for reminder-style sends inside a deployment. The diagnostic at the bottom of this file
--   confirms it — expect zeros.
-- ----------------------------------------------------------------------------
-- *** DEDUP — one row per (acct_no, cohort_month), anchored on first offer ***
--   1. pop_offer: one row per (acct_no, offer_start_date) — the original per-offer dedup
--      (multiple decis_resp rows for the same offer collapse to the longest-window one),
--      unchanged from the source file.
--   2. cohort_first: QUALIFY ROW_NUMBER() OVER (PARTITION BY acct_no, cohort_month
--      ORDER BY offer_start_date ASC) = 1 — first-touch offer wins grp and becomes Day 0
--      (anchor_dt = that offer's offer_start_date).
--   3. cohort_window: MAX(offer_end_date) across ALL of that acct_no's offers in the
--      cohort_month, so a later offer's window is never truncated by only looking at the
--      first-touch offer's own end date. Search window for success = [anchor_dt, window_end].
--   4. Success (raw plan-activation events from cards_crv_install_details) is pooled the same
--      way: gather every activation date from ANY of the client's offers that month, take the
--      EARLIEST absolute activation date, then rebase: vintage_day = earliest_activation_date -
--      anchor_dt (GREATEST(...,0) clamp preserved from source). A client who didn't convert on
--      offer 1 but did convert on offer 2 (same month) is no longer silently dropped, and a
--      client's base count is no longer inflated by having multiple offers pooled into the month.
-- ----------------------------------------------------------------------------
-- grp       : TRIM(action_control) IN ('Action','Control') — already CRV's native binary
--             language, no collapse needed.
-- Success   : UNCHANGED definition — RAW plan activation, first instl_txn_dt across the client's
--             offers in the cohort_month, via cards_crv_install_details. GREATEST(day,0) clamp
--             preserved. [VERIFY] install_type_ind filtering still unresolved — query runs over
--             ALL plan types (carried from crv_vintage_monthly.sql).
-- Spine     : data-driven per (cohort_month, grp) max window — MAX(window_end - anchor_dt) across
--             the pooled cohort, NOT capped at 90. CRV offer windows are short by campaign design
--             (~9 days per SME note in crv_vintage_v1_datalab.sql); a data-driven cap is the
--             deliberate, correct behavior here, not a bug.
-- Floor     : DATE '2026-01-01' on every scan (contract rule 6).
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_crv_cells;
--   DROP TABLE vt_crv_spine;

-- ============================================================================
-- STEP 1: cells — base (denominator) + cohort_max_day per (cohort_month, grp)
-- ============================================================================
CREATE VOLATILE TABLE vt_crv_cells AS (
    WITH pop_offer AS (
        SELECT
            acct_no, offer_start_date, offer_end_date,
            CAST(TRIM(action_control) AS VARCHAR(20)) AS grp
        FROM DL_MR_PROD.cards_crv_install_decis_resp
        WHERE offer_start_date >= DATE '2026-01-01'
          AND TRIM(action_control) IN ('Action', 'Control')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY acct_no, offer_start_date ORDER BY offer_end_date DESC
        ) = 1
    ),
    pop_raw AS (
        SELECT
            acct_no, offer_start_date, offer_end_date, grp,
            CAST(
                CAST(EXTRACT(YEAR FROM offer_start_date) AS VARCHAR(4)) || '-' ||
                CASE WHEN EXTRACT(MONTH FROM offer_start_date) < 10 THEN '0' ELSE '' END ||
                CAST(EXTRACT(MONTH FROM offer_start_date) AS VARCHAR(2))
            AS VARCHAR(7))                                AS cohort_month
        FROM pop_offer
    ),
    cohort_first AS (   -- [NOTE] first-touch: earliest offer wins grp + anchor date (never expected to fire — see header)
        SELECT acct_no, cohort_month, grp, offer_start_date AS anchor_dt
        FROM pop_raw
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY acct_no, cohort_month ORDER BY offer_start_date ASC
        ) = 1
    ),
    cohort_window AS (
        SELECT acct_no, cohort_month, MAX(offer_end_date) AS window_end
        FROM pop_raw
        GROUP BY acct_no, cohort_month
    )
    SELECT
        cf.cohort_month,
        cf.grp,
        COUNT(DISTINCT cf.acct_no)                       AS base,
        MAX(cw.window_end - cf.anchor_dt)                AS cohort_max_day
    FROM cohort_first cf
    INNER JOIN cohort_window cw
        ON cw.acct_no = cf.acct_no AND cw.cohort_month = cf.cohort_month
    GROUP BY 1, 2
) WITH DATA PRIMARY INDEX (cohort_month, grp) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_crv_cells COLUMN (cohort_month, grp);

-- ============================================================================
-- STEP 2: day spine, 0..GLOBAL_MAX (population-wide max pooled offer window)
-- ============================================================================
CREATE VOLATILE TABLE vt_crv_spine AS (
    SELECT (calendar_date - DATE '1900-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '1900-01-01')
          BETWEEN 0 AND (SELECT MAX(cohort_max_day) FROM vt_crv_cells)
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_crv_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
pop_offer AS (
    SELECT
        acct_no, offer_start_date, offer_end_date,
        CAST(TRIM(action_control) AS VARCHAR(20)) AS grp
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND TRIM(action_control) IN ('Action', 'Control')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY acct_no, offer_start_date ORDER BY offer_end_date DESC
    ) = 1
),
pop_raw AS (
    SELECT
        acct_no, offer_start_date, offer_end_date, grp,
        CAST(
            CAST(EXTRACT(YEAR FROM offer_start_date) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM offer_start_date) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM offer_start_date) AS VARCHAR(2))
        AS VARCHAR(7))                                AS cohort_month
    FROM pop_offer
),
cohort_first AS (   -- [NOTE] first-touch: earliest offer wins grp + anchor date (never expected to fire — see header)
    SELECT acct_no, cohort_month, grp, offer_start_date AS anchor_dt
    FROM pop_raw
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY acct_no, cohort_month ORDER BY offer_start_date ASC
    ) = 1
),
cohort_window AS (
    SELECT acct_no, cohort_month, MAX(offer_end_date) AS window_end
    FROM pop_raw
    GROUP BY acct_no, cohort_month
),
population AS (
    SELECT cf.acct_no, cf.cohort_month, cf.grp, cf.anchor_dt, cw.window_end
    FROM cohort_first cf
    INNER JOIN cohort_window cw
        ON cw.acct_no = cf.acct_no AND cw.cohort_month = cf.cohort_month
),

-- RAW success: first plan activation per (acct_no, offer_start_date) — pooled below across
-- every offer the client had this cohort_month
raw_conversions AS (
    SELECT
        d.acct_no,
        d.offer_start_date,
        -- [VERIFY] install_type_ind filtering unresolved — add
        --   AND d.install_type_ind IN (<confirmed values>)
        -- once confirmed
        MIN(d.instl_txn_dt) AS first_activation_dt
    FROM DL_MR_PROD.cards_crv_install_details d
    WHERE d.offer_start_date >= DATE '2026-01-01'
    GROUP BY d.acct_no, d.offer_start_date
),
raw_conversions_month AS (
    SELECT
        rc.acct_no,
        CAST(
            CAST(EXTRACT(YEAR FROM rc.offer_start_date) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM rc.offer_start_date) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM rc.offer_start_date) AS VARCHAR(2))
        AS VARCHAR(7))                                AS cohort_month,
        rc.first_activation_dt
    FROM raw_conversions rc
),
success_pooled AS (
    -- earliest activation across ALL of the client's offers in the cohort_month
    SELECT acct_no, cohort_month, MIN(first_activation_dt) AS first_activation_dt
    FROM raw_conversions_month
    GROUP BY acct_no, cohort_month
),

success AS (
    SELECT
        p.cohort_month,
        p.grp,
        p.acct_no,
        GREATEST(CAST(sp.first_activation_dt - p.anchor_dt AS INTEGER), 0) AS vintage_day
    FROM population p
    INNER JOIN success_pooled sp
        ON  sp.acct_no      = p.acct_no
        AND sp.cohort_month = p.cohort_month
),

daily_counts AS (
    SELECT cohort_month, grp, vintage_day, COUNT(*) AS responders
    FROM success
    GROUP BY cohort_month, grp, vintage_day
),

dense_grid AS (
    SELECT c.cohort_month, c.grp, c.base, s.vintage_day
    FROM vt_crv_cells c
    CROSS JOIN vt_crv_spine s
    WHERE s.vintage_day <= c.cohort_max_day
)

SELECT
    g.cohort_month,
    CAST('All' AS VARCHAR(20)) AS segment,
    g.grp,
    g.vintage_day,
    g.base,
    COALESCE(dc.responders, 0) AS responders,
    SUM(COALESCE(dc.responders, 0)) OVER (
        PARTITION BY g.cohort_month, g.grp
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_cum
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.grp           = g.grp
    AND dc.vintage_day    = g.vintage_day
ORDER BY g.cohort_month, g.grp, g.vintage_day;

DROP TABLE vt_crv_cells;
DROP TABLE vt_crv_spine;

-- ============================================================================
-- DIAGNOSTIC (commented out): how many accounts hit both arms in one month?
-- ============================================================================
-- SELECT cohort_month, COUNT(*) AS conflicted_accounts FROM (
--     SELECT acct_no, cohort_month FROM (
--         SELECT
--             acct_no,
--             CAST(
--                 CAST(EXTRACT(YEAR FROM offer_start_date) AS VARCHAR(4)) || '-' ||
--                 CASE WHEN EXTRACT(MONTH FROM offer_start_date) < 10 THEN '0' ELSE '' END ||
--                 CAST(EXTRACT(MONTH FROM offer_start_date) AS VARCHAR(2))
--             AS VARCHAR(7)) AS cohort_month,
--             CAST(TRIM(action_control) AS VARCHAR(20)) AS grp
--         FROM DL_MR_PROD.cards_crv_install_decis_resp
--         WHERE offer_start_date >= DATE '2026-01-01'
--           AND TRIM(action_control) IN ('Action', 'Control')
--     ) raw
--     GROUP BY acct_no, cohort_month
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x GROUP BY 1 ORDER BY 1;
