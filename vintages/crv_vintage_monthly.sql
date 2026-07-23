-- crv_vintage_monthly.sql
-- Campaign : CRV (Credit Card Installment Plan)
-- Source   : DL_MR_PROD.cards_crv_install_decis_resp (population/arm) +
--            DL_MR_PROD.cards_crv_install_details (RAW success — migrated off the curated
--            responder flag per Andre's direction, 2026-07-22)
-- Engine   : Teradata-direct (SYS_CALENDAR spine, volatile tables for TDWM cross-join clearance)
-- Success  : RAW plan activation — first instl_txn_dt per (acct_no, offer_start_date). The raw
--            table already keys to a specific offer, so no cross-deployment attribution needed.
-- [VERIFY] install_type_ind filtering unresolved — query runs over ALL plan types (TODO carried
--          over from vintages/crv_vintage_monthly_raw.sql, now absorbed into this file).
-- Anchor   : offer_start_date (treatment start), per deployment
-- Grain    : account (acct_no); no acct->clnt bridge (canon rule — stay at curated table's grain)
-- Arm      : action_control — raw values ARE 'Action'/'Control' already (arm_raw = arm)
-- Cohort bin: calendar month 'YYYYMM' of a deployment's own offer_start_date
-- Day window: 0..cohort_max_day (dynamic per (bin, arm) cell). cohort_max_day =
--          MAX(offer_end_date - offer_start_date), grouped by (cohort, arm_raw) — RE-VERIFIED
--          2026-07-22 against campaigns/CRV/vintage_reconciliation/crv_vintage_v1_datalab.sql's
--          own cells CTE: "GROUP BY cohort_month, arm" with "MAX(offer_end_date -
--          offer_start_date) AS cohort_max_day" (same file, same expression) — FIXED here to
--          group by (cohort, arm_raw) instead of cohort alone (prior version pooled Action+
--          Control together, which could pick up a longer window from the other arm). Computed
--          over ALL deployments in that (bin,arm) cell, not just the denominator's first-in-bin
--          ones — a later in-bin deployment can still contribute a numerator row and needs its
--          own window covered by the spine.
--          VERDICT (reviewer's ~9-day reading): REAL, NOT A BUG. CRV offer windows are
--          genuinely short by campaign design — the canon file's own comments say so directly:
--          "a short window, ~9 days" and "The window is small (~9 days), so this spine is tiny"
--          (crv_vintage_v1_datalab.sql, STEP 1/2 comments). A single-digit-to-low-double-digit
--          cohort_max_day (e.g. ~9, occasionally higher if a cohort_month blends >1 product wave
--          with a longer window) is the EXPECTED value, not a truncation defect.
-- Denominator: one row per (acct_no, bin) = the account's FIRST in-bin deployment (MIN
--          offer_start_date within the bin). Arm = that deployment's arm; if a client's in-bin
--          deployments carry conflicting arms, first-anchor wins. cohort_size = COUNT(DISTINCT
--          acct_no) on this deduped set.
-- Numerator: NOT deduped to the first-in-bin deployment. Every deployment in the population
--          gets its own success lookup against its own offer_start_date; at most one success per
--          deployment. Each success rolls up under the client's BIN arm (the denominator's
--          first-in-bin arm for that (acct_no,bin)), not necessarily this deployment's own arm.
--          cum_responses = cumulative SUCCESS EVENTS (one per deployment window), NOT distinct
--          clients.
-- RECONCILIATION: raw-source numbers here must be checked against the curated-responder canon
--          (campaigns/CRV/vintage_reconciliation/crv_vintage_v1_datalab.sql) before first use —
--          this is the datalab -> events-source migration check (see vintages/README.md).
-- Sourced from: vintages/crv_vintage_monthly_raw.sql (absorbed, deleted) for the raw success
--          join; campaigns/CRV/vintage_reconciliation/crv_vintage_v1_datalab.sql for the
--          GREATEST clamp / account-grain / per-cohort day-cap mechanics.
-- Monthly/quarterly: same structural template, differ ONLY in the cohort bin expression (STEP 1
--          and STEP 3 below) — the dedup / last-touch logic is otherwise identical.
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_crv_monthly_cells;
--   DROP TABLE vt_crv_monthly_spine;

-- ============================================================================
-- STEP 1: denominator cells — cohort_size (first-in-bin dedup) + cohort_max_day (all deployments)
-- ============================================================================
CREATE VOLATILE TABLE vt_crv_monthly_cells AS (
    WITH bin_arm_lookup AS (
        SELECT
            acct_no,
            CAST(
                CAST(EXTRACT(YEAR FROM offer_start_date) AS VARCHAR(4)) ||
                CASE WHEN EXTRACT(MONTH FROM offer_start_date) < 10 THEN '0' ELSE '' END ||
                CAST(EXTRACT(MONTH FROM offer_start_date) AS VARCHAR(2))
            AS VARCHAR(10))                             AS cohort,
            CAST(TRIM(action_control) AS VARCHAR(30))    AS arm_raw,
            CAST(TRIM(action_control) AS VARCHAR(30))    AS arm
        FROM DL_MR_PROD.cards_crv_install_decis_resp
        WHERE offer_start_date >= DATE '2026-01-01'
          AND TRIM(action_control) IN ('Action', 'Control')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY acct_no, cohort
            ORDER BY offer_start_date ASC
        ) = 1
    ),
    bin_max_day AS (
        -- matches crv_vintage_v1_datalab.sql's own cells CTE EXACTLY: GROUP BY cohort_month, arm
        -- (NOT cohort alone) — cohort_max_day is that CELL's own offer-window length, action and
        -- control kept separate, not pooled
        SELECT
            CAST(
                CAST(EXTRACT(YEAR FROM offer_start_date) AS VARCHAR(4)) ||
                CASE WHEN EXTRACT(MONTH FROM offer_start_date) < 10 THEN '0' ELSE '' END ||
                CAST(EXTRACT(MONTH FROM offer_start_date) AS VARCHAR(2))
            AS VARCHAR(10))                              AS cohort,
            CAST(TRIM(action_control) AS VARCHAR(30))     AS arm_raw,
            MAX(offer_end_date - offer_start_date)        AS cohort_max_day
        FROM DL_MR_PROD.cards_crv_install_decis_resp
        WHERE offer_start_date >= DATE '2026-01-01'
          AND TRIM(action_control) IN ('Action', 'Control')
        GROUP BY 1, 2
    )
    SELECT
        l.cohort, l.arm_raw, l.arm,
        COUNT(DISTINCT l.acct_no) AS cohort_size,
        MAX(m.cohort_max_day)     AS cohort_max_day
    FROM bin_arm_lookup l
    JOIN bin_max_day m ON m.cohort = l.cohort AND m.arm_raw = l.arm_raw
    GROUP BY l.cohort, l.arm_raw, l.arm
) WITH DATA PRIMARY INDEX (cohort, arm) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_crv_monthly_cells COLUMN (cohort, arm);

-- ============================================================================
-- STEP 2: day spine, 0..GLOBAL_MAX (population-wide max offer window)
-- ============================================================================
CREATE VOLATILE TABLE vt_crv_monthly_spine AS (
    SELECT (calendar_date - DATE '1900-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '1900-01-01')
          BETWEEN 0 AND (SELECT MAX(cohort_max_day) FROM vt_crv_monthly_cells)
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_crv_monthly_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
bin_arm_lookup AS (
    SELECT
        acct_no,
        CAST(
            CAST(EXTRACT(YEAR FROM offer_start_date) AS VARCHAR(4)) ||
            CASE WHEN EXTRACT(MONTH FROM offer_start_date) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM offer_start_date) AS VARCHAR(2))
        AS VARCHAR(10))                             AS cohort,
        CAST(TRIM(action_control) AS VARCHAR(30))    AS arm_raw,
        CAST(TRIM(action_control) AS VARCHAR(30))    AS arm
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND TRIM(action_control) IN ('Action', 'Control')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY acct_no, cohort
        ORDER BY offer_start_date ASC
    ) = 1
),

-- every deployment (NOT deduped) — each gets its own success lookup
all_deployments AS (
    SELECT
        acct_no,
        offer_start_date,
        CAST(
            CAST(EXTRACT(YEAR FROM offer_start_date) AS VARCHAR(4)) ||
            CASE WHEN EXTRACT(MONTH FROM offer_start_date) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM offer_start_date) AS VARCHAR(2))
        AS VARCHAR(10))                              AS cohort
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND TRIM(action_control) IN ('Action', 'Control')
),

-- RAW success: first plan activation per (acct_no, offer_start_date) — the raw table already
-- keys to a specific offer, so no last-touch attribution is needed across deployments
raw_conversions AS (
    SELECT
        d.acct_no,
        d.offer_start_date,
        -- [VERIFY] install_type_ind filtering unresolved — add
        --   AND d.install_type_ind IN (<confirmed values>)
        -- once confirmed
        MIN(CAST(d.instl_txn_dt - d.offer_start_date AS INTEGER)) AS first_activation_day_raw
    FROM DL_MR_PROD.cards_crv_install_details d
    WHERE d.offer_start_date >= DATE '2026-01-01'
    GROUP BY d.acct_no, d.offer_start_date
),

deployment_success AS (
    SELECT
        ad.acct_no, ad.cohort,
        GREATEST(rc.first_activation_day_raw, 0) AS vintage_day
    FROM all_deployments ad
    INNER JOIN raw_conversions rc
        ON  rc.acct_no          = ad.acct_no
        AND rc.offer_start_date = ad.offer_start_date
),

-- roll up under the client's BIN arm (first-in-bin deployment), not this deployment's own arm
numerator_binned AS (
    SELECT bl.cohort, bl.arm_raw, bl.arm, ds.vintage_day
    FROM deployment_success ds
    INNER JOIN bin_arm_lookup bl
        ON bl.acct_no = ds.acct_no AND bl.cohort = ds.cohort
),

daily_counts AS (
    SELECT cohort, arm_raw, arm, vintage_day, COUNT(*) AS n_events
    FROM numerator_binned
    GROUP BY cohort, arm_raw, arm, vintage_day
),

dense_grid AS (
    SELECT c.cohort, c.arm_raw, c.arm, c.cohort_size, s.vintage_day
    FROM vt_crv_monthly_cells c
    CROSS JOIN vt_crv_monthly_spine s
    WHERE s.vintage_day <= c.cohort_max_day
)

SELECT
    CAST('CRV' AS VARCHAR(10)) AS campaign,
    g.cohort,
    g.arm_raw,
    g.arm,
    g.vintage_day,
    g.cohort_size,
    SUM(COALESCE(dc.n_events, 0)) OVER (
        PARTITION BY g.cohort, g.arm_raw, g.arm
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_responses
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort      = g.cohort
    AND dc.arm_raw     = g.arm_raw
    AND dc.arm         = g.arm
    AND dc.vintage_day = g.vintage_day
ORDER BY g.cohort, g.arm, g.vintage_day;

DROP TABLE vt_crv_monthly_cells;
DROP TABLE vt_crv_monthly_spine;
