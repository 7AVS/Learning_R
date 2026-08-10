-- crv_campaign_vintage.sql
-- Contract  : vintages/OUTPUT_CONTRACT.md (locked 2026-08-10). 7 columns only:
--             cohort_month, deployment, grp, vintage_day, base, responders, responders_cum.
-- Campaign  : CRV (Credit Card Installment Plan) — CAMPAIGN scope
-- Engine    : Teradata-direct. SYS_CALENDAR spine in a VOLATILE TABLE + COLLECT STATISTICS
--             before the cross join (TDWM product-join guard). CTEs for everything else.
-- Source    : DL_MR_PROD.cards_crv_install_decis_resp (population/arm/window) +
--             DL_MR_PROD.cards_crv_install_details (RAW success — migrated off the curated
--             responder flag per Andre's direction, 2026-07-22)
-- Scope     : source table is already CRV-scoped — no additional population filter.
-- deployment: CRV has NO tactic_id / wave key in this source (confirmed — grepped repo,
--             none exists). offer_start_date IS the deployment key: every distinct
--             (acct_no, offer_start_date) row is one deployment membership. deployment =
--             CAST(offer_start_date AS VARCHAR(30)) 'YYYY-MM-DD'.
-- grp       : TRIM(action_control) IN ('Action','Control') — already CRV's native binary
--             language, no collapse needed.
-- Success   : UNCHANGED from source — RAW plan activation, first instl_txn_dt per
--             (acct_no, offer_start_date), via cards_crv_install_details. GREATEST(day,0)
--             clamp preserved. [VERIFY] install_type_ind filtering still unresolved — query
--             runs over ALL plan types (carried from crv_vintage_monthly.sql).
-- STRUCTURAL CHANGE from crv_vintage_monthly.sql: the old file pooled multiple deployments
--             within a cohort_month to a "first-in-bin" denominator + last-touch-style
--             "roll up under bin arm" numerator, because it had no deployment output column.
--             The new contract mandates deployment as an explicit dimension and forbids
--             pooling across deployments — so that dedup/roll-up machinery is GONE. Each
--             (acct_no, offer_start_date) pop row now IS its own deployment cell; success
--             joins directly on (acct_no, offer_start_date), no bin-arm reassignment. This
--             is a simplification, not a definition change — the success LOOKUP is identical.
-- Spine     : data-driven per (cohort_month, deployment, grp) max window — preserved from
--             source, NOT capped at 90. CRV offer windows are short by campaign design
--             (~9 days per SME note in crv_vintage_v1_datalab.sql); a data-driven cap is the
--             deliberate, correct behavior here, not a bug.
-- Floor     : DATE '2026-01-01' on every scan (contract rule 6) — was 2026-01-01 in source;
--             widened for compliance, harmless since CRV data does not exist before 2026.
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_crv_cells;
--   DROP TABLE vt_crv_spine;

-- ============================================================================
-- STEP 1: cells — base (denominator) + cohort_max_day per (cohort_month, deployment, grp)
-- ============================================================================
CREATE VOLATILE TABLE vt_crv_cells AS (
    WITH pop AS (
        SELECT
            acct_no, offer_start_date, offer_end_date,
            CAST(TRIM(action_control) AS VARCHAR(20)) AS grp
        FROM DL_MR_PROD.cards_crv_install_decis_resp
        WHERE offer_start_date >= DATE '2026-01-01'
          AND TRIM(action_control) IN ('Action', 'Control')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY acct_no, offer_start_date ORDER BY offer_end_date DESC
        ) = 1
    )
    SELECT
        CAST(
            CAST(EXTRACT(YEAR FROM offer_start_date) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM offer_start_date) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM offer_start_date) AS VARCHAR(2))
        AS VARCHAR(7))                                AS cohort_month,
        CAST(offer_start_date AS VARCHAR(30))          AS deployment,
        grp,
        COUNT(DISTINCT acct_no)                        AS base,
        MAX(offer_end_date - offer_start_date)          AS cohort_max_day
    FROM pop
    GROUP BY 1, 2, 3
) WITH DATA PRIMARY INDEX (cohort_month, deployment, grp) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_crv_cells COLUMN (cohort_month, deployment, grp);

-- ============================================================================
-- STEP 2: day spine, 0..GLOBAL_MAX (population-wide max offer window)
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
pop AS (
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

-- RAW success: first plan activation per (acct_no, offer_start_date) — the raw table already
-- keys to a specific offer, so no cross-deployment attribution is needed
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

success AS (
    SELECT
        CAST(
            CAST(EXTRACT(YEAR FROM p.offer_start_date) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM p.offer_start_date) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM p.offer_start_date) AS VARCHAR(2))
        AS VARCHAR(7))                                AS cohort_month,
        CAST(p.offer_start_date AS VARCHAR(30))        AS deployment,
        p.grp,
        GREATEST(rc.first_activation_day_raw, 0)       AS vintage_day
    FROM pop p
    INNER JOIN raw_conversions rc
        ON  rc.acct_no          = p.acct_no
        AND rc.offer_start_date = p.offer_start_date
),

daily_counts AS (
    SELECT cohort_month, deployment, grp, vintage_day, COUNT(*) AS responders
    FROM success
    GROUP BY cohort_month, deployment, grp, vintage_day
),

dense_grid AS (
    SELECT c.cohort_month, c.deployment, c.grp, c.base, s.vintage_day
    FROM vt_crv_cells c
    CROSS JOIN vt_crv_spine s
    WHERE s.vintage_day <= c.cohort_max_day
)

SELECT
    g.cohort_month,
    g.deployment,
    g.grp,
    g.vintage_day,
    g.base,
    COALESCE(dc.responders, 0) AS responders,
    SUM(COALESCE(dc.responders, 0)) OVER (
        PARTITION BY g.cohort_month, g.deployment, g.grp
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_cum
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.deployment    = g.deployment
    AND dc.grp           = g.grp
    AND dc.vintage_day    = g.vintage_day
ORDER BY g.cohort_month, g.deployment, g.grp, g.vintage_day;

DROP TABLE vt_crv_cells;
DROP TABLE vt_crv_spine;
