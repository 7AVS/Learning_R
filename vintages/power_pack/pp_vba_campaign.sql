-- vba_campaign_vintage.sql
-- Contract  : vintages/OUTPUT_CONTRACT.md (locked 2026-08-10, `deployment` DROPPED 2026-08-10).
--             7 columns only: cohort_month, segment, grp, vintage_day, base,
--             responders, responders_cum.
-- segment   : CAST('All' AS VARCHAR(20)) — constant. VBA has no pre-treatment population split
--             above Test/Control; segment carries real values only for AUH.
-- Campaign  : VBA (Visa Benefit Add) — CAMPAIGN scope
-- Engine    : Teradata-direct. SYS_CALENDAR spine in a VOLATILE TABLE + COLLECT STATISTICS
--             before the cross join (TDWM product-join guard). CTEs for everything else.
-- [VERIFY ENGINE] SCOT source (edl0_im.prod_yg80_pcbsharedzone.tsz_00222_...) is an EDL/Trino
--             catalog table. Whether it is reachable from a plain Teradata-direct session
--             (QueryGrid/foreign server) is UNCONFIRMED. If not reachable, this file must be
--             rewritten to run via Starburst federation (Trino syntax rules then apply).
-- Source    : DG6V01.tactic_evnt_ip_ar_hist (population) + p3c.appl_fact_dly (Casper) +
--             edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot (SCOT)
-- Population: SUBSTR(tactic_id,8,3)='VBA', treatmt_strt_dt >= DATE '2026-01-01'. DELETED the
--             dead filter SUBSTR(tactic_id,8,1)<>'J' — position 8 is always 'V' for VBA/VBU.
--
-- *** deployment DROPPED, 2026-08-10 *** — was tactic_id. Curve grain is now COHORT MONTH.
--
-- DEDUP IDENTIFIER: clnt_no — this file's success join keys on clnt_no throughout.
--
-- *** [NOTE] grp tie-break = FIRST-TOUCH ***
--   If a client appeared twice in one cohort_month with opposing arms, grp comes from their
--   FIRST treatment. Per Andre (2026-08-10) this should never fire: a client already live in a
--   deployment is not re-decisioned until it ends (trigger-style decisioning). Kept as a cheap
--   guard for reminder-style sends inside a deployment. The diagnostic at the bottom of this file
--   confirms it — expect zeros.
--
-- *** DEDUP — one row per (clnt_no, cohort_month), anchored on first wave ***
--   1. pop_wave: one row per (clnt_no, tactic_id) — the original per-wave dedup (multiple event
--      rows for the same client+wave collapse to the earliest treatmt_strt_dt), unchanged.
--   2. cohort_first: QUALIFY ROW_NUMBER() OVER (PARTITION BY clnt_no, cohort_month
--      ORDER BY treatmt_strt_dt ASC) = 1 — first-touch wave wins grp and becomes Day 0.
--   3. cohort_window: MAX(treatmt_end_dt) across ALL of that client's waves in the cohort_month,
--      so a later wave's window is never truncated by only looking at the first-touch wave's own
--      end date. Search window for success = [anchor_dt, window_end].
-- grp       : LEFT(TRIM(tst_grp_cd),1): 'C'->Control, 'T'->Test, from the client's first-touch
--             wave. Anything else is EXCLUDED from the population (not a third bucket — contract
--             requires grp strictly binary). [VERIFY] the C/T-prefix split itself is not
--             documented in the original campaigns/VBA_VBU/vba_vintage_curves.sql; confirmed
--             instead in the sibling harness campaigns/VBA_VBU/vba_summary_vintage_cell.py
--             (`tc()`, ~line 68) as VBA/VBU's real Test/Control rollup rule.
-- Success   : UNCHANGED definition — Casper + SCOT unioned and deduped to one earliest-approval
--             event. Casper: Status='A', PROD_APPRVD IN ('B','E'), CR_LMT_CHG_IND='N',
--             visa_prod_cd NOT IN ('CCL','BXX'), Cell_Code NOT IN ('PATACT','GV0320'), event date
--             = app_rcv_dt. SCOT: productcategory='CREDIT_CARD', statuscode='FULFILLED', event
--             date = creditapplication_createddatetime. Union is a plain UNION (not UNION ALL) on
--             (clnt_no, event_date) to dedupe the same physical event reported by both systems,
--             done BEFORE cohort attribution. Only the cross-wave pooling layer changed — see
--             DEDUP above; each client's cohort-month window now spans ALL their VBA waves that
--             month instead of measuring against a single wave's window.
-- Spine     : fixed 0-90 — preserved as a deliberate cap.
-- Floor     : DATE '2026-01-01' on every scan (contract rule 6), including Casper/SCOT event
--             tables.
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_vba_cells;
--   DROP TABLE vt_vba_spine;

-- ============================================================================
-- STEP 1: cells — base (denominator) per (cohort_month, grp)
-- ============================================================================
CREATE VOLATILE TABLE vt_vba_cells AS (
    WITH pop_wave AS (
        SELECT
            clnt_no, tactic_id, treatmt_strt_dt,
            CASE
                WHEN LEFT(TRIM(tst_grp_cd), 1) = 'C' THEN CAST('Control' AS VARCHAR(20))
                WHEN LEFT(TRIM(tst_grp_cd), 1) = 'T' THEN CAST('Test'    AS VARCHAR(20))
            END AS grp
        FROM DG6V01.tactic_evnt_ip_ar_hist
        WHERE treatmt_strt_dt >= DATE '2026-01-01'
          AND SUBSTR(tactic_id, 8, 3) = 'VBA'
          AND LEFT(TRIM(tst_grp_cd), 1) IN ('C', 'T')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY clnt_no, tactic_id ORDER BY treatmt_strt_dt ASC
        ) = 1
    ),
    pop_raw AS (
        SELECT
            clnt_no, treatmt_strt_dt, grp,
            CAST(
                CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
                CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
                CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
            AS VARCHAR(7))                        AS cohort_month
        FROM pop_wave
    ),
    cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire — see header)
        SELECT clnt_no, cohort_month, grp
        FROM pop_raw
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY clnt_no, cohort_month ORDER BY treatmt_strt_dt ASC
        ) = 1
    )
    SELECT cohort_month, grp, COUNT(DISTINCT clnt_no) AS base
    FROM cohort_first
    GROUP BY 1, 2
) WITH DATA PRIMARY INDEX (cohort_month, grp) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vba_cells COLUMN (cohort_month, grp);

-- ============================================================================
-- STEP 2: day spine 0-90 (deliberate fixed cap, preserved from source)
-- ============================================================================
CREATE VOLATILE TABLE vt_vba_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vba_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
pop_wave AS (
    SELECT
        clnt_no, tactic_id, treatmt_strt_dt, treatmt_end_dt,
        CASE
            WHEN LEFT(TRIM(tst_grp_cd), 1) = 'C' THEN CAST('Control' AS VARCHAR(20))
            WHEN LEFT(TRIM(tst_grp_cd), 1) = 'T' THEN CAST('Test'    AS VARCHAR(20))
        END AS grp
    FROM DG6V01.tactic_evnt_ip_ar_hist
    WHERE treatmt_strt_dt >= DATE '2026-01-01'
      AND SUBSTR(tactic_id, 8, 3) = 'VBA'
      AND LEFT(TRIM(tst_grp_cd), 1) IN ('C', 'T')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY clnt_no, tactic_id ORDER BY treatmt_strt_dt ASC
    ) = 1
),
pop_raw AS (
    SELECT
        clnt_no, treatmt_strt_dt, treatmt_end_dt, grp,
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
        AS VARCHAR(7))                        AS cohort_month
    FROM pop_wave
),
cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire — see header)
    SELECT clnt_no, cohort_month, grp, treatmt_strt_dt AS anchor_dt
    FROM pop_raw
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY clnt_no, cohort_month ORDER BY treatmt_strt_dt ASC
    ) = 1
),
cohort_window AS (
    -- widen search window to the latest end date across ALL the client's waves this month
    SELECT clnt_no, cohort_month, MAX(treatmt_end_dt) AS window_end
    FROM pop_raw
    GROUP BY clnt_no, cohort_month
),
population AS (
    SELECT cf.clnt_no, cf.cohort_month, cf.grp, cf.anchor_dt, cw.window_end
    FROM cohort_first cf
    INNER JOIN cohort_window cw
        ON cw.clnt_no = cf.clnt_no AND cw.cohort_month = cf.cohort_month
),

-- raw candidate success events, Casper (PRIMARY) — no deployment key on the event table itself
casper_events AS (
    SELECT DISTINCT app.bus_clnt_no AS clnt_no, app.app_rcv_dt AS event_date
    FROM p3c.appl_fact_dly app
    WHERE app.app_rcv_dt >= DATE '2026-01-01'
      AND app.Status IN ('A')
      AND app.PROD_APPRVD IN ('B', 'E')
      AND app.CR_LMT_CHG_IND = 'N'
      AND app.visa_prod_cd NOT IN ('CCL', 'BXX')
      AND (app.Cell_Code IS NULL OR app.Cell_Code NOT IN ('PATACT', 'GV0320'))
),

-- raw candidate success events, SCOT (SECONDARY) — one row per client (SCOT's snapshot
-- structure has no per-deployment key; MIN date + fulfilled flag is the most it supports)
scot_events_raw AS (
    SELECT
        CAST(creditapplication_borrowers_borrowersrfnumber AS INTEGER) AS clnt_no,
        MIN(CAST(creditapplication_createddatetime AS DATE))           AS event_date,
        MAX(CASE
            WHEN creditapplication_creditapplicationstatuscode IN ('FULFILLED') THEN 1 ELSE 0
        END)                                                           AS approved
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot
    WHERE creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory
          IN ('CREDIT_CARD')
      AND creditapplication_createddatetime >= DATE '2026-01-01'
    GROUP BY 1
),
scot_events AS (
    SELECT clnt_no, event_date FROM scot_events_raw WHERE approved = 1
),

-- union FIRST, dedupe the physical event (clnt_no, event_date) — plain UNION, not UNION ALL —
-- BEFORE joining to a cohort window
events AS (
    SELECT clnt_no, event_date FROM casper_events
    UNION
    SELECT clnt_no, event_date FROM scot_events
),

-- each client-month is measured against its OWN pooled window only (anchor_dt to window_end)
success AS (
    SELECT
        p.cohort_month,
        p.grp,
        p.clnt_no,
        MIN(CAST(e.event_date - p.anchor_dt AS INTEGER)) AS vintage_day
    FROM population p
    INNER JOIN events e
        ON  e.clnt_no    = p.clnt_no
        AND e.event_date BETWEEN p.anchor_dt AND p.window_end
    GROUP BY p.clnt_no, p.cohort_month, p.grp
),

daily_counts AS (
    SELECT cohort_month, grp, vintage_day, COUNT(*) AS responders
    FROM success
    WHERE vintage_day BETWEEN 0 AND 90
    GROUP BY cohort_month, grp, vintage_day
),

dense_grid AS (
    SELECT c.cohort_month, c.grp, c.base, s.vintage_day
    FROM vt_vba_cells c
    CROSS JOIN vt_vba_spine s
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

DROP TABLE vt_vba_cells;
DROP TABLE vt_vba_spine;

-- ============================================================================
-- DIAGNOSTIC (commented out): how many clients hit both arms in one month?
-- ============================================================================
-- SELECT cohort_month, COUNT(*) AS conflicted_clients FROM (
--     SELECT clnt_no, cohort_month FROM (
--         SELECT
--             clnt_no,
--             CAST(
--                 CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
--                 CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
--                 CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
--             AS VARCHAR(7)) AS cohort_month,
--             CASE
--                 WHEN LEFT(TRIM(tst_grp_cd), 1) = 'C' THEN CAST('Control' AS VARCHAR(20))
--                 WHEN LEFT(TRIM(tst_grp_cd), 1) = 'T' THEN CAST('Test'    AS VARCHAR(20))
--             END AS grp
--         FROM DG6V01.tactic_evnt_ip_ar_hist
--         WHERE treatmt_strt_dt >= DATE '2026-01-01'
--           AND SUBSTR(tactic_id, 8, 3) = 'VBA'
--           AND LEFT(TRIM(tst_grp_cd), 1) IN ('C', 'T')
--     ) raw
--     GROUP BY clnt_no, cohort_month
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x GROUP BY 1 ORDER BY 1;
