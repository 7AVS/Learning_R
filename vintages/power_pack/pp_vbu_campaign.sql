-- vbu_campaign_vintage.sql
-- Contract  : vintages/OUTPUT_CONTRACT.md (locked 2026-08-10). 7 columns only:
--             cohort_month, deployment, grp, vintage_day, base, responders, responders_cum.
-- Campaign  : VBU (Visa Benefit Upgrade) — CAMPAIGN scope
-- Engine    : Teradata-direct. SYS_CALENDAR spine in a VOLATILE TABLE + COLLECT STATISTICS
--             before the cross join (TDWM product-join guard). CTEs for everything else.
-- [VERIFY ENGINE] SCOT source (edl0_im.prod_yg80_pcbsharedzone.tsz_00222_...) is an EDL/Trino
--             catalog table. Whether it is reachable from a plain Teradata-direct session
--             (QueryGrid/foreign server) is UNCONFIRMED. If not reachable, this file must be
--             rewritten to run via Starburst federation (Trino syntax rules then apply).
-- Source    : DG6V01.tactic_evnt_ip_ar_hist (population) + p3c.appl_fact_dly (Casper) +
--             edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot (SCOT)
-- [FLAG] Success definition for VBU is Casper+SCOT application approval — the SAME definition
--             as VBA — per the 2026-07-22 methodology swap documented in
--             campaigns/VBA_VBU/vbu_summary_vintage_cell.py, REPLACING the earlier AIB/
--             dly_full_portfolio product-change definition ("Daniel Chin's original
--             framework", campaigns/VBA_VBU/vbu_vintage_original.sql — left in the repo,
--             untouched). This is a bigger swap than "add a second source" and was already
--             flagged to Andre in the prior file; carried forward here, not re-litigated.
-- Population: SUBSTR(tactic_id,8,3)='VBU', treatmt_strt_dt >= DATE '2026-01-01'. DELETED the
--             dead filter SUBSTR(tactic_id,8,1)<>'J' — position 8 is always 'V' for VBA/VBU,
--             so that filter could never exclude anything.
-- deployment: CAST(TRIM(tactic_id) AS VARCHAR(30)) — the wave, directly available.
-- grp       : LEFT(TRIM(tst_grp_cd),1): 'C'->Control, 'T'->Test. Anything else is EXCLUDED
--             from the population (not routed to a third bucket — contract requires grp
--             strictly binary). [VERIFY] the C/T-prefix split itself is not documented in the
--             original campaigns/VBA_VBU/vbu_vintage_original.sql (which uses tst_grp_cd
--             positions 6-8 as a FROM-product code, not an Action/Control split); confirmed
--             instead in the sibling harness campaigns/VBA_VBU/vbu_summary_vintage_cell.py
--             (`tc()`, ~line 64) as VBA/VBU's real Test/Control rollup rule.
-- Success   : UNCHANGED — Casper + SCOT unioned and deduped to one earliest-approval event.
--             Casper: Status='A', PROD_APPRVD IN ('B','E'), CR_LMT_CHG_IND='N', visa_prod_cd
--             NOT IN ('CCL','BXX'), Cell_Code NOT IN ('PATACT','GV0320'), event date =
--             app_rcv_dt. SCOT: productcategory='CREDIT_CARD', statuscode='FULFILLED', event
--             date = creditapplication_createddatetime (one row per client — SCOT's snapshot
--             structure supports only one signal ever per client). Union is a plain UNION
--             (not UNION ALL) on (clnt_no, event_date) to dedupe the same physical event
--             reported by both systems, done BEFORE deployment attribution. Same tables/
--             filters as vba_campaign_vintage.sql.
-- STRUCTURAL CHANGE from vbu_vintage_monthly.sql: the old file had no deployment output
--             column, so it collapsed multiple deployments per (client, cohort_month) to a
--             "first-in-bin" denominator and resolved numerator conflicts across overlapping
--             deployment windows via last-touch (most recent treatmt_strt_dt wins). The new
--             contract mandates deployment as an explicit dimension and forbids pooling across
--             deployments, so that cross-deployment resolution is GONE: each (clnt_no,
--             tactic_id) pop row is measured against its OWN window only. A client active in
--             two overlapping deployments can now show a responder in BOTH deployment curves
--             — expected under "curves never pool across deployments," not a bug, but
--             different from the old last-touch behavior. The Casper/SCOT union-dedup of the
--             physical event is preserved (guards against double-counting the same approval
--             from two systems); only the cross-deployment attribution layer was removed.
-- Spine     : fixed 0-90 — preserved as a deliberate cap (source used a hard 0-90 window, not
--             data-driven; not changed here).
-- Floor     : DATE '2026-01-01' on every scan (contract rule 6), including Casper/SCOT event
--             tables (source had no floor on those — added here; safe, since an event can only
--             match a window starting >= 2026-01-01 anyway, so this is a pushdown/perf fix,
--             not a behavior change).
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_vbu_cells;
--   DROP TABLE vt_vbu_spine;

-- ============================================================================
-- STEP 1: cells — base (denominator) per (cohort_month, deployment, grp)
-- ============================================================================
CREATE VOLATILE TABLE vt_vbu_cells AS (
    WITH pop AS (
        SELECT
            clnt_no, tactic_id, treatmt_strt_dt,
            CASE
                WHEN LEFT(TRIM(tst_grp_cd), 1) = 'C' THEN CAST('Control' AS VARCHAR(20))
                WHEN LEFT(TRIM(tst_grp_cd), 1) = 'T' THEN CAST('Test'    AS VARCHAR(20))
            END AS grp
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE treatmt_strt_dt >= DATE '2026-01-01'
          AND SUBSTR(tactic_id, 8, 3) = 'VBU'
          AND LEFT(TRIM(tst_grp_cd), 1) IN ('C', 'T')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY clnt_no, tactic_id ORDER BY treatmt_strt_dt ASC
        ) = 1
    )
    SELECT
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
        AS VARCHAR(7))                        AS cohort_month,
        CAST(TRIM(tactic_id) AS VARCHAR(30))   AS deployment,
        grp,
        COUNT(DISTINCT clnt_no)                AS base
    FROM pop
    GROUP BY 1, 2, 3
) WITH DATA PRIMARY INDEX (cohort_month, deployment, grp) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vbu_cells COLUMN (cohort_month, deployment, grp);

-- ============================================================================
-- STEP 2: day spine 0-90 (deliberate fixed cap, preserved from source)
-- ============================================================================
CREATE VOLATILE TABLE vt_vbu_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vbu_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
pop AS (
    SELECT
        clnt_no, tactic_id, treatmt_strt_dt, treatmt_end_dt,
        CASE
            WHEN LEFT(TRIM(tst_grp_cd), 1) = 'C' THEN CAST('Control' AS VARCHAR(20))
            WHEN LEFT(TRIM(tst_grp_cd), 1) = 'T' THEN CAST('Test'    AS VARCHAR(20))
        END AS grp
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE treatmt_strt_dt >= DATE '2026-01-01'
      AND SUBSTR(tactic_id, 8, 3) = 'VBU'
      AND LEFT(TRIM(tst_grp_cd), 1) IN ('C', 'T')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY clnt_no, tactic_id ORDER BY treatmt_strt_dt ASC
    ) = 1
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
-- BEFORE joining to a deployment window
events AS (
    SELECT clnt_no, event_date FROM casper_events
    UNION
    SELECT clnt_no, event_date FROM scot_events
),

-- each pop row is measured against its OWN window only — no cross-deployment attribution
success AS (
    SELECT
        CAST(
            CAST(EXTRACT(YEAR FROM p.treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM p.treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM p.treatmt_strt_dt) AS VARCHAR(2))
        AS VARCHAR(7))                        AS cohort_month,
        CAST(TRIM(p.tactic_id) AS VARCHAR(30)) AS deployment,
        p.grp,
        MIN(CAST(e.event_date - p.treatmt_strt_dt AS INTEGER)) AS vintage_day
    FROM pop p
    INNER JOIN events e
        ON  e.clnt_no    = p.clnt_no
        AND e.event_date BETWEEN p.treatmt_strt_dt AND p.treatmt_end_dt
    GROUP BY
        p.clnt_no,
        CAST(
            CAST(EXTRACT(YEAR FROM p.treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM p.treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM p.treatmt_strt_dt) AS VARCHAR(2))
        AS VARCHAR(7)),
        CAST(TRIM(p.tactic_id) AS VARCHAR(30)),
        p.grp
),

daily_counts AS (
    SELECT cohort_month, deployment, grp, vintage_day, COUNT(*) AS responders
    FROM success
    WHERE vintage_day BETWEEN 0 AND 90
    GROUP BY cohort_month, deployment, grp, vintage_day
),

dense_grid AS (
    SELECT c.cohort_month, c.deployment, c.grp, c.base, s.vintage_day
    FROM vt_vbu_cells c
    CROSS JOIN vt_vbu_spine s
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

DROP TABLE vt_vbu_cells;
DROP TABLE vt_vbu_spine;
