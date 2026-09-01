-- ============================================================================
-- MEASUREMENT — VINTAGE CURVES (CRV + PCL)                      [FINAL FILE]
-- Rebuilt 2026-08-31 to the HOUSE VINTAGE FORMAT (vintages/OUTPUT_CONTRACT.md,
-- Power Pack canon) after the first version shipped a timing histogram — wrong
-- shape, right numbers. Contract:
--   mne | cohort_month 'YYYY-MM' | segment | grp | vintage_day 0..90
--   | base | responders (incremental) | responders_cum
--   * FULL day spine — every cell gets all 91 rows, zeros included
--   * base fixed down the spine (never summed over vintage_day)
--   * counts only; rates computed in the pivot
--   * negative-day responses CLAMPED to day 0 (never dropped) — Data Lab rule;
--     covers the PMCS pre-assignment gap (1 known account)
-- EXPERIMENT MAPPING: segment = 'Pass'/'Fail' (@132, pre-treatment split);
--   grp = 'Test' (TG4 within Pass, TG1 within Fail) / 'Control' (TG8).
--   Standard read: two lines per segment chart; Pass = the causal banner test.
-- SUCCESS DEFINITION (one, end to end): curated responder + first_response_date
--   (CRV) / responder_cli + dt_cl_change (PCL). Vintage day-90 cum MUST tie to
--   measurement_response_summary.sql per cell.
-- STMT 1 = CRV response vintage (runs weekly).
-- STMT 2 = PCL response vintage (first clean wave = September 2026).
-- Engine: TERADATA-DIRECT. Day spine = WITH RECURSIVE (seed needs a FROM —
-- err 8842). If TDWM blocks the small cells x spine product join
-- ("F-uncnstrm PJ"), fall back to VOLATILE TABLE + COLLECT STATS for spine
-- and cells (canon quirk #1).
-- ============================================================================

-- ---------------------------------------------------------------------------
-- STMT 1 — CRV response vintage (account grain, anchor = assignment date)
-- ---------------------------------------------------------------------------
WITH RECURSIVE spine (vintage_day) AS (
    SELECT 0 FROM (SELECT 1 AS x) seed
    UNION ALL
    SELECT vintage_day + 1 FROM spine WHERE vintage_day < 90
),
expt AS (
    SELECT visa_acct_no, tactic_id,
           CASE WHEN substr(tactic_decisn_vrb_info, 132, 1) = 'Y'
                THEN 'Pass' ELSE 'Fail' END        AS seg,
           CASE WHEN tst_grp_cd = 'TG8'
                THEN 'Control' ELSE 'Test' END     AS grp,
           treatmt_strt_dt                         AS assign_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2026-08-14'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY visa_acct_no
                               ORDER BY treatmt_strt_dt, tactic_id) = 1
),
offers AS (
    SELECT e.seg, e.grp, e.assign_dt,
           CAST(EXTRACT(YEAR FROM e.assign_dt) AS VARCHAR(4)) || '-' ||
           SUBSTR('0' || TRIM(EXTRACT(MONTH FROM e.assign_dt)), -2) AS cohort_month,
           c.responder, c.first_response_date
    FROM expt e
    JOIN dl_mr_prod.cards_crv_install_decis_resp c
      ON c.acct_no   = e.visa_acct_no
     AND c.tactic_id = e.tactic_id                  -- deployment-exact
     AND c.offer_start_date >= DATE '2026-08-01'    -- loose pushdown only
),
cells AS (
    SELECT cohort_month, seg, grp, COUNT(*) AS base
    FROM offers
    GROUP BY 1, 2, 3
),
daily AS (
    SELECT cohort_month, seg, grp,
           CASE WHEN first_response_date < assign_dt THEN 0
                ELSE first_response_date - assign_dt END AS vintage_day,  -- clamp, never drop
           COUNT(*) AS responders
    FROM offers
    WHERE responder = 1
      AND first_response_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
)
SELECT
    CAST('CRV_EXPT' AS VARCHAR(20))            AS mne,
    g.cohort_month,
    g.seg                                      AS segment,
    g.grp,
    s.vintage_day,
    g.base,                                        -- fixed down the spine
    COALESCE(d.responders, 0)                  AS responders,
    SUM(COALESCE(d.responders, 0)) OVER (
        PARTITION BY g.cohort_month, g.seg, g.grp
        ORDER BY s.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                          AS responders_cum
FROM cells g
CROSS JOIN spine s
LEFT JOIN daily d
  ON d.cohort_month = g.cohort_month
 AND d.seg          = g.seg
 AND d.grp          = g.grp
 AND d.vintage_day  = s.vintage_day
ORDER BY g.cohort_month, g.seg, g.grp, s.vintage_day;

-- ---------------------------------------------------------------------------
-- STMT 2 — PCL response vintage (lead grain, anchor = PCL lead start;
-- cohort = PCL wave month; zero rows until a post-08-14 wave loads)
-- ---------------------------------------------------------------------------
WITH RECURSIVE spine (vintage_day) AS (
    SELECT 0 FROM (SELECT 1 AS x) seed
    UNION ALL
    SELECT vintage_day + 1 FROM spine WHERE vintage_day < 90
),
expt AS (
    SELECT visa_acct_no,
           CASE WHEN substr(tactic_decisn_vrb_info, 132, 1) = 'Y'
                THEN 'Pass' ELSE 'Fail' END        AS seg,
           CASE WHEN tst_grp_cd = 'TG8'
                THEN 'Control' ELSE 'Test' END     AS grp,
           treatmt_strt_dt                         AS assign_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2026-08-14'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY visa_acct_no
                               ORDER BY treatmt_strt_dt, tactic_id) = 1
),
leads AS (
    SELECT e.seg, e.grp,
           CAST(EXTRACT(YEAR FROM p.treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
           SUBSTR('0' || TRIM(EXTRACT(MONTH FROM p.treatmt_strt_dt)), -2) AS cohort_month,
           p.treatmt_strt_dt, p.responder_cli, p.dt_cl_change
    FROM expt e
    JOIN dl_mr_prod.cards_pli_decision_resp p
      ON p.acct_no = e.visa_acct_no
     AND p.treatmt_strt_dt >= DATE '2026-08-14'
    WHERE p.treatmt_strt_dt >= e.assign_dt
),
cells AS (
    SELECT cohort_month, seg, grp, COUNT(*) AS base     -- base = leads
    FROM leads
    GROUP BY 1, 2, 3
),
daily AS (
    SELECT cohort_month, seg, grp,
           CASE WHEN dt_cl_change < treatmt_strt_dt THEN 0
                ELSE dt_cl_change - treatmt_strt_dt END AS vintage_day,
           COUNT(*) AS responders
    FROM leads
    WHERE responder_cli = 1
      AND dt_cl_change IS NOT NULL
    GROUP BY 1, 2, 3, 4
)
SELECT
    CAST('CRV_EXPT_PCL' AS VARCHAR(20))        AS mne,
    g.cohort_month,
    g.seg                                      AS segment,
    g.grp,
    s.vintage_day,
    g.base,
    COALESCE(d.responders, 0)                  AS responders,
    SUM(COALESCE(d.responders, 0)) OVER (
        PARTITION BY g.cohort_month, g.seg, g.grp
        ORDER BY s.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                          AS responders_cum
FROM cells g
CROSS JOIN spine s
LEFT JOIN daily d
  ON d.cohort_month = g.cohort_month
 AND d.seg          = g.seg
 AND d.grp          = g.grp
 AND d.vintage_day  = s.vintage_day
ORDER BY g.cohort_month, g.seg, g.grp, s.vintage_day;
