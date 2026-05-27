-- CTU success validation
-- Validates the chequing-account-switch conversion logic against the CTU cohort
-- (tactic_id = '2026098CTU'). Aligned with the production query at
-- zp10-nba-measurement-data/src/sql/ctu_success.sql.
--
-- Currently emits: secondary_success (switch destination is an upgrade per the hardcoded
-- product ladder). Aggregation uses COUNT(DISTINCT clnt_no) so multi-AR clients are not
-- overcounted. No control arm for CTU — gross response only, no lift.
--
-- ---------------------------------------------------------------------------
-- TODO — primary_success placeholder (suppressed until target_product is sourced)
-- ---------------------------------------------------------------------------
-- The production query has DROPPED primary_success (switch-to-the-specific-targeted-
-- product). Likely because target_product was never reliably wired into any upstream
-- table — it would live in the tactic event table or a campaign-design lookup, and
-- nobody pinned that down. When the source is identified (probably a position in
-- tactic_decisn_vrb_info or a separate lookup), re-enable in three steps:
--   1. Add current_product / target_product columns to the `cohort` CTE.
--   2. Add a primary_success CASE to the `success` CTE:
--        CASE WHEN target_product IS NOT NULL AND target_product = latest_to_product
--             THEN 1 ELSE 0 END AS primary_success
--   3. Add primary_responders to the seg_counts:
--        COUNT(DISTINCT CASE WHEN primary_success = 1 THEN clnt_no END) AS primary_responders
-- ---------------------------------------------------------------------------
-- Engine: Starburst (Trino) over EDW (DG6V01 / DDWV01 federated).

WITH
cohort AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        treatmt_end_dt
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id = '2026098CTU'
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND SUBSTRING(tactic_decisn_vrb_info, 121, 30) LIKE '%MB%'
),

-- Distinct snap_dts and overall window — explicit IN-list / BETWEEN predicates
-- below let Teradata partition-prune the daily-snapshot tables instead of
-- scanning the full archive against per-row join conditions.
cohort_snap_dts AS (
    SELECT DISTINCT (treatmt_strt_dt - 1) AS snap_dt FROM cohort
),
cohort_window AS (
    SELECT MIN(treatmt_strt_dt) AS min_dt,
           MAX(treatmt_end_dt)  AS max_dt
    FROM cohort
),

-- Most recent applicable pba_acct_lkup snap_dt across the cohort's date span.
-- Precomputed once to avoid a correlated subquery on the lookup join.
pba_max_snap AS (
    SELECT MAX(snap_dt) AS snap_dt
    FROM ddwv01.pba_acct_lkup
    WHERE pda_typ_cd = 'C'
      AND snap_dt BETWEEN (SELECT MIN(treatmt_strt_dt) FROM cohort)
                      AND (SELECT MAX(treatmt_end_dt)  FROM cohort)
),

-- Pre-filter pba_acct_lkup to the current snap_dt + consumer rows.
-- Materialized once and joined twice (from/to) in switches_raw — cheaper than
-- scanning the full lookup against two different key sets.
pba_lkup_curr AS (
    SELECT acct_typ_cd, acct_clss_cd, srvc_fee_opt_cd, prod_en_nm
    FROM ddwv01.pba_acct_lkup
    WHERE pda_typ_cd = 'C'
      AND snap_dt    = (SELECT snap_dt FROM pba_max_snap)
),

-- Pre-campaign chequing product per (clnt, ar), snapshotted one day before treatment start.
-- Product label derived from acct_typ + acct_cls + flt_pr_tm_trnsctn (chequing tiers).
-- Also doubles as the (clnt_no, ar_id) source for switches_raw so we avoid a
-- second range-scan of clnt_ar_reltn_dly across the treatment window.
precamp_product AS (
    SELECT
        c.clnt_no,
        c.treatmt_strt_dt,
        c.treatmt_end_dt,
        b.ar_id,
        b.prmry_clnt_ind,
        CASE
            WHEN s.acct_typ = 13 AND s.acct_cls = 10    AND d.flt_pr_tm_trnsctn = 3 THEN 'RBC Student Banking'
            WHEN s.acct_typ = 13 AND s.acct_cls = 10    AND d.flt_pr_tm_trnsctn = 4 THEN 'RBC No Limit Banking for Students'
            WHEN s.acct_typ = 13 AND s.acct_cls = 0     AND d.flt_pr_tm_trnsctn = 2 THEN 'RBC Day to Day Banking'
            WHEN s.acct_typ = 13 AND s.acct_cls = 0     AND d.flt_pr_tm_trnsctn = 4 THEN 'RBC No Limit Banking'
            WHEN s.acct_typ = 13 AND s.acct_cls IN (8,9) AND d.flt_pr_tm_trnsctn = 0 THEN 'RBC Signature No Limit Banking'
        END AS from_product
    FROM cohort c
    INNER JOIN ddwv01.clnt_ar_reltn_dly b
        ON  b.clnt_no    = c.clnt_no
        AND b.dw_srvc_id = 1
        AND b.snap_dt    = c.treatmt_strt_dt - 1
    INNER JOIN ddwv01.ar_static_dly s
        ON  s.ar_id          = b.ar_id
        AND s.snap_dt        = b.snap_dt
        AND s.srvc_id        = 1
        AND s.open_cls_sts   = 'O'
        AND s.acct_typ       = 13
        AND s.acct_cls IN (0,8,9,10)
    INNER JOIN ddwv01.deposit_account_dly d
        ON  d.ar_id      = b.ar_id
        AND d.snap_dt    = b.snap_dt
        AND d.dw_srvc_id = 1
    WHERE b.snap_dt IN (SELECT snap_dt FROM cohort_snap_dts)
      AND s.snap_dt IN (SELECT snap_dt FROM cohort_snap_dts)
      AND d.snap_dt IN (SELECT snap_dt FROM cohort_snap_dts)
),

-- Account switch events during the treatment window, with product names looked up.
-- ar_id mapping comes from precamp_product (already at treatmt_strt_dt - 1) — avoids
-- the date-range scan on clnt_ar_reltn_dly that was blowing spool.
-- Most-recent switch per (clnt, ar) via ROW_NUMBER (QUALIFY is Teradata-only).
switches_raw AS (
    SELECT
        p.clnt_no,
        p.treatmt_strt_dt,
        p.treatmt_end_dt,
        sw.ar_id,
        sw.acct_sw_proc_dt                 AS switch_dt,
        sw.acct_sw_proc_tm                 AS switch_tm,
        sw.rec_typ_cd                      AS switch_channel,
        fl.prod_en_nm                      AS latest_from_product,
        tl.prod_en_nm                      AS latest_to_product,
        ROW_NUMBER() OVER (
            PARTITION BY p.clnt_no, sw.ar_id
            ORDER BY sw.acct_sw_proc_dt DESC, sw.acct_sw_proc_tm DESC
        ) AS rn
    FROM precamp_product p
    INNER JOIN ddwv01.dep_acct_sw_dly sw
        ON  sw.ar_id            = p.ar_id
        AND sw.acct_sw_proc_dt BETWEEN p.treatmt_strt_dt AND p.treatmt_end_dt
    INNER JOIN pba_lkup_curr fl
        ON  fl.acct_typ_cd     = sw.from_acct_typ
        AND fl.acct_clss_cd    = sw.from_acct_clss
        AND fl.srvc_fee_opt_cd = sw.from_fee_opt
    INNER JOIN pba_lkup_curr tl
        ON  tl.acct_typ_cd     = sw.to_acct_typ
        AND tl.acct_clss_cd    = sw.to_acct_clss
        AND tl.srvc_fee_opt_cd = sw.to_fee_opt
    WHERE sw.acct_sw_proc_dt BETWEEN (SELECT min_dt FROM cohort_window)
                                 AND (SELECT max_dt FROM cohort_window)
),

switches AS (
    SELECT clnt_no, ar_id, switch_dt, switch_channel,
           latest_from_product, latest_to_product
    FROM switches_raw
    WHERE rn = 1
),

-- Per (clnt, ar): assemble pre-campaign product, switch, and compute success flag
success AS (
    SELECT
        c.clnt_no,
        p.ar_id,
        p.from_product,
        s.switch_dt,
        s.latest_to_product,

        -- secondary: any upgrade per the hardcoded chequing-tier ladder
        -- (ladder matches production: zp10-nba-measurement-data/src/sql/ctu_success.sql)
        CASE
            WHEN p.from_product = 'RBC Student Banking'
             AND s.latest_to_product IN (
                'RBC No Limit Banking for Students','RBC Day to Day Banking',
                'RBC No Limit Banking','RBC Signature No Limit Banking','RBC VIP Banking')
            THEN 1
            WHEN p.from_product = 'RBC No Limit Banking for Students'
             AND s.latest_to_product IN (
                'RBC Student Banking','RBC Day to Day Banking',
                'RBC No Limit Banking','RBC Signature No Limit Banking','RBC VIP Banking')
            THEN 1
            WHEN p.from_product = 'RBC Day to Day Banking'
             AND s.latest_to_product IN (
                'RBC No Limit Banking','RBC Signature No Limit Banking',
                'RBC VIP Banking','RBC Advantage Banking')
            THEN 1
            WHEN p.from_product = 'RBC No Limit Banking'
             AND s.latest_to_product IN (
                'RBC Signature No Limit Banking','RBC VIP Banking','RBC Advantage Banking')
            THEN 1
            WHEN p.from_product = 'RBC Signature No Limit Banking'
             AND s.latest_to_product IN ('RBC VIP Banking')
            THEN 1
            ELSE 0
        END AS secondary_success
    FROM cohort c
    LEFT JOIN precamp_product p
        ON  p.clnt_no         = c.clnt_no
        AND p.treatmt_strt_dt = c.treatmt_strt_dt
    LEFT JOIN switches s
        ON  s.clnt_no = p.clnt_no
        AND s.ar_id   = p.ar_id
),

seg_counts AS (
    -- ALL grain (overall CTU cohort)
    SELECT
        CAST('ALL'     AS VARCHAR(50)) AS segment,
        CAST('OVERALL' AS VARCHAR(50)) AS segment_level,
        COUNT(DISTINCT clnt_no)                                          AS cohort_size,
        COUNT(DISTINCT CASE WHEN secondary_success = 1 THEN clnt_no END) AS secondary_responders
    FROM success

    UNION ALL

    -- FROM_PRODUCT grain (per pre-campaign product — for ladder validation)
    SELECT
        'FROM_PRODUCT' AS segment,
        from_product   AS segment_level,
        COUNT(DISTINCT clnt_no),
        COUNT(DISTINCT CASE WHEN secondary_success = 1 THEN clnt_no END)
    FROM success
    WHERE from_product IS NOT NULL
    GROUP BY from_product
)

SELECT
    segment,
    segment_level,
    cohort_size,
    secondary_responders
FROM seg_counts
ORDER BY segment, segment_level
;
