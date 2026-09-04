-- 03_march_curated_only_samples.sql
-- CRV x Success Library reconciliation, step 3 of 5: sample accounts where the
-- curated table says responder=1 but the library sees no in-window
-- p_card_installmt_purch event, to eyeball window-edge vs. data-gap misses.
-- ENGINE   : Starburst/Trino (federated dg6v01 <-> dw00_im <-> edl0_im).
-- Population/arm/window CTEs, curated table+columns, join key/casts: same
-- sources as campaigns/CRV/success_library_recon/02_march_account_diff.sql
-- (see that file's header for exact file:line citations).
-- Guard   : arm comes only from the tactic table.
-- Run date : not yet executed.

WITH

-- March 2026 CRV deployments, collapsed to one row per account x arm (file 01 rule, restricted to cohort_month = 2026-03)
tactic_cohort AS (
    SELECT
        visa_acct_no,
        CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END     AS arm,
        MIN(treatmt_eff_dt)                                             AS acct_treat_dt,
        MAX(treatmt_end_dt)                                             AS acct_treat_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_eff_dt >= DATE '2026-03-01'
      AND treatmt_eff_dt <  DATE '2026-04-01'
    GROUP BY
        visa_acct_no,
        CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END
),

-- library success inside the account's own [acct_treat_dt, acct_treat_end_dt] window (file 01 rule)
lib_success_flag AS (
    SELECT DISTINCT t.visa_acct_no, t.arm
    FROM tactic_cohort t
    JOIN edl0_im.prod_zp10_prod_staging.measurement_events_v2 m
        ON CAST(t.visa_acct_no AS DECIMAL(38,0)) = CAST(m.acct_no AS DECIMAL(38,0))
    WHERE m.event_cd = 'p_card_installmt_purch'
      AND m.event_date >= DATE '2026-03-01'
      AND m.event_date BETWEEN t.acct_treat_dt AND t.acct_treat_end_dt
),

-- curated responder flag + response date for the March cohort, collapsed per account
curated_march AS (
    SELECT
        CAST(acct_no AS DECIMAL(38,0))                                          AS acct_key,
        MAX(responder)                                                          AS cur_success,
        MAX(CASE WHEN responder = 1 THEN first_response_date END)               AS cur_response_dt
    FROM dw00_im.dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-03-01'
      AND offer_start_date <  DATE '2026-04-01'
      AND TRIM(action_control) IN ('Action', 'Control')
    GROUP BY CAST(acct_no AS DECIMAL(38,0))
),

-- curated_success=1, library in-window success=0: the population this file samples
curated_only AS (
    SELECT
        t.visa_acct_no,
        t.arm,
        t.acct_treat_dt,
        t.acct_treat_end_dt,
        cm.cur_response_dt
    FROM tactic_cohort t
    JOIN curated_march cm ON cm.acct_key = CAST(t.visa_acct_no AS DECIMAL(38,0))
    LEFT JOIN lib_success_flag ls ON ls.visa_acct_no = t.visa_acct_no AND ls.arm = t.arm
    WHERE cm.cur_success = 1
      AND ls.visa_acct_no IS NULL
),

-- nearest library event on/after acct_treat_dt, no upper bound -- narrowed to the curated_only set before the events join
nearest_event AS (
    SELECT
        co.visa_acct_no,
        MIN(m.event_date) AS nearest_event_dt
    FROM curated_only co
    JOIN edl0_im.prod_zp10_prod_staging.measurement_events_v2 m
        ON CAST(co.visa_acct_no AS DECIMAL(38,0)) = CAST(m.acct_no AS DECIMAL(38,0))
    WHERE m.event_cd = 'p_card_installmt_purch'
      AND m.event_date >= DATE '2026-03-01'
      AND m.event_date >= co.acct_treat_dt
    GROUP BY co.visa_acct_no
),

ranked AS (
    SELECT
        co.arm,
        co.visa_acct_no,
        co.acct_treat_dt,
        co.acct_treat_end_dt,
        co.cur_response_dt,
        ne.nearest_event_dt,
        CASE WHEN ne.nearest_event_dt IS NOT NULL
             THEN date_diff('day', co.acct_treat_end_dt, ne.nearest_event_dt)
        END AS gap_days_after_window_end,
        ROW_NUMBER() OVER (PARTITION BY co.arm ORDER BY co.visa_acct_no) AS rn
    FROM curated_only co
    LEFT JOIN nearest_event ne ON ne.visa_acct_no = co.visa_acct_no
)

SELECT
    arm,
    CAST(CAST(visa_acct_no AS DECIMAL(18,0)) AS VARCHAR(20)) AS visa_acct_no,
    acct_treat_dt,
    acct_treat_end_dt,
    cur_response_dt,
    nearest_event_dt,
    gap_days_after_window_end
FROM ranked
WHERE rn <= 10
ORDER BY arm, visa_acct_no;
