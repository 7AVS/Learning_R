-- 02_march_account_diff.sql
-- CRV x Success Library reconciliation, step 2 of 5: account-level diff for the
-- 2026-03 cohort, both arms, isolating where curated and library success disagree.
-- ENGINE   : Starburst/Trino (federated dg6v01 <-> dw00_im <-> edl0_im).
-- Population/arm/window CTEs and casts copied verbatim from
--   campaigns/CRV/success_library_recon/01_cohort_arm_summary.sql
-- Curated table + response columns (VERIFIED 2026-05-26):
--   references/campaign_query_cards.md:124-140 (CRV card) — table
--   dl_mr_prod.cards_crv_install_decis_resp, Starburst catalog dw00_im
--   (campaign_query_cards.md:124), grain account x tactic offer
--   (campaign_query_cards.md:125), response = responder/first_response_date
--   (campaign_query_cards.md:127), window cols offer_start_date/offer_end_date
--   (campaign_query_cards.md:128).
--   Column list/types cross-checked: schemas/crv_pcl_curated_schemas.md:26-41
--   (acct_no integer, responder smallint, first_response_date date).
-- Curated join key/cast: CAST(acct_no AS DECIMAL(38,0)) = CAST(visa_acct_no AS
--   DECIMAL(38,0)) and TRIM(action_control) IN ('Action','Control') population
--   filter, copied from
--   campaigns/CRV/vintage_reconciliation/crv_edw_join_population_diagnostic.sql:87-91.
-- Guard   : arm comes only from the tactic table (same guard as file 01); the
--   curated table is used only for its response flag/date, never for arm.
-- Run date : not yet executed.

WITH

-- March 2026 CRV deployments, collapsed to one row per account x arm (file 01, restricted to cohort_month = 2026-03)
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

-- library success anywhere from 2026-03-01 on, window ignored -- separates "event outside window" from "event absent"
lib_any_flag AS (
    SELECT DISTINCT t.visa_acct_no, t.arm
    FROM tactic_cohort t
    JOIN edl0_im.prod_zp10_prod_staging.measurement_events_v2 m
        ON CAST(t.visa_acct_no AS DECIMAL(38,0)) = CAST(m.acct_no AS DECIMAL(38,0))
    WHERE m.event_cd = 'p_card_installmt_purch'
      AND m.event_date >= DATE '2026-03-01'
),

-- curated responder flag for the March cohort, collapsed per account (multi-wave accounts MAX'd like the tactic side)
curated_march AS (
    SELECT
        CAST(acct_no AS DECIMAL(38,0)) AS acct_key,
        MAX(responder)                 AS cur_success
    FROM dw00_im.dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-03-01'
      AND offer_start_date <  DATE '2026-04-01'
      AND TRIM(action_control) IN ('Action', 'Control')
    GROUP BY CAST(acct_no AS DECIMAL(38,0))
),

acct_flags AS (
    SELECT
        t.visa_acct_no,
        t.arm,
        COALESCE(cm.cur_success, 0)                                     AS cur_success,
        CASE WHEN ls.visa_acct_no IS NOT NULL THEN 1 ELSE 0 END          AS lib_success,
        CASE WHEN la.visa_acct_no IS NOT NULL THEN 1 ELSE 0 END          AS lib_any
    FROM tactic_cohort t
    LEFT JOIN curated_march   cm ON cm.acct_key = CAST(t.visa_acct_no AS DECIMAL(38,0))
    LEFT JOIN lib_success_flag ls ON ls.visa_acct_no = t.visa_acct_no AND ls.arm = t.arm
    LEFT JOIN lib_any_flag     la ON la.visa_acct_no = t.visa_acct_no AND la.arm = t.arm
),

bucketed AS (
    SELECT
        arm,
        CASE
            WHEN cur_success = 1 AND lib_success = 1                     THEN 'both'
            WHEN cur_success = 1 AND lib_success = 0 AND lib_any = 1     THEN 'curated_only_in_lib_any'
            WHEN cur_success = 1 AND lib_success = 0 AND lib_any = 0     THEN 'curated_only_lib_none'
            WHEN cur_success = 0 AND lib_success = 1                     THEN 'library_only'
            ELSE 'neither'
        END AS bucket,
        visa_acct_no
    FROM acct_flags
)

SELECT
    arm,
    bucket,
    COUNT(DISTINCT visa_acct_no) AS accts
FROM bucketed
GROUP BY arm, bucket
ORDER BY arm, bucket;
