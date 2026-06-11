-- RECONSTRUCTION of work-env AUH_vintage.sql (phase_2/measurements/) from 9 photos, 2026-06-11.
-- Cohort + success CTEs confirmed from close-ups (lines 22-110 read at high legibility).
-- Remaining inferred (not character-confirmed): vintage_days spine source, Phase 2 IN-list
-- spellings (pattern-derived), detail/overall join minutiae, final SELECT framing.

-- AUH AU-ADD RESPONSE — VINTAGE CURVE (0-30 days)
-- Two AUH deployments, by Test/Control, strategy arm and model arm,
-- with a strategy-level 'Overall' line alongside the model breakdown.
-- Success = authorized user actually ADDED, read from the EVENT table, NOT the
-- ownership snapshot — snapshot CAPTR_DT is a refresh date and counted long-time holders.

WITH
-- 0-30 day spine; cross-joined so every segment has a row every vintage day
vintage_days AS (
    SELECT calendar_date - DATE '2000-01-01' AS vintage_day
    FROM SYS_CALENDAR.CALENDAR                                  -- [INFERRED: spine source not readable]
    WHERE calendar_date BETWEEN DATE '2000-01-01' AND DATE '2000-01-01' + 30
),

-- CONFIRMED from close-up (lines 23-66)
cohort AS (
    SELECT
        clnt_no,
        CAST(TACTIC_EVNT_ID AS BIGINT) AS acct_no,
        tactic_id,
        treatmt_strt_dt, treatmt_end_dt, tst_grp_cd,
        CASE WHEN RIGHT(trim(tst_grp_cd),2)='_C' THEN 'Control' ELSE 'Test' END AS test_group,
        treatmt_strt_dt AS wave_dt,

        /* strategy_arm - one mapping per deployment */
        CASE
            WHEN tactic_id='2026042AUH' THEN
                CASE WHEN trim(tst_grp_cd) IN ('NRGA','NRGA_C','NRR','NRR_C','NRS','NRS_C')
                    THEN 'NonReward'
                    ELSE 'Unknown' END
            WHEN tactic_id='2026119AUH' THEN
                CASE WHEN SUBSTR(tst_grp_cd,1,3) IN ('NRR','NRM','NRW') THEN 'NonReward'          -- [INFERRED spellings: photo fuzzy, pattern = prefix + 3rd char R/M/W]
                    WHEN SUBSTR(tst_grp_cd,1,3) IN ('RNR','RNM','RNW') THEN 'Rewards_No_Offer'    -- [INFERRED spellings]
                    WHEN SUBSTR(tst_grp_cd,1,3) IN ('ROR','ROM','ROW') THEN 'Rewards_Offer'       -- [INFERRED spellings]
                    ELSE 'Unknown' END
            ELSE 'Unknown'
        END AS strategy_arm,

        /* model_arm - one mapping per deployment.
           2026042AUH: NRGA=Web Visits, NRR=Random, NRS=Model.
           2026119AUH: 3rd char of the code = R/M/W. */
        CASE
            WHEN tactic_id='2026042AUH' THEN
                CASE WHEN trim(tst_grp_cd) IN ('NRGA','NRGA_C') THEN 'Web'
                    WHEN trim(tst_grp_cd) IN ('NRR','NRR_C') THEN 'Random'
                    WHEN trim(tst_grp_cd) IN ('NRS','NRS_C') THEN 'Model'
                    ELSE 'Unknown' END
            WHEN tactic_id='2026119AUH' THEN
                CASE WHEN SUBSTR(tst_grp_cd,3,1)='R' THEN 'Random'
                    WHEN SUBSTR(tst_grp_cd,3,1)='M' THEN 'Model'
                    WHEN SUBSTR(tst_grp_cd,3,1)='W' THEN 'Web'
                    ELSE 'Unknown' END
            ELSE 'Unknown'
        END AS model_arm,

        substr(tactic_decisn_vrb_info,21,3) AS prod_cd    -- product I offered
    FROM DG6V01.tactic_evnt_ip_ar_hist
    WHERE tactic_id IN ('2026042AUH','2026119AUH')    -- exact IDs: indexable, no governor trip
),

-- CONFIRMED from close-up (lines 68-110)
/* SUCCESS_EVENT. I read the real "authorized user added" event, NOT an
   ownership snapshot - the snapshot's CHG_DT is only an end/change marker
   and captr_dt is a refresh date, which counted long-time holders. The
   event table fires only when the add happens and evnt_dt is the true date.
      dtl_evnt_typ_cd = 191  -> "user added"
      ADD_RELTN_CD    = 3    -> authorized-user indicator
   Product comes from the portfolio (visa_prod_cd). No product allow-list,
   so a conversion to ANY product is detectable. The evnt_dt floor keeps
   the scan bounded so the workload governor doesn't kill it. */
au_event AS (
    SELECT a.acct_no, c.visa_prod_cd AS prod_cd, a.evnt_dt
    FROM D3CV12A.CR_CRD_ACCT_EVNT_DLY a
    INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO c
        ON a.clnt_no = c.clnt_no
        AND a.evnt_dt = c.DT_RECORD_EXT
        AND a.acct_no = c.acct_no
    WHERE a.dtl_evnt_typ_cd = 191
        AND a.ADD_RELTN_CD = 3
        AND a.evnt_dt >= DATE '2026-01-01'   -- floor covers both waves; widen if needed
),

/* Earliest add per account+product. MIN(evnt_dt) = first genuine
   acquisition, so repeat adds don't double-count and pre-existing
   holders fall out once I window on treatment dates. */
new_owner AS (
    SELECT acct_no, prod_cd, MIN(evnt_dt) AS first_owned_dt
    FROM au_event GROUP BY acct_no, prod_cd
),

/* ATTRIBUTION. Join success back to the campaign on the account and keep
   only events INSIDE that account's treatment window - ties the add to
   the campaign and drops anyone who already held it.
      first_app_dt        = earliest add to ANY product
      first_app_dt_target = earliest add to the product I offered */
success_events AS (
    SELECT c.wave_dt,c.strategy_arm,c.model_arm,c.test_group,c.acct_no,c.treatmt_strt_dt,
        MIN(n.first_owned_dt) AS first_app_dt,
        MIN(CASE WHEN trim(c.prod_cd)=trim(n.prod_cd) THEN n.first_owned_dt END) AS first_app_dt_target
    FROM cohort c
    INNER JOIN new_owner n
        ON n.acct_no=c.acct_no
        AND n.first_owned_dt BETWEEN c.treatmt_strt_dt AND c.treatmt_end_dt
    GROUP BY 1,2,3,4,5,6
),

-- MODEL-ARM GRAIN: denominators + daily counts at wave x strategy x model x test
-- COUNT(DISTINCT acct_no), never COUNT(*)
pop_d AS (
    SELECT wave_dt, strategy_arm, model_arm, test_group,
           COUNT(DISTINCT acct_no) AS total_population
    FROM cohort
    GROUP BY 1, 2, 3, 4
),
resp_d AS (   -- any-product success
    SELECT wave_dt, strategy_arm, model_arm, test_group,
           (first_app_dt - treatmt_strt_dt) AS vintage_day,
           COUNT(DISTINCT acct_no) AS responders
    FROM success_events
    WHERE first_app_dt IS NOT NULL AND (first_app_dt - treatmt_strt_dt) BETWEEN 0 AND 30
    GROUP BY 1, 2, 3, 4, 5
),
resp_d_m AS (   -- target-product success
    SELECT wave_dt, strategy_arm, model_arm, test_group,
           (first_app_dt_target - treatmt_strt_dt) AS vintage_day,
           COUNT(DISTINCT acct_no) AS responders_target
    FROM success_events
    WHERE first_app_dt_target IS NOT NULL AND (first_app_dt_target - treatmt_strt_dt) BETWEEN 0 AND 30
    GROUP BY 1, 2, 3, 4, 5
),
detail AS (
    SELECT p.wave_dt, p.strategy_arm, p.model_arm, p.test_group, v.vintage_day, p.total_population,
           COALESCE(r1.responders, 0)        AS responders,
           COALESCE(r2.responders_target, 0) AS responders_target
    FROM pop_d p CROSS JOIN vintage_days v
    LEFT JOIN resp_d r1
        ON r1.wave_dt = p.wave_dt AND r1.strategy_arm = p.strategy_arm
       AND r1.model_arm = p.model_arm AND r1.test_group = p.test_group AND r1.vintage_day = v.vintage_day
    LEFT JOIN resp_d_m r2
        ON r2.wave_dt = p.wave_dt AND r2.strategy_arm = p.strategy_arm
       AND r2.model_arm = p.model_arm AND r2.test_group = p.test_group AND r2.vintage_day = v.vintage_day
),

-- OVERALL GRAIN: rolled up across model arms, population counted ONCE at
-- strategy level (stops denominator double-counting); model_arm = 'Overall'
pop_o AS (
    SELECT wave_dt, strategy_arm, test_group, COUNT(DISTINCT acct_no) AS total_population
    FROM cohort
    GROUP BY 1, 2, 3
),
resp_o AS (
    SELECT wave_dt, strategy_arm, test_group,
           (first_app_dt - treatmt_strt_dt) AS vintage_day,
           COUNT(DISTINCT acct_no) AS responders
    FROM success_events
    WHERE first_app_dt IS NOT NULL AND (first_app_dt - treatmt_strt_dt) BETWEEN 0 AND 30
    GROUP BY 1, 2, 3, 4
),
resp_o_t AS (
    SELECT wave_dt, strategy_arm, test_group,
           (first_app_dt_target - treatmt_strt_dt) AS vintage_day,
           COUNT(DISTINCT acct_no) AS responders_target
    FROM success_events
    WHERE first_app_dt_target IS NOT NULL AND (first_app_dt_target - treatmt_strt_dt) BETWEEN 0 AND 30
    GROUP BY 1, 2, 3, 4
),
overall AS (
    SELECT p.wave_dt, p.strategy_arm, CAST('Overall' AS VARCHAR(50)) AS model_arm, p.test_group,
           v.vintage_day, p.total_population,
           COALESCE(r1.responders, 0)        AS responders,
           COALESCE(r2.responders_target, 0) AS responders_target
    FROM pop_o p CROSS JOIN vintage_days v
    LEFT JOIN resp_o r1
        ON r1.wave_dt = p.wave_dt AND r1.strategy_arm = p.strategy_arm
       AND r1.test_group = p.test_group AND r1.vintage_day = v.vintage_day
    LEFT JOIN resp_o_t r2
        ON r2.wave_dt = p.wave_dt AND r2.strategy_arm = p.strategy_arm
       AND r2.test_group = p.test_group AND r2.vintage_day = v.vintage_day
),
final_grain AS (
    SELECT * FROM detail
    UNION ALL
    SELECT * FROM overall
)
-- OUTPUT: window SUMs turn daily counts into running cumulative within each
-- segment. Pivot guidance: model_arm is a filter (never sum across it);
-- 'Overall' for roll-up; keep wave_dt so the two waves stay split;
-- vintage_day is the x-axis (don't sum it).
SELECT
    wave_dt, strategy_arm, model_arm, test_group, vintage_day,
    total_population, responders, responders_target,
    SUM(responders) OVER (PARTITION BY wave_dt, strategy_arm, model_arm, test_group
        ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_cum,
    SUM(responders_target) OVER (PARTITION BY wave_dt, strategy_arm, model_arm, test_group
        ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_target_cum
FROM final_grain
ORDER BY wave_dt, strategy_arm, model_arm, test_group, vintage_day;
