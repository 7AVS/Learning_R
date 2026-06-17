-- pcq_ms_vintage.sql
-- PCQ Modal Sales (MS) — cumulative converter curve, vintage_day = days since treatment start.
-- Engine: Teradata-direct (no EDL/GA4 source — no catalog prefix). Descriptive only, no control group.
--
-- ONE query: no volatile tables, no day-spine, no cross join (TDWM-safe).
--   ms_clients : scan the big tactic-event table ONCE; keep only MS client ids (MS lives only there).
--   client     : curated cohort collapsed to CLIENT x DEPLOYMENT grain so the category cells PARTITION
--                the population (additive), + ms flag + pre-treatment categories + first conversion day.
--   daily/cum  : new converters per event-day + running cumulative (window function).
-- A cumulative curve is monotonic, so event-day rows chart as the identical continuous line in Excel.
--
-- Categories are PRE-TREATMENT (deployment) only: decile, strategy_seg, test_group_latest, offered_product.
--   To roll up (MS overall / by one category): pivot in Excel, sum the DAILY columns (n_approved /
--   n_completed), then "Show Values As > Running Total" for the cumulative. cum_approved / cum_completed
--   are the ready-made curve for a SINGLE cell. cohort_size is constant per cell.
-- ms_targeted is client-level: a client ever MS-targeted in post-Jun PCQ is flagged MS across their rows.
-- tpa_ita='TPA' enforced — PCQ has no ITA arm.

WITH
ms_clients AS (                        -- the one expensive scan
    SELECT DISTINCT CLNT_NO
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE TREATMT_STRT_DT >= DATE '2026-06-01'
      AND SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
      AND SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
),
client AS (                            -- one row per client x deployment; categories null-safe for joins
    SELECT
        r.clnt_no,
        r.tactic_id,
        MAX(CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)        AS ms_targeted,
        COALESCE(MAX(r.model_score_decile), -1)                       AS model_score_decile,
        COALESCE(MAX(r.strtgy_seg_typ), '(null)')                     AS strtgy_seg_typ,
        COALESCE(MAX(r.test_group_latest), '(null)')                  AS test_group_latest,
        COALESCE(MAX(r.offer_prod_latest_name), '(null)')             AS offer_prod_latest_name,
        MIN(CASE WHEN r.app_approved  = 1 THEN r.days_to_respond END) AS first_approved_day,
        MIN(CASE WHEN r.app_completed = 1 THEN r.days_to_respond END) AS first_completed_day
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
    LEFT JOIN ms_clients m
           ON m.CLNT_NO = r.clnt_no
    WHERE r.mnemonic         = 'PCQ'
      AND r.decsn_year       = 2026
      AND r.tpa_ita          = 'TPA'
      AND r.treatmt_start_dt >= DATE '2026-06-01'
    GROUP BY r.clnt_no, r.tactic_id
),
cells AS (                             -- denominator: cohort_size per cell (constant across days)
    SELECT
        ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
        test_group_latest, offer_prod_latest_name,
        COUNT(*) AS cohort_size
    FROM client
    GROUP BY ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
             test_group_latest, offer_prod_latest_name
),
daily AS (                             -- new converters per cell per event-day, both metrics
    SELECT
        ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
        test_group_latest, offer_prod_latest_name,
        first_approved_day AS vintage_day,
        CAST(COUNT(*) AS BIGINT) AS n_approved, CAST(0 AS BIGINT) AS n_completed
    FROM client
    WHERE first_approved_day BETWEEN 0 AND 90
    GROUP BY ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
             test_group_latest, offer_prod_latest_name, first_approved_day

    UNION ALL

    SELECT
        ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
        test_group_latest, offer_prod_latest_name,
        first_completed_day, CAST(0 AS BIGINT), CAST(COUNT(*) AS BIGINT)
    FROM client
    WHERE first_completed_day BETWEEN 0 AND 90
    GROUP BY ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
             test_group_latest, offer_prod_latest_name, first_completed_day
),
daily_rollup AS (
    SELECT
        ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
        test_group_latest, offer_prod_latest_name, vintage_day,
        SUM(n_approved)  AS n_approved,
        SUM(n_completed) AS n_completed
    FROM daily
    GROUP BY ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
             test_group_latest, offer_prod_latest_name, vintage_day
)
SELECT
    d.ms_targeted,
    d.tactic_id,
    d.model_score_decile,
    d.strtgy_seg_typ,
    d.test_group_latest,
    d.offer_prod_latest_name,
    d.vintage_day,
    d.n_approved,
    d.n_completed,
    SUM(d.n_approved) OVER (
        PARTITION BY d.ms_targeted, d.tactic_id, d.model_score_decile,
                     d.strtgy_seg_typ, d.test_group_latest, d.offer_prod_latest_name
        ORDER BY d.vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_approved,
    SUM(d.n_completed) OVER (
        PARTITION BY d.ms_targeted, d.tactic_id, d.model_score_decile,
                     d.strtgy_seg_typ, d.test_group_latest, d.offer_prod_latest_name
        ORDER BY d.vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_completed,
    c.cohort_size
FROM daily_rollup d
JOIN cells c
  ON  c.ms_targeted            = d.ms_targeted
  AND c.tactic_id              = d.tactic_id
  AND c.model_score_decile     = d.model_score_decile
  AND c.strtgy_seg_typ         = d.strtgy_seg_typ
  AND c.test_group_latest      = d.test_group_latest
  AND c.offer_prod_latest_name = d.offer_prod_latest_name
ORDER BY 1, 2, 3, 4, 5, 6, 7;
