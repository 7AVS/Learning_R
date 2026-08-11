-- P11: PLI sales-modal FREQUENCY HAZARD - how many times does a client see the modal before
-- converting or dropping off, and does converting suppress further exposure.
-- Scope: May 2026 cohort ONLY, CHALLENGER (WMS) arm ONLY - frequency only exists where the
--   modal was actually served. No champion comparison here (see p9 for arm contrast).
-- Modal id = i_308392 (+ tiny i_335273), confirmed real PLI modal by behavior, not label
--   (see modal_item_id_lookup.md). client_group renames p9's strategy split: New-to-Campaign
--   = M8RHS9OI, Existing = LZJ4PENS.
-- Engine: Starburst/Trino. Counts only - no rates/percentages in this file, computed downstream.
-- 9881-safe: GA4 side cast only (TRY_CAST up_srf_id2_value), clnt_no raw uncast.
--
-- GA4 table note: p9/p10 both read edl0_im...tsz_00198_data_ga4_ecommerce_reduced (the "_reduced"
--   suffix). That exact name is NOT in the repo canon list (which shows ..._ecommerce and
--   ..._narrow) but it's what p9/p10 actually query against, so p11 matches them for comparability.
--
-- OUTPUT COLUMNS (grain: cohort_month x client_group x view_number, view_number 1-5, 5="5 or more"):
--   cohort_month                                     - month of treatmt_strt_dt (2026-05)
--   client_group                                     - New-to-Campaign / Existing (strategy_id split)
--   view_number                                      - which distinct modal-view session, 1..5 (5 = 5th session or later)
--   clients_targeted_in_this_group                    - reach denominator: total distinct clients in this cohort x client_group (constant across the 5 view_number rows - intended)
--   clients_reaching_at_least_this_many_views         - survival denominator: total view sessions >= view_number
--   clients_whose_last_view_was_this_number           - distribution: total view sessions == view_number (5 bucket = total >= 5)
--   clients_dismissing_on_this_view                   - dismissed (close/not now/dismiss) in the session ranked at view_number
--   clients_converting_on_or_after_this_view           - converted with dt_cl_change on/after the date of the view ranked at view_number (NOT a clean hazard - denominator still includes already-converted clients)
--   clients_still_unconverted_when_they_reached_this_view - hazard DENOMINATOR: at this view_number, dt_cl_change is null or still >= this view's date (not yet converted)
--   clients_converting_after_this_view_but_before_their_next_view - hazard NUMERATOR: converted in the window [this view's date, next view's date) - the marginal conversions attributable to this extra exposure
--   clients_with_any_view_after_their_conversion_date  - DIAGNOSTIC: at this view_number, client has >=1 view session dated after dt_cl_change (near-zero = conversion suppresses the modal). Client-level flag repeated across that client's view_number rows - do NOT sum this column

WITH pop AS (                                          -- May 2026 challenger clients, first deployment only
  SELECT
    clnt_no,
    CASE strategy_id WHEN 'M8RHS9OI' THEN 'New-to-Campaign'
                      WHEN 'LZJ4PENS' THEN 'Existing' ELSE strategy_id END AS client_group,
    responder_cli,
    dt_cl_change,
    treatmt_strt_dt,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_strt_dt) AS rn
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE report_groups_period LIKE '%R____WMS%'
    AND treatmt_strt_dt >= DATE '2026-05-01' AND treatmt_strt_dt < DATE '2026-06-01'
),
pop1 AS (                                              -- one row per client, cohort_month attached
  SELECT clnt_no, client_group, responder_cli, dt_cl_change, treatmt_strt_dt,
         date_format(treatmt_strt_dt, '%Y-%m') AS cohort_month
  FROM pop WHERE rn = 1
),
modal_events AS (                                      -- GA4 modal rows for May pop clients, post-treatment only
  SELECT m.clnt_no, m.event_name, m.event_date, m.sess, m.it_creative_name
  FROM (
    SELECT
      TRY_CAST(up_srf_id2_value AS DECIMAL(14,0)) AS clnt_no,   -- cast GA4 side only
      event_name,
      event_date,
      CAST(ep_ga_session_id AS VARCHAR) AS sess,
      it_creative_name
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('05','06','07','08')     -- May cohort through current month; extend as data accrues
      AND it_item_id IN ('i_308392','i_335273')
  ) m
  JOIN pop1 p ON p.clnt_no = m.clnt_no
  WHERE m.event_date >= p.treatmt_strt_dt
    AND m.event_date >= DATE '2026-05-01'                      -- redundant static floor alongside the dynamic window
),
view_sessions AS (                                     -- one row per client x session that contains a view_promotion, session date = earliest fire
  SELECT clnt_no, sess, MIN(event_date) AS session_date
  FROM modal_events
  WHERE event_name = 'view_promotion'
  GROUP BY clnt_no, sess
),
ranked_views AS (                                      -- chronological rank of each client's view sessions + their total session count
  SELECT
    clnt_no, sess, session_date,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY session_date, sess) AS view_number_raw,
    COUNT(*) OVER (PARTITION BY clnt_no) AS total_view_sessions
  FROM view_sessions
),
dismiss_by_session AS (                                -- dismiss flag per client x session (p9's close/not now/dismiss logic, verbatim)
  SELECT clnt_no, sess,
    MAX(CASE WHEN event_name = 'select_promotion'
              AND ( LOWER(it_creative_name) LIKE '%close%'
                 OR LOWER(it_creative_name) LIKE '%not now%'
                 OR LOWER(it_creative_name) LIKE '%dismiss%' )
             THEN 1 ELSE 0 END) AS dismissed
  FROM modal_events
  GROUP BY clnt_no, sess
),
session_detail AS (                                    -- per client-session: bucketed view_number (5 = 5th or later), dismiss flag, conversion facts
  SELECT
    p.cohort_month, p.client_group, rv.clnt_no, p.responder_cli, p.dt_cl_change,
    rv.total_view_sessions,
    LEAST(rv.view_number_raw, 5) AS view_number,
    rv.session_date,
    COALESCE(db.dismissed, 0) AS dismissed
  FROM ranked_views rv
  JOIN pop1 p ON p.clnt_no = rv.clnt_no
  LEFT JOIN dismiss_by_session db ON db.clnt_no = rv.clnt_no AND db.sess = rv.sess
),
bucketed_base AS (                                     -- collapse to client x view_number bucket: earliest date in bucket, any dismiss in bucket
  SELECT
    cohort_month, client_group, clnt_no, dt_cl_change, view_number,
    MIN(session_date) AS bucket_anchor_date,
    MAX(dismissed) AS bucket_dismissed
  FROM session_detail
  GROUP BY cohort_month, client_group, clnt_no, dt_cl_change, view_number
),
bucketed AS (                                          -- add each client's NEXT bucket's anchor date, for the hazard-interval numerator
  SELECT
    cohort_month, client_group, clnt_no, dt_cl_change, view_number,
    bucket_anchor_date, bucket_dismissed,
    LEAD(bucket_anchor_date) OVER (PARTITION BY clnt_no ORDER BY view_number) AS next_bucket_anchor_date
  FROM bucketed_base
),
client_group_pop AS (                                  -- every May client with their total capped view-session count (0 if never viewed)
  SELECT
    p.cohort_month, p.client_group, p.clnt_no, p.responder_cli, p.dt_cl_change,
    COALESCE(rv_tot.total_view_sessions, 0) AS total_view_sessions
  FROM pop1 p
  LEFT JOIN (SELECT DISTINCT clnt_no, total_view_sessions FROM ranked_views) rv_tot
    ON rv_tot.clnt_no = p.clnt_no
),
group_totals AS (                                      -- reach denominator: distinct clients targeted per cohort_month x client_group
  SELECT cohort_month, client_group, COUNT(DISTINCT clnt_no) AS clients_targeted_in_this_group
  FROM client_group_pop
  GROUP BY cohort_month, client_group
),
client_conversion_leakage AS (                         -- per client: did ANY view session land after their conversion date
  SELECT clnt_no,
    MAX(CASE WHEN dt_cl_change IS NOT NULL AND session_date > dt_cl_change THEN 1 ELSE 0 END) AS any_view_after_conversion
  FROM session_detail
  GROUP BY clnt_no
),
spine AS (                                             -- view_number spine 1-5 (5 = 5 or more)
  SELECT s AS view_number FROM UNNEST(sequence(1, 5)) AS t(s)
),
grid AS (                                              -- dense cohort_month x client_group x view_number grid
  SELECT DISTINCT cohort_month, client_group, view_number
  FROM client_group_pop CROSS JOIN spine
)
SELECT
  g.cohort_month,
  g.client_group,
  g.view_number,
  gt.clients_targeted_in_this_group,
  COUNT(DISTINCT CASE WHEN cgp.total_view_sessions >= g.view_number
                       THEN cgp.clnt_no END)                                   AS clients_reaching_at_least_this_many_views,
  COUNT(DISTINCT CASE WHEN LEAST(cgp.total_view_sessions, 5) = g.view_number
                       THEN cgp.clnt_no END)                                   AS clients_whose_last_view_was_this_number,
  COUNT(DISTINCT CASE WHEN b.clnt_no IS NOT NULL AND b.bucket_dismissed = 1
                       THEN cgp.clnt_no END)                                   AS clients_dismissing_on_this_view,
  COUNT(DISTINCT CASE WHEN b.clnt_no IS NOT NULL AND cgp.responder_cli = 1
                            AND cgp.dt_cl_change IS NOT NULL
                            AND cgp.dt_cl_change >= b.bucket_anchor_date
                       THEN cgp.clnt_no END)                                   AS clients_converting_on_or_after_this_view,
  COUNT(DISTINCT CASE WHEN b.clnt_no IS NOT NULL
                            AND (b.dt_cl_change IS NULL OR b.dt_cl_change >= b.bucket_anchor_date)
                       THEN cgp.clnt_no END)                                   AS clients_still_unconverted_when_they_reached_this_view,
  COUNT(DISTINCT CASE WHEN b.clnt_no IS NOT NULL AND b.dt_cl_change IS NOT NULL
                            AND b.dt_cl_change >= b.bucket_anchor_date
                            AND (b.next_bucket_anchor_date IS NULL OR b.dt_cl_change < b.next_bucket_anchor_date)
                       THEN cgp.clnt_no END)                                   AS clients_converting_after_this_view_but_before_their_next_view,
  COUNT(DISTINCT CASE WHEN b.clnt_no IS NOT NULL AND ccl.any_view_after_conversion = 1
                       THEN cgp.clnt_no END)                                   AS clients_with_any_view_after_their_conversion_date
FROM grid g
JOIN client_group_pop cgp
  ON cgp.cohort_month = g.cohort_month AND cgp.client_group = g.client_group
JOIN group_totals gt
  ON gt.cohort_month = g.cohort_month AND gt.client_group = g.client_group
LEFT JOIN bucketed b
  ON b.clnt_no = cgp.clnt_no AND b.view_number = g.view_number
LEFT JOIN client_conversion_leakage ccl
  ON ccl.clnt_no = cgp.clnt_no
GROUP BY g.cohort_month, g.client_group, g.view_number, gt.clients_targeted_in_this_group
ORDER BY g.cohort_month, g.client_group, g.view_number;
