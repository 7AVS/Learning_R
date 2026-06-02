-- ============================================================================
-- Q11 — THROTTLING DECISION TABLE  (one statement, 4 rows: one per cap)
-- "Keep a customer's first N CRV touches, stop after that." For N = 2,3,4,5:
--   pcl_leads_freed            = limit-increase offers whose CRV is fully cut by the cap.
--   pcl_conversions_recovered  = freed leads x their decile's suppression rate (Q08 new_decile),
--                                summed. THIS is the number that matters.
--   crv_conversions_given_up   = CRV sign-ups sitting in the cut touches (gross; ~28% are PCL swaps, Q07).
-- Decision: pick the cap with the best (recovered x $675) minus (given_up x $36).
-- ============================================================================
WITH crv_action_ranked AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date,
        responder AS crv_responder,
        ROW_NUMBER() OVER (PARTITION BY acct_no ORDER BY offer_start_date) AS crv_touch_number
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt AS pcl_strt_dt, treatmt_end_dt AS pcl_end_dt, new_decile
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01' AND channel LIKE '%MB%'
),
pcl_overlap_leads AS (
    SELECT p.new_decile, MIN(c.crv_touch_number) AS earliest_colliding_touch
    FROM pcl_universe p
    JOIN crv_action_ranked c
      ON c.acct_no = p.acct_no AND c.offer_start_date <= p.pcl_end_dt AND c.offer_end_date >= p.pcl_strt_dt
    GROUP BY p.acct_no, p.pcl_strt_dt, p.new_decile
),
caps AS (
    SELECT rn + 1 AS max_crv_touches
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY acct_no) AS rn FROM crv_action_ranked
          QUALIFY ROW_NUMBER() OVER (ORDER BY acct_no) <= 4) g
),
-- CRV given up, per cap (grain = CRV touch)
crv_cost AS (
    SELECT k.max_crv_touches,
        SUM(CASE WHEN c.crv_touch_number > k.max_crv_touches AND c.crv_responder = 1 THEN 1 ELSE 0 END)
            AS crv_conversions_given_up
    FROM crv_action_ranked c CROSS JOIN caps k
    GROUP BY k.max_crv_touches
),
-- PCL freed + recovered, per cap (grain = PCL overlap lead). Suppression rate per new_decile = Q08.
pcl_benefit AS (
    SELECT k.max_crv_touches,
        SUM(CASE WHEN o.earliest_colliding_touch > k.max_crv_touches THEN 1 ELSE 0 END)
            AS pcl_leads_freed,
        CAST(SUM(CASE WHEN o.earliest_colliding_touch > k.max_crv_touches
                 THEN CASE o.new_decile
                        WHEN 1 THEN 0.0304 WHEN 2 THEN 0.0215 WHEN 3 THEN 0.0093
                        WHEN 4 THEN 0.0061 WHEN 5 THEN 0.0057 WHEN 6 THEN 0.0034
                        WHEN 7 THEN 0.0036 WHEN 10 THEN 0.0038 ELSE 0 END
                 ELSE 0 END) AS DECIMAL(14,0))
            AS pcl_conversions_recovered
    FROM pcl_overlap_leads o CROSS JOIN caps k
    GROUP BY k.max_crv_touches
)
SELECT
    b.max_crv_touches,
    b.pcl_leads_freed,
    b.pcl_conversions_recovered,
    a.crv_conversions_given_up
FROM pcl_benefit b
JOIN crv_cost a ON a.max_crv_touches = b.max_crv_touches
ORDER BY b.max_crv_touches
;
