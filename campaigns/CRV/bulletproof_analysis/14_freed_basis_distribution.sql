-- ============================================================================
-- Q14 — THE BASIS FOR pcl_leads_freed (transparency check for Q11)
-- For every limit-increase offer that collided with CRV, what was the POSITION of the
-- earliest CRV touch it collided with? (1 = the customer's 1st CRV touch, 2 = 2nd, ...)
--
-- This is the hidden number behind Q11's "freed". Read it directly:
--   pcl_leads_freed at cap N  =  SUM of n_overlap_leads where earliest_colliding_crv_touch > N.
-- e.g. cap 3 frees every lead whose earliest collision was touch 4, 5, 6, ... (all cut by the cap).
-- Add up the rows past your cap and you get Q11's freed number — no black box.
-- ============================================================================
WITH crv_action_ranked AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date,
        ROW_NUMBER() OVER (PARTITION BY acct_no ORDER BY offer_start_date) AS crv_touch_number
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt AS pcl_strt_dt,
        treatmt_end_dt  AS pcl_end_dt,
        responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
pcl_overlap_leads AS (
    SELECT
        p.acct_no,
        p.pcl_strt_dt,
        p.responder_cli,
        MIN(c.crv_touch_number) AS earliest_colliding_crv_touch
    FROM pcl_universe p
    JOIN crv_action_ranked c
      ON c.acct_no          = p.acct_no
     AND c.offer_start_date <= p.pcl_end_dt
     AND c.offer_end_date   >= p.pcl_strt_dt
    GROUP BY p.acct_no, p.pcl_strt_dt, p.responder_cli
)
SELECT
    earliest_colliding_crv_touch,
    COUNT(*)             AS n_overlap_leads,
    SUM(responder_cli)   AS pcl_responders
FROM pcl_overlap_leads
GROUP BY earliest_colliding_crv_touch
ORDER BY earliest_colliding_crv_touch
;
