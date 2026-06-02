-- ============================================================================
-- Q15 — Who are the no_overlap PCL clients?  Long-format attribute profile x CRV history class
-- no_overlap = PCL leads (mobile banner) with NO CRV offer active during the PCL window.
-- crv_hist_class: prior_crv (CRV ended before window), later_crv (CRV starts after window),
--   never_crv (no CRV decision in window at all), other_crv (overlapping/edge).
-- GOAL: campaign-config docs change every deployment and can't be reconstructed over a 37-mo
--   window, so explain WHO these clients are from their own PCL attributes instead. Each PCL
--   descriptor is unpivoted into (attribute_name, attribute_value) and counted by class, so any
--   attribute where never_crv concentrates is the de-facto eligibility boundary CRV applies.
-- Single scan: CROSS JOIN UNNEST explodes one classified row into one row per attribute
--   (refs the join chain ONCE -> no spool blow-up). CASTs to VARCHAR are required for the unpivot.
-- Pivot: rows = attribute_name + attribute_value, columns = crv_hist_class, value = Sum of n_leads.
-- ============================================================================
WITH pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt AS pcl_strt_dt, treatmt_end_dt AS pcl_end_dt, responder_cli,
           new_decile, decile,
           credit_phase, wallet_band, value_for_money, bi_clnt_seg, lifetm_val_5yr_clnt_cd,
           rbc_tenure, age_band, life_stage,
           premier_client, pb_client, student_indicator,
           new_to_campaign, new_comer, newimm_seg, ngen, gu,
           product_grouping_current, multi_card_ind, vulnrbty_cd,
           mobile_active_at_decision, olb_active_90
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01' AND channel LIKE '%MB%'
),
crv_im_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND action_control = 'Control'
),
oa_keys AS (
    SELECT DISTINCT p.acct_no, p.pcl_strt_dt, p.pcl_end_dt
    FROM pcl_universe p JOIN crv_im_action c
      ON c.acct_no=p.acct_no AND c.offer_start_date<=p.pcl_end_dt AND c.offer_end_date>=p.pcl_strt_dt
),
oc_keys AS (
    SELECT DISTINCT p.acct_no, p.pcl_strt_dt, p.pcl_end_dt
    FROM pcl_universe p JOIN crv_control c
      ON c.acct_no=p.acct_no AND c.offer_start_date<=p.pcl_end_dt AND c.offer_end_date>=p.pcl_strt_dt
),
no_overlap AS (
    SELECT p.*
    FROM pcl_universe p
    LEFT JOIN oa_keys oa ON oa.acct_no=p.acct_no AND oa.pcl_strt_dt=p.pcl_strt_dt AND oa.pcl_end_dt=p.pcl_end_dt
    LEFT JOIN oc_keys oc ON oc.acct_no=p.acct_no AND oc.pcl_strt_dt=p.pcl_strt_dt AND oc.pcl_end_dt=p.pcl_end_dt
    WHERE oa.acct_no IS NULL AND oc.acct_no IS NULL
),
-- ONE row per account: collapses CRV history so the join below is 1-to-1 (kills fanout)
crv_summary AS (
    SELECT acct_no,
           MIN(offer_start_date) AS min_start,
           MIN(offer_end_date)   AS min_end,
           MAX(responder)        AS ever_conv
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-01-01'
    GROUP BY acct_no
),
classified AS (
    SELECT
        n.*,
        s.ever_conv,
        CASE WHEN s.acct_no IS NULL            THEN 'never_crv'
             WHEN s.min_end   < n.pcl_strt_dt  THEN 'prior_crv'
             WHEN s.min_start > n.pcl_end_dt   THEN 'later_crv'
             ELSE 'other_crv' END AS crv_hist_class
    FROM no_overlap n
    LEFT JOIN crv_summary s ON s.acct_no = n.acct_no
)
SELECT
    a.attribute_name,
    a.attribute_value,
    c.crv_hist_class,
    COUNT(*)            AS n_leads,
    SUM(c.responder_cli) AS n_responders,
    SUM(c.ever_conv)     AS n_ever_converted_crv
FROM classified c
CROSS JOIN UNNEST(ARRAY[
    ('new_decile',                CAST(c.new_decile AS VARCHAR)),
    ('decile',                    CAST(c.decile AS VARCHAR)),
    ('credit_phase',              CAST(c.credit_phase AS VARCHAR)),
    ('wallet_band',               CAST(c.wallet_band AS VARCHAR)),
    ('value_for_money',           CAST(c.value_for_money AS VARCHAR)),
    ('bi_clnt_seg',               CAST(c.bi_clnt_seg AS VARCHAR)),
    ('lifetm_val_5yr_clnt_cd',    CAST(c.lifetm_val_5yr_clnt_cd AS VARCHAR)),
    ('rbc_tenure',                CAST(c.rbc_tenure AS VARCHAR)),
    ('age_band',                  CAST(c.age_band AS VARCHAR)),
    ('life_stage',                CAST(c.life_stage AS VARCHAR)),
    ('premier_client',            CAST(c.premier_client AS VARCHAR)),
    ('pb_client',                 CAST(c.pb_client AS VARCHAR)),
    ('student_indicator',         CAST(c.student_indicator AS VARCHAR)),
    ('new_to_campaign',           CAST(c.new_to_campaign AS VARCHAR)),
    ('new_comer',                 CAST(c.new_comer AS VARCHAR)),
    ('newimm_seg',                CAST(c.newimm_seg AS VARCHAR)),
    ('ngen',                      CAST(c.ngen AS VARCHAR)),
    ('gu',                        CAST(c.gu AS VARCHAR)),
    ('product_grouping_current',  CAST(c.product_grouping_current AS VARCHAR)),
    ('multi_card_ind',            CAST(c.multi_card_ind AS VARCHAR)),
    ('vulnrbty_cd',               CAST(c.vulnrbty_cd AS VARCHAR)),
    ('mobile_active_at_decision', CAST(c.mobile_active_at_decision AS VARCHAR)),
    ('olb_active_90',             CAST(c.olb_active_90 AS VARCHAR))
]) AS a(attribute_name, attribute_value)
GROUP BY a.attribute_name, a.attribute_value, c.crv_hist_class
ORDER BY a.attribute_name, a.attribute_value, c.crv_hist_class
;
