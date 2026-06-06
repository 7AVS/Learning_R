-- ============================================================================
-- Q15 — Who are the no_overlap PCL clients?  Long-format attribute profile x CRV history class
-- no_overlap = PCL leads (mobile banner) with NO CRV offer active during the PCL window.
-- crv_hist_class: prior_crv (CRV ended before window), later_crv (CRV starts after window),
--   never_crv (no CRV decision in window at all), other_crv (overlapping/edge).
-- GOAL: campaign-config docs change every deployment and can't be reconstructed over a 37-mo
--   window, so explain WHO these clients are from their own PCL attributes instead. Each PCL
--   descriptor is unpivoted into (attribute_name, attribute_value) and counted by class, so any
--   attribute where never_crv concentrates is the de-facto eligibility boundary CRV applies.
-- Single scan (Teradata): CROSS JOIN to a 23-row tally + CASE explodes one classified row into
--   one row per attribute (refs the join chain ONCE -> no spool blow-up). VARCHAR casts are
--   required for the unpivot (uniform value type); TRIM on the name avoids CHAR-padding in labels.
-- Pivot: rows = attribute_name + attribute_value, columns = crv_hist_class, value = Sum of n_leads.
-- ============================================================================
WITH pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt AS pcl_strt_dt, treatmt_end_dt AS pcl_end_dt, responder_cli,
           new_decile, decile,
           credit_phase, wallet_band, value_for_money, bi_clnt_seg, lifetm_val_5yr_clnt_cd,
           rbc_tenure, age_band, life_stage,
           premier_client, pb_client, student_indicator,
           new_to_campaign, new_comer, newimm_seg, ngen, gu,
           product_grouping_current, multi_card_ind, vulnrblty_cd,
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
    attribute_name,
    attribute_value,
    crv_hist_class,
    COUNT(*)           AS n_leads,
    SUM(responder_cli) AS n_responders,
    SUM(ever_conv)     AS n_ever_converted_crv
FROM (
    SELECT
        c.crv_hist_class,
        c.responder_cli,
        c.ever_conv,
        TRIM(CASE a.n
            WHEN 1  THEN 'new_decile'                WHEN 2  THEN 'decile'
            WHEN 3  THEN 'credit_phase'              WHEN 4  THEN 'wallet_band'
            WHEN 5  THEN 'value_for_money'           WHEN 6  THEN 'bi_clnt_seg'
            WHEN 7  THEN 'lifetm_val_5yr_clnt_cd'    WHEN 8  THEN 'rbc_tenure'
            WHEN 9  THEN 'age_band'                  WHEN 10 THEN 'life_stage'
            WHEN 11 THEN 'premier_client'            WHEN 12 THEN 'pb_client'
            WHEN 13 THEN 'student_indicator'         WHEN 14 THEN 'new_to_campaign'
            WHEN 15 THEN 'new_comer'                 WHEN 16 THEN 'newimm_seg'
            WHEN 17 THEN 'ngen'                      WHEN 18 THEN 'gu'
            WHEN 19 THEN 'product_grouping_current'  WHEN 20 THEN 'multi_card_ind'
            WHEN 21 THEN 'vulnrblty_cd'               WHEN 22 THEN 'mobile_active_at_decision'
            WHEN 23 THEN 'olb_active_90'
        END) AS attribute_name,
        CASE a.n
            WHEN 1  THEN CAST(c.new_decile AS VARCHAR(40))                WHEN 2  THEN CAST(c.decile AS VARCHAR(40))
            WHEN 3  THEN CAST(c.credit_phase AS VARCHAR(40))              WHEN 4  THEN CAST(c.wallet_band AS VARCHAR(40))
            WHEN 5  THEN CAST(c.value_for_money AS VARCHAR(40))           WHEN 6  THEN CAST(c.bi_clnt_seg AS VARCHAR(40))
            WHEN 7  THEN CAST(c.lifetm_val_5yr_clnt_cd AS VARCHAR(40))    WHEN 8  THEN CAST(c.rbc_tenure AS VARCHAR(40))
            WHEN 9  THEN CAST(c.age_band AS VARCHAR(40))                  WHEN 10 THEN CAST(c.life_stage AS VARCHAR(40))
            WHEN 11 THEN CAST(c.premier_client AS VARCHAR(40))            WHEN 12 THEN CAST(c.pb_client AS VARCHAR(40))
            WHEN 13 THEN CAST(c.student_indicator AS VARCHAR(40))         WHEN 14 THEN CAST(c.new_to_campaign AS VARCHAR(40))
            WHEN 15 THEN CAST(c.new_comer AS VARCHAR(40))                 WHEN 16 THEN CAST(c.newimm_seg AS VARCHAR(40))
            WHEN 17 THEN CAST(c.ngen AS VARCHAR(40))                      WHEN 18 THEN CAST(c.gu AS VARCHAR(40))
            WHEN 19 THEN CAST(c.product_grouping_current AS VARCHAR(40))  WHEN 20 THEN CAST(c.multi_card_ind AS VARCHAR(40))
            WHEN 21 THEN CAST(c.vulnrblty_cd AS VARCHAR(40))               WHEN 22 THEN CAST(c.mobile_active_at_decision AS VARCHAR(40))
            WHEN 23 THEN CAST(c.olb_active_90 AS VARCHAR(40))
        END AS attribute_value
    FROM classified c
    CROSS JOIN (
        -- 23-row tally (1..23), one per attribute. Built from a real table (TOP 23)
        -- because Teradata UNION branches can't reference bare literals (err 3888).
        SELECT ROW_NUMBER() OVER (ORDER BY x) AS n
        FROM (SELECT TOP 23 1 AS x FROM dl_mr_prod.cards_pli_decision_resp) t
    ) a
) u
GROUP BY attribute_name, attribute_value, crv_hist_class
ORDER BY attribute_name, attribute_value, crv_hist_class
;

-- ============================================================================
-- Q15b — Timing diagnostic: later_crv gap buckets
-- Gap (whole months) between PCL treatmt_end_dt and the client's first CRV offer.
-- Reverse causation (PCL limit increase -> later CRV uptake) would concentrate
-- responders in the 0-1/2-3 buckets. Self-contained: CTEs above are scoped to the
-- prior statement (closed at the ;), so the chain is re-declared here.
-- later_crv condition (s.min_start > pcl_end_dt) is mutually exclusive with prior_crv,
-- so the WHERE alone reproduces the classified='later_crv' subset.
-- ============================================================================
WITH pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt AS pcl_strt_dt, treatmt_end_dt AS pcl_end_dt, responder_cli
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
crv_summary AS (
    SELECT acct_no, MIN(offer_start_date) AS min_start
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-01-01'
    GROUP BY acct_no
),
later_crv AS (
    SELECT n.responder_cli,
           n.pcl_strt_dt - (EXTRACT(DAY FROM n.pcl_strt_dt) - 1) AS pcl_month,
           ((CAST(EXTRACT(YEAR  FROM s.min_start) AS INTEGER) - CAST(EXTRACT(YEAR  FROM n.pcl_end_dt) AS INTEGER)) * 12)
           + (CAST(EXTRACT(MONTH FROM s.min_start) AS INTEGER) - CAST(EXTRACT(MONTH FROM n.pcl_end_dt) AS INTEGER)) AS months_gap
    FROM no_overlap n
    JOIN crv_summary s ON s.acct_no = n.acct_no
    WHERE s.min_start > n.pcl_end_dt
)
-- pcl_month added so gap buckets can be read WITHIN a calendar month (controls for the
-- mechanical entanglement: the 13+ bucket can only contain early, lower-responding leads).
SELECT
    pcl_month,
    CASE
        WHEN months_gap BETWEEN 0 AND 1  THEN '0-1'
        WHEN months_gap BETWEEN 2 AND 3  THEN '2-3'
        WHEN months_gap BETWEEN 4 AND 6  THEN '4-6'
        WHEN months_gap BETWEEN 7 AND 12 THEN '7-12'
        ELSE '13+'
    END                  AS gap_bucket,
    COUNT(*)             AS n_leads,
    SUM(responder_cli)   AS n_responders
FROM later_crv
GROUP BY pcl_month, gap_bucket
ORDER BY pcl_month, gap_bucket
;
