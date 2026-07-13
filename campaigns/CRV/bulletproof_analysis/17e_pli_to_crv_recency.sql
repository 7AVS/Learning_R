-- =============================================================================
-- Q17e -- HOW FAR APART? PLI take -> CRV, anchored on the actual limit-change
-- event (dt_cl_change; proven populated + pre-offer by Q17b's empty leakage bucket).
-- Two statements:
--   STMT 1 (the TARGETING view): CRV offers to prior-PLI-takers, bucketed by
--     days-since-PLI-take AT OFFER START (known at decision time -> implementable
--     as a CIDM rule). Conversion counts per bucket x arm -> rate + lift per
--     recency bucket in Excel. This finds N for "target takers within N days".
--   STMT 2 (the RHYTHM view): among CRV CONVERTERS with a prior take, the raw
--     distribution of days from PLI take (dt_cl_change) to CRV conversion
--     (first_response_date) -- descriptive event-to-event gap.
-- Anchor = LATEST prior take before the offer. Grain: one row per CRV offer
-- (rank in CTE, filter rn=1 -- no QUALIFY). Teradata-direct; counts only.
-- =============================================================================

-- STMT 1 -- conversion by days-since-PLI-take at offer start, x arm
WITH crv_offers AS (
    SELECT acct_no, offer_start_date, action_control, responder AS crv_resp
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
),
pli_takes AS (
    SELECT acct_no, dt_cl_change
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-01-01'
      AND responder_cli = 1
      AND dt_cl_change IS NOT NULL
),
ranked AS (
    SELECT c.action_control, c.crv_resp,
           (c.offer_start_date - p.dt_cl_change) AS days_since_take,
           ROW_NUMBER() OVER (PARTITION BY c.acct_no, c.offer_start_date
                              ORDER BY p.dt_cl_change DESC) AS rn
    FROM crv_offers c
    JOIN pli_takes p
      ON p.acct_no = c.acct_no
     AND p.dt_cl_change < c.offer_start_date
)
SELECT
    CASE WHEN days_since_take <= 30  THEN 'a. 000-030'
         WHEN days_since_take <= 60  THEN 'b. 031-060'
         WHEN days_since_take <= 90  THEN 'c. 061-090'
         WHEN days_since_take <= 120 THEN 'd. 091-120'
         WHEN days_since_take <= 180 THEN 'e. 121-180'
         WHEN days_since_take <= 270 THEN 'f. 181-270'
         WHEN days_since_take <= 365 THEN 'g. 271-365'
         ELSE                             'h. 365+' END AS recency_bucket,
    SUM(CASE WHEN action_control = 'Action'  THEN 1 ELSE 0 END)        AS offers_action,
    SUM(CASE WHEN action_control = 'Action'  THEN crv_resp ELSE 0 END) AS responders_action,
    SUM(CASE WHEN action_control = 'Control' THEN 1 ELSE 0 END)        AS offers_control,
    SUM(CASE WHEN action_control = 'Control' THEN crv_resp ELSE 0 END) AS responders_control
FROM ranked
WHERE rn = 1
GROUP BY 1
ORDER BY 1
;

-- STMT 2 -- event-to-event gap: PLI take -> CRV conversion, converters only
WITH crv_conv AS (
    SELECT acct_no, offer_start_date, first_response_date, action_control
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND responder = 1
      AND first_response_date IS NOT NULL
),
pli_takes AS (
    SELECT acct_no, dt_cl_change
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-01-01'
      AND responder_cli = 1
      AND dt_cl_change IS NOT NULL
),
ranked AS (
    SELECT c.action_control,
           (c.first_response_date - p.dt_cl_change) AS take_to_conv_days,
           ROW_NUMBER() OVER (PARTITION BY c.acct_no, c.offer_start_date
                              ORDER BY p.dt_cl_change DESC) AS rn
    FROM crv_conv c
    JOIN pli_takes p
      ON p.acct_no = c.acct_no
     AND p.dt_cl_change < c.offer_start_date
)
SELECT
    CASE WHEN take_to_conv_days <= 30  THEN 'a. 000-030'
         WHEN take_to_conv_days <= 60  THEN 'b. 031-060'
         WHEN take_to_conv_days <= 90  THEN 'c. 061-090'
         WHEN take_to_conv_days <= 120 THEN 'd. 091-120'
         WHEN take_to_conv_days <= 180 THEN 'e. 121-180'
         WHEN take_to_conv_days <= 270 THEN 'f. 181-270'
         WHEN take_to_conv_days <= 365 THEN 'g. 271-365'
         ELSE                               'h. 365+' END AS take_to_conv_bucket,
    action_control,
    COUNT(*) AS crv_converters
FROM ranked
WHERE rn = 1
GROUP BY 1, 2
ORDER BY 1, 2
;
