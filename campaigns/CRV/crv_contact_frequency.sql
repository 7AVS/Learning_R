-- CRV contact frequency by channel — Action arm only
-- Date floor 2024-10-01.
-- channels_deployed is a concatenated string ('EM_IM_DO', 'IM', etc.).
-- This query explodes it into one row per (deployment × channel touched)
-- using UNION ALL across the three codes CRV uses (EM, IM, DO).
--
-- Output grain: one row per (offer_month, channel).
-- unique_clients  = distinct accounts touched on that channel that month
-- total_deployments = total channel-touches (a single deployment to a
--                     multi-channel string counts once per channel)
-- Avg touches per client = total_deployments / unique_clients (compute in Excel).

WITH crv_action AS (
    SELECT
        acct_no,
        channels_deployed,
        offer_start_date,
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS offer_month
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Action'
),
channel_long AS (
    SELECT acct_no, offer_month, 'EM' AS channel
        FROM crv_action WHERE channels_deployed LIKE '%EM%'
    UNION ALL
    SELECT acct_no, offer_month, 'IM' AS channel
        FROM crv_action WHERE channels_deployed LIKE '%IM%'
    UNION ALL
    SELECT acct_no, offer_month, 'DO' AS channel
        FROM crv_action WHERE channels_deployed LIKE '%DO%'
)
SELECT
    offer_month,
    channel,
    COUNT(DISTINCT acct_no) AS unique_clients,
    COUNT(*)                AS total_deployments
FROM channel_long
GROUP BY offer_month, channel
ORDER BY offer_month, channel
;
