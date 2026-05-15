-- CRV contact frequency — Action arm only
-- One output: per (offer_month, channel bundle, cumulative contact count).
-- Each source row = one deployment event for one client.
-- contacts_to_date = client's running count of CRV-Action deployments
-- from 2024-10-01 through this deployment date.
--
-- Read:
--   contacts_to_date = 1            -> first-time contact in this window
--   contacts_to_date >= 2           -> repeat contact
--   response_rate across rising N   -> diminishing-returns signal
--   Distribution shift over months  -> saturation building up
--
-- Limit: October 2024 is the start of the window. Any pre-Oct contacts
-- are invisible — all October rows reset to contacts_to_date >= 1.

WITH crv_action AS (
    SELECT
        acct_no,
        channels_deployed,
        responder,
        offer_start_date,
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS offer_month
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Action'
),
running AS (
    SELECT
        offer_month,
        channels_deployed,
        responder,
        COUNT(*) OVER (
            PARTITION BY acct_no
            ORDER BY offer_start_date, channels_deployed
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS contacts_to_date
    FROM crv_action
)
SELECT
    offer_month,
    channels_deployed,
    contacts_to_date,
    COUNT(*)                                       AS deployments,
    SUM(responder)                                 AS responders,
    CAST(SUM(responder) AS DECIMAL(12,4))
        / NULLIF(COUNT(*), 0)                      AS response_rate
FROM running
GROUP BY offer_month, channels_deployed, contacts_to_date
ORDER BY offer_month, channels_deployed, contacts_to_date
;
