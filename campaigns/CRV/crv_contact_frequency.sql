-- CRV contact frequency — Action arm only, by channel bundle
-- Date floor 2024-10-01.
-- channels_deployed is the bundle string ('IM' alone or 'EM_IM_DO' together).
-- We group by it AS-IS (no decomposition) because the codes don't deploy
-- independently. Two natural client groups: IM-only and the triple bundle.

------------------------------------------------------------------------------
-- A) Monthly volume per channel bundle
--    Per offer_month × channels_deployed:
--      unique_clients      = distinct acct_no in that bundle, that month
--      total_deployments   = total deployment events (per-client rows)
--    Avg deployments per client = total_deployments / unique_clients (Excel).
------------------------------------------------------------------------------
WITH crv_action AS (
    SELECT
        acct_no,
        channels_deployed,
        offer_start_date,
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS offer_month
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Action'
)
SELECT
    offer_month,
    channels_deployed,
    COUNT(DISTINCT acct_no) AS unique_clients,
    COUNT(*)                AS total_deployments
FROM crv_action
GROUP BY offer_month, channels_deployed
ORDER BY offer_month, channels_deployed
;

------------------------------------------------------------------------------
-- B) Diminishing returns — CRV response rate by per-client contact frequency
--    For each (client × channel bundle), count deployments and flag whether
--    the client EVER responded. Then aggregate to (channel_bundle, n_contacts).
--    Output: at each frequency level, cumulative response rate.
--
--    Read: as n_contacts climbs, does response_rate rise sharply, flatten,
--    or decline? Flattening = diminishing marginal return on extra touches.
--
--    Caveat: response_rate here is "ever responded among clients with N
--    contacts." Clients with higher N had more chances to convert, so a
--    rising curve is partly mechanical. Directional only; not per-touch
--    causal. For true per-touch causal lift, sequence the touches and look
--    at hazard rate per touch (separate analysis).
--
--    Field guide:
--      n_contacts       = count of CRV Action deployments per client per bundle
--      clients          = number of distinct accounts with that exact n_contacts
--      responders       = of those clients, how many ever responded
--      response_rate    = responders / clients (cumulative)
------------------------------------------------------------------------------
WITH crv_action AS (
    SELECT
        acct_no,
        channels_deployed,
        responder
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Action'
),
per_client AS (
    SELECT
        channels_deployed,
        acct_no,
        COUNT(*)       AS n_contacts,
        MAX(responder) AS ever_responded
    FROM crv_action
    GROUP BY channels_deployed, acct_no
)
SELECT
    channels_deployed,
    n_contacts,
    COUNT(*)                                          AS clients,
    SUM(ever_responded)                               AS responders,
    CAST(SUM(ever_responded) AS DECIMAL(12,4))
        / NULLIF(COUNT(*), 0)                         AS response_rate
FROM per_client
GROUP BY channels_deployed, n_contacts
ORDER BY channels_deployed, n_contacts
;

------------------------------------------------------------------------------
-- C) Cumulative contact tracking over time (Action arm only)
--    Goal: trace contact-saturation curve month-by-month. For each
--    deployment event, compute the client's running contact count
--    through that date. Then aggregate per (month, bundle, cumulative count).
--
--    Read:
--      In month M, rows with contacts_to_date = 1  = first-ever contact
--      In month M, rows with contacts_to_date >= 2 = repeat contact
--      Over time, the distribution shifts toward higher contacts_to_date
--      as clients accumulate touches.
--
--    Limit: October 2024 is the data window start. Any contacts before
--    that are invisible — all October rows show contacts_to_date >= 1
--    starting from this window.
--
--    Grain: one row per deployment event (NOT distinct clients per month).
--    A client deployed twice in the same month appears twice with their
--    respective cumulative-count values. To get "% of this month's
--    contacts that were repeats", filter to month M then sum deployments
--    where contacts_to_date >= 2 over total deployments.
------------------------------------------------------------------------------
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
running AS (
    SELECT
        acct_no,
        channels_deployed,
        offer_start_date,
        offer_month,
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
    COUNT(*) AS deployments
FROM running
GROUP BY offer_month, channels_deployed, contacts_to_date
ORDER BY offer_month, channels_deployed, contacts_to_date
;
