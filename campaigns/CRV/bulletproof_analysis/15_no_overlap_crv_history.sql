-- ============================================================================
-- Q15 — Who are the no_overlap PCL converters?  (RESTING / COOLDOWN test)
-- Hypothesis: "no_overlap" PCL leads are not truly never-CRV — they are CRV-experienced
--   clients in a cooldown, so no CRV offer is ACTIVE during the PCL window and they don't
--   register as overlapping. If so, the ~21% no_overlap baseline is contaminated by rested
--   clients, not pure selection.
--
-- Same two tables as the base query (cards_pli_decision_resp MB + cards_crv_install_decis_resp).
-- The only change: for each no_overlap lead we re-look-up the account's FULL CRV history with
--   NO channel filter, NO arm filter, and an EXTENDED lookback (offer_start >= 2024-01-01) so we
--   catch CRV that ENDED BEFORE the PCL window. Channel is not a differentiator (all get the
--   mobile banner; the 'IM' label is a deployment-settings fluke), so history spans all arms.
--
-- Classes (priority): prior_crv (a CRV offer ended before the PCL window = candidate cooldown),
--   later_crv (CRV only after the window), never_crv (no CRV record at all).
-- days_since_crv = pcl_strt_dt − nearest prior CRV offer_end_date (the cooldown gap; if it
--   clusters around a value, that value IS the cooldown length).
-- ============================================================================
WITH pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt AS pcl_strt_dt, treatmt_end_dt AS pcl_end_dt, responder_cli,
        treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS pcl_month
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
-- the no_overlap group, exactly as the base query defines it
no_overlap AS (
    SELECT p.acct_no, p.pcl_strt_dt, p.pcl_end_dt, p.pcl_month, p.responder_cli
    FROM pcl_universe p
    LEFT JOIN oa_keys oa ON oa.acct_no=p.acct_no AND oa.pcl_strt_dt=p.pcl_strt_dt AND oa.pcl_end_dt=p.pcl_end_dt
    LEFT JOIN oc_keys oc ON oc.acct_no=p.acct_no AND oc.pcl_strt_dt=p.pcl_strt_dt AND oc.pcl_end_dt=p.pcl_end_dt
    WHERE oa.acct_no IS NULL AND oc.acct_no IS NULL
),
-- full CRV history per account, all arms/channels, extended lookback
crv_history AS (
    SELECT acct_no, offer_start_date, offer_end_date, responder
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-01-01'    -- <<< lookback for resting; push earlier if data exists
),
lead_class AS (
    SELECT
        n.pcl_month, n.responder_cli,
        MAX(CASE WHEN h.offer_end_date < n.pcl_strt_dt THEN h.offer_end_date END)               AS nearest_prior_end,
        MAX(CASE WHEN h.offer_end_date < n.pcl_strt_dt AND h.responder = 1 THEN 1 ELSE 0 END)   AS prior_converted,
        MAX(CASE WHEN h.offer_start_date > n.pcl_end_dt THEN 1 ELSE 0 END)                       AS has_later,
        n.pcl_strt_dt
    FROM no_overlap n
    LEFT JOIN crv_history h ON h.acct_no = n.acct_no
    GROUP BY n.acct_no, n.pcl_strt_dt, n.pcl_end_dt, n.pcl_month, n.responder_cli
),
classified AS (
    SELECT
        pcl_month, responder_cli, prior_converted,
        CASE WHEN nearest_prior_end IS NOT NULL THEN 'prior_crv'
             WHEN has_later = 1                 THEN 'later_crv'
             ELSE 'never_crv' END                                  AS crv_hist_class,
        CASE WHEN nearest_prior_end IS NOT NULL THEN pcl_strt_dt - nearest_prior_end END AS days_since_crv
    FROM lead_class
)
-- overall
SELECT
    CAST('overall' AS VARCHAR(20))                                 AS pcl_month,
    crv_hist_class,
    COUNT(*)                                                       AS n_leads,
    SUM(responder_cli)                                             AS n_responders,
    CAST(SUM(responder_cli) AS DECIMAL(12,4)) / NULLIF(COUNT(*),0) AS response_rate,
    SUM(prior_converted)                                          AS n_prior_crv_converted,
    AVG(CAST(days_since_crv AS DECIMAL(12,2)))                     AS avg_days_since_crv,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY days_since_crv)   AS p50_days_since_crv
FROM classified
GROUP BY crv_hist_class

UNION ALL
-- per PCL month
SELECT
    CAST(pcl_month AS VARCHAR(20)),
    crv_hist_class,
    COUNT(*),
    SUM(responder_cli),
    CAST(SUM(responder_cli) AS DECIMAL(12,4)) / NULLIF(COUNT(*),0),
    SUM(prior_converted),
    AVG(CAST(days_since_crv AS DECIMAL(12,2))),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY days_since_crv)
FROM classified
GROUP BY pcl_month, crv_hist_class

ORDER BY 1, 2
;
