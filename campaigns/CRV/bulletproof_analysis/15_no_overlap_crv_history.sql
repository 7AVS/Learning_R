-- ============================================================================
-- Q15 — Who are the no_overlap PCL converters?  (RESTING / COOLDOWN test)
-- Hypothesis: "no_overlap" PCL leads are not truly never-CRV — they are CRV-experienced
--   clients in a cooldown, so no CRV offer is ACTIVE during the PCL window. If so, the ~21%
--   no_overlap baseline is contaminated by rested clients, not pure selection.
--
-- Same two tables as the base query. CRV history is taken across ALL arms/channels with an
--   EXTENDED lookback, but PRE-AGGREGATED to one row per account first (min/max offer dates,
--   ever-converted) so the join to no_overlap is 1-to-1 — no fanout, no spool blow-up.
--   Channel is not a differentiator (all get the mobile banner; 'IM' label is a deploy fluke).
--
-- Classes (priority): prior_crv (a CRV offer ended before the PCL window = candidate cooldown),
--   later_crv (all CRV starts after the window), never_crv (no CRV record), other_crv (edge).
-- days_since_crv = pcl_strt_dt − latest CRV offer_end, defined only when ALL of the account's
--   CRV ended before the window (clean rested case). If it clusters, that value is the cooldown.
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
no_overlap AS (
    SELECT p.acct_no, p.pcl_strt_dt, p.pcl_end_dt, p.pcl_month, p.responder_cli
    FROM pcl_universe p
    LEFT JOIN oa_keys oa ON oa.acct_no=p.acct_no AND oa.pcl_strt_dt=p.pcl_strt_dt AND oa.pcl_end_dt=p.pcl_end_dt
    LEFT JOIN oc_keys oc ON oc.acct_no=p.acct_no AND oc.pcl_strt_dt=p.pcl_strt_dt AND oc.pcl_end_dt=p.pcl_end_dt
    WHERE oa.acct_no IS NULL AND oc.acct_no IS NULL
),
-- ONE row per account: collapses CRV history so the join below is 1-to-1 (kills the fanout)
crv_summary AS (
    SELECT
        acct_no,
        MIN(offer_start_date) AS min_start,
        MIN(offer_end_date)   AS min_end,
        MAX(offer_end_date)   AS max_end,
        MAX(responder)        AS ever_conv
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-01-01'    -- <<< lookback for resting; push earlier if data exists
    GROUP BY acct_no
),
classified AS (
    SELECT
        n.pcl_month, n.responder_cli, s.ever_conv,
        CASE WHEN s.acct_no IS NULL            THEN 'never_crv'
             WHEN s.min_end   < n.pcl_strt_dt  THEN 'prior_crv'
             WHEN s.min_start > n.pcl_end_dt   THEN 'later_crv'
             ELSE 'other_crv' END                                  AS crv_hist_class,
        CASE WHEN s.max_end < n.pcl_strt_dt THEN n.pcl_strt_dt - s.max_end END AS days_since_crv
    FROM no_overlap n
    LEFT JOIN crv_summary s ON s.acct_no = n.acct_no
)
-- overall
SELECT
    CAST('overall' AS VARCHAR(20))                                 AS pcl_month,
    crv_hist_class,
    COUNT(*)                                                       AS n_leads,
    SUM(responder_cli)                                             AS n_responders,
    CAST(SUM(responder_cli) AS DECIMAL(12,4)) / NULLIF(COUNT(*),0) AS response_rate,
    SUM(ever_conv)                                                AS n_ever_converted_crv,
    AVG(CAST(days_since_crv AS DECIMAL(12,2)))                     AS avg_days_since_crv
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
    SUM(ever_conv),
    AVG(CAST(days_since_crv AS DECIMAL(12,2)))
FROM classified
GROUP BY pcl_month, crv_hist_class

ORDER BY 1, 2
;
