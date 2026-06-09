-- 22_both_category_drilldown.sql
--
-- Drill-down / sanity check for the "Both" category: show ACTUAL clients who viewed BOTH the CRV
-- banner and the PCL banner inside their PCL deployment window, and list their raw banner events
-- with dates — so we can eyeball that a single client genuinely interacted with both, and WHEN
-- relative to the deployment window.
--
-- WINDOW = the deployment's own window: treatmt_strt_dt .. treatmt_end_dt (~90-day deployment life).
-- "Both" = >=1 CRV view AND >=1 PCL view inside that window.
-- Banner key = it_promotion_id (Excel Id). Table = _reduced. Join up_srf_id2_value = CLNT_NO.
-- Sample of 20 clients. Inspection only — read the timeline per client.

WITH
pcl AS (
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt BETWEEN DATE '2026-02-01' AND DATE '2026-04-30'
      AND channel LIKE '%MB%'
),
ga4 AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        event_date,
        event_name,
        it_promotion_id,
        CASE WHEN it_promotion_id IN ('87348','87342','87343','87344') THEN 'CRV'
             WHEN it_promotion_id IN ('156764','156788','162326','289661','289662','289664','289665','289666') THEN 'PCL'
        END AS banner
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE event_date >= DATE '2026-02-01'
      AND it_promotion_id IN ('87348','87342','87343','87344',
                              '156764','156788','162326','289661','289662','289664','289665','289666')
),
-- every banner event that lands inside a client's deployment window
client_events AS (
    SELECT
        p.clnt_no, p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt,
        g.event_date, g.banner, g.event_name, g.it_promotion_id
    FROM pcl p
    JOIN ga4 g
      ON g.clnt_no    = p.clnt_no
     AND g.event_date BETWEEN p.treatmt_strt_dt AND p.treatmt_end_dt
),
-- clients who VIEWED both banners in-window = the "Both" view category
both_clients AS (
    SELECT clnt_no
    FROM client_events
    WHERE event_name = 'view_item'
    GROUP BY clnt_no
    HAVING MAX(CASE WHEN banner = 'CRV' THEN 1 ELSE 0 END) = 1
       AND MAX(CASE WHEN banner = 'PCL' THEN 1 ELSE 0 END) = 1
),
sample_clients AS (
    SELECT clnt_no FROM both_clients ORDER BY clnt_no LIMIT 20
)
SELECT
    ce.clnt_no,
    ce.acct_no,
    ce.treatmt_strt_dt,                 -- window start
    ce.treatmt_end_dt,                  -- window end
    ce.event_date,                      -- when the interaction happened (inside the window)
    ce.banner,                          -- CRV or PCL
    ce.event_name,                      -- view_item (impression) / select_promotion (click)
    ce.it_promotion_id
FROM client_events ce
JOIN sample_clients s ON s.clnt_no = ce.clnt_no
ORDER BY ce.clnt_no, ce.event_date, ce.banner, ce.event_name
;
