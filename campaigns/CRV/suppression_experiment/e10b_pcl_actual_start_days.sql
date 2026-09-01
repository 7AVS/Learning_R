-- ============================================================================
-- e10b — day-level PCL actual_strt_dt tail (sizes the experiment-era leads)
-- e10 STMT 2: ~half of leads actually go to market 1-2 months after their
-- treatmt_strt_dt label; actuals run through Aug 14-19. Decision (ONE read,
-- ~31 rows): HOW MANY leads actually started ON/AFTER go-live (2026-08-14)?
--   Material volume -> e4 + PCL vintage switch scope/anchor to actual_strt_dt
--   and the PCL read starts NOW, not September.
-- Engine: STARBURST/TRINO.
-- ============================================================================

SELECT
    actual_strt_dt,
    date_trunc('month', treatmt_strt_dt)  AS treatmt_month,   -- label cohort it came from
    COUNT(*)                              AS leads,
    SUM(responder_cli)                    AS responders,
    SUM(CASE WHEN dt_cl_change >= DATE '2026-08-14'
             THEN responder_cli ELSE 0 END) AS responders_post_golive
FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
WHERE actual_strt_dt >= DATE '2026-08-01'
GROUP BY 1, 2
ORDER BY 1, 2
