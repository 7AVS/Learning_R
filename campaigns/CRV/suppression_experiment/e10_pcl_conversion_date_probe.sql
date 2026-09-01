-- ============================================================================
-- e10 — PCL conversion-field verification + actual_strt_dt sizing
-- Engine: STARBURST/TRINO (LIMIT not TOP; catalog prefix dw00_im).
-- STMT 1 (1 row): proves responder_cli + dt_cl_change carry the conversion —
--   expect responders = with_event_date (airtight property, Q17b v1) and a
--   sane date range. decision_dt is a DEAD column (confirmed NULL) — ignore.
-- STMT 2 (~15 rows): sizes the treatmt_strt_dt vs actual_strt_dt divergence
--   (redeploys?). Off-diagonal cells material -> "in-market during experiment"
--   may need actual_strt_dt in e4/vintage scope; tiny -> keep treatmt_strt_dt.
-- ============================================================================

-- STMT 1 — conversion fields work
SELECT COUNT(*)            AS responders,
       COUNT(dt_cl_change) AS with_event_date,
       MIN(dt_cl_change)   AS min_dt,
       MAX(dt_cl_change)   AS max_dt
FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
WHERE responder_cli = 1
  AND treatmt_strt_dt >= DATE '2026-01-01';

-- STMT 2 — treatmt vs actual start month cross-tab
SELECT
    date_trunc('month', treatmt_strt_dt)  AS treatmt_month,
    date_trunc('month', actual_strt_dt)   AS actual_month,
    COUNT(*)                              AS leads,
    SUM(responder_cli)                    AS responders,
    MAX(actual_strt_dt)                   AS max_actual_dt
FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
WHERE treatmt_strt_dt >= DATE '2026-05-01'
GROUP BY 1, 2
ORDER BY 1, 2;
