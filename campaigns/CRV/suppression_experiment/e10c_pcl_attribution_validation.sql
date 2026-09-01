-- ============================================================================
-- e10c — responder_cli ATTRIBUTION validation (Andre's question 2026-08-31)
-- responder_cli was inherited from the campaign, never window-validated.
-- Decision (ONE read, ~8 rows, one per treatmt month 2026): for responders,
-- where does dt_cl_change fall relative to the offer's ACTUAL in-market date
-- and window end?
--   conv_before_actual  > 0 and large  -> campaign credits conversions that
--     happened BEFORE the client saw the offer (organic misattribution;
--     symmetric across arms, but inflates levels and dilutes gaps)
--   conv_in_window dominant            -> attribution sane, question closed
--   conv_after_end material            -> late-response credit, note and move on
-- Engine: STARBURST/TRINO.
-- ============================================================================

SELECT
    date_trunc('month', treatmt_strt_dt)                        AS treatmt_month,
    COUNT(*)                                                    AS responders,
    SUM(CASE WHEN actual_strt_dt IS NULL THEN 1 ELSE 0 END)     AS actual_dt_null,
    SUM(CASE WHEN dt_cl_change <  actual_strt_dt THEN 1 ELSE 0 END) AS conv_before_actual,
    SUM(CASE WHEN dt_cl_change >= actual_strt_dt
              AND dt_cl_change <= treatmt_end_dt THEN 1 ELSE 0 END) AS conv_in_window,
    SUM(CASE WHEN dt_cl_change >  treatmt_end_dt THEN 1 ELSE 0 END) AS conv_after_end,
    MIN(date_diff('day', actual_strt_dt, dt_cl_change))         AS min_days_vs_actual,
    APPROX_PERCENTILE(CAST(date_diff('day', actual_strt_dt, dt_cl_change) AS DOUBLE), 0.5)
                                                                AS median_days_vs_actual,
    MAX(date_diff('day', actual_strt_dt, dt_cl_change))         AS max_days_vs_actual
FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
WHERE responder_cli = 1
  AND treatmt_strt_dt >= DATE '2026-01-01'
GROUP BY 1
ORDER BY 1
