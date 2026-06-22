-- s8_funnel_sequence_check.sql
-- Verify the green-banner FUNNEL ORDER that installments_lineage.html currently ASSUMES.
-- We have per-event counts; we have NOT confirmed the same clients move
--   impression -> click -> start -> activation in timestamp order. This checks it.
-- (Same-client linkage is via up_srf_id2_value; all events are narrow, "credit card installments".)
-- ENGINE: Starburst/Trino.

-- ============================================================
-- STMT 1 — Sample: dump the ordered event stream for clients who ACTIVATED a plan
-- ============================================================
-- Read the order with your eyes: does 'view - ... eligible transaction' come before the taps,
-- and do the taps come before 'plan activation success'? Or are taps alternative entries?
WITH activators AS (
    SELECT DISTINCT up_srf_id2_value AS clnt
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
    WHERE year = '2026' AND month = '06'
      AND event_name = 'tap'
      AND LOWER(ep_details) LIKE '%credit card installments - plan activation success%'
    LIMIT 5
)
SELECT
    n.up_srf_id2_value AS clnt,
    n.ep_ga_session_id,
    n.event_timestamp,
    n.event_name,
    n.ep_details
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow n
JOIN activators a ON n.up_srf_id2_value = a.clnt
WHERE n.year = '2026' AND n.month = '06'
  AND LOWER(n.ep_details) LIKE '%credit card installments%'
ORDER BY n.up_srf_id2_value, n.event_timestamp
LIMIT 120
;

-- ============================================================
-- STMT 2 — At scale: do activators actually have an impression + tap BEFORE activating?
-- ============================================================
-- If most activations are preceded by an impression and a tap (timestamp order), the funnel
-- holds. If many activate with NO prior impression, the assumed order is wrong.
WITH ev AS (
    SELECT
        up_srf_id2_value AS clnt,
        MIN(CASE WHEN LOWER(ep_details) = 'view - credit card installments - eligible transaction'
                 THEN event_timestamp END)                                   AS first_impr,
        MIN(CASE WHEN event_name = 'tap'
                  AND LOWER(ep_details) LIKE 'tap - credit card installments - %'
                  AND LOWER(ep_details) NOT LIKE '%plan activation success%'
                 THEN event_timestamp END)                                   AS first_tap,
        MIN(CASE WHEN LOWER(ep_details) LIKE '%plan activation success%'
                 THEN event_timestamp END)                                   AS first_activation
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
    WHERE year = '2026' AND month = '06'
      AND LOWER(ep_details) LIKE '%credit card installments%'
    GROUP BY 1
)
SELECT
    COUNT(*)                                                                   AS clients_any_installments,
    COUNT(first_activation)                                                    AS clients_activated,
    SUM(CASE WHEN first_activation IS NOT NULL AND first_impr IS NOT NULL
              AND first_impr <= first_activation THEN 1 ELSE 0 END)            AS activated_with_prior_impression,
    SUM(CASE WHEN first_activation IS NOT NULL AND first_tap IS NOT NULL
              AND first_tap  <= first_activation THEN 1 ELSE 0 END)            AS activated_with_prior_tap
FROM ev
;
