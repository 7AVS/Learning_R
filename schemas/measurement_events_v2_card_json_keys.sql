-- Success Library (measurement_events_v2): full JSON key set per CARD event code.
-- Purpose: the 1-row sample (2026-09-04) showed a flat, code-specific JSON; credlmt_inc was
-- truncated and p_card_actvn returned nothing. This lists every top-level key per code with
-- row counts and date range, and counts rows whose event_attributes is NULL as '(null_attributes)'.
-- Expected ~12 rows.
SELECT
    event_cd,
    k                   AS json_key,
    COUNT(*)            AS row_ct,
    MIN(event_date)     AS first_event_date,
    MAX(event_date)     AS last_event_date
FROM edl0_im.prod_zp10_prod_staging.measurement_events_v2
CROSS JOIN UNNEST(
    CASE WHEN event_attributes IS NULL THEN ARRAY['(null_attributes)']
         ELSE map_keys(CAST(json_parse(event_attributes) AS MAP(VARCHAR, JSON)))
    END) AS t(k)
WHERE event_cd IN ('p_card_actvn', 'p_card_apply', 'p_card_credlmt_inc',
                   'p_card_installmt_purch', 'p_card_open', 'p_card_upgrade')
  AND event_date >= DATE '2026-06-01'
GROUP BY 1, 2
ORDER BY 1, 2;
