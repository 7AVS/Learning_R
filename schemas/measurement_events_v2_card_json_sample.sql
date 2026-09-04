-- Success Library (measurement_events_v2): one raw JSON sample per CARD event code.
-- Purpose: see what campaign-derived fields event_attributes carries per code, so the
-- catalog can record the per-code JSON contract. 6 card codes -> 6 rows.
-- event_cd is a partition key: the IN list prunes the scan to card partitions only.
SELECT event_cd, event_date, event_name, event_type_cd, amount, event_attributes
FROM (
    SELECT
        event_cd, event_date, event_name, event_type_cd, amount, event_attributes,
        ROW_NUMBER() OVER (PARTITION BY event_cd ORDER BY event_date DESC) AS rn
    FROM edl0_im.prod_zp10_prod_staging.measurement_events_v2
    WHERE event_cd IN ('p_card_actvn', 'p_card_apply', 'p_card_credlmt_inc',
                       'p_card_installmt_purch', 'p_card_open', 'p_card_upgrade')
      AND event_date >= DATE '2026-06-01'
      AND event_attributes IS NOT NULL
) s
WHERE rn = 1
ORDER BY event_cd;
