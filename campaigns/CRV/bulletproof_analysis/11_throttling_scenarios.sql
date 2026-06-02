-- ============================================================================
-- Q11 — THROTTLING SCENARIOS (throttle vs. kill)   [ROLLING 30-DAY CAP]
-- Question: if we capped CRV-Action contacts instead of killing CRV on IM,
--   what would it cost (CRV conversions forgone) and free up (PCL suppression lifted)?
--
-- CAP DEFINITION: ROLLING 30-day frequency cap. frequency_cap = N means an account may
--   receive at most N CRV-Action contacts within any trailing 30 days; the (N+1)th and later
--   contact inside a 30-day window is REMOVED. rolling_contacts_30d counts each contact's
--   position within its own trailing-30-day window (1 = first in window).
--   (This REPLACES the earlier lifetime-cumulative cap, which over 20 months approximated a
--    near-kill — a lifetime cap of 2 freed 57% of overlap leads. Rolling = realistic throttle.
--    Window length is 30 days here; change "30" in both ranked CTEs to model 60/90 days.)
--
-- TWO statements, two accounting grains (run each):
--   STATEMENT 1 — CRV COST, grain = one CRV-Action contact, per frequency_cap.
--   STATEMENT 2 — PCL BENEFIT, grain = one PCL-mobile lead, per frequency_cap x PCL decile,
--                 BOTH propensity models (long format: decile_model + decile_value).
--
-- A contact is REMOVED under cap N iff rolling_contacts_30d > N.
-- A PCL lead is FREED under cap N iff ALL its overlapping Action contacts are removed, i.e.
--   the SMALLEST rolling count among its overlapping contacts is still > N
--   (min_overlapping_rolling_contacts > N). If any overlapping contact is kept, lead stays suppressed.
--
-- WHY DECILE (Statement 2): Q08 showed cannibalization is top-heavy — concentrated in PCL
--   propensity deciles 1-3, ~0 by 8-9. Recovery depends on a freed lead's decile.
--   Recovered PCL = pcl_leads_freed[decile] x cannibalization_gap[decile] (gap from Q08), in Excel.
--   pcl_responders_already_in_freed_leads is DESCRIPTIVE (already-converted), NOT recovery.
-- CRV cost (Statement 1) is GROSS; ~28% of overlap CRV conversions are PCL swaps (Q07) — net in Excel.
-- ============================================================================


-- ============================================================================
-- STATEMENT 1 — CRV COST by frequency_cap (4 rows)
-- ============================================================================
WITH crv_action_raw AS (
    SELECT
        acct_no,
        responder AS crv_responder,
        (offer_start_date - DATE '2024-10-01') AS day_num
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_action_ranked AS (
    SELECT
        acct_no,
        crv_responder,
        -- how many CRV-Action contacts fell in this contact's trailing 30 days (incl. itself)
        COUNT(*) OVER (PARTITION BY acct_no ORDER BY day_num
                       RANGE BETWEEN 30 PRECEDING AND CURRENT ROW) AS rolling_contacts_30d
    FROM crv_action_raw
),
caps AS (
    -- frequency_cap values {2,3,4,5} as rows. Teradata rejects a bare "SELECT 2 UNION ALL SELECT 3..."
    -- (err 3888: each UNION branch must reference a table), so we generate 4 rows off a real CTE.
    SELECT rn + 1 AS frequency_cap
    FROM (
        SELECT ROW_NUMBER() OVER (ORDER BY acct_no) AS rn
        FROM crv_action_ranked
        QUALIFY ROW_NUMBER() OVER (ORDER BY acct_no) <= 4
    ) g
)
SELECT
    k.frequency_cap,
    COUNT(*)                                                                    AS total_action_contacts,
    SUM(CASE WHEN c.rolling_contacts_30d > k.frequency_cap THEN 1 ELSE 0 END)   AS action_contacts_removed,
    SUM(CASE WHEN c.rolling_contacts_30d > k.frequency_cap AND c.crv_responder = 1
             THEN 1 ELSE 0 END)                                                 AS crv_responders_in_removed_contacts
FROM crv_action_ranked c
CROSS JOIN caps k
GROUP BY k.frequency_cap
ORDER BY k.frequency_cap
;


-- ============================================================================
-- STATEMENT 2 — PCL BENEFIT by frequency_cap x decile, BOTH models (long format)
-- ============================================================================
WITH crv_action_raw AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date,
        (offer_start_date - DATE '2024-10-01') AS day_num
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_action_ranked AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date,
        COUNT(*) OVER (PARTITION BY acct_no ORDER BY day_num
                       RANGE BETWEEN 30 PRECEDING AND CURRENT ROW) AS rolling_contacts_30d
    FROM crv_action_raw
),
pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt AS pcl_strt_dt,
        treatmt_end_dt  AS pcl_end_dt,
        new_decile,
        decile,
        responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
pcl_overlap_leads AS (
    -- One row per PCL lead overlapping >= 1 CRV-Action contact, carrying both decile models and
    -- the SMALLEST rolling-30d count among its overlapping contacts (the threshold a cap must clear).
    SELECT
        p.acct_no,
        p.pcl_strt_dt,
        p.new_decile,
        p.decile,
        p.responder_cli,
        MIN(c.rolling_contacts_30d) AS min_overlapping_rolling_contacts
    FROM pcl_universe p
    JOIN crv_action_ranked c
      ON c.acct_no          = p.acct_no
     AND c.offer_start_date <= p.pcl_end_dt
     AND c.offer_end_date   >= p.pcl_strt_dt
    GROUP BY p.acct_no, p.pcl_strt_dt, p.new_decile, p.decile, p.responder_cli
),
caps AS (
    SELECT rn + 1 AS frequency_cap
    FROM (
        SELECT ROW_NUMBER() OVER (ORDER BY acct_no) AS rn
        FROM crv_action_ranked
        QUALIFY ROW_NUMBER() OVER (ORDER BY acct_no) <= 4
    ) g
)
-- new_decile model: overall
SELECT
    k.frequency_cap,
    CAST('new_decile' AS VARCHAR(12))                                                       AS decile_model,
    CAST('ALL' AS VARCHAR(6))                                                               AS decile_value,
    COUNT(*)                                                                                AS total_pcl_overlap_leads,
    SUM(CASE WHEN o.min_overlapping_rolling_contacts > k.frequency_cap THEN 1 ELSE 0 END)   AS pcl_leads_freed,
    SUM(CASE WHEN o.min_overlapping_rolling_contacts > k.frequency_cap AND o.responder_cli = 1
             THEN 1 ELSE 0 END)                                                             AS pcl_responders_already_in_freed_leads
FROM pcl_overlap_leads o CROSS JOIN caps k
GROUP BY k.frequency_cap

UNION ALL
-- new_decile model: per decile value
SELECT
    k.frequency_cap,
    CAST('new_decile' AS VARCHAR(12)),
    CAST(o.new_decile AS VARCHAR(6)),
    COUNT(*),
    SUM(CASE WHEN o.min_overlapping_rolling_contacts > k.frequency_cap THEN 1 ELSE 0 END),
    SUM(CASE WHEN o.min_overlapping_rolling_contacts > k.frequency_cap AND o.responder_cli = 1
             THEN 1 ELSE 0 END)
FROM pcl_overlap_leads o CROSS JOIN caps k
GROUP BY k.frequency_cap, o.new_decile

UNION ALL
-- decile model: overall
SELECT
    k.frequency_cap,
    CAST('decile' AS VARCHAR(12)),
    CAST('ALL' AS VARCHAR(6)),
    COUNT(*),
    SUM(CASE WHEN o.min_overlapping_rolling_contacts > k.frequency_cap THEN 1 ELSE 0 END),
    SUM(CASE WHEN o.min_overlapping_rolling_contacts > k.frequency_cap AND o.responder_cli = 1
             THEN 1 ELSE 0 END)
FROM pcl_overlap_leads o CROSS JOIN caps k
GROUP BY k.frequency_cap

UNION ALL
-- decile model: per decile value
SELECT
    k.frequency_cap,
    CAST('decile' AS VARCHAR(12)),
    CAST(o.decile AS VARCHAR(6)),
    COUNT(*),
    SUM(CASE WHEN o.min_overlapping_rolling_contacts > k.frequency_cap THEN 1 ELSE 0 END),
    SUM(CASE WHEN o.min_overlapping_rolling_contacts > k.frequency_cap AND o.responder_cli = 1
             THEN 1 ELSE 0 END)
FROM pcl_overlap_leads o CROSS JOIN caps k
GROUP BY k.frequency_cap, o.decile

ORDER BY 2, 1, 3
;
