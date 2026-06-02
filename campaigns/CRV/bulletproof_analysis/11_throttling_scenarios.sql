-- ============================================================================
-- Q11 — THROTTLING SCENARIOS (throttle vs. kill)   [DURATION / TENURE CAP]
-- Question: if we limited how long we keep showing CRV to a customer (instead of killing it),
--   what would it cost (CRV conversions forgone) and free up (PCL suppression lifted)?
--
-- CAP DEFINITION: per-customer DURATION cap. max_crv_touches = N means we keep a customer's
--   FIRST N CRV-Action touches and drop every touch after that. crv_touch_number = the running
--   count of CRV-Action touches for that customer since 2024-10-01 (1 = their first ever).
--   CRV runs ~1 touch / customer / month, so "first N touches" ~= "first N months of CRV per
--   customer." This is a DURATION/tenure cap, NOT a per-month frequency cap.
--   (We tested a per-calendar-month frequency cap and it removed NOTHING — customers get ~1 CRV
--    touch/month, so nothing is ever bunched enough to trim. Frequency is not the lever; duration is.)
--
-- TWO statements, two accounting grains (run each):
--   STATEMENT 1 — CRV COST, grain = one CRV-Action touch, per max_crv_touches.
--   STATEMENT 2 — PCL BENEFIT, grain = one PCL-mobile lead, per max_crv_touches x PCL decile,
--                 BOTH propensity models (long format: decile_model + decile_value).
--
-- A touch is REMOVED under cap N iff crv_touch_number > N (it's past the customer's first N).
-- A PCL lead is FREED under cap N iff ALL its overlapping Action touches are removed, i.e. even
--   the EARLIEST overlapping touch is past the cap (min_overlapping_touch_number > N). If any
--   sub-cap touch still overlaps, the lead stays suppressed.
--
-- WHY DECILE (Statement 2): Q08 showed cannibalization is top-heavy (deciles 1-3, ~0 by 8-9).
--   Recovered PCL = pcl_leads_freed[decile] x cannibalization_gap[decile] (gap from Q08), in Excel.
--   pcl_responders_already_in_freed_leads is DESCRIPTIVE (already-converted), NOT recovery.
-- CRV cost (Statement 1) is GROSS; ~28% of overlap CRV conversions are PCL swaps (Q07) — net in Excel.
-- ============================================================================


-- ============================================================================
-- STATEMENT 1 — CRV COST by max_crv_touches (4 rows)
-- ============================================================================
WITH crv_action_ranked AS (
    SELECT
        acct_no,
        responder AS crv_responder,
        ROW_NUMBER() OVER (PARTITION BY acct_no ORDER BY offer_start_date) AS crv_touch_number
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
caps AS (
    -- max_crv_touches values {2,3,4,5} as rows. Teradata rejects a bare "SELECT 2 UNION ALL SELECT 3..."
    -- (err 3888: each UNION branch must reference a table), so we generate 4 rows off a real CTE.
    SELECT rn + 1 AS max_crv_touches
    FROM (
        SELECT ROW_NUMBER() OVER (ORDER BY acct_no) AS rn
        FROM crv_action_ranked
        QUALIFY ROW_NUMBER() OVER (ORDER BY acct_no) <= 4
    ) g
)
SELECT
    k.max_crv_touches,
    COUNT(*)                                                                   AS total_action_touches,
    SUM(CASE WHEN c.crv_touch_number > k.max_crv_touches THEN 1 ELSE 0 END)    AS action_touches_removed,
    SUM(CASE WHEN c.crv_touch_number > k.max_crv_touches AND c.crv_responder = 1
             THEN 1 ELSE 0 END)                                                AS crv_responders_in_removed_touches
FROM crv_action_ranked c
CROSS JOIN caps k
GROUP BY k.max_crv_touches
ORDER BY k.max_crv_touches
;


-- ============================================================================
-- STATEMENT 2 — PCL BENEFIT by max_crv_touches x decile, BOTH models (long format)
-- ============================================================================
WITH crv_action_ranked AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date,
        ROW_NUMBER() OVER (PARTITION BY acct_no ORDER BY offer_start_date) AS crv_touch_number
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
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
    -- One row per PCL lead overlapping >= 1 CRV-Action touch, carrying both decile models and the
    -- EARLIEST overlapping touch number (the cap threshold a lead must clear to be freed).
    SELECT
        p.acct_no,
        p.pcl_strt_dt,
        p.new_decile,
        p.decile,
        p.responder_cli,
        MIN(c.crv_touch_number) AS min_overlapping_touch_number
    FROM pcl_universe p
    JOIN crv_action_ranked c
      ON c.acct_no          = p.acct_no
     AND c.offer_start_date <= p.pcl_end_dt
     AND c.offer_end_date   >= p.pcl_strt_dt
    GROUP BY p.acct_no, p.pcl_strt_dt, p.new_decile, p.decile, p.responder_cli
),
caps AS (
    SELECT rn + 1 AS max_crv_touches
    FROM (
        SELECT ROW_NUMBER() OVER (ORDER BY acct_no) AS rn
        FROM crv_action_ranked
        QUALIFY ROW_NUMBER() OVER (ORDER BY acct_no) <= 4
    ) g
)
-- new_decile model: overall
SELECT
    k.max_crv_touches,
    CAST('new_decile' AS VARCHAR(12))                                                      AS decile_model,
    CAST('ALL' AS VARCHAR(6))                                                              AS decile_value,
    COUNT(*)                                                                               AS total_pcl_overlap_leads,
    SUM(CASE WHEN o.min_overlapping_touch_number > k.max_crv_touches THEN 1 ELSE 0 END)    AS pcl_leads_freed,
    SUM(CASE WHEN o.min_overlapping_touch_number > k.max_crv_touches AND o.responder_cli = 1
             THEN 1 ELSE 0 END)                                                            AS pcl_responders_already_in_freed_leads
FROM pcl_overlap_leads o CROSS JOIN caps k
GROUP BY k.max_crv_touches

UNION ALL
-- new_decile model: per decile value
SELECT
    k.max_crv_touches,
    CAST('new_decile' AS VARCHAR(12)),
    CAST(o.new_decile AS VARCHAR(6)),
    COUNT(*),
    SUM(CASE WHEN o.min_overlapping_touch_number > k.max_crv_touches THEN 1 ELSE 0 END),
    SUM(CASE WHEN o.min_overlapping_touch_number > k.max_crv_touches AND o.responder_cli = 1
             THEN 1 ELSE 0 END)
FROM pcl_overlap_leads o CROSS JOIN caps k
GROUP BY k.max_crv_touches, o.new_decile

UNION ALL
-- decile model: overall
SELECT
    k.max_crv_touches,
    CAST('decile' AS VARCHAR(12)),
    CAST('ALL' AS VARCHAR(6)),
    COUNT(*),
    SUM(CASE WHEN o.min_overlapping_touch_number > k.max_crv_touches THEN 1 ELSE 0 END),
    SUM(CASE WHEN o.min_overlapping_touch_number > k.max_crv_touches AND o.responder_cli = 1
             THEN 1 ELSE 0 END)
FROM pcl_overlap_leads o CROSS JOIN caps k
GROUP BY k.max_crv_touches

UNION ALL
-- decile model: per decile value
SELECT
    k.max_crv_touches,
    CAST('decile' AS VARCHAR(12)),
    CAST(o.decile AS VARCHAR(6)),
    COUNT(*),
    SUM(CASE WHEN o.min_overlapping_touch_number > k.max_crv_touches THEN 1 ELSE 0 END),
    SUM(CASE WHEN o.min_overlapping_touch_number > k.max_crv_touches AND o.responder_cli = 1
             THEN 1 ELSE 0 END)
FROM pcl_overlap_leads o CROSS JOIN caps k
GROUP BY k.max_crv_touches, o.decile

ORDER BY 2, 1, 3
;
