-- ============================================================================
-- Q11 — THROTTLING SCENARIOS (throttle vs. kill)
-- Question: if we capped CRV-Action contacts instead of killing CRV on IM,
--   what would it cost (CRV conversions forgone) and free up (PCL suppression lifted)?
--
-- TWO statements, two accounting grains (run each):
--   STATEMENT 1 — CRV COST, grain = one CRV-Action contact, per frequency_cap.
--   STATEMENT 2 — PCL BENEFIT, grain = one PCL-mobile lead, per frequency_cap x PCL decile.
--
-- WHY DECILE (Statement 2): Q08 showed cannibalization is top-heavy — concentrated in PCL
--   propensity deciles 1-3, ~0 by deciles 8-9. So a freed lead's recovery DEPENDS on its
--   decile. Breaking pcl_leads_freed by decile lets you (a) apply the decile-specific gap
--   instead of a misleading flat rate, and (b) see that throttling high-propensity accounts
--   recovers far more PCL per CRV contact cut than a blanket cap.
--   Decile field = new_decile (cv_score model, Q08 Statement 1). Swap to `decile` (model_score,
--   Q08 Statement 2) in the two places marked below to reproduce the older model.
--
-- CAP DEFINITION: lifetime-cumulative. frequency_cap = max CRV-Action contacts an account may
--   receive since 2024-10-01; contact #(cap+1) onward is removed. This is NOT a rolling
--   30/90-day cap (the more realistic exec policy) — that is a separate model, not built here.
--
-- A PCL lead is "freed" under cap N iff the cap removes ALL its overlapping Action contacts,
--   i.e. even its EARLIEST overlapping contact is beyond the cap (earliest_overlapping_contact_seq > N).
--   If any sub-cap contact still overlaps, the lead stays suppressed and is NOT freed.
--
-- RECOVERY IS NOT IN THIS QUERY (counts only). Recovered PCL conversions =
--   pcl_leads_freed[decile] x cannibalization_gap[decile], applied in Excel. The
--   pcl_responders_already_in_freed_leads column is DESCRIPTIVE (baseline converters among
--   freed leads), NOT the recovery.
-- CRV cost (Statement 1) is GROSS; ~28% of overlap-cohort CRV conversions are PCL swaps
--   (Q07) that convert on PCL anyway — net that out in Excel.
-- ============================================================================


-- ============================================================================
-- STATEMENT 1 — CRV COST by frequency_cap (4 rows)
-- ============================================================================
WITH crv_action_ranked AS (
    SELECT
        acct_no,
        responder AS crv_responder,
        ROW_NUMBER() OVER (PARTITION BY acct_no ORDER BY offer_start_date) AS contact_seq_in_account
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
caps AS (
    SELECT 2 AS frequency_cap
    UNION ALL SELECT 3
    UNION ALL SELECT 4
    UNION ALL SELECT 5
)
SELECT
    k.frequency_cap,
    COUNT(*)                                                                       AS total_action_contacts,
    SUM(CASE WHEN c.contact_seq_in_account > k.frequency_cap THEN 1 ELSE 0 END)    AS action_contacts_removed,
    SUM(CASE WHEN c.contact_seq_in_account > k.frequency_cap AND c.crv_responder = 1
             THEN 1 ELSE 0 END)                                                    AS crv_responders_in_removed_contacts
FROM crv_action_ranked c
CROSS JOIN caps k
GROUP BY k.frequency_cap
ORDER BY k.frequency_cap
;


-- ============================================================================
-- STATEMENT 2 — PCL BENEFIT by frequency_cap x PCL decile (+ an 'ALL' overall row)
-- ============================================================================
WITH crv_action_ranked AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date,
        ROW_NUMBER() OVER (PARTITION BY acct_no ORDER BY offer_start_date) AS contact_seq_in_account
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
        new_decile,                                   -- <<< swap to `decile` for the older model
        responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
pcl_overlap_leads AS (
    -- One row per PCL lead that overlaps >= 1 CRV-Action contact, with its earliest
    -- overlapping contact sequence (the threshold that decides whether a cap frees it).
    SELECT
        p.acct_no,
        p.pcl_strt_dt,
        p.new_decile,                                 -- <<< swap to `decile` for the older model
        p.responder_cli,
        MIN(c.contact_seq_in_account) AS earliest_overlapping_contact_seq
    FROM pcl_universe p
    JOIN crv_action_ranked c
      ON c.acct_no          = p.acct_no
     AND c.offer_start_date <= p.pcl_end_dt
     AND c.offer_end_date   >= p.pcl_strt_dt
    GROUP BY p.acct_no, p.pcl_strt_dt, p.new_decile, p.responder_cli
),
caps AS (
    SELECT 2 AS frequency_cap
    UNION ALL SELECT 3
    UNION ALL SELECT 4
    UNION ALL SELECT 5
)
-- Overall (all deciles pooled)
SELECT
    k.frequency_cap,
    CAST('ALL' AS VARCHAR(6))                                                              AS pcl_new_decile,
    COUNT(*)                                                                               AS total_pcl_overlap_leads,
    SUM(CASE WHEN o.earliest_overlapping_contact_seq > k.frequency_cap THEN 1 ELSE 0 END)  AS pcl_leads_freed,
    SUM(CASE WHEN o.earliest_overlapping_contact_seq > k.frequency_cap AND o.responder_cli = 1
             THEN 1 ELSE 0 END)                                                            AS pcl_responders_already_in_freed_leads
FROM pcl_overlap_leads o
CROSS JOIN caps k
GROUP BY k.frequency_cap

UNION ALL

-- Per PCL decile
SELECT
    k.frequency_cap,
    CAST(o.new_decile AS VARCHAR(6))                                                       AS pcl_new_decile,
    COUNT(*)                                                                               AS total_pcl_overlap_leads,
    SUM(CASE WHEN o.earliest_overlapping_contact_seq > k.frequency_cap THEN 1 ELSE 0 END)  AS pcl_leads_freed,
    SUM(CASE WHEN o.earliest_overlapping_contact_seq > k.frequency_cap AND o.responder_cli = 1
             THEN 1 ELSE 0 END)                                                            AS pcl_responders_already_in_freed_leads
FROM pcl_overlap_leads o
CROSS JOIN caps k
GROUP BY k.frequency_cap, o.new_decile

ORDER BY 1, 2
;
