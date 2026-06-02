-- ============================================================================
-- Q11 — THROTTLING SCENARIOS (throttle vs. kill)   [CALENDAR-MONTH CAP]
-- Question: if we capped CRV-Action contacts instead of killing CRV on IM,
--   what would it cost (CRV conversions forgone) and free up (PCL suppression lifted)?
--
-- CAP DEFINITION: per CALENDAR-MONTH frequency cap. frequency_cap = N means an account may
--   receive at most N CRV-Action contacts within a calendar month; the (N+1)th and later
--   contact in that month is REMOVED. contact_seq_in_month = the contact's rank within its
--   own (account, calendar-month) -- 1 = first that month.
--   (This REPLACES (a) the lifetime-cumulative cap, which over 20 months was a near-kill, and
--    (b) a true rolling-30-day window, which Teradata can't express as a window frame
--    [RANGE BETWEEN n PRECEDING is unsupported] without a heavy range self-join. Calendar-month
--    is the standard, cheap, defensible "max N per month" policy.)
--
-- TWO statements, two accounting grains (run each):
--   STATEMENT 1 — CRV COST, grain = one CRV-Action contact, per frequency_cap.
--   STATEMENT 2 — PCL BENEFIT, grain = one PCL-mobile lead, per frequency_cap x PCL decile,
--                 BOTH propensity models (long format: decile_model + decile_value).
--
-- A contact is REMOVED under cap N iff contact_seq_in_month > N.
-- A PCL lead is FREED under cap N iff ALL its overlapping Action contacts are removed, i.e.
--   the SMALLEST in-month sequence among its overlapping contacts is still > N
--   (min_overlapping_contact_seq_in_month > N). If any overlapping contact is kept, lead stays suppressed.
--
-- WHY DECILE (Statement 2): Q08 showed cannibalization is top-heavy (deciles 1-3, ~0 by 8-9).
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
        offer_start_date,
        responder AS crv_responder,
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS cal_month
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_action_ranked AS (
    SELECT
        acct_no,
        crv_responder,
        ROW_NUMBER() OVER (PARTITION BY acct_no, cal_month ORDER BY offer_start_date) AS contact_seq_in_month
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
    COUNT(*)                                                                     AS total_action_contacts,
    SUM(CASE WHEN c.contact_seq_in_month > k.frequency_cap THEN 1 ELSE 0 END)    AS action_contacts_removed,
    SUM(CASE WHEN c.contact_seq_in_month > k.frequency_cap AND c.crv_responder = 1
             THEN 1 ELSE 0 END)                                                  AS crv_responders_in_removed_contacts
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
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS cal_month
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
        ROW_NUMBER() OVER (PARTITION BY acct_no, cal_month ORDER BY offer_start_date) AS contact_seq_in_month
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
    -- the SMALLEST in-month sequence among its overlapping contacts (the threshold a cap must clear).
    SELECT
        p.acct_no,
        p.pcl_strt_dt,
        p.new_decile,
        p.decile,
        p.responder_cli,
        MIN(c.contact_seq_in_month) AS min_overlapping_contact_seq_in_month
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
    CAST('new_decile' AS VARCHAR(12))                                                            AS decile_model,
    CAST('ALL' AS VARCHAR(6))                                                                    AS decile_value,
    COUNT(*)                                                                                     AS total_pcl_overlap_leads,
    SUM(CASE WHEN o.min_overlapping_contact_seq_in_month > k.frequency_cap THEN 1 ELSE 0 END)    AS pcl_leads_freed,
    SUM(CASE WHEN o.min_overlapping_contact_seq_in_month > k.frequency_cap AND o.responder_cli = 1
             THEN 1 ELSE 0 END)                                                                  AS pcl_responders_already_in_freed_leads
FROM pcl_overlap_leads o CROSS JOIN caps k
GROUP BY k.frequency_cap

UNION ALL
-- new_decile model: per decile value
SELECT
    k.frequency_cap,
    CAST('new_decile' AS VARCHAR(12)),
    CAST(o.new_decile AS VARCHAR(6)),
    COUNT(*),
    SUM(CASE WHEN o.min_overlapping_contact_seq_in_month > k.frequency_cap THEN 1 ELSE 0 END),
    SUM(CASE WHEN o.min_overlapping_contact_seq_in_month > k.frequency_cap AND o.responder_cli = 1
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
    SUM(CASE WHEN o.min_overlapping_contact_seq_in_month > k.frequency_cap THEN 1 ELSE 0 END),
    SUM(CASE WHEN o.min_overlapping_contact_seq_in_month > k.frequency_cap AND o.responder_cli = 1
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
    SUM(CASE WHEN o.min_overlapping_contact_seq_in_month > k.frequency_cap THEN 1 ELSE 0 END),
    SUM(CASE WHEN o.min_overlapping_contact_seq_in_month > k.frequency_cap AND o.responder_cli = 1
             THEN 1 ELSE 0 END)
FROM pcl_overlap_leads o CROSS JOIN caps k
GROUP BY k.frequency_cap, o.decile

ORDER BY 2, 1, 3
;
