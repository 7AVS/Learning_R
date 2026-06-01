-- ============================================================================
-- Q11 — THROTTLING SCENARIOS (throttle vs. kill)
-- Question: if we capped CRV-Action contacts instead of killing CRV on IM,
--   what would it cost (CRV conversions forgone) and free up (PCL suppression lifted)?
--
-- Two separate accounting grains, joined on the cap value:
--   (A) CRV COST  — grain = one CRV-Action contact. "Removed" = beyond the cap.
--   (B) PCL BENEFIT — grain = one PCL-mobile lead. A lead is "freed" only if the cap
--       removes ALL of its overlapping Action contacts (i.e. even the EARLIEST one is
--       beyond the cap). If any sub-cap contact still overlaps, the lead is NOT freed.
--
-- CAP DEFINITION: lifetime-cumulative. frequency_cap = the max number of CRV-Action
--   contacts an account may receive since 2024-10-01; contact #(cap+1) onward is removed.
--   (NOT a rolling 30/90-day cap — that is a different, more realistic model not built here.)
--
-- RECOVERY IS NOT IN THIS QUERY. Throttling does not "recover" leads that already
--   converted. Recovered PCL conversions = pcl_leads_freed x cannibalization_gap, where
--   gap ~= 1.0pp (Q04 / Q12, flat across overlap length). Apply that multiply in Excel.
--   The pcl_responders_already_in_freed_leads column below is DESCRIPTIVE ONLY (baseline
--   converters among freed leads), it is NOT the recovery figure.
--
-- CRV COST IS GROSS. crv_responders_in_removed_contacts counts all forgone CRV
--   conversions; ~28% of overlap-cohort CRV conversions are PCL swaps (Q07) that would
--   convert on PCL anyway — net that out in Excel for true cost.
-- ============================================================================

WITH crv_action_ranked AS (
    -- Every CRV-Action (IM) contact since 2024-10-01, numbered chronologically per account.
    -- contact_seq_in_account = the Nth lifetime contact for this account (1 = first).
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date,
        responder AS crv_responder,
        ROW_NUMBER() OVER (PARTITION BY acct_no ORDER BY offer_start_date) AS contact_seq_in_account
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
pcl_universe AS (
    -- PCL-mobile deployments (one row per acct x PCL wave).
    SELECT
        acct_no,
        treatmt_strt_dt AS pcl_strt_dt,
        treatmt_end_dt  AS pcl_end_dt,
        responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
pcl_overlap_leads AS (
    -- One row per PCL lead that overlaps >= 1 CRV-Action contact.
    -- earliest_overlapping_contact_seq = the LOWEST contact sequence number among the
    -- Action contacts that overlap this PCL window. A cap of N frees this lead iff that
    -- earliest overlapping contact is itself beyond the cap (earliest_seq > N) -> all of
    -- its overlapping contacts are removed.
    SELECT
        p.acct_no,
        p.pcl_strt_dt,
        p.responder_cli,
        MIN(c.contact_seq_in_account) AS earliest_overlapping_contact_seq
    FROM pcl_universe p
    JOIN crv_action_ranked c
      ON c.acct_no          = p.acct_no
     AND c.offer_start_date <= p.pcl_end_dt
     AND c.offer_end_date   >= p.pcl_strt_dt
    GROUP BY p.acct_no, p.pcl_strt_dt, p.responder_cli
),
caps AS (
    SELECT 2 AS frequency_cap
    UNION ALL SELECT 3
    UNION ALL SELECT 4
    UNION ALL SELECT 5
),
-- (A) CRV COST side — counted over CRV-Action contacts.
crv_cost AS (
    SELECT
        k.frequency_cap,
        COUNT(*)                                                                              AS total_action_contacts,
        SUM(CASE WHEN c.contact_seq_in_account >  k.frequency_cap THEN 1 ELSE 0 END)          AS action_contacts_removed,
        SUM(CASE WHEN c.contact_seq_in_account >  k.frequency_cap AND c.crv_responder = 1
                 THEN 1 ELSE 0 END)                                                           AS crv_responders_in_removed_contacts
    FROM crv_action_ranked c
    CROSS JOIN caps k
    GROUP BY k.frequency_cap
),
-- (B) PCL BENEFIT side — counted over PCL overlap leads.
pcl_benefit AS (
    SELECT
        k.frequency_cap,
        COUNT(*)                                                                              AS total_pcl_overlap_leads,
        SUM(CASE WHEN o.earliest_overlapping_contact_seq > k.frequency_cap THEN 1 ELSE 0 END) AS pcl_leads_freed,
        SUM(CASE WHEN o.earliest_overlapping_contact_seq > k.frequency_cap AND o.responder_cli = 1
                 THEN 1 ELSE 0 END)                                                           AS pcl_responders_already_in_freed_leads
    FROM pcl_overlap_leads o
    CROSS JOIN caps k
    GROUP BY k.frequency_cap
)
SELECT
    a.frequency_cap,
    a.total_action_contacts,
    a.action_contacts_removed,
    a.crv_responders_in_removed_contacts,
    b.total_pcl_overlap_leads,
    b.pcl_leads_freed,
    b.pcl_responders_already_in_freed_leads
FROM crv_cost a
JOIN pcl_benefit b
  ON a.frequency_cap = b.frequency_cap
ORDER BY a.frequency_cap
;
