-- 54: Email-decisioned send funnel, all campaigns (MNE x cohort_month x arm)
-- ENGINE: Teradata-direct (single-source EDW, no EDL table — matches sibling packs 05/16/17).
-- PURPOSE: step 1 of the reachability probe — of everyone decisioned into the email channel
-- and NOT in a control cell, how many ever show up in the vendor send chain, and how many
-- were actually sent. This is bank-wide (every MNE on the tactic table), unlike 17 which
-- scoped to the five Cards MNEs.
--
-- GRAIN:
--   The underlying `flags` CTE in every block is one row per (CLNT_NO, TACTIC_ID) — one row
--   per DECISION, not per client. A client decisioned into two tactics of the same MNE in the
--   same month is two rows. `decisions_*` count columns are COUNT(*)/SUM(...) over that
--   decision grain; `distinct_clients_decisioned` is COUNT(DISTINCT CLNT_NO) alongside it so
--   both readings are available without silently collapsing multi-tactic clients.
--   Block 0 (two queries): profiling only — distinct TST_GRP_CD values and a per-MNE
--     classification summary. Read this BEFORE trusting Block 1/2's arm split.
--   Block 1: one row per (mne, cohort_yyyymm, raw test_group_code), restricted to the
--     NOT-control population, plus one CAST-matched GRAND_TOTAL row via UNION ALL.
--   Block 2: one row per (mne, arm_group_for_validation) collapsed across all months —
--     the same funnel by MNE only, with a CONTROL_ASSUMED row per MNE alongside the
--     NON_CONTROL_ASSUMED row so the control row's sent count can be eyeballed against
--     its decisioned count.
--
-- FILTERS:
--   TREATMT_STRT_DT >= 2024-01-01 (hard floor, feedback_2024_data_floor — TACTIC_EVNT_IP_AR_HIST
--     and VENDOR_FEEDBACK_EVENT are both CPU-kill risks unwindowed).
--   Email-decisioned = documented two-field production rule (UNSUB_TRACKING_KNOWLEDGE.md §6,
--     validated 2026-07-15 across 184 MNEs — NOT an assumption, this is canon):
--       SUBSTR(TACTIC_DECISN_VRB_INFO,121,30) LIKE '%EM%'  (55 MNEs, priority 1)
--       OR UPPER(COALESCE(ADDNL_DECISN_DATA1,'')) LIKE '%EM%'  (129 MNEs, priority 2)
--     KNOWN GAP (documented, not something this file fixes): 10 MNEs signal EM through a
--     different field (VRB_INFO slot 101, TACTIC_CELL_CD, TREATMT_MN — HPO/OII/OTC/RMG/SLC/
--     VRE/WWC/HPE/ZFE/REM) and are invisible to this filter; 73 MNEs in vendor feedback have
--     zero tactic rows in this window and can't be denominatored from the tactic side at all.
--     Both gaps undercount the true email-decisioned universe, never overcount it.
--   MASTER de-duped to DISTINCT (TREATMENT_ID, CLNT_NO, consumer_id_hashed) with CLNT_NO NOT
--     NULL — raw MASTER rows are ~1.123x inflated by card-level duplication (§20.2).
--   No junk TREATMENT_ID filter needed: every key here originates from
--     TACTIC_EVNT_IP_AR_HIST, which never contains vendor residue ids (DEFAULT/CABVRSN1 have
--     zero tactic-table rows, §20.12) — the join can't pick them up regardless of direction.
--
-- ASSUMPTION (the one thing NOT confirmed — flag loudly before using Block 1/2 numbers):
--   No universal Action/Control lookup exists for TST_GRP_CD across MNEs
--   (reference_cards_no_cell_code_lookup.md — Cards runs GTM, not ADTM, no cell dictionary).
--   Only two campaign-specific conventions are validated anywhere in this repo: AUH ('_C'
--   suffix = Control, Robin Ji email 2026-05-14, itself only a "working assumption") and PCD
--   ('%T'/'%C' suffix, with an unmapped OTHER bucket, Andre 2026-05-26). Applying a suffix
--   rule to EVERY MNE on the tactic table is an extrapolation of a 2-campaign pattern to
--   ~300+ campaigns and is explicitly the kind of thing feedback_no_invented_arm_labels.md
--   forbids doing silently. Working rule used below, labeled at every step so it is never
--   mistaken for a confirmed fact:
--     TRIM(TST_GRP_CD) LIKE '%C'  -> CONTROL_ASSUMED
--     TRIM(TST_GRP_CD) LIKE '%T'  -> ACTION_ASSUMED
--     otherwise                  -> UNCLASSIFIED_ASSUMED (kept IN the non-control population,
--                                    never silently dropped or silently counted as action)
--   Block 1's raw test_group_code column and Block 0's profiling queries exist specifically
--   so this guess can be checked/overridden per MNE before the numbers are trusted.
--
-- WHAT GOOD LOOKS LIKE:
--   Block 2's CONTROL_ASSUMED row for a given MNE should show decisions_sent_disposition1 near
--   zero relative to decisions_email_action (control cells should not receive the email).
--   A MNE where CONTROL_ASSUMED shows substantial sends means the suffix guess mis-tagged that
--   MNE's control cell (or that MNE has no control on this tactic) — do not trust
--   NON_CONTROL_ASSUMED numbers for that MNE without checking Block 0's profiling rows for it.
--   Sent/decisioned should land somewhere near Pack 17's 91-98% (Cards, 5 MNEs) as a loose
--   sanity anchor for the NON_CONTROL_ASSUMED population bank-wide, not as a hard bar.
-- =============================================================================

-- ===== BLOCK 0a: PROFILING — top 10 TST_GRP_CD codes by client volume, bank-wide =====
-- Read this first. Confirms (or breaks) the suffix guess against real codes and real volume.
SELECT TOP 10
    SUBSTR(t.TACTIC_ID, 8, 3)                        AS mne,
    TRIM(t.TST_GRP_CD)                                AS test_group_code,
    CASE WHEN TRIM(t.TST_GRP_CD) LIKE '%C' THEN 'CONTROL_ASSUMED'
         WHEN TRIM(t.TST_GRP_CD) LIKE '%T' THEN 'ACTION_ASSUMED'
         ELSE 'UNCLASSIFIED_ASSUMED' END              AS suffix_rule_guess,
    CAST(COUNT(DISTINCT t.CLNT_NO) AS BIGINT)         AS distinct_clients_email_decisioned
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
  AND (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
       OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
GROUP BY 1, 2, 3
ORDER BY distinct_clients_email_decisioned DESC;

-- ===== BLOCK 0b: PROFILING — per-MNE view of how much volume the suffix guess can't classify =====
-- Added beyond the top-10 list above because a volume-ranked top 10 can hide an MNE where the
-- guess fails 100% of the time if that MNE's own client count doesn't crack the top 10 codes
-- bank-wide. This shows every MNE at once, worst-unclassified-share first.
WITH tst_grp_by_mne AS (
    SELECT
        SUBSTR(t.TACTIC_ID, 8, 3)                        AS mne,
        TRIM(t.TST_GRP_CD)                                AS test_group_code,
        CASE WHEN TRIM(t.TST_GRP_CD) LIKE '%C' THEN 'CONTROL_ASSUMED'
             WHEN TRIM(t.TST_GRP_CD) LIKE '%T' THEN 'ACTION_ASSUMED'
             ELSE 'UNCLASSIFIED_ASSUMED' END              AS suffix_rule_guess,
        t.CLNT_NO
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
      AND (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
           OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
)
SELECT
    mne,
    CAST(COUNT(DISTINCT test_group_code) AS BIGINT)                                              AS distinct_test_group_codes_seen,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT)                                                       AS distinct_clients_total,
    CAST(COUNT(DISTINCT CASE WHEN suffix_rule_guess = 'UNCLASSIFIED_ASSUMED' THEN CLNT_NO END) AS BIGINT) AS distinct_clients_unclassified_by_guess,
    CAST(COUNT(DISTINCT CASE WHEN suffix_rule_guess = 'ACTION_ASSUMED' THEN CLNT_NO END) AS BIGINT)       AS distinct_clients_action_assumed,
    CAST(COUNT(DISTINCT CASE WHEN suffix_rule_guess = 'CONTROL_ASSUMED' THEN CLNT_NO END) AS BIGINT)      AS distinct_clients_control_assumed
FROM tst_grp_by_mne
GROUP BY 1
ORDER BY distinct_clients_unclassified_by_guess DESC;

-- ===== BLOCK 1: send funnel, MNE x cohort_month x raw arm code (NOT-control population) =====

WITH em_decis AS (
    SELECT DISTINCT
        t.CLNT_NO,
        t.TACTIC_ID,
        SUBSTR(t.TACTIC_ID, 8, 3)                        AS mne,
        TRIM(t.TST_GRP_CD)                                AS test_group_code,
        EXTRACT(YEAR FROM t.TREATMT_STRT_DT) * 100
          + EXTRACT(MONTH FROM t.TREATMT_STRT_DT)         AS cohort_yyyymm,
        CASE WHEN TRIM(t.TST_GRP_CD) LIKE '%C' THEN 'CONTROL_ASSUMED'
             WHEN TRIM(t.TST_GRP_CD) LIKE '%T' THEN 'ACTION_ASSUMED'
             ELSE 'UNCLASSIFIED_ASSUMED' END              AS arm_classification_assumed
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
      AND (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
           OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
),
action_population AS (
    -- ASSUMPTION filter: drop CONTROL_ASSUMED only. UNCLASSIFIED_ASSUMED stays in and stays
    -- labeled — never silently folded into action.
    SELECT * FROM em_decis WHERE arm_classification_assumed <> 'CONTROL_ASSUMED'
),
in_master AS (
    SELECT DISTINCT m.TREATMENT_ID, m.CLNT_NO, m.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
    WHERE m.CLNT_NO IS NOT NULL
),
sent_events AS (
    SELECT DISTINCT e.TREATMENT_ID, e.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_dt_tm >= DATE '2024-01-01'
      AND e.disposition_cd = 1
),
flags AS (
    -- Grain = (CLNT_NO, TACTIC_ID): one row per decision. A client decisioned twice in the
    -- same MNE/month (two different TACTIC_IDs) is two rows here, not collapsed to one.
    SELECT
        d.mne,
        d.cohort_yyyymm,
        d.test_group_code,
        d.arm_classification_assumed,
        d.CLNT_NO,
        d.TACTIC_ID,
        MAX(CASE WHEN im.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)            AS f_in_master,
        MAX(CASE WHEN se.consumer_id_hashed IS NOT NULL THEN 1 ELSE 0 END) AS f_sent
    FROM action_population d
    LEFT JOIN in_master im
        ON  im.TREATMENT_ID = d.TACTIC_ID
        AND im.CLNT_NO      = d.CLNT_NO
    LEFT JOIN sent_events se
        ON  se.TREATMENT_ID       = d.TACTIC_ID
        AND se.consumer_id_hashed = im.consumer_id_hashed
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
    CAST(mne AS VARCHAR(20))                        AS mne,
    cohort_yyyymm,
    CAST(test_group_code AS VARCHAR(30))            AS arm_label_raw_test_group_code,
    CAST(arm_classification_assumed AS VARCHAR(30)) AS arm_classification_assumed,
    CAST('DETAIL' AS VARCHAR(40))                   AS row_type,
    CAST(COUNT(*) AS BIGINT)                        AS decisions_email_action,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT)         AS distinct_clients_decisioned,
    CAST(SUM(f_in_master) AS BIGINT)                AS decisions_present_in_vendor_master,
    CAST(SUM(f_sent) AS BIGINT)                     AS decisions_sent_disposition1
FROM flags
GROUP BY 1, 2, 3, 4
UNION ALL
SELECT
    CAST('ALL_MNE' AS VARCHAR(20))                  AS mne,
    CAST(NULL AS INTEGER)                           AS cohort_yyyymm,
    CAST('ALL_ARMS' AS VARCHAR(30))                 AS arm_label_raw_test_group_code,
    CAST('ACTION_PLUS_UNCLASSIFIED' AS VARCHAR(30)) AS arm_classification_assumed,
    CAST('GRAND_TOTAL' AS VARCHAR(40))              AS row_type,
    CAST(COUNT(*) AS BIGINT)                        AS decisions_email_action,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT)         AS distinct_clients_decisioned,
    CAST(SUM(f_in_master) AS BIGINT)                AS decisions_present_in_vendor_master,
    CAST(SUM(f_sent) AS BIGINT)                     AS decisions_sent_disposition1
FROM flags
ORDER BY 5, 1, 2, 3, 4;

-- ===== BLOCK 2: same funnel by MNE only, action row vs control row side by side =====
-- Purpose: sanity-check the ASSUMPTION above. CONTROL_ASSUMED's sent count should be near
-- zero for a given MNE; if it isn't, that MNE's suffix classification is wrong.

WITH em_decis AS (
    SELECT DISTINCT
        t.CLNT_NO,
        t.TACTIC_ID,
        SUBSTR(t.TACTIC_ID, 8, 3)                        AS mne,
        CASE WHEN TRIM(t.TST_GRP_CD) LIKE '%C' THEN 'CONTROL_ASSUMED'
             ELSE 'NON_CONTROL_ASSUMED' END               AS arm_group_for_validation
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
      AND (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
           OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
),
in_master AS (
    SELECT DISTINCT m.TREATMENT_ID, m.CLNT_NO, m.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
    WHERE m.CLNT_NO IS NOT NULL
),
sent_events AS (
    SELECT DISTINCT e.TREATMENT_ID, e.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_dt_tm >= DATE '2024-01-01'
      AND e.disposition_cd = 1
),
flags AS (
    -- Grain = (CLNT_NO, TACTIC_ID): one row per decision, same fix as Block 1.
    SELECT
        d.mne,
        d.arm_group_for_validation,
        d.CLNT_NO,
        d.TACTIC_ID,
        MAX(CASE WHEN im.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)            AS f_in_master,
        MAX(CASE WHEN se.consumer_id_hashed IS NOT NULL THEN 1 ELSE 0 END) AS f_sent
    FROM em_decis d
    LEFT JOIN in_master im
        ON  im.TREATMENT_ID = d.TACTIC_ID
        AND im.CLNT_NO      = d.CLNT_NO
    LEFT JOIN sent_events se
        ON  se.TREATMENT_ID       = d.TACTIC_ID
        AND se.consumer_id_hashed = im.consumer_id_hashed
    GROUP BY 1, 2, 3, 4
)
SELECT
    mne,
    arm_group_for_validation,
    CAST(COUNT(*) AS BIGINT)                AS decisions_email_action,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS distinct_clients_decisioned,
    CAST(SUM(f_in_master) AS BIGINT)        AS decisions_present_in_vendor_master,
    CAST(SUM(f_sent) AS BIGINT)             AS decisions_sent_disposition1
FROM flags
GROUP BY 1, 2
ORDER BY 1, 2;
