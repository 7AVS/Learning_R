-- 55: MASTER presence vs sent(disposition_cd=1) crosstab, by MNE
-- ENGINE: Teradata-direct (single-source EDW, matches 17/54).
-- PURPOSE: step 2 of the reachability probe. Pack 17 found, for the five Cards MNEs it
-- covered, that clients_in_master == clients_sent row-for-row (§13 of
-- UNSUB_TRACKING_KNOWLEDGE.md) — i.e. "presence in MASTER implies sent". This file tests
-- that pattern bank-wide and, where it breaks, shows what those MASTER-but-not-sent clients
-- experienced instead.
--
-- POPULATION: identical to pack 54 Block 1 — email-decisioned (two-field production rule,
-- §6, canon) since 2024-01-01, NOT-control (same suffix-based ASSUMPTION as pack 54; see
-- pack 54's header and Block 0 profiling queries for the full caveat and evidence — not
-- repeated here to avoid two copies of the same disclaimer drifting apart). Read pack 54
-- first if the ASSUMPTION isn't already familiar.
--
-- GRAIN: one row per MNE (all cohort months and arm codes pooled — this file answers a
-- yes/no reachability question, not a trend question; pack 54 carries the trend). The
-- underlying `flags` CTE in both blocks is one row per (CLNT_NO, TACTIC_ID) — one row per
-- DECISION, not per client, same fix as pack 54: a client decisioned into two tactics of the
-- same MNE is two rows, not collapsed to one. `decisions_*` columns count those rows;
-- `distinct_clients_decisioned` (COUNT DISTINCT CLNT_NO) sits beside each total for the
-- client-level reading.
--
-- FILTERS: same as pack 54 — TREATMT_STRT_DT/disposition_dt_tm >= 2024-01-01, MASTER
-- de-duped to DISTINCT (TREATMENT_ID, CLNT_NO, consumer_id_hashed) with CLNT_NO NOT NULL
-- (§20.2), no junk-TREATMENT_ID filter needed (keys originate from the tactic table, §20.12).
--
-- STRUCTURAL NOTE (not a finding, a fact about how the join is built — flagging so the
-- "not in master but sent" cell isn't mistaken for an empirical result): VENDOR_FEEDBACK_EVENT
-- carries no CLNT_NO, only consumer_id_hashed (§2/§3). The only path from a decisioned CLNT_NO
-- to a consumer_id_hashed is through MASTER. So a client who is NOT in MASTER can never be
-- matched to an EVENT row in this query regardless of what actually happened — that quadrant
-- is zero BY CONSTRUCTION, not because we checked and it came back empty. It's included below
-- for completeness/audit only.
--
-- WHAT GOOD LOOKS LIKE: clients_in_master_not_sent should be small relative to
-- clients_in_master_and_sent for every MNE (reproducing Pack 17's finding at bank-wide scale).
-- If it is small everywhere, "in MASTER" is a safe proxy for "was sent" and future reachability
-- work can key off MASTER alone. If it is large for some MNEs, Block 2 shows why — did those
-- clients bounce, complain, open/click without a captured disposition_cd=1 row, or have zero
-- EVENT rows under that treatment at all (MASTER-only silence)?
-- =============================================================================

-- ===== BLOCK 1: crosstab — in MASTER (Y/N) x sent disposition_cd=1 (Y/N), by MNE =====

WITH em_decis AS (
    SELECT DISTINCT
        t.CLNT_NO,
        t.TACTIC_ID,
        SUBSTR(t.TACTIC_ID, 8, 3) AS mne
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
      AND (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
           OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
      -- ASSUMPTION (see pack 54 header): drop CONTROL_ASSUMED only; UNCLASSIFIED_ASSUMED stays in.
      AND TRIM(t.TST_GRP_CD) NOT LIKE '%C'
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
    -- Grain = (CLNT_NO, TACTIC_ID): one row per decision, not per client.
    SELECT
        d.mne,
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
    GROUP BY 1, 2, 3
)
SELECT
    mne,
    CAST(SUM(CASE WHEN f_in_master = 1 AND f_sent = 1 THEN 1 ELSE 0 END) AS BIGINT) AS decisions_in_master_and_sent,
    CAST(SUM(CASE WHEN f_in_master = 1 AND f_sent = 0 THEN 1 ELSE 0 END) AS BIGINT) AS decisions_in_master_not_sent,
    CAST(SUM(CASE WHEN f_in_master = 0 AND f_sent = 1 THEN 1 ELSE 0 END) AS BIGINT) AS decisions_not_in_master_but_sent_structurally_impossible,
    CAST(SUM(CASE WHEN f_in_master = 0 AND f_sent = 0 THEN 1 ELSE 0 END) AS BIGINT) AS decisions_not_in_master_not_sent,
    CAST(COUNT(*) AS BIGINT)                                                        AS decisions_email_action_total,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT)                                         AS distinct_clients_decisioned
FROM flags
GROUP BY 1
ORDER BY 1;

-- ===== BLOCK 2: disposition_cd distribution for MASTER-yes / sent-no clients, by MNE =====
-- Answers: of the clients_in_master_not_sent group above, what DID happen to them?

WITH em_decis AS (
    SELECT DISTINCT
        t.CLNT_NO,
        t.TACTIC_ID,
        SUBSTR(t.TACTIC_ID, 8, 3) AS mne
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
      AND (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
           OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
      AND TRIM(t.TST_GRP_CD) NOT LIKE '%C'
),
in_master AS (
    SELECT DISTINCT m.TREATMENT_ID, m.CLNT_NO, m.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
    WHERE m.CLNT_NO IS NOT NULL
),
vf_events_all AS (
    -- all disposition codes, same client/treatment population — no disposition_cd filter here
    SELECT DISTINCT e.TREATMENT_ID, e.consumer_id_hashed, e.disposition_cd
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_dt_tm >= DATE '2024-01-01'
),
flags AS (
    -- Grain = (CLNT_NO, TACTIC_ID): one row per decision, not per client.
    SELECT
        d.mne,
        d.CLNT_NO,
        d.TACTIC_ID,
        MAX(CASE WHEN im.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)      AS f_in_master,
        MAX(CASE WHEN ev.disposition_cd = 1 THEN 1 ELSE 0 END)       AS f_sent,
        MAX(CASE WHEN ev.disposition_cd = 2 THEN 1 ELSE 0 END)       AS f_opened,
        MAX(CASE WHEN ev.disposition_cd = 3 THEN 1 ELSE 0 END)       AS f_clicked,
        MAX(CASE WHEN ev.disposition_cd = 4 THEN 1 ELSE 0 END)       AS f_unsub,
        MAX(CASE WHEN ev.disposition_cd = 5 THEN 1 ELSE 0 END)       AS f_hardbounce,
        MAX(CASE WHEN ev.disposition_cd = 6 THEN 1 ELSE 0 END)       AS f_complaint,
        MAX(CASE WHEN ev.disposition_cd IS NOT NULL THEN 1 ELSE 0 END) AS f_any_event_row
    FROM em_decis d
    LEFT JOIN in_master im
        ON  im.TREATMENT_ID = d.TACTIC_ID
        AND im.CLNT_NO      = d.CLNT_NO
    LEFT JOIN vf_events_all ev
        ON  ev.TREATMENT_ID       = d.TACTIC_ID
        AND ev.consumer_id_hashed = im.consumer_id_hashed
    GROUP BY 1, 2, 3
),
master_not_sent AS (
    SELECT * FROM flags WHERE f_in_master = 1 AND f_sent = 0
)
SELECT
    mne,
    CAST(COUNT(*) AS BIGINT)                                                    AS decisions_in_master_without_disposition1,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT)                                     AS distinct_clients_decisioned,
    CAST(SUM(f_opened) AS BIGINT)                                               AS decisions_with_opened_disposition2,
    CAST(SUM(f_clicked) AS BIGINT)                                              AS decisions_with_clicked_disposition3,
    CAST(SUM(f_unsub) AS BIGINT)                                                AS decisions_with_unsub_disposition4,
    CAST(SUM(f_hardbounce) AS BIGINT)                                           AS decisions_with_hardbounce_disposition5,
    CAST(SUM(f_complaint) AS BIGINT)                                            AS decisions_with_complaint_disposition6,
    CAST(SUM(CASE WHEN f_any_event_row = 0 THEN 1 ELSE 0 END) AS BIGINT)        AS decisions_with_zero_event_rows_master_only
FROM master_not_sent
GROUP BY 1
ORDER BY 1;
