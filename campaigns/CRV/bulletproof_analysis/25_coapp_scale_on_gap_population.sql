-- ============================================================================
-- ENGINE: Teradata-direct (no EDL table) — Teradata syntax.
-- Q25 — CO-APPLICANT SCALE ON THE Q04 GAP POPULATION (overlap leads, by arm)
-- Question: how much of the overlap that produced the 1.08pp gap sits on accounts
-- that HAVE a distinct co-applicant (where CRV may have contacted a different
-- human than PCL)? Empirical ceiling on contamination — no CRV spec needed.
-- Population = Q04 verbatim (PCL mobile %MB%, Oct-2024+, lead grain, EXISTS flags).
-- Co-app source: DTZTAU.CIDM_CARDS_ACCT_ATTRS — CLNT_NO (primary) / CLNT_NO_A (co-app).
-- has_coapp derived as CLNT_NO_A present and <> CLNT_NO; PRIMARY_COAPP_IDENTICAL_IND
-- carried RAW (decode unverified — learn values from output, don't assume Y/N).
-- responders included per cell so the gap can be recomputed excluding co-app accounts.
-- NOTE: CRV decis_resp has NO clnt_no — classification is account-existence + PCL-side
-- match only. CIDM grain unverified: deduped to latest LOAD_DT per acct; check
-- leads totals reconcile with Q04 before reading shares.
-- ============================================================================
WITH pcl_universe AS (
    SELECT acct_no, clnt_no, treatmt_strt_dt, treatmt_end_dt, responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
overlap_action_keys AS (   -- Teradata: no EXISTS inside CASE — semi-join keys + LEFT JOIN instead
    SELECT DISTINCT p.acct_no, p.treatmt_strt_dt
    FROM pcl_universe p
    INNER JOIN crv_action ca
      ON ca.acct_no = p.acct_no
     AND ca.offer_start_date <= p.treatmt_end_dt
     AND ca.offer_end_date   >= p.treatmt_strt_dt
),
overlap_control_keys AS (
    SELECT DISTINCT p.acct_no, p.treatmt_strt_dt
    FROM pcl_universe p
    INNER JOIN crv_control cc
      ON cc.acct_no = p.acct_no
     AND cc.offer_start_date <= p.treatmt_end_dt
     AND cc.offer_end_date   >= p.treatmt_strt_dt
),
overlap_leads AS (
    SELECT p.acct_no, p.clnt_no, p.treatmt_strt_dt, p.responder_cli,
           CASE WHEN oa.acct_no IS NOT NULL THEN 'overlap_action'
                ELSE 'overlap_control' END AS arm
    FROM pcl_universe p
    LEFT JOIN overlap_action_keys oa
      ON oa.acct_no = p.acct_no AND oa.treatmt_strt_dt = p.treatmt_strt_dt
    LEFT JOIN overlap_control_keys oc
      ON oc.acct_no = p.acct_no AND oc.treatmt_strt_dt = p.treatmt_strt_dt
    WHERE oa.acct_no IS NOT NULL OR oc.acct_no IS NOT NULL
),
cidm AS (
    SELECT acct_no, CLNT_NO AS cidm_primary, CLNT_NO_A AS cidm_coapp, PRIMARY_COAPP_IDENTICAL_IND,
           ROW_NUMBER() OVER (PARTITION BY acct_no ORDER BY LOAD_DT DESC) AS rn
    FROM DTZTAU.CIDM_CARDS_ACCT_ATTRS
)
SELECT
    l.arm,
    c.PRIMARY_COAPP_IDENTICAL_IND,
    CASE WHEN c.acct_no IS NULL                                            THEN 'no_cidm_match'
         WHEN c.cidm_coapp IS NOT NULL AND c.cidm_coapp <> c.cidm_primary  THEN 'has_coapp'
         ELSE 'no_coapp' END                                               AS coapp_status,
    COUNT(*)                                                               AS leads,
    COUNT(DISTINCT l.acct_no)                                              AS accts,
    SUM(l.responder_cli)                                                   AS responders,
    SUM(CASE WHEN l.clnt_no = c.cidm_primary THEN 1 ELSE 0 END)            AS pcl_clnt_is_primary,
    SUM(CASE WHEN l.clnt_no = c.cidm_coapp   THEN 1 ELSE 0 END)            AS pcl_clnt_is_coapp
FROM overlap_leads l
LEFT JOIN cidm c
    ON c.acct_no = l.acct_no AND c.rn = 1
GROUP BY 1, 2, 3
ORDER BY 1, 3, 2;


-- Q25b: CIDM profile — do primary and co-applicant carry DIFFERENT client numbers,
-- and what does PRIMARY_COAPP_IDENTICAL_IND actually encode? (whole-table profile,
-- validates the premise + learns the indicator decode empirically)
SELECT
    PRIMARY_COAPP_IDENTICAL_IND,
    CASE WHEN CLNT_NO_A IS NULL      THEN 'coapp_null'
         WHEN CLNT_NO_A = CLNT_NO    THEN 'same_clnt_no'
         ELSE                             'different_clnt_no' END AS clnt_no_compare,
    COUNT(*)                 AS rows_,
    COUNT(DISTINCT acct_no)  AS accts
FROM DTZTAU.CIDM_CARDS_ACCT_ATTRS
GROUP BY 1, 2
ORDER BY 1, 2;
