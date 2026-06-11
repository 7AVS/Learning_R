-- ============================================================================
-- ENGINE: Teradata-direct (no EDL table) — Teradata syntax.
-- Q27 — Q04 CANNIBALIZATION GAP + CI, EXCLUDING CO-APPLICANT ACCOUNTS (conservative)
-- Same population/logic as Q04, minus every account with a DISTINCT co-applicant
-- (CIDM CLNT_NO_A present and <> CLNT_NO — the Q25b-validated flag; the IDENTICAL_IND
-- alone is not the right filter). no_cidm_match accounts stay in (not known co-app).
-- Expected from Q25 arithmetic: gap ~1.04pp (vs ~1.02 all-in) — this formalizes the CI.
-- CIDM confirmed 1 row per acct (Q25b: rows_ = accts), so no dedup needed.
-- ============================================================================
WITH coapp_accts AS (
    SELECT acct_no
    FROM DTZTAU.CIDM_CARDS_ACCT_ATTRS
    WHERE CLNT_NO_A IS NOT NULL
      AND CLNT_NO_A <> CLNT_NO
),
pcl_universe AS (
    SELECT p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt, p.responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp p
    LEFT JOIN coapp_accts x
      ON x.acct_no = p.acct_no
    WHERE p.treatmt_strt_dt >= DATE '2024-10-01'
      AND p.channel LIKE '%MB%'
      AND x.acct_no IS NULL
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
overlap_action_keys AS (
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
arm_leads AS (
    SELECT
        p.responder_cli,
        CASE WHEN oa.acct_no IS NOT NULL THEN 'action'
             WHEN oc.acct_no IS NOT NULL THEN 'control'
             ELSE 'none' END AS arm
    FROM pcl_universe p
    LEFT JOIN overlap_action_keys oa
      ON oa.acct_no = p.acct_no AND oa.treatmt_strt_dt = p.treatmt_strt_dt
    LEFT JOIN overlap_control_keys oc
      ON oc.acct_no = p.acct_no AND oc.treatmt_strt_dt = p.treatmt_strt_dt
),
agg AS (
    SELECT
        CAST(SUM(CASE WHEN arm = 'action'  THEN 1 ELSE 0 END) AS FLOAT) AS n_action,
        CAST(SUM(CASE WHEN arm = 'action'  THEN responder_cli ELSE 0 END) AS FLOAT) AS resp_action,
        CAST(SUM(CASE WHEN arm = 'control' THEN 1 ELSE 0 END) AS FLOAT) AS n_control,
        CAST(SUM(CASE WHEN arm = 'control' THEN responder_cli ELSE 0 END) AS FLOAT) AS resp_control
    FROM arm_leads
    WHERE arm <> 'none'
)
SELECT
    n_action,
    resp_action,
    n_control,
    resp_control,
    resp_action  / n_action  AS p_action,
    resp_control / n_control AS p_control,
    (resp_control / n_control - resp_action / n_action) AS gap,
    SQRT( (resp_action / n_action) * (CAST(1 AS FLOAT) - resp_action / n_action) / n_action
        + (resp_control / n_control) * (CAST(1 AS FLOAT) - resp_control / n_control) / n_control ) AS se,
    (resp_control / n_control - resp_action / n_action)
      - CAST(1.96 AS FLOAT) * SQRT( (resp_action / n_action) * (CAST(1 AS FLOAT) - resp_action / n_action) / n_action
        + (resp_control / n_control) * (CAST(1 AS FLOAT) - resp_control / n_control) / n_control ) AS ci_lower,
    (resp_control / n_control - resp_action / n_action)
      + CAST(1.96 AS FLOAT) * SQRT( (resp_action / n_action) * (CAST(1 AS FLOAT) - resp_action / n_action) / n_action
        + (resp_control / n_control) * (CAST(1 AS FLOAT) - resp_control / n_control) / n_control ) AS ci_upper,
    (resp_control / n_control - resp_action / n_action)
      / SQRT( (resp_action / n_action) * (CAST(1 AS FLOAT) - resp_action / n_action) / n_action
        + (resp_control / n_control) * (CAST(1 AS FLOAT) - resp_control / n_control) / n_control ) AS z_stat
FROM agg;
