-- ============================================================================
-- Which raw event code means "product change / upgrade"?
-- ENGINE: Teradata-direct   TABLE: D3CV12A.CR_CRD_ACCT_EVNT_DLY
--
-- WHY: v2 currently detects success by diffing daily snapshots of
-- D3CV12A.dly_full_portfolio and stamping it with DT_record_ext - the snapshot
-- EXTRACT date, not the date the product actually changed. That can miss a
-- change (snapshot gap) and invent one (a code that flickers), which is exactly
-- the pattern seen: Action short by 32, Control over by 6.
--
-- Andre: raw data only, no curated table. So the change date must come from the
-- raw event table. AUH uses dtl_evnt_typ_cd = 191 AND ADD_RELTN_CD = 3 for an
-- authorized-user add; the product-change equivalent is not documented anywhere
-- in the repo and will NOT be guessed.
--
-- METHOD: take VBU clients we already know changed to AIB, and look at what
-- event codes actually fire for them around that change. The code that shows up
-- for nearly all of them is the one.
-- ============================================================================

WITH base AS (
    SELECT DISTINCT
          E.clnt_no
        , CAST(E.tactic_id AS VARCHAR(50))                       AS tactic_id
        , E.treatmt_strt_dt                                      AS Treat_Start_DT
        , E.treatmt_end_dt                                       AS Treat_End_DT
        , E.addnl_data_dt
        , E.tst_grp_cd
    FROM DG6V01.tactic_evnt_ip_ar_hist E
    WHERE E.treatmt_strt_dt = DATE '2026-04-13'
      AND SUBSTR(E.tactic_id, 8, 3) = 'VBU'
      AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
      AND CAST(E.tactic_id AS VARCHAR(50))
          = SUBSTR(CAST(E.tactic_decisn_vrb_info AS VARCHAR(200)), 1, 10)
),
-- the accounts, no product gate (that gate dropped half the base)
acct AS (
    SELECT DISTINCT
          b.clnt_no, b.Treat_Start_DT, b.Treat_End_DT
        , a.acct_no
        , a.prod_cd_current AS prod_before
    FROM base b
    JOIN d3cv12a.cr_crd_rpts_acct a
      ON a.clnt_no = b.clnt_no
     AND a.ME_dt   = LAST_DAY(ADD_MONTHS(b.addnl_data_dt, -1))
     AND a.status  = 'OPEN'
),
-- accounts the snapshot says became AIB in the window - our current definition
converted AS (
    SELECT DISTINCT ac.clnt_no, ac.acct_no, ac.Treat_Start_DT
    FROM acct ac
    JOIN D3CV12A.dly_full_portfolio d
      ON d.acct_no = ac.acct_no
     AND d.DT_record_ext BETWEEN ac.Treat_Start_DT AND (ac.Treat_Start_DT + INTERVAL '90' DAY)
     AND d.visa_prod_cd = 'AIB'
     AND d.visa_prod_cd <> ac.prod_before
)

-- what raw events fire for those accounts in the same window?
SELECT
      e.dtl_evnt_typ_cd
    , COUNT(DISTINCT e.acct_no)                                  AS accts_with_event
    , COUNT(*)                                                   AS events
    , MIN(e.evnt_dt)                                             AS first_evnt_dt
    , MAX(e.evnt_dt)                                             AS last_evnt_dt
FROM converted c
JOIN D3CV12A.CR_CRD_ACCT_EVNT_DLY e
  ON e.acct_no = c.acct_no
 AND e.evnt_dt BETWEEN c.Treat_Start_DT AND (c.Treat_Start_DT + INTERVAL '90' DAY)
GROUP BY 1
ORDER BY accts_with_event DESC
;

-- ----------------------------------------------------------------------------
-- HOW TO READ IT
--   The top row by accts_with_event is the product-change event code - it should
--   cover most or all of the converted accounts. Codes appearing on only a
--   handful are unrelated activity (payments, statements, maintenance).
--   Once identified, v2 stops diffing snapshots and reads evnt_dt from this
--   table instead - a real event date, no curated table anywhere.
-- ----------------------------------------------------------------------------
