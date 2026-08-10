-- ============================================================================
-- Where does the VBU CONTROL arm disappear?
-- ENGINE: Teradata-direct
--
-- SYMPTOM: every VBU wave shows Control leads in the thousands but ZERO
-- successes, while Action runs 293-449. A control with literally no product
-- changes over 90 days is not credible.
--
-- HYPOTHESIS: the elig CTE requires
--     a.prod_cd_current = SUBSTR(b.tst_grp_cd, 6, 3)
-- i.e. the client's current product must equal characters 6-8 of tst_grp_cd.
-- Control codes begin with 'C' and may not carry a product code in those
-- positions. If so, no control client ever satisfies the join, so controls get
-- a denominator (counted from `base`) but can never reach acct_changes - their
-- numerator is structurally zero, not empirically zero.
--
-- That would make every VBU lift number wrong.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- BLOCK 1 - what do the codes actually look like, per arm?
-- Shows tst_grp_cd and what SUBSTR(...,6,3) extracts from it.
-- If the Control rows show blanks or non-product junk in from_product_code,
-- the hypothesis is confirmed. Expect ~20-40 rows.
-- ----------------------------------------------------------------------------
SELECT
      CASE WHEN UPPER(SUBSTR(TRIM(tst_grp_cd), 1, 1)) = 'C' THEN 'Control'
           ELSE 'Action' END                                     AS grp
    , TRIM(tst_grp_cd)                                           AS tst_grp_cd
    , SUBSTR(TRIM(tst_grp_cd), 6, 3)                             AS from_product_code
    , COUNT(DISTINCT clnt_no)                                    AS clients
FROM DG6V01.tactic_evnt_ip_ar_hist
WHERE SUBSTR(tactic_id, 8, 3) = 'VBU'
  AND treatmt_strt_dt >= DATE '2026-01-01'
GROUP BY 1, 2, 3
ORDER BY 1, 2
;


-- ----------------------------------------------------------------------------
-- BLOCK 2 - the funnel, by arm. THE DECISIVE ONE. 2 rows.
-- base_clients      = everyone in the population
-- elig_clients      = those surviving the eligibility join
-- changed_clients   = those with a product change in window
--
-- If elig_clients is ~0 for Control while base_clients is ~11,000, the
-- eligibility join is destroying the control arm and every VBU lift computed
-- from it is invalid.
-- ----------------------------------------------------------------------------
WITH base AS (
    SELECT DISTINCT
          E.clnt_no
        , CAST(E.tactic_id AS VARCHAR(50))                       AS tactic_id
        , E.treatmt_strt_dt                                      AS Treat_Start_DT
        , E.treatmt_end_dt                                       AS Treat_End_DT
        , E.addnl_data_dt
        , E.tst_grp_cd
        , CASE WHEN UPPER(SUBSTR(E.tst_grp_cd, 1, 1)) = 'C' THEN 'Control'
               ELSE 'Action' END                                 AS grp
    FROM DG6V01.tactic_evnt_ip_ar_hist E
    WHERE E.treatmt_strt_dt BETWEEN DATE '2025-10-01' AND DATE '2026-08-01'
      AND SUBSTR(E.tactic_id, 8, 3) = 'VBU'
      AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
      AND CAST(E.tactic_id AS VARCHAR(50))
          = SUBSTR(CAST(E.tactic_decisn_vrb_info AS VARCHAR(200)), 1, 10)
),
elig AS (
    SELECT DISTINCT
          b.clnt_no, b.tactic_id, b.Treat_Start_DT, b.Treat_End_DT, b.grp
        , a.acct_no
        , a.prod_cd_current                                      AS prod_before
        , SUBSTR(b.tst_grp_cd, 6, 3)                             AS from_product_code
    FROM base b
    JOIN d3cv12a.cr_crd_rpts_acct a
      ON a.clnt_no = b.clnt_no
     AND a.ME_dt   = LAST_DAY(ADD_MONTHS(b.addnl_data_dt, -1))
     AND (
          (a.prod_cd_current = SUBSTR(b.tst_grp_cd, 6, 3) AND b.tst_grp_cd <> 'XX')
       OR (a.prod_cd_current IN ('C00','C01','C02')       AND b.tst_grp_cd  = 'XX')
         )
     AND a.status = 'OPEN'
),
changed AS (
    SELECT DISTINCT e.clnt_no, e.grp
    FROM elig e
    JOIN D3CV12A.dly_full_portfolio d
      ON d.acct_no = e.acct_no
     AND d.DT_record_ext BETWEEN (e.Treat_Start_DT - INTERVAL '1' DAY)
                             AND (e.Treat_End_DT   + INTERVAL '5' DAY)
     AND d.visa_prod_cd <> e.prod_before
     AND d.visa_prod_cd <> e.from_product_code
)
SELECT
      b.grp
    , COUNT(DISTINCT b.clnt_no)                                  AS base_clients
    , COUNT(DISTINCT e.clnt_no)                                  AS elig_clients
    , COUNT(DISTINCT c.clnt_no)                                  AS changed_clients
FROM base b
LEFT JOIN elig    e ON e.clnt_no = b.clnt_no AND e.grp = b.grp
LEFT JOIN changed c ON c.clnt_no = b.clnt_no AND c.grp = b.grp
GROUP BY 1
ORDER BY 1
;
