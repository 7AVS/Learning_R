-- ============================================================================
-- Which elig condition is costing us the 42 missing VBU conversions?
-- ENGINE: Teradata-direct
--
-- FACTS: leads match the dashboard exactly (Action 28,821), conversions do not
-- (ours 429, dashboard 471). 429/471 = 91.1%.
--
-- STRUCTURAL CAUSE: the denominator is COUNT(DISTINCT clnt_no) from `base` -
-- every targeted client. The numerator only counts clients who survive the
-- `elig` join. Any client elig drops keeps their place in the denominator and
-- can never convert. If ~91% of base survives elig, that is the entire gap.
--
-- Neither widening the change window nor disabling the prior-AIB exclusion
-- moved the number, so the loss is upstream of both - it is in elig.
--
-- This measures how many clients each elig condition removes, one at a time.
-- 6 rows.
-- ============================================================================

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
      AND E.treatmt_strt_dt = DATE '2026-04-13'    -- the wave we are reconciling
)

-- 1. the denominator, as the vintage builds it
SELECT CAST('1 base (denominator)'            AS VARCHAR(40)) AS stage
     , COUNT(DISTINCT b.clnt_no)                              AS clients
FROM base b

UNION ALL

-- 2. elig exactly as written: all three conditions
SELECT CAST('2 elig, all conditions'          AS VARCHAR(40)), COUNT(DISTINCT b.clnt_no)
FROM base b
JOIN d3cv12a.cr_crd_rpts_acct a
  ON a.clnt_no = b.clnt_no
 AND a.ME_dt   = LAST_DAY(ADD_MONTHS(b.addnl_data_dt, -1))
 AND ( (a.prod_cd_current = SUBSTR(b.tst_grp_cd, 6, 3) AND b.tst_grp_cd <> 'XX')
    OR (a.prod_cd_current IN ('C00','C01','C02')       AND b.tst_grp_cd  = 'XX') )
 AND a.status = 'OPEN'

UNION ALL

-- 3. drop status = 'OPEN'
SELECT CAST('3 elig, no status filter'        AS VARCHAR(40)), COUNT(DISTINCT b.clnt_no)
FROM base b
JOIN d3cv12a.cr_crd_rpts_acct a
  ON a.clnt_no = b.clnt_no
 AND a.ME_dt   = LAST_DAY(ADD_MONTHS(b.addnl_data_dt, -1))
 AND ( (a.prod_cd_current = SUBSTR(b.tst_grp_cd, 6, 3) AND b.tst_grp_cd <> 'XX')
    OR (a.prod_cd_current IN ('C00','C01','C02')       AND b.tst_grp_cd  = 'XX') )

UNION ALL

-- 4. drop the product match
SELECT CAST('4 elig, no product match'        AS VARCHAR(40)), COUNT(DISTINCT b.clnt_no)
FROM base b
JOIN d3cv12a.cr_crd_rpts_acct a
  ON a.clnt_no = b.clnt_no
 AND a.ME_dt   = LAST_DAY(ADD_MONTHS(b.addnl_data_dt, -1))
 AND a.status  = 'OPEN'

UNION ALL

-- 5. drop the ME_dt snapshot condition (any month-end row for that client)
SELECT CAST('5 elig, no ME_dt condition'      AS VARCHAR(40)), COUNT(DISTINCT b.clnt_no)
FROM base b
JOIN d3cv12a.cr_crd_rpts_acct a
  ON a.clnt_no = b.clnt_no
 AND ( (a.prod_cd_current = SUBSTR(b.tst_grp_cd, 6, 3) AND b.tst_grp_cd <> 'XX')
    OR (a.prod_cd_current IN ('C00','C01','C02')       AND b.tst_grp_cd  = 'XX') )
 AND a.status = 'OPEN'

UNION ALL

-- 6. client simply exists on the account table at all
SELECT CAST('6 exists on cr_crd_rpts_acct'    AS VARCHAR(40)), COUNT(DISTINCT b.clnt_no)
FROM base b
JOIN d3cv12a.cr_crd_rpts_acct a
  ON a.clnt_no = b.clnt_no

ORDER BY 1
;

-- ----------------------------------------------------------------------------
-- HOW TO READ IT
--   Row 1 should be 28,821 (matches the dashboard).
--   Row 2 is what the vintage actually measures. If it is ~91% of row 1, that
--   ratio IS the 429/471 gap and elig is the whole story.
--   Whichever of rows 3, 4 or 5 jumps closest to row 1 names the condition
--   doing the damage - that is the one to relax or move out of the numerator.
-- ----------------------------------------------------------------------------
