-- c4: demand-shaping retrospective — does the banner CHANGE purchase behavior, or only harvest it?
-- Andre's question: clients who know installments exist may make larger purchases. If true, cutting
-- dormant cells costs future SPEND, not just installment conversions — the coverage curve is blind
-- to that. Test: Action vs Control (borrowed randomization, ITT) on POST-deployment eligible-txn
-- behavior. If dormant Action leads cross the $250 line at the same rate as dormant Control leads,
-- the banner is not shaping demand there and the cut is safe on both mandates (conversions + spend).
--
-- Design:
--   * Cohorts Sep-2025..Mar-2026 (matches c1). Outcome window = days 1..90 AFTER offer_start.
--   * Outcome = ELIGIBLE transactions (>=250 recipe) post-deployment: leads with >=1, txn count, $ sum.
--     (Total all-txn spend and DFP ADB deliberately excluded — spool; add later on a 1% sample if needed.)
--   * Segments: pre-deployment elig-30d (0 vs 1+) x prior contacts (0-4 vs 5+) — approximates the cut
--     rule WITHOUT the mobile dimension (deliberate: avoids bridge+event join; and in mobile-0 cells the
--     banner never renders, so the sharper demand-shaping split is elig/contacts).
--   * ITT caveats: email bundle (EM_IM_DO) is unchanged by IM suppression; installment plans themselves
--     generate eligible txns (that IS part of the effect, not a bug). Re-decisioning hits both arms.
-- ENGINE: Teradata-direct. Counts/sums only.

WITH crv_hist AS (
    SELECT
        acct_no,
        offer_start_date,
        year_mth_offer_start,
        action_control,
        DENSE_RANK() OVER (PARTITION BY acct_no ORDER BY offer_start_date) - 1 AS prior_crv_waves
    FROM DL_MR_PROD.cards_crv_install_decis_resp
),

leads AS (
    SELECT *
    FROM crv_hist
    WHERE offer_start_date >= DATE '2025-09-01'
      AND offer_start_date <  DATE '2026-04-01'
),

lead_keys AS (
    SELECT DISTINCT acct_no, offer_start_date
    FROM leads
),

-- eligible-txn pool (CIDM recipe), spanning pre-windows and 90d post-windows
txn_pool AS (
    SELECT t.acct_no, t.txn_dt, t.DR_TXN_AMT
    FROM D3CV12A.VISA_TXN_DLY t
    JOIN D3CV12A.lkup_txn_cd_catg k
      ON k.txn_cd = t.txn_cd
    WHERE t.DR_TXN_AMT >= 250
      AND t.txn_catg_cd <> 5001
      AND k.TXN_CATG_LVL_ID = 2
      AND k.catg_lvl_desc IN ('CAPR_OCRG_DB','PRCH_TRF_DB')
      AND t.txn_dt >= DATE '2025-08-01'      /* min(offer_start)-30 */
      AND t.txn_dt <  DATE '2026-07-01'      /* max(offer_start)+90 */
),

-- pre-deployment eligibility (same definition as c1: 30d strictly before)
pre_elig AS (
    SELECT k.acct_no, k.offer_start_date, COUNT(*) AS pre_cnt
    FROM lead_keys k
    JOIN txn_pool t
      ON t.acct_no = k.acct_no
     AND t.txn_dt >= k.offer_start_date - 30
     AND t.txn_dt <  k.offer_start_date
    GROUP BY 1, 2
),

-- post-deployment eligible-txn behavior (days 1..90 after offer_start)
post_elig AS (
    SELECT
        k.acct_no,
        k.offer_start_date,
        COUNT(*)          AS post_cnt,
        SUM(t.DR_TXN_AMT) AS post_amt
    FROM lead_keys k
    JOIN txn_pool t
      ON t.acct_no = k.acct_no
     AND t.txn_dt >  k.offer_start_date
     AND t.txn_dt <= k.offer_start_date + 90
    GROUP BY 1, 2
)

SELECT
    l.year_mth_offer_start AS cohort_month,
    CASE WHEN COALESCE(p.pre_cnt, 0) = 0 THEN 'pre_elig_0' ELSE 'pre_elig_1plus' END AS pre_elig_seg,
    CASE WHEN l.prior_crv_waves >= 5 THEN 'contacts_5plus' ELSE 'contacts_0_4' END   AS contact_seg,
    /* wide: arms as columns, counts/sums only */
    SUM(CASE WHEN l.action_control = 'Action'  THEN 1 ELSE 0 END) AS leads_action,
    SUM(CASE WHEN l.action_control = 'Control' THEN 1 ELSE 0 END) AS leads_control,
    SUM(CASE WHEN l.action_control = 'Action'  AND q.post_cnt >= 1 THEN 1 ELSE 0 END) AS post_elig_leads_action,
    SUM(CASE WHEN l.action_control = 'Control' AND q.post_cnt >= 1 THEN 1 ELSE 0 END) AS post_elig_leads_control,
    SUM(CASE WHEN l.action_control = 'Action'  THEN COALESCE(q.post_cnt, 0) ELSE 0 END) AS post_elig_txns_action,
    SUM(CASE WHEN l.action_control = 'Control' THEN COALESCE(q.post_cnt, 0) ELSE 0 END) AS post_elig_txns_control,
    SUM(CASE WHEN l.action_control = 'Action'  THEN COALESCE(q.post_amt, 0) ELSE 0 END) AS post_elig_amt_action,
    SUM(CASE WHEN l.action_control = 'Control' THEN COALESCE(q.post_amt, 0) ELSE 0 END) AS post_elig_amt_control
FROM leads l
LEFT JOIN pre_elig  p ON p.acct_no = l.acct_no AND p.offer_start_date = l.offer_start_date
LEFT JOIN post_elig q ON q.acct_no = l.acct_no AND q.offer_start_date = l.offer_start_date
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;
