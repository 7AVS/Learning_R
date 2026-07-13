-- c2: dimension diagnostics — is the huge "elig=0 x mobile=0" mass real or measurement artifact?
-- Trigger: v3/v4 results show ~55% of leads elig=0 and ~64% mobile=0. Both implausible:
-- eligibility is TRANSACTION-TRIGGERED, so decisioned leads with zero eligible txns pre-decision
-- should barely exist. TOP SUSPECT: silent key mismatch — curated acct_no may not be the same
-- numbering as VISA_TXN_DLY's account field (CRV vintage precedent: visa_acct_no grain != acct_no).
-- A wrong NAME errors loudly; a wrong IDENTITY joins quietly and produces false zeros at scale.
-- ENGINE: Teradata-direct. Run in order; each statement answers ONE question.
-- STMT 4/5 are the decisive ones — run them first if time-boxed.

-- ============================================================================
-- STMT 1 — bridge coverage: how many leads have NO acct->clnt match?
-- Bridge misses silently become mobile=0 in c1. If pct is material, the mobile=0
-- bin is contaminated and needs an 'unbridged' split (like s10 had).
-- ============================================================================
WITH leads AS (
    SELECT DISTINCT acct_no
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2025-09-01'
      AND offer_start_date <  DATE '2026-04-01'
),
bridge AS (
    SELECT acct_no
    FROM (
        SELECT r.acct_no,
               ROW_NUMBER() OVER (PARTITION BY r.acct_no ORDER BY r.ME_DT DESC) AS rn
        FROM D3CV12A.CR_CRD_RPTS_ACCT r
        JOIN leads l ON l.acct_no = r.acct_no
    ) x
    WHERE rn = 1
)
SELECT
    COUNT(*)                                                    AS lead_accts,
    SUM(CASE WHEN b.acct_no IS NOT NULL THEN 1 ELSE 0 END)      AS bridged,
    SUM(CASE WHEN b.acct_no IS NULL     THEN 1 ELSE 0 END)      AS unbridged
FROM leads l
LEFT JOIN bridge b ON b.acct_no = l.acct_no
;

-- ============================================================================
-- STMT 2 — Path B coverage: monthly distinct clients with a mobile-app
-- authentication in EXT_CDP_CHNL_EVNT. RBC app MAU is in the multi-millions;
-- GA4 alone shows ~1.5M+ monthly card-detail viewers. If these counts come back
-- far below that order of magnitude, Path B undercounts logins and the mobile=0
-- bin is inflated with false zeros -> need Path A (connection_log_all) or an
-- alternative source with a usable client key.
-- ============================================================================
SELECT
    EXTRACT(YEAR FROM EVNT_DT)  AS yr,
    EXTRACT(MONTH FROM EVNT_DT) AS mth,
    COUNT(*)                    AS auth_events,
    COUNT(DISTINCT CLNT_NO)     AS distinct_clients
FROM DDWV01.EXT_CDP_CHNL_EVNT
WHERE SRC_DTA_STORE_CD = '140'
  AND chnl_typ_cd = '021'
  AND actvy_typ_cd = '065'
  AND EVNT_DT >= DATE '2025-09-01'
  AND EVNT_DT <  DATE '2025-12-01'      /* 3 months is enough to judge level */
GROUP BY 1, 2
ORDER BY 1, 2
;

-- ============================================================================
-- STMT 3 — elig-txn window boundary: the offer is TRIGGERED by an eligible txn
-- posted on the cycle date (~offer_start itself), which c1's strictly-before
-- window EXCLUDES. Compare elig=0 shares under three windows on ONE cohort
-- month. If including the trigger day (or a 60d lookback) collapses the 0-bin,
-- the 36% elig=0 is a window artifact, not client reality.
-- ============================================================================
WITH leads AS (
    SELECT acct_no, offer_start_date
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND offer_start_date <  DATE '2026-02-01'   /* one cohort month */
),
txn_pool AS (
    SELECT t.acct_no, t.txn_dt
    FROM D3CV12A.VISA_TXN_DLY t
    JOIN D3CV12A.lkup_txn_cd_catg k
      ON k.txn_cd = t.txn_cd
    WHERE t.DR_TXN_AMT >= 250
      AND t.txn_catg_cd <> 5001
      AND k.TXN_CATG_LVL_ID = 2
      AND k.catg_lvl_desc IN ('CAPR_OCRG_DB','PRCH_TRF_DB')
      AND t.txn_dt >= DATE '2025-11-01'
      AND t.txn_dt <  DATE '2026-02-01'
),
per_lead AS (
    SELECT
        l.acct_no,
        l.offer_start_date,
        SUM(CASE WHEN t.txn_dt >= l.offer_start_date - 30
                  AND t.txn_dt <  l.offer_start_date THEN 1 ELSE 0 END) AS cnt_30d_strict,
        SUM(CASE WHEN t.txn_dt >= l.offer_start_date - 30
                  AND t.txn_dt <= l.offer_start_date THEN 1 ELSE 0 END) AS cnt_30d_incl_day0,
        SUM(CASE WHEN t.txn_dt >= l.offer_start_date - 60
                  AND t.txn_dt <= l.offer_start_date THEN 1 ELSE 0 END) AS cnt_60d_incl_day0
    FROM leads l
    LEFT JOIN txn_pool t
           ON t.acct_no = l.acct_no
    GROUP BY 1, 2
)
SELECT
    COUNT(*)                                                   AS leads,
    SUM(CASE WHEN cnt_30d_strict    = 0 THEN 1 ELSE 0 END)     AS zero_30d_strict,
    SUM(CASE WHEN cnt_30d_incl_day0 = 0 THEN 1 ELSE 0 END)     AS zero_30d_incl_day0,
    SUM(CASE WHEN cnt_60d_incl_day0 = 0 THEN 1 ELSE 0 END)     AS zero_60d_incl_day0
FROM per_lead
;

-- ============================================================================
-- STMT 4 — KEY IDENTITY / filter ladder (one cohort month). For each lead:
--   any_txn      = ANY row in VISA_TXN_DLY, 90d, NO filters at all
--   txn_250      = amount >= 250 only
--   full_recipe  = amount + quasi-cash + purchase-category filters
-- Reading: if zero_any_txn is huge (most credit cards post SOMETHING in 90d),
-- the JOIN KEY is broken -> stop, find the real key (visa_acct_no?).
-- If any_txn matches fine but zeros appear at txn_250/full_recipe, the FILTERS
-- or category mapping are what's killing detection.
-- ============================================================================
WITH leads AS (
    SELECT acct_no, offer_start_date
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND offer_start_date <  DATE '2026-02-01'
      AND acct_no MOD 97 = 0        /* ~1% deterministic sample — spool control; shares unbiased */
),
per_lead AS (
    SELECT
        l.acct_no,
        l.offer_start_date,
        SUM(CASE WHEN t.acct_no IS NOT NULL THEN 1 ELSE 0 END)                    AS any_txn,
        SUM(CASE WHEN t.DR_TXN_AMT >= 250 THEN 1 ELSE 0 END)                      AS txn_250,
        SUM(CASE WHEN t.DR_TXN_AMT >= 250 AND t.txn_catg_cd <> 5001
                  AND k.TXN_CATG_LVL_ID = 2
                  AND k.catg_lvl_desc IN ('CAPR_OCRG_DB','PRCH_TRF_DB')
             THEN 1 ELSE 0 END)                                                   AS full_recipe
    FROM leads l
    LEFT JOIN D3CV12A.VISA_TXN_DLY t
           ON t.acct_no = l.acct_no
          AND t.txn_dt >= l.offer_start_date - 90
          AND t.txn_dt <= l.offer_start_date
    LEFT JOIN D3CV12A.lkup_txn_cd_catg k
           ON k.txn_cd = t.txn_cd
    GROUP BY 1, 2
)
SELECT
    COUNT(*)                                              AS leads,
    SUM(CASE WHEN any_txn     = 0 THEN 1 ELSE 0 END)      AS zero_any_txn_90d,
    SUM(CASE WHEN txn_250     = 0 THEN 1 ELSE 0 END)      AS zero_txn_250_90d,
    SUM(CASE WHEN full_recipe = 0 THEN 1 ELSE 0 END)      AS zero_full_recipe_90d
FROM per_lead
;

-- ============================================================================
-- STMT 5 — the smoking gun: 10 RESPONDERS (they provably had an eligible txn)
-- and their raw VISA_TXN_DLY rows around their offer window. Responders with
-- ZERO rows at all = the key is broken (wrong identity, silent non-match).
-- Show actual records, not just counts.
-- ============================================================================
WITH sample_resp AS (
    SELECT acct_no, offer_start_date, first_response_date
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND offer_start_date <  DATE '2026-02-01'
      AND responder = 1
    QUALIFY ROW_NUMBER() OVER (ORDER BY acct_no) <= 10
)
SELECT
    s.acct_no,
    s.offer_start_date,
    s.first_response_date,
    t.txn_dt,
    t.DR_TXN_AMT,
    t.txn_catg_cd,
    t.txn_cd
FROM sample_resp s
LEFT JOIN D3CV12A.VISA_TXN_DLY t
       ON t.acct_no = s.acct_no
      AND t.txn_dt >= s.offer_start_date - 60
      AND t.txn_dt <= s.offer_start_date + 30
ORDER BY s.acct_no, t.txn_dt
;

-- ============================================================================
-- STMT 6 — behavioral profile of decisioned leads, 30d pre-deployment (one
-- cohort month): how are they using the card at selection time? Split by the
-- suspicious elig-recipe-zero flag. Reading: if "elig=0" leads show healthy
-- txn counts / spend / balances, the elig DETECTION is broken (key or filters);
-- if they're genuinely dormant, the puzzle moves upstream to how they got
-- decisioned. Balance = AVG(bal_current) over the window (DFP canon: snapshot
-- column, average it, never sum).
-- ============================================================================
WITH leads AS (
    SELECT acct_no, offer_start_date, responder
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND offer_start_date <  DATE '2026-02-01'
      AND acct_no MOD 97 = 0        /* ~1% deterministic sample — spool control; averages unbiased */
),
txn_beh AS (
    SELECT
        l.acct_no,
        l.offer_start_date,
        COUNT(t.acct_no)                                       AS txn_cnt_30d,
        AVG(t.DR_TXN_AMT)                                      AS avg_txn_amt,
        SUM(CASE WHEN t.DR_TXN_AMT >= 250 AND t.txn_catg_cd <> 5001
                  AND k.TXN_CATG_LVL_ID = 2
                  AND k.catg_lvl_desc IN ('CAPR_OCRG_DB','PRCH_TRF_DB')
             THEN 1 ELSE 0 END)                                AS elig_cnt_30d
    FROM leads l
    LEFT JOIN D3CV12A.VISA_TXN_DLY t
           ON t.acct_no = l.acct_no
          AND t.txn_dt >= l.offer_start_date - 30
          AND t.txn_dt <  l.offer_start_date
    LEFT JOIN D3CV12A.lkup_txn_cd_catg k
           ON k.txn_cd = t.txn_cd
    GROUP BY 1, 2
),
bal_beh AS (
    SELECT
        l.acct_no,
        l.offer_start_date,
        AVG(p.bal_current) AS adb_30d
    FROM leads l
    JOIN D3CV12A.DLY_FULL_PORTFOLIO p
      ON p.acct_no = l.acct_no
     AND p.dt_record_ext >= l.offer_start_date - 30
     AND p.dt_record_ext <  l.offer_start_date
    GROUP BY 1, 2
)
SELECT
    CASE WHEN COALESCE(t.elig_cnt_30d, 0) = 0 THEN 'elig_zero' ELSE 'elig_1plus' END AS elig_flag,
    l.responder,
    COUNT(*)              AS leads,
    AVG(t.txn_cnt_30d)    AS avg_txn_cnt_30d,
    AVG(t.avg_txn_amt)    AS avg_txn_amt,
    AVG(b.adb_30d)        AS avg_daily_balance_30d,
    SUM(CASE WHEN COALESCE(t.txn_cnt_30d, 0) = 0 THEN 1 ELSE 0 END) AS leads_zero_any_txn,
    SUM(CASE WHEN b.acct_no IS NULL THEN 1 ELSE 0 END)              AS leads_no_dfp_rows
FROM leads l
LEFT JOIN txn_beh t ON t.acct_no = l.acct_no AND t.offer_start_date = l.offer_start_date
LEFT JOIN bal_beh b ON b.acct_no = l.acct_no AND b.offer_start_date = l.offer_start_date
GROUP BY 1, 2
ORDER BY 1, 2
;
