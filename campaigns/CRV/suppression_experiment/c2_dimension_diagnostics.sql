-- c2: dimension diagnostics — is the huge "elig=0 x mobile=0" mass real or measurement artifact?
-- Trigger: v3/v4 results show ~64% of leads mobile=0 and ~36% elig=0 (Andre flagged as implausible).
-- ENGINE: Teradata-direct. Three small statements, run in order, each answers ONE question.

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
