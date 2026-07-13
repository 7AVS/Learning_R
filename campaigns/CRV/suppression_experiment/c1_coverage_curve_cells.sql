-- c1 v2: CRV suppression policy — coverage-curve CELL TABLE (retrospective, borrowed randomization)
-- Per policy-matrix cell x action_control: leads + converters. Coverage curve computed downstream.
-- ENGINE: Teradata-direct. CTEs only — rerunnable. If spool still blows, convert lead_keys to a
-- VOLATILE TABLE + COLLECT STATISTICS and rerun from STMT 1 (session-persistence caveat applies).
--
-- DIMENSION WINDOW LOGIC — all three are DECISION-TIME information (computable by CIDM at the
-- moment of decisioning, BEFORE any deployment is assigned to the client). This is the exact
-- logic the CIDM implementation ask must replicate:
--   eligible txns  = COUNT of qualifying purchases in the 30 DAYS BEFORE this lead's own
--                    offer_start_date. Filters are CIDM's own: VISA_TXN_DLY >=250,
--                    non-quasi-cash, purchase catgs via lkup_txn_cd_catg.
--   mobile logins  = COUNT of mobile-app client authentications in the SAME 30 days before
--                    this lead's offer_start_date (EXT_CDP_CHNL_EVNT, spec Path B;
--                    connection_log_all Path A skipped — no clnt_no on it).
--   prior contacts = COUNT of DISTINCT prior offer dates for this acct over the FULL campaign
--                    history (Oct-2024+). NOT a rolling window: '10+' means the acct was
--                    decisioned into CRV on 10+ earlier occasions before this deployment.
-- CIDM's trigger anchors (txn_dt = TRIAD proc_dt, me_dt decision-month) deliberately excluded —
-- they define when CIDM evaluates, not what the client is.
--
-- v2 fixes (Andre's critiques 2026-07-13):
--   * dimensions join a DISTINCT key spine (immune to duplicate lead rows)
--   * bridge is AS-OF wave date (latest ME_DT <= offer_start; fallback first post-wave month-end)
--   * prior contacts via DENSE_RANK (distinct wave dates, not row counts — tie/dup safe)
--   * big tables pre-filtered into pools before any join (spool)
--
-- !! UNVERIFIED FIELD NAME: VISA_TXN_DLY account key assumed `acct_no`.
--    (EXT_CDP_CHNL_EVNT.CLNT_NO confirmed; EVNT_DT per spec, fallback CAPTR_DT.)
-- PARAMS: wave window, 30d lookbacks, bin edges (EDITABLE).

-- ============================================================================
-- STMT 0 — dup guard: rows vs distinct accts per wave. rows = accts on every
-- wave => acct x wave grain is clean and STMT 1 counts are safe. If rows > accts
-- on any wave, STOP and inspect before trusting STMT 1.
-- ============================================================================
SELECT
    offer_start_date,
    COUNT(*)                AS lead_rows,
    COUNT(DISTINCT acct_no) AS distinct_accts
FROM DL_MR_PROD.cards_crv_install_decis_resp
WHERE offer_start_date >= DATE '2025-09-01'
  AND offer_start_date <  DATE '2026-04-01'
GROUP BY 1
ORDER BY 1
;

-- ============================================================================
-- STMT 1 — the cell table
-- ============================================================================
WITH crv_hist AS (
    SELECT
        acct_no,
        offer_start_date,
        year_mth_offer_start,
        action_control,
        responder,
        DENSE_RANK() OVER (PARTITION BY acct_no ORDER BY offer_start_date) - 1 AS prior_crv_waves
    FROM DL_MR_PROD.cards_crv_install_decis_resp
),

leads AS (
    SELECT *
    FROM crv_hist
    WHERE offer_start_date >= DATE '2025-09-01'   -- matured waves only
      AND offer_start_date <  DATE '2026-04-01'
),

-- distinct key spine: all dimension joins hang off this, so duplicate lead rows
-- (if any exist) cannot inflate dimension counts
lead_keys AS (
    SELECT DISTINCT acct_no, offer_start_date
    FROM leads
),

-- acct -> clnt AS-OF the wave: latest month-end at or before offer_start;
-- accounts too new for a pre-wave row fall back to their first post-wave month-end
-- (mapping only — no behavioral leakage). Restricted to lead accts BEFORE ranking (spool).
bridge AS (
    SELECT acct_no, offer_start_date, clnt_no
    FROM (
        SELECT
            k.acct_no,
            k.offer_start_date,
            r.clnt_no,
            ROW_NUMBER() OVER (
                PARTITION BY k.acct_no, k.offer_start_date
                ORDER BY CASE WHEN r.ME_DT <= k.offer_start_date THEN 0 ELSE 1 END,
                         CASE WHEN r.ME_DT <= k.offer_start_date THEN r.ME_DT END DESC,
                         r.ME_DT ASC
            ) AS rn
        FROM lead_keys k
        JOIN D3CV12A.CR_CRD_RPTS_ACCT r
          ON r.acct_no = k.acct_no
    ) x
    WHERE rn = 1
),

-- eligible-txn pool: CIDM txn filters applied BEFORE joining leads (spool)
txn_pool AS (
    SELECT t.acct_no, t.txn_dt
    FROM D3CV12A.VISA_TXN_DLY t
    JOIN D3CV12A.lkup_txn_cd_catg k
      ON k.txn_cd = t.txn_cd
    WHERE t.DR_TXN_AMT >= 250
      AND t.txn_catg_cd <> 5001                       /* quasi-cash */
      AND k.TXN_CATG_LVL_ID = 2
      AND k.catg_lvl_desc IN ('CAPR_OCRG_DB','PRCH_TRF_DB')
      AND t.txn_dt >= DATE '2025-08-01'               /* min(offer_start)-30 */
      AND t.txn_dt <  DATE '2026-04-01'
),

elig_txn AS (
    SELECT
        k.acct_no,
        k.offer_start_date,
        COUNT(*) AS elig_txn_cnt
    FROM lead_keys k
    JOIN txn_pool t
      ON t.acct_no = k.acct_no
     AND t.txn_dt >= k.offer_start_date - 30
     AND t.txn_dt <  k.offer_start_date
    GROUP BY 1, 2
),

-- mobile-auth pool: spec Path B filters applied BEFORE joining (spool)
mob_pool AS (
    SELECT c.CLNT_NO, c.EVNT_DT
    FROM DDWV01.EXT_CDP_CHNL_EVNT c
    WHERE c.SRC_DTA_STORE_CD = '140'                  /* Mobile */
      AND c.chnl_typ_cd = '021'                       /* Mobile Apps */
      AND c.actvy_typ_cd = '065'                      /* Client Authentication */
      AND c.EVNT_DT >= DATE '2025-08-01'
      AND c.EVNT_DT <  DATE '2026-04-01'
),

mob AS (
    SELECT
        b.acct_no,
        b.offer_start_date,
        COUNT(*) AS mobile_login_cnt
    FROM bridge b
    JOIN mob_pool c
      ON c.CLNT_NO = b.clnt_no
     AND c.EVNT_DT >= b.offer_start_date - 30
     AND c.EVNT_DT <  b.offer_start_date
    GROUP BY 1, 2
)

SELECT
    l.year_mth_offer_start AS cohort_month,                /* monthly cohort (daily decisioning) */
    CASE                                                   /* EDITABLE bins */
        WHEN COALESCE(e.elig_txn_cnt, 0) = 0 THEN 'a. 0'
        WHEN e.elig_txn_cnt = 1              THEN 'b. 1'
        WHEN e.elig_txn_cnt <= 3             THEN 'c. 2-3'
        WHEN e.elig_txn_cnt <= 6             THEN 'd. 4-6'
        ELSE                                      'e. 7+'
    END AS elig_txn_bin,
    CASE                                                   /* EDITABLE bins */
        WHEN COALESCE(m.mobile_login_cnt, 0) = 0 THEN 'a. 0'
        WHEN m.mobile_login_cnt <= 9             THEN 'b. 1-9'
        WHEN m.mobile_login_cnt <= 29            THEN 'c. 10-29'
        ELSE                                          'd. 30+'
    END AS mobile_login_bin,
    /* v6: NO binning on contacts — raw distinct-prior-offer-date count. Bin downstream
       (pivot grouping), so bin decisions never require a query rerun. Natural cap = however
       many decisioning occasions exist in the table's history. */
    l.prior_crv_waves AS prior_contact_cnt,
    /* WIDE output (v4): one row per cohort x cell, arms as columns — lift is a plain
       row formula downstream, no pivot calculated fields needed */
    SUM(CASE WHEN l.action_control = 'Action'  THEN 1 ELSE 0 END)           AS leads_action,
    SUM(CASE WHEN l.action_control = 'Control' THEN 1 ELSE 0 END)           AS leads_control,
    SUM(CASE WHEN l.action_control = 'Action'  THEN l.responder ELSE 0 END) AS conv_action,
    SUM(CASE WHEN l.action_control = 'Control' THEN l.responder ELSE 0 END) AS conv_control
FROM leads l
LEFT JOIN elig_txn e
       ON e.acct_no = l.acct_no AND e.offer_start_date = l.offer_start_date
LEFT JOIN mob m
       ON m.acct_no = l.acct_no AND m.offer_start_date = l.offer_start_date
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
;
