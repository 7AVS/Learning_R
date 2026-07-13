-- c1: CRV suppression policy — coverage-curve CELL TABLE (retrospective, borrowed randomization)
-- Per policy-matrix cell x action_control: leads + converters. The coverage curve (cut X% of
-- surface -> keep Y% of converters, ranked by lift) is computed FROM this table downstream.
-- ENGINE: Teradata-direct (all sources Teradata). CTEs only — rerunnable, no volatile tables.
-- Dimensions use CIDM's OWN definitions from the tech spec (campaigns/CRV/crv_tech_spec_notes.md):
--   eligible txns  = VISA_TXN_DLY DR_TXN_AMT>=250, txn_catg_cd<>5001, txn_dt=proc_dt,
--                    purchases via lkup_txn_cd_catg (LVL_ID=2, CAPR_OCRG_DB/PRCH_TRF_DB)
--   mobile logins  = EXT_CDP_CHNL_EVNT mobile-app client authentications (spec Path B; see mob CTE)
--   prior contacts = prior CRV waves per acct (curated table history)
-- DEVIATION from CIDM (deliberate): counting windows are the 30 DAYS BEFORE offer_start_date
-- (pre-treatment), not CIDM's in-decision-month view — required so dimensions are pre-treatment.
--
-- !! UNVERIFIED FIELD NAME (spec doesn't show the key column — check before first run):
--   VISA_TXN_DLY account key assumed `acct_no`.
--   (EXT_CDP_CHNL_EVNT.CLNT_NO is confirmed — used by the IMT pipeline. EVNT_DT per spec;
--    if it errors, the catalogued date field is CAPTR_DT.)
--
-- PARAMS to edit: wave window (matured waves), 30-day lookbacks, bin edges (marked EDITABLE).
-- If spool blows on VISA_TXN_DLY, run month-of-waves at a time and stack.

WITH crv_hist AS (
    SELECT
        acct_no,
        offer_start_date,
        action_control,
        responder,
        COUNT(*) OVER (PARTITION BY acct_no ORDER BY offer_start_date
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_crv_contacts
    FROM DL_MR_PROD.cards_crv_install_decis_resp
),

leads AS (
    SELECT *
    FROM crv_hist
    WHERE offer_start_date >= DATE '2025-09-01'   -- matured waves only (full response window)
      AND offer_start_date <  DATE '2026-04-01'
),

bridge AS (
    SELECT acct_no, clnt_no
    FROM D3CV12A.CR_CRD_RPTS_ACCT
    QUALIFY ROW_NUMBER() OVER (PARTITION BY acct_no ORDER BY ME_DT DESC) = 1
),

-- CIDM eligible-transaction recipe, 30d pre wave
elig_txn AS (
    SELECT
        l.acct_no,
        l.offer_start_date,
        COUNT(*) AS elig_txn_cnt
    FROM leads l
    JOIN D3CV12A.VISA_TXN_DLY t
      ON t.acct_no = l.acct_no
     AND t.txn_dt >= l.offer_start_date - 30
     AND t.txn_dt <  l.offer_start_date
    JOIN D3CV12A.lkup_txn_cd_catg k
      ON k.txn_cd = t.txn_cd
    WHERE t.DR_TXN_AMT >= 250
      AND t.txn_catg_cd <> 5001            /* 5001 = quasi-cash */
      /* NOTE: spec's txn_dt = TRIAD_INSTL_PLN_OFFR_REC.proc_dt is CIDM's TRIGGER anchor
         (cycle-date evaluation), not part of txn eligibility — deliberately omitted here;
         we count eligible-txn VOLUME over the 30d pre-wave window instead. */
      AND k.TXN_CATG_LVL_ID = 2
      AND k.catg_lvl_desc IN ('CAPR_OCRG_DB','PRCH_TRF_DB')
      AND t.txn_dt >= DATE '2025-08-01'    /* global prune: min(offer_start)-30 */
      AND t.txn_dt <  DATE '2026-04-01'
    GROUP BY 1, 2
),

-- CIDM mobile-active definition, counted as frequency, 30d pre wave.
-- Spec defines Mobile Active as a UNION of connection_log_all (Path A) and EXT_CDP_CHNL_EVNT
-- (Path B). connection_log_all has NO clnt_no (Andre-confirmed) and its key is unknown, so we
-- use Path B only: EXT_CDP_CHNL_EVNT is client-grain with confirmed CLNT_NO (IMT pipeline).
-- Path B = mobile-app client authentications; slight undercount vs the spec's union — acceptable
-- for a binning dimension.
mob AS (
    SELECT
        l.acct_no,
        l.offer_start_date,
        COUNT(*) AS mobile_login_cnt
    FROM leads l
    JOIN bridge b
      ON b.acct_no = l.acct_no
    JOIN DDWV01.EXT_CDP_CHNL_EVNT c
      ON c.CLNT_NO = b.clnt_no
     AND c.EVNT_DT >= l.offer_start_date - 30
     AND c.EVNT_DT <  l.offer_start_date
    WHERE c.SRC_DTA_STORE_CD = '140'       /* Mobile */
      AND c.chnl_typ_cd = '021'            /* Mobile Apps */
      AND c.actvy_typ_cd = '065'           /* Client Authentication */
      AND c.EVNT_DT >= DATE '2025-08-01'   /* global prune */
      AND c.EVNT_DT <  DATE '2026-04-01'
    GROUP BY 1, 2
)

SELECT
    CASE                                                   /* EDITABLE bins */
        WHEN COALESCE(e.elig_txn_cnt, 0) = 0 THEN 'a. 0'
        WHEN e.elig_txn_cnt = 1              THEN 'b. 1'
        WHEN e.elig_txn_cnt <= 3             THEN 'c. 2-3'
        ELSE                                      'd. 4+'
    END AS elig_txn_bin,
    CASE                                                   /* EDITABLE bins */
        WHEN COALESCE(m.mobile_login_cnt, 0) = 0 THEN 'a. 0'
        WHEN m.mobile_login_cnt <= 9             THEN 'b. 1-9'
        ELSE                                          'c. 10+'
    END AS mobile_login_bin,
    CASE                                                   /* EDITABLE bins */
        WHEN l.prior_crv_contacts = 0  THEN 'a. 0'
        WHEN l.prior_crv_contacts <= 2 THEN 'b. 1-2'
        ELSE                                'c. 3+'
    END AS prior_contact_bin,
    l.action_control,
    COUNT(*)         AS leads,
    SUM(l.responder) AS converters
FROM leads l
LEFT JOIN elig_txn e
       ON e.acct_no = l.acct_no AND e.offer_start_date = l.offer_start_date
LEFT JOIN mob m
       ON m.acct_no = l.acct_no AND m.offer_start_date = l.offer_start_date
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
;
