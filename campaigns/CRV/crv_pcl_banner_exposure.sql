-- =============================================================================
-- CRV x PCL banner EXPOSURE — (1) banner-selection STRESS TEST, (2) the BLOCK test.
-- =============================================================================
-- THE QUESTION (corrected): not "do CRV-shown clients see FEWER PLI impressions" (volume),
-- but does CRV exposure BLOCK PLI — shut a client out of the PLI banner entirely (the
-- decisioning serves one banner per slot, so CRV winning = PLI never shown)?
-- Measure: classify each overlap client by what they actually saw — both / only-PLI /
-- only-CRV (= shut out of PLI) / neither — and compare PLI-REACH (got PLI at all) by arm.
--
-- GA4 `_reduced` (Feb-2025+) = what was seen; curated PLI/CRV = overlap roster + arm.
-- Join CLNT_NO (curated) = up_srf_id2_value (GA4). Starburst/Trino. Counts only.
-- RUN #1 FIRST to validate the banner selection.
-- =============================================================================

-- ── #1 STRESS TEST: which it_item_names get tagged CRV / PLI / DROP / OTHER? ──────────
-- Eyeball: CRV rows really installments? PLI rows really CC limit-increase? loan/cheq
-- correctly dropped? anything important sitting in OTHER (= a CLI banner we're missing)?
SELECT
    CASE
        WHEN lower(it_item_name) LIKE '%cc-instalments%'                                THEN 'CRV'
        WHEN lower(it_item_name) LIKE '%ln_rcl%' OR lower(it_item_name) LIKE '%dgt_ln%' THEN 'DROP_loan'
        WHEN lower(it_item_name) LIKE '%cheq%'                                          THEN 'DROP_cheq'
        WHEN lower(it_item_name) LIKE '%pcd_ccpij%'                                     THEN 'DROP_pcd'
        WHEN ( lower(it_item_name) LIKE '%vcl%' OR lower(it_item_name) LIKE '%pcl%'
            OR lower(it_item_name) LIKE '%limit%increase%' )                            THEN 'PLI'
        ELSE 'OTHER'
    END                 AS family,
    lower(it_item_name) AS item_name,
    MIN(event_date)     AS first_seen,
    MAX(event_date)     AS last_seen,
    COUNT(*)            AS n_impressions
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2025', '2026')
  AND lower(event_name) = 'view_promotion'
  AND platform IN ('IOS', 'ANDROID')
  AND ( lower(it_item_name) LIKE '%vcl%'   OR lower(it_item_name) LIKE '%pcl%'
     OR lower(it_item_name) LIKE '%limit%increase%' OR lower(it_item_name) LIKE '%cc-instalments%'
     OR lower(it_item_name) LIKE '%ln_rcl%' OR lower(it_item_name) LIKE '%dgt_ln%'
     OR lower(it_item_name) LIKE '%cheq%' )
GROUP BY 1, 2
ORDER BY family, n_impressions DESC
;


-- ── #2 THE BLOCK TEST: does CRV exposure shut overlap clients OUT of PLI? ──────────────
-- Per arm, every overlap client classified by what they SAW (impression = view_promotion):
--   n_got_pli  : saw the PLI banner at all
--   n_got_crv  : saw the CRV banner at all
--   n_both     : saw BOTH (co-exposed)
--   n_only_pli : PLI only
--   n_only_crv : CRV only  <- the BLOCKED clients (got CRV, never got PLI)
--   n_neither  : saw neither
-- READ: PLI-reach = n_got_pli / n_clients, Action vs Control. Action < Control => CRV is
--   blocking PLI. And n_only_crv / n_got_crv = of CRV-exposed clients, the share shut out of PLI.
WITH
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND action_control = 'Control'
),
-- DO NOT cast clnt_no here (Teradata-side CAST pushes down as ROUND, err 9981).
pli AS (
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2025-02-01' AND channel LIKE '%MB%'
),
pli_flagged AS (
    SELECT p.clnt_no,
           MAX(CASE WHEN a.acct_no IS NOT NULL THEN 1 ELSE 0 END) AS any_action,
           MAX(CASE WHEN c.acct_no IS NOT NULL THEN 1 ELSE 0 END) AS any_control
    FROM pli p
    LEFT JOIN crv_action  a ON a.acct_no = p.acct_no
                           AND a.offer_start_date <= p.treatmt_end_dt AND a.offer_end_date >= p.treatmt_strt_dt
    LEFT JOIN crv_control c ON c.acct_no = p.acct_no
                           AND c.offer_start_date <= p.treatmt_end_dt AND c.offer_end_date >= p.treatmt_strt_dt
    GROUP BY p.clnt_no
),
roster AS (
    SELECT clnt_no,
           CASE WHEN any_action  = 1 THEN 'overlap_action'
                WHEN any_control = 1 THEN 'overlap_control'
                ELSE                      'no_overlap' END AS grp
    FROM pli_flagged
),
-- per client: did they see the PLI banner / the CRV banner at all (window)?
ga4 AS (
    SELECT TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
           MAX(CASE WHEN lower(it_item_name) NOT LIKE '%cc-instalments%' THEN 1 ELSE 0 END) AS got_pli,
           MAX(CASE WHEN lower(it_item_name) LIKE '%cc-instalments%'     THEN 1 ELSE 0 END) AS got_crv
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year IN ('2025', '2026')
      AND lower(event_name) = 'view_promotion'
      AND platform IN ('IOS', 'ANDROID')
      AND (
            lower(it_item_name) LIKE '%cc-instalments%'
         OR (
                ( lower(it_item_name) LIKE '%vcl%'
               OR lower(it_item_name) LIKE '%pcl%'
               OR lower(it_item_name) LIKE '%limit%increase%' )
            AND lower(it_item_name) NOT LIKE '%ln_rcl%'
            AND lower(it_item_name) NOT LIKE '%dgt_ln%'
            AND lower(it_item_name) NOT LIKE '%cheq%'
            AND lower(it_item_name) NOT LIKE '%pcd_ccpij%'
         )
          )
    GROUP BY 1
),
flags AS (
    SELECT r.grp,
           COALESCE(g.got_pli, 0) AS got_pli,
           COALESCE(g.got_crv, 0) AS got_crv
    FROM roster r
    LEFT JOIN ga4 g ON g.clnt_no = r.clnt_no
)
SELECT
    grp,
    COUNT(*)                                                        AS n_clients,
    SUM(got_pli)                                                    AS n_got_pli,
    SUM(got_crv)                                                    AS n_got_crv,
    SUM(CASE WHEN got_pli = 1 AND got_crv = 1 THEN 1 ELSE 0 END)    AS n_both,
    SUM(CASE WHEN got_pli = 1 AND got_crv = 0 THEN 1 ELSE 0 END)    AS n_only_pli,
    SUM(CASE WHEN got_pli = 0 AND got_crv = 1 THEN 1 ELSE 0 END)    AS n_only_crv,   -- BLOCKED from PLI
    SUM(CASE WHEN got_pli = 0 AND got_crv = 0 THEN 1 ELSE 0 END)    AS n_neither
FROM flags
GROUP BY grp
ORDER BY grp
;
