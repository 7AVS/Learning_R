-- ============================================================================
-- Q26 — BEHAVIOR MIX (Dormant/Transactor/Revolver) BY OVERLAP ARM — fairness check
-- Question: is the Q04 action-vs-control comparison balanced on client behavior,
-- and does behavior explain any of the gap? Both sides profiled + no_overlap.
-- Behavior = usg_bhvr_seg_at_cyc_cd from D3CV12A.CR_CRD_RPTS_ACCT, taken at the
-- MONTH-END BEFORE each lead's treatmt_strt_dt (pre-treatment — segment measured
-- during/after the campaign would be post-treatment conditioned).
-- Population = Q04 verbatim (PCL mobile %MB%, Oct-2024+, lead grain).
-- Raw segment values carried through (incl. null). responders per cell so the
-- gap can be recomputed within each behavior segment (stratified read).
-- SANITY before reading: leads summed across segments must equal Q04 arm totals
-- (if higher, CR_CRD_RPTS_ACCT has >1 row per acct x ME_DT — tell Claude).
-- ============================================================================
WITH pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt, treatmt_end_dt, responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
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
pcl_flagged AS (
    SELECT
        p.acct_no, p.treatmt_strt_dt, p.responder_cli,
        CASE WHEN EXISTS (
            SELECT 1 FROM crv_action ca
            WHERE ca.acct_no = p.acct_no
              AND ca.offer_start_date <= p.treatmt_end_dt
              AND ca.offer_end_date   >= p.treatmt_strt_dt
        ) THEN 'overlap_action'
        WHEN EXISTS (
            SELECT 1 FROM crv_control cc
            WHERE cc.acct_no = p.acct_no
              AND cc.offer_start_date <= p.treatmt_end_dt
              AND cc.offer_end_date   >= p.treatmt_strt_dt
        ) THEN 'overlap_control'
        ELSE 'no_overlap' END AS arm
    FROM pcl_universe p
),
bhvr AS (
    SELECT acct_no, ME_DT, usg_bhvr_seg_at_cyc_cd
    FROM D3CV12A.CR_CRD_RPTS_ACCT
    WHERE ME_DT BETWEEN DATE '2024-09-30' AND DATE '2026-05-31'   -- static floor/cap: pushes down
)
SELECT
    f.arm,
    b.usg_bhvr_seg_at_cyc_cd,
    COUNT(*)                      AS leads,
    COUNT(DISTINCT f.acct_no)     AS accts,
    SUM(f.responder_cli)          AS responders
FROM pcl_flagged f
LEFT JOIN bhvr b
    ON  b.acct_no = f.acct_no
    AND b.ME_DT   = date_add('day', -1, date_trunc('month', f.treatmt_strt_dt))   -- month-end BEFORE treatment
GROUP BY 1, 2
ORDER BY 1, 2;
