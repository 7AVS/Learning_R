-- PCQ Modal Sales (MS) vs benchmark — descriptive comparison, deployments from Jun 1 2026
-- Engine: Teradata-direct (no EDL/GA4 source — runs Teradata-direct; do NOT add a catalog prefix or it fails)
-- Descriptive / directional only — no control group.
--
-- Architecture:
--   Hop 1  DG6V01.TACTIC_EVNT_IP_AR_HIST  -> LIST of MS clients (MS lives ONLY here)
--   Hop 2  DL_MR_PROD.cards_tpa_pcq_decision_resp = the master:
--          ALL PCQ deployments after Jun 2026, every category/field, MS clients FLAGGED.
--          Everyone else in that window = the benchmark.
--
--   MS code = TACTIC_DECISN_VRB_INFO chars 121-30 LIKE '%MS%'  (same slice as mobile %MB%)
--   Cohort  = treatment start >= 2026-06-01 (treatment start, NOT event date)
--   Join    = clnt_no
--   Curated column names follow existing PCQ SQL — if one errors it is a rename, swap it.
--
-- pending/declined/approved: derive in Excel from app_completed + app_approved
--   completed=1 & approved=1 -> approved ; completed=1 & approved=0 -> declined ; completed=0 -> pending
-- email_disposition only meaningful where tactic_email = 1
-- score = model_score_decile / expected_value_decile
-- NOTE: TST_GRP_CD (tactic event) and impressions are NOT in curated. test_group_latest below
--   is curated's own test group. If you also want the tactic-event TST_GRP_CD / impressions
--   carried onto every row, say so and I'll widen the Hop-1 CTE to bring them.


-- ============================================================================
-- OUTPUT A: master — ALL PCQ deployments after Jun 1, MS clients flagged. Pivot in Excel.
-- ============================================================================
WITH ms_clients AS (
    SELECT DISTINCT CLNT_NO
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
      AND TREATMT_STRT_DT >= DATE '2026-06-01'
      AND SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
)
SELECT
    CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END   AS ms_targeted,
    r.clnt_no,
    r.acct_no,
    r.tactic_id,
    r.treatmt_start_dt,
    -- strategy / targeting
    r.strtgy_seg_typ,
    r.test_group_latest,
    r.model_score_decile,
    r.expected_value_decile,
    -- product offered vs acquired
    r.offer_prod_latest_name,
    r.product_applied_name,
    r.cr_lmt_approved,
    -- decision outcome
    r.app_completed,
    r.app_approved,
    r.asc_on_app_source,
    r.response_channel_grp,
    r.response_dt,
    r.days_to_respond,
    -- email funnel
    r.tactic_email,
    r.email_disposition
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
LEFT JOIN ms_clients m
       ON m.CLNT_NO = r.clnt_no
WHERE r.decsn_year      = 2026
  AND r.tpa_ita         = 'TPA'
  AND r.treatmt_start_dt >= DATE '2026-06-01'
ORDER BY ms_targeted DESC, r.tactic_id, r.clnt_no;


-- ============================================================================
-- OUTPUT B: headline counts — MS vs benchmark by deployment & curated test group (counts only).
-- ============================================================================
WITH ms_clients AS (
    SELECT DISTINCT CLNT_NO
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
      AND TREATMT_STRT_DT >= DATE '2026-06-01'
      AND SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
)
SELECT
    CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END   AS ms_targeted,
    r.tactic_id,
    r.test_group_latest,
    COUNT(*)                                            AS rows_acct_grain,
    COUNT(DISTINCT r.clnt_no)                           AS clients,
    SUM(CAST(COALESCE(r.app_completed, 0) AS BIGINT))   AS completed,
    SUM(CAST(COALESCE(r.app_approved, 0) AS BIGINT))    AS approved
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
LEFT JOIN ms_clients m
       ON m.CLNT_NO = r.clnt_no
WHERE r.decsn_year      = 2026
  AND r.tpa_ita         = 'TPA'
  AND r.treatmt_start_dt >= DATE '2026-06-01'
GROUP BY
    CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END,
    r.tactic_id,
    r.test_group_latest
ORDER BY ms_targeted DESC, r.tactic_id, r.test_group_latest;
