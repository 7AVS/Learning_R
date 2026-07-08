-- P6: recover the sales-modal TARGETED population from the tactic event table.
-- WHY: curated cards_pli_decision_resp has NO sales-modal channel flag (channel_mb =
-- mobile banner, a DIFFERENT surface). Modal targeting was never labelled in the curated
-- channel fields - it lives only in the tactic-event verbatim, exactly like PCQ Modal Sales.
-- Until we have the targeted population, the exposure-rate denominator (all deployed) is
-- wrong: it includes clients never configured to get the modal.
-- This DISCOVERY step confirms the code + slice for PCL and returns the corrected denominator.
--
-- Table: DG6V01.TACTIC_EVNT_IP_AR_HIST.  Engine: TERADATA-DIRECT (bare names, NO catalog prefix).
-- Window: May-June 2026 treatment start (matches P3/P4/P5).
-- Pattern borrowed verbatim from campaigns/PCQ/modal_sales/pcq_ms_vs_benchmark.sql:
--   MNE = SUBSTR(TACTIC_ID, 8, 3) = 'PCL'
--   MS  = SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
-- WIDTH NOTE: repo consistently uses length 30 for MS; a PCL doc references 31. Position 121
--   is the channel-code REGION (reused across campaigns: MB mobile banner, MS modal sales, IM
--   online banner) with campaign-varying widths (8/14/30). Block 1 shows the real slice so we
--   lock the exact width+code for PCL before building anything on it.

-- ============================================================================
-- BLOCK 1 - What deployment/channel codes sit at position 121 for PCL tactics?
-- Confirms an 'MS' (modal sales) code exists. Note 'MB' = mobile banner (NOT the modal).
-- ORDER surfaces the big codes; look for the slice(s) containing 'MS'.
-- ============================================================================
SELECT
  SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) AS chan_slice_121_30,
  COUNT(*)                                AS tactic_rows,
  COUNT(DISTINCT CLNT_NO)                 AS clients
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE SUBSTR(TACTIC_ID, 8, 3) = 'PCL'
  AND TREATMT_STRT_DT >= DATE '2026-05-01'
  AND TREATMT_STRT_DT <  DATE '2026-07-01'
GROUP BY SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30)
ORDER BY clients DESC;

-- ============================================================================
-- BLOCK 2 - Size the modal-targeted population (the CORRECTED denominator).
-- clients_ms is the denominator exposure rate should use, NOT all-deployed.
-- clients_mb shown alongside to keep the mobile-banner surface distinct.
-- ============================================================================
SELECT
  COUNT(DISTINCT CLNT_NO)                                                                        AS clients_pcl_all,
  COUNT(DISTINCT CASE WHEN SUBSTR(TACTIC_DECISN_VRB_INFO,121,30) LIKE '%MS%' THEN CLNT_NO END)   AS clients_ms,   -- modal-targeted
  COUNT(DISTINCT CASE WHEN SUBSTR(TACTIC_DECISN_VRB_INFO,121,30) LIKE '%MB%' THEN CLNT_NO END)   AS clients_mb    -- mobile banner (diff surface)
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE SUBSTR(TACTIC_ID, 8, 3) = 'PCL'
  AND TREATMT_STRT_DT >= DATE '2026-05-01'
  AND TREATMT_STRT_DT <  DATE '2026-07-01';
