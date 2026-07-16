-- P7: validate / discover the correct GA4 modal item_id via the ARM CONTRAST.
-- The tactic table (config) confirms: only CHALLENGER (WMS) is deployed the modal (MS);
-- CHAMPION (NMS) is NOT. So the TRUE modal item_id must be one CHALLENGER clients see and
-- CHAMPION clients do NOT. If i_333067/i_333070 shows meaningful CHAMPION viewers, it is the
-- wrong (or a shared) id, and the real modal id is whichever item_id here is challenger-only.
-- We do NOT look this up in the backlog (new experiment) - we discover it from the data.
-- Engine: Starburst/Trino (GA4 + curated). Counts only. Arm from curated report_groups_period
-- (now corroborated by the tactic RPT_GRP_CD result). Window: May-July exposure, May-June deploy.

-- NOTE (error 9881 fix): keep the Teradata-side CTE a plain projection with NO numeric CAST.
-- Starburst was pushing CAST(clnt_no AS BIGINT) into Teradata as a ROUND -> 9881. The join-key
-- casts live in the cross-catalog JOIN predicate instead, so Trino evaluates them, not Teradata.
WITH arm AS (                                   -- experiment arms only (challenger vs champion)
  SELECT
    clnt_no,                                    -- raw, uncast (no pushdown to wrap)
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END AS arm
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND treatmt_strt_dt >= DATE '2026-05-01' AND treatmt_strt_dt < DATE '2026-07-01'
),
surface AS (                                    -- everything shown on the Sales_Modal surface
  SELECT
    up_srf_id2_value,                           -- raw, uncast
    it_item_id
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026' AND month IN ('05','06','07')
    AND it_location_id IN ('IOS_Sales_Modal','Android_Sales_Modal')
    AND event_name = 'view_promotion'
)
-- BLOCK 1: per item_id, how many CHALLENGER vs CHAMPION clients saw it.
-- The real modal id = high challenger_viewers, ~0 champion_viewers.
SELECT
  s.it_item_id,
  COUNT(DISTINCT CASE WHEN a.arm = 'challenger' THEN a.clnt_no END) AS challenger_viewers,
  COUNT(DISTINCT CASE WHEN a.arm = 'champion'   THEN a.clnt_no END) AS champion_viewers
FROM surface s
-- 9881 fix: NEVER cast the Teradata column (Starburst renders decimal->int as ROUND, Teradata
-- rejects it). Leave clnt_no raw; cast ONLY the GA4 side to clnt_no's type (decimal(14,0)).
JOIN arm a ON a.clnt_no = TRY_CAST(s.up_srf_id2_value AS DECIMAL(14,0))
GROUP BY s.it_item_id
ORDER BY challenger_viewers DESC;

-- ============================================================================
-- BLOCK 2: arm sizes (denominators) so viewer counts above can be read as reach.
-- ============================================================================
SELECT
  arm,
  COUNT(DISTINCT clnt_no) AS clients
FROM arm
WHERE arm IS NOT NULL
GROUP BY arm
ORDER BY arm;
