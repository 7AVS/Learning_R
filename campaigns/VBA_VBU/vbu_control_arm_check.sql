-- ENGINE: Teradata-direct.
-- RESULT 2026-08-27 (Step 2, May-Jul 2025): Action resp_target 389/489/438 on 15,681/23,310/24,940 (2.5/2.1/1.8%);
--   Control 1/1/2 on 787/1,108/1,170 (~0.1%). Control = ~4.6% holdout. year_mon_start 'YYYY-MM' CONFIRMED.
--   `responder` = '1' matched NOTHING -> different literal; `responder_targetproduct` = '1' is correct.
-- Step 1 (fixed): distinct values of `responder` only — tells us its literal.
SELECT CAST(responder AS VARCHAR(20)) AS responder_val, COUNT(*) AS n
FROM DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT
GROUP BY 1 ORDER BY 1;

-- Step 2 (already run, kept for record)
SELECT year_mon_start, control
      ,SUM(CASE WHEN responder               = '1' THEN 1 ELSE 0 END) AS resp
      ,SUM(CASE WHEN responder_targetproduct = '1' THEN 1 ELSE 0 END) AS resp_target
      ,COUNT(*)                                                       AS clnts
FROM DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT
WHERE year_mon_start IN ('2025-05','2025-06','2025-07')
GROUP BY 1,2
ORDER BY 1,2;
