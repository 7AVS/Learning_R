-- ENGINE: Teradata-direct. RUN BEFORE TRUSTING vbu_q3_cube.sql.
-- Proves: (a) control arm exists and its responder flags are populated; (b) '1' is the responder literal;
-- (c) year_mon_start format. Expected: control rows with resp > 0 (organic upgrades exist). If control resp ~0, the
-- arm/flag definition is wrong and the cube is unusable.
-- Step 1: distinct values (format + literal check)
SELECT 'control' AS fld, control AS val, COUNT(*) AS n FROM DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT GROUP BY 1,2
UNION ALL
SELECT 'responder', responder, COUNT(*) FROM DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT GROUP BY 1,2
UNION ALL
SELECT 'responder_targetproduct', responder_targetproduct, COUNT(*) FROM DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT GROUP BY 1,2
UNION ALL
SELECT 'year_mon_start', CAST(year_mon_start AS VARCHAR(20)), COUNT(*) FROM DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT GROUP BY 1,2
ORDER BY 1,2;

-- Step 2: responders by arm for mature Q3 FY2025 months (adjust literals per Step 1 if needed)
SELECT year_mon_start, control
      ,SUM(CASE WHEN responder               = '1' THEN 1 ELSE 0 END) AS resp
      ,SUM(CASE WHEN responder_targetproduct = '1' THEN 1 ELSE 0 END) AS resp_target
      ,COUNT(*)                                                       AS clnts
FROM DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT
WHERE year_mon_start IN ('2025-05','2025-06','2025-07')
GROUP BY 1,2
ORDER BY 1,2;
