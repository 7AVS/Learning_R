-- DIAGNOSTIC (run alone, returns a handful of rows). Teradata-direct.
-- Why are O2P / CTU populations blank in value_capture_report_v3? This shows every O2P and CTU
-- deployment that ACTUALLY EXISTS, its client count, and its treatment date range. It tells us:
--   (a) the REAL tactic_id spellings (the report's are OCR guesses from photos), and
--   (b) whether treatment END dates fall inside the May 1 - Jul 31 window the report filters on --
--       in-flight deployments have NULL or future end dates, which the end-date filter drops,
--       which zeroes out the population.
SELECT
  tactic_id,
  COUNT(DISTINCT clnt_no)                                   AS clients,
  MIN(treatmt_strt_dt)                                      AS first_start,
  MAX(treatmt_strt_dt)                                      AS last_start,
  MIN(treatmt_end_dt)                                       AS first_end,
  MAX(treatmt_end_dt)                                       AS last_end,
  SUM(CASE WHEN treatmt_end_dt IS NULL THEN 1 ELSE 0 END)   AS rows_with_null_end
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE (tactic_id LIKE '%O2P' OR tactic_id LIKE '%CTU')
  AND treatmt_strt_dt >= DATE '2026-04-01'
GROUP BY tactic_id
ORDER BY tactic_id;
