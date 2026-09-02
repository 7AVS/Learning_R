-- ============================================================================
-- d1 — VBU deployment cycle: cadence, duration, re-entry (no spec exists;
-- learn it from the tactic base — same drill as CRV's e7/e12)
-- STMT 1 (~10-30 rows): one row per deployment date — cadence (monthly?
--   weekly? trigger?), window length (end dates), volumes. Decides whether
--   "monthly cohort" is a real unit or a label (dashboard uses monthly).
-- STMT 2 (~5 rows): re-entry — clients by number of distinct waves they
--   appear in. >1 material = clients roll into later deployments (the
--   re-entry methodology, read empirically).
-- Scope: ALL VBU (cycle is campaign-wide), floor 2026-04-01 for pattern.
-- Engine: TERADATA-DIRECT.
-- ============================================================================

-- STMT 1 — deployment calendar
SELECT
    treatmt_strt_dt                 AS deploy_dt,
    COUNT(DISTINCT tactic_id)       AS tactic_ids,
    COUNT(*)                        AS row_ct,
    COUNT(DISTINCT clnt_no)         AS clnt_ct,
    MIN(treatmt_end_dt)             AS min_end_dt,
    MAX(treatmt_end_dt)             AS max_end_dt
FROM dg6v01.tactic_evnt_ip_ar_hist
WHERE substr(tactic_id, 8, 3) = 'VBU'
  AND treatmt_strt_dt >= DATE '2026-04-01'
GROUP BY 1
ORDER BY 1;

-- STMT 3 (run after STMT 2) — dashboard reconciliation: July-10 deployment,
-- ALL VBU offers, action vs control totals. Dashboard shows 28,870 Action /
-- 11,007 Control for Jul-10 — control share ~28%, nothing like the ~5% seen
-- in the two model offers. This locates where the 11,007 comes from.
SELECT
    CASE WHEN TREATMT_MN LIKE 'BVBUNM%' THEN 'NOT_COMM' ELSE 'COMM' END AS comm_flag,
    TRIM(substr(TACTIC_DECISN_VRB_INFO, 34, 15))  AS offer,
    COUNT(DISTINCT clnt_no)                       AS clnt_ct
FROM dg6v01.tactic_evnt_ip_ar_hist
WHERE substr(tactic_id, 8, 3) = 'VBU'
  AND treatmt_strt_dt = DATE '2026-07-10'
GROUP BY 1, 2
ORDER BY 1, 2;

-- STMT 2 — re-entry: waves per client
SELECT
    waves_in,
    COUNT(*)                        AS clnts
FROM (
    SELECT clnt_no, COUNT(DISTINCT treatmt_strt_dt) AS waves_in
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'VBU'
      AND treatmt_strt_dt >= DATE '2026-04-01'
    GROUP BY 1
) t
GROUP BY 1
ORDER BY 1;
