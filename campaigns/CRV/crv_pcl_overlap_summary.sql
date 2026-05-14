-- CRV vs PCL — overlap summary
-- Counts only. Grain = CRV deployment start × PCL deployment start.
-- Roll up to month downstream in Excel if needed.
--
-- Overlap = any intersection between CRV and PCL treatment windows.
-- Date floor 2024-10-01 — change as needed.

WITH crv AS (
    SELECT
        clnt_no,
        treatmt_strt_dt  AS crv_strt_dt,
        treatmt_end_dt   AS crv_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2024-10-01'
),
pcl AS (
    SELECT
        clnt_no,
        treatmt_strt_dt  AS pcl_strt_dt,
        treatmt_end_dt   AS pcl_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'PCL'
      AND treatmt_strt_dt >= DATE '2024-10-01'
)
SELECT
    c.crv_strt_dt,
    p.pcl_strt_dt,
    COUNT(DISTINCT c.clnt_no)  AS overlapping_clients,
    COUNT(*)                   AS overlapping_event_pairs
FROM crv c
INNER JOIN pcl p
  ON c.clnt_no      = p.clnt_no
 AND c.crv_strt_dt <= p.pcl_end_dt
 AND c.crv_end_dt  >= p.pcl_strt_dt
GROUP BY c.crv_strt_dt, p.pcl_strt_dt
ORDER BY c.crv_strt_dt, p.pcl_strt_dt
;
