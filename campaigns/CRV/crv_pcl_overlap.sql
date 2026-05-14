-- CRV vs PCL — overlapping clients
-- Goal: identify clients whose CRV and PCL treatment windows intersect, to
-- test whether CRV cannibalized PCL conversion on the shared mobile spot.
--
-- Overlap definition (current): ANY intersection between CRV and PCL windows.
--   crv.strt <= pcl.end AND crv.end >= pcl.strt
-- Rationale: cannibalization requires only that the two campaigns were
-- simultaneously eligible to display to the same client at any point.
-- Full-containment would miss partial-overlap exposures that still compete.
--
-- Caveat (for downstream measurement, not this query): treatment-window
-- overlap = eligible-to-see overlap, not actual-impression overlap. To tighten
-- to impression-level, join EXT_CDP_CHNL_EVNT in the next step.
--
-- Date floor 2024-10-01 — change as needed.

WITH crv AS (
    SELECT
        clnt_no,
        tactic_id        AS crv_tactic_id,
        treatmt_strt_dt  AS crv_strt_dt,
        treatmt_end_dt   AS crv_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2024-10-01'
),
pcl AS (
    SELECT
        clnt_no,
        tactic_id        AS pcl_tactic_id,
        treatmt_strt_dt  AS pcl_strt_dt,
        treatmt_end_dt   AS pcl_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'PCL'
      AND treatmt_strt_dt >= DATE '2024-10-01'
)
SELECT
    c.clnt_no,
    c.crv_tactic_id,
    c.crv_strt_dt,
    c.crv_end_dt,
    c.crv_end_dt - c.crv_strt_dt  AS crv_window_days,
    p.pcl_tactic_id,
    p.pcl_strt_dt,
    p.pcl_end_dt,
    p.pcl_end_dt - p.pcl_strt_dt  AS pcl_window_days
FROM crv c
INNER JOIN pcl p
  ON c.clnt_no       = p.clnt_no
 AND c.crv_strt_dt  <= p.pcl_end_dt
 AND c.crv_end_dt   >= p.pcl_strt_dt
;
