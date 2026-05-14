-- CRV vs PCL — overlapping clients
-- Goal: identify clients whose CRV and PCL treatment windows intersect, to
-- test whether CRV cannibalized PCL conversion on the shared mobile spot.
--
-- Overlap definition (current): ANY intersection between CRV and PCL windows.
--   crv.strt <= pcl.end AND crv.end >= pcl.strt
-- Rationale: cannibalization requires only that the two campaigns were
-- simultaneously eligible to display to the same client at any point.
--
-- Channel: extracted from TACTIC_DECISN_VRB_INFO using the substr positions
-- from prior CRV/PCL exploratory queries:
--   CRV channel = substr(..., 121, 8)
--   PCL channel = substr(..., 121, 14)
-- NOT pre-filtered to mobile yet — run this, inspect distinct channel values,
-- then add the mobile literal in the next pass.
--
-- Caveats (for later, not this query):
--   1. Same-channel overlap ≠ mobile-only — apply the mobile filter once we
--      know the channel code/string.
--   2. Treatment-window overlap = eligible-to-see, not actual-impression.
--      Tighten via EXT_CDP_CHNL_EVNT join for the journey step.
--
-- Date floor 2024-10-01 — change as needed.

WITH crv AS (
    SELECT
        clnt_no,
        tactic_id                                AS crv_tactic_id,
        treatmt_strt_dt                          AS crv_strt_dt,
        treatmt_end_dt                           AS crv_end_dt,
        substr(tactic_decisn_vrb_info, 121, 8)   AS crv_channel
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2024-10-01'
),
pcl AS (
    SELECT
        clnt_no,
        tactic_id                                AS pcl_tactic_id,
        treatmt_strt_dt                          AS pcl_strt_dt,
        treatmt_end_dt                           AS pcl_end_dt,
        substr(tactic_decisn_vrb_info, 121, 14)  AS pcl_channel
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'PCL'
      AND treatmt_strt_dt >= DATE '2024-10-01'
)
SELECT
    c.clnt_no,
    c.crv_tactic_id,
    c.crv_channel,
    c.crv_strt_dt,
    c.crv_end_dt,
    c.crv_end_dt - c.crv_strt_dt  AS crv_window_days,
    p.pcl_tactic_id,
    p.pcl_channel,
    p.pcl_strt_dt,
    p.pcl_end_dt,
    p.pcl_end_dt - p.pcl_strt_dt  AS pcl_window_days
FROM crv c
INNER JOIN pcl p
  ON c.clnt_no       = p.clnt_no
 AND c.crv_strt_dt  <= p.pcl_end_dt
 AND c.crv_end_dt   >= p.pcl_strt_dt
;
