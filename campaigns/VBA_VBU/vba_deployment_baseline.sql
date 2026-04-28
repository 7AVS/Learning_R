-- VBA deployment baseline — total / action / control / SRM eyeball / gross / approved
-- Source: dw00_im.dl_mr_prod.nbo_vba_rbol_combined (NBA curated outcomes)
-- Filter: mnc='VBA', treatmt_strt_dt >= 2025-08-01
--
-- Cohort grain: (year_month, treatmt_strt_dt, tactic_id, tpa_ita_indicator).
-- All counts at client level (one row per client per tactic in source).
-- control = 'Action' / 'Control' for VBA, no NULLs (confirmed 2026-04-28).
-- tpa_ita_indicator NULL rolled up to 'NONE' — only populated for action variants.
-- net_response = approved/converted; gross_response = all applications started (approved + declined).
--
-- Sanity checks to run on first execution:
--   1. `unaccounted` column must be 0 every row.
--   2. MIN/MAX of gross_response and net_response should be 0/1 (binary assumption).

SELECT
    -- Cohort dimensions
    CAST(EXTRACT(YEAR FROM treatmt_strt_dt) * 100
       + EXTRACT(MONTH FROM treatmt_strt_dt) AS INTEGER)   AS year_month,
    treatmt_strt_dt,
    tactic_id,
    COALESCE(tpa_ita_indicator, 'NONE')                    AS tpa_ita_indicator,

    -- Population volumes
    COUNT(*)                                               AS total_clients,
    SUM(CASE WHEN control = 'Action'  THEN 1 ELSE 0 END)   AS n_action,
    SUM(CASE WHEN control = 'Control' THEN 1 ELSE 0 END)   AS n_control,

    -- Sanity: should be 0 every row; non-zero = control values outside {Action, Control}
    COUNT(*)
      - SUM(CASE WHEN control = 'Action'  THEN 1 ELSE 0 END)
      - SUM(CASE WHEN control = 'Control' THEN 1 ELSE 0 END) AS unaccounted,

    -- SRM eyeball (% of cohort that's Action). Swap for chi-square once expected ratio is known.
    ROUND(100.0 * SUM(CASE WHEN control = 'Action' THEN 1 ELSE 0 END)
                / NULLIFZERO(COUNT(*)), 2)                  AS action_pct,

    -- Conversion volumes (overall)
    SUM(COALESCE(gross_response, 0))                        AS gross,
    SUM(COALESCE(net_response,   0))                        AS approved,

    -- Conversion volumes split by Action / Control
    SUM(CASE WHEN control = 'Action'  THEN COALESCE(gross_response, 0) ELSE 0 END) AS gross_action,
    SUM(CASE WHEN control = 'Control' THEN COALESCE(gross_response, 0) ELSE 0 END) AS gross_control,
    SUM(CASE WHEN control = 'Action'  THEN COALESCE(net_response,   0) ELSE 0 END) AS approved_action,
    SUM(CASE WHEN control = 'Control' THEN COALESCE(net_response,   0) ELSE 0 END) AS approved_control

FROM dw00_im.dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc = 'VBA'
  AND treatmt_strt_dt >= DATE '2025-08-01'
GROUP BY
    CAST(EXTRACT(YEAR FROM treatmt_strt_dt) * 100
       + EXTRACT(MONTH FROM treatmt_strt_dt) AS INTEGER),
    treatmt_strt_dt,
    tactic_id,
    COALESCE(tpa_ita_indicator, 'NONE')
ORDER BY
    treatmt_strt_dt,
    tactic_id,
    tpa_ita_indicator;
