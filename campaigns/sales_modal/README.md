# Sales Modal — Cross-Campaign Consolidation

Consolidated 2026-07-16 from `campaigns/PCL_PLI/sales_modal/` and `campaigns/PCQ/modal_sales/` (both
folders now removed — see git history for prior locations). PCD's modal work was never in its own
folder; it lived pooled inside the PCD async-banner tracker (`campaigns/PCD/`) and stays there — this
folder adds a modal-specific split query alongside it (see PCD section below).

## Directory layout

```
campaigns/sales_modal/
  pcl/      PLI sales-modal discovery + measurement chain (was PCL_PLI/sales_modal/)
  pcq/      PCQ Modal Sales (MS) vs benchmark + vintage (was PCQ/modal_sales/)
  pcd/      PCD modal-creative-specific split (new; PCD's tracker itself stays in campaigns/PCD/)
  shared/   Parameterized templates generalized from the PCL/PCQ working queries
```

## PCL (`pcl/`)

| File | Status | Notes |
|---|---|---|
| `p9_vcl_full_measurement.sql` | **PRODUCTION** | Full arm x strategy x decile x engagement x exposure_bin measurement on the confirmed modal id (i_308392 + i_335273). Source of RESULTS_CATALOG numbers. |
| `p10_vintage_curves.sql` | **PRODUCTION** | Cumulative conversion vintage (`dt_cl_change`, `responder_cli`), same population as p9. |
| `p1_ga4_modal_discovery.sql` .. `p8_confirm_modal_id.sql`, `p2b_design_vs_delivery.sql`, `p2c_deployment_structure.sql`, `p2d_leakage_characterization.sql` | Superseded / diagnostic | The discovery chain that led to the id correction (P7/P8 arm contrast → i_308392, not the PLI-named i_333067/i_333070). Kept flat for audit trail — do not delete, this is the paper trail for *why* i_308392 is trusted. |
| `p11_select_creative_discovery.sql` | Diagnostic | Creative-name profiling for the dismiss taxonomy. |
| `RESULTS_CATALOG.md` | Reference | Every result screenshot logged: source pic, query, transcribed numbers, finding. |
| `slide1_conversion_narrative.md` | Reference | Slide copy draft + error log against mockup screenshots. |
| `REQUEST.md`, `modal_item_id_lookup.md` | Reference | Original ask; GA4 item-id → campaign lookup + the P7/P8 arm-contrast decision. |
| `build_modal_exposure_summary.py`, `modal_exposure_summary.xlsx` | **STALE — needs regeneration** | Hardcodes the WRONG-id exposure numbers (963/138,649 ≈ 0.7% reach) from before the P7/P8 id correction. p9 confirms real reach ≈ 76% (176,440/232,781) on i_308392. Regenerate from p9's output before using this file/sheet again. |

## PCQ (`pcq/`)

| File | Status | Notes |
|---|---|---|
| `pcq_ms_vs_benchmark.sql` | **PRODUCTION** | OUTPUT A (row-level, MS flagged) + OUTPUT B (counts-only rollup). Source generalized into `shared/ms_population_success_template.sql`. |
| `pcq_ms_vintage.sql` | **PRODUCTION** | Cumulative approved/completed vintage curve, long-format. Correctly applies the Period-ASC numerator gate (Canon #1 below) — but uses `test_group_latest` as the population/arm split rather than `ms_targeted` (Canon #2 below). |
| `pcq_ms_summary.sql` | Near-duplicate | QUERY 1 is a GROUPING-SETS restatement of `vs_benchmark` OUTPUT B (same ms_targeted x tactic_id x approved/completed counts). QUERY 2 adds a wide category cube. Keep for the cube; QUERY 1 is redundant with vs_benchmark. |
| `pcq_ms_banner_engagement_discovery.sql` | Diagnostic | GA4 banner-name discovery for PCQ MS (Starburst/Trino, not Teradata-direct like the other three files here). |

## `shared/` — parameterized templates

### `ms_population_success_template.sql` (Teradata-direct)
Generalizes the two-hop pattern (Hop 1: tactic-event MS flag; Hop 2: curated decision/resp join) from
`pcq_ms_vs_benchmark.sql`, cross-checked against `pcq_ms_summary.sql` and `pcq_ms_vintage.sql`, plus
PCL's curated column names from `pcl/p9_vcl_full_measurement.sql`.

| Parameter | PCQ value | PCL value |
|---|---|---|
| `<MNE>` | `'PCQ'` | `'PCL'` |
| `<CURATED_TABLE>` | `DL_MR_PROD.cards_tpa_pcq_decision_resp` | `DL_MR_PROD.cards_pli_decision_resp` (verify schema prefix) |
| `<CURATED_ALIAS_TRT_STRT>` | `treatmt_start_dt` | `treatmt_strt_dt` |
| `<TREATMENT_WINDOW_START>` | e.g. `DATE '2026-06-01'` | campaign-specific |
| `<MANDATORY_FILTERS>` | `r.decsn_year = 2026 AND r.tpa_ita = 'TPA'` | **not confirmed in this repo** — p9 has no equivalent filter |
| `<SUCCESS_NUMERATOR_1>` | `r.app_approved` | `r.responder_cli` |
| `<SUCCESS_NUMERATOR_2>` | `r.app_completed` | derived from `r.dt_cl_change` (see template header) |
| `<SUCCESS_ASC_GATE>` | `AND TRIM(r.asc_on_app_source) = 'Period-ASC'` | **not applicable** — no equivalent field confirmed on PCL curated |
| `<POPULATION_DIMENSION>` | `r.test_group_latest` | not confirmed — set to `NULL` or derive from `report_groups_period` |

### `ga4_modal_exposure_template.sql` (Starburst/Trino)
Generalizes the `modal` + `per_client` exposure/dismiss CTEs from `pcl/p9_vcl_full_measurement.sql`.
The population CTE (`pop`/`pop1` in p9) is campaign-specific and NOT generalized — plug in each
campaign's own cohort at `<POPULATION_CTE>`.

| Parameter | PCL value | Notes |
|---|---|---|
| `<IT_ITEM_ID_LIST>` | `'i_308392','i_335273'` | Confirmed by arm contrast (P7/P8), not by label — see `modal_item_id_lookup.md`. Re-confirm per campaign. |
| `<DISMISS_CREATIVE_PATTERNS>` | `'%close%'`, `'%not now%'`, `'%dismiss%'` | `it_creative_name` read `(not set)` on view rows in PCL — verify the dismiss bucket actually populates before trusting it for a new campaign. |
| `<GA4_YEAR_MONTH_FILTERS>` | `year = '2026' AND month IN ('05','06','07')` | Partition columns — always filter both. |
| `<POPULATION_CTE>` | (campaign-specific) | Must supply `clnt_no`, `arm`, `cohort_month` at minimum. |

Exposure unit = **distinct session**, decided in `pcl/p2_exposure_universe.sql` — not raw view rows
(which balloon from double-fire/session revisits). This template only applies to campaigns with a
served/not-served (or challenger/champion) arm contrast; without a clean no-modal baseline the
exposure/dismiss split has nothing to compare against.

## PROPOSED CANON — pending Andre sign-off

Both rules are documented in full in `shared/ms_population_success_template.sql`'s header. Summary:

1. **PCQ success numerators are ALWAYS Period-ASC-gated** (`asc_on_app_source = 'Period-ASC'`, numerator
   only — denominator stays all targeted clients). This is already memory canon
   (`reference_pcq_measurement_filters.md`), but as read in this repo, `pcq_ms_vs_benchmark.sql`
   (OUTPUT A/B) and `pcq_ms_summary.sql` (QUERY 1/2) do NOT apply it — both count raw
   `app_approved`/`app_completed` with no ASC filter. Only `pcq_ms_vintage.sql` applies it correctly.
2. **Canonical PCQ population split = `ms_targeted`** (the Hop-1 tactic-event delivery-truth flag).
   `test_group_latest` (`NG3_CHMP`/`NG3_CHLN`/`NG3_CHLG`) is a DIMENSION only, never the population
   split. As read, `pcq_ms_vintage.sql` uses `test_group_latest` (champion/challenger) AS the arm
   definition instead of `ms_targeted` — it does not apply this rule.

Neither rule has been signed off by Andre. Don't treat downstream PCQ MS numbers as final until they are.

## PCD note

PCD's SalesModal creatives (`PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP` / `_IAV`) are pooled
with `PPCN` and `Offer_Hub_Banner` inside the production async-banner engagement tracker
(`campaigns/PCD/async_banner_responder_engagement.sql`, BLOCK 1) via a single `it_item_name IN (...)`
filter — SalesModal has never had its own read. `pcd/pcd_modal_creative_split.sql` adds `it_item_name`
as a grouping dimension (via a converters × creative-universe cross join, so every creative shares the
same converter denominator) so SalesModal gets view/click_p/click_n counts split out from the other two
banners. It reuses the tracker's exact population, window, and click-classification IN-lists verbatim
and does NOT modify the original tracker file — it's an additional read run alongside it.

## Known-stale items

- `pcl/build_modal_exposure_summary.py` + `pcl/modal_exposure_summary.xlsx` hardcode the wrong-id
  exposure numbers (~0.7% reach, from before the P7/P8 arm-contrast id correction). Real reach on the
  confirmed id (i_308392) is ~76% per `p9_vcl_full_measurement.sql`. Regenerate before reusing.
- The two PROPOSED CANON rules above are unsigned. Any PCQ MS number pulled before sign-off should be
  treated as directional, not final.
