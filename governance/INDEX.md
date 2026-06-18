# Governance — Schema & Measurement Reference Index

This folder is an index, not a storage location. Canon files stay where they are
(references/, schemas/, campaigns/<CODE>/) — this index is the map.

**GOLDEN RULE: Campaign-scoped docs encode ONE campaign's logic. A field documented
for one campaign does NOT carry the same definition, window, success logic, or
parameters in another. Reusing a field across campaigns is a conscious decision
that requires re-validating its definition — never an inherited assumption.**

---

## A — UNIVERSAL (safe to reference from any campaign)

| Doc | Path | What it covers | Note |
|---|---|---|---|
| SQL / engine canon | `references/query_engine_guidelines.md` | The three environments (Starburst/Trino, Teradata-direct, YARN Spark), per-engine syntax rules, federation pushdown, partition pruning | Check every new query against this before shipping |
| Table grain + field meanings | `references/table_catalog_notes.md` | EDW/EDL table map §1; GA4 field meanings + join keys + banner-id rules §2; DLY_FULL_PORTFOLIO grain + field catalog §3 | For tables without a dedicated /schemas/ file, this is the reference |
| GA4 ecommerce column list | `schemas/ga4_ecommerce_schema.md` | Full GA4 ecommerce column inventory (full vs _reduced), types | Cross-campaign; not campaign-scoped |
| Account attributes | `schemas/cidm_cards_acct_attrs.md` | CIDM cards account attributes table | Table-level, not campaign-bound |
| Channel code dictionary | `governance/channel_codes.md` | All channel codes (BR/AC/MB/OB/EM/DM/OTH + specific codes), P/R classification, known gaps | Applies across PCD, PCL, CRV, VBA/VBU, CTU/O2P |
| Stats reference library | `references/stats/INDEX.md` | Power/MDE, selection bias / causal inference, A/B test design | Sub-files: power_analysis_mde.md, selection_bias_causal_inference.md, ab_test_design.md |
| Artifact taxonomy / governance doctrine | `campaigns/_templates/data_cataloging_governance.md` | Classification of artifact types (destinations, triggers, governance rules) | Universal — not campaign-specific |

---

## B — CAMPAIGN-SCOPED (logic is bound to one campaign; do NOT inherit elsewhere without re-validating)

### CRV / PCL

| Path | What it documents | SCOPE | Reuse warning |
|---|---|---|---|
| `schemas/crv_pcl_curated_schemas.md` | Column inventory for `cards_crv_install_decis_resp`, `cards_crv_install_details`, `cards_pli_decision_resp` | CRV + PCL. Exposure windows: CRV = `offer_start_date`→`offer_end_date`; PCL = `treatmt_strt_dt`→`treatmt_end_dt`. Conversion: CRV `responder`/`num_activations`; PCL `responder_cli`/`decisioned_acct`. | Window dates, responder definitions, and channel flag conventions differ between CRV and PCL even within this file — verify per table. |
| `campaigns/CRV/bulletproof_analysis/METRICS_DICTIONARY.md` | Grain definitions (lead / client / plan) and metric sources for CRV×PCL net-value calculation | CRV×PCL overlap analysis only. Grain-matched net methodology, specific query numbers (Q04, Q05, Q06, Q13), CRV NIBT per-plan economics (~$1000/plan). | These grain and query assignments are specific to the CRV×PCL cannibalization business case. Do not lift metric definitions or grains to other campaigns. |
| `campaigns/CRV/bulletproof_analysis/analysis_plan.md` | Rationale and locked methodology for the 12-query bulletproof analysis | CRV×PCL cross-LOB cannibalization claim, CRV mobile banner discontinuation recommendation. Locked 2026-05-26. | Methodology (borrowed PCL randomization as natural experiment) is a reusable pattern; the specific query logic and PCL exposure window are not. |
| `campaigns/CRV/channel_bulletproofing/s2_code_selection.md` | Locked GA4 event/code allowlist for CRV + PCL banner measurement | CRV: `it_item_id IN ('i_87340','i_87342','i_87343','i_87344')`; PCL: 12-item allowlist. Event contract: view_promotion = impression, select_promotion + p_/n_ prefix = click. Dec 2025–Feb 2026 measurement window. iOS-only caveat documented. | item_id allowlists are campaign-specific — do not reuse for other banners. The view_promotion/select_promotion event contract is cross-campaign (confirmed universal in CLI dashboard definitions and Google spec), but the id lists are not. |

### PCD

| Path | What it documents | SCOPE | Reuse warning |
|---|---|---|---|
| `schemas/pcd_curated_schemas.md` | Column inventory for `cards_pcd_ongoing_decis_resp` | PCD campaign. Exposure window = `response_start`→`response_end`. Channel flags = `channel_deploy_*` char(1) Y/N. Conversion = `responder_anyproduct`, `responder_targetproduct`, `success_cd_*`. | `channel_deploy_*` Y/N convention differs from VBA/nbo_pba_upgrade which uses smallint. Success codes (`success_cd_1/2`) are PCD-specific. `mnemonic` column present here but absent on nbo_pba_upgrade. |

### CTU / O2P

| Path | What it documents | SCOPE | Reuse warning |
|---|---|---|---|
| `schemas/nbo_pba_curated_schemas.md` | Column inventory for `nbo_pba_upgrade` (multi-campaign: CTU + O2P + other PBA upgrades) | CTU and O2P. Client-grain (no acct_no). Discriminator = `SUBSTR(tactic_id,8,3)`. Success = `primary_success` (product 43), `secondary_success` (any ladder upgrade). Pre/post behavioural block is chequing-specific. | Multi-campaign table — `comparison` column is a reporting-view label, NOT a campaign filter. Campaign isolation requires tactic_id substring. `control` field requires cross-validation against `tst_grp_cd` suffix before use (confirmed PCD trap). |
| `schemas/o2p_colleague_success_logic.md` | **Methodology doc** (not a column schema) — colleague's Teradata SQL for O2P success measurement | O2P only. Colleague's specific deployment `'202528O2P'`. Pre-approved gate `APP_TYP='P'`. Product codes 40/41/43. Approval status codes 32/37/45/47/51/56/62. 31-day window (we use 60). TG7=CONTROL. | This is a reference for alignment with the published metric, not a template to copy. Tactic IDs, gate conditions, and window length differ from our async banner deployments. Known bug in owner-indicator CASE logic — do not replicate. |

### VBA / VBU

| Path | What it documents | SCOPE | Reuse warning |
|---|---|---|---|
| `schemas/nbo_vba_rbol_combined.md` | Column inventory for `nbo_vba_rbol_combined` (VBA + RBOL curated table) | VBA campaign. Success = `net_response > 0` (approved applications), client-grain. `gross_response` = all applications started. `visa_response_channel` is a bundled 5-bucket attribution field, not the granular chnl_* flags. Filter: `mnc='VBA'` to exclude RBOL. | VBA success methodology (application-based, `net_response`) is different from VBU (product-change based). Do not conflate. `test_group` encodes VBA sub-segments (VBA only / VBA TPA / VBA ITA) — not a generic action/control field. |
| `schemas/cards_bizups_vbu_descresp_clnt.md` | Column inventory for `cards_bizups_vbu_descresp_clnt` (VBU curated table) | VBU campaign. Success = product-change (`responder_anyproduct` / `responder_targetproduct`). Exposure window = `response_start`→`response_end`. | VBU success is product-change, not card application. Even though VBA and VBU live in the same folder and share engineers, their success definitions are different and not interchangeable. Field names also diverge (e.g. `model_score` here vs `score` in VBA; `target_product` here vs `visa_offer_prod` in VBA). |
| `schemas/ucp_business_curated_fields.md` | 91-field curated selection from `tsz_00172_data_ucp4_business` for VBA decision-tree enrichment | VBA enrichment, 2026-04-28 field selection. Business client type. Fields explicitly screened to remove post-treatment leakage and sparse/null fields. | This is a pre-screened field list for ONE enrichment task. The underlying UCP-business table is larger; re-running a different analysis requires re-evaluating the field selection against leakage and sparsity. Do not assume this list is appropriate for non-VBA campaigns. |

### CLI

| Path | What it documents | SCOPE | Reuse warning |
|---|---|---|---|
| `campaigns/CLI/ga4_mobile_dashboard_metric_definitions.md` | Digital team's production definitions for every calculated field in the CLI mobile dashboard | CLI (Credit Limit Increase). Grain = COUNT DISTINCT `user_pseudo_id` (device, not `clnt_no`). Segment flags (`banner_crm_seg`, `banner_static_seg`, `banner_pa_seg`, `banner_q_seg`, `deeplink_seg`). Funnel steps specific to CLI offer flow (ep_firebase_screen values for CLI screens). | The event-level conventions (view_promotion = impression, it_creative_name p_/n_ prefix for click classification) are cross-campaign and cross-reference with s2_code_selection.md. But CLI screen names, segment flags, and funnel definitions are CLI-specific. Device grain vs client grain is a standing reconciliation point. |

---

## C — GAPS (no schema doc; do not assume governed)

| Table | Campaign / use | Status |
|---|---|---|
| `dl_mr_prod.cards_tpa_pcq_decision_resp` | PCQ curated decision/response table | No schema doc. Table used in PCQ queries but column definitions not captured. |
| `DTZV01.TACTIC_EVNT_IP_AR_H60M` | Most-joined table in the repo (tactic history, all campaigns) | No schema doc. Field list inferred from usage across queries. `SUBSTR(tactic_id,8,3)` for MNE is established pattern but full column inventory not documented. |
| CLI curated table | CLI (Credit Limit Increase) | No curated EDW table schema documented. EDA only at this stage. |
| `dl_mr_prod.nbo_pba_retention` | Sibling of `nbo_pba_upgrade` (CTU/O2P retention-side) | Mentioned in `schemas/nbo_pba_curated_schemas.md` as sibling but not yet transcribed. |
| `DLY_FULL_PORTFOLIO` (DDWV01) | Cross-campaign client-state lookups | Documented in `references/table_catalog_notes.md` §3 (grain + field catalog + past-due decode), but no dedicated `/schemas/` file. |
