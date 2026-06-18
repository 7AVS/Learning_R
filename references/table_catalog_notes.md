# Table Catalog Notes — Canon

Durable table facts: where tables live, their grain, and the field-level semantics you
need to query them correctly. This is the home for table knowledge that has no formal
schema doc. Curated-table schemas (CRV/PCL/PCD/NBO-PBA/O2P) live as their own files in
`/schemas/`; the full GA4 ecommerce column list lives in `schemas/ga4_ecommerce_schema.md`.
This doc covers the cross-cutting facts: the EDW/EDL table map, GA4 field MEANINGS (which
field carries what), and DLY_FULL_PORTFOLIO (which has no schema file anywhere).

Engine/syntax rules are NOT here — see `references/query_engine_guidelines.md`.

---

## 1. EDW (Teradata) vs EDL (Starburst lake) — where tables live

**EDW (Teradata; via Starburst use the `dw00_*` / schema prefix, Teradata-direct use bare schema):**

| Table | Purpose |
|---|---|
| `DG6V01.tactic_evnt_ip_ar_hist` | tactic event history |
| `DTZV01.TACTIC_EVNT_IP_AR_H60M` | tactic event, 60-month rolling |
| `DG6V01.CLNT_DERIV_DTA_HIST` | client segment / deriv history |
| `d3cv12a.appl_fact_dly` | credit-card application fact (Casper) |
| `d3cv12a.cr_crd_rpts_acct` | credit-card account reports (monthly snapshot) |
| `d3cv12a.dly_full_portfolio` | portfolio events (see §3) |
| `d3cv12a.CR_CRD_ACCT_EVNT_DLY` | account event table (AUH success: `dtl_evnt_typ_cd=191 AND ADD_RELTN_CD=3`) |
| `d3cv12a.ACCT_CRD_OWN_DLY_DELTA` | AUH snapshot source (legacy comparability only) |
| `dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp` | PCD decision/response |
| `dw00_im.dl_mr_prod.cards_pli_decision_resp` | PLI/PCL decision/response |
| `dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp` | PCQ/TPA decision/response (always filter `tpa_ita='TPA'`) |
| `dw00_im.dl_mr_prod.nbo_vba_rbol_combined` | VBA curated (pre-joins tactic+channel+response+conversion) |
| `dw00_im.dl_mr_prod.cards_bizups_vbu_descresp_clnt` | VBU curated outcomes (product-change success; schema at `schemas/cards_bizups_vbu_descresp_clnt.md`) |
| `DTZV01.VENDOR_FEEDBACK_MASTER` / `..._EVENT` | email vendor feedback / events |
| `DDWV01.EXT_CDP_CHNL_EVNT` | channel events |
| `SYS_CALENDAR.CALENDAR` | Teradata system calendar (Trino: use `UNNEST(sequence(...))` instead) |

`p3c` library = `d3cv12a` library (same tables). Use `d3cv12a`.

**EDL (Starburst / HDFS lake):**

| Table | Purpose |
|---|---|
| `edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce` | GA4 ecommerce (full; ~2 wk history) |
| `..._tsz_00198_data_ga4_ecommerce_reduced` | GA4 ecommerce (fewer cols, Feb-2025+ history — USE for multi-month) |
| `..._tsz_00198_data_ga4_narrow` | GA4 narrow |
| `edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot` | SCOT credit applications |

**HDFS paths** (use `/prod/...`, cluster prefix varies):
GA4 ecommerce `/prod/sz/tsz/00198/data/ga4-ecommerce`; GA4 narrow `/prod/sz/tsz/00198/data/ga4-narrow`;
tactic event hist `/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/`; SCOT `/prod/sz/tsz/00222/data/CREDIT_APPLICATION_SNAPSHOT`;
UCP `/prod/sz/tsz/00172/data/ucp4/`.

---

## 2. GA4 ecommerce — field meanings (which field carries what)

Full column list + types: `schemas/ga4_ecommerce_schema.md`. This section is the
*semantics* — what each field actually contains and which to use. Validated 2026-04-01
(ESV EDA), updated 2026-06-09 (CRV×PCL).

**Join key — `up_srf_id2_value` = `clnt_no`.** NOT `ep_srf_id2` (different id, does not
match CLNT_NO; cost a session of 0-match joins). Cast the GA4 side only:
`TRY_CAST(up_srf_id2_value AS BIGINT) = clnt_no`. On the tactic table `CLNT_NO` is a
direct column — do not derive it from `TACTIC_EVNT_ID`.

**Event semantics:**
- `view_promotion` = banner view/impression (~613M events). `select_promotion` = banner click (~721K).
- `view_item` = product detail page visit (NOT an impression). **CRV×PCL Q20/Q24 implication:** queries filtering on `view_item` for impression counts include product-page visits — use `view_promotion` only. [RESOLVED — see RBC implementation mapping below]

**RBC GA4 implementation mapping (Confluence, transcribed 2026-06-12)**

Event triggers (Google standard, confirmed for Salesforce dashboard banners):
- `view_promotion` = presented with a promotion = **banner impression** for Salesforce dashboard banners. Use this for impression counts on this surface.
- `select_promotion` = promotion click.
- `view_item` = user visits a product detail page (e.g. rbcroyalbank.com Avion Visa Infinite page fires on page load). **Not an impression event.**

Salesforce dashboard field mappings (confirmed May 2024):
- `it_item_id` / `it_promotion_id` ← `offerId`
- `it_item_name` / `it_promotion_name` ← `offerName` (Avion mobile: `activityName`)
- `it_creative_name` ← `creativeId`
- `it_creative_slot` ← `primaryButton.title` on mobile (button text, NOT position); web: `imageUrl`
- `it_location_id` ← `areaName` (values: `hero_banner`, `sub_banner_[#]`, `callout_[#]`)
- `it_item_index` ← `displayOrder` = banner display order/position on mobile
- `quantity` set by framework

Web surface additionally carries: `treatment_code` ← `customAttributes.TREATMT_CD`; `campaignMnemonic`; `segmentation_id` ← `customAttributes.CREATIVE_ID`; `offer_start` / `offer_end` / `offer_url` / `offerTemplate` / `offer_program`.

Four surfaces — discriminate using `it_location_id` + `affiliation`; do not mix:
- Web upper-funnel (rbcroyalbank.com)
- Mobile dashboard (Salesforce)
- Avion mobile
- Ampli/offers screens (`affiliation='ampli'`, `it_location_id` like `'ofi - offers - linked cards'`)

`it_item_index` availability: present in FULL ecommerce table only; dropped from `_reduced`. Use full table when position analysis is needed.

**Open question — mobile carousel firing:** whether `view_promotion` fires for all banners at page load vs. only on swipe-into-view is NOT documented (Intersection Observer appears only as a web solution). Confirm with digital team before interpreting `it_item_index` distributions on mobile.

**CRITICAL — `it_promotion_id` platform-dependent string format (discovered 2026-06-12 s6):**
- **iOS rows store the clean integer string** (`'87342'`); **Android rows store a float-cast string** (`'87342.0'`, len 7–8). A string `IN ('87342',...)` predicate silently excludes ~all Android volume.
- **NEVER use string IN-lists on `it_promotion_id`.** If using promotion_id, always use: `TRY_CAST(TRY_CAST(it_promotion_id AS DOUBLE) AS BIGINT) IN (87342, ...)` — numeric literals, no quotes.
- **Primary key for banner identity = `it_item_id` (`'i_'+id`), verified s7 2026-06-12 (zero disagreement, never missing, +~381K events promotion_id misses); promotion_id only with numeric cast, never raw strings.**
- Discovery: s6 run 2026-06-12 showed 87342 with 9,921,109 events under `'87342.0'` vs 638 under `'87342'` on Android; contradiction with s4 (which showed only 638 Android events for 87342) was a filter artifact — s4 itself used a string IN-list and therefore excluded the Android float-format rows.
- Prior iOS-only caveat in s2_code_selection.md and SQL headers is **RESOLVED-WRONG** (it was a filter artifact, not a tagging gap; Android is fully tracked). All live queries re-keyed to it_item_id (s7 2026-06-12).

**Banner identification — prefer `it_promotion_id`, then `it_item_id`; `it_item_name` is conditional:**
- `it_promotion_id` = the digital team's deployment **offer Id** (their Excel Id column). Most
  reliable cross-walk. Queries must use the numeric-cast pattern (see CRITICAL note above) — never a bare string IN-list.
- `it_item_id` = GA4 banner id = `'i_' || Salesforce offer Id` (e.g. `i_300102` CTU, `i_298045` O2P).
  Reliable for async/OLB banners. **AUH Phase 2 confirmed 2026-06-11**: all 8 OLB creatives present as
  `i_300108` (NonRewards), `i_308317` (RewardsNonOffer), `i_308314/308315/308333/308334/308335/308336`
  (OfferIAV/GCP/MC4/MC2/AVP/GPR) — while bare `it_promotion_id IN ('300108',...)` returned ZERO for the
  same banners. So for OLB inline-card banners the offer Id lands in `it_item_id` WITH the `i_` prefix,
  not in `it_promotion_id`; test both fields per campaign before locking the filter.
- `it_item_name` = raw creative/promo string. Safe ONLY for **non-NBO** codes (CRV `PB_CC_ALL_*`,
  lending `PB_LN_RCL_*` appear verbatim). **NBO-decisioning names do NOT exist in GA4** — GA4 logs
  the raw creative, not `NBO-PB_CC_PCL_*`; and names drift on hyphen-vs-underscore/case, so exact
  name match silently returns 0. Rule of thumb: non-NBO codes → raw creatives in `it_item_name`;
  `NBO-` codes → decisioning layer, absent from GA4. When given exact codes, match them exactly
  and FLAG a zero rather than swapping in a substring (`feedback_use_exact_codes_never_substrings`).

**Cross-campaign client-attribute tables (from work-env screenshots 2026-06-11):**
- `D3CV12A.CR_CRD_RPTS_ACCT` — monthly (`ME_DT` = month-end), acct + clnt grain. **`usg_bhvr_seg_at_cyc_cd`** = behavior segment: `Dormant` / `Transactor` / `Revolver` / null. Source for client-behavior dimension (CRV×PCL work).
- `DTZTAU.CIDM_CARDS_ACCT_ATTRS` — account grain, carries BOTH applicants: `CLNT_NO`/`CARD_NO`/`CLNT_SRCE_IND` (primary) and `CLNT_NO_A`/`CARD_NO_A`/`CLNT_SRCE_IND_A` (co-applicant; `_A` suffix = co-applicant counterpart), plus **`PRIMARY_COAPP_IDENTICAL_IND`** (X(1)). Also attrition/eligibility flags (GRO_MAILABLE_IND, INACTIVE_MONTHS_CNT, ON_BOOK_MONTHS_CNT, SECURED_CCACCT_IND, ...31 cols, `LOAD_DT`). Key for the CRV co-applicant overlap question: lets us flag overlap accounts that even HAVE a co-applicant. Indicator VALUES undecoded (Y/N assumed, unverified).

**Other content fields:**
- `it_creative_name` = button/creative label → use for click_p / click_n classification.
- `ip_sf_campaign_mnemonic` = campaign mnemonic (ESV, PCD, PCQ, PCL/VCL, VBA...). Present only on
  the FULL table; the `_reduced` history table drops the whole `ip_sf_*` block (get arm from the
  tactic table over history).
- `selected_promotion_name` = Salesforce insight names / user-action labels — NOT promo names.
- `event_timestamp` (bigint, µs) → true within-day event ordering (impression-vs-click sequence).

**Partition pruning:** `year`/`month`/`day` are varchar — filter them alongside any date filter
(`event_date` alone does NOT prune). See engine canon.

---

## 3. D3CV12A.DLY_FULL_PORTFOLIO — grain + field catalog

No formal schema doc exists anywhere in the repo; this is the authoritative empirical
inventory. Starburst path `dw00_im.d3cv12a.dly_full_portfolio`.

**Grain — event-driven, NOT truly daily despite the "DLY" name.**
- A row exists only when there's account activity (txn, status change, fee...). Gaps in
  `dt_record_ext` are normal — no row = no event that day. Up to ~31 rows per `acct_no × me_dt`.
- Grain key: `acct_no × dt_record_ext` (with gaps).
- Query in ONE scan + window functions — never multi-scan / multi-subquery on this table in one
  volatile (blows spool). For per-acct + per-month aggregates use two `ROW_NUMBER()` windows
  (`PARTITION BY acct_no ORDER BY dt_record_ext DESC` for last-overall;
  `PARTITION BY acct_no, me_dt ...` for last-of-month), then `GROUP BY acct_no` and pivot via
  `MAX(CASE...)`. (Trino: ranked CTE + `WHERE rn=1`, not QUALIFY — see engine canon.)

**Date fields:**
- `dt_record_ext` — actual record/extract date. THE daily date for filtering/joining/curves.
- `me_dt` — month-end tag (2025-01-31...). All rows in a month share it. NOT the record date.
- `acct_open_dt`, `acct_cls_dt`, `dt_writeoff_compl`, `lst_prch_dt`, `lst_ann_fee_dt`, `lst_updt_dt_tm`.

**Identifiers:**
- `acct_no` decimal(13,0) — primary join key. `clnt_no` decimal(13,0) — client id.
- `member_num` char(19) — **REWARDS membership number, NOT the card number.** Don't use as card id.
- `visa_prod_cd` char(3) — card product code. `provider_id` decimal(6,0).

**Financials — dual daily/MTD pattern.** `_dly` = that day's delta; `_mtd` = cumulative
month-to-date through that row's `dt_record_ext` (last row of month = monthly total):
`net_prch_amt_dly`/`_mtd` (net purchases), `net_pyrmt_amt_dly`/`_mtd` (payments),
`net_cash_adv_dly`/`_mtd`, `bt_cc_amt_dly`/`_mtd` (balance transfers), `net_int_amt_dly` (daily only).
- `bal_current` — point-in-time balance snapshot on that row. **Do NOT SUM it.**
- `accum_dly_bal_mtd` — accumulated daily balance MTD = **dollar-days for finance-charge math, NOT a balance and NOT cumulative purchases.**
- `net_prch_amt_dly` is a **daily delta, NOT cumulative.** `net_all_fees_amt_mtd`, `lst_ann_fee_chrg_amt`, `lylty_bal_amt`.

**CRITICAL — there is NO cumulative-spend column.** No YTD/LTD/cycle/total-purchase field exists
(confirmed via repo-wide grep). Names that do NOT exist: `net_purch_total`, `ytd_purch_amt`,
`ltd_purch_amt`, `cycle_purch_amt`, `total_purch`. To get cumulative spend, `SUM(net_prch_amt_dly)`
over the chosen window yourself.

**`status` char(4)** — account status at that row. Values: `OPEN` (active), `VOL` (voluntary
attrition), `WOFF` (write-off), `BKPT` (bankrupt), `COLL` (collections), `FRD` (fraud), `INV`
(involuntary). Once non-OPEN, stays non-OPEN. "Ever attrited" = `MAX(CASE WHEN status='VOL' THEN 1 ELSE 0 END)`.

**`cd_curr_pst_due` char(2)** — past-due bucket, confirmed by Andre 2026-05-15. Strict 30-day
buckets, hex tail at high tiers:

| Code | Days past due | | Code | Days past due |
|---|---|---|---|---|
| `NULL` | none (current) | | `07` | 181–210 |
| `01` | 1–30 | | `08` | 211–240 |
| `02` | 31–60 | | `09` | 241–270 |
| `03` | 61–90 | | `1A` | 271–300 |
| `04` | 91–120 | | `1B` | 301–330 |
| `05` | 121–150 | | `1C` | 331+ |
| `06` | 151–180 | | | |

- "Any past due" = `cd_curr_pst_due IS NOT NULL` (NOT the legacy PCQ filter `NOT IN ('','N','0')`,
  which works only by coincidence — those literals don't exist in the data; NULL is the
  not-past-due value). "90+" = `>= '03'`. "180+" = `IN ('06','07','08','09','1A','1B','1C')`.

**`cd_curr_ovrlmt`** — current overlimit code (string; `''`/`'N'`/`'0'` = not overlimit).

Don't invent columns not on this list — check a working `.sql` or ask Andre
(`feedback_no_guessing_fields`).
