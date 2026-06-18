# `dw00_im.dl_mr_prod.nbo_vba_rbol_combined`

NBA curated outcomes table — pre-joins tactic deployment, channel attribution, email engagement, and the dual-track conversion (VBA / RBOL) for the Visa Benefit Add and Royal Bank Online (?) campaigns.

> **Source:** Schema captured from screenshots Andre shared 2026-04-28. Field names and types transcribed visually; types may be off where the screenshot was unclear — Andre has the authoritative schema in his SQL tool. **Do not infer field semantics — when a field's purpose is unclear, ask.**

---

## Field inventory

Listed in the order they appeared on screen, grouped functionally. **Type column is best-effort from screenshots — verify if the type matters.** Meaning column is *inference only* unless explicitly confirmed by Andre.

### Identity / treatment

| Field | Type (inferred) | Confirmed meaning | Inferred / unclear |
|---|---|---|---|
| `report_date` | date | — | Probably the report-as-of date |
| `comparison` | varchar(50) | — | Unknown |
| `segment` | varchar(50) | — | Targeting segment label |
| `clnt_no` | integer | Client number | — |
| `mnc` | char(3) | Mnemonic — "VBA" / "RBOL" / etc. | — |
| `wave` | varchar(50) | Deployment wave label | — |
| `tactic_id` | char(10) | Tactic ID | — |
| `tactic_cell_cd` | varchar(10) | — | Tactic cell code |
| `treatmt_strt_dt` | date | Treatment start | — |
| `treatmt_end_dt` | date | Treatment end | — |
| `treatmt_mn` | varchar(10) | — | Treatment mnemonic? Treatment month? |
| `control` | varchar(10) | **`Action` / `Control`** — direct test-vs-control flag, clean two-value, no NULLs when `mnc='VBA'`. Confirmed 2026-04-28. | — |
| `test_group` | varchar(20) | **VBA sub-segment**: `VBA only` / `VBA TPA` / `VBA ITA` / `NULL`. Confirmed 2026-04-28 — these are the targeting variants within VBA, related to `tpa_ita_indicator` / `vba_tpa_rank` / `vba_ita_rank`. | — |
| `tst_grp_cd` | varchar(10) | Deployment-level test group code (per tactic specification). Finer-grained than `control`. Distinct values **TBD**. | — |

### Targeting / model

| Field | Type | Meaning |
|---|---|---|
| `nibt` | integer | **Unknown — ask** |
| `model` | varchar(20) | Model identifier / name |
| `lmt1` | varchar(2) | **Unknown — ask** |
| `lmt2` | integer | **Unknown — ask** |
| `rate` | decimal(5,2) | **Unknown — ask** |
| `score` | decimal(10,8) | Model score |
| `decile` | smallint | Decile bucket from model |
| `email_creative_id` | varchar(30) | Email creative identifier |

### Channel deployment flags

Per-channel boolean indicators. Mapped against the channel code dictionary (see "Channel code reference" section below).

| Field | Code | Channel Name | Description | P/R |
|---|---|---|---|---|
| `chnl_dm` | DM | Direct Mail | Direct Mail | P |
| `chnl_em` | EM | E-Mail | E-Mail | P |
| `chnl_do` | DO | Offers & Opportunities | Display Offer | R |
| `chnl_im` | IM | Online Banner | Internet Message | R |
| `chnl_in` | IN | Online Banner | Online Banking eOffer | R |
| `chnl_iu` | IU | Online Banner | Intercept Campaign | P |
| `chnl_rd` | RD | Advice Centre | Contact centre | P |
| `chnl_iv` | IV | **not in dictionary — ask** | — | — |
| `chnl_om` | OM | **not in dictionary — ask** | — | — |
| `chnl_zz` | ZZ | **not in dictionary — ask** | — | — |
| `channel` | — | (aggregated channel label, varchar) | — | — |
| `gu` | — | **Unknown — ask** | — | — |
| `csr_interactions` | — | CSR interactions count | — | — |

### O&O (online & offline?) actions / call center

| Field | Inferred meaning |
|---|---|
| `oando` | **Unknown — ask** (could be O&O = online and offline) |
| `oando_actioned` | Actioned in O&O |
| `oando_pending` | Pending in O&O |
| `oando_declined` | Declined in O&O |
| `oando_approved` | Approved in O&O |
| `tactic_call` | — |
| `cntct_atmpt_gnsis` | Contact attempts (Genesys) |
| `call_ans_gnsis` | Calls answered (Genesys) |
| `agt_gnsis` | Agent (Genesys) — unclear |
| `rpc_gnsis` | RPC (Genesys) — Right Party Contact? |
| `tactic_email` | — |

### Email engagement

| Field | Inferred meaning |
|---|---|
| `email_disposition` | Email disposition code |
| `email_status` | Email status code |
| `num_descn` | **Unknown — ask** |
| `num_target` | **Unknown — ask** |

### Generic response — **VBA conversion semantics confirmed**

| Field | Confirmed meaning |
|---|---|
| `gross_response` | **All applications started** (approved + declined). Confirmed Andre 2026-04-28. |
| `net_response` | **Applications approved → converted into a product.** This is the **VBA conversion indicator**: `net_response > 0` means the client converted. Confirmed Andre 2026-04-28. |
| `response_dt` | Response date |
| `prod_acq` | Product acquired |
| `cr_lmt` | Credit limit |
| `acct_no` | Account number |

**Relationships:**
- `gross_response − net_response` = applications declined.
- Reconciliation against the `visa_app_*` funnel: `visa_app_started` should align with `gross_response`, and `visa_app_approved` with `net_response`. Worth running as a sanity check before trusting either.
- For VBA success counting in any analytical query: use `net_response > 0` (or `SUM(net_response)` for aggregates), not `visa_app_approved`, unless you've verified they match exactly in the data.

### Client-level rollup methodology (CONFIRMED)

All VBA success signals (`gross_response`, `net_response`, `visa_app_*`) live at **client level**, not account level. Account-level fields (`visa_acct_no`, `visa_prod_acq`, `visa_cr_lmt`) tell you *what product* a client converted to, but they do **not** drive the success count.

**Response rate definition:**
- **Numerator** = `COUNT(DISTINCT clnt_no)` where `net_response > 0` (clients who converted)
- **Denominator** = `COUNT(DISTINCT clnt_no)` in the deployment population (= the targeted population from the tactic). The table contains both responders and non-responders, so the denominator is just the unique client count after filtering to `mnc='VBA'`.

A client who opened 2 cards in response = **1 converting client** (not 2). Account multiplicity does not inflate the numerator.

### VBA conversion track

> Application funnel for new credit card.

| Field | Inferred meaning |
|---|---|
| `visa_offer_prod` | Offered Visa product |
| `visa_offer_test` | Offer test variant |
| `visa_fee` | Fee variant |
| `visa_onoff` | **Unknown — ask** |
| `visa_acct_no` | Visa account number (if approved) |
| `visa_app_started` | App started flag |
| `visa_app_completed` | App completed flag |
| `visa_app_approved` | App approved flag |
| `visa_app_declined` | App declined flag |
| `visa_date_app_dec` | Application decision date |
| `visa_response_dt` | Visa-specific response date |
| `visa_asc_on_app` | OSC (Acquisition Strategy Code) on application — **analog to PCQ's `asc_on_app_source` (Andre confirmed verbally). Bucket logic for Period/Other/None TBD.** |
| `visa_prod_acq` | Visa product acquired (vs offered) |
| `visa_cr_lmt` | Visa credit limit |
| `visa_response_channel` | **Bundled** channel attribution for the conversion. Confirmed values: `Royal Direct`, `Online`, `Branch`, `Other`, `NULL` (5 buckets). See "Channel — deployment vs response" section below for the granular-vs-bundled distinction. |

### RBOL conversion track

> Parallel to the VBA track. RBOL = **unknown — Andre also unsure.** Filtered out of VBA-only analysis via `mnc='VBA'`.

| Field |
|---|
| `vbo` |
| `rbol_fee` |
| `rbol_onoff` |
| `rbol_offer_prod` |
| `rbol_offer` |
| `rbol_acct_no` |
| `rbol_app_started` |
| `rbol_app_completed` |
| `rbol_app_approved` |
| `rbol_app_declined` |
| `rbol_date_app_dec` |
| `rbol_response_dt` |
| `rbol_prod_acq` |
| `rbol_cr_lmt` |
| `rbol_response_channel` |
| `rbol` |

### Other flags

| Field | Inferred meaning |
|---|---|
| `blip` | **Unknown — ask** |
| `cpc_chng` | **Unknown — ask** (CPC change?) |
| `hsbc_ind` | HSBC indicator |
| `vba_tpa_rank` | VBA TPA ranking |
| `tpa_ita_indicator` | TPA ITA indicator |
| `hsbc_indicator` | (Duplicate of `hsbc_ind`?) |
| `vba_ita_rank` | VBA ITA ranking |

---

## Open questions for Andre

When ready to use this table for analytical work, confirm:

1. ~~Channel prefix codes — what does each abbreviation mean?~~ **Answered 2026-04-28 for `dm/em/do/im/in/iu/rd`. Still open: `chnl_iv`, `chnl_om`, `chnl_zz` (not in dictionary).**
2. `nibt`, `lmt1`, `lmt2`, `rate` — model / scoring fields with unclear semantics
3. `gu`, `oando`, `blip`, `cpc_chng`, `visa_onoff` — abbreviations not documented anywhere
4. `num_descn` vs `num_target` — what's the difference?
5. RBOL — what does it stand for? (Royal Bank Online?)
6. `visa_asc_on_app` bucketing — does this column hold the raw OSC code, or has it already been bucketed (e.g. Period-OSC / Other-OSC / NO-OSC)?
7. `hsbc_ind` vs `hsbc_indicator` — same field appears twice; intentional?
8. Treatment vs response date — `response_dt` (generic) vs `visa_response_dt` — which is authoritative for VBA success?

---

## Confirmation log

- **2026-04-28** — Andre confirmed: `net_response = approved/converted`, `gross_response = all started (approved + declined)`. **VBA conversion = `net_response > 0`.**
- **2026-04-28** — Andre confirmed `visa_response_channel` is a **bundled** attribution field with 5 values: `Royal Direct`, `Online`, `Branch`, `Other`, `NULL`. The deployment-side `chnl_*` flags are granular (banner, direct mail, internet, etc.) and do **not** map 1:1 to these buckets — multiple granular deployment channels can roll up to a single response-channel bucket. See "Channel — deployment vs response" below.
- **2026-04-28** — Andre confirmed all VBA success signals roll up at **client level**, not account level. Response-rate numerator = `COUNT(DISTINCT clnt_no WHERE net_response > 0)`; denominator = `COUNT(DISTINCT clnt_no)` in the targeted population. Account fields are for product detail only.
- **2026-04-28** — Andre provided the channel code dictionary (BR/AC/MB/OB/O&O/EM/DM/OTH groupings + 30+ specific codes including AM, HA, RD, OLB, IN, IM, IU, DO, OP, CM, SM, etc.). Captured in "Channel code reference" section. `chnl_dm`, `chnl_em`, `chnl_do`, `chnl_im`, `chnl_in`, `chnl_iu`, `chnl_rd` now mapped. `chnl_iv`, `chnl_om`, `chnl_zz` still not in dictionary — open question.
- **2026-04-28** — Andre confirmed values for the test-vs-control fields when `mnc='VBA'`: `control` ∈ `{Action, Control}` (no NULL) — direct test-vs-control flag. `test_group` ∈ `{VBA only, VBA TPA, VBA ITA, NULL}` — VBA sub-segment / targeting variant. `tst_grp_cd` is the deployment-level cell code (finer than `control`) — distinct values still TBD.

---

## Channel code reference

Channel codes: see `governance/channel_codes.md` (universal reference).

The full dictionary originally captured here has been extracted to the universal reference above. The `chnl_*` flag columns on this table map to the channel codes in that dictionary; `chnl_iv`, `chnl_om`, `chnl_zz` remain unresolved (not in dictionary — flagged in `governance/channel_codes.md`).

---

## Channel — deployment vs response

VBA has **two separate channel concepts** in this table. They are not redundant — they answer different questions.

### Deployment channels (granular — *what the client was exposed to*)

The `chnl_*` boolean flags (`chnl_do`, `chnl_dm`, `chnl_em`, `chnl_im`, `chnl_rd`, `chnl_iu`, `chnl_in`, `chnl_om`, `chnl_iv`, `chnl_zz`) record what specific channels each client was deployed to. One client can have multiple flags = 1 (e.g. exposed to both email and a banner). Granular: each flag is a distinct deployment channel (banner, direct mail, email, internet placement, etc.).

### Response channel (bundled — *where the conversion was detected*)

`visa_response_channel` records the channel attributed for the conversion, **bundled** into 5 buckets:

| Value | Notes |
|---|---|
| `Royal Direct` | — |
| `Online` | Multiple granular deployment channels (e.g. internet banner, online direct, etc.) roll up here |
| `Branch` | — |
| `Other` | Catch-all |
| `NULL` | No conversion / no channel attributed |

**Implication:** when analysing channel performance, the deployment-side `chnl_*` flags and `visa_response_channel` are **not the same axis**. Treat them as two views:
- "What channels reached this client?" → `chnl_*` flags
- "Through which bundled channel did this client convert?" → `visa_response_channel`

There is **no documented 1:1 mapping** from `chnl_*` codes to `visa_response_channel` buckets. The bundling logic is upstream of this table.
