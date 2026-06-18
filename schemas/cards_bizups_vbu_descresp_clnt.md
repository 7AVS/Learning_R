# `dw00_im.dl_mr_prod.cards_bizups_vbu_descresp_clnt`

NBA curated outcomes table for VBU (Visa Benefit Upgrade) — encodes **product change** as the success signal, not a new credit card application. Different methodology from the VBA table (`nbo_vba_rbol_combined`).

> **Source:** Schema captured from screenshots Andre shared 2026-04-28. Field meanings are *inference only* unless explicitly confirmed. **Do not assume — when a field's purpose is unclear, ask.**

---

## Field inventory

### Identity / treatment

| Field | Inferred meaning |
|---|---|
| `clnt_no` | Client number |
| `tactic_id` | Tactic ID |
| `target_product` | Target upgrade product code |
| `target_product_name` | Target product label |
| `target_product_grouping` | Higher-level grouping of the target product |
| `business_type` | **Unknown — ask** (business segment?) |
| `treatmt_mn` | Treatment mnemonic / month? |
| `fy_start` | Fiscal year start of treatment |
| `year_mon_start` | Year-month of treatment start |
| `response_start` | Response window start |
| `response_end` | Response window end |
| `report_group` | Reporting group label |
| `test_group` | Test group label |
| `control` | Control group label |
| `channel_contact` | Contact channel |

### Population counts

| Field | Inferred meaning |
|---|---|
| `num_clients` | Population in this slice |
| `num_resp_clients` | Responding clients |
| `num_responder_accts` | Responding accounts |
| `num_vbu_2020_fwd` | Population from 2020 forward |
| `num_vbu_2019_fwd` | Population from 2019 forward |

### Offer

| Field | Inferred meaning |
|---|---|
| `offer_bonus_points` | Bonus points in the offer |
| `offer_description` | Offer description text |

### Channel deployment flags

> Same prefix-code question as VBA — meanings of `dm`, `do`, `em`, `lvr`, `rd` not documented in repo. Asked Andre.

| Field | Inferred meaning |
|---|---|
| `channel_deploy_dm` | Direct mail? |
| `channel_deploy_do` | Direct online? |
| `channel_deploy_em` | Email? |
| `channel_deploy_lvr` | **Unknown — ask** (live voice response?) |
| `channel_deploy_rd` | **Unknown — ask** |
| `channel_redeploy_rd` | Redeployment via `rd` channel |

### Product-change success (VBU-specific)

> This is the heart of VBU success methodology. Daniel Chin's original SQL had two definitions ("any product change" vs "primary upgrade") — both appear here pre-built.

| Field | Inferred meaning |
|---|---|
| `responder` | Overall responder flag |
| `responder_anyproduct` | Any product change in window |
| `responder_targetproduct` | Change to the target upgrade product specifically |
| `from_product` | Product the client was on before the change |
| `new_product_client` | Product the client moved to |
| `dt_prod_change_client` | Date of product change |
| `fy_prod_change` | Fiscal year of the change |
| `month_prod_change` | Month of the change |

### O&O actions / call center / email

> Same suite as the VBA table. Same unknowns.

| Field |
|---|
| `csr_interactions` |
| `oando`, `oando_actioned`, `oando_pending`, `oando_declined`, `oando_approved` |
| `tactic_call` |
| `cntct_atmpt_gnsis` |
| `call_ans_gnsis` |
| `agt_gnsis` |
| `rpc_gnsis` |
| `tactic_email` |
| `email_disposition` |
| `email_status` |

### Targeting

| Field | Inferred meaning |
|---|---|
| `decile` | Model decile |
| `model_score` | Model score (vs VBA's `score` — naming differs) |

### Other

| Field | Inferred meaning |
|---|---|
| `opn_prod_cnt` | **Unknown — ask** (open products count?) |
| `new_to_campaign` | Flag for first-time campaign exposure |
| `gu` | **Unknown — ask** (also appears in VBA table) |
| `hsbc_ind` | HSBC indicator |

---

## Naming conflicts vs the VBA table

The same engineers appear to have built both tables, but the names diverge in places. Examples:

| Concept | VBA table | VBU table |
|---|---|---|
| Model score | `score` | `model_score` |
| Test group code | `tst_grp_cd` | (only `test_group` here) |
| Wave label | `wave` | (no equivalent — `treatmt_mn` / `year_mon_start`) |
| Treatment start | `treatmt_strt_dt` | `fy_start`, `year_mon_start`, `response_start` |
| Channel-deploy flags | `chnl_dm`, `chnl_do`, etc. | `channel_deploy_dm`, `channel_deploy_do`, etc. |
| Response success | `visa_app_approved` (and funnel) | `responder` / `responder_anyproduct` / `responder_targetproduct` |
| Product offered | `visa_offer_prod` | `target_product` |
| Product acquired | `visa_prod_acq` | `new_product_client` |

**Implication:** any cross-campaign analytical pattern needs explicit field-name mapping per table. There is no single naming convention — the tables are independently built.

---

## Open questions for Andre

1. Channel deploy prefix codes (`dm`, `do`, `em`, `lvr`, `rd`) — meanings?
2. `business_type` — segment or industry?
3. `lvr` channel — live voice response?
4. Population fields (`num_*`) — are these row-level counts (this row only) or aggregate counts already pre-computed at some grain?
5. `responder_anyproduct` vs `responder_targetproduct` vs `responder` — Daniel Chin's "any" / "primary upgrade" mapping holds?
6. `new_to_campaign` — flag definition?
7. `opn_prod_cnt`, `gu` — abbreviation meanings?
8. Grain confirmation — is this 1 row per `(clnt_no, tactic_id)`? Per `(clnt_no, treatment_period)`?
