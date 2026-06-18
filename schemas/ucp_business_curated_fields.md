# UCP-business curated field list — VBA enrichment

Final 91-field selection from `tsz_00172_data_ucp4_business` for VBA decision-tree enrichment. Captured from Andre's screenshots 2026-04-28.

> **Important:** UCP-business is a **separate table** from the personal UCP. Field-name prefixes on the screenshots show `tsz_00172_data_ucp4_business.*`, confirming the table is dedicated, not a CLNT_TYP filter on `ucp4`. Likely HDFS path: `/prod/sz/tsz/00172/data/ucp4_business/` — verify on first run. Earlier memory entries that suggested UCP-business was a filter on the unified table are corrected by this evidence.

> **Transcription caveat:** Image 2 (Transaction Metrics, 40 fields) was the blurriest of the three. Field names below are best-effort; verify against your source list before treating as final. Image 1 and Image 3 are clean.

---

## Summary by category

| Category | Field count |
|---|---|
| 1. Client Information | 8 |
| 2. Account and Banking Details | 12 |
| 3. Eligibility and Status | 6 |
| 4. Product and Service Metrics | 6 |
| 5. Transaction Metrics | 40 |
| 6. Miscellaneous Metrics | 10 |
| 7. Call Center and Online Metrics | 6 |
| 8. Business Segmentation | 3 |
| **Total** | **91** |

---

## 1. Client Information (8)

- `clnt_typ`
- `clnt_no`
- `active_email_ind`
- `dlqy_ind`
- `dt_opened`
- `lang_seg_cd`
- `non_rsdt_tax_cd`
- `post_cd`

## 2. Account and Banking Details (12)

- `onlin_bnkg_enrlmnt_dt`
- `tenure_rbc_rng`
- `tenure_rbc_years`
- `entitlement_cd`
- `fsa_cd`
- `gu_no_seg_cd`
- `reln_mg_unit_no`
- `srvc_cnt`
- `digital_trans_ind`
- `mobile_auth_ind`
- `mobile_trans_ind`
- `olb_auth_ind`

## 3. Eligibility and Status (6)

- `olb_enrolled_ind`
- `cpc_ent_eligible`
- `cpc_dm_eligible`
- `cpc_tm_eligible`
- `cpc_olb_eligible`
- `myadvisor_status`

## 4. Product and Service Metrics (6)

- `actv_prod_cnt`
- `actv_prod_srvc_cnt`
- `opn_prod_cnt`
- `bpol_ind`
- `mnthly_pac_amt`
- `rel_tp_seg_cd`

## 5. Transaction Metrics (~40) — *re-transcribed from clearer screenshot 2026-04-28*

Channel × action matrix. Channels: AC (advisor center), ATM, BRANCH, IVR (voice), MOBILE, OLB (online banking). Actions: AUTH (authentications), TRANS (total transactions), specific transaction types (ACCT_TRANSFER, BILL_PYMNT, ETRANSFER, IMT, TPP). Plus an `nmi_fs_*` cluster — NMI prefix meaning unknown, flagged below.

**Authentication / total counts**
- `ac_trans_cnt`
- `atm_auth_cnt`
- `atm_trans_cnt`
- `atm_trans_ind`
- `branch_auth_cnt`
- `branch_trans_cnt`
- `branch_trans_ind`
- `ivr_auth_cnt`
- `ivr_trans_cnt`
- `mobile_auth_cnt`
- `mobile_auth_nqb_cnt`
- `mobile_trans_cnt`
- `olb_auth_cnt`
- `olb_trans_cnt`

**Account transfer**
- `ac_trans_acct_transfer_cnt`
- `atm_trans_acct_transfer_cnt`
- `ivr_trans_acct_transfer_cnt`
- `mobile_trans_acct_transfer_amt`
- `mobile_trans_acct_transfer_cnt`
- `olb_trans_acct_transfer_amt`
- `olb_trans_acct_transfer_cnt`

**Bill payment**
- `ac_trans_bill_pymnt_cnt`
- `atm_trans_bill_pymnt_cnt`
- `ivr_trans_bill_pymnt_amt`
- `ivr_trans_bill_pymnt_cnt`
- `mobile_trans_bill_pymnt_amt`
- `mobile_trans_bill_pymnt_cnt`
- `olb_trans_bill_pymnt_amt`
- `olb_trans_bill_pymnt_cnt`

**E-transfer / IMT / TPP** (digital channels only)
- `mobile_trans_etransfer_amt`
- `mobile_trans_etransfer_cnt`
- `mobile_trans_imt_cnt`
- `mobile_trans_tpp_amt`
- `mobile_trans_tpp_cnt`
- `olb_trans_etransfer_amt`
- `olb_trans_etransfer_cnt`
- `olb_trans_imt_cnt`
- `olb_trans_tpp_amt`
- `olb_trans_tpp_cnt`

**NMI FS group (5 fields — `nmi` prefix not documented)**
- `nmi_fs_act_cls_cnt` — NMI FS account closed count?
- `nmi_fs_act_opn_cnt` — NMI FS account opened count?
- `nmi_fs_act_opn_cls_cnt` — accounts opened then closed?
- `nmi_fs_dep_cnt` — deposits?
- `nmi_fs_wl_cnt` — withdrawals?

> **Open question:** What does the `nmi_*` prefix stand for in UCP-business? `fs` is plausibly "Financial Services," `act` is "account," but `nmi` is not in any documentation we have. Confirm with data steward before using these fields in the tree (or drop until known).

> **Count note:** the LLM's category header said "40 fields"; my visual transcription comes to ~44. Possible duplicates or category overlap with Misc Metrics. Use Andre's source list as canonical.

## 6. Miscellaneous Metrics (10) — *partial; image clearer than #5*

- `trans_memo_bp_cnt`
- `trans_memo_tpp_cnt`
- `ac_trans_ccpmt_cnt`
- `atm_trans_ccpmt_cnt`
- `atm_trans_dep_amt`
- `atm_trans_dep_cnt`
- `atm_trans_wl_amt`
- `atm_trans_wl_cnt`
- `branch_trans_trvls_chq_cnt`
- `calendar_appt_cnt`

## 7. Call Center and Online Metrics (6)

- `ivr_trans_ccpmt_cnt`
- `mobile_trans_mobile_ccpmt_amt`
- `mobile_trans_mobile_ccpmt_cnt`
- `mobile_trans_mobile_chq_dep_cnt`
- `olb_trans_olb_ccpmt_amt`
- `olb_trans_olb_ccpmt_cnt`

## 8. Business Segmentation (3)

- `bus_seg`
- `bsc`
- `month_end_date` *(partition key — included for join, not a feature)*

---

## Notes for the enrichment query

- Filter `WHERE trim(CLNT_TYP) = 'Business'` even though the table appears to be business-specific — defensive sanity filter.
- Inner-join to the VBA participant client list derived from tactic events HDFS (`/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/` + `SUBSTR(TACTIC_ID, 8, 3) = 'VBA'`).
- Per-client month-end alignment with the `last_day(today − 1 month)` ceiling clamp.
- Output to `/user/427966379/vba_ucp_business_slice.parquet` for download to local `data/`.

---

## Confirmation log

- **2026-04-28** — Andre delivered the final curated 91-field list after a first pass that removed (a) all-NULL / sparse fields and (b) product-performance fields (post-treatment leakage). Categories and counts confirmed. Transaction Metrics field names partially transcribed from a blurry screenshot — flagged in §5.
