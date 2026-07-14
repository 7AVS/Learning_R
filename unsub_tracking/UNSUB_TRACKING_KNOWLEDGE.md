# Unsub Tracking — Knowledge Base (migration doc)

Everything learned building the bank-wide (NBA-wide) email unsubscription deep-dive.
Written 2026-07-14 for migration to a new environment. Repo folder: `unsub_tracking/`.

---

## 1. Mission & design

Holistic unsubscription tracking — not per-campaign. Two axes per unsubscriber:
- **Horizontal (timeline):** contact history before the unsub — how many contacts, cadence, and the exact deployment that triggered it.
- **Vertical (breadth):** how many distinct campaigns (MNEs) the client participated in.

**Statistical guardrail (non-negotiable):** unsubscriber-only analysis is selection-biased.
"Contacts before unsub" must be a hazard curve — P(unsub at contact n | still subscribed at n) — with at-risk denominators (clients who reached n contacts and did NOT unsub). Same for breadth: unsub rate per breadth level, not the breadth distribution of unsubscribers. Deployment attribution is *recorded* (see §3) — clean descriptively, but framed as diagnostic, not causal.

Targeted ≠ sent: the tactic table logs the decision; vendor feedback logs delivery. A client can be decisioned into email and never receive one (suppression, bounce). Targeting is the denominator concept; disposition 1 is the delivery reality.

---

## 2. Tables & schemas (empirically verified 2026-07-14)

Engine for all of this: **Teradata-direct** (EDW). Two-part names, no catalog prefix.
Full column catalogs: `schemas/vendor_feedback_tables_schema.md` (transcribed from Teradata metadata; source pics `pics/PXL_20260714_1604*.jpg`).

### DTZV01.VENDOR_FEEDBACK_EVENT — 9 columns (journey log)
`consumer_id_hashed (varchar), srvc_provdr_nm char(8), legal_entity_cd char(5), source_evnt_id varchar(30), disposition_dt_tm timestamp(6), disposition_tm_zone char(3), disposition_cd smallint, treatment_id varchar(50), load_tm timestamp(6)`

- **Grain = journey log.** One send → multiple rows sharing `(consumer_id_hashed, treatment_id)`, one per stage, each timestamped.
- `disposition_cd` (confirmed AUH Phase 1): **1=sent, 2=opened, 3=clicked, 4=unsubscribed, 5=hardbounce, 6=complaint.** 1→2→3 sequential; 4/5/6 outcome events.
- No `CLNT_NO` — resolve client through MASTER.
- Never count raw rows for funnel questions — collapse to send-journey grain first (§5).

### DTZV01.VENDOR_FEEDBACK_MASTER — 29 columns (send master)
Key cols: `treatment_id varchar(50) (=TACTIC_ID), clnt_no integer, consumer_id_hashed, email_addr, email_subj_line varchar(300), email_lang_cd, channel_type_cd char(3), cntct_mthd_typ char(3), category_cd, sub_category_cd, product_code, treatment_exp_dt (expiry, NOT send date), priority_score, card_no, load_tm` (full 29 in schema doc; col #17 `app_` vs `opp_product_typ_code` unresolved — photos conflict).

- Believed one row per client × email send — grain NOT yet verified (check via 01 pack Q3/Q4 fan-out).
- Rich analysis fields discovered: `email_subj_line` (creative-level unsub analysis), `channel_type_cd`/`cntct_mthd_typ` (channel candidates), `priority_score` (NBA priority at send).

### DG6V01.TACTIC_EVNT_IP_AR_HIST — decisioning / tactic history
(Alt name: DTZV01.TACTIC_EVNT_IP_AR_H60M — same logical table, 60-month rolling.)
Confirmed columns: `CLNT_NO, TACTIC_ID, TST_GRP_CD, RPT_GRP_CD, TACTIC_CELL_CD, TREATMT_STRT_DT, TREATMT_END_DT, TREATMT_MN, TACTIC_DECISN_VRB_INFO, ADDNL_DECISN_DATA1`.

- Grain: one row per client × tactic deployment (decision record).
- `TACTIC_ID` positions 8–10 = campaign MNE: `SUBSTR(TACTIC_ID, 8, 3)`.
- `TACTIC_DECISN_VRB_INFO` = packed string. **Never GROUP BY the raw column** — only a SUBSTR. Position 121 len 30 is the known marker slot (PCQ modal sales: `SUBSTR(...,121,30) LIKE '%MS%'`). Layout may be campaign-specific — verify per MNE before universal extraction.
- No native channel column (see §6).

---

## 3. Join map (the chain that makes attribution work)

```
EVENT (disposition, when)                    ← the unsub signal (disposition_cd=4)
  │  consumer_id_hashed + treatment_id       ← ONLY valid EVENT↔MASTER path
MASTER (clnt_no, creative, channel fields)   ← client + send context
  │  treatment_id = TACTIC_ID  AND  clnt_no = CLNT_NO   ← CLNT_NO REQUIRED (else fan-out to every decisioned client)
TACTIC_EVNT_IP_AR_HIST (TREATMT_STRT_DT, arm, MNE, packed info)  ← send timing + 60-mo contact history
```

**Attribution is directly recorded:** an unsub EVENT row carries the `treatment_id` of the send whose link was clicked — no last-touch inference needed. (Verify % non-null via 01 pack Q4a.)

**Dead ends — columns that DO NOT EXIST (cost us a broken first run):**
- `SEND_DT` (MASTER) — send timing only via the decisioning join above
- `FEEDBACK_ID` (both) — auh_explore.sql's join was never valid
- `EVENT_TYPE` (EVENT)
Old repo code (auh_explore.sql, parts of imt pipeline) references these — do not trust it over this doc / the schema doc.

---

## 4. MNE tracking scope (Andre, 2026-07-14)

Extraction stays ALL-MNE (the MNE falls out of `SUBSTR(treatment_id,8,3)` — no filter needed at extract time); this list is the REPORTING scope. Filter downstream with an exact IN-list, never substrings.

| Group | MNE | Description (from env screenshots) | Status |
|---|---|---|---|
| Cards | PCQ | Cards Acquisition | confirmed in env list |
| Cards | PCL | Proactive Credit Limit Increase | confirmed |
| Cards | PCD | Credit Card Upgrade | confirmed |
| Cards | AUH | Authorized User | confirmed |
| Cards | CLI | Card Limit Increase Nurture | confirmed |
| Cards | MVP | Card Acquisition Nurture | confirmed |
| Cards | CRV | (Cards pod campaign, runs today) | ⚠ NOT in env list — confirm code present in data |
| PBA | CTU | Chequing Account Right Fit | confirmed |
| Personal Lending | O2P | Pre-approved Overdraft Opportunity | confirmed |
| Payments | VDT | Activation Trigger | confirmed |
| Payments | VUI | Usage Trigger | confirmed |
| Payments | VUT | Wallet Provisioning | confirmed |
| Payments | VDA | BFCM Acquisition | confirmed (in env list) |
| Payments | VVD | — | ⚠ named by Andre, NOT in env list — possible typo for VDA? CONFIRM |
| Payments | VAW | (debit campaign, known from dashboard_sas work) | ⚠ not in env list — confirm |
| Payments | VCN | (debit campaign, known from dashboard_sas work) | ⚠ not in env list — confirm |
| Personal Loans | RCU | — | ⚠ named by Andre, code unconfirmed in tactic data |
| Personal Loans | RCL | — | ⚠ named by Andre; RCL appears as a product in NBR's description, not confirmed as an MNE |

Draft IN-list (fix the ⚠ entries before production use):
`('PCQ','PCL','PCD','AUH','CLI','MVP','CRV','CTU','O2P','VDT','VUI','VUT','VDA','VAW','VCN','RCU','RCL')`

### Full MNE dictionary (transcribed from env screenshots, pics/PXL_20260714_1647*/1648*.jpg)
Context: the screenshots came from an in-environment agent run answering "List all MNEs targeting EM (email)" — i.e., this is likely the email-targeting universe, not all NBA.

DAR Chequing Dormant Account (PBA-Echo) · CLI Card Limit Increase Nurture (Cards) · MVP Card Acquisition Nurture (Cards) · MNP Mortgage Nurture (HEF) · MAF Mortgage First Anniversary (HEF) · MSW Mortgage Switch (HEF) · NMO New Mortgage Opportunity (HEF) · MEC Mortgage Engagement (HEF) · AUS RBC Bank Access USA (US Banking) · AUT RBC Bank Access USA Trigger (US Banking) · MOS Investment Advice (PSI) · OFC/MWA/BPI/FND/ACK PBA Onboarding (PBA) · IMF International Money Transfer (PBA) · CRE Credit Education (PBA) · FRE Fraud Education (PBA) · PIE Investment Education (PSI) · PIH Investment Recommendation (PSI) · TFP TFSA/RRSP PAC set up (PSI) · TAO TFSA Acquisition (PSI) · PRA Chequing Restraint (PBA-Echo) · PPQ Chequing Account Acquisition (PBA) · PFS Chequing Funding (PBA-Echo) · CHQ Chequing Funding (PBA-Echo) ⚠ PFS/CHQ both read "Chequing Funding" across the two pics — verify · CTU Chequing Account Right Fit (PBA) · IDE Direct Investing Acct Acquisition (PSI) · GIS GIC Acquisition (PSI) · ESV HISA Savings Acquisition (PSI) · NBI Next Best Insurance Offer (INS) · NBR Loan Protector for Loan/RCL clients (INS) · PCQ Cards Acquisition (Cards) · PCL Proactive Credit Limit Increase (Cards) · PCD Credit Card Upgrade (Cards) · IOC Investment Offer Confirmation (PSI) · IOR Investment Offer Reminder (PSI) · AUH Authorized User (Cards) · MIR Mortgage Payment Increase Review (HEF) · MFO Mortgage Flood Prevention Offer (HEF) · CMU Accessory Dwelling Unit Opportunity (HEF) · MWP Mortgage Welcome Program (HEF) · MFY New Mortgage Touchpoint 3mo (HEF) · MAR Mortgage Auto Renewal Trigger (HEF) · RPB Chequing Retention (PBA-Echo) · VBP Balance Protector Acquisition (INS) · IPC IMT Proactive (Payments) · IRI IMT Reactive (Payments) · VDA BFCM Acquisition (Payments) · VDT Activation Trigger (Payments) · VUI Usage Trigger (Payments) · VUT Wallet Provisioning (Payments) · O2P Pre-approved Overdraft Opportunity (Personal Lending)

---

## 5. disposition_cd usage — journey patterns

Canonical patterns in `04_journey_query_patterns.sql`:
- **P1 (workhorse):** collapse to send-journey grain — GROUP BY `(consumer_id_hashed, treatment_id)`, `MAX(CASE WHEN disposition_cd = k …)` per stage flag, `MIN(CASE … disposition_dt_tm)` for first-occurrence times. Base for funnels, unsub-given-open, time-to-unsub.
- **P2:** campaign funnel — MNE × month × sequential stage counts, built on P1. Counts only; divide downstream.
- **P3:** ordered path — `ROW_NUMBER() OVER (PARTITION BY consumer, treatment ORDER BY disposition_dt_tm)`. Always scope to a treatment; unscoped = full-table window sort.
- **V1 (run before trusting funnels):** sequentiality violations — `unsub_without_sent` (preference-center opt-outs), `click_without_open` (image-blocking kills open pixel — expected, treat opens as a floor), `open_before_sent` (timezone/ordering).

---

## 6. Email-channel identification (open workstream)

Goal: identify deployments *targeted* for email (EM) from the tactic side.
Candidates, in confidence order:
1. `TACTIC_DECISN_VRB_INFO` pos 121 len 30 — hypothesis: slot holds deployed channel/surface markers (PCQ found `MS` there). Per-client truth if confirmed. Layout may vary by campaign.
2. `TREATMT_MN` — never profiled; GTM naming may embed channel.
3. `ADDNL_DECISN_DATA1` — "may contain channel info" per old IMT comment; undecoded.
4. `channel_type_cd` / `cntct_mthd_typ` on MASTER — feedback-side declaration (only exists where sends happened).

Ground-truth anchors: (a) tactic appears in VENDOR_FEEDBACK_MASTER at all = de-facto email deployed (can't see fully-suppressed email tactics though); (b) known-email vs known-banner campaigns as labeled examples — eyeball packed strings side by side to learn the layout before formalizing.
`governance/channel_codes.md`: EM=Email; IM=online banner, MB=mobile banner, CC, DM.
Validation pack: `03_tactic_join_channel_validation.sql` C1–C5 (C5 = 2×2 agreement matrix: in-MASTER × pos-121-EM). LIKE '%EM%' is discovery-only — production logic uses exact positions/codes once located.
Also worth pursuing: the deployment/CIDM team's tactic configuration record (tactic→channel dictionary) — beats forensic decoding if obtainable.

## 7. Engine rules & gotchas (Teradata-direct)

- `EXISTS` is only legal as a WHERE/ON predicate — NOT in select lists / CASE expressions. Use LEFT JOIN on DISTINCT projections for match-flag counting.
- `CAST(COUNT(*) AS BIGINT)` on any potentially-huge count (plain COUNT overflows, error 2616, >~2.1B rows).
- `SELECT TOP 5 *` for column discovery (works on the DTZV01 view layer; `HELP TABLE` may not). `TOP` is fine Teradata-direct; it is NOT Trino.
- Month buckets: `EXTRACT(YEAR)*100 + EXTRACT(MONTH)` (yyyymm) — type-agnostic (DATE or TIMESTAMP).
- CTEs (`WITH`) for prep; volatile tables ONLY where TDWM forces them (unconstrained product joins vs sys_calendar — not needed in these packs).
- Counts only in SQL — no rate divisions (divide in Excel/pandas downstream).
- History floor `DATE '2024-01-01'` on analysis scans (data reaches ~2018). EXCEPTION: join-coverage checks (03 J2/J3) run unwindowed — MASTER has no date column, so windowing the tactic side fakes join failures.
- Time grain in every extract output (month at minimum); pool downstream, never in extraction.

## 8. File index (repo `unsub_tracking/`)

| File | Purpose |
|---|---|
| `01_vendor_feedback_eda.sql` (+.py, unmaintained) | Table validation: catalogs, volumes, disposition mix, EVENT↔MASTER coverage, unsub attribution coverage, unsubs by MNE |
| `02_campaign_unsub_tracker.sql` (+.py, unmaintained) | League table: MNE × month × disposition counts from EVENT alone (no join) + NULL-treatment guard |
| `03_tactic_join_channel_validation.sql` | MASTER↔tactic join coverage + grain (J1–J4); EM channel-marker discovery (C1–C5) |
| `04_journey_query_patterns.sql` | disposition_cd usage patterns P1–P3 + sequentiality validation V1 |
| `UNSUB_TRACKING_KNOWLEDGE.md` | this doc |

Python note: `.py` versions discontinued at Andre's request (2026-07-14); SQL is the deliverable. The `.py` pattern, if ever needed again: pre-initialized `EDW` connector, `EDW.cursor()` → fetchall → DataFrame.

## 9. Open questions

1. Run outputs of packs 01–03 not yet reviewed (join coverage %, fan-out/grain buckets, channel-marker distributions, unsub attribution %).
2. MASTER grain unverified (one row per client × send?).
3. Semantics of disposition 4: one-click unsub vs preference-center vs list-level — determines whether an unsub kills all email or one program. Also whether repeated unsubs per client appear.
4. MASTER col #17: `app_` vs `opp_product_typ_code`.
5. ⚠ MNE codes to confirm: VVD (vs VDA), VAW, VCN, RCU, RCL, CRV presence in email universe; PFS vs CHQ duplicate description.
6. Wedge decision (saturation evidence vs campaign league table vs standing monitor) — base layer built to serve all three.
7. Retention window of vendor feedback tables (Q1c/Q2b outputs will show).
