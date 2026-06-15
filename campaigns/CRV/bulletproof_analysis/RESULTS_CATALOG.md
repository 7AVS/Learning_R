# CRV vs PCL Cannibalization — Results Catalog

Transcribed from result screenshots 2026-06-01. Source of truth for prior outputs so we don't re-transcribe.
All numbers exact as read; OCR ±1 digit possible on dense cells. Cross-check against re-run before quoting externally.

---

## Q00 — Randomization validation (randomization_validation.sql)

**Assignment buckets (account-level, distinct):**
| bucket | account_count | total | pct |
|---|---|---|---|
| action_only | 4,471,313 | 4,714,609 | 94.84% |
| control_only | 233,017 | 4,714,609 | 4.94% |
| both | 10,279 | 4,714,609 | 0.22% |

**Wave duration:**
| campaign | n_waves | dur_mean | dur_min | dur_max | n_months | waves/mo_mean |
|---|---|---|---|---|---|---|
| CRV-Action | 420 | 92.1 | 87 | 96 | 20 | 21 |
| PCL-mobile | 88 | 68.9 | 43 | 90 | 20 | 4.4 |

Notes: assignment is sticky (Action stays Action through the window). CRV waves ~90d, PCL ~69d. Only 0.22% of accounts ever appear in both arms → arms are clean.

---

## Q01 — Action/Control balance (action_control_balance_test.sql)

OVERALL: full_action_leads **23,058,958**; full_control_leads **1,218,758**; overlap_action_leads **6,381,886**; overlap_control_leads **331,214**.
% CRV_A overlap PCL = **28%**; % CRV_C overlap PCL = **27%** (monthly range 19–32%, dips Apr–May 2026).
Caption: "CRV A and C overlapping volumes with PCL are similar, allowing the comparison."
(Grain = leads = account-waves, not distinct accounts.)

---

## Q02 — Overlap-days distribution (overlap_days_distribution.sql)

| subset | arm | n | mean_days | p10 | p25 | p50 | p75 | p90 | min | max |
|---|---|---|---|---|---|---|---|---|---|---|
| all_leads | Action | 6,381,886 | 45.7 | 14 | 39 | 49 | 60 | 65 | 1 | 90 |
| all_leads | Control | 331,214 | 45.4 | 14 | 38 | 49 | 60 | 65 | 1 | 90 |
| pcl_responders | Action | 1,631,562 | 38.8 | 11 | 18 | 43 | 54 | 62 | 1 | 90 |
| pcl_responders | Control | 89,802 | 38.6 | 11 | 18 | 43 | 54 | 62 | 1 | 90 |

Action/Control overlap-day distributions are nearly identical → balanced exposure window.

---

## Q03 — Bidirectional overlap share (overlap_share_bidirectional.sql)

| direction | total | overlap_count | % |
|---|---|---|---|
| crv_action_with_pcl | 23,058,958 | 6,381,886 | 28% |
| crv_control_with_pcl | 1,218,758 | 331,214 | 27% |
| pcl_with_crv_action | 9,721,444 | 6,492,198 | **67%** |
| pcl_with_crv_control | 9,721,444 | 337,853 | 3% |

**67% of PCL-mobile deployments face a competing CRV-Action exposure.** Randomization holds (28% vs 27%).

---

## Q04 — Cannibalization gap + CI (ci_on_cannibalization_gap.sql) — THE HYPOTHESIS TEST

OVERALL: n_action **6,492,198**, resp_action **1,048,589**, n_control **337,853**, resp_control **58,218**.
p_action **16.15%**, p_control **17.23%**, **gap 1.08pp**, ci_lower 0.9%, ci_upper 1.2%, **z = 16.23**, significant.
**Control PCL rate is HIGHER than Action → CRV-Action SUPPRESSES PCL conversion by 1.08pp.**
SS in **17 of 20 months**; every month same direction (control > action). Non-sig months: Nov-24, Dec-24, Feb-25 (early, smaller n).

---

## Headline $ derivation (chart/summary view)

Incremental PCL conversions ≈ **42,407**. Confirmed PCL NIBT = **$675/conversion** (Andre, 2026-06-01).
→ Gross PCL recovery = 42,407 × $675 ≈ **$28.6M** (assumes zero CRV loss).
(The $29.47M figure in the original screenshot used ~$695 — a placeholder/OCR; $675 is the confirmed value.)
Gap basis ~1.08–1.11pp applied to overlap_action_leads base (~3.83M in the summary view).

---

## Q05 — CRV economics on overlap (crv_economics_on_overlap.sql) — overall row

4 cohorts = CRV arm (Action/Control) × PCL overlap (yes/no). Converters only.
| metric | action_no_ovlp | action_with_ovlp | control_no_ovlp | control_with_ovlp |
|---|---|---|---|---|
| n_accounts | 102,185 | 68,932 | 4,466 | 2,876 |
| n_transactions* | 1,216,051 | 694,422 | 45,869 | 23,193 |
| txns_per_acct* | 11.90 | 10.07 | 10.27 | 8.06 |
| mean_principal_per_acct | $6,698 | $5,689 | $5,862 | $4,781 |
| mean_txn_principal | $978 | $991 | $973 | $1,005 |
| mean_apr | 6.63% | 6.60% | 6.64% | 6.62% |
| mean_term | 6.87 | 6.52 | 7.07 | 6.78 |

*DATA QUALITY FLAG: `n_transactions` and `txns_per_acct` are inflated ~1.74× by a wave-fanout join (used for n_waves). True plans/account ≈ mean_principal_per_acct ÷ mean_txn_principal ≈ **6.85**, not 11.9. n_accounts, means, APR, term are clean.
Per-unit economics flat across all 4 cohorts → no product-mix/risk confound. Overlap propensity even (40.3% Action vs 39.2% Control of converters).
mean_principal_per_acct = total installment $ per client across all their plans in window (cumulative, not per purchase).

---

## Q06 — Net economics counts (net_economics_counts.sql) — overall row

PCL-lead-centric. Both PCL responders AND CRV responders share the same per-arm denominator (n_pcl_leads_*_overlap).
| metric | Action overlap | Control overlap |
|---|---|---|
| n_pcl_leads | 6,694,005 | 348,293 |
| pcl_responders | 1,052,246 | 58,391 |
| crv_responders | 127,166 | 5,096 |

Derived: PCL rate 15.72% (A) vs 16.76% (C) → gap 1.05pp (cross-checks Q04's 1.08pp ✓).
CRV rate 1.90% (A) vs 1.46% (C) → CRV incremental 0.44pp (the loss side; Control 1.46% = organic CRV uptake).
Per 1,000 overlap accts: running CRV buys +4.4 CRV but costs −10.5 PCL.
NET (full overlap base, $675 PCL / $36 CRV): PCL recovered ~70,000 × $675 = ~$47.2M; CRV forgone ~29,224 × $36 = ~$1.1M → **NET ≈ +$46.2M**. Recommendation HOLDS (CRV loss ~2% of PCL gain) IF CRV unit is thousands-basis.
Calculator: crv_pcl_net_calculator.xlsx (windowed, Include-flag month selector, editable multipliers; 2026-05 excluded as immature).
OPEN: (1) confirm CRV NIBT thousands vs millions at source — flips net pos/neg. (2) reconcile incremental-PCL base: 42,407 (headline, ~3.83M base) vs ~70,000 (Q06 full 6.69M base). (3) PCL value ideally on same 5-yr-NIBT basis as CRV.

## Q07 — Substitution test (substitution_test.sql) — overall

Decomposes overlap leads by conversion outcome, per arm. Reconciles exactly with Q06
(PCL converters = pcl_only+both; CRV converters = crv_only+both).
| outcome | Action (n) | Action % | Control (n) | Control % |
|---|---|---|---|---|
| crv_only | 74,113 | 1.107% | 2,836 | 0.814% |
| pcl_only | 999,193 | 14.927% | 56,131 | 16.116% |
| both | 53,053 | 0.793% | 2,260 | 0.649% |
| neither | 5,567,646 | 83.174% | 287,066 | 82.421% |
both-order (Action): crv_first 27,295 / pcl_first 23,745 / same_day 2,003 (~51/49, no strong order signal).

**Substitution vs incremental (the 72/28):** work in RATES not counts (Action/Control bases differ).
PCL converters (pcl_only+both): Action 15.720% vs Control 16.765% → stop-CRV gain = +1.045pp.
Recovered PCL comes only from the two non-PCL states that shrink when CRV stops:
  crv_only 1.107%→0.814% = −0.293pp (substitution, CRV→PCL swap)
  neither  83.174%→82.421% = −0.753pp (net-new, was converting nothing)
  0.293 + 0.753 = 1.046pp ≈ gain ✓
Split: net-new 0.753/1.045 = **72%**; substitution 0.293/1.045 = **28%**.
Takeaway: ~72% of recovered PCL is genuinely incremental (CRV was suppressing it to zero), only ~28% is a CRV↔PCL swap. Also: Action "neither" higher (83.17% vs 82.42%) → CRV slightly depresses total conversion. Strengthens recommendation; pre-empts the "you're just swapping products" objection.

## Q08 — Decile concentration (decile_distribution.sql) — overall, on NEW_DECILE (cv_score model)

WHERE is the cannibalization concentrated across PCL propensity. new_decile is cleanly monotonic
(decile 1 = highest PCL propensity ~41% conv, decile 10 = lowest ~4.6%; decile 0 = special/unscored, 2.1M leads, low gap — exclude from ranked reads). Gap = control − action PCL rate.
| new_decile | PCL gap (C−A) | CRV lift (A−C) |
|---|---|---|
| 1 | **+3.04pp** | +0.85 |
| 2 | **+2.15pp** | +0.73 |
| 3 | +0.93 | +0.65 |
| 4 | +0.61 | +0.55 |
| 5 | +0.57 | +0.51 |
| 6 | +0.34 | +0.32 |
| 7 | +0.36 | +0.33 |
| 8 | ~0 | +0.13 |
| 9 | ~0 | +0.11 |
| 10 | +0.38 | +0.10 |
LEARNED: cannibalization is **top-heavy** — concentrated in high-propensity PCL deciles (1–3), declining monotonically to ~0 by deciles 8–9. CRV steals the most PCL from the customers most likely to take PCL. Value trade worst for CRV in the top deciles (decile 1: ~3.04pp PCL @ $675 lost vs ~0.85pp CRV @ $36 gained ≈ 65× in PCL's favor). → SURGICAL THROTTLE MAP: suppress CRV on high-PCL-propensity deciles (1–3); bottom deciles CRV is ~free. Output shape: arm-columnar per decile (n_action_overlap/pcl_resp_action/crv_resp_action + control + no_overlap); two statements (new_decile, decile). Q06 idiom only (no UNION/COUNT/cohort-string — those caused the 2616 overflow).

Q08 results for BOTH models (new_decile and old decile) captured from screenshot 2026-06-01.

Old `decile` (model_score) model — PCL gap (Control − Action) per decile, captured 2026-06-01:
| decile | leads | PCL gap (C−A) |
|---|---|---|
| 1 | 575,025 | +2.34pp |
| 2 | 637,930 | +0.85 |
| 3 | 647,688 | +0.51 |
| 4 | 639,666 | +0.63 |
| 5 | 603,554 | +0.31 |
| 6 | 616,898 | ~+0.42 (approx, image quality) |
| 7 | 1,046,135 | ~+0.33 (approx) |
| 8 | 654,358 | +0.35 |
| 9 | 482,286 | +0.10 |
| 10 | 641,106 | +2.44pp |
Old `decile` model is NOT monotonic — heavy at both ends (deciles 1 and 10), because both are high-PCL-propensity in that model (decile 10 = ~41% conv, decile 1 = ~26%). Use NEW_DECILE (cleanly monotonic, 1=highest) for recovery math / throttle targeting. Old-decile deciles 6,7 gaps are approximate (image quality).

## Q09 — Arrival order (timing_within_overlap.sql) — monthly + overall

Deployment order: earliest overlapping CRV offer_start vs PCL treatmt_strt. crv_first/pcl_first/same_day × arm, counts only (gap in Excel). NOT the same as Q07's order (Q07 = CONVERSION order among dual-converters; Q09 = DEPLOYMENT order, all overlap leads).
Overall: crv_first ~89% of leads, pcl_first ~10%, same_day ~1% (noise).
Gaps from RAW COUNTS (Andre): crv_first ≈ **1.0pp**, pcl_first ≈ **0.95pp**. (Discard earlier 0.8/0.9 — those were subtracted off rounded screen rates, wrong. Always compute the gap from raw responder/lead counts, not displayed %.)
LEARNED: cannibalization gap is ~the SAME whether CRV or PCL arrived first (~1.0 vs ~0.95pp) → mechanism is **concurrent competition, not first-mover**. CRV suppresses PCL by being co-present in the window, regardless of sequence. The 89% crv_first is structural (CRV always-on, ~90d windows) + left-censored at Oct-2024 start — do NOT present it as a finding; DO present "gap independent of order → not a timing artifact." Edge months unreliable (Oct-24 left-censor; Apr/May-26 immature) — read the middle.

## Q12 — Dose-response + time-to-convert (overlap-day buckets × arm) — RAN 2026-06-01

Overlap-day buckets (1-3, 4-7, 8-14, 15-21, 22-30, 31-45, 46-60, 61-90) × arm. Dropped PERCENTILE_DISC — blew spool; kept single-pass mean only. Pivoted three ways in Excel: conversion rate (RR), responder counts, mean days-to-convert.

**SAMPLE:** responders concentrate overwhelmingly in long-overlap buckets (31-45, 46-60, 61-90). Short buckets (1-3, 4-7) are tiny n (Control only 58 / 272 responders total across 20 months) = noise. Both campaigns run continuously, so short overlaps are rare edge cases. Trust 06/07/08 buckets only.

**NO DOSE-RESPONSE (the headline).** Rate gap Control−Action in the well-powered buckets is flat ~1pp: +1.04 (31-45), +1.10 (46-60), +0.89 (61-90). Matches Q04's 1.08pp. Suppression is invariant to overlap length — a fixed concurrent-exposure penalty, not a cumulative dose. Consistent with Q09 (order-independent). Mechanism = both offers live at once, not CRV grinding PCL down over time.

**CONFOUND — read vertical not horizontal:** both arms' rates DECLINE across buckets (Action 18.6%→12.9%, Control 21.3%→13.8% from bucket 1-3 to 61-90). Control has no CRV deployment, so this decline is NOT cannibalization — it's selection (longer overlap windows select lower-converting leads/campaigns). The signal is the within-bucket Action-vs-Control gap (vertical), never the across-bucket slope (horizontal).

**INTENSIVE MARGIN (days-to-convert):** Action converts ~2-4 days slower than Control in every overlap bucket (overall: Action 28/25/22/22/21/21/20/28 vs Control 24/22/21/20/19/19/18/26 across buckets 1-3…61-90). Right-censoring makes this a LOWER BOUND on the slowdown. Read down columns not across (the 61-90 mean is inflated by long PCL window length, a mechanical confound). Per-month × short-bucket cells have wild outliers (Control 78, 66) from small n — ignore; trust overall row.

**NET:** rate side confirms ~1pp suppression invariant to overlap length; days side adds survivors convert 2-4 days slower. Both point the same way, neither depends on dose. "Stop CRV on IM" recommendation holds regardless of overlap duration.

## Q11 — Throttling scenarios (Statement 2: PCL benefit by frequency_cap × decile) — RAN 2026-06-01

Statement 2 (PCL benefit) first run. Statement 1 (CRV cost) not yet captured.
Population: total_pcl_overlap_leads ALL = **6,694,005** (both models agree). Grain = acct × PCL-wave (NOT distinct customers — a customer recurs across waves).

**leads_freed by lifetime cap (ALL):**
| cap | leads_freed | % |
|---|---|---|
| cap2 | 3,834,812 | 57% |
| cap3 | 2,790,946 | 42% |
| cap4 | 1,829,259 | 27% |
| cap5 | 872,428 | 13% |

**OPEN ISSUE 1 — lifetime cap ≈ near-kill.** A lifetime cap of 2 frees 57% of overlap leads because over 20 months of weekly redeployment "keep first 2 contacts ever" approximates "stop CRV after month 1". Lifetime cap is NOT a realistic throttle. NEED rolling-window cap (max N per 30/90 days) for a credible throttle story. Lifetime-cap freed numbers overstate a real frequency cap.

**OPEN ISSUE 2 — both decile models have a junk bucket.** new_decile dumps 2,108,440 leads (31%) into bucket 0 (unscored) + a -99999 sentinel row (1 lead); only ranks 4.6M of 6.7M. Older `decile` model has no 0 bucket but decile 7 is anomalously large (1,040,126 vs ~600–650K for other deciles) — likely its unscored-default dump; confirm with model owner. ~30% of leads not genuinely propensity-ranked either way. Lean on `decile` model (full coverage) over new_decile pending decile-7 confirmation.

**HOW TO USE (reminder):** recovered PCL = pcl_leads_freed[decile] × gap[decile] (gap from Q08, heavy in deciles 1–3, ~0 by 8–10). NOT the pcl_responders_already_in_freed_leads column (already-converted, not recovery). Net throttle value = PCL recovered (× $675, Q06) − CRV forgone (Statement 1 crv_responders_in_removed_contacts, net of Q07 28% swap, × $36).

NEXT: build rolling-cap version; capture Statement 1 (CRV cost) numbers.

---

## STATUS
- s7 RUN 2026-06-12 (pic 142023): it_item_id vs promotion_id — ZERO disagreements; ids_agree IOS 45,869,997 ev (99.3%) / ANDROID 13,892,145 ev (99.5%); promotion_id MISSING on 304,891 IOS + 75,845 ANDROID + 45 WEB events that item_id still captures. DECISION: it_item_id ('i_'+id) = THE identity key, all queries re-keyed (Andre's call, verified). Cross-wire row from s5b photo = misread.
- s6 RUN 2026-06-12 (pic 141209): CONTRADICTION SOLVED — it_promotion_id is float-cast on Android ('87342.0', len 7-8) vs clean on iOS ('87342'); all string IN-lists excluded Android (~27% of iOS volume: 87342 = 9,921,109 Android events under '87342.0' vs 638 under '87342'). Fix = numeric cast, applied to s1/s3/s4/Q20/Q24 + canon + s2. iOS-only caveat RETRACTED (filter artifact, not tagging gap). Arm comparisons from prior runs remain valid (iOS subpopulation); reach levels + Q24 freq-0 bucket revise on rerun. Also: ANDROID NULL promotion_id row = 23,224 events (minor).
- s5b RUN 2026-06-12 (pic 140531): ANDROID TRACKED UNDER THE SAME PROMOTION IDS — i_android_credit_card_details_m1 carries 87342 (9,916,438 ev / 508,199 clients), 156788 (2,671,862 / 173,763), 162326 (456,932 / 27,666), 282901, 289499, 289661; item names lowercase pb_cc_all_* convention; it_item_id = 'i_'+id both platforms. NO allowlist change needed; Q20/Q24 filter on promotion_id without platform filter → likely captured Android all along. CONTRADICTS s4 (87342 ANDROID = 638) — s4 photo was cut off; RERUN s4 to tiebreak before retracting the iOS-only caveat (s2/SQL headers currently carry it). Exact location strings captured: i_IOS_Credit_Card_Details_M1 / i_android_credit_card_details_m1 / WEB slot Credit_Card_Txn_Details_Marketing2_AreaOMNI (hosts LTY_REW Avion loyalty banners, NOT ours — web out of scope for CRV/PCL). VERIFY in source xlsx: one Android row showed promotion_id 162326 paired with it_item_id i_156764 (cross-wired ids or photo misread).
- s5 RUN 2026-06-12 (pics 133622/133725/133742): ANDROID IS TRACKED — case (b) confirmed. App-wide view_promotion: IOS 844,266,400 ev / 6,191,222 clients / 169 ids; ANDROID 275,992,948 ev / 2,227,789 clients / 214 ids; WEB 722,581,927 ev / 0 promotion_ids (web banners use other id fields). Android card slot exists: android_credit_card_details_m1 = 124M ev / 1.6M clients; also a location labeled iOS_Credit_Card_Details_M1 under platform ANDROID at 168M ev (label reuse or column artifact — verify). Stmt 3 shows Android item names in lowercase bcc_* convention + new numeric ids (289681, 290082, 289564, 293394, 294396, 1284439, 289499...) = candidate Android twins of our banners; CONTRADICTION FLAGGED: photo showed 87342 with 9.9M Android-location events vs s4's 638 platform-ANDROID events — id column read suspect. Tiebreaker = s5b_android_id_map.sql (pushed): id × platform × location for the card-details slots. IMPLICATION: GA4 engagement coverage can ~double once Android ids are mapped into the allowlist; until then all engagement numbers remain iOS-only. Side-finding: the card-details slot also hosts other campaigns (FIFA win-ticket, pcd_ppcn, joint) — attention competition is multi-banner, not just CRV vs PCL.
- Q24 RUN 2026-06-12 (pics 131923 Stmt1 / 131947 Stmt2; post-patch: view_promotion impressions, 12-id allowlist, capped CRV offer window): HEADLINE — suppression concentrates in FRESH PCL targets: within-bucket Control−Action conversion gap = +3.0pp at touch-1 (58.8% vs 55.8%), +3.3pp at touch-2 (45.9% vs 42.6%), +1.6pp at touch-3, ~0 at touch-4+. Stmt 2 reach-1 bucket: Control 31.1% vs Action 27.9% (+3.2pp). Implies targeted suppression test (fresh dual-eligibles) is the sharp experiment. GUARDS: (1) the down-column frequency gradient (59%→2%) is SURVIVORSHIP (responders exit program), never present as fatigue; (2) Stmt 2 freq-0 bucket (54% of clients, ~22% conversion, ~zero clicks) conflates true non-delivery with the Android GA4 hole — do not quote until s5 sizes it; conversion clearly has non-banner paths. Stmt 1 ran with buckets 1–10 (not 1-5+); Stmt 2 3+ bucket empty/absent.
- s4 surface check RUN 2026-06-12 (pic 125511): platforms IOS+ANDROID only, NO web. CRITICAL: effectively iOS-ONLY — 87342: 35,978,692 iOS events vs 638 Android (same ratio all PCL ids) → Android app does not fire these events; GA4 engagement covers the iOS slice of the mobile cohort only (mandatory caveat on all engagement numbers; Android-tagging question for digital team). New allowlist ids (87340/87343/87344/167715/167716/167717/289698) = ZERO rows in Feb–Apr 2026 (inactive in window) → prior CRV GA4 numbers NOT understated; expanded list matters for future windows. s2_code_selection.md is now FINAL — all confirmations closed; track closed; Q24 patched per contract.
- s3 Stmt 3 drill-downs RUN 2026-06-12 (pics 124609 CRV client / 125143 PCL client): twin pattern confirmed at raw-stream level — every render fires view_item(not set)+view_promotion(creative) 3–8ms apart; NEW FINDING: CLICKS double-fire too — one tap = select_promotion(not set) + select_promotion(p_View Offer) ~2ms apart, same session/promo. One client logged 4 view-pairs in a single session (re-renders). COUNTING DOCTRINE locked into s2_code_selection.md: never raw events (2× inflated + fidget-confounded); reach = distinct clients, intensity = distinct sessions/view-days, clicks = distinct clients classified via the creative-carrying row, CTR = clicking clients ÷ viewing clients.
- s3 (channel_bulletproofing) RUN 2026-06-12 (pics 124230/124240): TWIN TEST PROVEN — view_item & view_promotion fire within 1 second in 99.8% of sessions (CRV 20,030,440 of ~20.08M; PCL 5,809,695 of ~5.82M; residual 0.2% splits 50/50 = clock jitter). Click sanity passed (view_promotion precedes select_promotion 99.9%: CRV 429,889 vs 594; PCL 227,476 vs 230). VERDICT: view_item = co-fired implementation artifact, DISCARD; view_promotion = exposure event. s2_code_selection.md confirmation #1 closed. Remaining before s2 final: s4 surface check (platform per banner id) + volume refresh with corrected 16-id allowlist.
- CB04 ADDED 2026-06-12 (cb04_journey_vocabulary_census.sql): GA4 journey-vocabulary census — banner-session event/screen/details taxonomy, creative-name p.../n... prefix inventory, and documented CLI dashboard string presence check with drift-detection family totals.
- CB04 Stmt 1 RUN 2026-06-12 (pics 114215/114252/114300; 114244+114316 unreadable, mid-list rows, not re-shot): vocabulary census of sessions touching PCL/CRV banners. Findings: (1) ep_details confirmed concatenated label "event_name - source - campaign_string" (salesforce) or "event_name - trigger_name" (behavioural) — use for reading, never for logic; (2) CLI banner campaign strings found: pb_cc_all_23_06_rbc_vc_limitincrease_cli_pa (~142K events / ~93K clients) and ..._limitincrease_cli_static (~21K) — likely the SF_PA / SF_Static dashboard segment keys; whether cli_pa IS the PCL banner = OPEN, check ep_details on it_promotion_id IN (PCL list); (3) DATA-QUALITY FLAG: some rows appear to pair event_name=view_promotion with ep_details starting "select_promotion -" (and vice versa) — photo misalignment vs real tagging inconsistency UNRESOLVED, needs one clean screenshot; (4) unrestricted census = 300 rows mostly unrelated banners — prefer Stmt 2/Stmt 3 outputs for screenshots.
- Channel bulletproofing track CREATED 2026-06-12: new folder campaigns/CRV/channel_bulletproofing/ with CB01–CB03. Verifies the mechanism claim "CRV does not cut PCL banner reach; loss is in click-through." CB01 (cb01_impression_event_resolution.sql) is MANDATORY — resolves view_item vs view_promotion ambiguity flagged in Q20 header, then re-tests equal PCL reach by arm (view_item vs view_promotion side by side, Q20 universe verbatim). CB02 (cb02_impression_intensity_by_arm.sql) is CONDITIONAL on CB01 confirming the event and Q24 Stmt 2 showing asymmetry — tests impression frequency (impression-day buckets per arm), ruling out intensive-margin slot arbitration. CB03 (cb03_position_order_check.sql) is OPTIONAL — uses full ecommerce table (not _reduced) to test position fields (it_creative_slot / it_item_index) and within-session PCL-vs-CRV impression ordering. Q19 run status: NOT RUN (absent from validated/run list and no results captured in this catalog). Q19 used it_item_name strings (not it_promotion_id) and documented 0 PCL rows; it does not test view_item vs view_promotion nor reach parity by arm — CB01 is not redundant. Numbered-Q count in main analysis FROZEN at Q27.
- Q27 ADDED 2026-06-11: conservative gap — Q04 logic with co-applicant accounts EXCLUDED (CIDM acct_no join, CLNT_NO_A present and <> CLNT_NO; no_cidm_match kept). Full CI/z output. Expected ~1.04pp per Q25 arithmetic; this formalizes it for the record.
- Q25b RUN 2026-06-11 (pics 114644): CIDM 12.13M accts, 1 row/acct (rows_=accts everywhere). Distinct co-applicant (CLNT_NO_A different) = 2,158,348 accts (~17.8%). IDENTICAL_IND decode: Y≈same person (44,585 same_clnt_no; +10,019 coapp_null; +466 different), N = no-coapp (9,915,465) or distinct coapp. Premise validated: co-apps DO carry their own clnt_no; derived has_coapp (CLNT_NO_A <> CLNT_NO) is the right flag, not the indicator alone.
- Q25 RUN 2026-06-11 (pic 114639): co-app scale on gap population. Action: has_coapp 203,211 leads / 9,875 resp; no_coapp 6,673,682 / 1,078,480; no_cidm 1,167. Control: has_coapp 10,673 / 555; no_coapp 345,943 / 59,508; no_cidm 69. → has_coapp share ~2.95% action vs ~2.99% control (BALANCED). Gap excl. co-apps = 17.20% − 16.16% = 1.04pp vs 1.02pp all-in → HEADLINE GAP ROBUST to co-applicants; no exclusion rerun needed. PCL clnt = CIDM primary in >99.98% of matched leads (pcl_clnt_is_coapp ≈ 0) — PCL targets primaries. Co-app accts respond ~5% vs ~16-17% non-coapp (behavioral, both arms equally). NOTE: totals (6,878,686 / 356,723) ≈ +6% vs Q04's 2026-06-01 run — open-ended date window grew with data refresh.
- Q26 RUN 2026-06-11 (pic 114649, pre-pcl_month version): behavior mix by arm. Action: Transactor 78.1% / Revolver 21.5% / Dormant 0.38%. Control: 78.4% / 21.3% / 0.38% → MIX IDENTICAL, comparison FAIR. Stratified gap: Transactor 15.56−14.55 = 1.01pp; Revolver 21.46−20.34 = 1.12pp — gap holds WITHIN both segments, not composition-driven. no_overlap is the opposite profile (Revolver 69.8% / Transactor 29.5%) — CRV-overlap population is transactor-heavy vs never-CRV. Lead totals reconcile EXACTLY with Q25 by arm (grain clean; behavior nulls = 7 rows total). pcl_month added after this run for the monthly cut.
- Q25 ADDED 2026-06-11: co-applicant SCALE on the Q04 gap population — overlap leads by arm × has_coapp/no_coapp/no_cidm_match (source DTZTAU.CIDM_CARDS_ACCT_ATTRS, CLNT_NO vs CLNT_NO_A; PRIMARY_COAPP_IDENTICAL_IND carried raw, decode unverified). responders per cell → gap recomputable excluding co-app accounts. CRV decis_resp has NO clnt_no, so CRV-side human-match is not derivable from curated; this is the empirical ceiling. No CRV tech spec required.
- Q26 ADDED 2026-06-11: behavior mix (usg_bhvr_seg_at_cyc_cd: Dormant/Transactor/Revolver from D3CV12A.CR_CRD_RPTS_ACCT) by overlap arm incl. no_overlap — comparison-fairness/balance check, segment taken at month-end BEFORE treatmt_strt_dt (pre-treatment). responders per cell → stratified gap read. Sanity: segment lead totals must reconcile to Q04 arm totals (table grain unverified).
- Q22/Q23 RETIRED 2026-06-11. One-time diagnostics for the Both-category duplicate-row / co-exposure investigation (Q22 raw-event drilldown, Q23 view→click journey); findings captured in this catalog, files deleted per the Q10 convention. Numbers 22/23 are permanent gaps.
- Q24 ADDED 2026-06-11: PCL contact frequency (deployment-level) × CRV overlap status (overlap_action / overlap_control / no_overlap), Q20 universe and overlap convention. OPEN CAVEAT: CRV co-applicant targeting may inflate overlap — same account identifiers, different humans (CRV→co-applicant, PCL→primary). Pending CRV technical spec; candidate fields `joint_acct` (both CRV tables) and `acct_relation` (install_details), values undecoded. Final design 2026-06-11: Stmt 1 = cumulative 20-mo touch number (Q11 convention) at the Feb-Apr leads, w/ engagement overlay + converters; Stmt 2 = CHANNEL-side frequency — clients by deployments where the banner reached them (0-3+), clicks + converters per bucket. View-days variant considered and rejected (login-behavior confound, unit doesn't chain with Stmt 1; retrievable from git if fatigue becomes the question). Co-app accounts excluded in both.
- Validated/run: Q00–Q09, Q11 (Statement 2), Q12.
- Q12 dose-response (overlap-day buckets × arm): RUN 2026-06-01.
- Q11 Statement 2 (PCL benefit by frequency_cap × decile, both models): RAN 2026-06-01 (first run, open issues — see above). Statement 1 (CRV cost) not yet captured.
- Q10 RETIRED 2026-06-01. Both CRV and PCL redeploy weekly (multiple waves/month), so "delay vs block" has no quiet recovery window — a delayed PCL conversion re-enters the next week's suppressed overlap; on a treadmill, delay = block. Additionally, the pair-grain cross-join double-counted one PCL CLI across every overlapping CRV wave, making the day distribution an artifact of wave cadence rather than a real delay signal. The delay objection is answered by Q04: pure delay would wash out in steady state, so the persistent ~1pp gap across 17/20 months is the signature of net loss, not deferral.
- Hypothesis HOLDS + strengthened: 1.08pp / 42,407 / ~$28.6M GROSS @ $675/conv; net ~+$46.2M; cannibalization concentrated top deciles, independent of arrival order, ~72% incremental.
- Open: (a) CRV NIBT/conversion same currency as PCL $675 (William emailed); (b) confirm CRV unit thousands vs millions; (c) reconcile incremental-PCL base 42,407 vs ~70,000.

---

## Q20 — Wide view/click table at client grain, MB channel — captured 2026-06-09

Source: pics/20260609_150635.jpg, captured 2026-06-09.

**Populations:** overlap_action 1,064,491; overlap_control 55,155; no_overlap 437,380.

| arm | view_group | in_group | click_both | click_crv_only | click_pcl_only | click_neither |
|---|---|---|---|---|---|---|
| no_overlap | Both | 3,583 | 372 | 250 | 832 | 2,129 |
| overlap_action | Both | 363,080 | 43,076 | 14,769 | 56,713 | 248,522 |
| overlap_action | CRV only | 31,130 | 69 | 4,787 | 68 | 26,206 |
| no_overlap | CRV only | 823 | 1 | 123 | 2 | 697 |
| overlap_action | Neither | 405,988 | 7 | 20 | 50 | 405,911 |
| overlap_action | PCL only | 58,211 | 72 | 29 | 11,893 | 46,217 |
| no_overlap | Neither | 214,483 | 0 | 0 | 155 | 214,328 |
| overlap_control | Both | 5 | 0 | 1 | 0 | 4 |
| overlap_control | CRV only | 1 | 0 | 0 | 0 | 1 |
| no_overlap | PCL only | 136,389 | 1 | 0 | 56,662 | 79,726 |
| overlap_control | Neither | 22,501 | 0 | 0 | 18 | 22,483 |
| overlap_control | PCL only | 22,190 | 0 | 0 | 7,931 | 14,259 |

**Key findings:**
1. View-group coverage: 80.64% (Action) / 81.04% (Control) of population; remaining ~19% have no banner-view row (no qualifying GA4 app activity in window) — same share both arms.
2. Control contamination: 6 of 55,155 control clients with any CRV view (0.011%) — randomization clean.
3. (Derived) Population-level click rates: PCL any-click Control 14.41% vs Action 10.52% (−3.9pp); CRV any-click Action 5.90% vs Control ~0%.
4. (Derived) Conditional: both-viewers PCL click 27.5% vs Control PCL-only viewers 35.7%. CRV-only viewers CTR on CRV = 15.6% (4,856/31,130) — corrects earlier rounded read of ~0%.
5. (Derived) Total any-banner click: Control 14.41% vs Action 12.36%; per banner-viewer 35.7% vs 29.1% — co-exposure lowers total engagement, not just reallocates.
6. (Derived) no_overlap PCL-only viewers click PCL at 41.5% vs overlapped control 35.7% — selection difference, descriptive only.

---

## Q24 — PCL contact frequency × CRV overlap — RUN 2026-06-12

### Statement 1 (overlap_status | pcl_contact_freq_20mo | clients | deployments_in_window_total | converters | view_users | click_users | view_days_total | converters_viewed)

| overlap_status | pcl_contact_freq_20mo | clients | deployments_in_window_total | converters | view_users | click_users | view_days_total | converters_viewed |
|---|---|---|---|---|---|---|---|---|
| no_overlap | 1 | 100,494 | 100,528 | 53,343 | 44,225 | 22,106 | 232,499 | 23,866 |
| no_overlap | 2 | 87,981 | 94,586 | 38,473 | 41,132 | 18,576 | 256,462 | 18,433 |
| no_overlap | 3 | 91,498 | 102,665 | 32,338 | 44,201 | 18,208 | 321,089 | 16,067 |
| no_overlap | 4 | 62,994 | 73,899 | 12,745 | 30,219 | 9,947 | 249,545 | 6,597 |
| no_overlap | 5 | 57,706 | 67,875 | 7,779 | 27,099 | 7,448 | 229,976 | 3,904 |
| no_overlap | 6 | 65,163 | 78,461 | 6,909 | 30,499 | 7,766 | 272,315 | 3,515 |
| no_overlap | 7 | 68,557 | 86,746 | 5,228 | 31,244 | 7,484 | 296,343 | 2,511 |
| no_overlap | 8 | 64,592 | 80,733 | 3,453 | 27,409 | 6,013 | 254,304 | 1,585 |
| no_overlap | 9 | 73,374 | 88,792 | 4,090 | 32,187 | 6,896 | 299,301 | 1,905 |
| no_overlap | 10 | 33,674 | 66,816 | 1,027 | 15,643 | 3,739 | 222,555 | 533 |
| overlap_action | 1 | 42,095 | (illegible) | 23,504 | 20,433 | 8,570 | 114,343 | 11,082 |
| overlap_action | 2 | 43,732 | 52,561 | 18,634 | 21,692 | 8,902 | 156,697 | 9,342 |
| overlap_action | 3 | 47,239 | 61,680 | 14,026 | 24,914 | 8,557 | 219,375 | 7,484 |
| overlap_action | 4 | 34,704 | 47,900 | 6,049 | 18,571 | 5,227 | 184,303 | 3,422 |
| overlap_action | 5 | 32,642 | 45,210 | 3,959 | 17,157 | 4,204 | 168,774 | 2,256 |
| overlap_action | 6 | 38,331 | 54,287 | 3,910 | 20,288 | 4,710 | 208,077 | 2,169 |
| overlap_action | 7 | 40,538 | 60,712 | 2,805 | 21,467 | 4,814 | 237,322 | 1,561 |
| overlap_action | 8 | 39,066 | 59,394 | 2,247 | 19,832 | 4,258 | 217,041 | 1,244 |
| overlap_action | 9 | 44,211 | 66,423 | 2,239 | 22,493 | 4,682 | 250,141 | 1,231 |
| overlap_action | 10 | 26,037 | 51,907 | 656 | 13,591 | 3,097 | 190,441 | 365 |
| overlap_control | 1 | 2,223 | 2,223 | 1,307 | 1,132 | 675 | 6,629 | 676 |
| overlap_control | 2 | 2,358 | 2,777 | 1,083 | 1,185 | 632 | 8,932 | 583 |
| overlap_control | 3 | 2,455 | 3,148 | 767 | 1,318 | 598 | 12,013 | 411 |
| overlap_control | 4 | 1,714 | 2,391 | 300 | 920 | 346 | 10,325 | 179 |
| overlap_control | 5 | 1,673 | 2,323 | 195 | 897 | 278 | 9,834 | 108 |
| overlap_control | 6 | 1,874 | 2,647 | 195 | 1,036 | 331 | 12,019 | 105 |
| overlap_control | 7 | 1,909 | 3,111 | 150 | 1,086 | 312 | 13,477 | 87 |
| overlap_control | 8 | 1,988 | 3,032 | 132 | 1,002 | 297 | 12,838 | 82 |
| overlap_control | 9 | 2,263 | 3,364 | 120 | 1,158 | 289 | 15,534 | 70 |
| overlap_control | 10 | 1,316 | 2,622 | 24 | 708 | 208 | 11,569 | 14 |

### Statement 2 (overlap_status | banner_contact_freq | clients | deployments_total | click_users | converters)

| overlap_status | banner_contact_freq | clients | deployments_total | click_users | converters |
|---|---|---|---|---|---|
| no_overlap | 0 | 382,175 | 452,120 | 269 | 86,469 |
| no_overlap | 1 | 268,331 | 277,927 | 90,429 | 74,941 |
| no_overlap | 2 | 55,527 | 111,054 | 17,485 | 3,975 |
| overlap_action | 0 | 188,157 | 257,624 | 77 | 37,873 |
| overlap_action | 1 | 127,835 | 139,379 | 37,730 | 35,676 |
| overlap_action | 2 | 72,603 | 145,206 | 19,214 | 4,480 |
| overlap_control | 0 | 9,501 | 12,950 | 8 | 1,958 |
| overlap_control | 1 | 6,733 | 7,270 | 2,686 | 2,096 |
| overlap_control | 2 | 3,709 | 7,418 | 1,296 | 219 |
