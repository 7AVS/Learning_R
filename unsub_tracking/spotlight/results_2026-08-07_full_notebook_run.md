# Full notebook run — results transcribed from Andre's screenshots, 2026-08-07

Source: 18 screenshots of `unsub_analysis_notebook.ipynb` run in work env (Brain_Pyspark_Local_Mode),
notebook state = commit `6d6a2a2` (Q5 v2 rate charts, Q4 v2 extended bands, Q5a v3 per-email).
Transcribed same-day; screenshots deleted after this. Values eyeballed from charts are marked ~.

## Q0 — Monthly sends + unsub rate by LOB (Aug 2025 – Jun 2026)
| LOB | total sends | avg unsub rate |
|---|---|---|
| LOYALTY | 126.8M | 0.37% |
| PSI | 41.2M | 0.20% |
| CARDS | 37.9M | 0.18% |
| PBA | 25.2M | 0.12% |
| UNKNOWN | 18.9M | 0.15% |
| COMMERCIAL | 8.5M | 0.21% |

## Q1 — Top 10 by volume / by rate, Jan–Apr 2026 (senders ≥ 10K for rate ranking)
By volume (mne, senders, unsubs_attributed, rate%): VRE 4,170,680 / 37,699 / 0.90 · VME 3,410,953 / 22,068 / 0.65 · PAL 3,348,033 / 14,936 / 0.45 · VRG 1,845,113 / 13,124 / 0.71 · FWC 2,924,630 / 11,476 / 0.39 · TAO 2,028,274 / 6,168 / 0.30 · PCQ 771,197 / 4,468 / 0.58 · SBB 652,343 / 3,788 / 0.58 · IDE 866,889 / 3,676 / 0.42 · PCL 1,185,332 / 3,639 / 0.31

By rate: VMB 1.16% (165.1K) · VRE 0.90% (4.2M) · PRA 0.80% (12.3K) · VRG 0.71% (1.8M) · VME 0.65% (3.4M) · OBS 0.61% (47.5K) · SBB 0.58% (652.3K) · PCQ 0.58% (771.2K) · BBO 0.55% (30.7K) · POB 0.52% (36.2K)

PCQ is the only Cards-pod campaign in BOTH top-10s.

## Q2 — 12-month curve (Cards MNEs list n=31 printed in cell [3])
| ym | ent_unsubs | cards_unsubs | ent_sends | cards_pct |
|---|---|---|---|---|
| 202508 | 75,288 | 3,623 | 21,311,132 | 4.8 |
| 202509 | 77,272 | 4,333 | 20,530,526 | 5.6 |
| 202510 | 73,390 | 6,960 | 23,839,289 | 9.5 |
| 202511 | 68,374 | 4,506 | 25,133,175 | 6.6 |
| 202512 | 70,562 | 5,205 | 23,844,443 | 7.4 |
| 202601 | 67,512 | 4,602 | 23,469,193 | 6.8 |
| 202602 | 64,349 | 7,961 | 32,263,566 | 12.4 |

Q2b Cards share by month (full line): 4.8, 5.6, 9.5, 6.6, 7.4, 6.8, 12.4, 15.6, **peak 17.6 (202604)**, 13.0, 8.4, 7.9. Mature avg 9.9%. 202606–202607 shaded immature (bridge lag). Note printed: Cards deduped unique-person share (20.5%) is a different basis (unique clients, not events).
Q2c: FWC unsubs followed FWC send waves with 0–1 month lag (bars ~2.5M/5.2M/5.3M/2.6M sends; unsub line peaks ~6.8–7.0K).

## Q3 — action types + per-MNE (Jan–Apr 2026)
Q3a volume: FWC 11.5K · Deepen(7) 6.8K · Attract(5) 5.2K · Onboard(4) 1.1K · Retain(1) 340 · Operational(1) 0.
Q3b rate: Attract 0.56% (n=934.9K) · FWC 0.39% (2.9M) · Deepen 0.28% (2.5M) · Onboard 0.28% (406.3K) · Retain 0.22% (155.9K) · Operational 0.00% (540, small base).
Q3e audience / Q3f rate per Cards MNE: FWC 2.9M / 0.39% · PCL 1.2M / 0.31% · PCD 831.3K / 0.25% · PCQ 771.2K / 0.58% · COB 297.4K / 0.26% · CRV 245.3K / 0.28% · WJR 155.9K / 0.22% · VBA 138.6K / 0.31% · VBU 87.4K / 0.18% · AUH 86.6K / 0.17% · MWA 83.0K / 0.33% · CEC 25.1K / 0.50% · BCO 23.6K / 0.26% · VLI 13.5K / 0.16% · POT 2.3K / 0.48%△ · MET 969 / 0.10%△ · (WNH 540 cut off).

## Q4 — in-window frequency (original)
Distribution (stayers% / unsubs%): 1-2 (n=1.4M): 27/48 · 3-5 (2.4M): 49/40 · 6-10 (1.1M): 22/11 · 11+ (68.6K): 1/0.
Rate: 1-2 0.74% · 3-5 0.35% · 6-10 0.22% · 11+ 0.14%.

## Q4-LB v1 — in-window bucket × pre-window history (cache pm_q4_lookback.csv)
| bucket | prior_contact | clients | unsubs | rate% |
|---|---|---|---|---|
| 1-2 | mailed before | 589,251 | 7,466 | 1.267 |
| 1-2 | new to Cards | 761,157 | 2,524 | 0.332 |
| 3-5 | mailed before | 2,168,221 | 7,795 | 0.360 |
| 3-5 | new to Cards | 215,825 | 615 | 0.285 |
| 6-10 | mailed before | 1,052,936 | 2,297 | 0.218 |
| 6-10 | new to Cards | 27,199 | 64 | 0.235 |
| 11+ | mailed before | 68,391 | 98 | 0.143 |
| 11+ | new to Cards | 215 | 0 | 0.000 |

(6-10/new clients count confirmed from 2026-08-06 transcription of the same cached table.)

## Q4 v2 — bands on TOTAL Cards emails Oct 2025 – Apr 2026 (NEW, cache pm_q4_lookback_v2.csv)
| band | n | stayers% | unsubs% | rate% |
|---|---|---|---|---|
| 1-2 | 935.5K | 19 | 22 | 0.50 |
| 3-5 | 894.2K | 18 | **43** | **1.00** |
| 6-10 | 2.6M | 54 | 30 | 0.24 |
| 11-20 | 412.8K | 8 | 5 | 0.24 |
| 21+ | 1.6K | 0 | 0 | 0.12 |

**READ: with honest 7-month bands, the risk peak MOVES from "1-2" to "3-5" (1.00%, 2× any other band). Unsubs are over-represented 43% vs 18% of stayers in 3-5; under-represented in 6-10 (30 vs 54). "One email and they leave" is dead — the bleed zone is low-but-repeated contact.** Caveat printed on chart: unsub stops the count → leavers tilt low-band partly by construction (some of the 3-5 peak may be truncated would-be-6-10 clients).

## OVERLAP (FIFA isolated) — mutually exclusive exposure groups, Jan–Apr
Sum 6,962,584 of ~10.4M mailed. Partial combos not shown: Cards+FIFA 95,035 · Cards+Loyalty 806,845 · FIFA+Loyalty 1,378,428.
| segment | clients | unsub_cards | unsub_fwc | unsub_loy | unsub_any | own-list rate | avg emails |
|---|---|---|---|---|---|---|---|
| Cards only (ex-FIFA) | 1,151,734 | 6,330 | 0 | 940 | 7,169 | 0.55% | 2.4 |
| FIFA only | 224,158 | 19 | 2,390 | 21 | 2,419 | 1.07% | 4.5 |
| Loyalty only | 2,079,375 | 379 | 0 | 25,175 | 25,270 | 1.21% | 4.7 |
| All three | 1,227,009 | — | — | — | — | Cards 0.18% / FIFA 0.28% / Loyalty 0.65% | Cards 2.8 / FIFA 4.2 / Loyalty 13.2 |

Deep-dive, single-side groups (clients mailed · own rate): Cards side: PCQ 507.2K·0.70% · FWC 319.2K·0.97% · PCL 316.2K·0.46% · WJR 103.8K·0.23% · COB 98.0K·0.29% · VBA 94.1K·0.31% · PCD 77.3K·0.42% · CRV 53.5K·0.52% · MWA 23.6K·0.40% · AUH 22.6K·0.32%. Loyalty side: VRE 1.3M·1.21% · VME 1.2M·0.82% · VRG 450.3K·1.20% · VMB 49.3K·1.75% · VO3 15.3K·0.04% · VJB△ 7.5K·0.12% · VMF△ 4.0K·0.12%.
Top combos (clients · unsub any-of-these): FWC+VME+VRE 424.4K·0.80% · FWC+VME+VRE+VRG 333.2K·0.88% · VME+VRE 326.4K·0.89% · FWC+VRE 236.7K·1.18% · FWC+VRE+VRG 223.4K·1.33% · VME+VRE+VRG 175.9K·1.23% · FWC+PCL+VME+VRE 127.4K·0.84% (rest cut off).

## ATTRITION (descriptive, groups not matched)
Chart 1 — Jun-2025 cardholders, exits by Jun 2026: STAYERS (n=3.2M): lost cards 1.63%, no longer present 1.30%. LEAVERS (n=6.3K held cards): lost cards 1.91%, no longer present 1.74%.
Chart 2 — whole mailed cohort (relationship presence, NOT card attrition): STAYERS still present 4.2M, no longer present 113,491 (2.6%). LEAVERS no longer present 752 (5.9%).

## PROFIT CHECK (two bases)
(a) survivors only: STAYER $811→$1,008 (+$197, +24.3%, n=4,202,840); LEAVER $576→$731 (+$155, +26.9%, n=12,037).
(b) everyone anchored, no-longer-present = $0: STAYER $795→$982 (+$187, +23.6%, n=4,312,667); LEAVER $550→$688 (+$138, +25.1%, n=12,763).
Finding survives on both bases; (b) is the reported number.

## D section (cohort 4,783,193 mailed by Cards on/before anchor). Gates: "SANITY GATES PASSED: cohort anchor, sub-group sum, delta arithmetic."
D1 spend (DFP-matched cardholders): STAYERS $2,147→$2,195 (+$48, +2.3%, n=4.8M); LEAVERS_ALL $2,360→$2,333 (−$27, −1.1%, n=14.1K).
D2b product count: STAYERS 2.44→2.49 (+0.051); LEAVERS_ALL 1.99→2.06 (+0.073).
D5 migration (rows=then, cols=now, row%): STAYERS (n=3.3M): Revolver 74.1/22.1/3.8 · Transactor 17.5/78.5/4.0 · Dormant 16.9/24.1/59.0. LEAVERS_ALL (n=6.3K): Revolver 69.1/27.1/3.9 · Transactor 15.8/80.8/3.4 · Dormant 16.5/24.5/(cell not legible in screenshot, ≈59 by residual). Excludes other/no_data: 1,384,280 stayers, 7,547 leavers.
D6 tier×segment leaver rate (n): High: Rev 0.19% (737) · Trans 0.25% (1,961) · Dorm 0.25% (3). Mid: 0.16% (770) · 0.22% (1,495) · 0.11% (8). Low: 0.13% (503) · 0.17% (639) · 0.16% (477). Total leavers in cohort: 14,140. (D5 n=6.3K vs D6 14,140: D5 is restricted to leavers with card-behavior segment data.)

## Q5a v3 — per-email propensity vs contact intensity by age (NEW; universe = Cards-mailed only)
| age | per-1k-emails | emails/client | n |
|---|---|---|---|
| <25 | 1.37 | 4.4 | 461.9K |
| 25-34 | 1.13 | 4.7 | 890.3K |
| 35-49 | 0.81 | 4.5 | 1.4M |
| 50-64 | 0.76 | 4.5 | 1.1M |
| 65+ | **1.11** | 4.4 | 745.4K |
| unbucketed | 0.00 | 1.3 | 96 |

Overall: 0.96 per 1k emails; 4.5 emails/client.

**READ (two findings):**
1. **Over-contacting is ruled out for age.** Contact intensity is flat (4.4–4.7 across every band). The young's higher unsub is genuine per-email propensity (1.37 vs 0.96 overall), not more chances to click.
2. **On the clean Cards-mailed denominator the age story is U-SHAPED, not "young only": 65+ jumps to 1.11 per 1k — ABOVE overall.** v1/v2 showed 65+ at ~0.99x/below-avg because their Cards series was diluted by all-RBC-mailed denominators (65+ are less likely to be Cards-mailed). Middle-age (35-64) is the low-risk core. This is a story change vs Q5a v1/v2 and the Aug-3 reference values.

## Ambiguities flagged during transcription
- Q2b peak: title says ~18%, annotation 17.6% at 202604 — trust 17.6 (confirmed in second screenshot).
- D5 leavers Dormant→Dormant cell illegible; ≈59% by residual, re-print from notebook if used in deck.
- Q0/Q2c bar heights are chart-eyeballed (~), not table-read.
