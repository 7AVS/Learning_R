# Experiment Design Report: VBU Propensity-Model Holdout Test

**Author:** Andre (design support: Cards measurement workbench)
**Created:** 2026-09-02
**Last Updated:** 2026-09-02
**Version:** v0.1, draft, pending CIDM feasibility answers

---

## 0. Footnote version (the whole experiment in four lines)

> Client-level random 70/30 holdout within the model-based VBU offers (~8–13K clients per
> offer per wave); the 30% holdout receives no VBU communication. Two waves: wave 1 fully
> powers the headline read (expected ~1.8–2.4% vs ~0), wave 2 replicates and firms the
> band-level calibration (bands 1–4 individually; bands 5–9 as one bottom-bucket upper bound).
> Cost of the answer: ~100–160 forgone upgrades per wave.

---

## 1. Executive Summary
- **Why test:** the VBU propensity model (CPX→AIB path) selects who receives the model-based
  upgrade offers. Its selections have never been causally validated. Today's ~5%
  not-communicated group is not a designed randomized control (see §7c) and is too small for
  band-level reads regardless.
- **What is tested:** whether communicating the model's selections causes upgrades, and
  whether those upgrades concentrate where the model's score says they will.
- **Success looks like:** treatment converts at the historical ~1.8% (NR) / ~2.4% (R_55) with
  holdout ≈ 0, and causal lift declines monotonically from band 1 (~3.4–7.9pp) toward ~0 at
  bands 5+.
- **How long / how safe:** 1 wave for the headline (read mature ~80 days after deploy),
  2 waves for band calibration and replication. Reversible at any wave boundary. Cost at the
  recommended 70/30: ~102–160 forgone upgrades per wave (dial table in §5).

## 2. Strategic Context
The model path is live BAU, monthly waves since at least January 2026 (workstation pivot,
2026-09-03; Jan is the earliest month shown, so the true start may predate it), tactics
consolidated 5→1 over Apr–Aug. The
business runs it without a causal value number. This test converts the model from "deployed"
to "measured": a defensible dollar/upgrade value for model-targeted communication, plus a
calibration read that tells CIDM whether the score cutoff is placed correctly. Rebate/offer
content (NR vs R_55 vs rule-based 35K) is explicitly OUT OF SCOPE: no arm varies the offer.

## 3. Hypothesis & Objectives
- **Population:** all clients selected by the propensity model for a model-based offer
  (AIB_25K_NR, AIB_25K_R_55) in the wave.
- **Primary Hypothesis (program value):** communicating the model's picks causes upgrades.
  - H₀: τ = 0, H₁: τ > 0 (one-sided; pre-justified because holdout conversion has a
    structural floor near zero, so negative τ is not plausible for this metric),
    τ = E[Y(Comm) − Y(NoTouch)], Y = target-product upgrade within the ~80-day response
    window. Read separately per offer.
  - Expected under H₁: NR ≈ +1.8pp, R_55 ≈ +2.4pp (vs ≈0 control; b3, mature Jun/Jul waves).
- **Secondary Hypothesis (HTE, the "is the model working" read):** causal lift declines
  monotonically with score band, from band 1 (≈ 3.4–7.9pp; June band-1 rates 3.44% NR /
  7.89% R_55) toward ~0 in the pooled bottom bucket (bands 5–9). Valid at band level
  because randomization is client-level; every pre-treatment band inherits it.
- **Success Criteria:** primary lift significant per offer (α = 0.05); band-1 lift
  significant and larger than bands 4+ lift.
- **Scope boundary (state, don't assume):** this design proves the model's selections respond
  and the response follows the score ordering. It does NOT prove the model beats random or
  rule-based targeting: nobody outside the model's selection is enrolled. That claim would
  need a below-cutoff arm: separate decision, out of scope.

## 4. Experiment Design & Assignment
- **Design Type:** RCT, simple (unstratified) client-level randomization. No band
  stratification needed: scores are stored at assignment, band reads are analytic.
- **Randomization Unit:** client.
- **Eligibility:** model-selected for AIB_25K_NR or AIB_25K_R_55 in the wave (CIDM waterfall
  as-is upstream of the split).
- **Treatment Arms:**

  | Arm | Description | Allocation |
  |-----|-------------|-----------|
  | Treatment | Communicated the model-assigned offer, BAU channels | 70% |
  | Holdout | NO VBU touch in any channel: not communicated, not moved to another offer/path | 30% |

  Arm assignment persists for the test's duration: a wave-1 holdout client stays held out
  in wave 2 (prevents contamination; re-entry is rare). CIDM to confirm.

  Allocation is a dial (§5): 70/30 recommended; 50/50 adds only marginal bands 5–7 reads at
  ~1.7× the cost.

- **Critical mechanics (CIDM asks, §9):** (1) the split must be a fresh random draw, NOT the
  existing NM mechanism, whose share is score-linked (§7c); (2) holdout clients receive
  nothing from VBU (suppression, not reassignment); (3) model score + band written to the
  ledger for BOTH arms at assignment (position @21,8 / band @50,2 verified 100% in t1).

## 5. Sample Size & Power Analysis
- **Baseline (treatment side):** NR 1.79%, R_55 2.40–2.41% in the recent mature waves
  (Jun/Jul). Full-year range across mature waves (Jan–Jul): NR 1.25–1.99%, R_55 2.36–3.56% —
  the swing is mostly band-mix (which deciles get contacted varies by month), which is why
  the read is by band. **Control side expected ≈ 0**, resting on the mechanism (no touch, no
  offer-driven upgrade); consistent with 3 target-product conversions in ~7,550 historical
  NM clients (Jan–Aug) and ~0 any-product organic switching. The historical NM group is
  supporting evidence, not proof (not a verified random control, §7c).
- **Significance:** α = 0.05 one-sided primary. **Power:** 80%.
- **Because control ≈ 0 this is a detection/precision problem, not a classic MDE:** with a
  ~2% vs ~0 contrast, even a 10% holdout detects the pooled effect. The holdout is sized for
  band-level precision, not headline power.

- **Detection mechanic (why holdout size matters):** with a zero-converting control, the
  Fisher-style p-value when all conversions land in treatment is ≈ (treatment share)^k, so
  the conversions k needed for significance grows as the holdout shrinks: ~5 at 50% holdout,
  ~14 at 20%, ~29 at 10% (α=.05 one-sided; 80% power needs expected events ≈ k + 0.84√k).
  Treatment volume barely grows as holdout shrinks → small holdouts lose the thin bands.

**The holdout-size dial (per offer; NR is the binding case):**

| Holdout | Headline (per offer) | Band read, 1 wave | Band read, 2 waves | Forgone upgrades/wave |
|---------|---------------------|-------------------|--------------------|-----------------------|
| 10% | Yes | Bands 1–3 | 1–4 | ~34–53 |
| 20% | Yes | Bands 1–4 | 1–4, firmer | ~68–107 |
| **30%** | **Yes** | **Bands 1–4** | **1–4 clean + replication** | **~102–160** |
| 50% | Yes | Bands 1–4 (+6 marginal) | 1–4 clean; 5–7 marginal (~50–70% power) | ~170–267 |

- **Recommendation: 70/30 × 2 waves.** Bands 5–7 are underpowered even at 50/50 over 2 waves
  (~4–6 expected conversions vs ~7 needed), so the 50/50 premium (~175 extra forgone upgrades
  over 2 waves) buys mostly marginal reads. 70/30 × 2 waves holds out roughly the same total
  clients as 50/50 × 1 wave: same evidence budget, plus replication.
- **Bands 5–9:** pooled as one bottom bucket, reported as an upper bound only ("consistent
  with ~0, at most X%"). Bands 8–9 are never readable individually at any split (~0.1% rate
  needs ~30–60K/band; model selects a few hundred).
- **Duration decision:** 1 wave = headline; 2 waves = headline + full calibration depth for
  the chosen split, plus replication (wave-to-wave stability is what makes the number
  defensible). Recommended: commit 2 waves, wave 1 read as interim headline.
- **Cost calculation (forgone upgrades):** holdout% × Σ_offer (wave COMM volume × baseline).
  June-sized wave: NR 13,408×1.79% = 240; R_55 12,162×2.41% = 293 → 533 total → 50% forgoes
  ~267 (July-sized: 339 → ~170). Re-entry across waves is rare (~5% twice, none 3+), so
  forgone ≈ lost within horizon, not delayed. Assumes holdout converts ~0 (0/2,956
  historical, §7c footnote applies).

## 6. Analysis Plan
- **Primary Test:** per offer, one-sided two-proportion comparison; exact (Fisher /
  Barnard) given near-zero control counts. Report lift with 95% CI.
- **Multiple Testing:** two offers = two independent business decisions → no correction
  across offers (pod convention: Bonferroni only within one decision). Secondary band reads
  are descriptive/calibration, reported with CIs, no family-wise gate.
- **Subgroup (pre-specified):** score band (1–9), per offer, pooled waves. Monotone-trend
  check on lift by band (Cochran-Armitage style).
- **Measurement path:** arms + config from TACTIC_EVNT_IP_AR_HIST; conversion ONLY from
  curated cards_bizups_vbu_descresp_clnt via the proven clnt_no + tactic_id bridge (b2, 100%).

## 7. Randomization & Quality Checks
### 7a. Sample Ratio Mismatch
Chi-square on 50/50 at wave close AND at day 1 (assignment file), per offer and per band.
The per-band check is the guard that the draw was truly score-independent.
### 7b. Covariate Balance
SMD on model score (both arms carry it), plus band-share comparison treatment vs holdout.
SMD > 0.1 → investigate before reading results.
### 7c. Why the existing NM group is not the control
The BAU not-communicated share is score-linked: NR decile 1 held out at ~8.3% vs ~4.0% at
decile 4 (June; gradient repeats every wave). Whatever draws it knows the score → not a
simple random cut. The experiment therefore requires a fresh draw (§4) and the historical
"0/2,956" claim carries a footnote until CIDM explains the NM mechanism.

## 8. Risk & Mitigation Plan
| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Holdout not truly no-touch (reassigned or picked up by another campaign) | Medium | Kills the estimand | Explicit CIDM confirmation; post-launch tactic-history audit of holdout clients |
| Split drawn by the existing (score-linked) NM mechanism | Medium | Biased control | §7a per-band SRM on day-1 assignment file |
| Read taken before maturity | Medium | Understated lift (treatment-side Aug read: 0.44% at ~2.5 wks vs ~1.8% mature) | No read before ~80 days post-deploy |
| Forgone conversions escalate if test forgotten | Low | Cost creep | Fixed wave count with explicit stop/extend decision |
| Scores/bands missing on holdout ledger rows | Low | Loses band read | Ledger spec §4(3); verified pattern from t1 |

## 9. Final Decision Path
- **Test Owner:** Andre (measurement); {business owner TBD}
- **Approval:** {TBD} + CIDM feasibility sign-off
- **Open CIDM questions (blocking launch, not design):** (1) is the current ~5% NM a
  deliberate random holdout, and why is its share score-linked for NR? (2) can the mechanism
  run a fresh client-level random 50/50 within the model-based offers, holdout = no VBU touch?
- **Monitoring:** SRM at day 1; volumes weekly; single read at ~80 days per wave.

## 10. Appendix
- Baselines & volumes: `vbu_deployment_results_by_offer.xlsx`; wave × offer × decile × arm
  transcription: `workstation_pivot_2026-09-02.md` (cross-checked vs b4: NR d1 136=136 exact).
- Probe lineage: b1 census, b2 bridge, b3 baselines, b4 discrimination, d1 calendar, t1/s1
  score verification (this folder).
- Full context: `experiment_config_2026-09.md`.
