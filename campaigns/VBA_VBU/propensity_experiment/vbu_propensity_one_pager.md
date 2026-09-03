# VBU — Propensity Model Holdout Test

> **Footnote version:** score-band holdout, implemented as a single client-level random 50/50
> across the model-based offers; holdout receives no VBU communication. One wave powers the
> headline (expected ~1.8–2.4% vs ~0); a second wave adds replication and band depth.
> Cost at 50/50: ~170–270 forgone upgrades per wave (20% holdout: ~70–110, keeps bands 1–4).

## Testing Snapshot
| Field | Value |
|-------|-------|
| Strategic Objective | Validate the VBU propensity model's targeting (CPX→AIB path) |
| Goal | Upsell — card upgrade |
| Treatment Period | Next wave, {TBD — monthly cadence, ~13th} |
| Response Period | ~80 days post-deploy (no read before maturity) |
| Channels | BAU VBU channels, unchanged |
| Experiment Design | Score-band holdout: single client-level random 50/50 RCT across the model-based population (no band logic in decisioning — scores stored at assignment; every band inherits its own ~50/50; results read per band) |
| Significance | 95% (α = 0.05, one-sided primary) |
| Power | 80% — overpowered for the headline; split sized for band-level depth |
| Treatment/Control Split | 50/50 (decision dial: 20% keeps headline + bands 1–4 at ~40% of cost) |

## Metrics (expected — pre-launch)
| Group | Pop Count (per wave) | Communicated | Expected Conversion % |
|-------|---------------------|--------------|----------------------|
| Treatment — AIB_25K_NR | ~4,300–6,700 | Yes | ~1.79% |
| Holdout — AIB_25K_NR | ~4,300–6,700 | No VBU touch | ≈ 0% |
| Treatment — AIB_25K_R_55 | ~3,900–6,100 | Yes | ~2.40% |
| Holdout — AIB_25K_R_55 | ~3,900–6,100 | No VBU touch | ≈ 0% |

Baselines: mature Jun/Jul 2026 waves (stable across both). Historical not-communicated:
0 target-product conversions / 2,956+ — expected holdout behavior ≈ 0.

## Test Results
| Comparison | Channel | P-Value | Lift | SRM |
|-----------|---------|---------|------|-----|
| Pending launch | — | — | — | Day-1 check per band |

## At-A-Glance
| Field | Value |
|-------|-------|
| Clients | ~16–26K per wave (both offers) |
| Period | {TBD} + ~80-day response window |
| Design | RCT, client-level 50/50, holdout = no VBU touch |
| Channels | BAU |
| Readiness | Pending — CIDM feasibility (fresh random draw; holdout truly untouched) |
| Attribution | Direct causal |
| Causal Lift | Expected: +1.8pp (NR) / +2.4pp (R_55) pooled; band 1 ~3.4–7.9pp fading to ~0 by band 5 |
| Decision | Pre-registered: certify model value + calibration by band; bands 8–9 reported as bottom-bucket upper bound only |
| Notes | Offer content (NR vs R_55 vs rule-based 35K) out of scope — no arm varies the offer. Full design: `vbu_propensity_doe_report.md` |
