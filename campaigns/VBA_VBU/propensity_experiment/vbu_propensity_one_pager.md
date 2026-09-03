# VBU: Propensity Model Holdout Test

> **Footnote version:** score-band holdout, a single client-level random 70/30 split
> across the model-based offers; the 30% holdout receives no VBU communication and stays
> held out for both waves. Wave 1 powers the headline (expected ~1.8–2.4% vs ~0, read ~80
> days post-deploy), wave 2 replicates and firms the band-level calibration (bands 1–4;
> 5–9 as bottom-bucket bound). Cost: ~102–160 forgone upgrades per wave.

## Testing Snapshot
| Field | Value |
|-------|-------|
| Strategic Objective | Validate the VBU propensity model's targeting (CPX→AIB path) |
| Goal | Upsell: card upgrade |
| Treatment Period | Next wave, {TBD: monthly cadence, ~13th} |
| Response Period | ~80 days post-deploy (no read before maturity) |
| Channels | BAU VBU channels, unchanged |
| Experiment Design | Score-band holdout: single client-level random 70/30 RCT across the model-based population. No band logic in decisioning; scores stored at assignment; every band inherits its own ~70/30; results read per band. |
| Significance | 95% (α = 0.05, one-sided primary) |
| Power | 80%, overpowered for the headline; split sized for band-level depth |
| Treatment/Control Split | 70/30 recommended (dial in DOE §5; 50/50 adds only marginal bands 5–7 reads at ~1.7× the cost) |

## Metrics (expected, pre-launch)
| Group | Pop Count (per wave) | Communicated | Expected Conversion % |
|-------|---------------------|--------------|----------------------|
| Treatment: AIB_25K_NR | ~6,300–9,900 | Yes | ~1.79% |
| Holdout: AIB_25K_NR | ~2,700–4,300 | No VBU touch | ≈ 0% |
| Treatment: AIB_25K_R_55 | ~5,700–9,000 | Yes | ~2.40% |
| Holdout: AIB_25K_R_55 | ~2,400–3,800 | No VBU touch | ≈ 0% |

Baselines: recent mature waves (Jun/Jul 2026); the full-year range across mature waves is
NR 1.25–1.99%, R_55 2.36–3.56%. Historical not-communicated: 3 target-product conversions
in ~7,550 clients (Jan–Aug); expected holdout behavior ≈ 0.

## Test Results
| Comparison | Channel | P-Value | Lift | SRM |
|-----------|---------|---------|------|-----|
| Pending launch | N/A | N/A | N/A | Day-1 check per band |

## At-A-Glance
| Field | Value |
|-------|-------|
| Clients | ~17–27K per wave (both offers) |
| Period | {TBD} + ~80-day response window |
| Design | RCT, client-level 70/30, holdout = no VBU touch, arms persist across both waves |
| Channels | BAU |
| Readiness | Pending: CIDM feasibility (fresh random draw; holdout truly untouched) |
| Attribution | Direct causal |
| Causal Lift | Expected: +1.8pp (NR) / +2.4pp (R_55) pooled; band 1 ~3.4–7.9pp fading to ~0 by band 5 |
| Decision | Pre-registered: certify model value + calibration by band; bands 8–9 reported as bottom-bucket upper bound only |
| Notes | Offer content (NR vs R_55 vs rule-based 35K) out of scope; no arm varies the offer. Full design: `vbu_propensity_doe_report.md` |
