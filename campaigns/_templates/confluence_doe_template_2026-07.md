# Confluence DOE Template (pod standard) — transcribed 2026-09-03

Source: Confluence page "PCD Async" (Pages / … / CARDS / NBA), created Jul 2026, last
updated Jul 21 2026. Screenshots in `.claude/uploads` 2026-09-03 (2 pics). This is the
pod's LIVE experiment-documentation format — 7 sections + header table. Italic text in
the original = instructions to delete when filling.

## Header table

| Field | Example (PCD Async) |
|---|---|
| MNE | PCD |
| Description | Async Banner |
| Treatment Start Date | |
| Treatment End Date | |
| Channel | |
| TST_GRP_CD | |
| Measurement Package Link | (SharePoint .pptx link) |
| Tableau Dashboard Link | N/A |
| GitHub Link | |
| Measurement & Analytics Lead | |

*(please delete any italic context with underline)*

## 1 - Executive Summary
*Provide a one to two sentence "at-a-glance"; focus on clarity, raise assumptions; DO NOT
write super long article*
- Why are we testing?
- What is being tested and measured?
- What will success look like?
- How long and how safe?

## 2 - Hypothesis & Objectives
- Primary Hypothesis — *describe hypothesis and state the hypothesis statements in math
  formula*: H0: Treatment Effect = 0; H1: Treatment Effect ≥ 0
- Secondary Hypotheses (Optional): *beside the primary treatment effect, any learning on
  client segmentations, creative, can be secondary*
- Metrics: Primary / Secondary / Guard Rail: Email Unsubscribe Rate

## 3 - Experiment Design & Treatment Assignment
- Design Type — *Describe which test is chosen (Z Test, T test, Anova Test, Chi-Square
  Test…) and Why*
- Randomization Unit
- Eligibility Rules
- Treatment Arms — *Visualization the split tree, including Split Ratio, 90/10, 80/20, etc.*

## 4 - Sample Size & Power Analysis
- Baseline Conversion Rate: *Describe how we obtained the baseline rate, with what kind of
  assumptions*
- Power Analysis & Minimum Detectable Effect
- Power Calculation Steps: *attach the calculation, or scenarios*
- Final Sample Size Require: *If we can't have enough sample size for a wave, how long do
  we need to run?*

## 5 - Analysis Plan and Results
- Randomization & Quality Checks: SRM — Sample Ratio Mismatch; Covariate Balance (SMD Method)
- Primary Test Results: Interim *(Follow O'Brien-Fleming interim analysis)*; Final
- Results table: Leads/Unique Clients | Treatment Metrics | Control Metrics | ATE (Lift) |
  P-value | Confidence Interval (Lower Bound, Upper Bound) | $ Value (Size × lift × NIBT);
  rows = Treatment / Control
- Subgroup Analysis — *for each client group, follow the same table format above*
- Multiple Testing Adjustment — *If there are multiple Primary Metrics, apply Bonferroni
  adjustment*

## 6 - Risks & Mitigation Plan

## 7 - Appendix
- Slides SharePoint links / Code / excel / anything

## Notes vs our repo template (`doe_report_template.md`)
Same skeleton (our 10 sections fold into these 7): our §1-2 → 1; §3 → 2; §4 → 3; §5 → 4;
§6-7 → 5; §8 → 6; §9 has no Confluence home (owner/approval live in the header table);
§10 → 7. Confluence adds: TST_GRP_CD in header, $ Value column (Size × lift × NIBT),
O'Brien-Fleming interim convention, unsubscribe guardrail.
