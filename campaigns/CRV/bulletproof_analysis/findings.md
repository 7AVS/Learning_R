CRV-PCL BULLETPROOF ANALYSIS — FINDINGS LOG
============================================

Cohort window: offer_start_date / treatmt_strt_dt >= 2024-10-01
               (May 2026 partial-month excluded at reporting, not in SQL)
Methodology:   PCL-LEAD CENTRIC (corrected 2026-05-26 end of day) —
               each (acct x PCL deployment) is one observation.
               Multi-CRV-wave overlap handled by EXISTS semi-joins
               (no fan-out). Q01/Q05/Q10/Q11 remain CRV-side by design.
Channel filter (CRITICAL — different per campaign):
   CRV side: channels_deployed LIKE '%IM%' (CRV's mobile deployment
             stamps IM in data — backend labeling quirk)
   PCL side: channel LIKE '%MB%' (PCL labels mobile correctly as MB)
   Both target the mobile banner surface. NEVER filter PCL with %IM%.
Queries:       00_randomization_validation.sql through
               11_throttling_scenarios.sql in this folder.

This file is the rolling log for findings as each query is run.
One section per query, appended in run order. Keep entries terse:
result table + interpretation + side observations + open follow-ups.


================================================================
Query 00 — Randomization design (sticky vs per-wave)
Run date: 2026-05-26
================================================================

Result A — account bucket summary:
  bucket          accounts     pct_share
  action_only     4,471,313    94.80%
  control_only      233,017     4.90%
  both               10,279     0.20%
  TOTAL           4,714,609

Result B (sample of 100 "both" accounts): NOT YET REVIEWED.

Interpretation:
- CRV randomization is EFFECTIVELY STICKY.
- 0.20% crossover ("both" bucket) is small enough to treat as edge
  cases. Section E's Action-priority dedup was a near-no-op.
- Lead-grain methodology still the right formalism but the practical
  impact on the original 0.97pp headline is negligible.

Side observations:
- Control holdout = control_only + both = 5.10% of accounts ever in
  CRV. On the smaller side for an RCT (typical 10-20%). Worth
  tracking when interpreting the CI on the 0.97pp gap (query 04).

Open follow-ups:
- Review Result B to understand WHEN the 10,279 crossovers happen
  (early vs late waves, specific eligibility-model change date?).
  Low priority — not blocking.


================================================================
Query 01 — Action/Control balance test
Run date: 2026-05-26
================================================================

Per-month side-by-side: full CRV Action and Control lead counts vs
overlap-with-PCL-mobile Action and Control lead counts.

Action lead counts in 'full_crv' rows hit hundreds of thousands to
~1.8M per month (e.g., 2024-10: 833,415; 2025-05: 1,877,642).

Initial run came back with control_count = 0 everywhere — caused
by Teradata CHAR truncation in UNION ALL when 'Action' (6 chars)
unioned with 'Control' (7 chars) and the unified type forced to
CHAR(6), truncating 'Control' to 'Contro'. Fixed by CAST(arm AS
VARCHAR(10)) in all CTEs across queries.

After fix: side-by-side layout works. Balance check pending
detailed read once Andre filters/pivots in Excel.

Interpretation framework:
- Compare Action:Control ratio in full vs in overlap.
- If ratios match → randomization preserved on the subset → 0.97pp
  / 1.5pp gap is causal.
- If ratios differ significantly → randomization breaking down at
  the overlap stage → cannibalization estimate needs caveating.


================================================================
Query 02 — Overlap days distribution
Run date: 2026-05-26
================================================================

Output filtered to 'overall' rows:

subset          arm      n          mean   p10  p25  p50  p75  p90  min  max
all_leads       Action   6,381,886  45.66  14   39   49   60   65   1    90
all_leads       Control    331,214  45.41  14   38   49   60   65   1    90
pcl_responders  Action   1,631,562  38.77  11   18   43   54   62   1    90
pcl_responders  Control     89,802  38.58  11   18   43   54   62   1    90

Two findings:

1) Randomization holds on overlap days.
   Action and Control distributions are essentially identical across
   all percentiles. Overlap duration is not biased by arm.

2) PCL responders have SHORTER overlap with CRV than the average lead.
   - all_leads mean = 45.7 days; pcl_responders mean = 38.8 days
   - all_leads p50 = 49 days; pcl_responders p50 = 43 days
   - ~6-7 day shorter overlap among converters.

   Consistent with the cannibalization mechanism: less CRV competition
   (shorter overlap window) correlates with more PCL conversion.

Side observation: max = 90 days across all rows. There's a 90-day
cap somewhere (likely PCL or CRV deployment window). Doesn't affect
the analysis but worth confirming.

Open follow-ups:
- Per-month detail not yet inspected — should confirm the
  pcl_responder vs all_leads gap is stable month-to-month.


================================================================
Query 03 — Bidirectional overlap share (4 directions)
Run date: 2026-05-26
================================================================

Overall rows:

direction              total       overlap     pct
crv_action_with_pcl    23,058,958  6,381,886   28%
crv_control_with_pcl    1,218,758    331,214   27%
pcl_with_crv_action     9,721,444  6,492,198   67%
pcl_with_crv_control    9,721,444    337,853    3%

Two findings:

1) Randomization confidence check PASSED.
   CRV-Action overlaps PCL at 28%. CRV-Control overlaps PCL at 27%.
   1-point difference — Action and Control encounter PCL at
   essentially the same rate. The Q04 gap is on comparable
   populations.

2) PCL footprint of CRV-Action is large.
   67% of PCL-mobile leads are touched by a CRV-Action deployment
   during their window. That's the magnitude story for the deck:
   two-thirds of mobile PCL deployments compete with CRV.

The 67% vs 3% asymmetry (CRV-Action vs CRV-Control share of PCL) is
just population size: CRV-Action has ~19x more leads than CRV-Control
(holdout is small). Not a randomization issue.


================================================================
Query 04 — CI on the cannibalization gap (PCL-LEAD CENTRIC)
Run date: 2026-05-26
================================================================

** Headline figure for the bulletproof analysis **

Overall (PCL-mobile, Oct 2024 – May 2026):
n_action = 6,492,198    p_action = 16.15%
n_control = 337,853     p_control = 17.23%
gap = 1.08pp            se = 0.001
95% CI = [0.9pp, 1.2pp]
z = 16.23               significant_at_95 = 1

Per-month: 20 months reported.
- Every single month: Control > Action (direction 100% consistent)
- significant_at_95 = 1 on most months; 0 on a few smaller-sample
  months (Nov 2024, Dec 2024, Feb 2025) — CI too wide there but
  direction still positive
- Range of monthly gaps: ~0.4pp to ~1.6pp

Interpretation:
- Cannibalization is statistically overwhelming and directionally
  consistent. Real and reproducible.

METHODOLOGY CORRECTION NOTE — what changed mid-session:
- Initial Q04 build used CRV-LEAD grain (count CRV waves with PCL
  overlap, denominator = CRV waves). Produced inflated 1.5pp gap
  because clients with multiple CRV waves got weighted multiple times.
- Refactored to PCL-LEAD grain (count PCL leads with CRV overlap,
  denominator = PCL leads) — matches original Section E and the
  deck framing. Produces the 1.08pp figure above.
- Q00 Result C added to characterize wave durations and multi-wave-
  per-month frequency so we can audit when the difference matters.

Reconciliation across the three numbers we've seen:
                          Window          PCL filter   Gap     Status
Original deck             12 mo           %IM% online  0.97pp  Published
CRV-centric Q4 (wrong)    20 mo           %MB% mobile  1.50pp  Discarded
PCL-centric Q4 (correct)  20 mo           %MB% mobile  1.08pp  Bulletproof headline

The 1.08pp is the right number going forward. Close to the deck's
0.97pp (same framing, different window/channel) — story confirmed.
The 1.5pp was a methodology artifact and should not be cited anywhere.

How to read per-month rows going forward:
- Same direction every month (Control > Action) → robustness ✓
- Magnitude varies high/low → expected, note it, not a flag
- Direction REVERSES (Action > Control) on any month → ⚠️ red flag,
  investigate before defending the headline
- significant_at_95 = 0 on a small-sample month → wide CI, not noise
  in the headline. Direction still informative.

Open follow-ups:
- Sweep Q02, Q06, Q07, Q08, Q09 to PCL-lead centric for consistency.
- Run Q00 Result C to confirm wave-period geometry (whether multi-wave-
  per-month is rare or common, which explains the CRV-centric vs
  PCL-centric divergence in absolute counts).


================================================================
Query 00 Result C — Deployment characterization
Run date: 2026-05-26
================================================================

| campaign   | n_distinct_waves | duration_mean | duration_min | duration_max | n_months | waves_per_month_mean | waves_per_month_min | waves_per_month_max |
|------------|------------------|---------------|--------------|--------------|----------|----------------------|---------------------|---------------------|
| CRV-Action | 420              | 92.1 days     | 87           | 96           | 20       | 21                   | 17                  | 25                  |
| PCL-mobile | 88               | 68.9 days     | 43           | 90           | 20       | 4.4                  | 1                   | 10                  |

Interpretation:
- CRV-Action deploys ~21 waves/month, each ~92 days long (3-month offers).
- PCL-mobile deploys ~4.4 waves/month, each ~69 days long.
- A typical client COULD be touched by multiple CRV waves AND multiple
  PCL deployments within the same month. Multi-wave overlap is real.
- Wave counts include some test/QA deployments with 1-5 accounts (noise).
  Real-volume waves are 19K-69K accounts each. Doesn't affect cannibalization
  measurement because PCL-centric counts each PCL lead once regardless.


================================================================
Q04 — UPDATED with PCL-LEAD CENTRIC headline (1.08pp)
Run date: 2026-05-26 (rerun late session)
================================================================

** PCL-centric refactor (replaces the discarded 1.5pp CRV-centric build) **

Overall:
n_action = 6,492,198    p_action = 16.15%   (PCL leads with CRV-Action overlap)
n_control = 337,853     p_control = 17.23%
gap = 1.08pp            se = 0.001
95% CI = [0.9pp, 1.2pp]
z = 16.23               significant_at_95 = 1

Per-month: 20 months reported. All show Control > Action; direction
never reverses. Some smaller-sample months (Nov/Dec 2024, Feb 2025)
have wider CIs and sig=0, but direction still positive.

Audit verdict (independent agent, 2026-05-26):
- METHODOLOGY CORRECT
- EXISTS semi-join guarantees each PCL lead counted exactly once
  regardless of CRV-wave count (no fan-out)
- Equivalent to Section E's INNER JOIN + DISTINCT pattern; both produce
  the same answer
- One minor note: Q04 allows a PCL lead to be flagged on both arms
  (not mutually exclusive like Section E's Action-priority rule);
  this is acceptable for a two-proportion z-test on independent arms

Reconciliation with original deck (still standing):
                          Window          PCL filter   Gap     Status
Original deck             12 mo           %IM% online  0.97pp  Published
CRV-centric Q4 (wrong)    20 mo           %MB% mobile  1.50pp  DISCARDED
PCL-centric Q4 (correct)  20 mo           %MB% mobile  1.08pp  HEADLINE


================================================================
Query 05 — CRV installment economics (4-cohort split)
Run date: 2026-05-26
================================================================

Output: 4 cohorts x (overall + ~20 months). Columns:
  crv_cohort, slice, n_waves, n_accounts, n_transactions, txns_per_acct,
  mean_principal_per_acct, mean/p50/p90 APR, mean/p50/p90 term,
  mean/p50/p90 per-txn principal.

Fee columns dropped — always empty in data.

Cohorts:
- action_with_pcl_overlap
- action_no_pcl_overlap
- control_with_pcl_overlap
- control_no_pcl_overlap

Key reading on overall rows:
- n_accounts dedupes across the 20-month window; n_transactions sums.
- Sum of monthly n_accounts >> overall n_accounts (clients active in
  multiple months).
- Sum of monthly n_transactions == overall n_transactions (always).
- Overall txns_per_acct ~ 11 vs monthly ~ 2-3 → strong recurring usage.
  A typical converter takes ~11 installment plans across 20 months.

Iteration history this session:
- v1: activation channel breakdown — dropped (codes weren't useful)
- v2: 2 cohorts (action only) — Andre wanted Control too
- v3: 4 cohorts WHERE clause (Action ∨ Control with appropriate channel filters)
- Syntax fixes: EXISTS-in-CASE moved to EXISTS-in-WHERE; CAST FORMAT dropped;
  'cohort' alias renamed to 'crv_cohort' defensively; fee columns dropped;
  prncp_amt -> prncpl_amt column name correction


================================================================
Session end status (2026-05-26 late)
================================================================

VALIDATED + INTERPRETED:
- Q00 — randomization sticky (94.8% / 4.9% / 0.2%)
- Q00 Result C — CRV-Action ~21 waves/month, PCL ~4.4 waves/month
- Q01 — CHAR-truncation bug found and fixed; volumes now match original
- Q02 — PCL responders have ~7 day shorter overlap than all-leads
- Q03 — 28% / 27% randomization confidence; 67% PCL footprint of CRV
- Q04 — 1.08pp PCL-centric gap, z=16.23, every month consistent. Audit ✓
- Q05 — 4-cohort installment economics. ~11 plans/converter over 20 months

NOT YET RUN / VALIDATED:
- Q06 (net economics counts)
- Q07 (substitution + conversion order)
- Q08 (decile distribution)
- Q09 (timing within overlap)
- Q10 (time-to-PCL)
- Q11 (throttling scenarios)

OPEN METHODOLOGY:
- Add "recurring clients" block to Q05: per cohort, n_recurring (≥2 plans),
  pct_recurring, p50/p90/max plans per acct, mean_months_active.
  ~8 cols × 4 rows. Andre approved direction; deferred to next session.

OPEN DATA INPUT:
- CRV $-per-conversion from Andre (for net economics in Excel)
