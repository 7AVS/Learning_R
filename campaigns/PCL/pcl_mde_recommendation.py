# PCL Mobile Channel Test — Recommended Design
# Email draft for internal review before sharing with Tracey
#
# To: Patel, Maya; Chin, Daniel; Bajaj, Kabir
# Subject: RE: Testing Approach - to discuss control requirement for 5% Lift
#
# Hi team,
#
# Here's the recommended design for the PLI mobile channel test.
#
# RECOMMENDATION: 10/45/45 split
#
# Arm                  | Description                              | Allocation | ~Monthly n
# Champion             | Product Page + Offers Hub (all channels) | 10%        | 48,682
# Challenger A         | Champion + Sales Model placement         | 45%        | 219,069
# Challenger B         | Champion + Mobile Dashboard placement    | 45%        | 219,069
#
# WHY THIS WORKS:
# - Target lift: 5% relative (0.63pp absolute on 12.51% baseline)
# - Worst-case MDE across all comparisons: 0.41pp — well below the 0.63pp target
# - All three comparisons are powered at 95% confidence / 80% power
# - Stress-tested across pessimistic (10.50%), current (12.51%), and optimistic (15.40%) baselines — powered in all scenarios
#
# WHY 10% CHAMPION (NOT HIGHER):
# - 10% is sufficient because both Challengers are large (219K each), giving strong power
#   for the primary comparisons (Champion vs A, Champion vs B)
# - Increasing Champion share would reduce Challenger sizes and weaken the A vs B comparison
# - The A vs B comparison (which placement performs better) requires equal sizing — 45/45 is optimal
#
# PRE-REGISTERED COMPARISONS:
# 1. Champion vs Challenger A — does Sales Model lift response? (primary)
# 2. Champion vs Challenger B — does Dashboard lift response? (primary)
# 3. Challenger A vs Challenger B — which addition performs better? (exploratory)
#
# STRESS TEST SUMMARY:
#
# Scenario                  | Baseline RR | Target Lift (abs) | Worst Case MDE | Powered?
# Pessimistic (Jan 2026)    | 10.50%      | 0.53pp            | 0.38pp         | YES
# Current (FY 2025 avg)     | 12.51%      | 0.63pp            | 0.41pp         | YES
# Optimistic (Nov 2025)     | 15.40%      | 0.77pp            | 0.45pp         | YES
#
# Happy to walk through in more detail.
#
# Andre
