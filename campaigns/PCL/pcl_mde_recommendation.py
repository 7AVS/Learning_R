# PCL Mobile Channel Test — Recommended Design
# Email draft for internal review before sharing with Tracey
#
# To: Patel, Maya; Chin, Daniel; Bajaj, Kabir
# Subject: RE: Testing Approach - to discuss control requirement for 5% Lift
#
# Hi team,
#
# Here's the recommended design for the PLI mobile channel test.
# Baseline assumptions and supporting data are in the attached document.
#
# RECOMMENDED SPLIT: 10/45/45
#
# Arm                  | Description                              | Allocation | ~Monthly n
# Champion             | Product Page + Offers Hub (all channels) | 10%        | 48,682
# Challenger A         | Champion + Sales Model placement         | 45%        | 219,069
# Challenger B         | Champion + Mobile Dashboard placement    | 45%        | 219,069
#
# RATIONALE:
# - 10% Champion provides sufficient power for both primary comparisons
#   (Champion vs A, Champion vs B) while maximizing Challenger sample sizes
# - Equal 45/45 split between Challengers is required for the A vs B comparison
# - Target lift: 5% relative (0.63pp absolute on 12.51% baseline)
# - Worst-case MDE: 0.41pp — below the 0.63pp target across all scenarios
# - 95% confidence, 80% power
#
# COMPARISONS:
# 1. Champion vs Challenger A — does Sales Model placement lift response? (primary)
# 2. Champion vs Challenger B — does Mobile Dashboard placement lift response? (primary)
# 3. Challenger A vs Challenger B — which placement performs better? (exploratory)
#
# STRESS TEST SUMMARY:
#
# Scenario                  | Baseline RR | Target Lift (abs) | Worst Case MDE | Powered?
# Pessimistic (Jan 2026)    | 10.50%      | 0.53pp            | 0.38pp         | YES
# Current (FY 2025 avg)     | 12.51%      | 0.63pp            | 0.41pp         | YES
# Optimistic (Nov 2025)     | 15.40%      | 0.77pp            | 0.45pp         | YES
#
# Let me know if you'd like to walk through the details.
#
# Andre
