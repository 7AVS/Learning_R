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
# BASELINE ASSUMPTIONS:
#
# The baseline response rate drives the entire calculation. Here's how we arrived at it:
#
# Source: cards_pli_decision_resp, mobile population-level response rate
# (mobile responders / total eligible population, not mobile-only responders)
#
# Monthly RR history (FY 2025):
#   Apr 2025:  14%    |  Jul 2025:  18%    |  Oct 2025:  19%
#   May 2025:  14%    |  Aug 2025:  16%    |  Nov 2025:  22%
#   Jun 2025:  16%    |  Sep 2025:  19%    |  Dec 2025:  19%
#                                           |  Jan 2026:  15%
#
# FY 2025 monthly average: 12.51% (used as primary baseline)
# Monthly population average: 486,821 eligible clients
#
# Three scenarios tested to ensure robustness:
#   Pessimistic: 10.50% (Jan 2026 — lowest recent month, 55,560/528,002)
#   Current:     12.51% (FY 2025 monthly average)
#   Optimistic:  15.40% (Nov 2025 — highest recent month, 99,704/648,547)
#
# The design is powered across all three scenarios.
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
