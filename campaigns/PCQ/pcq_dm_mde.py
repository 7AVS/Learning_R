# -*- coding: utf-8 -*-
"""
pcq_dm_mde.py
=============
Minimum Detectable Effect (MDE) calculator for the PCQ Direct Mail test.

Campaign context
----------------
PCQ (Product Card Upgrade / Third-Party Acquisition via PCQ channel) is running
a direct mail (DM) expansion into the 7th decile (and optionally the 8th decile)
of its credit card acquisition model. The test question is:

    Does receiving a DM piece cause a meaningful lift in response rate?

Design:
    - Test group  : receives the DM
    - Control group: held out, receives NO DM
    - Measurement : two-proportion z-test (test rate vs. control rate)

MDE formula (two-proportion z-test, two-sided):
-----------------------------------------------
MDE = (z_alpha + z_beta) * sqrt(p0 * (1 - p0) * (1/n_control + 1/n_test))

Where:
    p0        = baseline proportion (control group expected response rate)
    z_alpha   = critical value for significance level alpha (two-sided)
                  alpha=0.05  -> z_alpha = 1.96
    z_beta    = critical value for statistical power (1 - beta)
                  power=0.80  -> z_beta  = 0.84
    n_control = number of records held in control (no DM)
    n_test    = number of records who receive DM

The formula gives the smallest absolute difference between the test rate and
the control rate that the test can reliably detect at the chosen alpha and power.

Solving for n_control given a fixed MDE:
-----------------------------------------
If the control fraction is f (so n_control = f * N and n_test = (1-f) * N):

    MDE = K * sqrt(p0*(1-p0) * (1/(f*N) + 1/((1-f)*N)))
    MDE^2 = K^2 * p0*(1-p0) * (1/(f*N) + 1/((1-f)*N))

Solving for N (total population):
    N = K^2 * p0*(1-p0) * (1/f + 1/(1-f)) / MDE^2

And then n_control = f * N.

Standard library only — no scipy / numpy / statsmodels.
"""

import math
import sys
import io

# Force UTF-8 output so em-dashes and other chars render correctly on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
Z_ALPHA = 1.96   # two-sided alpha = 0.05
Z_BETA  = 0.84   # power = 80%
K       = Z_ALPHA + Z_BETA   # combined multiplier = 2.80

# Lift benchmarks supplied by T (team stakeholder)
BENCHMARK_LOW_PP  = 0.005   # 0.5 percentage point lift — conservative target
BENCHMARK_HIGH_PP = 0.008   # 0.8 percentage point lift — T's mentioned figure


# ---------------------------------------------------------------------------
# Core MDE function
# ---------------------------------------------------------------------------
def mde(p0: float, n_control: int, n_test: int) -> float:
    """
    Return MDE in absolute proportion units (e.g. 0.003 means 0.3pp).

    Parameters
    ----------
    p0        : baseline response rate (proportion, not percent)
    n_control : control group size
    n_test    : test group size (receives DM)
    """
    variance_term = p0 * (1 - p0) * (1 / n_control + 1 / n_test)
    return K * math.sqrt(variance_term)


# ---------------------------------------------------------------------------
# Minimum total population to detect a given lift at fixed control fraction
# ---------------------------------------------------------------------------
def min_n_total(p0: float, control_frac: float, target_lift: float) -> int:
    """
    Solve for total population N needed to detect `target_lift` (absolute pp)
    given a fixed control fraction.

    N = K^2 * p0*(1-p0) * (1/f + 1/(1-f)) / target_lift^2
    """
    f  = control_frac
    numerator   = K**2 * p0 * (1 - p0) * (1 / f + 1 / (1 - f))
    denominator = target_lift**2
    return math.ceil(numerator / denominator)


# ---------------------------------------------------------------------------
# Achievability check
# ---------------------------------------------------------------------------
def achievability(mde_val: float) -> str:
    """
    Quick flag comparing the calculated MDE to the team's benchmarks.
    A smaller MDE is better -- it means we can detect smaller lifts.
    """
    if mde_val <= BENCHMARK_LOW_PP:
        return "Detects >=0.5pp lift  [GOOD]"
    elif mde_val <= BENCHMARK_HIGH_PP:
        return "Detects >=0.8pp lift  [MARGINAL]"
    else:
        return f"MDE={mde_val*100:.2f}pp > 0.8pp [UNDERPOWERED]"


# ---------------------------------------------------------------------------
# Scenario runner
# ---------------------------------------------------------------------------
def run_scenario(
    label: str,
    total_population: int,
    baseline: float,
    control_splits: list,
):
    """
    For a given scenario, print results for each control split.
    """
    col_w = 72
    print("=" * col_w)
    print(f"  {label}")
    print(f"  Total population : {total_population:,}")
    print(f"  Baseline rate    : {baseline*100:.3f}%")
    print("=" * col_w)
    print(
        f"  {'Ctrl%':>5}  {'n_ctrl':>8}  {'n_test':>8}  "
        f"{'MDE (pp)':>9}  {'Test rate':>10}  {'Assessment'}"
    )
    print("-" * col_w)
    for ctrl_pct in control_splits:
        n_ctrl = round(total_population * ctrl_pct)
        n_tst  = total_population - n_ctrl
        if n_ctrl == 0 or n_tst == 0:
            continue
        mde_val       = mde(baseline, n_ctrl, n_tst)
        test_rate     = baseline + mde_val
        assessment    = achievability(mde_val)
        print(
            f"  {ctrl_pct*100:>4.0f}%  {n_ctrl:>8,}  {n_tst:>8,}  "
            f"{mde_val*100:>8.3f}pp  {test_rate*100:>9.3f}%  {assessment}"
        )
    print()


# ---------------------------------------------------------------------------
# Min-n solver: what's the smallest control pool to detect a target lift?
# ---------------------------------------------------------------------------
def run_min_n_table(label: str, baseline: float, control_splits: list):
    """
    For each control split, solve for the minimum TOTAL population N
    needed to detect 0.5pp and 0.8pp lifts. Then report n_control.
    """
    col_w = 72
    print(f"  Minimum population to detect target lift — {label}")
    print(f"  Baseline: {baseline*100:.3f}%   z_alpha={Z_ALPHA}  z_beta={Z_BETA}")
    print("-" * col_w)
    print(
        f"  {'Ctrl%':>5}  {'N for 0.5pp':>12}  {'n_ctrl(0.5pp)':>14}  "
        f"{'N for 0.8pp':>12}  {'n_ctrl(0.8pp)':>14}"
    )
    print("-" * col_w)
    for ctrl_pct in control_splits:
        n_total_05 = min_n_total(baseline, ctrl_pct, BENCHMARK_LOW_PP)
        n_ctrl_05  = round(n_total_05 * ctrl_pct)
        n_total_08 = min_n_total(baseline, ctrl_pct, BENCHMARK_HIGH_PP)
        n_ctrl_08  = round(n_total_08 * ctrl_pct)
        print(
            f"  {ctrl_pct*100:>4.0f}%  {n_total_05:>12,}  {n_ctrl_05:>14,}  "
            f"{n_total_08:>12,}  {n_ctrl_08:>14,}"
        )
    print()


# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------
CONTROL_SPLITS = [0.10, 0.20, 0.30, 0.50]

# Scenario A — 7th decile only
BASELINE_7   = 0.0046   # 0.46% per campaign background notes
POP_7        = 55_000   # approximate 7th decile monthly volume

# Scenario B — 7th + 8th decile combined
# Weighted baseline: (55048 * 0.0046 + 43790 * 0.0028) / (55048 + 43790)
# Numerators: 0.46% of 55048 = 253.2 responses; 0.28% of 43790 = 122.6 responses
N_7          = 55_048
N_8          = 43_790
RESP_7       = N_7 * BASELINE_7          # ~253
RESP_8       = N_8 * 0.0028             # ~123
BASELINE_78  = (RESP_7 + RESP_8) / (N_7 + N_8)
POP_78       = N_7 + N_8               # ~98,838 → round to 99,000

# T's number — 120K DMs per month
POP_120K     = 120_000


# ---------------------------------------------------------------------------
# Main output
# ---------------------------------------------------------------------------
def main():
    print()
    print("=" * 72)
    print("  PCQ DIRECT MAIL -- MDE ANALYSIS")
    print("  95% confidence (two-sided)  |  80% power")
    print("  Formula: MDE = (z_alpha + z_beta) * sqrt(p0*(1-p0)*(1/n_c + 1/n_t))")
    print("=" * 72)
    print()
    print(f"  Constants: z_alpha={Z_ALPHA}  z_beta={Z_BETA}  K={K}")
    print(f"  Benchmarks: low={BENCHMARK_LOW_PP*100:.1f}pp  high={BENCHMARK_HIGH_PP*100:.1f}pp")
    print()

    # --- Scenario A: 7th decile, actual pop --------------------------------
    run_scenario(
        label            = "Scenario A — 7th Decile Only (pop ~55,000)",
        total_population = POP_7,
        baseline         = BASELINE_7,
        control_splits   = CONTROL_SPLITS,
    )

    # --- Scenario B: 7th+8th combined, actual pop --------------------------
    print(
        f"  Scenario B baseline calculation:\n"
        f"    7th decile: {N_7:,} records × {BASELINE_7*100:.2f}% = {RESP_7:.0f} expected responses\n"
        f"    8th decile: {N_8:,} records × 0.28% = {RESP_8:.0f} expected responses\n"
        f"    Combined  : ({RESP_7:.0f} + {RESP_8:.0f}) / ({N_7:,} + {N_8:,}) = "
        f"{BASELINE_78*100:.4f}%\n"
    )
    run_scenario(
        label            = f"Scenario B — 7th + 8th Decile (pop ~{POP_78:,})",
        total_population = POP_78,
        baseline         = BASELINE_78,
        control_splits   = CONTROL_SPLITS,
    )

    # --- Scenario A: 7th decile, 120K pop ----------------------------------
    run_scenario(
        label            = "Scenario A (120K) — 7th Decile @ 120,000 DMs/month",
        total_population = POP_120K,
        baseline         = BASELINE_7,
        control_splits   = CONTROL_SPLITS,
    )

    # --- Scenario B: 7th+8th combined, 120K pop ----------------------------
    run_scenario(
        label            = "Scenario B (120K) — 7th + 8th Decile @ 120,000 DMs/month",
        total_population = POP_120K,
        baseline         = BASELINE_78,
        control_splits   = CONTROL_SPLITS,
    )

    # --- Minimum N tables --------------------------------------------------
    print("=" * 72)
    print("  MINIMUM POPULATION TO DETECT TARGET LIFTS")
    print("  (solve for N given control fraction and target MDE)")
    print("=" * 72)
    print()
    run_min_n_table(
        label          = "7th Decile (baseline 0.46%)",
        baseline       = BASELINE_7,
        control_splits = CONTROL_SPLITS,
    )
    run_min_n_table(
        label          = f"7th+8th Decile (baseline {BASELINE_78*100:.4f}%)",
        baseline       = BASELINE_78,
        control_splits = CONTROL_SPLITS,
    )

    # --- Key takeaways -----------------------------------------------------
    print("=" * 72)
    print("  INTERPRETATION NOTES")
    print("=" * 72)
    print("""
  1. MDE shrinks as control % decreases (more test records = more power),
     but holding out fewer people also reduces control reliability. 10%
     control is the minimum worth considering for a stable baseline.

  2. At 55K (7th decile only), even 10% control (~5.5K ctrl / ~49.5K test)
     gives an MDE well above 0.5pp. This means the test is underpowered
     to detect the conservative 0.5pp target with 55K records alone.

  3. At 120K DMs/month (T's figure), power improves materially. A 10%
     control hold at 120K is worth examining as the most efficient design.

  4. The 0.8pp lift T mentioned is the HIGH end of what seems achievable.
     If the true lift is smaller, the test will need more records to see it.

  5. Weighted combined baseline for 7th+8th is lower than 7th alone
     (because 8th decile converts less). This slightly increases the MDE
     for combined scenarios — check whether the volume gain compensates.

  6. These MDEs assume equal within-group variance (pooled proportion p0).
     If actual test-group rates diverge significantly from p0, the formula
     is still directionally correct for design planning purposes.
""")


if __name__ == "__main__":
    main()
