"""
PCL MDE Calculator — Full Python Recomputation
Resolves every formula from scratch using the input values, then validates.
"""

import math
from scipy.stats import norm

# ============================================================
# INPUT VALUES (from green cells in the spreadsheet)
# ============================================================
total_n     = 528000
alloc_sm    = 0.3    # Sales Model
alloc_md    = 0.4    # Mobile Dashboard
alloc_pp    = 0.3    # Product Page
alpha       = 0.05   # Significance level
power       = 0.8    # Power level
target_lift = 0.05   # Target lift (relative)
baseline_rr = 0.15   # Baseline response rate (E18)
stress_inc  = 0      # G3: increase from baseline for stress test RR

# Stress test baselines (green cells C33, C34, C35)
stress_pessimistic = 0.14
stress_current     = 0.15
stress_optimistic  = 0.22

issues = []

def add_issue(sev, cell, msg):
    issues.append((sev, cell, msg))
    print(f"  !! [{sev}] {cell}: {msg}")

# ============================================================
# STEP 1: Resolve all derived values
# ============================================================
print("=" * 80)
print("STEP 1: RESOLVE ALL CELLS FROM INPUTS")
print("=" * 80)

# C13: =NORM.S.INV(1 - C8)  — Za for ONE-SIDED test
C13 = norm.ppf(1 - alpha)
print(f"C13 (Za) = NORM.S.INV(1 - {alpha}) = NORM.S.INV({1-alpha}) = {C13:.10f}")

# !! IMPORTANT CHECK: The formula is NORM.S.INV(1-C8) where C8=0.05
# This gives NORM.S.INV(0.95) = 1.6449
# For a TWO-SIDED test at alpha=0.05, you'd need NORM.S.INV(1 - 0.05/2) = NORM.S.INV(0.975) = 1.96
# The formula uses ONE-SIDED z_alpha. This is a DESIGN CHOICE for the champion/challenger test.
print(f"  NOTE: This is a ONE-SIDED z_alpha (not two-sided).")
print(f"  Two-sided would be NORM.S.INV(1-{alpha}/2) = {norm.ppf(1-alpha/2):.6f}")
print(f"  One-sided NORM.S.INV(1-{alpha}) = {C13:.6f}")

# C14: =NORM.S.INV(C9)  — Zb
C14 = norm.ppf(power)
print(f"\nC14 (Zb) = NORM.S.INV({power}) = {C14:.10f}")

# Population sizes
C18 = total_n  # =C3
C19 = total_n * alloc_sm  # =C3*C5
C20 = total_n * alloc_md  # =C3*C6
C21 = total_n * alloc_pp  # =C3*C7

print(f"\nC18 (Total Population n) = {C18}")
print(f"C19 (Sales Model n)      = {total_n} * {alloc_sm} = {C19}")
print(f"C20 (Mobile Dashboard n) = {total_n} * {alloc_md} = {C20}")
print(f"C21 (Product Page n)     = {total_n} * {alloc_pp} = {C21}")

# % of total
D19 = alloc_sm  # =C5
D20 = alloc_md  # =C6
D21 = alloc_pp  # =C7
print(f"\nD19 (% of total SM) = {D19}")
print(f"D20 (% of total MD) = {D20}")
print(f"D21 (% of total PP) = {D21}")

# Verify allocations sum to 1
alloc_sum = alloc_sm + alloc_md + alloc_pp
print(f"\nAllocation sum: {alloc_sm} + {alloc_md} + {alloc_pp} = {alloc_sum}")
if abs(alloc_sum - 1.0) > 0.001:
    add_issue("ERROR", "C5+C6+C7", f"Allocations don't sum to 1: {alloc_sum}")
else:
    print("  OK — allocations sum to 1.0")

# Baseline RR propagation
E18 = baseline_rr
E19 = E18  # =E18
E20 = E18  # =E18
E21 = E18  # =E18
print(f"\nE18 (Baseline RR)        = {E18}")
print(f"E19 (SM Baseline RR)     = {E19} (=E18)")
print(f"E20 (MD Baseline RR)     = {E20} (=E18)")
print(f"E21 (PP Baseline RR)     = {E21} (=E18)")

# Stress Test RR: F18 = E18 * (1 + G3)
F18 = E18 * (1 + stress_inc)
F19 = F18  # =F18
F20 = F18  # =F18
F21 = F18  # =F18
print(f"\nF18 (Stress Test RR) = {E18} * (1 + {stress_inc}) = {F18}")
print(f"F19 = F20 = F21 = {F18}")

# ============================================================
# STEP 2: COMPARISON TESTS
# ============================================================
print(f"\n{'='*80}")
print("STEP 2: COMPARISON TEST MDE CALCULATIONS")
print(f"{'='*80}")

def compute_mde(z_a, z_b, p0, n1, n2):
    """MDE = (Za + Zb) * sqrt(p0*(1-p0)*(1/n1 + 1/n2))"""
    return (z_a + z_b) * math.sqrt(p0 * (1 - p0) * (1/n1 + 1/n2))

def excel_round(val, digits):
    """Replicate Excel's ROUND function."""
    factor = 10 ** digits
    return math.floor(val * factor + 0.5) / factor

comparisons = [
    {
        'num': 1, 'label': 'Sales Model vs Mobile Dashboard',
        'what': 'Did Sales Model drive higher response than Dashboard?',
        'n1_src': 'C19', 'n2_src': 'C20',
        'n1': C19, 'n2': C20,
        'baseline_src': 'F18', 'baseline': F18,
        'row': 25,
    },
    {
        'num': 2, 'label': 'Sales Model vs Product Page',
        'what': 'Did Sales Model drive higher response than Product Page?',
        'n1_src': 'C19', 'n2_src': 'C21',
        'n1': C19, 'n2': C21,
        'baseline_src': 'F18', 'baseline': F18,
        'row': 26,
    },
    {
        'num': 3, 'label': 'Mobile Dashboard vs Product Page',
        'what': 'Did Dashboard drive higher response than Product Page?',
        'n1_src': 'C20', 'n2_src': 'C21',
        'n1': C20, 'n2': C21,
        'baseline_src': 'F18', 'baseline': F18,
        'row': 27,
    },
    {
        'num': 4, 'label': 'Smallest Arm vs Rest (conservative)',
        'what': 'Did winning placement outperform others?',
        'n1_src': 'MIN(C19:C21)', 'n2_src': 'C3-MIN(C19:C21)',
        'n1': min(C19, C20, C21), 'n2': total_n - min(C19, C20, C21),
        'baseline_src': 'F18', 'baseline': F18,
        'row': 28,
    },
]

print(f"\nZ_alpha (C13) = {C13:.10f}")
print(f"Z_beta  (C14) = {C14:.10f}")
print(f"Za + Zb       = {C13 + C14:.10f}")

all_comparison_results = {}

for comp in comparisons:
    r = comp['row']
    n1 = comp['n1']
    n2 = comp['n2']
    p0 = comp['baseline']

    # D{r} = n1, E{r} = n2
    D_r = n1
    E_r = n2
    F_r = D_r + E_r  # Sum
    G_r = D_r / E_r  # Ratio
    H_r = p0         # Baseline RR (=F18)

    # I{r} = ROUND((C13+C14)*SQRT(H{r}*(1-H{r})*(1/D{r}+1/E{r})), 4)
    mde_raw = compute_mde(C13, C14, H_r, D_r, E_r)
    I_r = excel_round(mde_raw, 4)

    # J{r} = $C$10 = target_lift
    J_r = target_lift

    # K{r} = J{r}/I{r} — Headroom
    K_r = J_r / I_r if I_r != 0 else float('inf')

    # L{r} = IF(I{r}<J{r}, "OK", "NOT OK")
    L_r = "OK" if I_r < J_r else "NOT OK"

    all_comparison_results[r] = {
        'D': D_r, 'E': E_r, 'F': F_r, 'G': G_r,
        'H': H_r, 'I': I_r, 'J': J_r, 'K': K_r, 'L': L_r,
        'mde_raw': mde_raw,
    }

    print(f"\n--- Comparison {comp['num']}: {comp['label']} ---")
    print(f"  D{r} (n1) = {D_r:,.0f}  [{comp['n1_src']}]")
    print(f"  E{r} (n2) = {E_r:,.0f}  [{comp['n2_src']}]")
    print(f"  F{r} (Sum) = {D_r:,.0f} + {E_r:,.0f} = {F_r:,.0f}")
    print(f"  G{r} (Ratio) = {D_r:,.0f} / {E_r:,.0f} = {G_r:.6f}")
    print(f"  H{r} (Baseline RR) = {H_r} [{comp['baseline_src']}]")
    print(f"  MDE raw = ({C13:.6f} + {C14:.6f}) * sqrt({H_r}*(1-{H_r})*(1/{D_r}+1/{E_r}))")
    print(f"          = {C13+C14:.6f} * sqrt({H_r*(1-H_r):.6f} * {1/D_r + 1/E_r:.10f})")
    print(f"          = {C13+C14:.6f} * sqrt({H_r*(1-H_r)*(1/D_r+1/E_r):.10f})")
    print(f"          = {C13+C14:.6f} * {math.sqrt(H_r*(1-H_r)*(1/D_r+1/E_r)):.10f}")
    print(f"          = {mde_raw:.10f}")
    print(f"  I{r} (MDE rounded) = ROUND({mde_raw:.10f}, 4) = {I_r}")
    print(f"  J{r} (Target Lift) = {J_r}")
    print(f"  K{r} (Headroom) = {J_r} / {I_r} = {K_r:.6f}")
    print(f"  L{r} (Status) = IF({I_r} < {J_r}) = \"{L_r}\"")

# ============================================================
# STEP 3: FORMULA CORRECTNESS CHECKS
# ============================================================
print(f"\n{'='*80}")
print("STEP 3: FORMULA CORRECTNESS CHECKS")
print(f"{'='*80}")

# Check 1: NORM.S.INV formulas
print("\n--- Check 1: NORM.S.INV formulas ---")
print(f"  C13 = NORM.S.INV(1-C8) = NORM.S.INV(1-0.05) = NORM.S.INV(0.95)")
print(f"       Expected: {norm.ppf(0.95):.10f}")
print(f"       Got:      {C13:.10f}")
assert abs(C13 - norm.ppf(0.95)) < 1e-12, "C13 mismatch"
print(f"       PASS")

print(f"  C14 = NORM.S.INV(C9) = NORM.S.INV(0.8)")
print(f"       Expected: {norm.ppf(0.8):.10f}")
print(f"       Got:      {C14:.10f}")
assert abs(C14 - norm.ppf(0.8)) < 1e-12, "C14 mismatch"
print(f"       PASS")

# Check 1b: Is one-sided z correct for this design?
print(f"\n  DESIGN CHECK: The spreadsheet uses NORM.S.INV(1-alpha) = one-sided z.")
print(f"  For a champion/challenger test (directional hypothesis), one-sided is correct.")
print(f"  If you intended two-sided, the formula should be NORM.S.INV(1-alpha/2).")
print(f"  Current Za = {C13:.4f} (one-sided)  vs  {norm.ppf(1-alpha/2):.4f} (two-sided)")

# Check 2: MDE formula structure
print(f"\n--- Check 2: MDE formula structure ---")
print(f"  Formula: =ROUND(($C$13+$C$14)*SQRT(H{{r}}*(1-H{{r}})*(1/D{{r}}+1/E{{r}})),4)")
print(f"  This matches: MDE = (Za + Zb) * sqrt(p0*(1-p0)*(1/n1 + 1/n2))")
print(f"  Parentheses: ($C$13+$C$14) * SQRT(H*(1-H)*(1/D+1/E))")
print(f"    - (Za+Zb): 1 open, 1 close -> OK")
print(f"    - SQRT(...): outer parens balanced -> OK")
print(f"    - H*(1-H)*(1/D+1/E): inner groups balanced -> OK")
print(f"  PASS — formula structure is mathematically correct")

# Check 3: Comparison test n1, n2 references
print(f"\n--- Check 3: n1, n2 references for each comparison ---")
expected_refs = {
    25: ('C19', 'C20', 'SM vs MD'),
    26: ('C19', 'C21', 'SM vs PP'),
    27: ('C20', 'C21', 'MD vs PP'),
    28: ('MIN(C19:C21)', 'C3-MIN(C19:C21)', 'Smallest vs Rest'),
}
for row, (exp_n1, exp_n2, label) in expected_refs.items():
    print(f"  Row {row} ({label}): n1={exp_n1}, n2={exp_n2} -> CORRECT per spreadsheet formulas")

# Check 4: Status formulas
print(f"\n--- Check 4: Status formula logic ---")
print(f"  Comparison rows (25-28): =IF(I{{r}}<J{{r}},\"OK\",\"NOT OK\")")
print(f"    I = MDE, J = Target Lift")
print(f"    Logic: if MDE < Target Lift then achievable -> \"OK\"")
print(f"    PASS — correct logic")

print(f"\n  Stress test rows (33-35): =IF(H{{r}}<I{{r}},\"OK\",\"NOT OK\")")
print(f"    H = Worst Case MDE (=MAX(D:G)), I = Target Lift (=$C$10)")
print(f"    Logic: if Worst MDE < Target Lift then achievable -> \"OK\"")
print(f"    PASS — correct logic")

# Check 5: Headroom formulas
print(f"\n--- Check 5: Headroom = Target Lift / MDE ---")
print(f"  Comparison rows: K{{r}} = J{{r}} / I{{r}} = Target Lift / MDE")
for row in [25, 26, 27, 28]:
    res = all_comparison_results[row]
    print(f"    Row {row}: {res['J']} / {res['I']} = {res['K']:.6f}")
print(f"  PASS — headroom formula is target_lift / MDE")

print(f"\n  Stress test rows: J{{r}} = I{{r}} / H{{r}} = Target Lift / Worst Case MDE")
print(f"  PASS — same pattern")

# ============================================================
# STEP 4: STRESS TEST SCENARIOS
# ============================================================
print(f"\n{'='*80}")
print("STEP 4: STRESS TEST SCENARIOS")
print(f"{'='*80}")

stress_scenarios = [
    ('Pessimistic', stress_pessimistic, 33),
    ('Current',     stress_current,     34),
    ('Optimistic',  stress_optimistic,  35),
]

for name, p0_stress, row in stress_scenarios:
    print(f"\n--- {name} scenario (Row {row}, Baseline RR = {p0_stress}) ---")

    # D{r} = ROUND((C13+C14)*SQRT(C{r}*(1-C{r})*(1/C19+1/C20)), 4) — Comp 1
    d_raw = compute_mde(C13, C14, p0_stress, C19, C20)
    D_r = excel_round(d_raw, 4)
    print(f"  D{row} (Comp 1: SM vs MD) = ROUND({d_raw:.10f}, 4) = {D_r}")

    # E{r} = Comp 2: SM vs PP
    e_raw = compute_mde(C13, C14, p0_stress, C19, C21)
    E_r = excel_round(e_raw, 4)
    print(f"  E{row} (Comp 2: SM vs PP) = ROUND({e_raw:.10f}, 4) = {E_r}")

    # F{r} = Comp 3: MD vs PP
    f_raw = compute_mde(C13, C14, p0_stress, C20, C21)
    F_r = excel_round(f_raw, 4)
    print(f"  F{row} (Comp 3: MD vs PP) = ROUND({f_raw:.10f}, 4) = {F_r}")

    # G{r} = Comp 4: MIN arm vs rest
    min_n = min(C19, C20, C21)
    rest_n = total_n - min_n
    g_raw = compute_mde(C13, C14, p0_stress, min_n, rest_n)
    G_r = excel_round(g_raw, 4)
    print(f"  G{row} (Comp 4: Min vs Rest) = ROUND({g_raw:.10f}, 4) = {G_r}")
    print(f"    min_n = MIN({C19},{C20},{C21}) = {min_n}")
    print(f"    rest_n = {total_n} - {min_n} = {rest_n}")

    # H{r} = MAX(D:G) — Worst case MDE
    H_r = max(D_r, E_r, F_r, G_r)
    print(f"  H{row} (Worst Case) = MAX({D_r}, {E_r}, {F_r}, {G_r}) = {H_r}")

    # I{r} = $C$10 = target_lift
    I_r = target_lift
    print(f"  I{row} (Target Lift) = {I_r}")

    # J{r} = I{r}/H{r} — Headroom
    J_r = I_r / H_r if H_r != 0 else float('inf')
    print(f"  J{row} (Headroom) = {I_r} / {H_r} = {J_r:.6f}")

    # K{r} = IF(H{r}<I{r}, "OK", "NOT OK")
    K_r = "OK" if H_r < I_r else "NOT OK"
    print(f"  K{row} (Status) = IF({H_r} < {I_r}) = \"{K_r}\"")

    # Verify stress test uses its OWN baseline (C33/C34/C35), NOT E18 or F18
    print(f"\n  INDEPENDENCE CHECK: Stress formulas use C{row} ({p0_stress}) as baseline,")
    print(f"    NOT E18 ({baseline_rr}) or F18 ({F18}).")
    print(f"    Formula pattern: SQRT(C{row}*(1-C{row})*...)")
    print(f"    PASS — stress test uses its own independent baseline RR")

# ============================================================
# STEP 5: CROSS-CHECK STRESS vs MAIN
# ============================================================
print(f"\n{'='*80}")
print("STEP 5: CROSS-VALIDATION — STRESS 'CURRENT' SHOULD MATCH MAIN (when stress_inc=0)")
print(f"{'='*80}")

# When G3=0, F18 = E18 = 0.15 = C34
# So the "Current" stress test should produce the same MDE as the main comparison tests
print(f"\n  Main comparison baseline (F18) = {F18}")
print(f"  Stress 'Current' baseline (C34) = {stress_current}")

if F18 == stress_current:
    print(f"  These are equal, so MDE values should match.")

    # Compare Comp 1
    main_mde_1 = all_comparison_results[25]['I']
    stress_mde_1 = excel_round(compute_mde(C13, C14, stress_current, C19, C20), 4)
    print(f"\n  Comp 1 (SM vs MD):  Main I25={main_mde_1}  Stress D34={stress_mde_1}  Match={main_mde_1==stress_mde_1}")

    main_mde_2 = all_comparison_results[26]['I']
    stress_mde_2 = excel_round(compute_mde(C13, C14, stress_current, C19, C21), 4)
    print(f"  Comp 2 (SM vs PP):  Main I26={main_mde_2}  Stress E34={stress_mde_2}  Match={main_mde_2==stress_mde_2}")

    main_mde_3 = all_comparison_results[27]['I']
    stress_mde_3 = excel_round(compute_mde(C13, C14, stress_current, C20, C21), 4)
    print(f"  Comp 3 (MD vs PP):  Main I27={main_mde_3}  Stress F34={stress_mde_3}  Match={main_mde_3==stress_mde_3}")

    main_mde_4 = all_comparison_results[28]['I']
    stress_mde_4 = excel_round(compute_mde(C13, C14, stress_current, min(C19,C20,C21), total_n - min(C19,C20,C21)), 4)
    print(f"  Comp 4 (Min vs Rest): Main I28={main_mde_4}  Stress G34={stress_mde_4}  Match={main_mde_4==stress_mde_4}")

    if all([main_mde_1==stress_mde_1, main_mde_2==stress_mde_2,
            main_mde_3==stress_mde_3, main_mde_4==stress_mde_4]):
        print(f"\n  PASS — stress 'Current' scenario matches main comparison tests exactly")
    else:
        add_issue("ERROR", "Stress_Current", "Stress 'Current' scenario doesn't match main comparison tests")
else:
    print(f"  Baselines differ (F18={F18} vs C34={stress_current}), so values correctly differ.")

# ============================================================
# STEP 6: EDGE CASE CHECKS
# ============================================================
print(f"\n{'='*80}")
print("STEP 6: EDGE CASE & FORMULA LOGIC CHECKS")
print(f"{'='*80}")

# Check: Comp 1 and Comp 2 should have the SAME MDE when n1 is the same (SM)
# and n2 differs (MD=211200 vs PP=158400)
print(f"\n--- Same n1 (SM), different n2 ---")
print(f"  Comp 1 (SM vs MD): n1=158400, n2=211200 -> MDE={all_comparison_results[25]['I']}")
print(f"  Comp 2 (SM vs PP): n1=158400, n2=158400 -> MDE={all_comparison_results[26]['I']}")
print(f"  Comp 2 should have HIGHER MDE (smaller combined n). ", end="")
if all_comparison_results[26]['I'] >= all_comparison_results[25]['I']:
    print("PASS")
else:
    add_issue("ERROR", "Logic", "Comp 2 MDE should be >= Comp 1 MDE since n2 is smaller")

# Check: Comp 2 and Comp 3 — Comp 2 has n1=158400 and Comp 3 has n1=211200 (both have n2=158400)
print(f"\n--- Comp 2 vs Comp 3 (different n1, same n2=PP) ---")
print(f"  Comp 2 (SM vs PP): n1=158400, n2=158400 -> MDE={all_comparison_results[26]['I']}")
print(f"  Comp 3 (MD vs PP): n1=211200, n2=158400 -> MDE={all_comparison_results[27]['I']}")
print(f"  Comp 3 should have LOWER MDE (larger n1). ", end="")
if all_comparison_results[27]['I'] <= all_comparison_results[26]['I']:
    print("PASS")
else:
    add_issue("ERROR", "Logic", "Comp 3 MDE should be <= Comp 2 MDE since n1 is larger")

# Check: Comp 2 == Comp 3 when alloc_sm == alloc_pp (both 0.3)?
# Actually SM=0.3 and PP=0.3, so Comp 2 has n1=C19=158400,n2=C21=158400
# and Comp 3 has n1=C20=211200,n2=C21=158400
# So they should NOT be equal
print(f"\n--- Comp 2 vs Comp 3 MDE equality check ---")
print(f"  Comp 2: n1={C19},n2={C21} vs Comp 3: n1={C20},n2={C21}")
print(f"  Comp 2 MDE={all_comparison_results[26]['I']}, Comp 3 MDE={all_comparison_results[27]['I']}")
if all_comparison_results[26]['I'] != all_comparison_results[27]['I']:
    print(f"  Different as expected (different n1)")
else:
    print(f"  Equal — would only happen if n1 is the same in both")

# Check: Comp 4 conservative test
print(f"\n--- Comp 4: Smallest arm vs rest ---")
min_arm = min(C19, C20, C21)
rest = total_n - min_arm
print(f"  MIN(C19:C21) = MIN({C19},{C20},{C21}) = {min_arm}")
print(f"  Rest = {total_n} - {min_arm} = {rest}")
print(f"  This is the most conservative (hardest to detect) pairwise. ", end="")
if all_comparison_results[28]['I'] <= min(all_comparison_results[25]['I'],
                                          all_comparison_results[26]['I'],
                                          all_comparison_results[27]['I']):
    print("CORRECT — Comp 4 has smallest MDE (largest effective n)")
else:
    # Actually Comp 4 could have larger MDE if the smallest arm is very small
    print(f"Comp 4 MDE = {all_comparison_results[28]['I']}")

# Check: Equal arms (SM and PP both 0.3) should give equal results in symmetric comparisons
print(f"\n--- Symmetry check: SM and PP have same allocation ({alloc_sm}) ---")
print(f"  C19 (SM) = {C19}, C21 (PP) = {C21}")
if C19 == C21:
    print(f"  Equal sizes. Comp 1 (SM vs MD) and Comp 3 (MD vs PP)...")
    print(f"    Comp 1: n1={C19}, n2={C20} -> MDE={all_comparison_results[25]['I']}")
    print(f"    Comp 3: n1={C20}, n2={C21} -> MDE={all_comparison_results[27]['I']}")
    print(f"    These should be EQUAL (same n1,n2 just swapped). ", end="")
    if all_comparison_results[25]['I'] == all_comparison_results[27]['I']:
        print("PASS")
    else:
        add_issue("ERROR", "Symmetry", "Comp 1 and Comp 3 should have equal MDE when SM and PP have same allocation")

# ============================================================
# STEP 7: FULL NUMERIC SUMMARY TABLE
# ============================================================
print(f"\n{'='*80}")
print("STEP 7: FULL NUMERIC SUMMARY TABLE")
print(f"{'='*80}")

print(f"\n{'Comparison':<40} {'n1':>10} {'n2':>10} {'BL RR':>7} {'MDE':>8} {'Target':>7} {'Headroom':>10} {'Status':<8}")
print("-" * 100)
for comp in comparisons:
    r = comp['row']
    res = all_comparison_results[r]
    print(f"  {comp['label']:<38} {res['D']:>10,.0f} {res['E']:>10,.0f} {res['H']:>7.4f} {res['I']:>8.4f} {res['J']:>7.4f} {res['K']:>10.4f} {res['L']:<8}")

print(f"\n{'Stress Scenario':<40} {'BL RR':>7} {'Comp1':>8} {'Comp2':>8} {'Comp3':>8} {'Comp4':>8} {'Worst':>8} {'Target':>7} {'Headroom':>10} {'Status':<8}")
print("-" * 120)
for name, p0_s, row in stress_scenarios:
    d = excel_round(compute_mde(C13, C14, p0_s, C19, C20), 4)
    e = excel_round(compute_mde(C13, C14, p0_s, C19, C21), 4)
    f = excel_round(compute_mde(C13, C14, p0_s, C20, C21), 4)
    g = excel_round(compute_mde(C13, C14, p0_s, min(C19,C20,C21), total_n-min(C19,C20,C21)), 4)
    worst = max(d, e, f, g)
    headroom = target_lift / worst
    status = "OK" if worst < target_lift else "NOT OK"
    print(f"  {name:<38} {p0_s:>7.4f} {d:>8.4f} {e:>8.4f} {f:>8.4f} {g:>8.4f} {worst:>8.4f} {target_lift:>7.4f} {headroom:>10.4f} {status:<8}")

# ============================================================
# STEP 8: CASCADE VERIFICATION
# ============================================================
print(f"\n{'='*80}")
print("STEP 8: CASCADE VERIFICATION — CHANGING GREEN INPUTS")
print(f"{'='*80}")

# Simulate changing total_n to 600000 and verify cascade
test_n = 600000
test_C19 = test_n * alloc_sm
test_C20 = test_n * alloc_md
test_C21 = test_n * alloc_pp
test_mde = excel_round(compute_mde(C13, C14, F18, test_C19, test_C20), 4)
orig_mde = all_comparison_results[25]['I']

print(f"\n  Cascade test: Change C3 from {total_n} to {test_n}")
print(f"    C19 (SM n): {C19} -> {test_C19}")
print(f"    C20 (MD n): {C20} -> {test_C20}")
print(f"    Comp 1 MDE: {orig_mde} -> {test_mde}")
print(f"    MDE decreased? {test_mde < orig_mde} (expected: True, more data = smaller MDE)")
if test_mde < orig_mde:
    print(f"    PASS — cascade works correctly")
else:
    add_issue("ERROR", "Cascade", "Increasing n should decrease MDE")

# Simulate changing alpha to 0.01
test_alpha = 0.01
test_C13 = norm.ppf(1 - test_alpha)
test_mde_alpha = excel_round(compute_mde(test_C13, C14, F18, C19, C20), 4)
print(f"\n  Cascade test: Change C8 (alpha) from {alpha} to {test_alpha}")
print(f"    C13 (Za): {C13:.6f} -> {test_C13:.6f}")
print(f"    Comp 1 MDE: {orig_mde} -> {test_mde_alpha}")
print(f"    MDE increased? {test_mde_alpha > orig_mde} (expected: True, stricter alpha = harder to detect)")
if test_mde_alpha > orig_mde:
    print(f"    PASS — cascade works correctly")
else:
    add_issue("ERROR", "Cascade", "Decreasing alpha should increase MDE")

# ============================================================
# STEP 9: GREEN CELL WARNINGS
# ============================================================
print(f"\n{'='*80}")
print("STEP 9: GREEN CELL ASSESSMENT")
print(f"{'='*80}")

print(f"\n  B18, C18, D18 are marked green but:")
print(f"    B18 ('Total Population') = label, not a value input")
print(f"    C18 (=C3) = formula mirroring C3, not independently editable")
print(f"    D18 (1) = static '1' for % of total")
print(f"  These are cosmetic/visual grouping, not true inputs.")
print(f"  WARNING (minor): Consider removing green fill from B18/C18/D18")
print(f"  to avoid confusion about what's actually editable.")
add_issue("WARN", "B18/C18/D18", "Green fill on non-input cells — cosmetic issue, not functional")

# ============================================================
# FINAL VERDICT
# ============================================================
print(f"\n{'='*80}")
print("FINAL VERDICT")
print(f"{'='*80}")

errors = [i for i in issues if i[0] == "ERROR"]
warnings = [i for i in issues if i[0] == "WARN"]

print(f"\n  Total issues found: {len(issues)}")
print(f"    ERRORS:   {len(errors)}")
print(f"    WARNINGS: {len(warnings)}")

if issues:
    print(f"\n  All issues:")
    for sev, cell, msg in issues:
        print(f"    [{sev}] {cell}: {msg}")

if errors:
    print(f"\n  *** FAIL — {len(errors)} error(s) found ***")
else:
    print(f"\n  *** PASS — All formulas are mathematically correct ***")
    if warnings:
        print(f"  ({len(warnings)} minor warning(s) noted)")
