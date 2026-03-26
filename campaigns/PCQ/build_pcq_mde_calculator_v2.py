"""
PCQ MDE Calculator v2 — Inverted framing:
"Given population and control %, what's the smallest lift we can detect?"
"""

import os
import openpyxl
from openpyxl.styles import (
    PatternFill, Font, Alignment, Border, Side, numbers
)
from openpyxl.utils import get_column_letter

OUT = r"C:\Users\andre\New_projects\cards\campaigns\PCQ\pcq_mde_calculator_v2.xlsx"

# ── Colours & fills ──────────────────────────────────────────────────────────
GREEN  = PatternFill("solid", fgColor="C6EFCE")
GREY   = PatternFill("solid", fgColor="D9D9D9")
BLUE   = PatternFill("solid", fgColor="4472C4")
WHITE  = PatternFill("solid", fgColor="FFFFFF")

# ── Fonts ────────────────────────────────────────────────────────────────────
BOLD          = Font(bold=True)
BLUE_HDR_FONT = Font(bold=True, color="FFFFFF")
ITALIC        = Font(italic=True)

# ── Borders ──────────────────────────────────────────────────────────────────
thin = Side(style="thin")
THIN_BORDER = Border(left=thin, right=thin, top=thin, bottom=thin)

# ── Number formats ───────────────────────────────────────────────────────────
PCT_FMT   = "0.00%"
PCT_FMT4  = "0.0000%"
NUM_FMT   = "#,##0"
NUM_FMT2  = "#,##0.00"


# ── Helpers ──────────────────────────────────────────────────────────────────
def _apply(cell, value=None, fill=None, font=None, align=None,
           border=THIN_BORDER, num_format=None):
    if value is not None:
        cell.value = value
    if fill:
        cell.fill = fill
    if font:
        cell.font = font
    if align:
        cell.alignment = align
    if border:
        cell.border = border
    if num_format:
        cell.number_format = num_format


def section_header(ws, row, col, text, span_end=None):
    """Grey merged header row."""
    cell = ws.cell(row=row, column=col)
    _apply(cell, value=text, fill=GREY, font=BOLD,
           align=Alignment(horizontal="left", vertical="center"))
    if span_end:
        ws.merge_cells(
            start_row=row, start_column=col,
            end_row=row, end_column=span_end
        )


def col_header_row(ws, row, labels, start_col=1):
    """Blue column-header row."""
    for i, lbl in enumerate(labels):
        c = ws.cell(row=row, column=start_col + i)
        _apply(c, value=lbl, fill=BLUE, font=BLUE_HDR_FONT,
               align=Alignment(horizontal="center", vertical="center",
                               wrap_text=True))


def green_val(ws, row, col, value, num_format=None):
    c = ws.cell(row=row, column=col)
    _apply(c, value=value, fill=GREEN, num_format=num_format)
    return c


def formula_cell(ws, row, col, formula, num_format=None):
    c = ws.cell(row=row, column=col)
    _apply(c, value=formula, num_format=num_format)
    return c


def label_cell(ws, row, col, text, bold=False):
    c = ws.cell(row=row, column=col)
    _apply(c, value=text, font=BOLD if bold else None, fill=WHITE)
    return c


# ── Workbook ─────────────────────────────────────────────────────────────────
wb = openpyxl.Workbook()
ws = wb.active
ws.title = "MDE Calculator"

# ────────────────────────────────────────────────────────────────────────────
# SECTION 1 — Statistical Parameters  (rows 2-8)
# ────────────────────────────────────────────────────────────────────────────
section_header(ws, 1, 1, "SECTION 1 — Statistical Parameters", span_end=3)

# Column headers for section
col_header_row(ws, 2, ["Parameter", "Value", "Note"], start_col=1)

params = [
    (3, "Confidence level (1-alpha)",  0.95,   None,    None),
    (4, "Test type",                   "Two-sided", None, "Two-sided: Za=1.96. Change to 1.6449 for one-sided"),
    (5, "Za (critical value, significance)", 1.96,   None, None),
    (6, "Zb (critical value, power)",  0.8416, None,    "For 80% power"),
    (7, "Combined (Za + Zb)",          None,   "=C5+C6", None),
]

for row, lbl, val, fml, note in params:
    label_cell(ws, row, 1, lbl)
    if fml:
        formula_cell(ws, row, 2, fml, num_format=NUM_FMT2)
    else:
        green_val(ws, row, 2, val)
    if note:
        c = ws.cell(row=row, column=3)
        _apply(c, value=note, font=ITALIC, fill=WHITE)

# ────────────────────────────────────────────────────────────────────────────
# SECTION 2 — Scenario Inputs  (rows 9-25)
# ────────────────────────────────────────────────────────────────────────────
section_header(ws, 9, 1, "SECTION 2 — Scenario Inputs  (green = editable)", span_end=11)

s2_headers = [
    "Scenario", "Deciles Included", "Total Population",
    "Baseline Rate\n(Period-ASC)", "Control %",
    "n_Control", "n_Test",
    "MDE (pp)", "Detectable\nTest Rate",
    "Min Lift\nto Act", "Powered for\nMin Lift?"
]
col_header_row(ws, 10, s2_headers, start_col=1)

scenarios = [
    ("A: 7th only",                "7",    55048,   0.0030, 0.20, 0.005),
    ("B: 7th + 8th",               "7-8",  98838,   0.0025, 0.20, 0.005),
    ("C: 7th-9th",                 "7-9",  167054,  0.0019, 0.20, 0.005),
    ("D: 120K budget (7th-9th)",   "7-9",  120000,  0.0019, 0.20, 0.005),
    ("E: All deciles (7-10)",      "7-10", 235642,  0.0016, 0.20, 0.005),
    ("F: Custom",                  "—",    120000,  0.0030, 0.20, 0.005),
]

# Rows 11-16
for i, (name, deciles, pop, base, ctrl, min_lift) in enumerate(scenarios):
    r = 11 + i
    # Col A — scenario name
    label_cell(ws, r, 1, name)
    # Col B — deciles (label, not editable)
    label_cell(ws, r, 2, deciles)
    # Col C — population (green)
    green_val(ws, r, 3, pop, num_format=NUM_FMT)
    # Col D — baseline (green)
    green_val(ws, r, 4, base, num_format=PCT_FMT4)
    # Col E — control % (green)
    green_val(ws, r, 5, ctrl, num_format=PCT_FMT)

    C = get_column_letter(3)   # population col
    D = get_column_letter(4)   # baseline col
    E = get_column_letter(5)   # control % col
    F = get_column_letter(6)   # n_control col
    G = get_column_letter(7)   # n_test col

    # n_Control = pop * ctrl%
    formula_cell(ws, r, 6, f"={C}{r}*{E}{r}", num_format=NUM_FMT)
    # n_Test = pop * (1 - ctrl%)
    formula_cell(ws, r, 7, f"={C}{r}*(1-{E}{r})", num_format=NUM_FMT)
    # MDE
    mde_fml = (
        f"=ROUND($C$7*SQRT({D}{r}*(1-{D}{r})*(1/{F}{r}+1/{G}{r})),6)"
    )
    formula_cell(ws, r, 8, mde_fml, num_format=PCT_FMT4)
    # Detectable test rate
    formula_cell(ws, r, 9, f"={D}{r}+H{r}", num_format=PCT_FMT4)
    # Min lift to act (green editable)
    green_val(ws, r, 10, min_lift, num_format=PCT_FMT)
    # Powered?
    formula_cell(ws, r, 11,
                 f'=IF(H{r}<=J{r},"YES","NO — MDE too large")')

# ────────────────────────────────────────────────────────────────────────────
# SECTION 3 — Control Split Sensitivity  (rows 28-40)
# ────────────────────────────────────────────────────────────────────────────
S3_START = 28
section_header(ws, S3_START, 1,
               "SECTION 3 — Control Split Sensitivity  (Scenario A: 7th decile, pop & baseline from Section 2)",
               span_end=6)

s3_headers = [
    "Control %", "n_Control", "n_Test",
    "MDE (pp)", "Detectable Rate", "Revenue Impact Note"
]
col_header_row(ws, S3_START + 1, s3_headers, start_col=1)

ctrl_splits = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.50]
for i, ctrl in enumerate(ctrl_splits):
    r = S3_START + 2 + i
    # Control % — green editable
    green_val(ws, r, 1, ctrl, num_format=PCT_FMT)
    # n_Control = Scenario A pop (C11) * ctrl%
    formula_cell(ws, r, 2, f"=ROUND($C$11*A{r},0)", num_format=NUM_FMT)
    # n_Test
    formula_cell(ws, r, 3, f"=ROUND($C$11*(1-A{r}),0)", num_format=NUM_FMT)
    # MDE — uses combined from C7, baseline from D11
    mde = f"=ROUND($C$7*SQRT($D$11*(1-$D$11)*(1/B{r}+1/C{r})),6)"
    formula_cell(ws, r, 4, mde, num_format=PCT_FMT4)
    # Detectable rate
    formula_cell(ws, r, 5, f"=$D$11+D{r}", num_format=PCT_FMT4)
    # Revenue impact note
    c = ws.cell(row=r, column=6)
    _apply(c, value=f'=TEXT(B{r},"#,##0")&" clients withheld from DM"',
           border=THIN_BORDER)

# ────────────────────────────────────────────────────────────────────────────
# SECTION 4 — Baseline Sensitivity  (rows 43-55)
# ────────────────────────────────────────────────────────────────────────────
S4_START = 43
section_header(ws, S4_START, 1,
               "SECTION 4 — Baseline Sensitivity  (fixed pop=120,000 | control=20%)",
               span_end=4)

s4_headers = ["Baseline Rate", "MDE (pp)", "Detectable Rate", "Notes"]
col_header_row(ws, S4_START + 1, s4_headers, start_col=1)

baselines = [0.0010, 0.0015, 0.0020, 0.0025, 0.0030, 0.0040, 0.0050, 0.0100]
notes_s4 = [
    "Below observed 7th-decile range",
    "Below observed 7th-decile range",
    "~7th-9th decile blended",
    "~7th-9th decile blended",
    "7th decile (Jan 2026)",
    "",
    "",
    "Top deciles only"
]

pop_s4   = 120000
ctrl_s4  = 0.20
n_ctrl_s4 = pop_s4 * ctrl_s4         # 24000
n_test_s4 = pop_s4 * (1 - ctrl_s4)   # 96000

for i, (base, note) in enumerate(zip(baselines, notes_s4)):
    r = S4_START + 2 + i
    green_val(ws, r, 1, base, num_format=PCT_FMT4)
    mde = (
        f"=ROUND($C$7*SQRT(A{r}*(1-A{r})"
        f"*(1/{n_ctrl_s4}+1/{n_test_s4})),6)"
    )
    formula_cell(ws, r, 2, mde, num_format=PCT_FMT4)
    formula_cell(ws, r, 3, f"=A{r}+B{r}", num_format=PCT_FMT4)
    c = ws.cell(row=r, column=4)
    _apply(c, value=note, fill=WHITE)

# ────────────────────────────────────────────────────────────────────────────
# SECTION 5 — Minimum Sample Size  (rows 58-70)
# ────────────────────────────────────────────────────────────────────────────
S5_START = 58
section_header(ws, S5_START, 1,
               "SECTION 5 — Minimum Sample Size  (How many clients do we need to detect a given lift?)",
               span_end=4)

# Editable: target control % and label
r_ctrl  = S5_START + 1
r_base_ref = S5_START + 2
label_cell(ws, r_ctrl, 1, "Control % (editable):")
green_val(ws, r_ctrl, 2, 0.20, num_format=PCT_FMT)
label_cell(ws, r_base_ref, 1, "Baseline (from Scenario A, D11):")
c_base_ref = ws.cell(row=r_base_ref, column=2)
_apply(c_base_ref, value="=$D$11", num_format=PCT_FMT4, fill=WHITE)

s5_hdr_row = S5_START + 3
s5_headers = ["Target Lift (pp)", "Min N Total", "Min n_Control", "Min n_Test"]
col_header_row(ws, s5_hdr_row, s5_headers, start_col=1)

target_lifts = [0.0010, 0.0015, 0.0020, 0.0030, 0.0050, 0.0080, 0.0100]

# f = control %, (1-f) = test %
# N = (Za+Zb)^2 * p0*(1-p0) * (1/f + 1/(1-f)) / MDE^2
for i, lift in enumerate(target_lifts):
    r = s5_hdr_row + 1 + i
    green_val(ws, r, 1, lift, num_format=PCT_FMT4)
    # Min N Total
    ctrl_ref  = f"${get_column_letter(2)}${r_ctrl}"
    base_ref  = f"${get_column_letter(2)}${r_base_ref}"
    combined  = "$C$7"
    n_total = (
        f"=CEILING(({combined}^2*{base_ref}*(1-{base_ref})"
        f"*(1/{ctrl_ref}+1/(1-{ctrl_ref})))/A{r}^2,1)"
    )
    formula_cell(ws, r, 2, n_total, num_format=NUM_FMT)
    formula_cell(ws, r, 3, f"=ROUND(B{r}*{ctrl_ref},0)",  num_format=NUM_FMT)
    formula_cell(ws, r, 4, f"=ROUND(B{r}*(1-{ctrl_ref}),0)", num_format=NUM_FMT)

# ────────────────────────────────────────────────────────────────────────────
# SECTION 6 — Reference Data  (rows 73+)
# ────────────────────────────────────────────────────────────────────────────
S6_START = 73
section_header(ws, S6_START, 1,
               "SECTION 6 — Reference Data: Jan 2026 Deployment (2026010PCQ) — Decile Breakdown",
               span_end=8)

s6_headers = [
    "Decile", "Total Clients", "DM Targeted", "DM Coverage",
    "Approved All", "Rate All",
    "Approved Period-ASC", "Rate Period-ASC"
]
col_header_row(ws, S6_START + 1, s6_headers, start_col=1)

ref_data = [
    ("1",     51780, 42539, 0.82, 4755, 0.0918, 3840, 0.0742),
    ("2",     48555, 41027, 0.85, 1793, 0.0369, 1445, 0.0298),
    ("3",     49293, 41873, 0.85, 1137, 0.0231,  925, 0.0188),
    ("4",     50270, 39502, 0.79,  730, 0.0145,  585, 0.0116),
    ("5",     47002, 35876, 0.76,  484, 0.0103,  389, 0.0083),
    ("6",     49254, 29641, 0.60,  342, 0.0069,  251, 0.0051),
    ("7",     55048,  3297, 0.06,  252, 0.0046,  166, 0.0030),
    ("8",     43790,   698, 0.02,  121, 0.0028,   78, 0.0018),
    ("9",     68216,   453, 0.01,  115, 0.0017,   76, 0.0011),
    ("10",    68588,   285, 0.00,   57, 0.0008,   39, 0.0006),
    ("1-6",  296154, 230458, 0.78, 9241, 0.0312, 7435, 0.0251),
    ("TOTAL",531796, 235191, None, 9786, 0.0184, 7794, 0.0147),
]

for i, row_data in enumerate(ref_data):
    r = S6_START + 2 + i
    decile, tot, dm_tgt, dm_cov, app_all, rate_all, app_asc, rate_asc = row_data
    vals = [decile, tot, dm_tgt, dm_cov, app_all, rate_all, app_asc, rate_asc]
    fmts = [None, NUM_FMT, NUM_FMT, PCT_FMT, NUM_FMT, PCT_FMT4, NUM_FMT, PCT_FMT4]
    for j, (v, fmt) in enumerate(zip(vals, fmts)):
        c = ws.cell(row=r, column=1 + j)
        if v is None:
            _apply(c, value="—", fill=WHITE)
        else:
            _apply(c, value=v, fill=WHITE, num_format=fmt)

# ────────────────────────────────────────────────────────────────────────────
# SECTION 7 — Notes  (rows 90+)
# ────────────────────────────────────────────────────────────────────────────
S7_START = 90
section_header(ws, S7_START, 1, "SECTION 7 — Notes & Assumptions", span_end=4)

notes = [
    "1. Period-ASC filter required for true campaign-attributed conversions (per Daniel Chin, March 26, 2026).",
    "2. Baseline rates are from 2026010PCQ (Jan 2026 deployment, most mature ~90 day window).",
    "3. 7th decile is NOT currently targeted for DM — volume there is leakage (~6% coverage).",
    "4. DM is an awareness channel — clients apply via Online/Mobile/Branch, not directly via DM.",
    "5. Clients without DM may be Not Eligible (not opt-outs) — invalid as a no-DM comparison group.",
    "6. Green cells are editable — change them to explore scenarios.",
    "7. MDE = minimum detectable effect. If the true DM lift is smaller than MDE, the test won't detect it.",
    "8. Two-sided test: detects both improvement and degradation. Change Za to 1.6449 for one-sided.",
]

for i, note in enumerate(notes):
    r = S7_START + 1 + i
    c = ws.cell(row=r, column=1)
    _apply(c, value=note, fill=WHITE,
           align=Alignment(wrap_text=True))
    ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=4)

# ────────────────────────────────────────────────────────────────────────────
# Column widths
# ────────────────────────────────────────────────────────────────────────────
col_widths = {
    1: 38,   # scenario / label
    2: 16,   # deciles / value
    3: 16,   # population
    4: 18,   # baseline
    5: 12,   # control %
    6: 14,   # n_control
    7: 14,   # n_test
    8: 14,   # MDE
    9: 16,   # detectable rate
    10: 14,  # min lift
    11: 22,  # powered?
}
for col, width in col_widths.items():
    ws.column_dimensions[get_column_letter(col)].width = width

# Row heights for header rows
for r in [1, 9, S3_START, S4_START, S5_START, S6_START, S7_START]:
    ws.row_dimensions[r].height = 22
for r in [2, 10, S3_START + 1, S4_START + 1, s5_hdr_row, S6_START + 1]:
    ws.row_dimensions[r].height = 38

# Freeze panes below row 2 (section 1 header)
ws.freeze_panes = "A3"

# ────────────────────────────────────────────────────────────────────────────
# Save
# ────────────────────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(OUT), exist_ok=True)
wb.save(OUT)
print(f"Saved: {OUT}")

# ── Force recalculation via win32com ─────────────────────────────────────────
try:
    import win32com.client
    xl = win32com.client.Dispatch("Excel.Application")
    xl.Visible = False
    wb2 = xl.Workbooks.Open(os.path.abspath(OUT))
    wb2.RefreshAll()
    xl.Calculate()
    wb2.Save()
    wb2.Close()
    xl.Quit()
    print("Excel recalculation complete.")
except Exception as e:
    print(f"win32com recalc skipped: {e}")
