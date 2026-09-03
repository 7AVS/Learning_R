"""Build vbu_split_explainer.xlsx — ONE slide: two tables + three lines.
Table 1 = actual results by score band (Jun+Jul pooled, mature waves).
Table 2 = the 50/50 vs 70/30 decision. v2: numbers, minimal words.
"""
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

BOLD = Font(bold=True)
HDR = PatternFill("solid", fgColor="D9E1F2")
REC = PatternFill("solid", fgColor="E2EFDA")
BOX = Border(*[Side(style="thin")] * 4)
PCT = "0.00%"
NUM = "#,##0"

wb = Workbook()
ws = wb.active
ws.title = "holdout decision"
ws.sheet_view.showGridLines = False

def cell(r, c, v, bold=False, fill=None, fmt=None):
    x = ws.cell(row=r, column=c, value=v)
    x.border = BOX
    if bold: x.font = BOLD
    if fill: x.fill = fill
    if fmt: x.number_format = fmt
    return x

ws.cell(row=1, column=1, value="VBU model-based offers — holdout size (results Jun+Jul 2026)").font = Font(bold=True, size=13)

# Table 1 — what we've seen
r = 3
for j, h in enumerate(["Score band", "NR clients", "NR upgrades", "NR rate",
                       "R_55 clients", "R_55 upgrades", "R_55 rate"], 1):
    cell(r, j, h, bold=True, fill=HDR)
rows = [
    ("1",   4212, 131, 3448, 241),
    ("2",   4601, 106, 3166,  89),
    ("3",   4351,  76, 3591,  64),
    ("4",   4173,  54, 3940,  47),
    ("5-9", 4611,  26, 5769,  38),
]
for i, (band, n_nr, u_nr, n_r, u_r) in enumerate(rows):
    rr = r + 1 + i
    cell(rr, 1, band)
    cell(rr, 2, n_nr, fmt=NUM); cell(rr, 3, u_nr); cell(rr, 4, f"=C{rr}/B{rr}", fmt=PCT)
    cell(rr, 5, n_r, fmt=NUM);  cell(rr, 6, u_r);  cell(rr, 7, f"=F{rr}/E{rr}", fmt=PCT)
t = r + 6
cell(t, 1, "All (communicated)", bold=True)
cell(t, 2, 21948, bold=True, fmt=NUM); cell(t, 3, 393, bold=True); cell(t, 4, f"=C{t}/B{t}", bold=True, fmt=PCT)
cell(t, 5, 19914, bold=True, fmt=NUM); cell(t, 6, 479, bold=True); cell(t, 7, f"=F{t}/E{t}", bold=True, fmt=PCT)
cell(t + 1, 1, "Not communicated", bold=True)
cell(t + 1, 2, 1192, fmt=NUM); cell(t + 1, 3, 0); cell(t + 1, 4, 0, fmt=PCT)
cell(t + 1, 5, 954, fmt=NUM);  cell(t + 1, 6, 0); cell(t + 1, 7, 0, fmt=PCT)

# Table 2 — the decision
r2 = t + 4
for j, h in enumerate(["Per monthly wave", "50/50", "70/30 (recommended)"], 1):
    cell(r2, j, h, bold=True, fill=HDR)
dec = [
    ("Clients held out", "~8,500–13,500", "~5,100–8,100"),
    ("Upgrades given up", "~170–270", "~100–160"),
    ("Reads bands 1–4 (9 of 10 upgrades)", "Yes", "Yes"),
    ("Reads bands 5–9", "No", "No"),
    ("Waves", "2", "2"),
]
for i, (a, b, c) in enumerate(dec):
    rr = r2 + 1 + i
    cell(rr, 1, a)
    cell(rr, 2, b)
    cell(rr, 3, c, fill=REC)

# Three lines
r3 = r2 + 8
for i, line in enumerate([
    "No offer = no upgrades (0 of 2,146). Any random holdout proves the campaign causes them.",
    "9 of 10 upgrades come from bands 1–4 — fully readable at 70/30.",
    "Bands 5–9 are unreadable even at 50/50. The extra ~110 lost upgrades buy nothing.",
]):
    ws.cell(row=r3 + i, column=1, value="• " + line)

for col, w in (("A", 34), ("B", 15), ("C", 15), ("D", 10), ("E", 15), ("F", 15), ("G", 10)):
    ws.column_dimensions[col].width = w

path = (r"C:\Users\andre\New_projects\cards\campaigns\VBA_VBU\propensity_experiment"
        r"\vbu_split_explainer.xlsx")
wb.save(path)
print(f"saved {path}")
