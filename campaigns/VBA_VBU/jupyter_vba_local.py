# %% [markdown]
# # VBA Local — Parquet Exploration
#
# Reads parquets exported from `VBA.ipynb` / `vba_vintage_original.py` on the work env.
# `DATA` defaults to the UNC share where the parquets already live (same path used by the source notebook).
# All cells are self-contained — re-import + re-define `DATA` so any cell can be run independently.

# %%
from pathlib import Path
import pandas as pd

DATA = Path(r'\\maple.fg.rbc.com\data\Toronto\wrkgrp\wrkgrp16\Marketing Services & Transformation\Marketing Analytics\Pod of Gold\Cards\VBA\DeepDive\data')

print('DATA =', DATA)
print('exists:', DATA.exists())

if DATA.exists():
    files = sorted(DATA.glob('*.parquet'))
    for f in files:
        size_mb = f.stat().st_size / 1e6
        print(f'  {f.name:<40} {size_mb:>8.2f} MB')
else:
    print('!! DATA folder not reachable. Check the UNC path or your share access.')

# %% [markdown]
# ## vba_tree_input.parquet — decision-tree analytical file
# Curated VBA + portfolio (`last_*`, `total_purch_post`, `days_post_*`) + UCP-business slice.

# %%
from pathlib import Path
import pandas as pd

DATA = Path(r'\\maple.fg.rbc.com\data\Toronto\wrkgrp\wrkgrp16\Marketing Services & Transformation\Marketing Analytics\Pod of Gold\Cards\VBA\DeepDive\data')

df = pd.read_parquet(DATA / 'vba_tree_input.parquet')

print('shape:', df.shape)
print()
print('dtypes:')
print(df.dtypes.to_string())
print()
print('head:')
df.head()

# %% [markdown]
# ## vba_vintage_base.parquet — vintage curves master input
# Pulled from curated VBA table, filtered to tactic_id position 8-10 = 'VBA' and not starting with 'J'.

# %%
from pathlib import Path
import pandas as pd

DATA = Path(r'\\maple.fg.rbc.com\data\Toronto\wrkgrp\wrkgrp16\Marketing Services & Transformation\Marketing Analytics\Pod of Gold\Cards\VBA\DeepDive\data')

df = pd.read_parquet(DATA / 'vba_vintage_base.parquet')

print('shape:', df.shape)
print()
print('dtypes:')
print(df.dtypes.to_string())
print()
print('head:')
df.head()

# %% [markdown]
# ## vba_ucp_business_slice.parquet — UCP business enrichment
# Joined onto tree input on `clnt_no` (one row per client at month-end snapshot).

# %%
from pathlib import Path
import pandas as pd

DATA = Path(r'\\maple.fg.rbc.com\data\Toronto\wrkgrp\wrkgrp16\Marketing Services & Transformation\Marketing Analytics\Pod of Gold\Cards\VBA\DeepDive\data')

df = pd.read_parquet(DATA / 'vba_ucp_business_slice.parquet')

print('shape:', df.shape)
print()
print('dtypes:')
print(df.dtypes.to_string())
print()
print('head:')
df.head()

# %% [markdown]
# ## A/B by wave — Dec 2025 / Jan 2026 / Feb 2026 (finalized waves)
# Action vs Control net_response side-by-side. Lift in pp and relative %.
# Uses `vba_vintage_base.parquet` (full lead population, not the responders-only tree input).

# %%
from pathlib import Path
import pandas as pd

DATA = Path(r'\\maple.fg.rbc.com\data\Toronto\wrkgrp\wrkgrp16\Marketing Services & Transformation\Marketing Analytics\Pod of Gold\Cards\VBA\DeepDive\data')

df = pd.read_parquet(DATA / 'vba_vintage_base.parquet')
df['treatmt_strt_dt'] = pd.to_datetime(df['treatmt_strt_dt'], errors='coerce')

waves = ['2025-12', '2026-01', '2026-02']
sub = df[df['treatmt_strt_dt'].dt.to_period('M').astype(str).isin(waves)].copy()

print('Distinct treatmt_strt_dt × control:')
print(sub.groupby(['treatmt_strt_dt', 'control']).size().to_string())
print()

agg = (
    sub.groupby(['treatmt_strt_dt', 'control'])
       .agg(n=('control', 'size'), resp=('net_response', 'sum'))
)
agg['rate'] = agg['resp'] / agg['n']

wide = agg.unstack('control')
wide.columns = [f'{m}_{c}' for m, c in wide.columns]

if 'rate_Action' in wide.columns and 'rate_Control' in wide.columns:
    wide['lift_pp']  = (wide['rate_Action'] - wide['rate_Control']) * 100
    wide['lift_rel'] = (wide['rate_Action'] / wide['rate_Control'] - 1) * 100

print('Wave-level A/B comparison:')
print(wide.round(4).to_string())

# %% [markdown]
# ## A/B by wave × product — Dec 2025 / Jan 2026 / Feb 2026
# Same comparison split by `visa_offer_prod` (CPX / MC6 / AIB / MCB).

# %%
from pathlib import Path
import pandas as pd

DATA = Path(r'\\maple.fg.rbc.com\data\Toronto\wrkgrp\wrkgrp16\Marketing Services & Transformation\Marketing Analytics\Pod of Gold\Cards\VBA\DeepDive\data')

df = pd.read_parquet(DATA / 'vba_vintage_base.parquet')
df['treatmt_strt_dt'] = pd.to_datetime(df['treatmt_strt_dt'], errors='coerce')

waves = ['2025-12', '2026-01', '2026-02']
sub = df[df['treatmt_strt_dt'].dt.to_period('M').astype(str).isin(waves)].copy()

agg = (
    sub.groupby(['treatmt_strt_dt', 'visa_offer_prod', 'control'])
       .agg(n=('control', 'size'), resp=('net_response', 'sum'))
)
agg['rate'] = agg['resp'] / agg['n']

wide = agg.unstack('control')
wide.columns = [f'{m}_{c}' for m, c in wide.columns]

if 'rate_Action' in wide.columns and 'rate_Control' in wide.columns:
    wide['lift_pp']  = (wide['rate_Action'] - wide['rate_Control']) * 100
    wide['lift_rel'] = (wide['rate_Action'] / wide['rate_Control'] - 1) * 100

print('Wave × product A/B comparison:')
print(wide.round(4).to_string())

# %% [markdown]
# ## Offer test within Action — Dec 2025 / Jan 2026 / Feb 2026
# Q1 GTM lists three Avion variants for AIB/CPX: 35K + full fee, 35K + FW, 35K + Full Fee + 2X points.
# This cell exposes how the offer test is encoded in the data — value counts of the candidate fields,
# then net_response by `visa_offer_prod × visa_offer_test`.

# %%
from pathlib import Path
import pandas as pd

DATA = Path(r'\\maple.fg.rbc.com\data\Toronto\wrkgrp\wrkgrp16\Marketing Services & Transformation\Marketing Analytics\Pod of Gold\Cards\VBA\DeepDive\data')

df = pd.read_parquet(DATA / 'vba_vintage_base.parquet')
df['treatmt_strt_dt'] = pd.to_datetime(df['treatmt_strt_dt'], errors='coerce')

waves = ['2025-12', '2026-01', '2026-02']
sub = df[
    df['treatmt_strt_dt'].dt.to_period('M').astype(str).isin(waves)
    & (df['control'] == 'Action')
].copy()

print('Action-group rows in Dec/Jan/Feb:', len(sub))
print()

for col in ['visa_offer_prod', 'visa_offer_test', 'visa_fee', 'email_creative_id']:
    if col in sub.columns:
        print(f'--- {col} value counts ---')
        print(sub[col].value_counts(dropna=False).head(20).to_string())
        print()

if 'visa_offer_test' in sub.columns:
    agg = (
        sub.groupby(['treatmt_strt_dt', 'visa_offer_prod', 'visa_offer_test'])
           .agg(n=('control', 'size'), resp=('net_response', 'sum'))
    )
    agg['rate'] = agg['resp'] / agg['n']
    print('Offer test breakout (Action only):')
    print(agg.round(4).to_string())

# %% [markdown]
# ## WestJet (MCB) approver client profile — Q1 26
# Who's taking the WJ MasterCard? Profiles MCB approvers in Action group, Dec/Jan/Feb 26.
# Uses vba_tree_input.parquet (responders only, joined to portfolio + UCP-business).
# Adjust the approval filter to whichever flag is populated in your data.

# %%
from pathlib import Path
import pandas as pd

DATA = Path(r'\\maple.fg.rbc.com\data\Toronto\wrkgrp\wrkgrp16\Marketing Services & Transformation\Marketing Analytics\Pod of Gold\Cards\VBA\DeepDive\data')

df = pd.read_parquet(DATA / 'vba_tree_input.parquet')
df['treatmt_strt_dt'] = pd.to_datetime(df['treatmt_strt_dt'], errors='coerce')

print('Total tree-input rows:', len(df))
print('visa_offer_prod values:', df['visa_offer_prod'].value_counts(dropna=False).head(10).to_dict())
print('control values:', df['control'].value_counts(dropna=False).to_dict())
for col in ['gross_response', 'net_response', 'visa_app_approved']:
    if col in df.columns:
        print(f'{col} values: {df[col].value_counts(dropna=False).head().to_dict()}')
print()

waves = ['2025-12', '2026-01', '2026-02']
approval_col = 'visa_app_approved' if 'visa_app_approved' in df.columns else 'gross_response'

mcb = df[
    df['treatmt_strt_dt'].dt.to_period('M').astype(str).isin(waves)
    & (df['visa_offer_prod'] == 'MCB')
    & (df['control'] == 'Action')
    & (df[approval_col] == 1)
].copy()

print(f'MCB approvers in Q1 26 Action group: {len(mcb)}')
print(f'Approval filter used: {approval_col}')
print()

profile_cols = [
    ('decile', 'value_counts'),
    ('tenure_rbc_years', 'describe'),
    ('tenure_rbc_rng', 'value_counts'),
    ('ucp_tenure_rbc_years', 'describe'),
    ('ucp_tenure_rbc_rng', 'value_counts'),
    ('bus_seg', 'value_counts'),
    ('bsc', 'value_counts'),
    ('ucp_bus_seg', 'value_counts'),
    ('ucp_bsc', 'value_counts'),
    ('actv_prod_cnt', 'describe'),
    ('opn_prod_cnt', 'describe'),
    ('ucp_actv_prod_cnt', 'describe'),
    ('digital_trans_ind', 'value_counts'),
    ('mobile_trans_ind', 'value_counts'),
    ('olb_auth_ind', 'value_counts'),
    ('ucp_digital_trans_ind', 'value_counts'),
    ('ucp_mobile_trans_ind', 'value_counts'),
    ('lang_seg_cd', 'value_counts'),
    ('ucp_lang_seg_cd', 'value_counts'),
    ('last_bal', 'describe'),
    ('total_purch_post', 'describe'),
]

for col, mode in profile_cols:
    if col in mcb.columns:
        print(f'--- {col} ---')
        if mode == 'describe' and mcb[col].dtype.kind in 'fi':
            print(mcb[col].describe().to_string())
        else:
            print(mcb[col].value_counts(dropna=False).head(10).to_string())
        print()

# %% [markdown]
# ## Scratch — write queries here
# Each cell self-contained: re-import, re-define `DATA`, re-load the parquet you need.

# %%
