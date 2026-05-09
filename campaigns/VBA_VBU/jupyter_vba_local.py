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
# ## Scratch — write queries here
# Each cell self-contained: re-import, re-define `DATA`, re-load the parquet you need.

# %%
