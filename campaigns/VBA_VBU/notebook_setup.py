import builtins
import pandas as pd
from IPython.core.interactiveshell import InteractiveShell
from IPython.display import display

InteractiveShell.ast_node_interactivity = 'all'

pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 200)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)
pd.set_option('display.expand_frame_repr', False)

_native_print = builtins.print
def _smart_print(*args, **kwargs):
    if len(args) == 1 and isinstance(args[0], (pd.DataFrame, pd.Series)):
        display(args[0])
    else:
        _native_print(*args, **kwargs)
builtins.print = _smart_print
