"""Execute all code cells in dtc_bot_analytics_ravi.ipynb and report errors."""
from __future__ import annotations

import json
import os
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NB_PATH = ROOT / 'dtc_bot_analytics_ravi.ipynb'

os.environ.setdefault('PLOTLY_RENDERER', 'json')
import plotly.io as pio
pio.renderers.default = 'json'

nb = json.loads(NB_PATH.read_text(encoding='utf-8'))
ns: dict = {'__name__': '__main__', 'display': lambda x: None}
errors: list[tuple[int, str, str]] = []

for i, cell in enumerate(nb['cells']):
    if cell.get('cell_type') != 'code':
        continue
    src = ''.join(cell.get('source', []))
    if not src.strip() or src.strip().startswith('#'):
        if src.strip().startswith('#') and 'observability' in src.lower():
            print(f'Cell {i}: skip commented appendix')
            continue
    print(f'--- Cell {i} ---')
    try:
        exec(compile(src, f'cell_{i}', 'exec'), ns)
        print('OK')
    except Exception as exc:
        tb = traceback.format_exc()
        errors.append((i, str(exc), tb))
        print(f'FAIL: {exc}')
        print(tb)

if errors:
    print(f'\n{len(errors)} cell(s) failed')
    raise SystemExit(1)
print('\nAll cells passed')
