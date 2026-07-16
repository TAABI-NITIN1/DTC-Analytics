"""Generate ai_fleet_evaluation_analytics.ipynb from dtc_bot template (sources only)."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'dtc_bot_analytics_ravi.ipynb'
OUT = ROOT / 'ai_fleet_evaluation_analytics.ipynb'

BLUEPRINT_INTRO = """# Taabi AI Analyst — Complete Evaluation Analytics

Operational intelligence for the AI platform (not just pass/fail).

**Layers:** Run → Session → Turn → Trace → System (see [eval_analytics_blueprint.md](docs/eval_analytics_blueprint.md))

**Setup**
1. Activate venv from project root; `pip install pandas plotly jupyter`
2. Set `EVAL_ANALYTICS_RUN_ID` (default: production 1000-session run)
3. **Restart kernel** after pulling notebook updates, then Run All

**Static dashboard:** After analysis, export bundles:
```powershell
python -m evaluation.analytics summarize --run-id eval_20260520_085108_190cf6
python -m evaluation.analytics export-dashboard --run-id eval_20260520_085108_190cf6 --copy-to-eval-dashboard
cd eval-dashboard && npm install && npm run dev
```
"""

SETUP_NOTE = """
# Export dashboard bundle (optional)
from evaluation.analytics.export_dashboard import write_dashboard_bundle
p = write_dashboard_bundle(RUN_ID, baseline_run_id=BASELINE_RUN_ID or None)
print('dashboard_bundle:', p)
"""


def strip_outputs(cell: dict) -> dict:
    if cell.get('cell_type') == 'code':
        cell['outputs'] = []
        cell['execution_count'] = None
    return cell


def main() -> None:
    nb = json.loads(SRC.read_text(encoding='utf-8'))
    cells = [strip_outputs(c) for c in nb['cells']]

    cells[0] = {
        'cell_type': 'markdown',
        'metadata': {},
        'source': [line + '\n' for line in BLUEPRINT_INTRO.splitlines()],
    }

    # Append export cell before appendix if present
    export_cell = {
        'cell_type': 'code',
        'metadata': {},
        'source': [line + '\n' for line in SETUP_NOTE.strip().splitlines()],
        'outputs': [],
        'execution_count': None,
    }
    insert_at = len(cells)
    for i, c in enumerate(cells):
        src = ''.join(c.get('source', []))
        if 'Appendix' in src and c.get('cell_type') == 'markdown':
            insert_at = i
            break
    cells.insert(insert_at, export_cell)

    nb['cells'] = cells
    nb['metadata'] = {
        'kernelspec': {'display_name': 'Python 3', 'language': 'python', 'name': 'python3'},
        'language_info': {'name': 'python', 'version': '3.11.0'},
    }
    OUT.write_text(json.dumps(nb, indent=1), encoding='utf-8')
    print(f'Wrote {OUT} ({len(cells)} cells)')


if __name__ == '__main__':
    main()
