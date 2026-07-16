"""Patch coverage cell to fix customer_name KeyError."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NB = ROOT / "ai_fleet_evaluation_analytics.ipynb"

OLD = '    cat = sessions_catalog[["session_id", "max_turns", "session_type", "customer_name"]].copy()\n'

NEW_BLOCK = '''    if "customer_name" not in sessions_catalog.columns:
        sessions_catalog = flatten_catalog_sessions(sessions_catalog)
    cat_cols = [c for c in ["session_id", "max_turns", "session_type", "customer_name"] if c in sessions_catalog.columns]
    cat = sessions_catalog[cat_cols].copy()
'''

nb = json.loads(NB.read_text(encoding="utf-8"))
patched = 0
for cell in nb["cells"]:
    if cell.get("cell_type") != "code":
        continue
    src = "".join(cell.get("source", []))
    if OLD.strip() in src.replace("\r\n", "\n"):
        src = src.replace(OLD, NEW_BLOCK)
        cell["source"] = [line + "\n" for line in src.splitlines()]
        if cell["source"] and not cell["source"][-1].endswith("\n"):
            cell["source"][-1] += "\n"
        patched += 1
    elif "cat_cols" not in src and "missing_turns" in src and "sessions_catalog" in src:
        # Old pattern without cat_cols - fix the block
        old2 = '    cat = sessions_catalog[["session_id", "max_turns", "session_type", "customer_name"]].copy()'
        if old2 in src:
            src = src.replace(old2, NEW_BLOCK.strip())
            cell["source"] = [line + "\n" for line in src.splitlines()]
            patched += 1

# Ensure re-flatten preamble exists in coverage cell
PREAMBLE = """# Re-flatten catalog if kernel has stale sessions_catalog from an older notebook version
if "sessions_catalog" not in dir() or sessions_catalog.empty:
    _cat = load_json(CATALOG_PATH) or {}
    sessions_catalog = flatten_catalog_sessions(pd.DataFrame(_cat.get("sessions", [])))
elif "customer_name" not in sessions_catalog.columns:
    sessions_catalog = flatten_catalog_sessions(sessions_catalog)

"""

for cell in nb["cells"]:
    if cell.get("cell_type") != "code":
        continue
    src = "".join(cell.get("source", []))
    if "Unique sessions (deduped rollups)" in src and PREAMBLE.strip() not in src:
        if not src.startswith(PREAMBLE.strip()[:20]):
            # insert after first line if cs = primary
            if 'cs = primary["meta"]' in src and PREAMBLE not in src:
                src = src.replace(
                    'cs = primary["meta"]',
                    PREAMBLE + 'cs = primary["meta"]',
                    1,
                )
                cell["source"] = [line + "\n" for line in src.splitlines()]
                patched += 1

NB.write_text(json.dumps(nb, indent=1), encoding="utf-8")
print(f"Patched cells: {patched}")
print(f"Wrote {NB}")
