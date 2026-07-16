import json
from pathlib import Path
nb = json.loads(Path("ai_fleet_evaluation_analytics.ipynb").read_text(encoding="utf-8"))
for i, c in enumerate(nb["cells"]):
    src = "".join(c.get("source", []))
    if "missing_turns" in src:
        print("CELL", i)
        print(src[:800])
        print("---HAS OLD---", '[["session_id"' in src and "cat_cols" not in src)
