"""Force-rewrite coverage cell with inline catalog flatten (no stale-code dependency)."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NB = ROOT / "ai_fleet_evaluation_analytics.ipynb"

COVERAGE_SOURCE = r'''# --- Coverage (production) — self-contained catalog flatten ---
def _catalog_customer_name(row):
    if isinstance(row.get("customer_name"), str):
        return row.get("customer_name")
    ctx = row.get("context")
    if isinstance(ctx, dict):
        return ctx.get("customer_name")
    return None

if "sessions_catalog" not in dir() or sessions_catalog.empty:
    _raw = load_json(CATALOG_PATH) or {}
    sessions_catalog = pd.DataFrame(_raw.get("sessions", []))
if "customer_name" not in sessions_catalog.columns:
    sessions_catalog = sessions_catalog.copy()
    sessions_catalog["customer_name"] = sessions_catalog.apply(_catalog_customer_name, axis=1)

cs = primary["meta"].get("collection_summary", {})
display(pd.Series(cs) if cs else pd.Series({"note": "no collection_summary"}))

n_unique = rollups_p["session_id"].nunique() if not rollups_p.empty else 0
print(f"Unique sessions (deduped rollups): {n_unique}")

if not rollups_p.empty and "session_type" in rollups_p.columns:
    display(rollups_p.groupby("session_type").agg(
        sessions=("session_id", "count"),
        avg_turns=("turns_count", "mean"),
        gate_pass_rate=("gate_passed", "mean"),
        avg_failures=("failure_count", "mean"),
    ).round(3))

if not turns_p.empty:
    turns_per_sess = turns_p.groupby("session_id").size()
    print(f"Turn rows: {len(turns_p)}, sessions with turns: {turns_per_sess.shape[0]}")
    fig, ax = plt.subplots(figsize=(8, 4))
    turns_per_sess.value_counts().sort_index().plot(kind="bar", ax=ax)
    ax.set_title("Turns per session (production)")
    ax.set_xlabel("turn_count")
    plt.tight_layout()
    plt.show()

if not sessions_catalog.empty and not turns_p.empty:
    cat_cols = [c for c in ["session_id", "max_turns", "session_type", "customer_name"] if c in sessions_catalog.columns]
    cat = sessions_catalog[cat_cols].copy()
    actual = turns_p.groupby("session_id").size().rename("actual_turns").reset_index()
    cov = cat.merge(actual, on="session_id", how="left")
    cov["actual_turns"] = cov["actual_turns"].fillna(0).astype(int)
    cov["missing_turns"] = cov["max_turns"] - cov["actual_turns"]
    incomplete = cov[cov["missing_turns"] > 0]
    print(f"Sessions missing turns vs catalog: {len(incomplete)}")
    display(incomplete.head(15))

if not primary["excel_agg"].empty:
    print("Excel Aggregates (sample):")
    display(primary["excel_agg"].head(12))
'''

nb = json.loads(NB.read_text(encoding="utf-8"))
for i, cell in enumerate(nb["cells"]):
    src = "".join(cell.get("source", []))
    if "missing_turns" in src and cell.get("cell_type") == "code":
        cell["source"] = [line + "\n" for line in COVERAGE_SOURCE.splitlines()]
        cell["outputs"] = []
        cell["execution_count"] = None
        print(f"Rewrote cell {i}")
        break

NB.write_text(json.dumps(nb, indent=1), encoding="utf-8")
print("Done. Reload file in editor, Restart Kernel, Run All.")
