"""Execute notebook logic without Jupyter."""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

ARTIFACTS = ROOT / "evaluation" / "artifacts"
RUNS = {
    "production_1000": ARTIFACTS / "eval_20260520_085108_190cf6",
    "phase1_local": ARTIFACTS / "eval_20260520_065502_5e18bb",
    "phase2_smoke50": ARTIFACTS / "eval_20260520_081915_ef7840",
}
PRIMARY_RUN = "production_1000"
CATALOG_PATH = ROOT / "evaluation" / "conversational_scenarios" / "sessions_1000.json"
EXPORT_DIR = ARTIFACTS / "analytics_exports"
COST_PER_1M = {
    "gpt-3.5-turbo": {"input": 0.50, "output": 1.50},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
}
ANALYST_MODEL = "gpt-3.5-turbo"
JUDGE_MODEL = "gpt-4o-mini"
SIMULATOR_MODEL = "gpt-4o-mini"
SIMULATOR_TOKENS_PER_TURN = 800


def load_json(path: Path):
    if not path.is_file():
        return None
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def load_jsonl(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def dedupe_sessions(df: pd.DataFrame, key: str = "session_id") -> pd.DataFrame:
    if df.empty or key not in df.columns:
        return df.copy()
    out = df.copy()
    if "timestamp" in out.columns:
        out = out.sort_values("timestamp")
    return out.drop_duplicates(subset=[key], keep="last")


def normalize_phase1_turns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "token_usage" in out.columns:
        tu = out["token_usage"].apply(lambda x: x if isinstance(x, dict) else {})
        out["tokens_prompt"] = tu.apply(lambda d: d.get("prompt"))
        out["tokens_completion"] = tu.apply(lambda d: d.get("completion"))
        out["tokens_total"] = tu.apply(lambda d: d.get("total"))
    if "sql_event_count" in out.columns and "sql_query_count" not in out.columns:
        out["sql_query_count"] = out["sql_event_count"]
    if "elapsed_ms" in out.columns and "latency_sec" not in out.columns:
        out["latency_sec"] = out["elapsed_ms"] / 1000.0
    if "langsmith" in out.columns:
        ls = out["langsmith"].apply(lambda x: x if isinstance(x, dict) else {})
        if "request_id" not in out.columns:
            out["request_id"] = ls.apply(lambda d: d.get("request_id"))
    out["session_type"] = out.get("session_type", "scenario")
    if "scenario_id" in out.columns and "session_id" not in out.columns:
        out["session_id"] = out["scenario_id"]
    return out


def load_run(name: str) -> dict:
    base = RUNS[name]
    meta = {}
    for fname in ("collection_summary.json", "session_summary.json", "summary.json", "eval_target.json"):
        p = base / fname
        if p.is_file():
            meta[fname.replace(".json", "")] = load_json(p)
    turns = pd.DataFrame()
    rollups = pd.DataFrame()
    if (base / "session_turns.jsonl").is_file():
        turns = load_jsonl(base / "session_turns.jsonl")
        rollups = dedupe_sessions(load_jsonl(base / "session_rollups.jsonl"))
    elif (base / "turns.jsonl").is_file():
        turns = normalize_phase1_turns(load_jsonl(base / "turns.jsonl"))
        rollups = load_jsonl(base / "scenarios_rollup.jsonl")
    turns["run_label"] = name
    if not rollups.empty:
        rollups["run_label"] = name
    excel_agg = pd.DataFrame()
    excel_path = base / "full_evaluation_report.xlsx"
    if excel_path.is_file():
        try:
            excel_agg = pd.read_excel(excel_path, sheet_name="Aggregates")
        except Exception:
            pass
    return {
        "name": name,
        "turns": turns,
        "rollups": rollups,
        "meta": meta,
        "excel_agg": excel_agg,
        "sql_events": load_jsonl(base / "sql_events.jsonl"),
        "validation_turns": load_jsonl(base / "validation" / "validation_turn_scores.jsonl"),
    }


def cost_usd(prompt, completion, model):
    rates = COST_PER_1M.get(model, COST_PER_1M["gpt-4o-mini"])
    return (prompt / 1e6 * rates["input"]) + (completion / 1e6 * rates["output"])


def add_cost_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for c in ("tokens_prompt", "tokens_completion"):
        if c not in out.columns:
            out[c] = 0
    out["tokens_prompt"] = pd.to_numeric(out["tokens_prompt"], errors="coerce").fillna(0)
    out["tokens_completion"] = pd.to_numeric(out["tokens_completion"], errors="coerce").fillna(0)
    out["analyst_cost_usd"] = out.apply(
        lambda r: cost_usd(r["tokens_prompt"], r["tokens_completion"], ANALYST_MODEL), axis=1
    )
    out["judge_cost_usd"] = out.apply(
        lambda r: cost_usd(r["tokens_prompt"] * 0.15, r["tokens_completion"] * 0.05, JUDGE_MODEL), axis=1
    )
    out["simulator_cost_usd"] = 0.0
    if "follow_up_source" in out.columns:
        sim_mask = out["follow_up_source"] == "simulated"
        out.loc[sim_mask, "simulator_cost_usd"] = cost_usd(
            SIMULATOR_TOKENS_PER_TURN * 0.7, SIMULATOR_TOKENS_PER_TURN * 0.3, SIMULATOR_MODEL
        )
    out["total_cost_usd"] = out["analyst_cost_usd"] + out["judge_cost_usd"] + out["simulator_cost_usd"]
    return out


def main():
    DATA = {k: load_run(k) for k in RUNS}
    primary = DATA[PRIMARY_RUN]
    turns_p = add_cost_columns(primary["turns"].copy())
    rollups_p = primary["rollups"]
    assert rollups_p["session_id"].nunique() == 1000, rollups_p["session_id"].nunique()
    assert len(turns_p) == 895, len(turns_p)
    catalog = load_json(CATALOG_PATH) or {}
    sessions_catalog = pd.DataFrame(catalog.get("sessions", []))
    assert len(sessions_catalog) == 1000
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    print("OK: production 1000 sessions, 895 turns, catalog 1000")
    for k in RUNS:
        print(f"  {k}: turns={len(DATA[k]['turns'])}")


if __name__ == "__main__":
    main()
