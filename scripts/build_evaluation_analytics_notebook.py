"""One-shot generator for ai_fleet_evaluation_analytics.ipynb."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "ai_fleet_evaluation_analytics.ipynb"


def md(source: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": source.splitlines(keepends=True)}


def code(source: str) -> dict:
    return {
        "cell_type": "code",
        "metadata": {},
        "source": source.splitlines(keepends=True),
        "outputs": [],
        "execution_count": None,
    }


def notebook(cells: list[dict]) -> dict:
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.10.0"},
        },
        "cells": cells,
    }


CELLS: list[dict] = []

CELLS.append(md("""# Fleet AI Evaluation Analytics

End-to-end observability for DTC Analytics AI bot evaluation runs:
- **Primary:** 1000-session production benchmark (`eval_20260520_085108_190cf6`)
- **Baselines:** Phase 1 local (`065502`) and Phase 2 smoke 50 (`081915`)

**Data hygiene:** `session_rollups.jsonl` may contain duplicate `session_id` rows from restarts — always dedupe (keep latest `timestamp`) before session-level KPIs."""))

CELLS.append(code("""# Optional: install viz libs if missing
# %pip install -q seaborn plotly jupyter"""))

CELLS.append(code("""from __future__ import annotations

import json
import os
import warnings
from collections import Counter
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

%matplotlib inline

try:
    import seaborn as sns
    sns.set_theme(style="whitegrid", palette="colorblind")
except ImportError:
    sns = None

try:
    from IPython.display import display, Markdown
except ImportError:
    display = print
    Markdown = print

# --- Config ---
def find_project_root() -> Path:
    here = Path.cwd().resolve()
    for p in [here, *here.parents]:
        if (p / "evaluation" / "artifacts").is_dir():
            return p
    return here

PROJECT_ROOT = find_project_root()
ARTIFACTS = PROJECT_ROOT / "evaluation" / "artifacts"

RUNS = {
    "production_1000": ARTIFACTS / "eval_20260520_085108_190cf6",
    "phase1_local": ARTIFACTS / "eval_20260520_065502_5e18bb",
    "phase2_smoke50": ARTIFACTS / "eval_20260520_081915_ef7840",
}
PRIMARY_RUN = "production_1000"
CATALOG_PATH = PROJECT_ROOT / "evaluation" / "conversational_scenarios" / "sessions_1000.json"
EXPORT_DIR = ARTIFACTS / "analytics_exports"

# Cost model (USD per 1M tokens) — edit as needed
COST_PER_1M = {
    "gpt-3.5-turbo": {"input": 0.50, "output": 1.50},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
}

ANALYST_MODEL = "gpt-3.5-turbo"
JUDGE_MODEL = "gpt-4o-mini"
SIMULATOR_MODEL = "gpt-4o-mini"
SIMULATOR_TOKENS_PER_TURN = 800  # estimate when simulated follow-ups lack token rows

LANGSMITH_SAMPLE_N = 50

print(f"Project root: {PROJECT_ROOT}")
for k, p in RUNS.items():
    print(f"  {k}: exists={p.is_dir()}")"""))

CELLS.append(md("## 0. Load helpers"))

CELLS.append(code("""def load_json(path: Path) -> dict | list | None:
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
    \"\"\"Map Phase 1 turns.jsonl to Phase 2-like columns where possible.\"\"\"
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
        out["langsmith_project"] = ls.apply(lambda d: d.get("project"))
    out["session_type"] = out.get("session_type", "scenario")
    if "scenario_id" in out.columns and "session_id" not in out.columns:
        out["session_id"] = out["scenario_id"]
    return out


def load_run(name: str) -> dict[str, Any]:
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
        rollups_raw = load_jsonl(base / "session_rollups.jsonl")
        rollups = dedupe_sessions(rollups_raw)
    elif (base / "turns.jsonl").is_file():
        turns = normalize_phase1_turns(load_jsonl(base / "turns.jsonl"))
        rollups = load_jsonl(base / "scenarios_rollup.jsonl")

    turns["run_label"] = name
    if not rollups.empty:
        rollups["run_label"] = name

    excel_path = base / "full_evaluation_report.xlsx"
    excel_agg = pd.DataFrame()
    if excel_path.is_file():
        try:
            excel_agg = pd.read_excel(excel_path, sheet_name="Aggregates")
        except Exception:
            pass

    return {
        "name": name,
        "path": base,
        "meta": meta,
        "turns": turns,
        "rollups": rollups,
        "excel_agg": excel_agg,
        "sql_events": load_jsonl(base / "sql_events.jsonl"),
        "validation_turns": load_jsonl(base / "validation" / "validation_turn_scores.jsonl"),
    }


def pct(series: pd.Series, q: float) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    return float(s.quantile(q)) if len(s) else float("nan")


def cost_usd(prompt: float, completion: float, model: str) -> float:
    rates = COST_PER_1M.get(model, COST_PER_1M["gpt-4o-mini"])
    return (prompt / 1e6 * rates["input"]) + (completion / 1e6 * rates["output"])


def flatten_catalog_sessions(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"Extract customer_name/mode from nested context dict.\"\"\"
    if df.empty:
        return df
    out = df.copy()
    if "context" in out.columns:
        ctx = out["context"].apply(lambda x: x if isinstance(x, dict) else {})
        if "customer_name" not in out.columns:
            out["customer_name"] = ctx.apply(lambda d: d.get("customer_name"))
        if "mode" not in out.columns:
            out["mode"] = ctx.apply(lambda d: d.get("mode"))
    return out


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
    # Judge uses similar token order-of-magnitude; approximate as 20% of analyst tokens on judge model
    out["judge_cost_usd"] = out.apply(
        lambda r: cost_usd(r["tokens_prompt"] * 0.15, r["tokens_completion"] * 0.05, JUDGE_MODEL),
        axis=1,
    )
    if "follow_up_source" in out.columns:
        sim_mask = out["follow_up_source"] == "simulated"
        out.loc[sim_mask, "simulator_cost_usd"] = cost_usd(
            SIMULATOR_TOKENS_PER_TURN * 0.7, SIMULATOR_TOKENS_PER_TURN * 0.3, SIMULATOR_MODEL
        )
    out["simulator_cost_usd"] = out.get("simulator_cost_usd", pd.Series(0.0, index=out.index)).fillna(0)
    out["total_cost_usd"] = out["analyst_cost_usd"] + out["judge_cost_usd"] + out["simulator_cost_usd"]
    return out


DATA = {k: load_run(k) for k in RUNS}
primary = DATA[PRIMARY_RUN]
turns_p = add_cost_columns(primary["turns"].copy())
rollups_p = primary["rollups"]
print(f"Primary turns: {len(turns_p)}, rollups raw deduped: {len(rollups_p)}")"""))

# Continue building sections in the script file - I'll append more cells programmatically in the same file

def add_section(title: str, md_text: str, code_text: str):
    CELLS.append(md(f"## {title}\n\n{md_text}"))
    CELLS.append(code(code_text))


add_section(
    "1. Data inventory",
    "Schema overview and catalog load.",
    """catalog = load_json(CATALOG_PATH) or {}
sessions_catalog = flatten_catalog_sessions(pd.DataFrame(catalog.get("sessions", [])))
print("Catalog sessions:", len(sessions_catalog))
if not sessions_catalog.empty:
    display(sessions_catalog.groupby("session_type").size().rename("count"))
    if "customer_name" in sessions_catalog.columns:
        top_cust = sessions_catalog["customer_name"].value_counts().head(10)
        print("Top customers in catalog:")
        display(top_cust)

for label, pack in DATA.items():
    t, r = pack["turns"], pack["rollups"]
    print(f"\\n{label}: turns={len(t)}, rollups={len(r)}, cols_turns={len(t.columns) if len(t) else 0}")

# Schema diff
cols = {}
for label, pack in DATA.items():
    cols[label] = set(pack["turns"].columns) if not pack["turns"].empty else set()
if cols:
    all_cols = sorted(set().union(*cols.values()))
    schema_df = pd.DataFrame({k: [c in cols[k] for c in all_cols] for k in cols}, index=all_cols)
    display(schema_df.head(40))""",
)

add_section(
    "2. Coverage & completeness (production)",
    "Deduped session counts, turn coverage vs catalog, Excel cross-check.",
    """# --- Coverage (production) — self-contained catalog flatten ---
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
    display(primary["excel_agg"].head(12))""",
)

add_section(
    "3. Quality & judge metrics",
    "Trace judge, batch judge, segmentation by type/category/tier.",
    """JUDGE_COLS = [
    "trace_judge_final_score", "batch_judge_correctness", "batch_judge_completeness",
    "batch_judge_relevance", "batch_judge_hallucination_risk", "intent_match", "tool_f1", "keyword_hits",
]

if not turns_p.empty:
    for c in JUDGE_COLS:
        if c in turns_p.columns:
            s = pd.to_numeric(turns_p[c], errors="coerce").dropna()
            if len(s):
                print(f"{c}: mean={s.mean():.3f} p50={s.median():.3f} n={len(s)}")

    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    for ax, col in zip(axes.ravel(), ["trace_judge_final_score", "batch_judge_correctness", "tool_f1", "intent_match"]):
        if col in turns_p.columns:
            pd.to_numeric(turns_p[col], errors="coerce").dropna().hist(ax=ax, bins=25)
            ax.set_title(col)
    plt.tight_layout()
    plt.show()

    if "trace_judge_root_cause" in turns_p.columns:
        display(turns_p["trace_judge_root_cause"].value_counts().head(10))

    for group_col in ["session_type", "category", "difficulty_tier", "follow_up_source", "customer_name"]:
        if group_col in turns_p.columns:
            g = turns_p.groupby(group_col).agg(
                n=("session_id", "count"),
                trace_score=("trace_judge_final_score", "mean"),
                batch_corr=("batch_judge_correctness", "mean"),
                tool_f1=("tool_f1", "mean"),
            ).round(3).sort_values("n", ascending=False)
            print(f"\\n--- By {group_col} ---")
            display(g.head(12))

    if sns and "category" in turns_p.columns and "difficulty_tier" in turns_p.columns:
        pivot = turns_p.pivot_table(
            index="category", columns="difficulty_tier",
            values="trace_judge_final_score", aggfunc="mean"
        )
        if not pivot.empty:
            fig, ax = plt.subplots(figsize=(10, max(4, len(pivot) * 0.25)))
            sns.heatmap(pivot, annot=True, fmt=".2f", cmap="RdYlGn", ax=ax, vmin=0, vmax=1)
            ax.set_title("Trace judge score: category x tier")
            plt.tight_layout()
            plt.show()

# Phase 1 validation
p1 = DATA["phase1_local"]
if p1["meta"].get("summary"):
    print("\\nPhase 1 dimension averages:")
    dims = p1["meta"]["summary"].get("dimension_averages", {})
    display(pd.Series(dims).sort_values())
if not p1["validation_turns"].empty:
    vt = p1["validation_turns"]
    if "status" in vt.columns:
        display(vt["status"].value_counts())""",
)

add_section(
    "4. Tooling, SQL & analyst behavior",
    "SQL success, tools, intent mismatches; Phase 1 sql_events.",
    """if not turns_p.empty:
    if "sql_success_rate" in turns_p.columns:
        sr = pd.to_numeric(turns_p["sql_success_rate"], errors="coerce").dropna()
        print(f"SQL success rate: mean={sr.mean():.3f}")
        if "trace_judge_final_score" in turns_p.columns:
            turns_p["_sql"] = pd.to_numeric(turns_p["sql_success_rate"], errors="coerce")
            turns_p["_tj"] = pd.to_numeric(turns_p["trace_judge_final_score"], errors="coerce")
            print("Corr SQL success vs trace judge:", turns_p[["_sql", "_tj"]].corr().iloc[0, 1])

    if "sql_query_count" in turns_p.columns:
        sq = pd.to_numeric(turns_p["sql_query_count"], errors="coerce").dropna()
        if len(sq):
            display(turns_p.loc[sq.index].groupby(
                pd.cut(sq, bins=[-0.1, 1, 2, 5, 20, 999], labels=["1", "2", "3-5", "6-20", "20+"])
            )["trace_judge_final_score"].mean())

    if "expected_intent" in turns_p.columns and "actual_intent" in turns_p.columns:
        mm = turns_p[turns_p["expected_intent"] != turns_p["actual_intent"]]
        print(f"Intent mismatches: {len(mm)}")
        display(mm[["session_id", "turn_index", "expected_intent", "actual_intent", "trace_judge_final_score"]].head(15))

p1_sql = DATA["phase1_local"]["sql_events"]
if not p1_sql.empty:
    print("\\nPhase 1 SQL events:", len(p1_sql))
    for col in ["status", "error_type", "table_name"]:
        if col in p1_sql.columns:
            display(p1_sql[col].value_counts().head(8))""",
)

add_section(
    "5. Latency & reliability",
    "Percentiles, time drift, failure_reasons.",
    """if not turns_p.empty and "latency_sec" in turns_p.columns:
    lat = pd.to_numeric(turns_p["latency_sec"], errors="coerce").dropna()
    print(f"Latency sec: mean={lat.mean():.1f} p50={pct(lat,0.5):.1f} p90={pct(lat,0.9):.1f} p99={pct(lat,0.99):.1f}")
    if "session_type" in turns_p.columns:
        display(turns_p.groupby("session_type")["latency_sec"].agg(["mean", "median", lambda x: pct(x, 0.9)]).round(2))

    if "timestamp" in turns_p.columns:
        ts = turns_p.copy()
        ts["ts"] = pd.to_datetime(ts["timestamp"], errors="coerce")
        ts = ts.dropna(subset=["ts"])
        if len(ts) > 10:
            roll = ts.set_index("ts").sort_index()["latency_sec"].astype(float).rolling(50, min_periods=10).mean()
            fig, ax = plt.subplots(figsize=(12, 4))
            roll.plot(ax=ax)
            ax.set_title("Rolling mean latency (50-turn window)")
            ax.set_ylabel("sec")
            plt.tight_layout()
            plt.show()

if "failure_reasons" in turns_p.columns:
    reasons = []
    for fr in turns_p["failure_reasons"].dropna():
        if isinstance(fr, list):
            reasons.extend(fr)
        elif fr:
            reasons.append(str(fr))
    if reasons:
        display(pd.Series(Counter(reasons)).sort_values(ascending=False).head(20))

bad = turns_p[turns_p.get("status", pd.Series(dtype=str)) != "ok"] if "status" in turns_p.columns else pd.DataFrame()
if not bad.empty:
    print(f"Non-ok turns: {len(bad)}")
    display(bad[["session_id", "turn_index", "status", "failure_reasons"]].head(10))""",
)

add_section(
    "6. Token usage & cost model",
    "Analyst + judge + simulator costs; overhead %.",
    """if not turns_p.empty:
    for c in ("tokens_prompt", "tokens_completion", "tokens_total"):
        if c in turns_p.columns:
            s = pd.to_numeric(turns_p[c], errors="coerce").dropna()
            print(f"{c}: sum={int(s.sum()):,} mean={s.mean():.0f}")

    cost_by_type = turns_p.groupby("session_type").agg(
        turns=("session_id", "count"),
        analyst_usd=("analyst_cost_usd", "sum"),
        judge_usd=("judge_cost_usd", "sum"),
        total_usd=("total_cost_usd", "sum"),
    ).round(2)
    display(cost_by_type)

    total_analyst = turns_p["analyst_cost_usd"].sum()
    total_judge = turns_p["judge_cost_usd"].sum() + turns_p["simulator_cost_usd"].sum()
    overhead_pct = 100 * total_judge / total_analyst if total_analyst else 0
    print(f"Judge+simulator overhead: {overhead_pct:.1f}% of analyst cost")
    print(f"Estimated total run cost: ${turns_p['total_cost_usd'].sum():.2f}")
    per_1000 = turns_p["total_cost_usd"].sum() / max(1, rollups_p["session_id"].nunique()) * 1000
    print(f"Extrapolated cost per 1000 sessions: ${per_1000:.2f}")

    fig, ax = plt.subplots(figsize=(8, 4))
    turns_p.groupby("category")["total_cost_usd"].sum().sort_values(ascending=False).head(12).plot(kind="barh", ax=ax)
    ax.set_title("Total cost by category (top 12)")
    plt.tight_layout()
    plt.show()""",
)

add_section(
    "7. Multi-turn & dynamic conversations",
    "Turn depth vs scores; follow-up sources; memory/efficiency when present.",
    """multi = turns_p[turns_p["session_type"].isin(["static_multi", "dynamic_multi"])] if "session_type" in turns_p.columns else pd.DataFrame()
if not multi.empty:
    if "turn_index" in multi.columns:
        by_turn = multi.groupby("turn_index").agg(
            n=("session_id", "count"),
            trace=("trace_judge_final_score", "mean"),
            batch=("batch_judge_correctness", "mean"),
        ).round(3)
        display(by_turn)
        fig, ax = plt.subplots(figsize=(8, 4))
        by_turn["trace"].plot(marker="o", ax=ax, label="trace judge")
        by_turn["batch"].plot(marker="s", ax=ax, label="batch judge")
        ax.set_title("Score vs turn index (multi-turn)")
        ax.legend()
        plt.tight_layout()
        plt.show()

    if "follow_up_source" in multi.columns:
        display(multi["follow_up_source"].value_counts())

    if "dynamic_policy" in multi.columns:
        display(multi.groupby("dynamic_policy")["trace_judge_final_score"].mean().round(3))

# Phase 1 memory/efficiency nested dicts
p1t = DATA["phase1_local"]["turns"]
if not p1t.empty and "memory" in p1t.columns:
    mem = p1t["memory"].apply(lambda x: x if isinstance(x, dict) else {})
    mem_df = pd.DataFrame(list(mem))
    if not mem_df.empty:
        print("Phase 1 memory metrics (mean):")
        display(mem_df.mean(numeric_only=True).round(3))
else:
    print("Phase 2 turns: nested memory/efficiency not on all rows — use Phase 1 for deep memory analytics.")

if not turns_p.empty and "tokens_total" in turns_p.columns:
    tok_by_sess = pd.to_numeric(turns_p["tokens_total"], errors="coerce").groupby(turns_p["session_id"]).sum()
    turns_count = turns_p.groupby("session_id").size()
    sc = pd.DataFrame({"tokens": tok_by_sess, "turns": turns_count})
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(sc["turns"], sc["tokens"], alpha=0.4)
    ax.set_xlabel("turns per session")
    ax.set_ylabel("total tokens")
    ax.set_title("Conversation length vs tokens")
    plt.tight_layout()
    plt.show()""",
)

add_section(
    "8. Customer & catalog fidelity",
    "VRL mix; catalog vs run customer mismatches.",
    """if not sessions_catalog.empty and not turns_p.empty:
    if "customer_name" not in sessions_catalog.columns:
        sessions_catalog = flatten_catalog_sessions(sessions_catalog)
    cat_cust = sessions_catalog.set_index("session_id")["customer_name"]
    run_cust = turns_p.drop_duplicates("session_id").set_index("session_id")["customer_name"]
    joined = pd.DataFrame({"catalog": cat_cust, "run": run_cust}).dropna(how="all")
    joined["match"] = joined["catalog"] == joined["run"]
    mism = joined[~joined["match"]]
    print(f"Customer mismatches (catalog vs first turn): {len(mism)}")
    if len(mism):
        display(mism.head(10))

    vrl_mask = turns_p["customer_name"].str.contains("VRL", case=False, na=False)
    print(f"VRL turns: {vrl_mask.sum()} / {len(turns_p)} ({100*vrl_mask.mean():.1f}%)")
    compare = turns_p.assign(segment=np.where(vrl_mask, "VRL", "Other")).groupby("segment").agg(
        n=("session_id", "count"),
        trace=("trace_judge_final_score", "mean"),
        latency=("latency_sec", "mean"),
        tokens=("tokens_total", "mean"),
    ).round(3)
    display(compare)""",
)

add_section(
    "9. Environment comparison (local vs production)",
    "Normalized per-turn metrics across three runs.",
    """def run_kpis(pack: dict) -> dict:
    t = add_cost_columns(pack["turns"].copy())
    meta = pack["meta"]
    api = ""
    for m in meta.values():
        if isinstance(m, dict):
            api = m.get("api_base_url") or api
    if t.empty:
        return {"run": pack["name"], "turns": 0}
    return {
        "run": pack["name"],
        "turns": len(t),
        "api": api,
        "avg_latency_sec": pd.to_numeric(t.get("latency_sec"), errors="coerce").mean(),
        "avg_trace_judge": pd.to_numeric(t.get("trace_judge_final_score"), errors="coerce").mean(),
        "avg_batch_judge": pd.to_numeric(t.get("batch_judge_correctness"), errors="coerce").mean(),
        "sql_success_mean": pd.to_numeric(t.get("sql_success_rate"), errors="coerce").mean(),
        "tokens_per_turn": pd.to_numeric(t.get("tokens_total"), errors="coerce").mean(),
        "cost_per_turn_usd": t["total_cost_usd"].mean(),
    }

cmp = pd.DataFrame([run_kpis(DATA[k]) for k in RUNS])
display(cmp.round(3))

print("\\nNote: compare per-turn metrics only — run sizes and session mixes differ.")

fig, ax = plt.subplots(figsize=(10, 4))
x = cmp["run"]
width = 0.35
ax.bar(x, cmp["avg_trace_judge"], label="trace judge")
ax.bar(x, cmp["avg_batch_judge"], alpha=0.7, label="batch judge")
ax.set_ylim(0, 1.05)
ax.legend()
ax.set_title("Judge scores by run")
plt.xticks(rotation=15)
plt.tight_layout()
plt.show()""",
)

add_section(
    "10. LangSmith observability",
    "request_id coverage; optional API sample (quota-safe).",
    """if not turns_p.empty:
    rid = turns_p["request_id"].notna() & (turns_p["request_id"].astype(str).str.len() > 0)
    print(f"request_id coverage: {rid.mean()*100:.1f}% ({rid.sum()}/{len(turns_p)})")
    if "langsmith_project" in turns_p.columns:
        print("Project:", turns_p["langsmith_project"].dropna().iloc[0] if turns_p["langsmith_project"].notna().any() else "n/a")

    ls_export = turns_p[["session_id", "turn_index", "request_id", "category", "trace_judge_final_score"]].dropna(subset=["request_id"])
    display(ls_export.head(8))
    print("Deep link: LangSmith project 'AI for Vehicle Health' + filter request_id")
else:
    ls_export = pd.DataFrame()

# Optional LangSmith API
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except Exception:
    pass

api_key = os.getenv("LANGSMITH_API_KEY") or os.getenv("LANGCHAIN_API_KEY")
if not api_key:
    print("LANGSMITH_API_KEY not set — skipping live trace fetch.")
else:
    try:
        from langsmith import Client
        client = Client()
        sample_ids = ls_export["request_id"].drop_duplicates().head(LANGSMITH_SAMPLE_N).tolist()
        fetched = []
        for rid in sample_ids[:10]:  # small sample to avoid quota
            try:
                run = client.read_run(rid)
                fetched.append({
                    "request_id": rid,
                    "status": getattr(run, "status", None),
                    "total_tokens": getattr(run, "total_tokens", None),
                    "latency_ms": getattr(run, "latency_ms", None),
                })
            except Exception as e:
                fetched.append({"request_id": rid, "error": str(e)[:120]})
        print(f"LangSmith read_run sample: {len(fetched)} rows")
        display(pd.DataFrame(fetched))
    except Exception as e:
        print(f"LangSmith API skipped: {e}")""",
)

add_section(
    "11. Executive summary & exports",
    "KPI snapshot and CSV export.",
    """def executive_summary() -> str:
    lines = ["## Executive Summary", ""]
    if not rollups_p.empty:
        lines.append(f"- **Sessions (deduped):** {rollups_p['session_id'].nunique()}")
        lines.append(f"- **Gate pass rate:** {rollups_p['gate_passed'].mean()*100:.1f}%")
    if not turns_p.empty:
        lines.append(f"- **Turns:** {len(turns_p)}")
        lines.append(f"- **Avg trace judge:** {pd.to_numeric(turns_p['trace_judge_final_score'], errors='coerce').mean():.3f}")
        lines.append(f"- **Avg batch correctness:** {pd.to_numeric(turns_p['batch_judge_correctness'], errors='coerce').mean():.3f}")
        lines.append(f"- **Est. total cost:** ${turns_p['total_cost_usd'].sum():.2f}")
        worst = turns_p.nsmallest(10, "trace_judge_final_score")[["session_id", "category", "trace_judge_final_score", "failure_reasons"]]
        lines.append("\\n### Lowest-scoring turns")
        lines.append("```")
        lines.append(worst.to_string(index=False))
        lines.append("```")
    return "\\n".join(lines)

display(Markdown(executive_summary()))

EXPORT_DIR.mkdir(parents=True, exist_ok=True)
stamp = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
if not turns_p.empty:
    turns_p.to_csv(EXPORT_DIR / f"production_turns_enriched_{stamp}.csv", index=False)
if not rollups_p.empty:
    rollups_p.to_csv(EXPORT_DIR / f"production_sessions_deduped_{stamp}.csv", index=False)
if "cmp" in dir():
    cmp.to_csv(EXPORT_DIR / f"run_comparison_{stamp}.csv", index=False)
print(f"Exported to {EXPORT_DIR}")""",
)

add_section(
    "12. Appendix",
    "Column glossary and how to add new runs.",
    """print("Turn columns (production sample):", list(turns_p.columns)[:30], "...")
print("\\nTo analyze a new run: add path to RUNS dict and re-run notebook.")
print("\\nRe-run evaluation: see README.md Phase 2 section (run_1000_collection.py)")""",
)

if __name__ == "__main__":
    nb = notebook(CELLS)
    OUT.write_text(json.dumps(nb, indent=1), encoding="utf-8")
    print(f"Wrote {OUT} ({len(CELLS)} cells)")
