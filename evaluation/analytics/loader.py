"""Load and normalize evaluation run artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from evaluation.excel_export import load_jsonl_rows
from evaluation.local_store import DEFAULT_OUTPUT_DIR


@dataclass
class RunBundle:
    run_id: str
    run_dir: Path
    layout: str  # 'session' | 'conversational'
    collection_summary: dict[str, Any] = field(default_factory=dict)
    session_summary: dict[str, Any] = field(default_factory=dict)
    turns: list[dict[str, Any]] = field(default_factory=list)
    sessions: list[dict[str, Any]] = field(default_factory=list)
    sql_events: list[dict[str, Any]] = field(default_factory=list)
    trace_events: list[dict[str, Any]] = field(default_factory=list)
    findings: list[dict[str, Any]] = field(default_factory=list)
    validation_scores: list[dict[str, Any]] = field(default_factory=list)
    analytics_summary: dict[str, Any] = field(default_factory=dict)
    catalog: dict[str, Any] = field(default_factory=dict)

    @property
    def version(self) -> dict[str, Any]:
        v = self.collection_summary.get('version') or self.session_summary.get('version')
        return v if isinstance(v, dict) else {}


def _dedupe_sessions(rows: list[dict[str, Any]], key: str = 'session_id') -> list[dict[str, Any]]:
    """Keep last rollup per session_id (resume checkpoints may duplicate)."""
    seen: dict[str, dict[str, Any]] = {}
    for row in rows:
        sid = str(row.get(key) or row.get('scenario_id') or '').strip()
        if not sid:
            continue
        if row.get('error') and seen.get(sid) and not seen[sid].get('error'):
            continue
        seen[sid] = row
    return list(seen.values())


def _detect_layout(run_dir: Path) -> str:
    if (run_dir / 'session_turns.jsonl').exists():
        return 'session'
    if (run_dir / 'turns.jsonl').exists():
        return 'conversational'
    return 'session'


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


def _load_findings(run_dir: Path) -> list[dict[str, Any]]:
    findings_dir = run_dir / 'validation'
    path = findings_dir / 'findings.jsonl'
    if path.exists():
        return load_jsonl_rows(path)
    return load_jsonl_rows(run_dir / 'validation_findings.jsonl')


def list_runs(artifacts_dir: Path | None = None) -> list[dict[str, Any]]:
    base = Path(artifacts_dir or DEFAULT_OUTPUT_DIR)
    from evaluation.analytics.registry import load_index

    indexed = {str(e.get('run_id')): e for e in load_index(base)}
    for child in sorted(base.iterdir(), reverse=True):
        if not child.is_dir() or not child.name.startswith('eval_'):
            continue
        if child.name not in indexed:
            indexed[child.name] = {'run_id': child.name, 'run_dir': str(child)}
    return sorted(indexed.values(), key=lambda e: str(e.get('finished_at') or e.get('run_id') or ''), reverse=True)


def load_run(
    run_id: str,
    *,
    artifacts_dir: Path | None = None,
    use_parquet_cache: bool = True,
) -> RunBundle:
    base = Path(artifacts_dir or DEFAULT_OUTPUT_DIR)
    run_dir = base / run_id
    if not run_dir.exists():
        raise FileNotFoundError(f'Run directory not found: {run_dir}')

    layout = _detect_layout(run_dir)
    bundle = RunBundle(run_id=run_id, run_dir=run_dir, layout=layout)
    bundle.collection_summary = _load_json(run_dir / 'collection_summary.json')
    bundle.session_summary = _load_json(run_dir / 'session_summary.json') or _load_json(run_dir / 'summary.json')
    bundle.analytics_summary = _load_json(run_dir / 'analytics_summary.json')

    if layout == 'session':
        bundle.turns = load_jsonl_rows(run_dir / 'session_turns.jsonl')
        bundle.sessions = _dedupe_sessions(load_jsonl_rows(run_dir / 'session_rollups.jsonl'))
    else:
        bundle.turns = load_jsonl_rows(run_dir / 'turns.jsonl')
        bundle.sessions = _dedupe_sessions(load_jsonl_rows(run_dir / 'scenarios_rollup.jsonl'), key='scenario_id')

    if not bundle.turns or not use_parquet_cache:
        pass  # loaded from jsonl above

    bundle.sql_events = load_jsonl_rows(run_dir / 'sql_events.jsonl')
    bundle.trace_events = load_jsonl_rows(run_dir / 'trace_events.jsonl')
    bundle.findings = _load_findings(run_dir)
    bundle.validation_scores = load_jsonl_rows(run_dir / 'validation' / 'scores.jsonl')

    sessions_file = bundle.collection_summary.get('sessions_file')
    if sessions_file:
        cat_path = Path(str(sessions_file))
        if cat_path.exists():
            try:
                cat_data = json.loads(cat_path.read_text(encoding='utf-8'))
                bundle.catalog = cat_data if isinstance(cat_data, dict) else {'sessions': cat_data}
            except json.JSONDecodeError:
                pass

    return bundle


def enrich_turns_with_validation(turns: list[dict[str, Any]], findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Attach finding counts per turn from validation findings."""
    by_turn: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for f in findings:
        sid = str(f.get('scenario_id') or f.get('session_id') or '')
        ti = int(f.get('turn_index') or 0)
        by_turn.setdefault((sid, ti), []).append(f)

    enriched = []
    for t in turns:
        row = dict(t)
        sid = str(row.get('session_id') or row.get('scenario_id') or '')
        ti = int(row.get('turn_index') or 0)
        flist = by_turn.get((sid, ti), [])
        row['validation_finding_count'] = len(flist)
        row['hallucination_finding_count'] = sum(1 for x in flist if str(x.get('failure_type', '')).startswith('hallucination.'))
        row['safety_finding_count'] = sum(1 for x in flist if str(x.get('failure_type', '')).startswith('safety.'))
        enriched.append(row)
    return enriched
