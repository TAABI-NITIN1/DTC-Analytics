"""Run registry at evaluation/artifacts/index.json."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from evaluation.local_store import DEFAULT_OUTPUT_DIR, utc_now_iso

INDEX_FILENAME = 'index.json'


def index_path(artifacts_dir: Path | None = None) -> Path:
    base = artifacts_dir or DEFAULT_OUTPUT_DIR
    return Path(base) / INDEX_FILENAME


def load_index(artifacts_dir: Path | None = None) -> list[dict[str, Any]]:
    path = index_path(artifacts_dir)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except json.JSONDecodeError:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get('runs'), list):
        return data['runs']
    return []


def save_index(entries: list[dict[str, Any]], artifacts_dir: Path | None = None) -> Path:
    path = index_path(artifacts_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(entries, indent=2, ensure_ascii=False), encoding='utf-8')
    return path


def register_run(
    *,
    run_id: str,
    run_dir: Path,
    collection_summary: dict[str, Any] | None = None,
    analytics_summary: dict[str, Any] | None = None,
    artifacts_dir: Path | None = None,
) -> dict[str, Any]:
    """Append or update a run entry in the artifacts index."""
    entries = load_index(artifacts_dir)
    version = (collection_summary or {}).get('version') or (analytics_summary or {}).get('version') or {}
    entry = {
        'run_id': run_id,
        'run_dir': str(run_dir),
        'registered_at': utc_now_iso(),
        'started_at': (collection_summary or {}).get('started_at'),
        'finished_at': (collection_summary or {}).get('finished_at') or (analytics_summary or {}).get('finished_at'),
        'phase': (collection_summary or {}).get('phase'),
        'eval_environment': (collection_summary or {}).get('eval_environment'),
        'api_base_url': (collection_summary or {}).get('api_base_url'),
        'model_name': version.get('model_name') if isinstance(version, dict) else None,
        'git_commit': version.get('git_commit') if isinstance(version, dict) else None,
        'sessions_evaluated': (collection_summary or {}).get('sessions_evaluated'),
        'turns_evaluated': (collection_summary or {}).get('turns_evaluated'),
        'ai_health_score': (analytics_summary or {}).get('run_metrics', {}).get('ai_health_score')
        if isinstance((analytics_summary or {}).get('run_metrics'), dict)
        else (analytics_summary or {}).get('ai_health_score'),
        'pass_rate': (analytics_summary or {}).get('run_metrics', {}).get('pass_rate')
        if isinstance((analytics_summary or {}).get('run_metrics'), dict)
        else None,
    }
    entries = [e for e in entries if str(e.get('run_id')) != run_id]
    entries.append(entry)
    entries.sort(key=lambda e: str(e.get('finished_at') or e.get('registered_at') or ''), reverse=True)
    save_index(entries, artifacts_dir)
    return entry
