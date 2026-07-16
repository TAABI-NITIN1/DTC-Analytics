from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / 'artifacts'


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def _is_nan_like(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, float):
        return math.isnan(value) or math.isinf(value)
    try:
        f = float(value)
        return math.isnan(f) or math.isinf(f)
    except (TypeError, ValueError):
        return False


def sanitize_for_json(value: Any) -> Any:
    """Replace NaN/Inf so output is valid JSON for browsers (JSON.parse rejects NaN)."""
    if _is_nan_like(value):
        return None
    if isinstance(value, dict):
        return {str(k): sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize_for_json(v) for v in value]
    return value


def json_default(value: Any) -> str:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if _is_nan_like(value):
        return 'null'
    return str(value)


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=json_default, separators=(',', ':'))


def hash_text(text: str) -> str:
    return 'sha256:' + hashlib.sha256((text or '').encode('utf-8')).hexdigest()


def stable_hash_json(value: Any) -> str:
    return hash_text(stable_json(value))


def hash_file(path: Path) -> str:
    return 'sha256:' + hashlib.sha256(path.read_bytes()).hexdigest()


def compute_dataset_hash(paths: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(Path(p) for p in paths):
        digest.update(str(path.as_posix()).encode('utf-8'))
        digest.update(b'\0')
        digest.update(path.read_bytes())
        digest.update(b'\0')
    return 'sha256:' + digest.hexdigest()


_FORBIDDEN_SQL_WORDS = {
    'insert', 'update', 'delete', 'create', 'alter', 'drop', 'truncate', 'optimize',
    'system', 'attach', 'detach', 'rename', 'grant', 'revoke', 'kill', 'set',
}


_STRING_LITERAL_RE = re.compile(r"'(?:''|[^'])*'|\"(?:\\\"|[^\"])*\"")
_NUMBER_LITERAL_RE = re.compile(r'(?<![A-Za-z_])[-+]?\d+(?:\.\d+)?(?![A-Za-z_])')
_LINE_COMMENT_RE = re.compile(r'--.*?(?=\n|$)')
_BLOCK_COMMENT_RE = re.compile(r'/\*.*?\*/', re.DOTALL)
_WHITESPACE_RE = re.compile(r'\s+')


def normalize_sql(query: str, preview_chars: int = 140) -> dict[str, Any]:
    """Return a privacy-preserving SQL fingerprint.

    Raw SQL may contain customer names, vehicle IDs, DTCs, or operational details.
    This function masks literals before storing a short preview and hash.
    """
    raw = str(query or '')
    no_comments = _BLOCK_COMMENT_RE.sub(' ', _LINE_COMMENT_RE.sub(' ', raw))
    masked = _STRING_LITERAL_RE.sub('?', no_comments)
    masked = _NUMBER_LITERAL_RE.sub('?', masked)
    normalized = _WHITESPACE_RE.sub(' ', masked).strip().rstrip(';').lower()
    preview = normalized[:preview_chars]
    words = {w.lower() for w in re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', normalized)}
    forbidden = sorted(words & _FORBIDDEN_SQL_WORDS)
    return {
        'sql_hash': hash_text(normalized),
        'sql_preview': preview,
        'normalized_sql': normalized,
        'forbidden_words': forbidden,
        'is_read_only_shape': normalized.startswith(('select ', 'with ')) and not forbidden,
        'has_select_star': bool(re.search(r'\bselect\s+\*\b', normalized)),
    }


def flatten_for_csv(value: Any) -> Any:
    if value is None:
        return ''
    if isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, ensure_ascii=False, default=json_default, sort_keys=True)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    clean = sanitize_for_json(value)
    path.write_text(
        json.dumps(clean, ensure_ascii=False, indent=2, default=json_default, allow_nan=False),
        encoding='utf-8',
    )


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8', newline='') as f:
        f.write(json.dumps(record, ensure_ascii=False, default=json_default, sort_keys=True) + '\n')


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        keys: list[str] = []
        seen = set()
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    keys.append(key)
        fieldnames = keys
    with path.open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow({k: flatten_for_csv(row.get(k)) for k in fieldnames})


class LocalEvaluationStore:
    """Local-only writer for conversational evaluation artifacts.

    This class intentionally has no ClickHouse dependency and must not create or
    modify any database tables. It writes append-friendly artifacts under a run
    directory for replay and benchmark comparison.
    """

    def __init__(self, output_dir: str | Path = DEFAULT_OUTPUT_DIR, run_id: str | None = None):
        self.output_dir = Path(output_dir)
        self.run_id = run_id or self._new_run_id()
        self.run_dir = self.output_dir / self.run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        (self.run_dir / 'replay').mkdir(parents=True, exist_ok=True)
        (self.run_dir / 'optional_xlsx').mkdir(parents=True, exist_ok=True)

        self.turn_rows: list[dict[str, Any]] = []
        self.sql_rows: list[dict[str, Any]] = []
        self.scenario_rows: list[dict[str, Any]] = []
        self.token_rows: list[dict[str, Any]] = []
        self.validation_claim_rows: list[dict[str, Any]] = []
        self.validation_evidence_rows: list[dict[str, Any]] = []
        self.validation_map_rows: list[dict[str, Any]] = []
        self.validation_finding_rows: list[dict[str, Any]] = []
        self.validation_score_rows: list[dict[str, Any]] = []
        self.human_review_rows: list[dict[str, Any]] = []

    @staticmethod
    def _new_run_id() -> str:
        stamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        suffix = hashlib.sha1(stamp.encode('utf-8')).hexdigest()[:6]
        return f'eval_{stamp}_{suffix}'

    def path(self, name: str) -> Path:
        return self.run_dir / name

    def write_manifest(self, manifest: dict[str, Any]) -> None:
        write_json(self.path('run_manifest.json'), manifest)
        write_json(self.path('replay') / 'manifest.json', manifest)

    def write_scenario_snapshot(self, suite: dict[str, Any]) -> None:
        write_json(self.path('scenario_catalog_snapshot.json'), suite)

    def append_turn(self, record: dict[str, Any]) -> None:
        self.turn_rows.append(record)
        append_jsonl(self.path('turns.jsonl'), record)

    def append_sql_event(self, record: dict[str, Any]) -> None:
        self.sql_rows.append(record)
        append_jsonl(self.path('sql_events.jsonl'), record)

    def append_scenario_rollup(self, record: dict[str, Any]) -> None:
        self.scenario_rows.append(record)
        append_jsonl(self.path('scenarios_rollup.jsonl'), record)

    def append_token_row(self, record: dict[str, Any]) -> None:
        self.token_rows.append(record)

    def append_validation_claim(self, record: dict[str, Any]) -> None:
        self.validation_claim_rows.append(record)
        append_jsonl(self.path('validation') / 'claims.jsonl', record)

    def append_validation_evidence(self, record: dict[str, Any]) -> None:
        self.validation_evidence_rows.append(record)
        append_jsonl(self.path('validation') / 'evidence_items.jsonl', record)

    def append_claim_evidence_map(self, record: dict[str, Any]) -> None:
        self.validation_map_rows.append(record)
        append_jsonl(self.path('validation') / 'claim_evidence_map.jsonl', record)

    def append_validation_finding(self, record: dict[str, Any]) -> None:
        self.validation_finding_rows.append(record)
        append_jsonl(self.path('validation') / 'validation_findings.jsonl', record)

    def append_validation_turn_score(self, record: dict[str, Any]) -> None:
        self.validation_score_rows.append(record)
        append_jsonl(self.path('validation') / 'validation_turn_scores.jsonl', record)

    def append_human_review_item(self, record: dict[str, Any]) -> None:
        self.human_review_rows.append(record)
        append_jsonl(self.path('validation') / 'human_review_queue.jsonl', record)

    def write_summary(self, summary: dict[str, Any]) -> None:
        write_json(self.path('summary.json'), summary)

    def write_benchmark_delta(self, delta: dict[str, Any] | None) -> None:
        if delta:
            write_json(self.path('benchmark_delta.json'), delta)
            rows = delta.get('scenario_deltas') if isinstance(delta.get('scenario_deltas'), list) else []
            if rows:
                write_csv(self.path('benchmark_delta.csv'), rows)

    def write_latest_pointer(self) -> None:
        write_json(self.output_dir / 'latest.json', {'run_id': self.run_id, 'run_dir': str(self.run_dir), 'updated_at': utc_now_iso()})

    def finalize_tables(self) -> None:
        metrics_rows = []
        for row in self.turn_rows:
            flat = {
                'run_id': row.get('run_id'),
                'scenario_id': row.get('scenario_id'),
                'category': row.get('category'),
                'difficulty_tier': row.get('difficulty_tier'),
                'turn_index': row.get('turn_index'),
                'request_id': row.get('request_id'),
                'elapsed_ms': row.get('elapsed_ms'),
                'expected_intent': row.get('expected_intent'),
                'actual_intent': row.get('actual_intent'),
                'status_label': row.get('status_label'),
                'gate_passed': row.get('gate_passed'),
                'violated_gates': row.get('violated_gates'),
            }
            for key, value in (row.get('dimensions') or {}).items():
                flat[f'dimension_{key}'] = value
            for key, value in (row.get('memory') or {}).items():
                flat[f'memory_{key}'] = value
            for key, value in (row.get('efficiency') or {}).items():
                flat[f'efficiency_{key}'] = value
            metrics_rows.append(flat)
        write_csv(self.path('metrics.csv'), metrics_rows)
        write_csv(self.path('tokens.csv'), self.token_rows)

        if self.sql_rows:
            write_csv(self.path('sql_events.csv'), self.sql_rows)
        if self.scenario_rows:
            write_csv(self.path('scenarios_rollup.csv'), self.scenario_rows)

        validation_dir = self.path('validation')
        if self.validation_claim_rows:
            write_csv(validation_dir / 'claims.csv', self.validation_claim_rows)
        if self.validation_evidence_rows:
            write_csv(validation_dir / 'evidence_items.csv', self.validation_evidence_rows)
        if self.validation_map_rows:
            write_csv(validation_dir / 'claim_evidence_map.csv', self.validation_map_rows)
        if self.validation_finding_rows:
            write_csv(validation_dir / 'validation_findings.csv', self.validation_finding_rows)
        if self.validation_score_rows:
            write_csv(validation_dir / 'validation_turn_scores.csv', self.validation_score_rows)
        if self.human_review_rows:
            write_csv(validation_dir / 'human_review_queue.csv', self.human_review_rows)

    def write_validation_summary(self, summary: dict[str, Any]) -> None:
        write_json(self.path('validation') / 'validation_summary.json', summary)

    def write_excel_workbook_if_available(self) -> bool:
        try:
            from openpyxl import Workbook  # type: ignore
        except Exception:
            return False

        wb = Workbook()
        default = wb.active
        default.title = 'summary_note'
        default.append(['artifact', 'path'])
        default.append(['run_dir', str(self.run_dir)])
        self._add_sheet(wb, 'metrics', self.turn_rows[:500])
        self._add_sheet(wb, 'scenarios', self.scenario_rows[:500])
        self._add_sheet(wb, 'tokens', self.token_rows[:1000])
        self._add_sheet(wb, 'sql_events', self.sql_rows[:1000])
        workbook_path = self.path('optional_xlsx') / 'benchmark_summary.xlsx'
        wb.save(workbook_path)
        return True

    @staticmethod
    def _add_sheet(wb: Any, title: str, rows: list[dict[str, Any]]) -> None:
        ws = wb.create_sheet(title=title[:31])
        if not rows:
            ws.append(['empty'])
            return
        keys: list[str] = []
        seen = set()
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    keys.append(key)
        ws.append(keys)
        for row in rows:
            ws.append([flatten_for_csv(row.get(k)) for k in keys])

    def copy_baseline_marker(self, baseline_dir: str | Path | None) -> None:
        if not baseline_dir:
            return
        src = Path(baseline_dir)
        if src.exists():
            target = self.run_dir / 'baselines' / src.name
            target.parent.mkdir(parents=True, exist_ok=True)
            summary = src / 'summary.json'
            manifest = src / 'run_manifest.json'
            for candidate in [summary, manifest]:
                if candidate.exists():
                    shutil.copy2(candidate, target.parent / f'{src.name}_{candidate.name}')
