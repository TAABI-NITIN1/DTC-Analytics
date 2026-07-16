"""Aggregate validation findings into failure taxonomy rollups."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from evaluation.analytics.schema import FAILURE_CATEGORIES
from evaluation.validators.taxonomy import CRITICAL_FAILURES


def _category_for(failure_type: str) -> str:
    ft = str(failure_type or '')
    for cat, prefix in FAILURE_CATEGORIES.items():
        if ft.startswith(prefix):
            return cat
    return 'other'


def _turn_key(finding: dict[str, Any]) -> tuple[str, int]:
    return (
        str(finding.get('scenario_id') or finding.get('session_id') or ''),
        int(finding.get('turn_index') or 0),
    )


def _session_id(finding: dict[str, Any]) -> str:
    return str(finding.get('scenario_id') or finding.get('session_id') or '')


def aggregate_failures(
    findings: list[dict[str, Any]],
    *,
    total_turns: int | None = None,
    total_sessions: int | None = None,
) -> dict[str, Any]:
    """Roll up findings into claim-level counts and turn/session-level affected counts."""
    by_type: Counter[str] = Counter()
    by_category_claims: Counter[str] = Counter()
    by_severity: Counter[str] = Counter()
    by_session: dict[str, list[str]] = defaultdict(list)
    critical_sessions: set[str] = set()

    turns_any: set[tuple[str, int]] = set()
    turns_by_category: dict[str, set[tuple[str, int]]] = defaultdict(set)
    turns_hallucination: set[tuple[str, int]] = set()
    turns_safety: set[tuple[str, int]] = set()
    turns_memory: set[tuple[str, int]] = set()
    turns_efficiency: set[tuple[str, int]] = set()
    turns_by_type: dict[str, set[tuple[str, int]]] = defaultdict(set)
    sessions_any: set[str] = set()

    for f in findings:
        ft = str(f.get('failure_type') or '')
        if not ft:
            continue
        by_type[ft] += 1
        cat = _category_for(ft)
        by_category_claims[cat] += 1
        sev = str(f.get('severity') or 'low')
        by_severity[sev] += 1
        sid = _session_id(f)
        tk = _turn_key(f)
        if sid:
            by_session[sid].append(ft)
            sessions_any.add(sid)
        turns_any.add(tk)
        turns_by_category[cat].add(tk)
        turns_by_type[ft].add(tk)
        if ft.startswith('hallucination.'):
            turns_hallucination.add(tk)
        if ft.startswith('safety.'):
            turns_safety.add(tk)
        if ft.startswith('memory.'):
            turns_memory.add(tk)
        if ft.startswith('efficiency.'):
            turns_efficiency.add(tk)
        if ft in CRITICAL_FAILURES or f.get('human_review_required'):
            if sid:
                critical_sessions.add(sid)

    claim_total = sum(by_type.values())
    turns_n = total_turns if total_turns is not None else max(len(turns_any), 1)
    sessions_n = total_sessions if total_sessions is not None else max(len(sessions_any), 1)

    def _rate(n: int) -> float:
        return round(n / max(1, turns_n), 4)

    return {
        # Claim-level (can be >> turns; one answer may trigger many regex flags)
        'claim_level_flag_count': claim_total,
        'total_findings': claim_total,
        'by_failure_type': dict(by_type.most_common()),
        'by_category': dict(by_category_claims.most_common()),
        'by_category_claims': dict(by_category_claims.most_common()),
        'by_severity': dict(by_severity.most_common()),
        'top_failure_types': [t for t, _ in by_type.most_common(20)],
        'hallucination_count': sum(c for t, c in by_type.items() if t.startswith('hallucination.')),
        'safety_count': sum(c for t, c in by_type.items() if t.startswith('safety.')),
        'memory_count': sum(c for t, c in by_type.items() if t.startswith('memory.')),
        'efficiency_count': sum(c for t, c in by_type.items() if t.startswith('efficiency.')),
        # Turn/session-level (preferred for dashboards)
        'turns_with_any_flag': len(turns_any),
        'sessions_with_any_flag': len(sessions_any),
        'turns_with_hallucination_flag': len(turns_hallucination),
        'turns_with_safety_flag': len(turns_safety),
        'turns_with_memory_flag': len(turns_memory),
        'turns_with_efficiency_flag': len(turns_efficiency),
        'turns_with_any_flag_rate': _rate(len(turns_any)),
        'turns_with_hallucination_flag_rate': _rate(len(turns_hallucination)),
        'by_category_turns': {k: len(v) for k, v in sorted(turns_by_category.items(), key=lambda x: -len(x[1]))},
        'by_failure_type_turns': {k: len(v) for k, v in sorted(turns_by_type.items(), key=lambda x: -len(x[1]))},
        'critical_session_count': len(critical_sessions),
        'critical_sessions': sorted(critical_sessions)[:50],
        'metrics_note': (
            'Claim-level counts sum every validator flag (regex hits per answer). '
            'Turns-with-flag counts are unique turns — use these for rates. '
            'Inflated when tool/SQL evidence was not stored at collection time.'
        ),
    }
