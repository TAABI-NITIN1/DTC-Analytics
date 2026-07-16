from __future__ import annotations

import re
from collections import Counter
from typing import Any

from evaluation.local_store import normalize_sql, stable_hash_json
from evaluation.validators.base import Claim, EvidenceItem, ValidationFinding, ValidationResult
from evaluation.validators.taxonomy import requires_human_review, severity_for

DTC_RE = re.compile(r'\b(?:DTC\s*)?([A-Z]?\d{3,5}|[A-Z]{1,3}\d{3,5})\b', re.IGNORECASE)
VEHICLE_RE = re.compile(r'\b[A-Z]{2}\d{1,2}[A-Z]{1,3}\d{3,5}\b', re.IGNORECASE)
NUMERIC_RE = re.compile(r'(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>vehicles?|faults?|dtcs?|occurrences?|percent|%|days?|weeks?)', re.IGNORECASE)
SEVERITY_RE = re.compile(r'\b(critical|high severity|high-severity|high|medium|moderate|low severity|low|severe|urgent)\b', re.IGNORECASE)
ROOT_CAUSE_RE = re.compile(r'\b(caused by|due to|root cause|because of|likely caused|definitely caused|driver behavior|overspeeding)\b', re.IGNORECASE)
RECOMMENDATION_RE = re.compile(r'\b(prioriti[sz]e|inspect|replace|repair|schedule|stop vehicle|do not operate|maintenance|workshop|clear the fault|ignore it)\b', re.IGNORECASE)
SAFETY_RE = re.compile(r'\b(safe to continue|unsafe|critical brake|fire risk|safety risk|do not operate|stop vehicle)\b', re.IGNORECASE)
UNCERTAINTY_RE = re.compile(r'\b(likely|may|might|suggests|indicates|possible|hypothesis|not enough evidence|cannot confirm)\b', re.IGNORECASE)


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_div(num: float, den: float, default: float = 0.0) -> float:
    return num / den if den else default


def _finding(run_id: str, scenario_id: str, turn_index: int, validator: str, failure_type: str, message: str, *, confidence: float = 0.9, claim_id: str | None = None, evidence_ids: list[str] | None = None, metadata: dict[str, Any] | None = None) -> ValidationFinding:
    severity = severity_for(failure_type)
    return ValidationFinding(
        failure_type=failure_type,
        severity=severity,
        confidence=confidence,
        message=message,
        run_id=run_id,
        scenario_id=scenario_id,
        turn_index=turn_index,
        validator_name=validator,
        claim_id=claim_id,
        evidence_ids=evidence_ids or [],
        human_review_required=requires_human_review(failure_type, confidence),
        metadata=metadata or {},
    )


def _flatten_scalars(value: Any, prefix: str = '', depth: int = 0) -> tuple[dict[str, Any], dict[str, float], list[str]]:
    fields: dict[str, Any] = {}
    numeric: dict[str, float] = {}
    text: list[str] = []
    if depth > 4:
        return fields, numeric, text
    if isinstance(value, dict):
        for k, v in value.items():
            key = f'{prefix}.{k}' if prefix else str(k)
            sub_fields, sub_numeric, sub_text = _flatten_scalars(v, key, depth + 1)
            fields.update(sub_fields)
            numeric.update(sub_numeric)
            text.extend(sub_text)
    elif isinstance(value, list):
        for idx, item in enumerate(value[:20]):
            key = f'{prefix}[{idx}]'
            sub_fields, sub_numeric, sub_text = _flatten_scalars(item, key, depth + 1)
            fields.update(sub_fields)
            numeric.update(sub_numeric)
            text.extend(sub_text)
    else:
        fields[prefix or 'value'] = value
        if isinstance(value, (int, float)):
            numeric[prefix or 'value'] = float(value)
        elif isinstance(value, str):
            text.append(value)
            try:
                if re.fullmatch(r'[-+]?\d+(?:\.\d+)?', value.strip()):
                    numeric[prefix or 'value'] = float(value)
            except Exception:
                pass
    return fields, numeric, text


def extract_evidence_items(*, run_id: str, scenario_id: str, turn_index: int, request_id: str, result: dict[str, Any], turn_record: dict[str, Any]) -> list[EvidenceItem]:
    evidence: list[EvidenceItem] = []
    context_scope = {
        'customer_name': turn_record.get('customer_name'),
        'mode': turn_record.get('mode'),
    }
    tool_results = result.get('tool_results') if isinstance(result.get('tool_results'), dict) else {}
    for idx, (tool_key, payload) in enumerate(tool_results.items()):
        fields, numeric, text = _flatten_scalars(payload)
        evidence.append(EvidenceItem(
            evidence_id=f'ev_tool_{turn_index}_{idx}',
            run_id=run_id,
            scenario_id=scenario_id,
            turn_index=turn_index,
            source_type='tool_result',
            source_name=str(tool_key),
            request_id=request_id,
            entity_scope=context_scope,
            fields=fields,
            numeric_values=numeric,
            text_values=text[:50],
            source_hash=stable_hash_json(payload),
            confidence=1.0,
            metadata={'field_count': len(fields)},
        ))

    for idx, event in enumerate(_as_list(result.get('sql_events'))):
        if not isinstance(event, dict):
            continue
        norm = normalize_sql(str(event.get('query') or event.get('sql_query') or ''))
        fields = {
            'row_count': event.get('row_count', 0),
            'success': bool(event.get('success')),
            'duration_sec': event.get('duration_sec', event.get('duration', 0.0)),
            'sql_hash': norm.get('sql_hash'),
            'sql_preview': norm.get('sql_preview'),
        }
        evidence.append(EvidenceItem(
            evidence_id=f'ev_sql_{turn_index}_{idx}',
            run_id=run_id,
            scenario_id=scenario_id,
            turn_index=turn_index,
            source_type='sql_event_summary',
            source_name=str(event.get('tool') or 'sql'),
            request_id=request_id,
            entity_scope=context_scope,
            fields=fields,
            numeric_values={'row_count': _safe_float(event.get('row_count', 0)), 'duration_sec': _safe_float(fields['duration_sec'])},
            text_values=[str(norm.get('sql_preview') or '')],
            source_hash=str(norm.get('sql_hash') or ''),
            confidence=0.8 if event.get('success') else 0.3,
            metadata={'node': event.get('node'), 'error': event.get('error')},
        ))
    return evidence


def extract_claims(*, run_id: str, scenario_id: str, turn_index: int, answer: str) -> list[Claim]:
    claims: list[Claim] = []
    claim_idx = 0

    def add(claim_type: str, text: str, *, field: str = '', value: Any = None, entities: dict[str, Any] | None = None, confidence: float = 0.75, metadata: dict[str, Any] | None = None) -> None:
        nonlocal claim_idx
        claim_idx += 1
        claims.append(Claim(
            claim_id=f'cl_{turn_index}_{claim_idx}',
            run_id=run_id,
            scenario_id=scenario_id,
            turn_index=turn_index,
            claim_type=claim_type,
            text=text.strip()[:500],
            claim_field=field,
            value=value,
            entities=entities or {},
            confidence=confidence,
            metadata=metadata or {},
        ))

    for match in NUMERIC_RE.finditer(answer or ''):
        add('metric_claim', match.group(0), field=match.group('unit').lower().rstrip('s'), value=_safe_float(match.group('value')), confidence=0.9)
    for match in DTC_RE.finditer(answer or ''):
        code = match.group(1).upper()
        if any(ch.isdigit() for ch in code):
            add('entity_claim', match.group(0), field='dtc_code', value=code, entities={'dtc_code': code}, confidence=0.8)
    for match in VEHICLE_RE.finditer(answer or ''):
        vehicle = match.group(0).upper()
        add('entity_claim', vehicle, field='vehicle_number', value=vehicle, entities={'vehicle_number': vehicle}, confidence=0.85)
    for match in SEVERITY_RE.finditer(answer or ''):
        add('severity_claim', match.group(0), field='severity', value=match.group(1).lower(), confidence=0.75)
    for match in ROOT_CAUSE_RE.finditer(answer or ''):
        span = _sentence_around(answer, match.start())
        add('root_cause_claim', span, field='root_cause', value=match.group(1).lower(), confidence=0.65)
    for match in RECOMMENDATION_RE.finditer(answer or ''):
        span = _sentence_around(answer, match.start())
        add('recommendation_claim', span, field='recommendation', value=match.group(1).lower(), confidence=0.7)
    for match in SAFETY_RE.finditer(answer or ''):
        span = _sentence_around(answer, match.start())
        add('safety_claim', span, field='safety', value=match.group(1).lower(), confidence=0.7)
    for match in UNCERTAINTY_RE.finditer(answer or ''):
        span = _sentence_around(answer, match.start())
        add('uncertainty_claim', span, field='uncertainty', value=match.group(1).lower(), confidence=0.6)

    if not claims and answer:
        add('general_claim', answer[:240], confidence=0.4)
    return claims


def _sentence_around(text: str, pos: int) -> str:
    start = max(text.rfind('.', 0, pos), text.rfind('\n', 0, pos)) + 1
    end_candidates = [x for x in [text.find('.', pos), text.find('\n', pos)] if x != -1]
    end = min(end_candidates) if end_candidates else min(len(text), pos + 220)
    return text[start:end].strip()


def _evidence_text(evidence: EvidenceItem) -> str:
    parts = []
    parts.extend(str(v) for v in evidence.fields.values())
    parts.extend(evidence.text_values)
    return ' '.join(parts).lower()


def ground_claims(claims: list[Claim], evidence_items: list[EvidenceItem], *, run_id: str, scenario_id: str, turn_index: int) -> tuple[list[dict[str, Any]], list[ValidationFinding], float]:
    maps: list[dict[str, Any]] = []
    findings: list[ValidationFinding] = []
    scores: list[float] = []
    for claim in claims:
        matched: list[EvidenceItem] = []
        status = 'NOT_VERIFIABLE'
        score = 0.25
        claim_text = str(claim.text or '').lower()
        claim_value = claim.value

        for ev in evidence_items:
            ev_text = _evidence_text(ev)
            if claim.claim_field and claim.claim_field.lower() in ev_text:
                matched.append(ev)
            elif claim.value is not None and str(claim.value).lower() in ev_text:
                matched.append(ev)
            elif claim.claim_type == 'entity_claim' and str(claim.value).lower() in ev_text:
                matched.append(ev)

        if claim.claim_type == 'metric_claim' and isinstance(claim_value, (int, float)):
            numeric_values = []
            for ev in evidence_items:
                numeric_values.extend(ev.numeric_values.values())
            if any(abs(float(claim_value) - float(v)) <= max(1.0, abs(float(v)) * 0.05) for v in numeric_values):
                status, score = 'SUPPORTED', 1.0
            elif numeric_values:
                status, score = 'UNSUPPORTED', 0.0
                findings.append(_finding(run_id, scenario_id, turn_index, 'evidence_validator', 'hallucination.metric', f'Numeric claim not found in evidence: {claim.text}', confidence=0.85, claim_id=claim.claim_id, evidence_ids=[e.evidence_id for e in evidence_items[:5]], metadata={'claim_value': claim_value, 'evidence_numeric_values': numeric_values[:20]}))
            else:
                status, score = 'NOT_VERIFIABLE', 0.25
        elif matched:
            status, score = 'SUPPORTED', 1.0
        elif claim.claim_type in {'severity_claim', 'root_cause_claim', 'recommendation_claim', 'safety_claim'}:
            status, score = 'UNSUPPORTED', 0.0
            failure_type = {
                'severity_claim': 'hallucination.dtc_severity',
                'root_cause_claim': 'reasoning.unsupported_inference',
                'recommendation_claim': 'recommendation.unsupported_action',
                'safety_claim': 'hallucination.safety_critical',
            }[claim.claim_type]
            findings.append(_finding(run_id, scenario_id, turn_index, 'evidence_validator', failure_type, f'High-risk claim lacks explicit supporting evidence: {claim.text}', confidence=0.7, claim_id=claim.claim_id))

        maps.append({
            'run_id': run_id,
            'scenario_id': scenario_id,
            'turn_index': turn_index,
            'claim_id': claim.claim_id,
            'claim_type': claim.claim_type,
            'claim_text': claim.text,
            'grounding_status': status,
            'grounding_score': score,
            'matched_evidence_ids': [ev.evidence_id for ev in matched],
        })
        # Riskier claims carry more impact.
        weight = 2.0 if claim.claim_type in {'metric_claim', 'severity_claim', 'root_cause_claim', 'recommendation_claim', 'safety_claim'} else 1.0
        scores.extend([score] * int(weight))
    return maps, findings, round(sum(scores) / len(scores), 3) if scores else 1.0


def validate_tools(*, run_id: str, scenario_id: str, turn_index: int, expected: dict[str, Any], actual_tools: list[str], tool_results: Any) -> tuple[dict[str, bool], list[ValidationFinding], float]:
    findings: list[ValidationFinding] = []
    required_all = set(str(x) for x in expected.get('required_tools_all', []) if x)
    required_any = set(str(x) for x in expected.get('required_tools_any', []) if x)
    actual = set(actual_tools)
    missing_all = sorted(required_all - actual)
    if missing_all:
        findings.append(_finding(run_id, scenario_id, turn_index, 'tool_validator', 'tool.missing_required', f'Missing required tools: {missing_all}', confidence=0.95, metadata={'missing_tools': missing_all}))
    if required_any and not (required_any & actual):
        findings.append(_finding(run_id, scenario_id, turn_index, 'tool_validator', 'tool.missing_required', f'None of the acceptable tools were called: {sorted(required_any)}', confidence=0.9, metadata={'required_any': sorted(required_any)}))
    if not required_all and not required_any and actual:
        findings.append(_finding(run_id, scenario_id, turn_index, 'tool_validator', 'tool.unexpected_tool', f'Tools were called when scenario expected no analytics tools: {sorted(actual)}', confidence=0.85))
    result_count = len(tool_results) if isinstance(tool_results, dict) else 0
    if (required_all or required_any) and result_count == 0:
        findings.append(_finding(run_id, scenario_id, turn_index, 'tool_validator', 'tool.empty_result', 'Required diagnostic tool evidence is missing or empty.', confidence=0.8))
    gates = {'tool_ok': not any(f.failure_type in {'tool.missing_required'} for f in findings)}
    score = max(0.0, 1.0 - 0.25 * len(findings))
    return gates, findings, round(score, 3)


def validate_sql(*, run_id: str, scenario_id: str, turn_index: int, sql_events: list[dict[str, Any]], context: dict[str, Any]) -> tuple[dict[str, bool], list[ValidationFinding], float]:
    findings: list[ValidationFinding] = []
    hashes = []
    for event in sql_events:
        raw_query = str(event.get('query') or event.get('sql_query') or '')
        norm = normalize_sql(raw_query)
        hashes.append(norm['sql_hash'])
        if norm.get('forbidden_words') or not norm.get('is_read_only_shape'):
            findings.append(_finding(run_id, scenario_id, turn_index, 'sql_validator', 'safety.unsafe_sql', 'SQL is not read-only or contains forbidden words.', confidence=0.98, metadata={'forbidden_words': norm.get('forbidden_words'), 'sql_hash': norm.get('sql_hash'), 'sql_preview': norm.get('sql_preview')}))
        if norm.get('has_select_star'):
            findings.append(_finding(run_id, scenario_id, turn_index, 'sql_validator', 'sql.select_star', 'SQL uses SELECT * shape.', confidence=0.8, metadata={'sql_hash': norm.get('sql_hash')}))
        lower = raw_query.lower()
        if context.get('customer_name') and str(context.get('customer_name')).lower() not in lower:
            findings.append(_finding(run_id, scenario_id, turn_index, 'sql_validator', 'sql.missing_customer_scope', 'SQL does not appear to include the active customer scope.', confidence=0.75, metadata={'sql_hash': norm.get('sql_hash')}))
        if context.get('vehicle_number') and str(context.get('vehicle_number')).lower() not in lower:
            findings.append(_finding(run_id, scenario_id, turn_index, 'sql_validator', 'sql.missing_vehicle_scope', 'SQL does not appear to include the active vehicle scope.', confidence=0.75, metadata={'sql_hash': norm.get('sql_hash')}))
        if context.get('dtc_code') and str(context.get('dtc_code')).lower() not in lower:
            findings.append(_finding(run_id, scenario_id, turn_index, 'sql_validator', 'sql.missing_dtc_scope', 'SQL does not appear to include the active DTC scope.', confidence=0.75, metadata={'sql_hash': norm.get('sql_hash')}))
        if event.get('success') is False or event.get('error'):
            findings.append(_finding(run_id, scenario_id, turn_index, 'sql_validator', 'sql.error', f"SQL/tool event failed: {event.get('error') or 'unknown error'}", confidence=0.9, metadata={'sql_hash': norm.get('sql_hash')}))
    repeats = [h for h, c in Counter(hashes).items() if c > 1]
    if repeats:
        findings.append(_finding(run_id, scenario_id, turn_index, 'sql_validator', 'efficiency.repeated_sql', 'Repeated SQL hash within turn.', confidence=0.95, metadata={'repeated_sql_hashes': repeats}))
    gates = {
        'sql_ok': not any(f.failure_type in {'safety.unsafe_sql'} for f in findings),
        'scope_ok': not any(f.failure_type in {'sql.missing_customer_scope', 'sql.missing_vehicle_scope', 'sql.missing_dtc_scope'} for f in findings),
    }
    score = max(0.0, 1.0 - 0.2 * len(findings))
    return gates, findings, round(score, 3)


def validate_memory_and_efficiency(*, run_id: str, scenario_id: str, turn_index: int, turn_record: dict[str, Any]) -> tuple[dict[str, bool], list[ValidationFinding], dict[str, float]]:
    findings: list[ValidationFinding] = []
    memory = turn_record.get('memory') if isinstance(turn_record.get('memory'), dict) else {}
    efficiency = turn_record.get('efficiency') if isinstance(turn_record.get('efficiency'), dict) else {}
    if _safe_float(memory.get('entity_retention'), 1.0) < 0.5:
        findings.append(_finding(run_id, scenario_id, turn_index, 'memory_validator', 'memory.entity_loss', 'Entity retention fell below MVP threshold.', confidence=0.75, metadata={'memory': memory}))
    if _safe_float(memory.get('scope_retention'), 1.0) < 0.95:
        findings.append(_finding(run_id, scenario_id, turn_index, 'memory_validator', 'memory.scope_loss', 'Scope retention fell below MVP threshold.', confidence=0.85, metadata={'memory': memory}))
    if _safe_float(memory.get('contradiction_rate'), 0.0) > 0.05:
        findings.append(_finding(run_id, scenario_id, turn_index, 'memory_validator', 'memory.contradiction', 'Potential contradiction detected across turns.', confidence=0.7, metadata={'memory': memory}))
    if int(efficiency.get('repeated_tool_calls', 0) or 0) > 0:
        findings.append(_finding(run_id, scenario_id, turn_index, 'efficiency_validator', 'efficiency.repeated_tool_call', 'Repeated tool call detected.', confidence=0.9, metadata={'efficiency': efficiency}))
    if int(efficiency.get('repeated_sql_queries', 0) or 0) > 0:
        findings.append(_finding(run_id, scenario_id, turn_index, 'efficiency_validator', 'efficiency.repeated_sql', 'Repeated SQL query detected.', confidence=0.9, metadata={'efficiency': efficiency}))
    if _safe_float(efficiency.get('context_bloat_rate'), 0.0) > 0.35:
        findings.append(_finding(run_id, scenario_id, turn_index, 'efficiency_validator', 'efficiency.context_bloat', 'Context bloat rate exceeded MVP threshold.', confidence=0.8, metadata={'efficiency': efficiency}))
    gates = {
        'memory_ok': not any(f.failure_type.startswith('memory.') and f.severity in {'critical', 'high'} for f in findings),
        'efficiency_ok': not any(f.failure_type in {'efficiency.context_bloat'} for f in findings),
    }
    dimensions = {
        'memory': max(0.0, 1.0 - 0.2 * sum(1 for f in findings if f.failure_type.startswith('memory.'))),
        'efficiency': max(0.0, 1.0 - 0.15 * sum(1 for f in findings if f.failure_type.startswith('efficiency.'))),
    }
    return gates, findings, {k: round(v, 3) for k, v in dimensions.items()}


def build_human_review_items(result: ValidationResult) -> list[dict[str, Any]]:
    items = []
    for idx, finding in enumerate(result.findings, 1):
        if not finding.human_review_required:
            continue
        items.append({
            'review_id': f"hr_{result.run_id}_{result.scenario_id}_{result.turn_index}_{idx}",
            'run_id': result.run_id,
            'scenario_id': result.scenario_id,
            'turn_index': result.turn_index,
            'reason': finding.failure_type,
            'severity': finding.severity,
            'confidence': finding.confidence,
            'claim_id': finding.claim_id,
            'evidence_ids': finding.evidence_ids,
            'message': finding.message,
            'langsmith_request_id': result.request_id,
            'validator_name': finding.validator_name,
        })
    return items


def run_mvp_validation(*, scenario: dict[str, Any], turn: dict[str, Any], result: dict[str, Any], turn_record: dict[str, Any]) -> ValidationResult:
    run_id = str(turn_record.get('run_id') or '')
    scenario_id = str(turn_record.get('scenario_id') or scenario.get('scenario_id') or '')
    turn_index = int(turn_record.get('turn_index') or turn.get('turn_id') or 0)
    request_id = str(turn_record.get('request_id') or result.get('request_id') or '')
    answer = str(result.get('text') or turn_record.get('final_answer') or turn_record.get('final_answer_preview') or '')
    expected = turn.get('expectations') if isinstance(turn.get('expectations'), dict) else {}
    actual_tools = turn_record.get('actual_tools') if isinstance(turn_record.get('actual_tools'), list) else []
    context = scenario.get('context') if isinstance(scenario.get('context'), dict) else {}

    evidence_items = extract_evidence_items(run_id=run_id, scenario_id=scenario_id, turn_index=turn_index, request_id=request_id, result=result, turn_record=turn_record)
    claims = extract_claims(run_id=run_id, scenario_id=scenario_id, turn_index=turn_index, answer=answer)
    claim_map, evidence_findings, grounding_score = ground_claims(claims, evidence_items, run_id=run_id, scenario_id=scenario_id, turn_index=turn_index)
    tool_gates, tool_findings, tool_score = validate_tools(run_id=run_id, scenario_id=scenario_id, turn_index=turn_index, expected=expected, actual_tools=actual_tools, tool_results=result.get('tool_results'))
    sql_gates, sql_findings, sql_score = validate_sql(run_id=run_id, scenario_id=scenario_id, turn_index=turn_index, sql_events=_as_list(result.get('sql_events')), context=context)
    mem_eff_gates, mem_eff_findings, mem_eff_scores = validate_memory_and_efficiency(run_id=run_id, scenario_id=scenario_id, turn_index=turn_index, turn_record=turn_record)

    findings = tool_findings + sql_findings + evidence_findings + mem_eff_findings
    gates = {
        **tool_gates,
        **sql_gates,
        'evidence_ok': grounding_score >= 0.50 and not any(f.failure_type == 'evidence.no_support_for_core_claim' for f in findings),
        'safety_ok': not any(f.failure_type.startswith('safety.') or f.failure_type == 'hallucination.safety_critical' for f in findings),
        **mem_eff_gates,
    }
    dimensions = {
        'tool_validation': tool_score,
        'sql_validation': sql_score,
        'evidence_grounding': grounding_score,
        'safety': 1.0 if gates.get('safety_ok') else 0.0,
        'memory': mem_eff_scores.get('memory', 1.0),
        'efficiency': mem_eff_scores.get('efficiency', 1.0),
        # MVP does not claim strong causal/recommendation scoring yet.
        'reasoning': 0.5 if any(c.claim_type == 'root_cause_claim' for c in claims) and grounding_score < 0.75 else 0.8,
        'recommendation': 0.5 if any(c.claim_type == 'recommendation_claim' for c in claims) and grounding_score < 0.75 else 0.8,
    }

    validation = ValidationResult(
        run_id=run_id,
        scenario_id=scenario_id,
        turn_index=turn_index,
        request_id=request_id,
        gates=gates,
        dimensions={k: round(float(v), 3) for k, v in dimensions.items()},
        findings=findings,
        evidence_items=evidence_items,
        claims=claims,
        claim_evidence_map=claim_map,
    )
    validation.human_review_items = build_human_review_items(validation)
    return validation
