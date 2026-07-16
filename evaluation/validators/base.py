from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any


@dataclass
class EvidenceItem:
    evidence_id: str
    run_id: str
    scenario_id: str
    turn_index: int
    source_type: str
    source_name: str
    request_id: str = ''
    entity_scope: dict[str, Any] = field(default_factory=dict)
    fields: dict[str, Any] = field(default_factory=dict)
    numeric_values: dict[str, float] = field(default_factory=dict)
    text_values: list[str] = field(default_factory=list)
    source_hash: str = ''
    confidence: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Claim:
    claim_id: str
    run_id: str
    scenario_id: str
    turn_index: int
    claim_type: str
    text: str
    claim_field: str = ''
    value: Any = None
    entities: dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.7
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationFinding:
    failure_type: str
    severity: str
    confidence: float
    message: str
    run_id: str
    scenario_id: str
    turn_index: int
    validator_name: str
    claim_id: str | None = None
    evidence_ids: list[str] = field(default_factory=list)
    human_review_required: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationResult:
    run_id: str
    scenario_id: str
    turn_index: int
    request_id: str
    gates: dict[str, bool] = field(default_factory=dict)
    dimensions: dict[str, float] = field(default_factory=dict)
    findings: list[ValidationFinding] = field(default_factory=list)
    evidence_items: list[EvidenceItem] = field(default_factory=list)
    claims: list[Claim] = field(default_factory=list)
    claim_evidence_map: list[dict[str, Any]] = field(default_factory=list)
    human_review_items: list[dict[str, Any]] = field(default_factory=list)

    def turn_score_record(self) -> dict[str, Any]:
        return {
            'run_id': self.run_id,
            'scenario_id': self.scenario_id,
            'turn_index': self.turn_index,
            'request_id': self.request_id,
            'gates': self.gates,
            'dimensions': self.dimensions,
            'finding_count': len(self.findings),
            'critical_finding_count': sum(1 for f in self.findings if f.severity == 'critical'),
            'high_finding_count': sum(1 for f in self.findings if f.severity == 'high'),
            'status': self.status(),
        }

    def status(self) -> str:
        if any(f.severity == 'critical' for f in self.findings):
            return 'FAIL_CRITICAL'
        if not all(self.gates.values()) if self.gates else False:
            return 'FAIL_GATE'
        if self.findings:
            return 'PASS_WITH_WARNINGS'
        return 'PASS'
