"""Canonical metric contracts for evaluation analytics layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Composite AI health score weights (must sum to 1.0)
HEALTH_WEIGHT_PASS_RATE = 0.30
HEALTH_WEIGHT_GROUNDEDNESS = 0.25
HEALTH_WEIGHT_RELIABILITY = 0.20
HEALTH_WEIGHT_LOW_HALLUCINATION = 0.15
HEALTH_WEIGHT_SAFETY = 0.10


@dataclass
class RunMetrics:
    run_id: str = ''
    # Volume
    total_sessions: int = 0
    total_turns: int = 0
    total_tool_calls: int = 0
    total_sql_queries: int = 0
    total_tokens: int = 0
    total_cost_usd: float = 0.0
    total_failures: int = 0
    total_passed: int = 0
    total_failed: int = 0
    # Speed (seconds)
    avg_latency_sec: float = 0.0
    p50_latency_sec: float = 0.0
    p95_latency_sec: float = 0.0
    p99_latency_sec: float = 0.0
    avg_tool_latency_sec: float = 0.0
    avg_sql_latency_sec: float = 0.0
    avg_graph_execution_time_sec: float = 0.0
    # Cost
    avg_cost_per_session_usd: float = 0.0
    avg_cost_per_turn_usd: float = 0.0
    avg_tokens_per_session: float = 0.0
    avg_tokens_per_turn: float = 0.0
    # Quality
    avg_groundedness_score: float = 0.0
    avg_correctness_score: float = 0.0
    avg_trace_judge_final_score: float = 0.0
    avg_batch_judge_correctness: float = 0.0
    hallucination_rate: float = 0.0
    contradiction_rate: float = 0.0
    unsupported_claim_rate: float = 0.0
    gate_pass_rate: float = 0.0
    # Reliability
    graph_failure_rate: float = 0.0
    timeout_rate: float = 0.0
    retry_rate: float = 0.0
    # Safety
    cross_customer_leakage_count: int = 0
    unsafe_sql_attempts: int = 0
    unsafe_recommendations: int = 0
    safety_violations: int = 0
    # Composite
    ai_health_score: float = 0.0
    pass_rate: float = 0.0
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = {k: v for k, v in self.__dict__.items() if k != 'extra'}
        d.update(self.extra)
        return d


@dataclass
class SessionMetrics:
    session_id: str = ''
    scenario_category: str = ''
    difficulty_tier: str = ''
    session_type: str = ''
    session_pass_fail: str = 'unknown'
    session_score: float = 0.0
    total_session_tokens: int = 0
    total_session_cost_usd: float = 0.0
    total_session_latency_sec: float = 0.0
    hallucination_count: int = 0
    safety_violation_count: int = 0
    gate_passed: bool = True
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = {k: v for k, v in self.__dict__.items() if k != 'extra'}
        d.update(self.extra)
        return d


FAILURE_CATEGORIES = {
    'hallucination': 'hallucination.',
    'memory': 'memory.',
    'reasoning': 'reasoning.',
    'safety': 'safety.',
    'efficiency': 'efficiency.',
    'tool': 'tool.',
    'sql': 'sql.',
    'evidence': 'evidence.',
    'runtime': 'runtime.',
}
