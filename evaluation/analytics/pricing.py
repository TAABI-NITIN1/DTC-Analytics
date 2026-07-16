"""Token cost estimation for evaluation runs."""

from __future__ import annotations

import os
from typing import Any

# USD per 1M tokens (defaults; override via env)
_DEFAULT_MODEL_PRICING: dict[str, tuple[float, float]] = {
    'gpt-4o': (2.50, 10.00),
    'gpt-4o-mini': (0.15, 0.60),
    'gpt-4.1': (2.00, 8.00),
    'gpt-4.1-mini': (0.40, 1.60),
    'gpt-3.5-turbo': (0.50, 1.50),
    'gpt-5': (5.00, 15.00),
    'gpt-5-mini': (0.50, 2.00),
}


def _parse_env_rate(name: str, default: float) -> float:
    raw = os.getenv(name, '')
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


def model_rates(model_name: str) -> tuple[float, float]:
    """Return (input_usd_per_1m, output_usd_per_1m) for a model."""
    model = (model_name or '').strip().lower()
    if os.getenv('EVAL_COST_INPUT_PER_1M') or os.getenv('EVAL_COST_OUTPUT_PER_1M'):
        return (
            _parse_env_rate('EVAL_COST_INPUT_PER_1M', 2.50),
            _parse_env_rate('EVAL_COST_OUTPUT_PER_1M', 10.00),
        )
    for key, rates in _DEFAULT_MODEL_PRICING.items():
        if key in model:
            return rates
    return _DEFAULT_MODEL_PRICING['gpt-4o-mini']


def estimate_cost_usd(
    *,
    prompt_tokens: int,
    completion_tokens: int,
    model_name: str = '',
) -> float:
    inp_rate, out_rate = model_rates(model_name)
    cost = (prompt_tokens / 1_000_000.0) * inp_rate + (completion_tokens / 1_000_000.0) * out_rate
    return round(cost, 6)


def turn_cost_from_row(row: dict[str, Any], model_name: str = '') -> float:
    return estimate_cost_usd(
        prompt_tokens=int(row.get('tokens_prompt') or 0),
        completion_tokens=int(row.get('tokens_completion') or 0),
        model_name=model_name or str(row.get('model_name') or ''),
    )


def version_model_name(version: dict[str, Any] | None) -> str:
    if not isinstance(version, dict):
        return os.getenv('EVAL_DEFAULT_MODEL', 'gpt-4o-mini')
    return str(version.get('model_name') or os.getenv('EVAL_DEFAULT_MODEL', 'gpt-4o-mini'))
