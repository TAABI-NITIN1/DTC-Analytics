"""LangSmith-backed evaluation runner for repo question sets.

This script seeds a LangSmith dataset from the local evaluation JSON files when
requested, then runs `langsmith.evaluate` against a simple target callable.

Example:
    python evaluation/run_langsmith_evaluation.py \
        --dataset-name "first 100 questions testing" \
        --seed-from-local \
        --limit 100
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EVAL_DIR = PROJECT_ROOT / 'evaluation'

sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / '.env')
except ImportError:
    pass

from langsmith import Client, evaluate  # noqa: E402
from langsmith.evaluation import EvaluationResult, run_evaluator  # noqa: E402

from src.ai_analyst import chat  # noqa: E402


LOCAL_DATASET_FILES = ['fleet_questions.json', 'vehicle_questions.json', 'dtc_questions.json']


def _load_local_records(limit: int) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for filename in LOCAL_DATASET_FILES:
        path = EVAL_DIR / filename
        if not path.exists():
            continue
        with path.open('r', encoding='utf-8') as handle:
            loaded = json.load(handle)
        if isinstance(loaded, list):
            for item in loaded:
                if isinstance(item, dict) and item.get('question'):
                    records.append(item)
                    if limit and len(records) >= limit:
                        return records
    return records


def _example_inputs(record: dict[str, Any]) -> dict[str, Any]:
    return {
        'question': str(record.get('question', '')),
        'context': record.get('context') or {},
    }


def _example_outputs(record: dict[str, Any]) -> dict[str, Any]:
    return {
        'reference_answer': str(record.get('reference_answer', '')),
    }


def _example_metadata(record: dict[str, Any]) -> dict[str, Any]:
    return {
        'id': record.get('id', ''),
        'expected_intent': record.get('expected_intent', ''),
        'expected_tools': record.get('expected_tools', []),
        'expected_output_contains': record.get('expected_output_contains', []),
    }


def _seed_dataset(client: Client, dataset_name: str, limit: int) -> None:
    if client.has_dataset(dataset_name=dataset_name):
        return

    records = _load_local_records(limit)
    client.create_dataset(
        dataset_name=dataset_name,
        description='Seeded from local evaluation question sets.',
        inputs_schema={
            'type': 'object',
            'properties': {
                'question': {'type': 'string'},
                'context': {'type': 'object'},
            },
            'required': ['question'],
        },
        outputs_schema={
            'type': 'object',
            'properties': {
                'reference_answer': {'type': 'string'},
            },
        },
        metadata={
            'source': 'local-evaluation-json',
            'record_count': len(records),
        },
    )
    if records:
        client.create_examples(
            dataset_name=dataset_name,
            examples=[
                {
                    'inputs': _example_inputs(record),
                    'outputs': _example_outputs(record),
                    'metadata': _example_metadata(record),
                }
                for record in records
            ],
        )


def _normalize_text(value: Any) -> str:
    return ' '.join(str(value or '').split()).strip()


def _extract_prediction(run_outputs: Any) -> str:
    if isinstance(run_outputs, dict):
        if 'output' in run_outputs:
            return str(run_outputs.get('output', ''))
        if 'text' in run_outputs:
            return str(run_outputs.get('text', ''))
    return str(run_outputs or '')


@run_evaluator
def exact_match(run, example):
    prediction = _normalize_text(_extract_prediction(getattr(run, 'outputs', None)))
    reference_outputs = getattr(example, 'outputs', None) or {}
    reference = _normalize_text(reference_outputs.get('reference_answer', ''))
    matched = prediction == reference
    return EvaluationResult(
        key='exact_match',
        score=1.0 if matched else 0.0,
        value=matched,
        comment='Prediction matched the stored reference answer.' if matched else 'Prediction did not match the stored reference answer.',
    )


def _target(inputs: dict[str, Any]) -> dict[str, Any]:
    question = str(inputs.get('question', ''))
    context = inputs.get('context') or {}
    result = chat([{'role': 'user', 'content': question}], context=context)
    return {'output': str(result.get('text', ''))}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Run a LangSmith evaluation over repo question sets.')
    parser.add_argument('--dataset-name', default='first 100 questions testing', help='LangSmith dataset name to evaluate.')
    parser.add_argument('--experiment-prefix', default='first 100 questions testing experiment', help='LangSmith experiment prefix.')
    parser.add_argument('--limit', type=int, default=100, help='Maximum number of local questions to seed into LangSmith.')
    parser.add_argument('--seed-from-local', action='store_true', help='Create the LangSmith dataset from local evaluation JSON if it does not exist.')
    return parser


def main() -> int:
    args = build_parser().parse_args()

    api_key = os.getenv('LANGSMITH_API_KEY', '').strip()
    if not api_key:
        print('LANGSMITH_API_KEY is required to run LangSmith evaluations.')
        return 1

    client = Client(api_key=api_key)
    if args.seed_from_local:
        _seed_dataset(client, args.dataset_name, args.limit)

    evaluate(
        _target,
        data=args.dataset_name,
        evaluators=[exact_match],
        experiment_prefix=args.experiment_prefix,
        client=client,
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())