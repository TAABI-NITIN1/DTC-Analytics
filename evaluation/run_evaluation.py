"""
Automated evaluation runner for the Taabi AI Analyst.

Loads question datasets from evaluation/*.json, runs chat() for each question,
scores against expected outputs, logs aggregate metrics to MLflow, and writes
a timestamped JSON report.

Usage:
    python evaluation/run_evaluation.py

Prerequisites:
    - OPENAI_API_KEY env var set (or .env file in project root)
    - ClickHouse reachable (for data tools to return real results)
    - MLflow optionally running at MLFLOW_TRACKING_URI
"""
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Force UTF-8 on Windows terminals to avoid cp1252 encoding errors
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
os.environ.setdefault('GIT_PYTHON_REFRESH', 'quiet')  # suppress MLflow git warning
os.environ.setdefault('PYTHONIOENCODING', 'utf-8')

# Add project root to sys.path so 'src' package is importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')
except ImportError:
    pass

from src.ai_analyst import chat  # noqa: E402 — needs .env loaded first

try:
    import mlflow
    _mlflow_uri = os.getenv('MLFLOW_TRACKING_URI', 'http://localhost:5000')
    mlflow.set_tracking_uri(_mlflow_uri)
    mlflow.set_experiment('taabi_ai_analyst_eval')
except ImportError:
    mlflow = None  # type: ignore[assignment]

EVAL_DIR = Path(__file__).parent


# ── Dataset loading ─────────────────────────────────────────────

def _load_datasets() -> list[dict]:
    """Load all question JSON files from the evaluation/ directory."""
    questions = []
    for fname in ['fleet_questions.json', 'vehicle_questions.json', 'dtc_questions.json']:
        fpath = EVAL_DIR / fname
        if fpath.exists():
            with open(fpath, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
            questions.extend(loaded)
            print(f"  Loaded {len(loaded):>3} questions from {fname}")
        else:
            print(f"  [WARN] {fname} not found — skipping")
    return questions


# ── Scoring ─────────────────────────────────────────────────────

def _score_question(q: dict, result: dict, latency: float) -> dict:
    """Score a single question result against expectations."""
    # Intent accuracy
    intent_match = int(result.get('intent', '') == q.get('expected_intent', ''))

    # Tool recall: fraction of expected tools that were actually called
    expected_tools = set(q.get('expected_tools', []))
    called_tools   = set(result.get('tools_called', []))
    tool_recall = (
        len(expected_tools & called_tools) / len(expected_tools)
        if expected_tools else 1.0
    )

    # Keyword accuracy: fraction of expected keywords present in output text
    expected_kws = q.get('expected_output_contains', [])
    text_lower   = (result.get('text', '') or '').lower()
    keyword_hits = (
        sum(1 for kw in expected_kws if kw.lower() in text_lower) / len(expected_kws)
        if expected_kws else 1.0
    )

    tok = result.get('token_usage') or {'prompt': 0, 'completion': 0}
    return {
        'id':                q['id'],
        'question':          q['question'],
        'expected_intent':   q.get('expected_intent', ''),
        'actual_intent':     result.get('intent', ''),
        'intent_match':      intent_match,
        'expected_tools':    sorted(expected_tools),
        'actual_tools':      sorted(called_tools),
        'tool_recall':       round(tool_recall, 3),
        'keyword_hits':      round(keyword_hits, 3),
        'latency_sec':       latency,
        'tokens_prompt':     tok.get('prompt', 0),
        'tokens_completion': tok.get('completion', 0),
        'nodes_executed':    result.get('nodes_executed', []),
        'failure_reasons':   result.get('failure_reasons', []),
    }


# ── Evaluation loop ─────────────────────────────────────────────

def run_evaluation() -> None:
    questions = _load_datasets()
    if not questions:
        print('No questions found. Add JSON files to the evaluation/ directory.')
        return

    n_total = len(questions)
    print(f'\nRunning evaluation on {n_total} questions...\n')
    print(f'  {"#":>4}  {"ID":<12}  {"Intent":^5}  {"Tools":>6}  {"KWds":>5}  {"Lat":>6}  Question')
    print('  ' + '-' * 90)

    scored_results: list[dict] = []

    for i, q in enumerate(questions, 1):
        t0 = time.time()
        try:
            result = chat(
                messages=[{'role': 'user', 'content': q['question']}],
                context=q.get('context'),
            )
        except Exception as exc:
            print(f'  [{i:>3}/{n_total}] {q["id"]:<12}  ERROR: {exc}')
            result = {
                'text': '', 'chart': None, 'intent': '', 'tools_called': [],
                'token_usage': {'prompt': 0, 'completion': 0},
                'nodes_executed': [], 'failure_reasons': [f'exception:{exc}'],
            }
        latency = round(time.time() - t0, 2)

        scored = _score_question(q, result, latency)
        scored_results.append(scored)

        intent_icon = 'Y' if scored['intent_match'] else 'N'
        print(
            f'  [{i:>3}/{n_total}] {q["id"]:<12}  '
            f'{intent_icon}  '
            f'{scored["tool_recall"]:>5.0%}  '
            f'{scored["keyword_hits"]:>4.0%}  '
            f'{latency:>5.1f}s  '
            f'{q["question"][:55]}...'
        )

    # ── Aggregate metrics ──────────────────────────────────────
    n = len(scored_results)
    intent_accuracy  = sum(r['intent_match'] for r in scored_results) / n
    avg_tool_recall  = sum(r['tool_recall'] for r in scored_results) / n
    avg_keyword_acc  = sum(r['keyword_hits'] for r in scored_results) / n
    avg_latency      = sum(r['latency_sec'] for r in scored_results) / n
    total_tok_prompt = sum(r['tokens_prompt'] for r in scored_results)
    total_tok_comp   = sum(r['tokens_completion'] for r in scored_results)
    failure_rate     = sum(1 for r in scored_results if r['failure_reasons']) / n

    print(f'\n{"=" * 60}')
    print(f'  Questions evaluated   : {n}')
    print(f'  Intent accuracy       : {intent_accuracy:.1%}')
    print(f'  Avg tool recall       : {avg_tool_recall:.1%}')
    print(f'  Avg keyword accuracy  : {avg_keyword_acc:.1%}')
    print(f'  Avg latency           : {avg_latency:.2f}s')
    print(f'  Total tokens (prompt) : {total_tok_prompt:,}')
    print(f'  Total tokens (compl.) : {total_tok_comp:,}')
    print(f'  Failure rate          : {failure_rate:.1%}')
    print(f'{"=" * 60}\n')

    # ── MLflow logging ─────────────────────────────────────────
    if mlflow is not None:
        try:
            run_name = f"eval_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
            with mlflow.start_run(run_name=run_name):
                mlflow.log_params({
                    'dataset':          'fleet+vehicle+dtc',
                    'questions_total':  str(n),
                    'evaluation_date':  datetime.now(timezone.utc).date().isoformat(),
                })
                mlflow.log_metrics({
                    'intent_accuracy':         round(intent_accuracy, 4),
                    'avg_tool_recall':         round(avg_tool_recall, 4),
                    'avg_keyword_accuracy':    round(avg_keyword_acc, 4),
                    'avg_latency_sec':         round(avg_latency, 3),
                    'total_tokens_prompt':     float(total_tok_prompt),
                    'total_tokens_completion': float(total_tok_comp),
                    'failure_rate':            round(failure_rate, 4),
                    'questions_evaluated':     float(n),
                })
            print(f"  [MLflow] Metrics logged to experiment 'taabi_ai_analyst_eval' (run: {run_name})")
        except Exception as exc:
            print(f'  [MLflow] Logging failed: {exc}')
    else:
        print('  [MLflow] Not available — install mlflow to enable metric logging.')

    # ── JSON report ────────────────────────────────────────────
    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    report_path = EVAL_DIR / f'results_{ts}.json'
    report = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'summary': {
            'questions_evaluated':    n,
            'intent_accuracy':        round(intent_accuracy, 4),
            'avg_tool_recall':        round(avg_tool_recall, 4),
            'avg_keyword_accuracy':   round(avg_keyword_acc, 4),
            'avg_latency_sec':        round(avg_latency, 3),
            'total_tokens_prompt':    total_tok_prompt,
            'total_tokens_completion': total_tok_comp,
            'failure_rate':           round(failure_rate, 4),
        },
        'per_question': scored_results,
    }
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, default=str)
    print(f'  [Report] Written to {report_path.relative_to(PROJECT_ROOT)}')


if __name__ == '__main__':
    run_evaluation()
