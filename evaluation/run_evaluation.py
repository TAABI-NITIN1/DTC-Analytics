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
import subprocess
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
    from langchain_openai import ChatOpenAI
    from langchain_core.messages import HumanMessage, SystemMessage
except ImportError:
    ChatOpenAI = None  # type: ignore[assignment]
    HumanMessage = None  # type: ignore[assignment]
    SystemMessage = None  # type: ignore[assignment]

try:
    import mlflow
    _mlflow_uri = os.getenv('MLFLOW_TRACKING_URI', 'http://localhost:5000')
    mlflow.set_tracking_uri(_mlflow_uri)
    mlflow.set_experiment('taabi_ai_analyst_eval')
except ImportError:
    mlflow = None  # type: ignore[assignment]

EVAL_DIR = Path(__file__).parent

IMPORTANT_METRICS = [
    'weighted_score',
    'avg_correctness',
    'avg_hallucination_risk',
    'intent_accuracy',
    'avg_tool_f1',
    'avg_latency_sec',
    'failure_rate',
]


def _safe_div(num: float, den: float, fallback: float = 0.0) -> float:
    return num / den if den else fallback


def _truthy_env(name: str, default: str = '0') -> bool:
    return os.getenv(name, default).strip().lower() in {'1', 'true', 'yes', 'on'}


def _get_git_commit() -> str:
    env_sha = os.getenv('GIT_COMMIT_SHA', '').strip()
    if env_sha:
        return env_sha
    try:
        return subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'],
            cwd=PROJECT_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return ''


def _build_eval_version_metadata() -> dict:
    return {
        'release_version': os.getenv('AI_ANALYST_RELEASE_VERSION', ''),
        'service_version': os.getenv('AI_ANALYST_SERVICE_VERSION', ''),
        'git_commit': _get_git_commit(),
        'env_name': os.getenv('DEPLOYMENT_ENV', os.getenv('ENV_NAME', 'dev')),
        'model_name': os.getenv('AI_ANALYST_MODEL_NAME', 'gpt-3.5-turbo'),
        'dataset_version': os.getenv('AI_ANALYST_DATASET_VERSION', 'v1'),
        'evaluation_method': 'llm_judge' if _truthy_env('EVAL_USE_LLM_JUDGE', '0') else 'heuristic',
    }


def _extract_node_latency_stats(trace_log: list[dict]) -> dict:
    node_events = [e for e in trace_log if isinstance(e, dict) and e.get('type') == 'node']
    durations = [float(e.get('duration', 0.0) or 0.0) for e in node_events]
    per_node: dict[str, float] = {}
    for event in node_events:
        node_name = str(event.get('node', 'unknown'))
        per_node[node_name] = per_node.get(node_name, 0.0) + float(event.get('duration', 0.0) or 0.0)
    return {
        'node_events_count': len(node_events),
        'node_latency_total_sec': round(sum(durations), 3),
        'node_latency_avg_sec': round(_safe_div(sum(durations), len(durations), 0.0), 3),
        'node_latency_max_sec': round(max(durations) if durations else 0.0, 3),
        'node_latency_by_node': {k: round(v, 3) for k, v in sorted(per_node.items())},
    }


def _build_llm_judge() -> object | None:
    if not _truthy_env('EVAL_USE_LLM_JUDGE', '0'):
        return None
    if ChatOpenAI is None:
        return None
    api_key = os.getenv('OPENAI_API_KEY', '').strip()
    if not api_key:
        return None
    return ChatOpenAI(
        model=os.getenv('EVAL_JUDGE_MODEL', 'gpt-4o-mini'),
        api_key=api_key,
        temperature=0,
        timeout=45,
        model_kwargs={'response_format': {'type': 'json_object'}},
    )


def _llm_semantic_scores(judge_llm: object | None, q: dict, result: dict) -> dict | None:
    if judge_llm is None or HumanMessage is None or SystemMessage is None:
        return None
    try:
        prompt = {
            'question': q.get('question', ''),
            'expected_intent': q.get('expected_intent', ''),
            'expected_keywords': q.get('expected_output_contains', []),
            'reference_answer': q.get('reference_answer', ''),
            'predicted_answer': result.get('text', ''),
        }
        resp = judge_llm.invoke([
            SystemMessage(content=(
                'Score the assistant answer and return JSON with keys: '
                'correctness, relevance, completeness, hallucination_risk, rationale. '
                'Each score must be between 0 and 1. '
                'correctness/relevance/completeness: higher is better. '
                'hallucination_risk: higher means more hallucination.'
            )),
            HumanMessage(content=json.dumps(prompt, ensure_ascii=False)),
        ])
        data = json.loads((getattr(resp, 'content', '') or '{}').strip())
        return {
            'correctness': round(float(data.get('correctness', 0.0)), 3),
            'relevance': round(float(data.get('relevance', 0.0)), 3),
            'completeness': round(float(data.get('completeness', 0.0)), 3),
            'hallucination_risk': round(float(data.get('hallucination_risk', 1.0)), 3),
            'judge_rationale': str(data.get('rationale', '')),
            'judge_mode': 'llm',
        }
    except Exception:
        return None


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

def _score_question(q: dict, result: dict, latency: float, judge_llm: object | None = None) -> dict:
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
    tool_precision = _safe_div(len(expected_tools & called_tools), len(called_tools), 1.0)
    tool_f1 = _safe_div(2 * tool_precision * tool_recall, tool_precision + tool_recall, 0.0)

    # Keyword accuracy: fraction of expected keywords present in output text
    expected_kws = q.get('expected_output_contains', [])
    text_lower   = (result.get('text', '') or '').lower()
    keyword_hits = (
        sum(1 for kw in expected_kws if kw.lower() in text_lower) / len(expected_kws)
        if expected_kws else 1.0
    )

    # Semantic quality metrics (LLM judge if enabled, else deterministic heuristic)
    semantic = _llm_semantic_scores(judge_llm, q, result)
    if semantic is None:
        correctness = keyword_hits
        relevance = (0.6 * intent_match) + (0.4 * keyword_hits)
        completeness = (0.5 * keyword_hits) + (0.5 * tool_recall)
        hallucination_risk = max(0.0, round(1.0 - (0.6 * keyword_hits + 0.4 * tool_recall), 3))
        semantic = {
            'correctness': round(correctness, 3),
            'relevance': round(relevance, 3),
            'completeness': round(completeness, 3),
            'hallucination_risk': hallucination_risk,
            'judge_rationale': 'heuristic fallback',
            'judge_mode': 'heuristic',
        }

    tok = result.get('token_usage') or {'prompt': 0, 'completion': 0}
    trace_log = result.get('trace_log', []) or []
    node_stats = _extract_node_latency_stats(trace_log if isinstance(trace_log, list) else [])

    return {
        'id':                q['id'],
        'question':          q['question'],
        'expected_intent':   q.get('expected_intent', ''),
        'actual_intent':     result.get('intent', ''),
        'intent_match':      intent_match,
        'expected_tools':    sorted(expected_tools),
        'actual_tools':      sorted(called_tools),
        'tool_recall':       round(tool_recall, 3),
        'tool_precision':    round(tool_precision, 3),
        'tool_f1':           round(tool_f1, 3),
        'keyword_hits':      round(keyword_hits, 3),
        'correctness':       semantic['correctness'],
        'relevance':         semantic['relevance'],
        'completeness':      semantic['completeness'],
        'hallucination_risk': semantic['hallucination_risk'],
        'hallucination_score': round(1.0 - semantic['hallucination_risk'], 3),
        'judge_mode':        semantic['judge_mode'],
        'judge_rationale':   semantic['judge_rationale'],
        'latency_sec':       latency,
        'tokens_prompt':     tok.get('prompt', 0),
        'tokens_completion': tok.get('completion', 0),
        'tokens_total':      tok.get('prompt', 0) + tok.get('completion', 0),
        'nodes_executed':    result.get('nodes_executed', []),
        **node_stats,
        'failure_reasons':   result.get('failure_reasons', []),
        'request_id':        result.get('request_id'),
        'version':           result.get('version', {}),
    }


# ── Evaluation loop ─────────────────────────────────────────────

def run_evaluation() -> None:
    version_meta = _build_eval_version_metadata()
    judge_llm = _build_llm_judge()

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

        scored = _score_question(q, result, latency, judge_llm=judge_llm)
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
    avg_tool_precision = sum(r['tool_precision'] for r in scored_results) / n
    avg_tool_f1 = sum(r['tool_f1'] for r in scored_results) / n
    avg_keyword_acc  = sum(r['keyword_hits'] for r in scored_results) / n
    avg_correctness  = sum(r['correctness'] for r in scored_results) / n
    avg_relevance    = sum(r['relevance'] for r in scored_results) / n
    avg_completeness = sum(r['completeness'] for r in scored_results) / n
    avg_hallucination_risk = sum(r['hallucination_risk'] for r in scored_results) / n
    avg_hallucination_score = sum(r['hallucination_score'] for r in scored_results) / n
    avg_latency      = sum(r['latency_sec'] for r in scored_results) / n
    avg_node_latency  = sum(r['node_latency_avg_sec'] for r in scored_results) / n
    avg_node_latency_max = sum(r['node_latency_max_sec'] for r in scored_results) / n
    total_tok_prompt = sum(r['tokens_prompt'] for r in scored_results)
    total_tok_comp   = sum(r['tokens_completion'] for r in scored_results)
    total_tok_all    = sum(r['tokens_total'] for r in scored_results)
    failure_rate     = sum(1 for r in scored_results if r['failure_reasons']) / n

    weighted_score = (
        0.35 * avg_correctness
        + 0.15 * avg_relevance
        + 0.15 * avg_completeness
        + 0.15 * avg_hallucination_score
        + 0.10 * min(1.0, 15.0 / max(avg_latency, 0.001))
        + 0.10 * ((avg_tool_precision + avg_tool_recall) / 2)
    )

    print(f'\n{"=" * 60}')
    print(f'  Questions evaluated   : {n}')
    print(f'  Intent accuracy       : {intent_accuracy:.1%}')
    print(f'  Avg tool F1           : {avg_tool_f1:.1%}')
    print(f'  Correctness           : {avg_correctness:.1%}')
    print(f'  Hallucination risk    : {avg_hallucination_risk:.1%}')
    print(f'  Avg latency           : {avg_latency:.2f}s')
    print(f'  Failure rate          : {failure_rate:.1%}')
    print(f'  Weighted score        : {weighted_score:.1%}')
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
                    'eval_method':      version_meta.get('evaluation_method', 'heuristic'),
                    'release_version':  version_meta.get('release_version', ''),
                    'service_version':  version_meta.get('service_version', ''),
                    'git_commit':       version_meta.get('git_commit', ''),
                    'env_name':         version_meta.get('env_name', ''),
                    'model_name':       version_meta.get('model_name', ''),
                    'dataset_version':  version_meta.get('dataset_version', 'v1'),
                })
                mlflow.log_metrics({
                    'weighted_score':          round(weighted_score, 4),
                    'avg_correctness':         round(avg_correctness, 4),
                    'avg_hallucination_risk':  round(avg_hallucination_risk, 4),
                    'intent_accuracy':         round(intent_accuracy, 4),
                    'avg_tool_f1':             round(avg_tool_f1, 4),
                    'avg_latency_sec':         round(avg_latency, 3),
                    'failure_rate':            round(failure_rate, 4),
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
        'version': version_meta,
        'summary': {
            'questions_evaluated':    n,
            'important_metrics': IMPORTANT_METRICS,
            'weighted_score':         round(weighted_score, 4),
            'avg_correctness':        round(avg_correctness, 4),
            'avg_hallucination_risk': round(avg_hallucination_risk, 4),
            'intent_accuracy':        round(intent_accuracy, 4),
            'avg_tool_f1':            round(avg_tool_f1, 4),
            'avg_latency_sec':        round(avg_latency, 3),
            'failure_rate':           round(failure_rate, 4),
        },
        'per_question': scored_results,
    }
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, default=str)
    print(f'  [Report] Written to {report_path.relative_to(PROJECT_ROOT)}')


if __name__ == '__main__':
    run_evaluation()
