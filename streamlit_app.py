from datetime import date, datetime, timedelta
import json

import pandas as pd
import streamlit as st
from graphviz import Digraph
from langchain_core.messages import AIMessage

from src.ai_analyst import chat
from src.clickhouse_utils import get_clickhouse_client
from src.config import load_env
from src.langsmith_sync import sync_langsmith_root_runs_to_clickhouse
from src.observability_store import OBS_TABLES
import logging
import os

try:
    from langsmith import Client as LangSmithClient
except Exception:
    LangSmithClient = None

# Ensure .env values are loaded when launching Streamlit directly.
load_env()

api_key = os.getenv('OPENAI_API_KEY')


st.set_page_config(page_title='AI Analyst Observability', layout='wide')


if 'run_history' not in st.session_state:
    st.session_state.run_history = []
if 'langsmith_rate_limited' not in st.session_state:
    st.session_state.langsmith_rate_limited = False


def _is_setup_error_payload(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return True
    text = str(payload.get('text') or '').lower()
    return 'openai api key is not configured' in text


def _parse_json_field(value, fallback):
    if isinstance(value, (dict, list)):
        return value
    if not value:
        return fallback
    try:
        return json.loads(value)
    except Exception:
        return fallback


def _get_db_client():
    try:
        return get_clickhouse_client()
    except Exception:
        return None


def _get_langsmith_client():
    if LangSmithClient is None:
        return None
    try:
        return LangSmithClient()
    except Exception:
        return None


def _as_plain_dict(value):
    if isinstance(value, dict):
        return value
    try:
        return dict(value)
    except Exception:
        return {}


def _extract_text_from_messages(messages_obj) -> str:
    if not isinstance(messages_obj, list) or not messages_obj:
        return ''
    last = messages_obj[-1]
    if isinstance(last, AIMessage):
        return str(last.content or '')
    if isinstance(last, dict):
        if 'content' in last:
            return str(last.get('content') or '')
        data = last.get('data') if isinstance(last.get('data'), dict) else {}
        return str(data.get('content') or '')
    return str(getattr(last, 'content', '') or '')


def _to_datetime_utc(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except Exception:
            return None
    return None


def _is_langsmith_rate_limit_error(exc: Exception) -> bool:
    name = type(exc).__name__.lower()
    text = str(exc).lower()
    return 'ratelimit' in name or 'rate limit' in text or '429' in text


@st.cache_data(ttl=60, show_spinner=False)
def _read_langsmith_runs(limit: int = 200, filters: dict | None = None, project_name: str | None = None) -> pd.DataFrame:
    client = _get_langsmith_client()
    if client is None:
        return pd.DataFrame()

    filters = filters or {}
    runs_out = []
    try:
        runs = list(client.list_runs(
            project_name=project_name or os.getenv('LANGCHAIN_PROJECT') or os.getenv('LANGSMITH_PROJECT') or 'default',
            is_root=True,
            limit=int(limit),
        ))
    except Exception as exc:
        if _is_langsmith_rate_limit_error(exc):
            st.session_state['langsmith_rate_limited'] = True
        return pd.DataFrame()

    for run in runs:
        run_id = str(getattr(run, 'id', '') or '')
        if not run_id:
            continue

        metadata = _as_plain_dict(getattr(run, 'extra', {}) or {}).get('metadata', {})
        metadata = _as_plain_dict(metadata)
        start_time = getattr(run, 'start_time', None)
        start_dt = _to_datetime_utc(start_time)
        if start_dt is None:
            continue

        start_date = filters.get('start_date')
        end_date = filters.get('end_date')
        if isinstance(start_date, date) and start_dt.date() < start_date:
            continue
        if isinstance(end_date, date) and start_dt.date() > end_date:
            continue

        intent = str(metadata.get('intent') or '')
        env_name = str(metadata.get('env_name') or '')
        release_version = str(metadata.get('release_version') or '')
        if filters.get('intent') not in (None, '', 'All') and intent != filters.get('intent'):
            continue
        if filters.get('env_name') not in (None, '', 'All') and env_name != filters.get('env_name'):
            continue
        if filters.get('release_version') not in (None, '', 'All') and release_version != filters.get('release_version'):
            continue

        outputs = _as_plain_dict(getattr(run, 'outputs', {}) or {})
        evaluation = _as_plain_dict(outputs.get('evaluation', {}))
        final_score = float(evaluation.get('final_score', 0.0) or 0.0)
        root_cause = str(evaluation.get('root_cause') or 'unknown')

        if filters.get('root_cause') not in (None, '', 'All') and root_cause != filters.get('root_cause'):
            continue

        query = str(metadata.get('query') or '')
        if not query:
            inputs = _as_plain_dict(getattr(run, 'inputs', {}) or {})
            query = str(inputs.get('query') or '')

        runs_out.append(
            {
                'request_id': str(metadata.get('request_id') or run_id),
                'run_id': run_id,
                'ts': start_dt.isoformat(),
                'query': query,
                'intent': intent,
                'final_score': final_score,
                'root_cause': root_cause,
                'env_name': env_name,
                'release_version': release_version,
                'project_name': str(getattr(run, 'session_name', '') or ''),
            }
        )

    if not runs_out:
        return pd.DataFrame()

    st.session_state['langsmith_rate_limited'] = False
    return pd.DataFrame(runs_out).sort_values('ts', ascending=False)


def _load_run_from_langsmith(run_id: str, request_id: str | None = None, project_name: str | None = None) -> dict | None:
    client = _get_langsmith_client()
    if client is None:
        return None

    try:
        run = client.read_run(run_id=run_id)
    except Exception:
        return None

    outputs = _as_plain_dict(getattr(run, 'outputs', {}) or {})
    extra = _as_plain_dict(getattr(run, 'extra', {}) or {})
    metadata = _as_plain_dict(extra.get('metadata', {}) or {})
    version = _as_plain_dict(outputs.get('version', {}) or {})
    evaluation = _as_plain_dict(outputs.get('evaluation', {}) or {})
    trace_log = outputs.get('trace_log') if isinstance(outputs.get('trace_log'), list) else []

    text = str(outputs.get('text') or '')
    if not text:
        text = _extract_text_from_messages(outputs.get('messages'))

    payload = {
        'request_id': str(request_id or metadata.get('request_id') or getattr(run, 'id', '')),
        'intent': str(outputs.get('intent') or metadata.get('intent') or ''),
        'text': text,
        'trace_log': trace_log,
        'evaluation': evaluation,
        'nodes_executed': outputs.get('nodes_executed') if isinstance(outputs.get('nodes_executed'), list) else [],
        'version': version,
        'prompt_versions': _as_plain_dict(version.get('prompt_versions', {}) or {}),
        'sql_events': outputs.get('sql_events') if isinstance(outputs.get('sql_events'), list) else [],
        'tools_called': outputs.get('tools_called') if isinstance(outputs.get('tools_called'), list) else [],
    }

    start_time = _to_datetime_utc(getattr(run, 'start_time', None))
    return {
        'timestamp': (start_time.isoformat() if isinstance(start_time, datetime) else ''),
        'query': str(metadata.get('query') or ''),
        'payload': payload,
        'project_name': str(project_name or getattr(run, 'session_name', '') or ''),
    }


def _read_filter_options() -> dict:
    client = _get_db_client()
    if client is None:
        return {
            'intents': [],
            'root_causes': [],
            'env_names': [],
            'release_versions': [],
            'prompt_keys': [],
            'prompt_versions': [],
        }

    def _safe_distinct(query: str, col_name: str) -> list[str]:
        try:
            rows = client.execute(query)
            values = [str(r[0]) for r in rows if r and r[0] not in (None, '')]
            return sorted(set(values))
        except Exception:
            return []

    return {
        'intents': _safe_distinct(f"SELECT DISTINCT intent FROM {OBS_TABLES['requests']} ORDER BY intent", 'intent'),
        'root_causes': _safe_distinct(f"SELECT DISTINCT root_cause FROM {OBS_TABLES['requests']} ORDER BY root_cause", 'root_cause'),
        'env_names': _safe_distinct(f"SELECT DISTINCT env_name FROM {OBS_TABLES['requests']} ORDER BY env_name", 'env_name'),
        'release_versions': _safe_distinct(f"SELECT DISTINCT release_version FROM {OBS_TABLES['requests']} ORDER BY release_version", 'release_version'),
        'prompt_keys': _safe_distinct(f"SELECT DISTINCT prompt_key FROM {OBS_TABLES['prompt_scores']} ORDER BY prompt_key", 'prompt_key'),
        'prompt_versions': _safe_distinct(f"SELECT DISTINCT prompt_version FROM {OBS_TABLES['prompt_scores']} ORDER BY prompt_version", 'prompt_version'),
    }


@st.cache_data(ttl=300, show_spinner=False)
def _read_langsmith_filter_options(project_name: str, lookback_days: int = 30) -> dict:
    default_start = date.today() - timedelta(days=int(lookback_days))
    rows_df = _read_langsmith_runs(
        limit=400,
        filters={'start_date': default_start, 'end_date': date.today()},
        project_name=project_name,
    )
    if rows_df.empty:
        return {
            'intents': [],
            'root_causes': [],
            'env_names': [],
            'release_versions': [],
            'prompt_keys': [],
            'prompt_versions': [],
        }

    def _distinct(col_name: str) -> list[str]:
        if col_name not in rows_df.columns:
            return []
        values = [str(v) for v in rows_df[col_name].dropna().tolist() if str(v)]
        return sorted(set(values))

    return {
        'intents': _distinct('intent'),
        'root_causes': _distinct('root_cause'),
        'env_names': _distinct('env_name'),
        'release_versions': _distinct('release_version'),
        'prompt_keys': [],
        'prompt_versions': [],
    }


def _build_request_where(filters: dict, alias: str = 'r') -> tuple[str, dict]:
    clauses = []
    params: dict = {}

    start_date = filters.get('start_date')
    end_date = filters.get('end_date')
    if isinstance(start_date, date):
        clauses.append(f"toDate({alias}.ts) >= %(start_date)s")
        params['start_date'] = start_date.isoformat()
    if isinstance(end_date, date):
        clauses.append(f"toDate({alias}.ts) <= %(end_date)s")
        params['end_date'] = end_date.isoformat()

    for key in ['intent', 'root_cause', 'env_name', 'release_version']:
        value = filters.get(key)
        if value and value != 'All':
            clauses.append(f"{alias}.{key} = %({key})s")
            params[key] = value

    prompt_key = filters.get('prompt_key')
    prompt_version = filters.get('prompt_version')
    exists_clauses = []
    if prompt_key and prompt_key != 'All':
        exists_clauses.append('ps.prompt_key = %(prompt_key)s')
        params['prompt_key'] = prompt_key
    if prompt_version and prompt_version != 'All':
        exists_clauses.append('ps.prompt_version = %(prompt_version)s')
        params['prompt_version'] = prompt_version
    if exists_clauses:
        exists_sql = ' AND '.join(exists_clauses)
        clauses.append(
            f"EXISTS (SELECT 1 FROM {OBS_TABLES['prompt_scores']} ps WHERE ps.request_id = {alias}.request_id AND {exists_sql})"
        )

    if not clauses:
        return '', params
    return 'WHERE ' + ' AND '.join(clauses), params


def _build_prompt_where(filters: dict, alias: str = 'p') -> tuple[str, dict]:
    clauses = []
    params: dict = {}

    start_date = filters.get('start_date')
    end_date = filters.get('end_date')
    if isinstance(start_date, date):
        clauses.append(f"toDate({alias}.ts) >= %(start_date)s")
        params['start_date'] = start_date.isoformat()
    if isinstance(end_date, date):
        clauses.append(f"toDate({alias}.ts) <= %(end_date)s")
        params['end_date'] = end_date.isoformat()

    for key in ['intent', 'root_cause', 'env_name', 'release_version', 'prompt_key', 'prompt_version']:
        value = filters.get(key)
        if value and value != 'All':
            clauses.append(f"{alias}.{key} = %({key})s")
            params[key] = value

    if not clauses:
        return '', params
    return 'WHERE ' + ' AND '.join(clauses), params


def _suggest_refinement(prompt_key: str, root_cause: str) -> str:
    root = (root_cause or '').lower()
    if 'no_tool_data' in root or 'sql_error' in root:
        return 'Tighten investigate prompt: require minimum tool coverage and explicit customer-scoped retrieval before fallback.'
    if 'fault_analysis_empty' in root:
        return 'Strengthen fault analysis prompt with required sections: system grouping, severity split, and top-3 evidence-backed findings.'
    if 'explain_empty' in root:
        return 'Strengthen explain prompt with mandatory response template and non-empty completion constraints.'
    if prompt_key == 'explain':
        return 'Add few-shot examples for concise but complete manager-facing explanations with explicit next actions.'
    if prompt_key == 'root_cause':
        return 'Increase co-occurrence reasoning constraints and require ranked hypotheses with confidence and mechanism.'
    return 'Refine prompt instructions with stricter output schema and at least one high-quality example for this node.'


def _build_prompt_refinement_queue(cohorts_df: pd.DataFrame, failure_df: pd.DataFrame) -> pd.DataFrame:
    if cohorts_df.empty:
        return pd.DataFrame()

    queue = cohorts_df.copy()
    if failure_df.empty:
        queue['dominant_root_cause'] = 'unknown'
        queue['root_cause_runs'] = 0
    else:
        dominant = (
            failure_df.sort_values(['prompt_key', 'prompt_version', 'runs'], ascending=[True, True, False])
            .groupby(['prompt_key', 'prompt_version'], as_index=False)
            .first()[['prompt_key', 'prompt_version', 'root_cause', 'runs']]
            .rename(columns={'root_cause': 'dominant_root_cause', 'runs': 'root_cause_runs'})
        )
        queue = queue.merge(dominant, on=['prompt_key', 'prompt_version'], how='left')
        queue['dominant_root_cause'] = queue['dominant_root_cause'].fillna('unknown')
        queue['root_cause_runs'] = queue['root_cause_runs'].fillna(0).astype(int)

    queue['priority_score'] = (
        ((1.0 - queue['avg_node_score'].astype(float)).clip(lower=0.0, upper=1.0) * 0.7)
        + ((queue['runs'].astype(float).clip(lower=0.0, upper=50.0) / 50.0) * 0.3)
    ).round(3)

    queue['suggested_refinement'] = queue.apply(
        lambda row: _suggest_refinement(str(row['prompt_key']), str(row['dominant_root_cause'])),
        axis=1,
    )

    return queue.sort_values(['priority_score', 'avg_node_score', 'runs'], ascending=[False, True, False])[
        [
            'prompt_key',
            'prompt_version',
            'avg_node_score',
            'avg_final_score',
            'runs',
            'dominant_root_cause',
            'root_cause_runs',
            'priority_score',
            'suggested_refinement',
        ]
    ]


def _read_recent_requests(limit: int = 200, filters: dict | None = None) -> pd.DataFrame:
    client = _get_db_client()
    if client is None:
        return pd.DataFrame()
    try:
        where_sql, params = _build_request_where(filters or {}, alias='r')
        params['limit'] = int(limit)
        rows = client.execute(
            f'''
            SELECT r.request_id, r.ts, r.query, r.intent, r.final_score, r.root_cause
            FROM {OBS_TABLES['requests']} r
            {where_sql}
            ORDER BY r.ts DESC
            LIMIT %(limit)s
            ''',
            params,
        )
        return pd.DataFrame(rows, columns=['request_id', 'ts', 'query', 'intent', 'final_score', 'root_cause'])
    except Exception:
        return pd.DataFrame()


def _read_prompt_cohorts(limit: int = 100, filters: dict | None = None) -> pd.DataFrame:
    client = _get_db_client()
    if client is None:
        return pd.DataFrame()
    try:
        where_sql, params = _build_prompt_where(filters or {}, alias='p')
        params['limit'] = int(limit)
        rows = client.execute(
            f'''
            SELECT
                p.prompt_key,
                p.prompt_version,
                avg(p.node_score) AS avg_node_score,
                avg(p.final_score) AS avg_final_score,
                count() AS runs
            FROM {OBS_TABLES['prompt_scores']} p
            {where_sql}
            GROUP BY p.prompt_key, p.prompt_version
            HAVING runs >= 3
            ORDER BY avg_node_score ASC, runs DESC
            LIMIT %(limit)s
            ''',
            params,
        )
        return pd.DataFrame(rows, columns=['prompt_key', 'prompt_version', 'avg_node_score', 'avg_final_score', 'runs'])
    except Exception:
        return pd.DataFrame()


def _read_prompt_failure_buckets(limit: int = 100, filters: dict | None = None) -> pd.DataFrame:
    client = _get_db_client()
    if client is None:
        return pd.DataFrame()
    try:
        where_sql, params = _build_prompt_where(filters or {}, alias='p')
        params['limit'] = int(limit)
        if where_sql:
            where_sql = where_sql + ' AND p.node_score < 0.7'
        else:
            where_sql = 'WHERE p.node_score < 0.7'
        rows = client.execute(
            f'''
            SELECT
                p.prompt_key,
                p.prompt_version,
                p.root_cause,
                count() AS runs,
                avg(p.node_score) AS avg_node_score
            FROM {OBS_TABLES['prompt_scores']} p
            {where_sql}
            GROUP BY p.prompt_key, p.prompt_version, p.root_cause
            ORDER BY runs DESC
            LIMIT %(limit)s
            ''',
            params,
        )
        return pd.DataFrame(rows, columns=['prompt_key', 'prompt_version', 'root_cause', 'runs', 'avg_node_score'])
    except Exception:
        return pd.DataFrame()


def _load_run_from_db(request_id: str) -> dict | None:
    client = _get_db_client()
    if client is None:
        return None

    req_rows = client.execute(
        f'''
        SELECT request_id, ts, query, intent, response_text, final_score,
               correctness, completeness, relevance, groundedness,
             root_cause, judge_mode, evaluation_json, version_json, prompt_versions_json
        FROM {OBS_TABLES['requests']}
        WHERE request_id = %(request_id)s
        ORDER BY ts DESC
        LIMIT 1
        ''',
        {'request_id': request_id},
    )
    if not req_rows:
        return None
    req = req_rows[0]

    node_rows = client.execute(
        f'''
        SELECT node, status, duration_sec, row_count, sql_query,
               metrics_json, input_summary_json, output_summary_json, failure_reasons_json
        FROM {OBS_TABLES['nodes']}
        WHERE request_id = %(request_id)s
        ORDER BY ts ASC, node ASC
        ''',
        {'request_id': request_id},
    )

    nodes = []
    for row in node_rows:
        nodes.append(
            {
                'type': 'node',
                'request_id': request_id,
                'node': row[0],
                'status': row[1],
                'duration': float(row[2] or 0.0),
                'row_count': int(row[3] or 0),
                'sql_query': row[4] or '',
                'metrics': _parse_json_field(row[5], {}),
                'input_summary': _parse_json_field(row[6], {}),
                'output_summary': _parse_json_field(row[7], {}),
                'failure_reasons': _parse_json_field(row[8], []),
            }
        )

    score_rows = client.execute(
        f'''
        SELECT node, score
        FROM {OBS_TABLES['node_scores']}
        WHERE request_id = %(request_id)s
        ''',
        {'request_id': request_id},
    )
    node_scores = {str(row[0]): float(row[1]) for row in score_rows}

    evaluation_json = _parse_json_field(req[12], {})
    version_json = _parse_json_field(req[13], {})
    prompt_versions_json = _parse_json_field(req[14], {})
    evaluation_json.update(
        {
            'final_score': float(req[5] or 0.0),
            'correctness': float(req[6] or 0.0),
            'completeness': float(req[7] or 0.0),
            'relevance': float(req[8] or 0.0),
            'groundedness': float(req[9] or 0.0),
            'root_cause': req[10] or 'unknown',
            'judge_mode': req[11] or '',
            'node_scores': node_scores or evaluation_json.get('node_scores', {}),
        }
    )

    return {
        'timestamp': str(req[1]),
        'query': str(req[2] or ''),
        'payload': {
            'request_id': str(req[0]),
            'intent': str(req[3] or ''),
            'text': str(req[4] or ''),
            'trace_log': nodes,
            'evaluation': evaluation_json,
            'nodes_executed': [n['node'] for n in nodes],
            'version': version_json,
            'prompt_versions': prompt_versions_json,
        },
    }


st.title('AI Analyst Observability Dashboard')

st.sidebar.header('Run Query')
query = st.sidebar.text_area('User Query', height=120)
customer_name = st.sidebar.text_input('Customer Name', value='VRL LOGISTICS LIMITED')
mode = st.sidebar.selectbox('Mode', ['general', 'fleet', 'vehicle', 'dtc'])
data_source = st.sidebar.radio('History Source', ['ClickHouse', 'Session'], index=0)
langsmith_project = st.sidebar.text_input(
    'LangSmith Project',
    value=os.getenv('LANGCHAIN_PROJECT') or os.getenv('LANGSMITH_PROJECT') or 'default',
)
sync_lookback_days = st.sidebar.number_input('Sync Lookback Days', min_value=1, max_value=180, value=14, step=1)
sync_limit = st.sidebar.number_input('Sync Max Runs', min_value=10, max_value=5000, value=200, step=10)
sync_clicked = st.sidebar.button('Sync LangSmith -> ClickHouse')

st.sidebar.caption(
    'Runtime Env: '
    + f"OpenAI={'OK' if bool(os.getenv('OPENAI_API_KEY')) else 'Missing'} | "
    + f"LangSmith={'OK' if bool(os.getenv('LANGSMITH_API_KEY')) else 'Missing'}"
)

if sync_clicked:
    sync_result = sync_langsmith_root_runs_to_clickhouse(
        project_name=langsmith_project,
        limit=int(sync_limit),
        lookback_days=int(sync_lookback_days),
    )
    if sync_result.get('ok'):
        st.sidebar.success(
            f"Synced: inserted={sync_result.get('inserted', 0)}, "
            f"skipped_existing={sync_result.get('skipped_existing', 0)}, "
            f"failed={sync_result.get('failed', 0)}"
        )
    else:
        st.sidebar.error(str(sync_result.get('error') or 'LangSmith sync failed.'))

filters: dict = {}
if data_source == 'ClickHouse':
    st.sidebar.header('Analytics Filters')
    filter_options = _read_filter_options()
    default_start = date.today() - timedelta(days=14)
    date_range = st.sidebar.date_input('Date Range', value=(default_start, date.today()))

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = date_range
        end_date = date_range

    filters = {
        'start_date': start_date,
        'end_date': end_date,
        'intent': st.sidebar.selectbox('Intent Filter', ['All'] + filter_options['intents']),
        'root_cause': st.sidebar.selectbox('Root Cause Filter', ['All'] + filter_options['root_causes']),
        'env_name': st.sidebar.selectbox('Environment Filter', ['All'] + filter_options['env_names']),
        'release_version': st.sidebar.selectbox('Release Filter', ['All'] + filter_options['release_versions']),
        'prompt_key': st.sidebar.selectbox('Prompt Key Filter', ['All'] + filter_options['prompt_keys']),
        'prompt_version': st.sidebar.selectbox('Prompt Version Filter', ['All'] + filter_options['prompt_versions']),
    }

run_clicked = st.sidebar.button('Run', type='primary')

if run_clicked and query.strip():
    if not api_key:
        st.sidebar.error('OPENAI_API_KEY is missing. Add it in your environment or .env, then rerun.')
    else:
        payload = chat(
            messages=[{'role': 'user', 'content': query.strip()}],
            context={
                'customer_name': customer_name.strip(),
                'mode': mode,
                'force_detailed_response': True,
            },
        )
        if _is_setup_error_payload(payload):
            st.sidebar.error(str(payload.get('text') or 'AI service is not configured.'))
        else:
            st.session_state.run_history.insert(
                0,
                {
                    'timestamp': datetime.utcnow().isoformat(),
                    'query': query.strip(),
                    'payload': payload,
                },
            )

history = st.session_state.run_history
selected = None

if data_source == 'ClickHouse':
    recent_df = _read_recent_requests(limit=200, filters=filters)
    if recent_df.empty:
        st.sidebar.warning('No ClickHouse runs match current filters; using session history.')
        data_source = 'Session'
    else:
        request_ids = recent_df['request_id'].astype(str).tolist()
        label_map = {
            str(row['request_id']): f"{row['ts']} | {str(row['intent'])} | score={float(row['final_score'] or 0):.3f}"
            for _, row in recent_df.iterrows()
        }
        selected_request_id = st.sidebar.selectbox('Select Request', request_ids, format_func=lambda rid: label_map.get(str(rid), str(rid)))
        selected = _load_run_from_db(selected_request_id)

if data_source == 'Session':
    if not history:
        st.info('Run a query from the sidebar to start collecting traces, or sync LangSmith into ClickHouse.')
        st.stop()
    selected_index = st.sidebar.selectbox(
        'Select Run',
        options=list(range(len(history))),
        format_func=lambda i: f"{history[i]['timestamp']} | {history[i]['payload'].get('request_id', 'no-request-id')}",
    )
    selected = history[selected_index]

if selected is None:
    st.info('No run selected.')
    st.stop()

payload = selected['payload']
trace = payload.get('trace_log', []) or []
evaluation = payload.get('evaluation', {}) or {}

st.subheader('Final Answer')
st.write(payload.get('text', ''))

summary_cols = st.columns(5)
summary_cols[0].metric('Request ID', payload.get('request_id', ''))
summary_cols[1].metric('Intent', payload.get('intent', ''))
summary_cols[2].metric('Nodes', len(payload.get('nodes_executed', []) or []))
summary_cols[3].metric('Final Score', f"{float(evaluation.get('final_score', 0.0)):.3f}")
summary_cols[4].metric('Root Cause', str(evaluation.get('root_cause', 'unknown')))

st.subheader('Execution Graph')
node_events = [e for e in trace if isinstance(e, dict) and e.get('type') == 'node']
node_scores = evaluation.get('node_scores', {}) if isinstance(evaluation.get('node_scores'), dict) else {}

if node_events:
    dot = Digraph(comment='Execution Flow')
    dot.attr(rankdir='LR')

    for i, event in enumerate(node_events):
        node_name = str(event.get('node', f'node_{i}'))
        duration = float(event.get('duration', 0.0) or 0.0)
        status = str(event.get('status', 'unknown'))
        node_score = node_scores.get(node_name)

        color = 'lightgreen' if status == 'success' else 'lightcoral'
        if isinstance(node_score, (int, float)):
            if node_score < 0.6:
                color = 'lightcoral'
            elif node_score < 0.8:
                color = 'khaki'
            else:
                color = 'lightgreen'

        label = f"{node_name}\\n{duration:.3f}s\\nstatus={status}"
        if isinstance(node_score, (int, float)):
            label += f"\\nscore={float(node_score):.2f}"

        dot.node(str(i), label=label, style='filled', fillcolor=color)
        if i > 0:
            dot.edge(str(i - 1), str(i))

    st.graphviz_chart(dot)
else:
    st.warning('No node events found in trace_log for this run.')

st.subheader('Evaluation Panel')
st.json(evaluation)

prompt_versions_payload = payload.get('prompt_versions') or ((payload.get('version') or {}).get('prompt_versions') if isinstance(payload.get('version'), dict) else {})
if isinstance(prompt_versions_payload, dict) and prompt_versions_payload:
    st.subheader('Prompt Versions (Run)')
    st.dataframe(
        pd.DataFrame(
            [{'prompt_key': k, 'prompt_version': v} for k, v in prompt_versions_payload.items()]
        ),
        use_container_width=True,
    )

st.subheader('Node Timeline')
timeline_rows = []
for event in node_events:
    timeline_rows.append(
        {
            'node': event.get('node'),
            'duration_sec': float(event.get('duration', 0.0) or 0.0),
            'status': event.get('status', 'unknown'),
            'sql_events': len(event.get('sql_events', []) or []),
            'row_count': int(event.get('row_count', 0) or 0),
        }
    )

if timeline_rows:
    timeline_df = pd.DataFrame(timeline_rows)
    st.dataframe(timeline_df, use_container_width=True)

st.subheader('Node Drilldown')
for event in node_events:
    node_name = event.get('node', 'unknown')
    with st.expander(f"{node_name}"):
        st.write('Status:', event.get('status', 'unknown'))
        st.write('Duration (sec):', event.get('duration', 0))
        st.write('Row Count:', event.get('row_count', 0))
        if event.get('sql_query'):
            st.code(event.get('sql_query', ''), language='sql')
        st.markdown('**Metrics**')
        st.json(event.get('metrics', {}))
        st.markdown('**Input Summary**')
        st.json(event.get('input_summary', {}))
        st.markdown('**Output Summary**')
        st.json(event.get('output_summary', {}))
        if event.get('sql_events'):
            st.markdown('**SQL Events**')
            st.json(event.get('sql_events', []))

with st.expander('Raw Trace Log'):
    st.json(trace)

if data_source == 'ClickHouse':
    st.subheader('Trends')
    client = _get_db_client()
    if client is not None:
        req_where_sql, req_params = _build_request_where(filters, alias='r')
        trend_rows = client.execute(
            f'''
            SELECT toDate(r.ts) AS d, avg(r.final_score) AS avg_score, count() AS runs
            FROM {OBS_TABLES['requests']} r
            {req_where_sql}
            GROUP BY d
            ORDER BY d ASC
            ''',
            req_params,
        )
        if trend_rows:
            trend_df = pd.DataFrame(trend_rows, columns=['date', 'avg_score', 'runs'])
            st.line_chart(trend_df.set_index('date')[['avg_score']])

        root_rows = client.execute(
            f'''
            SELECT r.root_cause, count() AS runs
            FROM {OBS_TABLES['requests']} r
            {req_where_sql}
            GROUP BY r.root_cause
            ORDER BY runs DESC
            LIMIT 10
            ''',
            req_params,
        )
        if root_rows:
            root_df = pd.DataFrame(root_rows, columns=['root_cause', 'runs'])
            st.dataframe(root_df, use_container_width=True)

    st.subheader('Prompt Quality Analytics')
    cohorts_df = _read_prompt_cohorts(limit=200, filters=filters)
    if cohorts_df.empty:
        st.info('No prompt cohort data yet. Run more queries to populate prompt analytics.')
    else:
        left, right = st.columns(2)
        with left:
            st.markdown('**Lowest Avg Node Score (Needs Refinement)**')
            st.dataframe(cohorts_df.sort_values(['avg_node_score', 'runs'], ascending=[True, False]).head(10), use_container_width=True)
        with right:
            st.markdown('**Highest Avg Node Score (Performing Well)**')
            st.dataframe(cohorts_df.sort_values(['avg_node_score', 'runs'], ascending=[False, False]).head(10), use_container_width=True)

        heat = cohorts_df.pivot_table(index='prompt_key', columns='prompt_version', values='avg_node_score', aggfunc='mean')
        if not heat.empty:
            st.markdown('**Prompt Score Matrix (avg node score)**')
            st.dataframe(heat, use_container_width=True)

    failure_df = _read_prompt_failure_buckets(limit=100, filters=filters)
    if not failure_df.empty:
        st.markdown('**Low-Score Root Cause Buckets (node_score < 0.70)**')
        st.dataframe(failure_df, use_container_width=True)

    st.subheader('Prompt Refinement Queue')
    refinement_df = _build_prompt_refinement_queue(cohorts_df, failure_df)
    if refinement_df.empty:
        st.info('No refinement candidates yet for current filters.')
    else:
        st.dataframe(refinement_df.head(20), use_container_width=True)

