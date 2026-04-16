import json
import logging
from datetime import datetime, timezone

from src.clickhouse_utils import get_clickhouse_client


log = logging.getLogger(__name__)

OBS_TABLES = {
    'requests': 'ai_obs_requests',
    'nodes': 'ai_obs_nodes',
    'sql_events': 'ai_obs_sql_events',
    'sql_planner_events': 'ai_obs_sql_planner_events',
    'node_scores': 'ai_obs_node_scores',
    'prompt_scores': 'ai_obs_prompt_scores',
    'prompt_versions': 'ai_obs_prompt_versions',
    'prompt_inventory': 'ai_obs_prompt_inventory',
}

_NODE_TO_PROMPT_KEY = {
    'detect_intent': 'intent',
    'investigate_data': 'investigate',
    'analyze_faults': 'fault_analysis',
    'reason_root_cause': 'root_cause',
    'assess_maintenance': 'maintenance',
    'generate_recommendation': 'recommendation',
    'explain': 'explain',
}


def _iso_to_dt(value: str | None) -> datetime:
    if value:
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            pass
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ensure_observability_tables() -> None:
    client = get_clickhouse_client()

    client.execute(
        f'''
        CREATE TABLE IF NOT EXISTS {OBS_TABLES['requests']} (
            request_id String,
            ts DateTime,
            query String,
            intent String,
            customer_name String,
            mode String,
            response_text String,
            final_score Float32,
            correctness Float32,
            completeness Float32,
            relevance Float32,
            groundedness Float32,
            root_cause String,
            judge_mode String,
            tools_called_count UInt16,
            nodes_executed_count UInt16,
            failure_count UInt16,
            tokens_prompt UInt32,
            tokens_completion UInt32,
            evaluation_json String,
            version_json String,
            release_version String,
            service_version String,
            model_name String,
            env_name String,
            prompt_versions_json String,
            failure_reasons_json String,
            planner_events_count UInt16,
            planner_success_count UInt16,
            planner_error_count UInt16,
            planner_fix_attempts_sum UInt16
        )
        ENGINE = MergeTree
        ORDER BY (ts, request_id)
        '''
    )

    client.execute(f"ALTER TABLE {OBS_TABLES['requests']} ADD COLUMN IF NOT EXISTS release_version String")
    client.execute(f"ALTER TABLE {OBS_TABLES['requests']} ADD COLUMN IF NOT EXISTS service_version String")
    client.execute(f"ALTER TABLE {OBS_TABLES['requests']} ADD COLUMN IF NOT EXISTS model_name String")
    client.execute(f"ALTER TABLE {OBS_TABLES['requests']} ADD COLUMN IF NOT EXISTS env_name String")
    client.execute(f"ALTER TABLE {OBS_TABLES['requests']} ADD COLUMN IF NOT EXISTS prompt_versions_json String")
    client.execute(f"ALTER TABLE {OBS_TABLES['requests']} ADD COLUMN IF NOT EXISTS failure_reasons_json String")
    client.execute(f"ALTER TABLE {OBS_TABLES['requests']} ADD COLUMN IF NOT EXISTS customer_name String")
    client.execute(f"ALTER TABLE {OBS_TABLES['requests']} ADD COLUMN IF NOT EXISTS mode String")
    client.execute(f"ALTER TABLE {OBS_TABLES['requests']} ADD COLUMN IF NOT EXISTS planner_events_count UInt16")
    client.execute(f"ALTER TABLE {OBS_TABLES['requests']} ADD COLUMN IF NOT EXISTS planner_success_count UInt16")
    client.execute(f"ALTER TABLE {OBS_TABLES['requests']} ADD COLUMN IF NOT EXISTS planner_error_count UInt16")
    client.execute(f"ALTER TABLE {OBS_TABLES['requests']} ADD COLUMN IF NOT EXISTS planner_fix_attempts_sum UInt16")

    client.execute(
        f'''
        CREATE TABLE IF NOT EXISTS {OBS_TABLES['nodes']} (
            request_id String,
            ts DateTime,
            node String,
            customer_name String,
            mode String,
            status String,
            duration_sec Float32,
            row_count UInt32,
            sql_query String,
            metrics_json String,
            input_summary_json String,
            output_summary_json String,
            failure_reasons_json String
        )
        ENGINE = MergeTree
        ORDER BY (request_id, ts, node)
        '''
    )
    client.execute(f"ALTER TABLE {OBS_TABLES['nodes']} ADD COLUMN IF NOT EXISTS customer_name String")
    client.execute(f"ALTER TABLE {OBS_TABLES['nodes']} ADD COLUMN IF NOT EXISTS mode String")

    client.execute(
        f'''
        CREATE TABLE IF NOT EXISTS {OBS_TABLES['sql_events']} (
            request_id String,
            ts DateTime,
            node String,
            tool String,
            customer_name String,
            mode String,
            success UInt8,
            row_count UInt32,
            duration_sec Float32,
            query String,
            error String
        )
        ENGINE = MergeTree
        ORDER BY (request_id, ts, node)
        '''
    )
    client.execute(f"ALTER TABLE {OBS_TABLES['sql_events']} ADD COLUMN IF NOT EXISTS customer_name String")
    client.execute(f"ALTER TABLE {OBS_TABLES['sql_events']} ADD COLUMN IF NOT EXISTS mode String")

    client.execute(
        f'''
        CREATE TABLE IF NOT EXISTS {OBS_TABLES['sql_planner_events']} (
            request_id String,
            ts DateTime,
            tool_call_id String,
            tool String,
            customer_name String,
            mode String,
            generated_by String,
            fix_attempts UInt16,
            success UInt8,
            selected_tables_json String,
            selected_tables_csv String,
            generated_query String,
            error String,
            retry_reasons_json String,
            query String
        )
        ENGINE = MergeTree
        ORDER BY (request_id, ts, tool)
        '''
    )
    client.execute(f"ALTER TABLE {OBS_TABLES['sql_planner_events']} ADD COLUMN IF NOT EXISTS customer_name String")
    client.execute(f"ALTER TABLE {OBS_TABLES['sql_planner_events']} ADD COLUMN IF NOT EXISTS mode String")
    client.execute(f"ALTER TABLE {OBS_TABLES['sql_planner_events']} ADD COLUMN IF NOT EXISTS retry_reasons_json String")

    client.execute(
        f'''
        CREATE TABLE IF NOT EXISTS {OBS_TABLES['node_scores']} (
            request_id String,
            ts DateTime,
            node String,
            customer_name String,
            mode String,
            score Float32
        )
        ENGINE = MergeTree
        ORDER BY (request_id, ts, node)
        '''
    )
    client.execute(f"ALTER TABLE {OBS_TABLES['node_scores']} ADD COLUMN IF NOT EXISTS customer_name String")
    client.execute(f"ALTER TABLE {OBS_TABLES['node_scores']} ADD COLUMN IF NOT EXISTS mode String")

    client.execute(
        f'''
        CREATE TABLE IF NOT EXISTS {OBS_TABLES['prompt_scores']} (
            request_id String,
            ts DateTime,
            prompt_key String,
            prompt_version String,
            node String,
            customer_name String,
            mode String,
            node_score Float32,
            final_score Float32,
            root_cause String,
            intent String,
            env_name String,
            release_version String
        )
        ENGINE = MergeTree
        ORDER BY (ts, prompt_key, prompt_version, request_id)
        '''
    )
    client.execute(f"ALTER TABLE {OBS_TABLES['prompt_scores']} ADD COLUMN IF NOT EXISTS customer_name String")
    client.execute(f"ALTER TABLE {OBS_TABLES['prompt_scores']} ADD COLUMN IF NOT EXISTS mode String")

    client.execute(
        f'''
        CREATE TABLE IF NOT EXISTS {OBS_TABLES['prompt_versions']} (
            request_id String,
            ts DateTime,
            prompt_key String,
            prompt_version String,
            customer_name String,
            mode String,
            intent String,
            env_name String,
            release_version String,
            service_version String,
            model_name String,
            git_commit String
        )
        ENGINE = MergeTree
        ORDER BY (ts, prompt_key, prompt_version, request_id)
        '''
    )
    client.execute(f"ALTER TABLE {OBS_TABLES['prompt_versions']} ADD COLUMN IF NOT EXISTS customer_name String")
    client.execute(f"ALTER TABLE {OBS_TABLES['prompt_versions']} ADD COLUMN IF NOT EXISTS mode String")

    client.execute(
        f'''
        CREATE TABLE IF NOT EXISTS {OBS_TABLES['prompt_inventory']} (
            prompt_key String,
            prompt String,
            prompt_version String
        )
        ENGINE = MergeTree
        ORDER BY (prompt_key, prompt_version)
        '''
    )
    client.execute(f"ALTER TABLE {OBS_TABLES['prompt_inventory']} ADD COLUMN IF NOT EXISTS prompt String")


def persist_observability_run(*, query: str, payload: dict) -> None:
    request_id = str(payload.get('request_id') or '')
    if not request_id:
        return

    trace_log = payload.get('trace_log') if isinstance(payload.get('trace_log'), list) else []
    sql_events = payload.get('sql_events') if isinstance(payload.get('sql_events'), list) else []
    sql_planner_events = payload.get('sql_planner_events') if isinstance(payload.get('sql_planner_events'), list) else []
    evaluation = payload.get('evaluation') if isinstance(payload.get('evaluation'), dict) else {}
    customer_name = str(payload.get('customer_name') or '')
    mode = str(payload.get('mode') or '')

    node_events = [e for e in trace_log if isinstance(e, dict) and e.get('type') == 'node']
    run_ts = _iso_to_dt(datetime.now(timezone.utc).isoformat())

    client = get_clickhouse_client()

    req_insert = (
        f'INSERT INTO {OBS_TABLES["requests"]} '
        '(request_id, ts, query, intent, customer_name, mode, response_text, final_score, correctness, completeness, relevance, groundedness, '
        'root_cause, judge_mode, tools_called_count, nodes_executed_count, failure_count, tokens_prompt, tokens_completion, '
        'evaluation_json, version_json, release_version, service_version, model_name, env_name, prompt_versions_json, failure_reasons_json, '
        'planner_events_count, planner_success_count, planner_error_count, planner_fix_attempts_sum) VALUES'
    )
    version_info = payload.get('version') if isinstance(payload.get('version'), dict) else {}
    prompt_versions = version_info.get('prompt_versions') if isinstance(version_info.get('prompt_versions'), dict) else {}
    prompt_catalog = version_info.get('prompt_catalog') if isinstance(version_info.get('prompt_catalog'), dict) else {}
    planner_events_count = len(sql_planner_events)
    planner_success_count = sum(1 for e in sql_planner_events if isinstance(e, dict) and e.get('success'))
    planner_error_count = max(planner_events_count - planner_success_count, 0)
    planner_fix_attempts_sum = sum(int((e or {}).get('fix_attempts', 0) or 0) for e in sql_planner_events if isinstance(e, dict))
    req_row = [
        (
            request_id,
            run_ts,
            str(query or ''),
            str(payload.get('intent') or ''),
            customer_name,
            mode,
            str(payload.get('text') or ''),
            float(evaluation.get('final_score', 0.0) or 0.0),
            float(evaluation.get('correctness', 0.0) or 0.0),
            float(evaluation.get('completeness', 0.0) or 0.0),
            float(evaluation.get('relevance', 0.0) or 0.0),
            float(evaluation.get('groundedness', 0.0) or 0.0),
            str(evaluation.get('root_cause') or ''),
            str(evaluation.get('judge_mode') or ''),
            int(len(payload.get('tools_called') or [])),
            int(len(payload.get('nodes_executed') or [])),
            int(len(payload.get('failure_reasons') or [])),
            int((payload.get('token_usage') or {}).get('prompt', 0) or 0),
            int((payload.get('token_usage') or {}).get('completion', 0) or 0),
            json.dumps(evaluation, default=str),
            json.dumps(version_info, default=str),
            str(version_info.get('release_version') or ''),
            str(version_info.get('service_version') or ''),
            str(version_info.get('model_name') or ''),
            str(version_info.get('env_name') or ''),
            json.dumps(prompt_versions, default=str),
            json.dumps(payload.get('failure_reasons') or [], default=str),
            int(planner_events_count),
            int(planner_success_count),
            int(planner_error_count),
            int(planner_fix_attempts_sum),
        )
    ]
    client.execute(req_insert, req_row)

    if node_events:
        node_insert = (
            f'INSERT INTO {OBS_TABLES["nodes"]} '
            '(request_id, ts, node, customer_name, mode, status, duration_sec, row_count, sql_query, metrics_json, '
            'input_summary_json, output_summary_json, failure_reasons_json) VALUES'
        )
        node_rows = []
        for event in node_events:
            node_rows.append(
                (
                    request_id,
                    run_ts,
                    str(event.get('node') or ''),
                    str(event.get('customer_name') or customer_name),
                    str(((event.get('input_summary') or {}).get('context') or {}).get('mode') or mode),
                    str(event.get('status') or 'unknown'),
                    float(event.get('duration', 0.0) or 0.0),
                    int(event.get('row_count', 0) or 0),
                    str(event.get('sql_query') or ''),
                    json.dumps(event.get('metrics') or {}, default=str),
                    json.dumps(event.get('input_summary') or {}, default=str),
                    json.dumps(event.get('output_summary') or {}, default=str),
                    json.dumps(event.get('failure_reasons') or [], default=str),
                )
            )
        client.execute(node_insert, node_rows)

    if sql_events:
        sql_insert = (
            f'INSERT INTO {OBS_TABLES["sql_events"]} '
            '(request_id, ts, node, tool, customer_name, mode, success, row_count, duration_sec, query, error) VALUES'
        )
        sql_rows = []
        for event in sql_events:
            if not isinstance(event, dict):
                continue
            sql_rows.append(
                (
                    request_id,
                    run_ts,
                    str(event.get('node') or ''),
                    str(event.get('tool') or ''),
                    customer_name,
                    mode,
                    int(bool(event.get('success'))),
                    int(event.get('row_count', 0) or 0),
                    float(event.get('duration_sec', 0.0) or 0.0),
                    str(event.get('query') or ''),
                    str(event.get('error') or ''),
                )
            )
        if sql_rows:
            client.execute(sql_insert, sql_rows)

    if sql_planner_events:
        planner_insert = (
            f'INSERT INTO {OBS_TABLES["sql_planner_events"]} '
            '(request_id, ts, tool_call_id, tool, customer_name, mode, generated_by, fix_attempts, success, selected_tables_json, selected_tables_csv, generated_query, error, retry_reasons_json, query) VALUES'
        )
        planner_rows = []
        for event in sql_planner_events:
            if not isinstance(event, dict):
                continue
            selected_tables = event.get('selected_tables') if isinstance(event.get('selected_tables'), list) else []
            planner_rows.append(
                (
                    request_id,
                    run_ts,
                    str(event.get('tool_call_id') or ''),
                    str(event.get('tool') or ''),
                    customer_name,
                    mode,
                    str(event.get('generated_by') or ''),
                    int(event.get('fix_attempts', 0) or 0),
                    int(bool(event.get('success'))),
                    json.dumps(selected_tables, default=str),
                    str(event.get('selected_tables_csv') or ''),
                    str(event.get('generated_query') or ''),
                    str(event.get('error') or ''),
                    json.dumps(event.get('retry_reasons') or [], default=str),
                    str(event.get('query') or ''),
                )
            )
        if planner_rows:
            client.execute(planner_insert, planner_rows)

    node_scores = evaluation.get('node_scores')
    if isinstance(node_scores, dict) and node_scores:
        scores_insert = f'INSERT INTO {OBS_TABLES["node_scores"]} (request_id, ts, node, customer_name, mode, score) VALUES'
        score_rows = []
        for node, score in node_scores.items():
            try:
                value = float(score)
            except Exception:
                continue
            score_rows.append((request_id, run_ts, str(node), customer_name, mode, value))
        if score_rows:
            client.execute(scores_insert, score_rows)

        if isinstance(prompt_versions, dict):
            prompt_insert = (
                f'INSERT INTO {OBS_TABLES["prompt_scores"]} '
                '(request_id, ts, prompt_key, prompt_version, node, customer_name, mode, node_score, final_score, root_cause, intent, env_name, release_version) VALUES'
            )
            prompt_rows = []
            for node, score in node_scores.items():
                prompt_key = _NODE_TO_PROMPT_KEY.get(str(node))
                if not prompt_key:
                    continue
                prompt_version = str(prompt_versions.get(prompt_key) or '')
                try:
                    node_score_value = float(score)
                except Exception:
                    continue
                prompt_rows.append(
                    (
                        request_id,
                        run_ts,
                        prompt_key,
                        prompt_version,
                        str(node),
                        customer_name,
                        mode,
                        node_score_value,
                        float(evaluation.get('final_score', 0.0) or 0.0),
                        str(evaluation.get('root_cause') or ''),
                        str(payload.get('intent') or ''),
                        str(version_info.get('env_name') or ''),
                        str(version_info.get('release_version') or ''),
                    )
                )
            if prompt_rows:
                client.execute(prompt_insert, prompt_rows)

    if isinstance(prompt_versions, dict) and prompt_versions:
        prompt_versions_insert = (
            f'INSERT INTO {OBS_TABLES["prompt_versions"]} '
            '(request_id, ts, prompt_key, prompt_version, customer_name, mode, intent, env_name, release_version, service_version, model_name, git_commit) VALUES'
        )
        prompt_version_rows = []
        for prompt_key, prompt_version in prompt_versions.items():
            prompt_version_rows.append(
                (
                    request_id,
                    run_ts,
                    str(prompt_key),
                    str(prompt_version),
                    customer_name,
                    mode,
                    str(payload.get('intent') or ''),
                    str(version_info.get('env_name') or ''),
                    str(version_info.get('release_version') or ''),
                    str(version_info.get('service_version') or ''),
                    str(version_info.get('model_name') or ''),
                    str(version_info.get('git_commit') or ''),
                )
            )
        if prompt_version_rows:
            client.execute(prompt_versions_insert, prompt_version_rows)

        inventory_rows = []
        for prompt_key, prompt_version in prompt_versions.items():
            key = str(prompt_key)
            version = str(prompt_version)
            if key and version:
                inventory_rows.append((key, str(prompt_catalog.get(key) or ''), version))

        if inventory_rows:
            # Keep inventory table strictly unique by inserting only unseen pairs.
            seen_query_parts = []
            for key, _, version in inventory_rows:
                key_escaped = key.replace("'", "''")
                version_escaped = version.replace("'", "''")
                seen_query_parts.append(f"(prompt_key = '{key_escaped}' AND prompt_version = '{version_escaped}')")

            existing_pairs = set()
            if seen_query_parts:
                existing_rows = client.execute(
                    f"SELECT prompt_key, prompt_version FROM {OBS_TABLES['prompt_inventory']} WHERE " + " OR ".join(seen_query_parts)
                )
                existing_pairs = {(str(r[0]), str(r[1])) for r in existing_rows if r and len(r) >= 2}

            to_insert = [(k, p, v) for (k, p, v) in inventory_rows if (k, v) not in existing_pairs]
            if to_insert:
                client.execute(
                    f"INSERT INTO {OBS_TABLES['prompt_inventory']} (prompt_key, prompt, prompt_version) VALUES",
                    to_insert,
                )


def try_persist_observability_run(*, query: str, payload: dict) -> bool:
    try:
        ensure_observability_tables()
        persist_observability_run(query=query, payload=payload)
        return True
    except Exception as exc:
        log.warning('Observability persistence skipped: %s', exc)
        return False
