"""Generate the 1000-session evaluation catalog."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EVAL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')
except ImportError:
    pass

DEFAULT_CUSTOMER = 'VRL LOGISTICS LIMITED'
DEFAULT_VRL_FRACTION = 0.7
TEST_CUSTOMER_MARKERS = ('test', 'iot testing', 'prism_test')
FALLBACK_OTHER_CUSTOMERS = [
    'JAIN FREIGHT CARRIERS',
    'EKLAVYA-HSG-JKY-JV',
    'SPARK CIVIL INFRAPROJECTS',
    'ROHAN BUILDERS(INDIA) PRIVATE LIMIT',
    'Skymix concrete',
    'KKG RMC INFRA PVT LTD',
    'SHRI SHRIDHAR TRANSPORT',
    'KEC INTERNATIONAL LTD',
    'AK CARGO',
    'AETREUM CONCRETE',
]
DYNAMIC_POLICIES = [
    'investigate_deeper',
    'maintenance_focus',
    'executive_summary',
    'challenge_claim',
    'memory_test',
]

SINGLE_TEMPLATES = {
    'Fleet Health': {
        'target_intent': 'fleet_health',
        'required_tools_any': ['get_fleet_health', 'get_fleet_system_health'],
        'evidence_anchors': ['health', 'vehicle', 'fleet'],
        'questions': [
            'What is the overall fleet health status for the last {window}?',
            'Give me a fleet health snapshot for {customer}.',
            'How healthy is our fleet over the past {window}?',
            'Summarize fleet diagnostic health for {customer} this {window}.',
            'What is our fleet health score for the {window}?',
        ],
    },
    'Fleet DTC Distribution': {
        'target_intent': 'fleet_health',
        'required_tools_any': ['get_fleet_dtc_distribution', 'get_fleet_trends'],
        'evidence_anchors': ['dtc', 'fault', 'code'],
        'questions': [
            'What are the top DTC codes affecting our fleet in the last {window}?',
            'Show the most common fault codes across the fleet for {window}.',
            'Which DTCs are most frequent for {customer} recently?',
            'What fault codes dominate our fleet diagnostics this {window}?',
        ],
    },
    'Fleet Trends': {
        'target_intent': 'trend_analysis',
        'required_tools_any': ['get_fleet_trends', 'get_fleet_fault_trends'],
        'evidence_anchors': ['trend', 'increase', 'decrease'],
        'questions': [
            'Show fleet fault trends over the last {window}.',
            'Are active faults increasing or decreasing over {window}?',
            'What is the fleet fault trend for {customer} in the {window}?',
        ],
    },
    'Maintenance Prioritization': {
        'target_intent': 'maintenance_prioritization',
        'required_tools_any': ['get_maintenance_priority'],
        'evidence_anchors': ['priority', 'maintenance', 'critical'],
        'questions': [
            'Which vehicles need maintenance most urgently in the {window}?',
            'What should maintenance prioritize first for {customer}?',
            'Show maintenance priority vehicles for our fleet.',
        ],
    },
    'Vehicle Investigation': {
        'target_intent': 'vehicle_investigation',
        'required_tools_any': ['get_vehicle_health', 'get_vehicle_faults'],
        'evidence_anchors': ['vehicle', 'health', 'fault'],
        'questions': [
            'Which vehicles have the lowest health scores in our fleet?',
            'Show vehicles with the most active faults in the {window}.',
            'List high-risk vehicles for {customer}.',
            'Which vehicles have recurring critical faults?',
        ],
    },
    'DTC Investigation': {
        'target_intent': 'fault_correlation',
        'required_tools_any': ['get_dtc_details', 'get_dtc_fleet_impact'],
        'evidence_anchors': ['dtc', 'impact', 'vehicle'],
        'questions': [
            'What is the fleet impact of DTC {dtc}?',
            'Explain DTC {dtc} and how many vehicles are affected.',
            'Which vehicles are affected by DTC {dtc}?',
            'How severe is DTC {dtc} across our fleet?',
        ],
    },
    'Co-occurrence Analysis': {
        'target_intent': 'fault_correlation',
        'required_tools_any': ['get_dtc_cooccurrence', 'get_dtc_fleet_impact'],
        'evidence_anchors': ['co-occur', 'pattern', 'dtc'],
        'questions': [
            'Which DTCs frequently occur together in our fleet?',
            'Are there recurring DTC pairs we should investigate?',
            'Show co-occurring fault patterns for {customer}.',
        ],
    },
}

STATIC_FLOW_TEMPLATES = [
    {
        'category': 'Fleet Investigation',
        'difficulty_tier': 'Hard',
        'seed': 'Why are {system} faults increasing in our fleet over the {window}?',
        'followups': [
            'Which vehicles are most affected?',
            'What should maintenance prioritize first?',
        ],
        'expectations_seed': {
            'target_intent': 'fleet_health',
            'required_tools_any': ['get_fleet_dtc_distribution', 'get_fleet_trends', 'run_sql'],
            'evidence_anchors': ['dtc', 'vehicles', 'trend'],
        },
    },
    {
        'category': 'Vehicle Diagnostics',
        'difficulty_tier': 'Medium',
        'seed': 'Investigate vehicle {vehicle} and summarize active faults.',
        'followups': [
            'What is the most critical active DTC on that vehicle?',
            'What maintenance action do you recommend?',
        ],
        'expectations_seed': {
            'target_intent': 'vehicle_investigation',
            'required_tools_any': ['get_vehicle_health', 'get_vehicle_faults'],
            'evidence_anchors': ['vehicle', 'dtc', 'active'],
        },
    },
    {
        'category': 'DTC Investigation',
        'difficulty_tier': 'Medium',
        'seed': 'Analyze DTC {dtc} across our fleet.',
        'followups': [
            'Which vehicles have this code active right now?',
            'Is this likely a systemic issue or isolated failures?',
        ],
        'expectations_seed': {
            'target_intent': 'fault_correlation',
            'required_tools_any': ['get_dtc_fleet_impact', 'get_dtc_details'],
            'evidence_anchors': ['dtc', 'vehicles', 'impact'],
        },
    },
    {
        'category': 'Maintenance Prioritization',
        'difficulty_tier': 'Hard',
        'seed': 'What vehicles should maintenance prioritize this week for {customer}?',
        'followups': [
            'Why is the top vehicle ranked highest?',
            'What is the operational risk if we delay repairs?',
        ],
        'expectations_seed': {
            'target_intent': 'maintenance_prioritization',
            'required_tools_any': ['get_maintenance_priority'],
            'evidence_anchors': ['priority', 'critical', 'vehicle'],
        },
    },
]

DYNAMIC_SEED_TEMPLATES = [
    ('Fleet Investigation', 'Why are ABS-related faults increasing in our fleet?', 'investigate_deeper'),
    ('Fleet Investigation', 'Give me an overview of our top recurring chassis faults.', 'investigate_deeper'),
    ('Maintenance Prioritization', 'Which vehicles need urgent maintenance attention?', 'maintenance_focus'),
    ('Executive Summary', 'Summarize our fleet diagnostic health for leadership.', 'executive_summary'),
    ('DTC Investigation', 'What is the operational impact of our most common DTC codes?', 'investigate_deeper'),
    ('Vehicle Diagnostics', 'Which vehicles look highest risk right now?', 'maintenance_focus'),
    ('Co-occurrence Analysis', 'Are there fault patterns that keep repeating together?', 'investigate_deeper'),
    ('Fleet Trends', 'Are critical faults trending up or down recently?', 'challenge_claim'),
    ('Fleet Health', 'How healthy is our fleet overall?', 'executive_summary'),
    ('Memory Test', 'We saw issues with wheel sensor faults — what should I know?', 'memory_test'),
]

WINDOWS = ['7 days', '30 days', '90 days']
SYSTEMS = ['ABS', 'engine', 'emission', 'electrical', 'brake', 'transmission']
DIFFICULTIES = ['Easy', 'Medium', 'Hard']


def git_commit() -> str:
    try:
        return subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'],
            cwd=PROJECT_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return ''


def normalize_key(text: str) -> str:
    return re.sub(r'\s+', ' ', str(text or '').strip().lower())


def question_hash(text: str) -> str:
    return hashlib.sha256(normalize_key(text).encode('utf-8')).hexdigest()[:16]


def unique_message(text: str, seen_hashes: set[str], suffix: str) -> tuple[str, str] | None:
    candidate = text
    h = question_hash(candidate)
    if h not in seen_hashes:
        seen_hashes.add(h)
        return candidate, h
    candidate = f'{text} ({suffix})'
    h = question_hash(candidate)
    if h not in seen_hashes:
        seen_hashes.add(h)
        return candidate, h
    return None


def load_base_questions() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for fname in ['fleet_questions.json', 'vehicle_questions.json', 'dtc_questions.json']:
        path = EVAL_DIR / fname
        if path.exists():
            rows.extend(json.loads(path.read_text(encoding='utf-8')))
    return rows


def load_core_scenarios() -> list[dict[str, Any]]:
    path = EVAL_DIR / 'conversational_scenarios' / 'fleet_diagnostics_core.json'
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding='utf-8'))
    return data.get('scenarios') if isinstance(data.get('scenarios'), list) else []


def fetch_grounding(customer_name: str) -> dict[str, Any]:
    grounding = {'dtc_codes': ['810', '789', '791', '792', '655', '629'], 'systems': SYSTEMS}
    if os.getenv('EVAL_SKIP_CLICKHOUSE_GROUNDING', '').strip().lower() in {'1', 'true', 'yes'}:
        return grounding
    try:
        from src.clickhouse_utils import get_clickhouse_client
        from src.clickhouse_utils_v2 import V2_TABLES
        client = get_clickhouse_client()
        vfm = V2_TABLES['vehicle_fault_master']
        rows = client.execute(
            f"""
            SELECT dtc_code, count() AS c
            FROM {vfm}
            WHERE customer_name = %(cust)s AND is_resolved = 0
            GROUP BY dtc_code
            ORDER BY c DESC
            LIMIT 12
            """,
            {'cust': customer_name},
        )
        if rows:
            grounding['dtc_codes'] = [str(r[0]) for r in rows if r[0]]
    except Exception as exc:
        print(f'  [WARN] ClickHouse grounding skipped: {exc}')
    return grounding


def _is_test_customer(name: str) -> bool:
    lowered = str(name or '').strip().lower()
    return any(marker in lowered for marker in TEST_CUSTOMER_MARKERS)


def fetch_fleet_customers(
    *,
    primary_customer: str,
    min_vehicles: int = 5,
    limit: int = 12,
) -> list[tuple[str, int]]:
    """Return (customer_name, vehicle_count) sorted by fleet size descending."""
    if os.getenv('EVAL_SKIP_CLICKHOUSE_GROUNDING', '').strip().lower() in {'1', 'true', 'yes'}:
        return []
    try:
        from src.clickhouse_utils import get_clickhouse_client
        from src.clickhouse_utils_v2 import V2_TABLES
        vhs = V2_TABLES['vehicle_health_summary']
        rows = get_clickhouse_client().execute(
            f"""
            SELECT customer_name, count() AS n
            FROM {vhs}
            WHERE customer_name != ''
            GROUP BY customer_name
            HAVING n >= %(min_vehicles)s
            ORDER BY n DESC
            LIMIT %(limit)s
            """,
            {'min_vehicles': int(min_vehicles), 'limit': int(limit)},
        )
        return [(str(r[0]), int(r[1])) for r in rows if r[0]]
    except Exception as exc:
        print(f'  [WARN] ClickHouse customer list skipped: {exc}')
        return []


def resolve_customer_pool(
    primary_customer: str,
    *,
    other_customers: list[str] | None = None,
    ground_in_clickhouse: bool = True,
) -> tuple[str, list[str]]:
    primary = (primary_customer or DEFAULT_CUSTOMER).strip() or DEFAULT_CUSTOMER
    others: list[str] = []
    seen = {primary.lower()}

    if other_customers:
        for name in other_customers:
            n = str(name or '').strip()
            if n and n.lower() not in seen and not _is_test_customer(n):
                others.append(n)
                seen.add(n.lower())

    if not others and ground_in_clickhouse:
        for name, _count in fetch_fleet_customers(primary_customer=primary):
            if name.lower() != primary.lower() and not _is_test_customer(name) and name not in others:
                others.append(name)

    if not others:
        for name in FALLBACK_OTHER_CUSTOMERS:
            if name.lower() != primary.lower() and name not in others:
                others.append(name)

    return primary, others


def apply_customer_to_session(session: dict[str, Any], customer_name: str, *, previous_name: str) -> None:
    context = dict(session.get('context') or {})
    context['customer_name'] = customer_name
    session['context'] = context
    if previous_name and previous_name != customer_name:
        if isinstance(session.get('seed_message'), str):
            session['seed_message'] = session['seed_message'].replace(previous_name, customer_name)
        for turn in session.get('static_turns') or []:
            if isinstance(turn, dict) and isinstance(turn.get('user_message'), str):
                turn['user_message'] = turn['user_message'].replace(previous_name, customer_name)


def assign_customer_mix(
    sessions: list[dict[str, Any]],
    *,
    primary_customer: str,
    other_customers: list[str],
    vrl_fraction: float,
    rng: random.Random,
) -> dict[str, Any]:
    if not sessions:
        return {'primary': primary_customer, 'others': other_customers, 'counts': {}}
    if not other_customers:
        for sess in sessions:
            apply_customer_to_session(
                sess, primary_customer,
                previous_name=str((sess.get('context') or {}).get('customer_name') or ''),
            )
        return {
            'primary': primary_customer,
            'others': [],
            'vrl_fraction': 1.0,
            'counts': {primary_customer: len(sessions)},
        }

    vrl_fraction = max(0.0, min(1.0, float(vrl_fraction)))
    vrl_target = int(round(len(sessions) * vrl_fraction))
    indices = list(range(len(sessions)))
    rng.shuffle(indices)
    vrl_indices = set(indices[:vrl_target])
    counts: dict[str, int] = {primary_customer: 0}
    for other in other_customers:
        counts[other] = 0

    for idx, sess in enumerate(sessions):
        prev = str((sess.get('context') or {}).get('customer_name') or primary_customer)
        if idx in vrl_indices:
            cust = primary_customer
        else:
            cust = rng.choice(other_customers)
        apply_customer_to_session(sess, cust, previous_name=prev)
        counts[cust] = counts.get(cust, 0) + 1

    return {
        'primary': primary_customer,
        'others': other_customers,
        'vrl_fraction': vrl_fraction,
        'vrl_target': vrl_target,
        'counts': counts,
    }


def make_single_session(
    *,
    session_id: str,
    category: str,
    seed_message: str,
    customer_name: str,
    expectations: dict[str, Any],
    difficulty_tier: str = 'Medium',
    source: str = 'generated',
) -> dict[str, Any]:
    mode = 'fleet'
    if category.startswith('Vehicle'):
        mode = 'vehicle'
    elif category.startswith('DTC') or 'Co-occurrence' in category:
        mode = 'general'
    return {
        'session_id': session_id,
        'session_type': 'single',
        'category': category,
        'difficulty_tier': difficulty_tier,
        'source': source,
        'context': {'customer_name': customer_name, 'mode': mode},
        'seed_message': seed_message,
        'static_turns': [],
        'dynamic_policy': None,
        'max_turns': 1,
        'expectations': expectations,
    }


def scenario_to_static_session(scenario: dict[str, Any], customer_name: str) -> dict[str, Any]:
    turns = scenario.get('turns') if isinstance(scenario.get('turns'), list) else []
    if not turns:
        raise ValueError('scenario has no turns')
    seed = turns[0]
    static_turns = []
    for idx, turn in enumerate(turns[1:], start=2):
        static_turns.append({
            'turn_id': turn.get('turn_id', idx),
            'user_message': str(turn.get('user_message') or turn.get('message') or ''),
            'expectations': turn.get('expectations') if isinstance(turn.get('expectations'), dict) else {},
            'policies': turn.get('policies') if isinstance(turn.get('policies'), dict) else {},
        })
    context = dict(scenario.get('context') or {})
    if not context.get('customer_name'):
        context['customer_name'] = customer_name
    return {
        'session_id': f"sess_static_{scenario.get('scenario_id', 'unknown')}",
        'session_type': 'static_multi',
        'category': scenario.get('category', 'Fleet Investigation'),
        'difficulty_tier': scenario.get('difficulty_tier', 'Medium'),
        'source': 'fleet_diagnostics_core',
        'context': context,
        'seed_message': str(seed.get('user_message') or seed.get('message') or ''),
        'static_turns': static_turns,
        'dynamic_policy': None,
        'max_turns': len(turns),
        'expectations': seed.get('expectations') if isinstance(seed.get('expectations'), dict) else {},
    }


def expand_single_sessions(
    *,
    count: int,
    customer_name: str,
    grounding: dict[str, Any],
    seen_hashes: set[str],
    rng: random.Random,
) -> list[dict[str, Any]]:
    sessions: list[dict[str, Any]] = []
    base_questions = load_base_questions()
    for q in base_questions:
        msg = str(q.get('question') or '')
        h = question_hash(msg)
        if h in seen_hashes:
            continue
        seen_hashes.add(h)
        category = 'Fleet Health'
        if str(q.get('id', '')).startswith('veh_'):
            category = 'Vehicle Investigation'
        elif str(q.get('id', '')).startswith('dtc_'):
            category = 'DTC Investigation'
        sessions.append(make_single_session(
            session_id=f"sess_single_{q.get('id', len(sessions)+1)}",
            category=category,
            seed_message=msg,
            customer_name=customer_name,
            expectations={
                'target_intent': q.get('expected_intent', ''),
                'required_tools_any': q.get('expected_tools', []),
                'evidence_anchors': q.get('expected_output_contains', []),
            },
            difficulty_tier='Medium',
            source='base60',
        ))
        if len(sessions) >= count:
            return sessions[:count]

    idx = len(sessions)
    attempts = 0
    max_attempts = max(count * 50, 5000)
    while len(sessions) < count and attempts < max_attempts:
        attempts += 1
        category, meta = rng.choice(list(SINGLE_TEMPLATES.items()))
        template = rng.choice(meta['questions'])
        fmt = {
            'window': rng.choice(WINDOWS),
            'customer': customer_name,
            'dtc': rng.choice(grounding['dtc_codes']),
            'system': rng.choice(SYSTEMS),
        }
        base_msg = template.format(**fmt)
        unique = unique_message(base_msg, seen_hashes, f'variant {idx + 1}')
        if unique is None:
            continue
        msg, _ = unique
        idx += 1
        sessions.append(make_single_session(
            session_id=f'sess_single_gen_{idx:04d}',
            category=category,
            seed_message=msg,
            customer_name=customer_name,
            expectations={
                'target_intent': meta['target_intent'],
                'required_tools_any': meta['required_tools_any'],
                'evidence_anchors': meta['evidence_anchors'],
            },
            difficulty_tier=rng.choice(DIFFICULTIES),
            source='template',
        ))
    return sessions


def expand_static_multi_sessions(
    *,
    count: int,
    customer_name: str,
    grounding: dict[str, Any],
    seen_hashes: set[str],
    rng: random.Random,
) -> list[dict[str, Any]]:
    sessions: list[dict[str, Any]] = []
    for scenario in load_core_scenarios():
        try:
            sess = scenario_to_static_session(scenario, customer_name)
        except ValueError:
            continue
        h = question_hash(sess['seed_message'])
        if h in seen_hashes:
            continue
        seen_hashes.add(h)
        sessions.append(sess)
        if len(sessions) >= count:
            return sessions[:count]

    idx = len(sessions)
    vehicles = ['HR37E0357_OBD', 'KA63A0129_OBD', 'MH12AB1234_OBD', 'GJ01CD5678_OBD']
    attempts = 0
    max_attempts = max(count * 50, 5000)
    while len(sessions) < count and attempts < max_attempts:
        attempts += 1
        flow = rng.choice(STATIC_FLOW_TEMPLATES)
        fmt = {
            'window': rng.choice(WINDOWS),
            'customer': customer_name,
            'dtc': rng.choice(grounding['dtc_codes']),
            'system': rng.choice(SYSTEMS),
            'vehicle': rng.choice(vehicles),
        }
        seed_msg = flow['seed'].format(**fmt)
        unique = unique_message(seed_msg + '|' + flow['category'], seen_hashes, f'flow {idx + 1}')
        if unique is None:
            continue
        seed_msg, _ = unique
        idx += 1
        static_turns = []
        for t_idx, follow in enumerate(flow['followups'], start=2):
            static_turns.append({
                'turn_id': t_idx,
                'user_message': follow,
                'expectations': {
                    'target_intent': flow['expectations_seed'].get('target_intent', ''),
                    'required_tools_any': flow['expectations_seed'].get('required_tools_any', []),
                    'evidence_anchors': flow['expectations_seed'].get('evidence_anchors', []),
                },
                'policies': {'must_scope_customer': True},
            })
        sessions.append({
            'session_id': f'sess_static_gen_{idx:04d}',
            'session_type': 'static_multi',
            'category': flow['category'],
            'difficulty_tier': flow['difficulty_tier'],
            'source': 'template_flow',
            'context': {'customer_name': customer_name, 'mode': 'fleet'},
            'seed_message': seed_msg,
            'static_turns': static_turns,
            'dynamic_policy': None,
            'max_turns': 1 + len(static_turns),
            'expectations': flow['expectations_seed'],
        })
    return sessions


def expand_dynamic_sessions(
    *,
    count: int,
    customer_name: str,
    grounding: dict[str, Any],
    seen_hashes: set[str],
    rng: random.Random,
) -> list[dict[str, Any]]:
    sessions: list[dict[str, Any]] = []
    idx = 0
    attempts = 0
    max_attempts = max(count * 50, 2000)
    while len(sessions) < count and attempts < max_attempts:
        attempts += 1
        if idx < len(DYNAMIC_SEED_TEMPLATES):
            category, seed, policy = DYNAMIC_SEED_TEMPLATES[idx]
        else:
            category = rng.choice(list(SINGLE_TEMPLATES.keys()))
            policy = rng.choice(DYNAMIC_POLICIES)
            seed = rng.choice(SINGLE_TEMPLATES[category]['questions']).format(
                window=rng.choice(WINDOWS),
                customer=customer_name,
                dtc=rng.choice(grounding['dtc_codes']),
                system=rng.choice(SYSTEMS),
            )
        idx += 1
        unique = unique_message(seed + '|' + policy, seen_hashes, f'dynamic {idx}')
        if unique is None:
            continue
        seed, _ = unique
        max_turns = rng.choice([3, 4, 5])
        meta = SINGLE_TEMPLATES.get(category, SINGLE_TEMPLATES['Fleet Health'])
        sessions.append({
            'session_id': f'sess_dynamic_{idx:04d}',
            'session_type': 'dynamic_multi',
            'category': category,
            'difficulty_tier': rng.choice(['Medium', 'Hard', 'Expert']),
            'source': 'dynamic_seed',
            'context': {'customer_name': customer_name, 'mode': 'fleet'},
            'seed_message': seed,
            'static_turns': [],
            'dynamic_policy': policy,
            'max_turns': max_turns,
            'expectations': {
                'target_intent': meta['target_intent'],
                'required_tools_any': meta['required_tools_any'],
                'evidence_anchors': meta['evidence_anchors'],
            },
        })
    return sessions


def generate_catalog(
    *,
    single_count: int,
    static_multi_count: int,
    dynamic_multi_count: int,
    customer_name: str,
    ground_in_clickhouse: bool,
    seed: int,
    vrl_fraction: float = DEFAULT_VRL_FRACTION,
    other_customers: list[str] | None = None,
) -> dict[str, Any]:
    rng = random.Random(seed)
    seen_hashes: set[str] = set()
    primary_customer, pool_others = resolve_customer_pool(
        customer_name,
        other_customers=other_customers,
        ground_in_clickhouse=ground_in_clickhouse,
    )
    if ground_in_clickhouse:
        grounding = fetch_grounding(primary_customer)
    else:
        grounding = {'dtc_codes': ['810', '789', '791', '792', '655', '629'], 'systems': SYSTEMS}

    singles = expand_single_sessions(
        count=single_count,
        customer_name=primary_customer,
        grounding=grounding,
        seen_hashes=seen_hashes,
        rng=rng,
    )
    static_multi = expand_static_multi_sessions(
        count=static_multi_count,
        customer_name=primary_customer,
        grounding=grounding,
        seen_hashes=seen_hashes,
        rng=rng,
    )
    dynamic_multi = expand_dynamic_sessions(
        count=dynamic_multi_count,
        customer_name=primary_customer,
        grounding=grounding,
        seen_hashes=seen_hashes,
        rng=rng,
    )
    sessions = singles + static_multi + dynamic_multi
    customer_distribution = assign_customer_mix(
        sessions,
        primary_customer=primary_customer,
        other_customers=pool_others,
        vrl_fraction=vrl_fraction,
        rng=random.Random(seed + 999),
    )
    return {
        'catalog_id': 'sessions_1000',
        'catalog_version': 'v1',
        'dataset_version': os.getenv('AI_ANALYST_DATASET_VERSION', 'ravi_v2'),
        'default_customer_name': primary_customer,
        'customer_distribution': customer_distribution,
        'session_counts': {
            'single': len(singles),
            'static_multi': len(static_multi),
            'dynamic_multi': len(dynamic_multi),
            'total': len(sessions),
        },
        'grounding': grounding,
        'sessions': sessions,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Generate 1000-session evaluation catalog.')
    parser.add_argument('--output', default=str(EVAL_DIR / 'conversational_scenarios' / 'sessions_1000.json'))
    parser.add_argument('--single-count', type=int, default=int(os.getenv('EVAL_SINGLE_COUNT', '650') or '650'))
    parser.add_argument('--static-multi-count', type=int, default=int(os.getenv('EVAL_STATIC_MULTI_COUNT', '250') or '250'))
    parser.add_argument('--dynamic-multi-count', type=int, default=int(os.getenv('EVAL_DYNAMIC_MULTI_COUNT', '100') or '100'))
    parser.add_argument('--customer-name', default=os.getenv('EVAL_CUSTOMER_NAME', DEFAULT_CUSTOMER))
    parser.add_argument('--vrl-fraction', type=float, default=float(os.getenv('EVAL_VRL_FRACTION', str(DEFAULT_VRL_FRACTION)) or str(DEFAULT_VRL_FRACTION)))
    parser.add_argument('--other-customers', default=os.getenv('EVAL_OTHER_CUSTOMERS', ''), help='Comma-separated non-VRL customers')
    parser.add_argument('--ground-in-clickhouse', action='store_true', default=True)
    parser.add_argument('--no-ground-in-clickhouse', dest='ground_in_clickhouse', action='store_false')
    parser.add_argument('--seed', type=int, default=int(os.getenv('EVAL_CATALOG_SEED', '42') or '42'))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    other_customers = [x.strip() for x in str(args.other_customers or '').split(',') if x.strip()]
    catalog = generate_catalog(
        single_count=args.single_count,
        static_multi_count=args.static_multi_count,
        dynamic_multi_count=args.dynamic_multi_count,
        customer_name=args.customer_name,
        ground_in_clickhouse=args.ground_in_clickhouse,
        seed=args.seed,
        vrl_fraction=args.vrl_fraction,
        other_customers=other_customers or None,
    )
    output = Path(args.output)
    if not output.is_absolute():
        output = PROJECT_ROOT / output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(catalog, indent=2, ensure_ascii=False), encoding='utf-8')

    manifest = {
        'catalog_id': catalog['catalog_id'],
        'catalog_version': catalog['catalog_version'],
        'output': str(output),
        'session_counts': catalog['session_counts'],
        'git_commit': git_commit(),
        'generator_model': os.getenv('EVAL_GENERATOR_MODEL', 'template-only'),
        'seed': args.seed,
    }
    manifest_path = output.with_name('catalog_manifest.json')
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(f"Generated {catalog['session_counts']['total']} sessions -> {output}")
    print(f"  single={catalog['session_counts']['single']} static_multi={catalog['session_counts']['static_multi']} dynamic_multi={catalog['session_counts']['dynamic_multi']}")
    dist = catalog.get('customer_distribution') or {}
    if dist.get('counts'):
        vrl_n = dist['counts'].get(catalog['default_customer_name'], 0)
        total = catalog['session_counts']['total']
        print(f"  customers: VRL={vrl_n}/{total} ({round(100*vrl_n/total,1)}%) others={total-vrl_n}")
        for name, count in sorted(dist['counts'].items(), key=lambda x: -x[1])[:8]:
            print(f"    {name}: {count}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
