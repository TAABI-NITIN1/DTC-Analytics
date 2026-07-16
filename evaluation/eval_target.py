"""Resolve evaluation target: production VM backend vs local dev backend."""
from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

PRODUCTION_HOSTS = {
    '4.224.101.147',
}

LOCAL_HOSTS = {
    '127.0.0.1',
    'localhost',
    '0.0.0.0',
}


def resolve_eval_target(api_base_url: str) -> dict[str, Any]:
    explicit = (os.getenv('EVAL_ENVIRONMENT') or os.getenv('DEPLOYMENT_ENV') or '').strip().lower()
    url = (api_base_url or os.getenv('EVAL_API_BASE_URL') or 'http://127.0.0.1:8005').strip().rstrip('/')
    parsed = urlparse(url if '://' in url else f'http://{url}')
    host = (parsed.hostname or '').lower()

    if explicit in {'production', 'prod', 'staging', 'local', 'dev'}:
        environment = 'production' if explicit in {'production', 'prod', 'staging'} else 'local'
    elif host in PRODUCTION_HOSTS:
        environment = 'production'
    elif host in LOCAL_HOSTS:
        environment = 'local'
    else:
        environment = 'production' if host and not host.startswith('192.168.') else 'local'

    return {
        'eval_environment': environment,
        'api_base_url': url,
        'backend_host': host or 'unknown',
        'is_production': environment == 'production',
    }


def target_fields(api_base_url: str) -> dict[str, Any]:
    target = resolve_eval_target(api_base_url)
    return {
        'eval_environment': target['eval_environment'],
        'api_base_url': target['api_base_url'],
        'backend_host': target['backend_host'],
        'is_production': target['is_production'],
    }
