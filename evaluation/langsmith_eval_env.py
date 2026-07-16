"""LangSmith settings for local evaluation runners.

Traces are created on the API server (e.g. production VM). The eval client should
not duplicate-ingest runs locally (that causes 429 noise and can abort PowerShell).
"""
from __future__ import annotations

import logging
import os
import sys


def configure_eval_langsmith(*, tracing_on: bool = True) -> None:
    if tracing_on:
        os.environ['LANGSMITH_TRACING'] = 'true'
    # Prevent the eval runner process from posting duplicate traces to LangSmith.
    os.environ['LANGCHAIN_TRACING_V2'] = 'false'
    os.environ.setdefault('LANGCHAIN_TRACING', 'false')

    for name in ('langsmith', 'langsmith.client', 'langchain', 'langchain_core'):
        logging.getLogger(name).setLevel(logging.ERROR)

    # LangSmith may log rate-limit errors to stderr; keep them off the console.
    if hasattr(sys, 'stderr') and hasattr(sys.stderr, 'reconfigure'):
        try:
            sys.stderr.reconfigure(encoding='utf-8', errors='replace')
        except Exception:
            pass
