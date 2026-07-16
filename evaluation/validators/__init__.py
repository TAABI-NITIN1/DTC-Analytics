"""Local-only MVP validators for conversational AI evaluation.

This package intentionally has no ClickHouse dependency. Validators consume local
runner records plus in-memory AI responses and emit local artifacts only.
"""

from evaluation.validators.mvp import run_mvp_validation

__all__ = ['run_mvp_validation']
