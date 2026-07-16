"""Runtime simulated fleet-user for dynamic evaluation follow-ups."""
from __future__ import annotations

import json
import os
from typing import Any

try:
    from langchain_core.messages import HumanMessage, SystemMessage
    from langchain_openai import ChatOpenAI
except ImportError:
    ChatOpenAI = None  # type: ignore[assignment,misc]
    HumanMessage = None  # type: ignore[assignment,misc]
    SystemMessage = None  # type: ignore[assignment,misc]

POLICY_HINTS = {
    'investigate_deeper': 'Ask a natural follow-up that drills deeper into the same investigation (vehicles, DTCs, trends).',
    'maintenance_focus': 'Ask what maintenance should prioritize first and why, in plain operational language.',
    'executive_summary': 'Ask for a concise executive summary of impact, risk, and recommended next steps.',
    'challenge_claim': 'Politely challenge a specific claim from the last answer and ask for evidence or clarification.',
    'memory_test': 'Ask a vague follow-up that assumes prior context ("What about the critical ones?", "And those vehicles?").',
}


def _build_llm() -> Any | None:
    if ChatOpenAI is None or HumanMessage is None or SystemMessage is None:
        return None
    api_key = os.getenv('OPENAI_API_KEY', '').strip()
    if not api_key:
        return None
    return ChatOpenAI(
        model=os.getenv('EVAL_SIMULATOR_MODEL', 'gpt-4o-mini'),
        api_key=api_key,
        temperature=0.4,
        timeout=int(os.getenv('EVAL_SIMULATOR_TIMEOUT_SEC', '30') or '30'),
        model_kwargs={'response_format': {'type': 'json_object'}},
    )


def _fallback_follow_up(policy: str, last_answer: str) -> dict[str, str]:
    snippets = {
        'investigate_deeper': 'Which vehicles are most affected by this?',
        'maintenance_focus': 'What should maintenance prioritize first?',
        'executive_summary': 'Can you summarize the operational impact in one paragraph?',
        'challenge_claim': 'Are you sure about those numbers? What evidence supports that?',
        'memory_test': 'What about the critical ones?',
    }
    return {
        'user_message': snippets.get(policy, 'Can you explain that further?'),
        'follow_up_intent': policy,
        'simulator_mode': 'fallback',
    }


def generate_follow_up(
    *,
    session: dict[str, Any],
    messages: list[dict[str, str]],
    last_answer: str,
    policy: str | None = None,
) -> dict[str, Any]:
    """Generate the next user message based on conversation history."""
    policy = str(policy or session.get('dynamic_policy') or 'investigate_deeper')
    llm = _build_llm()
    if llm is None:
        return _fallback_follow_up(policy, last_answer)

    context = session.get('context') if isinstance(session.get('context'), dict) else {}
    transcript = [
        {'role': m.get('role'), 'content': (m.get('content') or '')[:1500]}
        for m in messages[-8:]
        if m.get('role') in {'user', 'assistant'}
    ]
    payload = {
        'session_id': session.get('session_id'),
        'category': session.get('category'),
        'customer_name': context.get('customer_name'),
        'policy': policy,
        'policy_hint': POLICY_HINTS.get(policy, POLICY_HINTS['investigate_deeper']),
        'conversation': transcript,
        'last_assistant_answer': (last_answer or '')[:2500],
    }
    system = (
        'You simulate a non-technical fleet owner or maintenance manager chatting with a diagnostics AI. '
        'Generate ONE realistic follow-up user message that references specifics from the last assistant answer '
        '(DTC codes, vehicle IDs, counts, systems mentioned). Use plain business language. '
        'Return strict JSON: {"user_message": "...", "follow_up_intent": "..."}'
    )
    try:
        resp = llm.invoke([
            SystemMessage(content=system),
            HumanMessage(content=json.dumps(payload, ensure_ascii=False)),
        ])
        data = json.loads((getattr(resp, 'content', '') or '{}').strip())
        user_message = str(data.get('user_message') or '').strip()
        if not user_message:
            return _fallback_follow_up(policy, last_answer)
        return {
            'user_message': user_message,
            'follow_up_intent': str(data.get('follow_up_intent') or policy),
            'simulator_mode': 'llm',
        }
    except Exception:
        return _fallback_follow_up(policy, last_answer)
