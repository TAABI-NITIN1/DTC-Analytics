import json
from datetime import datetime, timezone
from uuid import uuid4

from src.clickhouse_utils import get_clickhouse_client


CONVERSATION_TABLE = 'ai_chat_conversations'
MESSAGE_TABLE = 'ai_chat_messages'


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _clean_text(value) -> str:
    return str(value or '').strip()


def _normalize_customer(value) -> str:
    return _clean_text(value).lower()


def ensure_conversation_tables() -> None:
    client = get_clickhouse_client()
    client.execute(
        f'''
        CREATE TABLE IF NOT EXISTS {CONVERSATION_TABLE} (
            conversation_id String,
            customer_name String,
            mode String,
            created_ts DateTime,
            context_json String
        )
        ENGINE = MergeTree
        ORDER BY (customer_name, created_ts, conversation_id)
        '''
    )
    client.execute(f"ALTER TABLE {CONVERSATION_TABLE} ADD COLUMN IF NOT EXISTS customer_name String")
    client.execute(f"ALTER TABLE {CONVERSATION_TABLE} ADD COLUMN IF NOT EXISTS mode String")
    client.execute(f"ALTER TABLE {CONVERSATION_TABLE} ADD COLUMN IF NOT EXISTS created_ts DateTime")
    client.execute(f"ALTER TABLE {CONVERSATION_TABLE} ADD COLUMN IF NOT EXISTS context_json String")

    client.execute(
        f'''
        CREATE TABLE IF NOT EXISTS {MESSAGE_TABLE} (
            conversation_id String,
            message_index UInt32,
            role String,
            content String,
            ts DateTime,
            request_id String,
            intent String,
            metadata_json String
        )
        ENGINE = MergeTree
        ORDER BY (conversation_id, message_index, ts)
        '''
    )
    client.execute(f"ALTER TABLE {MESSAGE_TABLE} ADD COLUMN IF NOT EXISTS request_id String")
    client.execute(f"ALTER TABLE {MESSAGE_TABLE} ADD COLUMN IF NOT EXISTS intent String")
    client.execute(f"ALTER TABLE {MESSAGE_TABLE} ADD COLUMN IF NOT EXISTS metadata_json String")


def get_conversation(conversation_id: str) -> dict | None:
    cid = _clean_text(conversation_id)
    if not cid:
        return None

    client = get_clickhouse_client()
    rows = client.execute(
        f'''
        SELECT conversation_id, customer_name, mode, created_ts, context_json
        FROM {CONVERSATION_TABLE}
        WHERE conversation_id = %(conversation_id)s
        ORDER BY created_ts DESC
        LIMIT 1
        ''',
        {'conversation_id': cid},
    )
    if not rows:
        return None

    row = rows[0]
    context_json = row[4] if len(row) > 4 else ''
    context = {}
    if context_json:
        try:
            context = json.loads(context_json)
        except Exception:
            context = {}

    return {
        'conversation_id': str(row[0] or ''),
        'customer_name': str(row[1] or ''),
        'mode': str(row[2] or ''),
        'created_ts': row[3],
        'context': context,
    }


def ensure_conversation(*, conversation_id: str | None, customer_name: str, mode: str, context: dict | None) -> str:
    cid = _clean_text(conversation_id) or str(uuid4())
    existing = get_conversation(cid)
    if existing is not None:
        return cid

    clean_customer = _clean_text(customer_name)
    clean_mode = _clean_text(mode) or 'general'
    context_json = json.dumps(context or {}, default=str)

    client = get_clickhouse_client()
    client.execute(
        f'INSERT INTO {CONVERSATION_TABLE} (conversation_id, customer_name, mode, created_ts, context_json) VALUES',
        [
            (
                cid,
                clean_customer,
                clean_mode,
                _utc_now_naive(),
                context_json,
            )
        ],
    )
    return cid


def validate_conversation_customer_scope(conversation_id: str, customer_name: str | None) -> None:
    cid = _clean_text(conversation_id)
    if not cid:
        return
    existing = get_conversation(cid)
    if existing is None:
        return

    existing_customer = _normalize_customer(existing.get('customer_name'))
    request_customer = _normalize_customer(customer_name)
    if existing_customer and request_customer and existing_customer != request_customer:
        raise ValueError('conversation_id does not belong to the requested customer scope')


def get_recent_conversation_messages(conversation_id: str, limit: int = 12) -> list[dict]:
    cid = _clean_text(conversation_id)
    if not cid:
        return []

    safe_limit = max(1, min(int(limit or 12), 100))
    client = get_clickhouse_client()
    rows = client.execute(
        f'''
        SELECT message_index, role, content, ts
        FROM {MESSAGE_TABLE}
        WHERE conversation_id = %(conversation_id)s
        ORDER BY message_index DESC
        LIMIT %(limit)s
        ''',
        {
            'conversation_id': cid,
            'limit': safe_limit,
        },
    )

    ordered = list(reversed(rows))
    return [
        {
            'message_index': int(row[0] or 0),
            'role': str(row[1] or ''),
            'content': str(row[2] or ''),
            'ts': row[3],
        }
        for row in ordered
    ]


def _next_message_index(conversation_id: str) -> int:
    client = get_clickhouse_client()
    rows = client.execute(
        f'''
        SELECT max(message_index)
        FROM {MESSAGE_TABLE}
        WHERE conversation_id = %(conversation_id)s
        ''',
        {'conversation_id': conversation_id},
    )
    if not rows:
        return 0
    value = rows[0][0]
    if value is None:
        return 0
    try:
        return int(value) + 1
    except Exception:
        return 0


def append_conversation_message(
    *,
    conversation_id: str,
    role: str,
    content: str,
    request_id: str = '',
    intent: str = '',
    metadata: dict | None = None,
) -> None:
    cid = _clean_text(conversation_id)
    if not cid:
        return

    clean_role = _clean_text(role)
    clean_content = str(content or '')
    if not clean_role:
        return

    client = get_clickhouse_client()
    message_index = _next_message_index(cid)
    metadata_json = json.dumps(metadata or {}, default=str)
    client.execute(
        f'''
        INSERT INTO {MESSAGE_TABLE}
        (conversation_id, message_index, role, content, ts, request_id, intent, metadata_json)
        VALUES
        ''',
        [
            (
                cid,
                int(message_index),
                clean_role,
                clean_content,
                _utc_now_naive(),
                _clean_text(request_id),
                _clean_text(intent),
                metadata_json,
            )
        ],
    )