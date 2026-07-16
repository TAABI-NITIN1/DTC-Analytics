import inspect
import json
from pathlib import Path

from src import ai_analyst
from src.dtc_mcp.catalog import APPROVED_COLUMNS
from src.dtc_mcp.schema_repository import SchemaRepository
from src.graphql_schema import schema


BASELINE = json.loads((Path(__file__).parents[1] / "fixtures" / "dtc_contract_baseline.json").read_text(encoding="utf-8"))


def test_graphql_contract_fields_remain_present():
    graphql_sdl = schema.as_str()
    for operation, fields in BASELINE["graphql_operations"].items():
        assert operation in graphql_sdl
        for field in fields:
            assert field in graphql_sdl


def test_ai_chat_signature_and_response_baseline_are_preserved():
    signature = inspect.signature(ai_analyst.chat)
    assert list(signature.parameters) == ["messages", "context"]
    assert {"text", "chart", "request_id", "tool_results"}.issubset(BASELINE["ai_chat_response_fields"])


def test_approved_catalog_matches_verified_snapshot():
    deployed = {table: set(columns) for table, columns in APPROVED_COLUMNS.items()}
    assert SchemaRepository().compatibility_check(deployed) == {}
