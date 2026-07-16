"""Non-secret deployed-schema compatibility check for approved DTC MCP tables."""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.clickhouse_utils import get_clickhouse_client
from src.config import load_env
from src.dtc_mcp.catalog import APPROVED_COLUMNS
from src.dtc_mcp.schema_repository import SchemaRepository


def main() -> int:
    load_env(PROJECT_ROOT / ".env")
    try:
        client = get_clickhouse_client()
        deployed = {
            table: {str(row[0]) for row in client.execute(f"DESCRIBE TABLE {table}")}
            for table in APPROVED_COLUMNS
        }
        missing = SchemaRepository().compatibility_check(deployed)
    except Exception:
        print("DTC MCP schema compatibility check failed without exposing connection details.", file=sys.stderr)
        return 2
    if missing:
        for table, columns in sorted(missing.items()):
            print(f"MISMATCH {table}: missing {','.join(columns)}")
        return 1
    print(f"PASS: {len(deployed)} approved DTC analytics tables are compatible.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
