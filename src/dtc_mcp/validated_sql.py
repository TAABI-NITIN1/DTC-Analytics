from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import sqlglot
from pydantic import Field
from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope

from src.dtc_mcp.catalog import APPROVED_COLUMNS
from src.dtc_mcp.config import DTCSettings
from src.dtc_mcp.models import ErrorCode, RepositoryResult, StrictModel, TenantContext
from src.dtc_mcp.repository import RepositoryError, RepositoryExecutor


class ValidatedSQLInput(StrictModel):
    question_or_reason: str = Field(min_length=8, max_length=500)
    sql: str = Field(min_length=8, max_length=20_000)
    maximum_rows: int | None = Field(default=None, ge=1, le=200)


@dataclass(frozen=True)
class ValidatedQuery:
    sql: str
    parameters: dict[str, Any]
    tables: tuple[str, ...]
    limit: int


class SQLPolicy:
    FORBIDDEN_FUNCTIONS = {"file", "url", "remote", "remotesecure", "executable", "executablepool", "dictionary", "dictget", "mysql", "postgresql", "odbc", "jdbc", "hdfs", "s3", "azureblobstorage"}
    ALLOWED_FUNCTIONS = {"count", "countif", "uniq", "uniqexact", "uniqexactif", "sum", "sumif", "avg", "avgif", "min", "max", "round", "toint32", "toint64", "touint32", "touint64", "tofloat64", "tostring", "todate", "todatetime", "coalesce", "if", "multiif", "lower", "upper", "length", "date_diff", "datediff", "now", "today", "current_date", "current_timestamp", "arrayjoin", "grouparray", "argmax", "argmin", "any", "anylast"}
    DOMAIN_HINTS = {"fleet health", "top dtc", "fault trend", "vehicle health", "vehicle fault", "fleet impact", "cooccurrence", "maintenance priority"}

    def __init__(self, settings: DTCSettings):
        self.settings = settings

    def validate(self, values: ValidatedSQLInput, context: TenantContext) -> ValidatedQuery:
        if not self.settings.dynamic_sql_enabled:
            raise RepositoryError(ErrorCode.FORBIDDEN, "Validated SQL is disabled")
        reason = values.question_or_reason.lower()
        if any(hint in reason for hint in self.DOMAIN_HINTS):
            raise RepositoryError(ErrorCode.QUERY_REJECTED, "Use the matching domain tool instead of dynamic SQL")
        if any(marker in values.sql for marker in ("--", "/*", "*/", "#")):
            raise RepositoryError(ErrorCode.QUERY_REJECTED, "SQL comments are not permitted")
        try:
            statements = sqlglot.parse(values.sql, read="clickhouse")
        except Exception as exc:
            raise RepositoryError(ErrorCode.QUERY_REJECTED, "SQL could not be parsed safely") from exc
        if len(statements) != 1 or not isinstance(statements[0], exp.Select):
            raise RepositoryError(ErrorCode.QUERY_REJECTED, "Exactly one read-only SELECT or WITH query is required")
        expression = statements[0]
        forbidden_nodes = {"Insert", "Update", "Delete", "Create", "Drop", "Alter", "TruncateTable", "Command", "Grant", "Attach", "Detach", "Merge"}
        if any(type(node).__name__ in forbidden_nodes for node in expression.walk()):
            raise RepositoryError(ErrorCode.QUERY_REJECTED, "Mutation and DDL constructs are not permitted")
        if expression.find(exp.Union) or expression.find(exp.Intersect) or expression.find(exp.Except):
            raise RepositoryError(ErrorCode.QUERY_REJECTED, "Set operations are not permitted")
        if len(list(expression.find_all(exp.Join))) > 3 or any(str(join.args.get("kind") or "").upper() == "CROSS" for join in expression.find_all(exp.Join)):
            raise RepositoryError(ErrorCode.QUERY_REJECTED, "Join policy exceeded")
        if len(list(expression.find_all(exp.Subquery))) > 3:
            raise RepositoryError(ErrorCode.QUERY_REJECTED, "Subquery depth policy exceeded")
        if expression.find(exp.Star):
            raise RepositoryError(ErrorCode.QUERY_REJECTED, "Wildcard columns are not permitted")

        cte_names = {cte.alias for cte in expression.find_all(exp.CTE) if cte.alias}
        tables: list[str] = []
        for table in expression.find_all(exp.Table):
            name = table.name
            if name in cte_names:
                continue
            if not name or name.lower() == "system" or (table.db and table.db.lower() == "system"):
                raise RepositoryError(ErrorCode.QUERY_REJECTED, "System and table-function sources are not permitted")
            if name not in APPROVED_COLUMNS:
                raise RepositoryError(ErrorCode.QUERY_REJECTED, "SQL references an unapproved table")
            if table.db and self.settings.allowed_database and table.db != self.settings.allowed_database:
                raise RepositoryError(ErrorCode.QUERY_REJECTED, "SQL references an unapproved database")
            tables.append(name)
        if not tables:
            raise RepositoryError(ErrorCode.QUERY_REJECTED, "SQL must reference an approved table")

        allowed_columns = set().union(*(APPROVED_COLUMNS[table] for table in tables))
        aliases = {alias.alias for alias in expression.find_all(exp.Alias) if alias.alias}
        for column in expression.find_all(exp.Column):
            if column.name in {"clientLoginId", "customer_name", "tenant_id"}:
                raise RepositoryError(ErrorCode.SCOPE_VIOLATION, "Tenant predicates are server controlled")
            if column.name not in allowed_columns and column.name not in aliases:
                raise RepositoryError(ErrorCode.QUERY_REJECTED, "SQL references an unapproved column")
        for function in expression.find_all(exp.Func):
            name = function.sql_name().lower()
            normalized_name = name.replace("_", "")
            if normalized_name in self.FORBIDDEN_FUNCTIONS or (name not in self.ALLOWED_FUNCTIONS and not name.startswith("anonymous")):
                raise RepositoryError(ErrorCode.QUERY_REJECTED, "SQL references an unapproved function")
            if isinstance(function, exp.Anonymous) and function.name.lower() not in self.ALLOWED_FUNCTIONS:
                raise RepositoryError(ErrorCode.QUERY_REJECTED, "SQL references an unapproved function")

        for subtraction in expression.find_all(exp.Sub):
            right = subtraction.expression
            if isinstance(right, exp.Literal) and not right.is_string:
                try:
                    if int(right.this) > self.settings.max_lookback_days:
                        raise RepositoryError(ErrorCode.QUERY_REJECTED, "Requested lookback exceeds the configured maximum")
                except ValueError:
                    pass
        for between in expression.find_all(exp.Between):
            if isinstance(between.this, exp.Column) and between.this.name.lower() in {"date", "event_date", "event_date_ist", "ts", "first_ts", "last_ts"}:
                low, high = between.args.get("low"), between.args.get("high")
                if isinstance(low, exp.Literal) and isinstance(high, exp.Literal) and low.is_string and high.is_string:
                    try:
                        if (date.fromisoformat(high.this) - date.fromisoformat(low.this)).days > self.settings.max_lookback_days:
                            raise RepositoryError(ErrorCode.QUERY_REJECTED, "Requested lookback exceeds the configured maximum")
                    except ValueError:
                        raise RepositoryError(ErrorCode.QUERY_REJECTED, "Date bounds must be ISO dates")
        original_where_count = len(list(expression.find_all(exp.Where)))
        if original_where_count == 0:
            raise RepositoryError(ErrorCode.QUERY_REJECTED, "Dynamic SQL requires a meaningful server-verifiable constraint")
        scalar_aggregate = bool(expression.find(exp.AggFunc)) and not expression.args.get("group")
        if not expression.args.get("order") and not scalar_aggregate:
            raise RepositoryError(ErrorCode.QUERY_REJECTED, "Dynamic SQL result sets require deterministic ordering")

        tenant_tables = {table for table in tables if "clientLoginId" in APPROVED_COLUMNS[table]}
        for scope in traverse_scope(expression):
            predicates = []
            for alias, (_node, source) in scope.selected_sources.items():
                if isinstance(source, exp.Table) and source.name in tenant_tables:
                    predicates.append(sqlglot.parse_one(f"{alias}.clientLoginId IN {{tenant_ids:Array(String)}}", read="clickhouse"))
            if predicates:
                predicate = predicates[0]
                for extra in predicates[1:]:
                    predicate = exp.and_(predicate, extra)
                scope.expression.where(predicate, append=True, copy=False)

        limit = min(values.maximum_rows or self.settings.max_result_rows, self.settings.max_result_rows)
        expression.set("limit", exp.Limit(expression=exp.Literal.number(limit + 1)))
        final_sql = expression.sql(dialect="clickhouse")
        reparsed = sqlglot.parse_one(final_sql, read="clickhouse")
        if not isinstance(reparsed, exp.Select):
            raise RepositoryError(ErrorCode.QUERY_REJECTED, "Final executable query failed validation")
        for scope in traverse_scope(reparsed):
            tenant_aliases = {alias for alias, (_node, source) in scope.selected_sources.items() if isinstance(source, exp.Table) and source.name in tenant_tables}
            scoped_aliases = {column.table for column in scope.expression.find_all(exp.Column) if column.name == "clientLoginId" and column.table}
            if tenant_aliases - scoped_aliases:
                raise RepositoryError(ErrorCode.QUERY_REJECTED, "Final executable query is not provably tenant scoped")
        return ValidatedQuery(sql=final_sql, parameters={"tenant_ids": list(context.allowed_customer_ids)}, tables=tuple(sorted(set(tables))), limit=limit)


class ValidatedSQLService:
    def __init__(self, executor: RepositoryExecutor, policy: SQLPolicy):
        self.executor = executor
        self.policy = policy

    def run(self, values: ValidatedSQLInput, context: TenantContext) -> RepositoryResult:
        query = self.policy.validate(values, context)
        return self.executor.execute(query.sql, parameters=query.parameters, columns=None, tables=query.tables, context=context, limit=query.limit, query_type="validated_sql", filters_applied={"tenant_scope": "server", "policy": "sqlglot_clickhouse_v1"})
