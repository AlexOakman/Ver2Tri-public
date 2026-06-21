"""
Runtime Trino testing and comparison for migrated SQL parts.
"""

import json
import os
import re
import warnings
import difflib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from openai import OpenAI
import sqlparse
from dotenv import load_dotenv
from urllib3.exceptions import InsecureRequestWarning

from config import settings
from core.llm_profiles import (
    ensure_no_proxy_for_llm,
    get_openai_request_kwargs,
    resolve_profile,
    resolve_stage_profile,
    strip_openai_provider_prefix,
)
from core.migration_knowledge import MigrationKnowledgeRegistry, PartIntentMemory, RepairPatchGuard
from core.state_manager import StateManager


TECHNICAL_COLUMNS = {"launch_id", "version_id", "constant"}
NUMERIC_TYPE_MARKERS = ("integer", "bigint", "smallint", "tinyint", "decimal", "double", "real")
FORBIDDEN_TEST_READ_SCHEMAS = {"sandbox", "target_schema"}
INTROSPECT_MARKER = "VER2TRI_INTROSPECT:"
APPLY_TO_PART_MARKER = "VER2TRI_APPLY_TO_PART:"
DIAGNOSTIC_QUERY_START_MARKER = "VER2TRI_DIAGNOSTIC_QUERY_START"
DIAGNOSTIC_QUERY_END_MARKER = "VER2TRI_DIAGNOSTIC_QUERY_END"
TABLE_REF_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+(?P<table>(?:(?:[A-Za-z_][\w$]*\.){0,2}[A-Za-z_][\w$]*))",
    re.IGNORECASE,
)
SET_DIRECTIVE_RE = re.compile(r"^\s*@set\s+(?P<name>[A-Za-z_][\w]*)\s*=\s*(?P<value>.+?)\s*$", re.IGNORECASE)
JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL | re.IGNORECASE)


class TrinoRuntimeError(Exception):
    """Raised when Trino runtime testing cannot continue."""


class RawRepairLLMClient:
    """Small OpenAI-compatible text client for runtime repair-agent calls."""

    def __init__(self):
        ensure_no_proxy_for_llm(settings.llm_base_url)
        self.client = OpenAI(base_url=settings.llm_base_url, api_key=settings.llm_api_key)

    def complete(self, prompt: str) -> str:
        request_kwargs = get_openai_request_kwargs(resolve_stage_profile("repair"))
        response = self.client.chat.completions.create(
            model=self._model_name(),
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=settings.llm_max_tokens,
            response_format={"type": "text"},
            timeout=settings.llm_timeout,
            **request_kwargs,
        )
        return response.choices[0].message.content or ""

    def _model_name(self) -> str:
        profile_name = resolve_stage_profile("repair")
        model = resolve_profile(profile_name).model
        return strip_openai_provider_prefix(model)


@dataclass
class HeaderMetadata:
    datamart: Optional[str] = None
    datamart_schema: Optional[str] = None
    datamart_table: Optional[str] = None
    target_table: Optional[str] = None
    type: Optional[str] = None
    date_col: Optional[str] = None
    days_col: Optional[str] = None
    days_to_keep: Optional[str] = None
    keys: List[str] = field(default_factory=list)
    params: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    scheduled: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class QualifiedTable:
    schema: Optional[str]
    table: str

    @property
    def lower_key(self) -> Tuple[Optional[str], str]:
        return (self.schema.lower() if self.schema else None, self.table.lower())

    def render(self, default_schema: str) -> str:
        schema = self.schema or default_schema
        return f"{schema}.{self.table}"


@dataclass
class RuntimeExecutionState:
    executed_parts_successfully: set[int] = field(default_factory=set)
    store_params_by_part: Dict[int, Dict[str, str]] = field(default_factory=dict)
    runtime_tables_created: set[str] = field(default_factory=set)
    table_to_creator_part: Dict[str, int] = field(default_factory=dict)
    expected_tables_by_part: Dict[int, set[str]] = field(default_factory=dict)
    current_run_id: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    missing_table_rebuild_attempts: set[Tuple[int, int]] = field(default_factory=set)


class RuntimeSampleLimiter:
    """Applies a bounded sample LIMIT to runtime smoke-test SQL."""

    @classmethod
    def apply(cls, sql: str, limit: int) -> str:
        if limit <= 0:
            return sql
        statement = sql.strip()
        if re.search(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?\S+\s+AS\s+", statement, re.IGNORECASE):
            statement = cls._process_create_table_as(statement, limit)
        else:
            statement = cls._append_limit(statement, limit)
        return cls._ensure_semicolon(statement)

    @classmethod
    def _process_create_table_as(cls, sql: str, limit: int) -> str:
        pattern = re.compile(
            r"(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?\S+\s+AS\s+)(\(?.*)",
            re.IGNORECASE | re.DOTALL,
        )

        def replace(match: re.Match[str]) -> str:
            prefix = match.group(1)
            rest = match.group(2)
            rest_stripped = rest.lstrip()
            if not rest_stripped.startswith("("):
                return prefix + cls._append_limit(rest, limit).rstrip(";")

            content = rest_stripped[1:]
            close_index = cls._matching_close_paren(content)
            if close_index is None:
                return match.group(0) + f"\nLIMIT {limit}"

            before_close = re.sub(
                r"\s+LIMIT\s+\d+\s*$",
                "",
                content[:close_index].rstrip(),
                flags=re.IGNORECASE,
            )
            after_close = content[close_index + 1 :]
            return prefix + "(" + before_close + f"\nLIMIT {limit}" + ")" + after_close

        return pattern.sub(replace, sql)

    @staticmethod
    def _append_limit(sql: str, limit: int) -> str:
        sql = re.sub(r"\s+LIMIT\s+\d+\s*(;|$)", r"\1", sql, flags=re.IGNORECASE)
        if re.search(r"\bLIMIT\s+\d+\b", sql, re.IGNORECASE):
            return sql
        if sql.endswith(";"):
            return sql[:-1] + f"\nLIMIT {limit};"
        return sql + f"\nLIMIT {limit}"

    @staticmethod
    def _matching_close_paren(content: str) -> Optional[int]:
        depth = 1
        in_string = False
        string_char: Optional[str] = None
        for index, char in enumerate(content):
            if not in_string:
                if char in ("'", '"'):
                    if index == 0 or content[index - 1] != "\\":
                        in_string = True
                        string_char = char
                elif char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                    if depth == 0:
                        return index
            elif char == string_char and (index == 0 or content[index - 1] != "\\"):
                in_string = False
        return None

    @staticmethod
    def _ensure_semicolon(sql: str) -> str:
        sql = sql.rstrip()
        while sql.endswith(";"):
            sql = sql[:-1].rstrip()

        lines = sql.split("\n")
        comment_match = re.match(r"(.*?)(\s*--.*)$", lines[-1])
        if comment_match:
            lines[-1] = comment_match.group(1).rstrip() + ";" + comment_match.group(2)
            return "\n".join(lines)
        return sql + ";"


@dataclass
class RepairAction:
    tool: str
    args: Dict[str, Any]
    purpose: str


@dataclass
class RepairPlannerResult:
    hypothesis: str
    target_part_candidate: int
    actions: List[RepairAction]
    stop_and_fix_now: bool
    why: str


@dataclass
class SQLLineEdit:
    op: str
    line: Optional[int] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    after_line: Optional[int] = None
    old: Optional[str] = None
    old_lines: List[str] = field(default_factory=list)
    new: Optional[str] = None
    new_lines: List[str] = field(default_factory=list)


@dataclass
class RepairFinalResult:
    target_part: int
    fixed_sql: str
    summary: str
    confidence: float
    used_evidence: List[str]
    edits: List[SQLLineEdit] = field(default_factory=list)
    change_type: str = "full_rewrite"
    reason: str = ""
    expected_preserved_invariants: List[str] = field(default_factory=list)
    risk_notes: List[str] = field(default_factory=list)
    need_more_actions: bool = False
    actions: List[RepairAction] = field(default_factory=list)


class SQLPatchApplier:
    """Applies line-numbered SQL edits against an immutable original snapshot."""

    VALID_OPS = {"replace_line", "replace_range", "insert_after_line"}

    @classmethod
    def apply(cls, old_sql: str, edits: List[SQLLineEdit]) -> str:
        if not edits:
            raise TrinoRuntimeError("Line patch must contain at least one edit")

        trailing_newline = old_sql.endswith("\n")
        original_lines = old_sql.splitlines()
        operations = [cls._validate_edit(edit, original_lines, index) for index, edit in enumerate(edits)]
        cls._validate_no_overlapping_replacements(operations)

        patched_lines = list(original_lines)
        for operation in sorted(operations, key=lambda item: (item["start"], item["order"]), reverse=True):
            patched_lines[operation["start"] : operation["end"]] = operation["new_lines"]

        patched_sql = "\n".join(patched_lines)
        if trailing_newline:
            patched_sql += "\n"
        return patched_sql

    @classmethod
    def summary(cls, edits: List[SQLLineEdit]) -> List[Dict[str, Any]]:
        summaries = []
        for edit in edits:
            if edit.op == "replace_line":
                summaries.append({"op": edit.op, "line": edit.line, "changed_lines": 1})
            elif edit.op == "replace_range":
                old_count = max((edit.end_line or 0) - (edit.start_line or 0) + 1, 0)
                summaries.append(
                    {
                        "op": edit.op,
                        "start_line": edit.start_line,
                        "end_line": edit.end_line,
                        "old_lines": old_count,
                        "new_lines": len(edit.new_lines),
                    }
                )
            elif edit.op == "insert_after_line":
                summaries.append({"op": edit.op, "after_line": edit.after_line, "new_lines": len(edit.new_lines)})
        return summaries

    @classmethod
    def _validate_edit(cls, edit: SQLLineEdit, lines: List[str], order: int) -> Dict[str, Any]:
        if edit.op not in cls.VALID_OPS:
            raise TrinoRuntimeError(f"Unknown SQL line edit op: {edit.op}")

        if edit.op == "replace_line":
            line = cls._require_int(edit.line, "line")
            if line < 1 or line > len(lines):
                raise TrinoRuntimeError(f"replace_line line out of range: {line}; file has {len(lines)} lines")
            expected = edit.old
            if expected is None:
                raise TrinoRuntimeError("replace_line requires old")
            actual = lines[line - 1]
            if actual != expected:
                raise TrinoRuntimeError(
                    f"replace_line old mismatch at line {line}: expected {expected!r}, actual {actual!r}"
                )
            new_line = edit.new
            if new_line is None:
                raise TrinoRuntimeError("replace_line requires new")
            cls._reject_embedded_newline(new_line, "new")
            return {"op": edit.op, "start": line - 1, "end": line, "new_lines": [new_line], "order": order}

        if edit.op == "replace_range":
            start_line = cls._require_int(edit.start_line, "start_line")
            end_line = cls._require_int(edit.end_line, "end_line")
            if start_line < 1 or end_line < start_line or end_line > len(lines):
                raise TrinoRuntimeError(
                    f"replace_range out of range: {start_line}-{end_line}; file has {len(lines)} lines"
                )
            if not edit.old_lines:
                raise TrinoRuntimeError("replace_range requires old_lines")
            actual_lines = lines[start_line - 1 : end_line]
            if actual_lines != edit.old_lines:
                raise TrinoRuntimeError(
                    f"replace_range old_lines mismatch at {start_line}-{end_line}: "
                    f"expected {edit.old_lines!r}, actual {actual_lines!r}"
                )
            cls._reject_embedded_newlines(edit.new_lines, "new_lines")
            return {"op": edit.op, "start": start_line - 1, "end": end_line, "new_lines": edit.new_lines, "order": order}

        after_line = cls._require_int(edit.after_line, "after_line")
        if after_line < 0 or after_line > len(lines):
            raise TrinoRuntimeError(f"insert_after_line out of range: {after_line}; file has {len(lines)} lines")
        if not edit.new_lines:
            raise TrinoRuntimeError("insert_after_line requires new_lines")
        cls._reject_embedded_newlines(edit.new_lines, "new_lines")
        return {"op": edit.op, "start": after_line, "end": after_line, "new_lines": edit.new_lines, "order": order}

    @staticmethod
    def _validate_no_overlapping_replacements(operations: List[Dict[str, Any]]) -> None:
        replacements = [op for op in operations if op["start"] != op["end"]]
        replacements.sort(key=lambda item: item["start"])
        previous_end = -1
        for operation in replacements:
            if operation["start"] < previous_end:
                raise TrinoRuntimeError("Overlapping replace edits are not allowed")
            previous_end = operation["end"]

    @staticmethod
    def _require_int(value: Optional[int], field_name: str) -> int:
        if not isinstance(value, int):
            raise TrinoRuntimeError(f"{field_name} must be an integer")
        return value

    @staticmethod
    def _reject_embedded_newline(value: str, field_name: str) -> None:
        if "\n" in value or "\r" in value:
            raise TrinoRuntimeError(f"{field_name} must be a single line")

    @classmethod
    def _reject_embedded_newlines(cls, values: List[str], field_name: str) -> None:
        for value in values:
            if not isinstance(value, str):
                raise TrinoRuntimeError(f"{field_name} must contain only strings")
            cls._reject_embedded_newline(value, field_name)


@dataclass
class RepairSession:
    session_id: str
    root_failed_part: int
    initial_error: str
    planner_output: Optional[Dict[str, Any]] = None
    actions: List[Dict[str, Any]] = field(default_factory=list)
    action_results: List[Dict[str, Any]] = field(default_factory=list)
    final_fix_target_part: Optional[int] = None
    final_fix_file: Optional[str] = None
    summary: Optional[str] = None
    status: str = "started"


class HeaderParser:
    """Small parser for the YAML-like @header block used in Vertica/Trino files."""

    HEADER_RE = re.compile(r"/\*\s*@header(?P<body>.*?)\*/", re.IGNORECASE | re.DOTALL)

    @classmethod
    def parse(cls, sql: str) -> HeaderMetadata:
        match = cls.HEADER_RE.search(sql)
        if not match:
            return HeaderMetadata()

        metadata = HeaderMetadata()
        section: Optional[str] = None
        active_item: Optional[Dict[str, str]] = None

        for raw_line in match.group("body").splitlines():
            line = raw_line.rstrip()
            stripped = line.strip()
            if not stripped:
                continue

            key_value = re.match(r"^([A-Za-z_][\w-]*):\s*(.*)$", stripped)
            list_item = re.match(r"^-\s*(.*)$", stripped)

            if key_value and not raw_line.startswith(" "):
                section = None
                key = key_value.group(1)
                value = cls._clean_scalar(key_value.group(2))
                if value == "":
                    section = key
                    if section == "params":
                        active_item = None
                    continue
                cls._set_top_level(metadata, key, value)
                active_item = None
                continue

            if section == "keys" and list_item:
                metadata.keys.append(cls._clean_scalar(list_item.group(1)))
                continue

            if section == "params":
                if list_item:
                    active_item = cls._parse_inline_mapping(list_item.group(1))
                    if "name" in active_item:
                        metadata.params[active_item["name"]] = active_item
                    continue
                if active_item and key_value:
                    active_item[key_value.group(1)] = cls._clean_scalar(key_value.group(2))
                continue

            if section == "scheduled":
                if list_item:
                    active_item = cls._parse_inline_mapping(list_item.group(1))
                    metadata.scheduled.append(active_item)
                    continue
                if active_item and key_value:
                    active_item[key_value.group(1)] = cls._clean_scalar(key_value.group(2))

        if metadata.datamart:
            parts = metadata.datamart.split(".", 1)
            if len(parts) == 2:
                metadata.datamart_schema, metadata.datamart_table = parts[0], parts[1]
            else:
                metadata.datamart_table = parts[0]
            metadata.target_table = cls._target_table_name(metadata.datamart_table)

        return metadata

    @staticmethod
    def _clean_scalar(value: str) -> str:
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            return value[1:-1]
        return value

    @classmethod
    def _parse_inline_mapping(cls, text: str) -> Dict[str, str]:
        match = re.match(r"^([A-Za-z_][\w-]*):\s*(.*)$", text.strip())
        if not match:
            return {"value": cls._clean_scalar(text)}
        return {match.group(1): cls._clean_scalar(match.group(2))}

    @staticmethod
    def _set_top_level(metadata: HeaderMetadata, key: str, value: str) -> None:
        key_lower = key.lower()
        if key_lower == "datamart":
            metadata.datamart = value
        elif key_lower == "type":
            metadata.type = value
        elif key_lower == "date_col":
            metadata.date_col = value
        elif key_lower == "days_col":
            metadata.days_col = value
        elif key_lower == "days_to_keep":
            metadata.days_to_keep = value

    @staticmethod
    def _target_table_name(datamart_table: str) -> str:
        return datamart_table if datamart_table.lower().endswith("_trino") else f"{datamart_table}_trino"

    @classmethod
    def strip_header(cls, sql: str) -> str:
        return cls.HEADER_RE.sub("", sql, count=1).lstrip()


class TrinoSQLPreparer:
    """Rewrites generated SQL so runtime-created tables live in the configured test schema."""

    TARGET_RE = re.compile(
        r"\b(?P<verb>CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?|INSERT\s+INTO)\s+"
        r"(?P<name>(?:[A-Za-z_][\w$]*\.)?[A-Za-z_][\w$]*)",
        re.IGNORECASE,
    )
    CREATE_TARGET_RE = re.compile(
        r"\bCREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+"
        r"(?P<name>(?:[A-Za-z_][\w$]*\.)?[A-Za-z_][\w$]*)",
        re.IGNORECASE,
    )

    QUALIFIED_RE_TEMPLATE = r"(?<![\w$]){schema}\.{table}(?![\w$])"

    def __init__(self, trino_schema: str, header: HeaderMetadata):
        self.trino_schema = trino_schema
        self.header = header
        self.created_tables: Dict[Tuple[Optional[str], str], QualifiedTable] = {}
        self.final_target_schema = self._default_final_target_schema()

    def discover_runtime_targets(self, parts: Iterable[str]) -> None:
        for sql in parts:
            for table in self._find_created_tables(sql):
                if self._is_final_target(table) and table.schema:
                    self.final_target_schema = table.schema
                self.created_tables[table.lower_key] = table

        if self.header.target_table:
            final_schema = self.final_target_schema or self.header.datamart_schema
            self.created_tables[(final_schema.lower() if final_schema else None, self.header.target_table.lower())] = (
                QualifiedTable(final_schema, self.header.target_table)
            )

    def rewrite_part_sql(self, sql: str) -> str:
        sql = HeaderParser.strip_header(sql)
        sql = self._rewrite_statement_targets(sql)
        sql = self._rewrite_known_qualified_references(sql)
        return sql

    def runtime_table_names(self) -> List[str]:
        names = {table.table for table in self.created_tables.values()}
        return sorted(names)

    def _find_statement_targets(self, sql: str) -> List[QualifiedTable]:
        tables = []
        for match in self.TARGET_RE.finditer(sql):
            tables.append(self._parse_table(match.group("name")))
        return tables

    def _find_created_tables(self, sql: str) -> List[QualifiedTable]:
        tables = []
        for match in self.CREATE_TARGET_RE.finditer(sql):
            tables.append(self._parse_table(match.group("name")))
        return tables

    def _rewrite_statement_targets(self, sql: str) -> str:
        def replace(match: re.Match[str]) -> str:
            table = self._parse_table(match.group("name"))
            if self._is_final_target(table):
                return f"{match.group('verb')} {self._render_final_target(table)}"
            self.created_tables[table.lower_key] = table
            return f"{match.group('verb')} {self.trino_schema}.{table.table}"

        return self.TARGET_RE.sub(replace, sql)

    def _rewrite_known_qualified_references(self, sql: str) -> str:
        result = sql
        for table in self.created_tables.values():
            if table.schema is None:
                continue
            if self._is_final_target(table):
                continue
            pattern = self.QUALIFIED_RE_TEMPLATE.format(
                schema=re.escape(table.schema),
                table=re.escape(table.table),
            )
            result = re.sub(pattern, f"{self.trino_schema}.{table.table}", result, flags=re.IGNORECASE)
        return result

    def _is_final_target(self, table: QualifiedTable) -> bool:
        return bool(self.header.target_table and table.table.lower() == self.header.target_table.lower())

    def _render_final_target(self, table: QualifiedTable) -> str:
        schema = table.schema or self.final_target_schema
        if schema and schema.lower() == (self.header.datamart_schema or "").lower() and schema.lower() == "dma":
            schema = self.final_target_schema or "sandbox"
        if schema:
            return f"{schema}.{table.table}"
        return table.table

    def _default_final_target_schema(self) -> Optional[str]:
        schema = self.header.datamart_schema
        if schema and schema.lower() == "dma":
            return "sandbox"
        return schema

    @staticmethod
    def _parse_table(name: str) -> QualifiedTable:
        parts = name.split(".", 1)
        if len(parts) == 2:
            return QualifiedTable(parts[0], parts[1])
        return QualifiedTable(None, parts[0])


class ParameterResolver:
    """Builds runtime parameter values from header params, @set overrides and defaults."""

    PARAM_RE = re.compile(r"(?P<brace>\$\{(?P<brace_name>[A-Za-z_][\w]*)\})|:(?P<colon_name>[A-Za-z_][\w]*)\b")

    def __init__(
        self,
        header: HeaderMetadata,
        now: Optional[datetime] = None,
        explicit_values: Optional[Dict[str, str]] = None,
    ):
        self.header = header
        self.now = now or datetime.now()
        self.explicit_values = explicit_values or {}
        self.values = self._build_values()

    @classmethod
    def from_sql_parts(
        cls,
        header: HeaderMetadata,
        sql_parts: Iterable[str],
        now: Optional[datetime] = None,
    ) -> "ParameterResolver":
        explicit_values: Dict[str, str] = {}
        for sql in sql_parts:
            explicit_values.update(cls._parse_set_directives(sql))
        return cls(header, now=now, explicit_values=explicit_values)

    def substitute(self, sql: str) -> str:
        return self._replace_outside_comments(sql, self._replace_match)

    def _replace_match(self, match: re.Match[str]) -> str:
        name = match.group("brace_name") or match.group("colon_name")
        if name not in self.values:
            self.values[name] = self._default_value_for_param(name)
        return self.values[name]

    def _build_values(self) -> Dict[str, str]:
        values: Dict[str, str] = {}
        for name, attrs in self.header.params.items():
            default = attrs.get("default")
            values[name] = self._literal_for_param(name, default)

        for scheduled_item in self.header.scheduled:
            for name, value in scheduled_item.items():
                if name == "user_name":
                    continue
                values[name] = self._literal_for_param(name, value)

        for required in ("actual_date", "first_date", "last_date", "launch_id", "version_id"):
            values.setdefault(required, self._default_value_for_param(required))

        for name, value in self.explicit_values.items():
            values[name] = self._literal_for_param(name, value)

        return values

    def _literal_for_param(self, name: str, value: Optional[str]) -> str:
        if value:
            resolved = self._resolve_macro(value)
            if self._looks_like_date_param(name, resolved):
                return f"'{resolved}'"
            if re.fullmatch(r"-?\d+(?:\.\d+)?", resolved):
                return resolved
            return f"'{resolved}'"
        return self._default_value_for_param(name)

    def _default_value_for_param(self, name: str) -> str:
        name_lower = name.lower()
        if "month" in name_lower:
            month_start = self.now.replace(day=1)
            return f"'{month_start.strftime('%Y-%m-%d')}'"
        if self._looks_like_date_param(name, None):
            return f"'{(self.now - timedelta(days=1)).strftime('%Y-%m-%d')}'"
        return "1"

    def _resolve_macro(self, value: str) -> str:
        today_match = re.fullmatch(r"\$TODAY(?:\[(?P<offset>-?\d+)\])?", value.strip(), re.IGNORECASE)
        if today_match:
            offset = int(today_match.group("offset") or 0)
            return (self.now + timedelta(days=offset)).strftime("%Y-%m-%d")
        current_month_match = re.fullmatch(r"\$CURRENT_MONTH(?:\[(?P<offset>-?\d+)\])?", value.strip(), re.IGNORECASE)
        if current_month_match:
            offset = int(current_month_match.group("offset") or 0)
            month_anchor = self.now.replace(day=1)
            month_anchor = month_anchor + timedelta(days=offset * 31)
            month_anchor = month_anchor.replace(day=1)
            return month_anchor.strftime("%Y-%m-%d")
        return value

    @staticmethod
    def _looks_like_date_param(name: str, value: Optional[str]) -> bool:
        if "date" in name.lower() or name.lower() in {"dt", "day"}:
            return True
        return bool(value and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value))

    def _replace_outside_comments(self, sql: str, replacer) -> str:
        result: List[str] = []
        index = 0
        while index < len(sql):
            if sql.startswith("--", index):
                end = sql.find("\n", index)
                if end == -1:
                    result.append(sql[index:])
                    break
                result.append(sql[index:end])
                index = end
                continue
            if sql.startswith("/*", index):
                end = sql.find("*/", index + 2)
                if end == -1:
                    result.append(sql[index:])
                    break
                result.append(sql[index:end + 2])
                index = end + 2
                continue
            next_comment_positions = [pos for pos in (sql.find("--", index), sql.find("/*", index)) if pos != -1]
            end = min(next_comment_positions) if next_comment_positions else len(sql)
            result.append(self.PARAM_RE.sub(replacer, sql[index:end]))
            index = end
        return "".join(result)

    @staticmethod
    def _parse_set_directives(sql: str) -> Dict[str, str]:
        values: Dict[str, str] = {}
        for raw_line in sql.splitlines():
            match = SET_DIRECTIVE_RE.match(raw_line)
            if not match:
                continue
            value = match.group("value").strip().rstrip(";")
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            values[match.group("name")] = value
        return values


class RuntimeVariableResolver(ParameterResolver):
    """Semantic alias used by the modular pipeline/reporting layer."""


class TrinoConnectionFactory:
    """Lazy Trino DB-API connection factory."""

    def connect(self):
        load_dotenv(Path(".env"), override=False)

        runtime_schema = settings.trino_schema or settings.trino_test_schema
        if not runtime_schema:
            raise TrinoRuntimeError("TRINO_TEST_SCHEMA or TRINO_SCHEMA is required for runtime testing")

        try:
            import getpass
            import trino
            from trino.auth import BasicAuthentication
        except ImportError as exc:
            raise TrinoRuntimeError("Package 'trino' is required. Install project dependencies first.") from exc

        warnings.simplefilter("ignore", InsecureRequestWarning)
        self._ensure_no_proxy_for_trino()

        user = settings.trino_user or getpass.getuser()
        auth = BasicAuthentication(user, settings.trino_password or "")
        return trino.dbapi.connect(
            host=settings.trino_host,
            port=settings.trino_port,
            user=user,
            catalog=settings.trino_catalog,
            schema=runtime_schema,
            http_scheme="https" if settings.trino_ssl else "http",
            auth=auth,
            verify=False,
        )

    def _ensure_no_proxy_for_trino(self) -> None:
        host = settings.trino_host
        if not host:
            return

        current = os.environ.get("NO_PROXY", "")
        entries = [entry.strip() for entry in current.split(",") if entry.strip()]
        desired = [host]

        changed = False
        for entry in desired:
            if entry not in entries:
                entries.append(entry)
                changed = True

        if changed:
            updated = ",".join(entries)
            os.environ["NO_PROXY"] = updated
            os.environ["no_proxy"] = updated


class TrinoRuntimeTester:
    """Executes migrated parts in Trino and fixes technical runtime failures."""

    REPAIR_AGENT_MAX_ACTIONS = 4
    REPAIR_AGENT_MAX_DB_ACTIONS = 2
    REPAIR_AGENT_MAX_DIAGNOSTIC_QUERIES = 2
    REPAIR_AGENT_MAX_FOLLOWUP_ROUNDS = 2
    TRANSIENT_EXECUTION_RETRIES = 2
    REPAIR_AGENT_ALLOWED_TOOLS = {
        "read_trino_part",
        "read_trino_part_lines",
        "read_vertica_part",
        "read_part_dependencies",
        "read_runtime_state",
        "inspect_runtime_table",
        "inspect_information_schema",
        "run_diagnostic_query",
        "list_related_parts",
        "list_part_versions",
        "diff_part_versions",
        "read_part_intent",
        "read_full_script",
        "search_parts",
        "inspect_alias_sources",
        "resolve_column_reference",
        "inspect_source_columns",
        "suggest_column_candidates",
        "run_column_probe",
    }

    def __init__(
        self,
        state_manager: StateManager,
        connection: Optional[Any] = None,
        connection_factory: Optional[TrinoConnectionFactory] = None,
    ):
        self.state_manager = state_manager
        self.query_name = state_manager.query_name
        self.connection = connection
        self.connection_factory = connection_factory or TrinoConnectionFactory()
        self.runtime_schema = settings.trino_schema or settings.trino_test_schema
        self.raw_repair_client = RawRepairLLMClient()

    def _get_runtime_schema(self) -> str:
        return getattr(self, "runtime_schema", None) or settings.trino_schema or settings.trino_test_schema

    def _initialize_runtime_report(self) -> Dict[str, Any]:
        return {
            "success": False,
            "schema": self.runtime_schema,
            "parts": [],
            "fixes": [],
            "compare": {},
            "started_at": datetime.utcnow().isoformat(),
            "stage_meta": {
                "query_name": self.query_name,
                "runtime_schema": self.runtime_schema,
                "max_fix_iterations": settings.trino_test_max_fix_iterations,
            },
            "variables": {},
            "part_execution": [],
            "runtime_fix_attempts": [],
            "introspection": [],
            "diagnostic_queries": [],
            "runtime_tables": [],
            "reconciliation_analysis": {},
            "warnings": [],
            "replays": [],
            "event_log": [],
            "repair_sessions": [],
        }

    def _bootstrap_runtime_run(
        self,
        report: Dict[str, Any],
    ) -> tuple[HeaderMetadata, Dict[int, str], Any, RuntimeExecutionState, TrinoSQLPreparer, RuntimeVariableResolver, List[int]]:
        self._log_event(
            report,
            "runtime_test_started",
            schema=self.runtime_schema,
            max_fix_iterations=settings.trino_test_max_fix_iterations,
        )
        self._reassemble_artifacts()
        header = self._load_header()
        part_sql_by_num = self._load_latest_parts()
        ordered_parts = sorted(part_sql_by_num)
        connection = self.connection or self.connection_factory.connect()
        runtime_state = self._build_runtime_execution_state(header, part_sql_by_num)
        preparer = self._build_preparer(header, part_sql_by_num)
        resolver = RuntimeVariableResolver.from_sql_parts(header, part_sql_by_num.values())
        report["variables"] = dict(resolver.values)
        report["runtime_tables"] = sorted(runtime_state.runtime_tables_created)
        self._drop_runtime_tables(connection, sorted(runtime_state.runtime_tables_created))
        return header, part_sql_by_num, connection, runtime_state, preparer, resolver, ordered_parts

    def _handle_fix_limit_exceeded(
        self,
        report: Dict[str, Any],
        failed_part: int,
        current_attempt: int,
        total_parts: int,
    ) -> tuple[bool, Dict[str, Any]]:
        report["error"] = f"Part {failed_part} failed after runtime fixes"
        self._log_event(
            report,
            "fix_limit_exceeded",
            part=failed_part,
            fix_attempt=current_attempt,
            error=report["error"],
        )
        self._update_runtime_status(
            status="failed",
            phase="stopped",
            part_num=failed_part,
            total_parts=total_parts,
            error_text=report["error"],
            fix_attempt=current_attempt,
            message="Превышен лимит runtime-fix попыток",
        )
        self._write_report(report)
        return False, report

    def _finalize_successful_run(
        self,
        report: Dict[str, Any],
        header: HeaderMetadata,
        part_sql_by_num: Dict[int, str],
        runtime_state: RuntimeExecutionState,
    ) -> tuple[bool, Dict[str, Any]]:
        resolver = RuntimeVariableResolver.from_sql_parts(header, part_sql_by_num.values())
        resolver.values.update(self._flatten_store_params(runtime_state.store_params_by_part))
        report["variables"] = dict(resolver.values)
        report["success"] = True
        self._log_event(report, "runtime_test_finished", success=True, total_parts=len(part_sql_by_num))
        self._update_runtime_status(
            status="success",
            phase="finished",
            total_parts=len(part_sql_by_num),
            message="Техническое Trino runtime test завершено",
        )
        self._write_report(report)
        return True, report

    def _record_saved_fix(
        self,
        report: Dict[str, Any],
        *,
        failed_part: int,
        current_attempt: int,
        error_text: str,
        error_signature: str,
        fixed_path: Path,
        target_part: int,
        repair_summary: Optional[str],
    ) -> None:
        report["fixes"].append(
            {
                "part": failed_part,
                "attempt": current_attempt,
                "file": str(fixed_path),
                "error": error_text,
                "error_signature": error_signature,
                "fix_target_part": target_part,
                "summary": repair_summary,
            }
        )
        report["runtime_fix_attempts"].append(
            {
                "part": failed_part,
                "attempt": current_attempt,
                "error": error_text,
                "error_signature": error_signature,
                "fixed_file": str(fixed_path),
                "fix_target_part": target_part,
                "rebuild_strategy": "targeted_rebuild",
                "summary": repair_summary,
            }
        )
        self._log_event(
            report,
            "fix_saved",
            part=failed_part,
            fix_attempt=current_attempt,
            fixed_file=str(fixed_path),
            fix_target_part=target_part,
        )
        self.state_manager.append_knowledge(
            "llm_fix_history",
            {
                "part_num": failed_part,
                "attempt": current_attempt,
                "error": error_text,
                "error_signature": error_signature,
                "fixed_file": str(fixed_path),
                "fix_target_part": target_part,
                "summary": repair_summary,
                "timestamp": datetime.utcnow().isoformat(),
            },
        )

    def _refresh_runtime_context_after_fix(
        self,
        report: Dict[str, Any],
        header: HeaderMetadata,
        part_sql_by_num: Dict[int, str],
        runtime_state: RuntimeExecutionState,
    ) -> tuple[Dict[int, str], TrinoSQLPreparer, RuntimeExecutionState, RuntimeVariableResolver]:
        self._reassemble_artifacts()
        part_sql_by_num = self._load_latest_parts()
        preparer = self._build_preparer(header, part_sql_by_num)
        runtime_state = self._refresh_runtime_execution_state(runtime_state, header, part_sql_by_num)
        resolver = self._rebuild_resolver(header, part_sql_by_num, runtime_state)
        report["variables"] = dict(resolver.values)
        report["runtime_tables"] = sorted(runtime_state.runtime_tables_created)
        return part_sql_by_num, preparer, runtime_state, resolver

    def run(self) -> Tuple[bool, Dict[str, Any]]:
        if not self._get_runtime_schema():
            return False, {"success": False, "error": "TRINO_TEST_SCHEMA or TRINO_SCHEMA is not configured"}
        self.runtime_schema = self._get_runtime_schema()
        self._update_runtime_status(status="running", phase="bootstrap", message="Подготовка Trino runtime test")
        report = self._initialize_runtime_report()

        try:
            header, part_sql_by_num, connection, runtime_state, preparer, resolver, ordered_parts = self._bootstrap_runtime_run(report)
            attempt_counts: Dict[Tuple[int, str], int] = {}
            current_index = 0
            while current_index < len(ordered_parts):
                part_num = ordered_parts[current_index]
                result = self._execute_single_part(
                    connection=connection,
                    part_num=part_num,
                    total_parts=len(ordered_parts),
                    part_sql_by_num=part_sql_by_num,
                    preparer=preparer,
                    resolver=resolver,
                    report=report,
                    runtime_state=runtime_state,
                )
                if result["success"]:
                    current_index += 1
                    continue

                failed_part = part_num
                error_text = result["error"]
                error_signature = self._runtime_error_signature(error_text)
                report["failed_part"] = failed_part
                report["error"] = error_text

                fix_result = None
                while fix_result is None:
                    attempt_key = (failed_part, error_signature)
                    attempt_counts[attempt_key] = attempt_counts.get(attempt_key, 0) + 1
                    current_attempt = attempt_counts[attempt_key]
                    self._log_event(
                        report,
                        "part_failed" if current_attempt == 1 else "repair_attempt_started",
                        part=failed_part,
                        total_parts=len(ordered_parts),
                        fix_attempt=current_attempt,
                        error=error_text,
                    )
                    self._update_runtime_status(
                        status="failed_part",
                        phase="fixing",
                        part_num=failed_part,
                        total_parts=len(ordered_parts),
                        error_text=error_text,
                        fix_attempt=current_attempt,
                        message=f"Ошибка в part {failed_part}, попытка исправления {current_attempt}",
                        root_failed_part=failed_part,
                    )
                    if current_attempt > settings.trino_test_max_fix_iterations:
                        return self._handle_fix_limit_exceeded(report, failed_part, current_attempt, len(ordered_parts))

                    try:
                        fix_result = self._fix_part(
                            connection,
                            failed_part,
                            error_text,
                            header,
                            part_sql_by_num,
                            runtime_state,
                            preparer,
                            resolver,
                            report,
                            current_attempt,
                        )
                    except TrinoRuntimeError as exc:
                        report["error"] = str(exc)
                        self._log_event(
                            report,
                            "repair_attempt_failed",
                            part=failed_part,
                            fix_attempt=current_attempt,
                            error=str(exc),
                        )
                        self._update_runtime_status(
                            status="failed_part",
                            phase="fixing",
                            part_num=failed_part,
                            total_parts=len(ordered_parts),
                            error_text=str(exc),
                            fix_attempt=current_attempt,
                            message=f"Repair-agent attempt {current_attempt} failed; trying next attempt",
                            root_failed_part=failed_part,
                        )
                        self._write_report(report)
                        if current_attempt >= settings.trino_test_max_fix_iterations:
                            raise
                fixed_path = fix_result["path"]
                target_part = fix_result["target_part"]
                repair_summary = fix_result.get("summary")
                self._record_saved_fix(
                    report,
                    failed_part=failed_part,
                    current_attempt=current_attempt,
                    error_text=error_text,
                    error_signature=error_signature,
                    fixed_path=fixed_path,
                    target_part=target_part,
                    repair_summary=repair_summary,
                )

                part_sql_by_num, preparer, runtime_state, resolver = self._refresh_runtime_context_after_fix(
                    report,
                    header,
                    part_sql_by_num,
                    runtime_state,
                )
                current_index = self._invalidate_from_part(
                    connection=connection,
                    ordered_parts=ordered_parts,
                    start_part=target_part,
                    runtime_state=runtime_state,
                    resolver=resolver,
                    report=report,
                    reason="upstream_logic_fix" if target_part != failed_part else "current_part_fix",
                    root_failed_part=failed_part,
                    fix_target_part=target_part,
                )

            return self._finalize_successful_run(report, header, part_sql_by_num, runtime_state)
        except Exception as exc:
            report["error"] = str(exc)
            self._log_event(report, "runtime_test_exception", error=str(exc))
            self._update_runtime_status(
                status="failed",
                phase="exception",
                error_text=str(exc),
                message="Runtime test завершился исключением",
            )
            self._write_report(report)
            return False, report

    def _plan_repair_or_raise(
        self,
        *,
        connection: Any,
        part_num: int,
        error_text: str,
        header: HeaderMetadata,
        part_sql_by_num: Dict[int, str],
        runtime_state: RuntimeExecutionState,
        preparer: TrinoSQLPreparer,
        resolver: ParameterResolver,
        report: Dict[str, Any],
        session: RepairSession,
    ) -> RepairPlannerResult:
        try:
            return self._plan_repair(
                connection=connection,
                part_num=part_num,
                error_text=error_text,
                header=header,
                part_sql_by_num=part_sql_by_num,
                runtime_state=runtime_state,
                preparer=preparer,
                resolver=resolver,
                report=report,
                session=session,
            )
        except Exception as exc:
            session.status = "planner_failed"
            session.summary = f"planner_failed: {exc}"
            self._finish_repair_session(report, session)
            raise TrinoRuntimeError(f"repair_agent_planner_failed: {exc}") from exc

    def _produce_final_repair_or_raise(
        self,
        *,
        connection: Any,
        part_num: int,
        error_text: str,
        header: HeaderMetadata,
        part_sql_by_num: Dict[int, str],
        runtime_state: RuntimeExecutionState,
        preparer: TrinoSQLPreparer,
        resolver: ParameterResolver,
        report: Dict[str, Any],
        session: RepairSession,
        planner_result: RepairPlannerResult,
        action_results: List[Dict[str, Any]],
    ) -> RepairFinalResult:
        try:
            return self._produce_final_repair(
                connection=connection,
                part_num=part_num,
                error_text=error_text,
                header=header,
                part_sql_by_num=part_sql_by_num,
                runtime_state=runtime_state,
                preparer=preparer,
                resolver=resolver,
                report=report,
                session=session,
                planner_result=planner_result,
                action_results=action_results,
            )
        except Exception as exc:
            session.status = "final_fix_failed"
            session.summary = f"final_fix_failed: {exc}"
            self._finish_repair_session(report, session)
            raise TrinoRuntimeError(f"repair_agent_final_fix_failed: {exc}") from exc

    def _finalize_successful_repair(
        self,
        *,
        report: Dict[str, Any],
        session: RepairSession,
        part_num: int,
        attempt_num: int,
        final_result: RepairFinalResult,
    ) -> Dict[str, Any]:
        target_part_num = final_result.target_part
        self._log_event(
            report,
            "repair_fix_generated",
            part=part_num,
            session_id=session.session_id,
            fix_target_part=target_part_num,
            summary=final_result.summary,
            change_type=final_result.change_type,
            confidence=final_result.confidence,
            used_evidence=final_result.used_evidence,
            edit_summary=SQLPatchApplier.summary(final_result.edits),
        )
        saved_path, guard_result = self._save_guarded_fix(
            target_part_num,
            final_result.fixed_sql,
            report=report,
            source_stage="repair_agent",
            root_failed_part=part_num,
            attempt_num=attempt_num,
            final_result=final_result,
        )
        session.final_fix_target_part = target_part_num
        session.final_fix_file = str(saved_path)
        session.summary = final_result.summary
        session.status = "success"
        self._log_event(
            report,
            "repair_fix_saved",
            part=part_num,
            session_id=session.session_id,
            current_target_part=target_part_num,
            fixed_file=str(saved_path),
            status="success",
            guard_result=guard_result,
        )
        self._finish_repair_session(report, session)
        return {
            "path": saved_path,
            "target_part": target_part_num,
            "summary": final_result.summary,
            "guard_result": guard_result,
        }

    def _build_preparer(self, header: HeaderMetadata, part_sql_by_num: Dict[int, str]) -> TrinoSQLPreparer:
        preparer = TrinoSQLPreparer(self.runtime_schema, header)
        preparer.discover_runtime_targets(part_sql_by_num.values())
        return preparer

    def _build_runtime_execution_state(self, header: HeaderMetadata, part_sql_by_num: Dict[int, str]) -> RuntimeExecutionState:
        expected_tables_by_part = self._expected_runtime_tables_by_part(header, part_sql_by_num)
        table_to_creator_part: Dict[str, int] = {}
        for part_num in sorted(expected_tables_by_part):
            for table_name in expected_tables_by_part[part_num]:
                table_to_creator_part.setdefault(table_name, part_num)
        runtime_tables_created = {
            table_name
            for table_names in expected_tables_by_part.values()
            for table_name in table_names
        }
        return RuntimeExecutionState(
            runtime_tables_created=runtime_tables_created,
            table_to_creator_part=table_to_creator_part,
            expected_tables_by_part=expected_tables_by_part,
        )

    def _refresh_runtime_execution_state(
        self,
        runtime_state: RuntimeExecutionState,
        header: HeaderMetadata,
        part_sql_by_num: Dict[int, str],
    ) -> RuntimeExecutionState:
        fresh = self._build_runtime_execution_state(header, part_sql_by_num)
        fresh.executed_parts_successfully = set(runtime_state.executed_parts_successfully)
        fresh.store_params_by_part = dict(runtime_state.store_params_by_part)
        fresh.current_run_id = runtime_state.current_run_id
        fresh.missing_table_rebuild_attempts = set(runtime_state.missing_table_rebuild_attempts)
        return fresh

    def _rebuild_resolver(
        self,
        header: HeaderMetadata,
        part_sql_by_num: Dict[int, str],
        runtime_state: RuntimeExecutionState,
    ) -> RuntimeVariableResolver:
        resolver = RuntimeVariableResolver.from_sql_parts(header, part_sql_by_num.values())
        resolver.values.update(self._flatten_store_params(runtime_state.store_params_by_part))
        return resolver

    def _flatten_store_params(self, store_params_by_part: Dict[int, Dict[str, str]]) -> Dict[str, str]:
        flattened: Dict[str, str] = {}
        for _, params in sorted(store_params_by_part.items()):
            flattened.update(params)
        return flattened

    def _expected_runtime_tables_by_part(self, header: HeaderMetadata, part_sql_by_num: Dict[int, str]) -> Dict[int, set[str]]:
        mapping: Dict[int, set[str]] = {}
        parser = TrinoSQLPreparer(self.runtime_schema, header)
        for part_num, sql in part_sql_by_num.items():
            names: set[str] = set()
            for table in parser._find_created_tables(sql):
                names.add(table.table.lower())
            mapping[part_num] = names
        return mapping

    def _execute_single_part(
        self,
        *,
        connection: Any,
        part_num: int,
        total_parts: int,
        part_sql_by_num: Dict[int, str],
        preparer: TrinoSQLPreparer,
        resolver: RuntimeVariableResolver,
        report: Dict[str, Any],
        runtime_state: RuntimeExecutionState,
    ) -> Dict[str, Any]:
        sql = part_sql_by_num[part_num]
        prepared_sql = resolver.substitute(preparer.rewrite_part_sql(sql))
        if part_num != 0 and not self._is_store_part(part_num):
            prepared_sql = self._apply_runtime_sample_limit(prepared_sql)
        version = self.state_manager.get_latest_version_number(part_num)
        log_entry = {
            "part": part_num,
            "version": version,
            "prepared_sql": prepared_sql,
            "declared_variables": dict(resolver.values),
            "runtime_tables": sorted(runtime_state.runtime_tables_created),
            "started_at": datetime.utcnow().isoformat(),
        }
        try:
            if self._is_store_part(part_num):
                self._log_event(
                    report,
                    "store_part_started",
                    part=part_num,
                    total_parts=total_parts,
                    version=version,
                )
                self.state_manager.set_current_operation(
                    f"🧮 Trino @store Part {part_num}/{total_parts - 1}",
                    {"part": part_num, "schema": self.runtime_schema, "phase": "trino_testing", "store": True},
                )
                self._update_runtime_status(
                    status="running",
                    phase="executing_store",
                    part_num=part_num,
                    total_parts=total_parts,
                    message=f"Выполняется @store part {part_num}",
                )
                store_result = self._execute_store_part(connection, prepared_sql, resolver)
                runtime_state.store_params_by_part[part_num] = store_result.get("stored_params", {})
                log_entry.update({"status": store_result["status"], **store_result})
                report["parts"].append({"part": part_num, "status": store_result["status"], "attempt": 0, **store_result})
                self._log_event(
                    report,
                    "store_part_succeeded",
                    part=part_num,
                    total_parts=total_parts,
                    version=version,
                    stored_params=store_result.get("stored_params", {}),
                )
            else:
                self._log_event(
                    report,
                    "part_started",
                    part=part_num,
                    total_parts=total_parts,
                    version=version,
                )
                self.state_manager.set_current_operation(
                    f"🧪 Trino runtime test Part {part_num}/{total_parts - 1}",
                    {"part": part_num, "schema": self.runtime_schema, "phase": "trino_testing"},
                )
                self._update_runtime_status(
                    status="running",
                    phase="executing_part",
                    part_num=part_num,
                    total_parts=total_parts,
                    message=f"Выполняется part {part_num}",
                )
                self._execute_sql_with_transient_retries(
                    connection=connection,
                    sql=prepared_sql,
                    part_num=part_num,
                    total_parts=total_parts,
                    version=version,
                    runtime_state=runtime_state,
                    report=report,
                )
                log_entry["status"] = "ok"
                report["parts"].append({"part": part_num, "status": "ok", "attempt": 0})
                self._log_event(
                    report,
                    "part_succeeded",
                    part=part_num,
                    total_parts=total_parts,
                    version=version,
                )
            runtime_state.executed_parts_successfully.add(part_num)
            self._assemble_final_artifact()
            log_entry["finished_at"] = datetime.utcnow().isoformat()
            report["part_execution"].append(log_entry)
            self._write_report(report)
            return {"success": True}
        except Exception as exc:
            error_text = str(exc)
            log_entry["status"] = "error"
            log_entry["error"] = error_text
            log_entry["finished_at"] = datetime.utcnow().isoformat()
            report["part_execution"].append(log_entry)
            report["parts"].append({"part": part_num, "status": "error", "attempt": 0, "error": error_text})
            self._update_runtime_status(
                status="failed_part",
                phase="failed",
                part_num=part_num,
                total_parts=total_parts,
                error_text=error_text,
                message=f"Ошибка выполнения part {part_num}",
                root_failed_part=part_num,
            )
            self._write_report(report)
            return {"success": False, "error": error_text}

    def _resolve_missing_table_producer(
        self,
        error_text: str,
        failed_part: int,
        runtime_state: RuntimeExecutionState,
    ) -> Optional[int]:
        match = re.search(r"Table '([^']+)' does not exist", error_text, re.IGNORECASE)
        if not match:
            return None
        table_name = match.group(1).split(".")[-1].lower()
        producer_part = runtime_state.table_to_creator_part.get(table_name)
        if producer_part is None or producer_part >= failed_part:
            return None
        return producer_part

    def _invalidate_from_part(
        self,
        *,
        connection: Any,
        ordered_parts: List[int],
        start_part: int,
        runtime_state: RuntimeExecutionState,
        resolver: RuntimeVariableResolver,
        report: Dict[str, Any],
        reason: str,
        root_failed_part: int,
        fix_target_part: int,
    ) -> int:
        start_index = ordered_parts.index(start_part)
        invalidated_parts = ordered_parts[start_index:]
        for part_num in invalidated_parts:
            runtime_state.executed_parts_successfully.discard(part_num)
            runtime_state.store_params_by_part.pop(part_num, None)
        drop_tables = sorted(
            {
                table_name
                for part_num in invalidated_parts
                for table_name in runtime_state.expected_tables_by_part.get(part_num, set())
            }
        )
        if drop_tables:
            self._drop_runtime_tables(connection, drop_tables)
        resolver.values.update(self._flatten_store_params(runtime_state.store_params_by_part))
        report["replays"].append(
            {
                "after_failed_part": root_failed_part,
                "fix_target_part": fix_target_part,
                "rebuild_start_part": start_part,
                "strategy": "targeted_rebuild",
                "reason": reason,
            }
        )
        self._log_event(
            report,
            "rebuild_started",
            part=start_part,
            root_failed_part=root_failed_part,
            fix_target_part=fix_target_part,
            rebuild_start_part=start_part,
            rebuild_scope_end_part=ordered_parts[-1],
            reason=reason,
            dropped_tables=drop_tables,
        )
        self._update_runtime_status(
            status="running",
            phase="rebuild",
            part_num=start_part,
            rebuild_start_part=start_part,
            root_failed_part=root_failed_part,
            current_fix_target_part=fix_target_part,
            message=f"Targeted rebuild с part {start_part}",
        )
        return start_index

    def _replay_until_part(
        self,
        *,
        connection: Any,
        ordered_parts: List[int],
        target_index: int,
        part_sql_by_num: Dict[int, str],
        header: HeaderMetadata,
        report: Dict[str, Any],
    ) -> Dict[str, Any]:
        preparer = TrinoSQLPreparer(self.runtime_schema, header)
        preparer.discover_runtime_targets(part_sql_by_num.values())
        resolver = RuntimeVariableResolver.from_sql_parts(header, part_sql_by_num.values())
        report["runtime_tables"] = preparer.runtime_table_names()

        for index in range(target_index + 1):
            part_num = ordered_parts[index]
            sql = self._load_part_content(part_num, is_vertica=False)
            prepared_sql = resolver.substitute(preparer.rewrite_part_sql(sql))
            version = self.state_manager.get_latest_version_number(part_num)
            operation = {
                "part": part_num,
                "attempt": 0,
                "schema": self.runtime_schema,
                "phase": "trino_testing",
                "replay_target_index": target_index,
                "replay_target_part": ordered_parts[target_index],
            }
            log_entry = {
                "part": part_num,
                "version": version,
                "prepared_sql": prepared_sql,
                "declared_variables": dict(resolver.values),
                "runtime_tables": preparer.runtime_table_names(),
                "started_at": datetime.utcnow().isoformat(),
                "replay_target_part": ordered_parts[target_index],
            }
            try:
                if self._is_store_part(part_num):
                    self._log_event(
                        report,
                        "store_part_started",
                        part=part_num,
                        total_parts=len(ordered_parts),
                        version=version,
                        replay_target=ordered_parts[target_index],
                    )
                    self.state_manager.set_current_operation(
                        f"🧮 Trino @store Part {part_num}/{len(ordered_parts) - 1}",
                        {**operation, "store": True},
                    )
                    self._update_runtime_status(
                        status="running",
                        phase="executing_store",
                        part_num=part_num,
                        total_parts=len(ordered_parts),
                        replay_target_part=ordered_parts[target_index],
                        message=f"Выполняется @store part {part_num}",
                    )
                    store_result = self._execute_store_part(connection, prepared_sql, resolver)
                    log_entry.update({"status": store_result["status"], **store_result})
                    report["parts"].append({"part": part_num, "status": store_result["status"], "attempt": 0, **store_result})
                    self._log_event(
                        report,
                        "store_part_finished",
                        part=part_num,
                        total_parts=len(ordered_parts),
                        version=version,
                        replay_target=ordered_parts[target_index],
                        store_status=store_result["status"],
                        stored_params=store_result.get("stored_params", {}),
                    )
                else:
                    self._log_event(
                        report,
                        "sql_part_started",
                        part=part_num,
                        total_parts=len(ordered_parts),
                        version=version,
                        replay_target=ordered_parts[target_index],
                    )
                    self.state_manager.set_current_operation(
                        f"🧪 Trino runtime test Part {part_num}/{len(ordered_parts) - 1}",
                        operation,
                    )
                    self._update_runtime_status(
                        status="running",
                        phase="executing_part",
                        part_num=part_num,
                        total_parts=len(ordered_parts),
                        replay_target_part=ordered_parts[target_index],
                        message=f"Выполняется part {part_num}",
                    )
                    self._execute_sql(connection, prepared_sql)
                    log_entry["status"] = "ok"
                    report["parts"].append({"part": part_num, "status": "ok", "attempt": 0})
                    self._log_event(
                        report,
                        "sql_part_finished",
                        part=part_num,
                        total_parts=len(ordered_parts),
                        version=version,
                        replay_target=ordered_parts[target_index],
                    )
                log_entry["finished_at"] = datetime.utcnow().isoformat()
                report["part_execution"].append(log_entry)
                self._write_report(report)
            except Exception as exc:
                error_text = str(exc)
                log_entry["status"] = "error"
                log_entry["error"] = error_text
                log_entry["finished_at"] = datetime.utcnow().isoformat()
                report["part_execution"].append(log_entry)
                report["parts"].append({"part": part_num, "status": "error", "attempt": 0, "error": error_text})
                self._log_event(
                    report,
                    "part_execution_error",
                    part=part_num,
                    total_parts=len(ordered_parts),
                    version=version,
                    replay_target=ordered_parts[target_index],
                    error=error_text,
                )
                self._update_runtime_status(
                    status="failed_part",
                    phase="failed",
                    part_num=part_num,
                    total_parts=len(ordered_parts),
                    replay_target_part=ordered_parts[target_index],
                    error_text=error_text,
                    message=f"Ошибка выполнения part {part_num}",
                )
                self._write_report(report)
                return {"success": False, "failed_part": part_num, "error": error_text}

        return {"success": True}

    def _execute_sql(self, connection: Any, sql: str) -> None:
        cursor = connection.cursor()
        for statement in sqlparse.split(sql):
            statement = statement.strip().rstrip(";").strip()
            if statement:
                cursor.execute(statement)
                try:
                    cursor.fetchall()
                except Exception:
                    pass

    def _apply_runtime_sample_limit(self, sql: str) -> str:
        limit = getattr(settings, "trino_test_sample_limit", 0) or 0
        return RuntimeSampleLimiter.apply(sql, limit)

    def _execute_sql_with_transient_retries(
        self,
        *,
        connection: Any,
        sql: str,
        part_num: int,
        total_parts: int,
        version: int,
        runtime_state: RuntimeExecutionState,
        report: Dict[str, Any],
    ) -> None:
        for retry_index in range(self.TRANSIENT_EXECUTION_RETRIES + 1):
            try:
                self._execute_sql(connection, sql)
                return
            except Exception as exc:
                if retry_index >= self.TRANSIENT_EXECUTION_RETRIES or not self._is_transient_execution_error(str(exc)):
                    raise
                drop_tables = sorted(runtime_state.expected_tables_by_part.get(part_num, set()))
                if drop_tables:
                    self._drop_runtime_tables(connection, drop_tables)
                self._log_event(
                    report,
                    "transient_execution_retry",
                    part=part_num,
                    total_parts=total_parts,
                    version=version,
                    retry_attempt=retry_index + 1,
                    max_retries=self.TRANSIENT_EXECUTION_RETRIES,
                    dropped_tables=drop_tables,
                    error=str(exc),
                )
                self._update_runtime_status(
                    status="running",
                    phase="transient_retry",
                    part_num=part_num,
                    total_parts=total_parts,
                    error_text=str(exc),
                    message=f"Transient Trino execution error in part {part_num}; retry {retry_index + 1}",
                )

    def _is_transient_execution_error(self, error_text: str) -> bool:
        normalized = error_text.lower()
        markers = (
            "zero-length, empty document",
            "zero length, empty document",
            "expecting value: line 1 column 1",
            "empty response",
        )
        return any(marker in normalized for marker in markers)

    def _execute_store_part(
        self,
        connection: Any,
        sql: str,
        resolver: ParameterResolver,
    ) -> Dict[str, Any]:
        statement = self._single_statement(sql)
        if re.search(r"\bversion_id\b", statement, re.IGNORECASE):
            resolver.values["version_id"] = "1"
            return {
                "status": "ignored_version_id_store",
                "stored_params": {"version_id": "1"},
                "reason": "version_id is forbidden in Trino SQL",
            }
        if self._contains_forbidden_test_reads(statement):
            raise TrinoRuntimeError("@store cannot read from protected target schemas during Trino tests")

        cursor = connection.cursor()
        cursor.execute(statement)
        rows = cursor.fetchall()
        if not rows:
            return {"status": "store_empty", "stored_params": {}}

        column_names = [item[0] for item in (cursor.description or [])]
        if not column_names:
            return {"status": "store_no_columns", "stored_params": {}}

        stored_params = {}
        for name, value in zip(column_names, rows[0]):
            literal = self._value_to_sql_literal(value)
            resolver.values[name] = literal
            stored_params[name] = literal

        return {"status": "store_ok", "stored_params": stored_params}

    def _contains_forbidden_test_reads(self, sql: str) -> bool:
        for match in TABLE_REF_RE.finditer(sql):
            table_name = match.group("table")
            parts = [part.lower() for part in table_name.split(".")[:-1]]
            if any(part in FORBIDDEN_TEST_READ_SCHEMAS for part in parts):
                return True
        return False

    def _single_statement(self, sql: str) -> str:
        statements = [statement.strip().rstrip(";").strip() for statement in sqlparse.split(sql) if statement.strip()]
        if len(statements) != 1:
            raise TrinoRuntimeError(f"@store part must contain exactly one SELECT statement, got {len(statements)}")
        return statements[0]

    def _value_to_sql_literal(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        if hasattr(value, "isoformat"):
            return f"'{value.isoformat()}'"
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    def _is_store_part(self, part_num: int) -> bool:
        path = self.state_manager.vertica_parts_path / f"{self.query_name}_part_{part_num}.sql"
        if not path.exists():
            return False
        return "@store" in path.read_text(encoding="utf-8").lower()

    def _fix_part(
        self,
        connection: Any,
        part_num: int,
        error_text: str,
        header: HeaderMetadata,
        part_sql_by_num: Dict[int, str],
        runtime_state: RuntimeExecutionState,
        preparer: TrinoSQLPreparer,
        resolver: ParameterResolver,
        report: Dict[str, Any],
        attempt_num: int,
    ) -> Dict[str, Any]:
        session = self._start_repair_session(report, part_num, error_text, attempt_num)
        planner_result = self._plan_repair_or_raise(
            connection=connection,
            part_num=part_num,
            error_text=error_text,
            header=header,
            part_sql_by_num=part_sql_by_num,
            runtime_state=runtime_state,
            preparer=preparer,
            resolver=resolver,
            report=report,
            session=session,
        )

        action_results: List[Dict[str, Any]] = []
        if not planner_result.stop_and_fix_now and planner_result.actions:
            action_results.extend(
                self._execute_repair_actions(
                    connection=connection,
                    planner_result=planner_result,
                    runtime_state=runtime_state,
                    preparer=preparer,
                    report=report,
                    session=session,
                )
            )

        final_result = self._produce_final_repair_or_raise(
            connection=connection,
            part_num=part_num,
            error_text=error_text,
            header=header,
            part_sql_by_num=part_sql_by_num,
            runtime_state=runtime_state,
            preparer=preparer,
            resolver=resolver,
            report=report,
            session=session,
            planner_result=planner_result,
            action_results=action_results,
        )

        if final_result.need_more_actions:
            session.status = "repair_agent_exhausted"
            session.summary = "agent requested more actions after allowed rounds"
            self._log_event(
                report,
                "repair_session_exhausted",
                part=part_num,
                session_id=session.session_id,
                status=session.status,
            )
            self._finish_repair_session(report, session)
            raise TrinoRuntimeError("repair_agent_exhausted")

        return self._finalize_successful_repair(
            report=report,
            session=session,
            part_num=part_num,
            attempt_num=attempt_num,
            final_result=final_result,
        )

    def _start_repair_session(
        self,
        report: Dict[str, Any],
        part_num: int,
        error_text: str,
        attempt_num: int,
    ) -> RepairSession:
        session = RepairSession(
            session_id=f"{self.query_name}-part{part_num}-attempt{attempt_num}-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}",
            root_failed_part=part_num,
            initial_error=error_text,
        )
        self._log_event(
            report,
            "repair_session_started",
            part=part_num,
            session_id=session.session_id,
            root_failed_part=part_num,
            fix_attempt=attempt_num,
            status=session.status,
        )
        self._update_runtime_status(
            status="running",
            phase="repair_planning",
            part_num=part_num,
            root_failed_part=part_num,
            current_repair_session_id=session.session_id,
            current_repair_phase="planning",
            current_repair_target_part=part_num,
            current_repair_plan=None,
        )
        return session

    def _finish_repair_session(self, report: Dict[str, Any], session: RepairSession) -> None:
        report.setdefault("repair_sessions", []).append(
            {
                "session_id": session.session_id,
                "root_failed_part": session.root_failed_part,
                "initial_error": session.initial_error,
                "planner_output": session.planner_output,
                "actions": session.actions,
                "action_results": session.action_results,
                "final_fix_target_part": session.final_fix_target_part,
                "final_fix_file": session.final_fix_file,
                "summary": session.summary,
                "status": session.status,
            }
        )
        self._log_event(
            report,
            "repair_session_finished" if session.status == "success" else "repair_session_exhausted",
            part=session.root_failed_part,
            session_id=session.session_id,
            current_target_part=session.final_fix_target_part,
            status=session.status,
            summary=session.summary,
        )
        self._update_runtime_status(
            current_repair_session_id=None,
            current_repair_phase=None,
            current_repair_target_part=None,
            current_repair_plan=None,
            last_repair_summary=session.summary,
        )

    def _plan_repair(
        self,
        *,
        connection: Any,
        part_num: int,
        error_text: str,
        header: HeaderMetadata,
        part_sql_by_num: Dict[int, str],
        runtime_state: RuntimeExecutionState,
        preparer: TrinoSQLPreparer,
        resolver: ParameterResolver,
        report: Dict[str, Any],
        session: RepairSession,
    ) -> RepairPlannerResult:
        del connection, header, part_sql_by_num
        context = self._build_planner_context(
            part_num=part_num,
            error_text=error_text,
            runtime_state=runtime_state,
            preparer=preparer,
            resolver=resolver,
        )
        raw_text = self._call_raw_json_mode(context, "planner")
        planner_result = self._parse_repair_plan(raw_text, default_part=part_num)
        session.planner_output = {
            "hypothesis": planner_result.hypothesis,
            "target_part_candidate": planner_result.target_part_candidate,
            "actions": [
                {"tool": action.tool, "args": action.args, "purpose": action.purpose}
                for action in planner_result.actions
            ],
            "stop_and_fix_now": planner_result.stop_and_fix_now,
            "why": planner_result.why,
        }
        self._log_event(
            report,
            "repair_plan_generated",
            part=part_num,
            session_id=session.session_id,
            current_target_part=planner_result.target_part_candidate,
            status="success",
            hypothesis=planner_result.hypothesis,
        )
        self._update_runtime_status(
            current_repair_phase="planned",
            current_repair_target_part=planner_result.target_part_candidate,
            current_repair_plan=session.planner_output,
        )
        return planner_result

    def _execute_repair_actions(
        self,
        *,
        connection: Any,
        planner_result: RepairPlannerResult,
        runtime_state: RuntimeExecutionState,
        preparer: TrinoSQLPreparer,
        report: Dict[str, Any],
        session: RepairSession,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        db_tools = {"inspect_runtime_table", "inspect_information_schema", "run_diagnostic_query", "inspect_source_columns", "run_column_probe"}
        file_tools = {
            "read_trino_part",
            "read_trino_part_lines",
            "read_vertica_part",
            "list_part_versions",
            "diff_part_versions",
            "read_full_script",
            "search_parts",
        }
        db_actions_used = sum(1 for action in session.actions if action.get("tool") in db_tools)
        diagnostic_actions_used = sum(1 for action in session.actions if action.get("tool") == "run_diagnostic_query")
        file_action_counts: Dict[str, int] = {}
        for action in session.actions:
            if action.get("tool") in file_tools:
                file_action_counts[action["tool"]] = file_action_counts.get(action["tool"], 0) + 1

        for action_index, action in enumerate(planner_result.actions, start=len(session.actions) + 1):
            if len(session.actions) >= self.REPAIR_AGENT_MAX_ACTIONS:
                break
            if action.tool in db_tools and db_actions_used >= self.REPAIR_AGENT_MAX_DB_ACTIONS:
                break
            if action.tool == "run_diagnostic_query" and diagnostic_actions_used >= self.REPAIR_AGENT_MAX_DIAGNOSTIC_QUERIES:
                break
            if action.tool in file_tools and file_action_counts.get(action.tool, 0) >= 2:
                break

            action_record = {
                "index": action_index,
                "tool": action.tool,
                "args": action.args,
                "purpose": action.purpose,
            }
            session.actions.append(action_record)
            self._log_event(
                report,
                "repair_action_started",
                part=session.root_failed_part,
                session_id=session.session_id,
                action_index=action_index,
                tool=action.tool,
                tool_args=action.args,
                current_target_part=planner_result.target_part_candidate,
                status="started",
            )
            self._update_runtime_status(
                current_repair_phase="executing_action",
                current_repair_target_part=planner_result.target_part_candidate,
            )
            result = self._execute_repair_action(
                connection=connection,
                action=action,
                runtime_state=runtime_state,
                preparer=preparer,
            )
            if action.tool == "run_diagnostic_query":
                report.setdefault("diagnostic_queries", []).append(
                    {
                        "part": session.root_failed_part,
                        "attempt": action_index,
                        "query": action.args.get("sql"),
                        "result": result.get("result"),
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                )
            if action.tool in {"inspect_runtime_table", "inspect_information_schema"}:
                report.setdefault("introspection", []).append(
                    {
                        "part": session.root_failed_part,
                        "attempt": action_index,
                        "requested_items": [action.tool, action.args],
                        "result": result.get("result"),
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                )
            results.append(result)
            session.action_results.append(result)
            self._log_event(
                report,
                "repair_action_finished",
                part=session.root_failed_part,
                session_id=session.session_id,
                action_index=action_index,
                tool=action.tool,
                tool_args=action.args,
                status=result.get("status", "ok"),
            )
            if action.tool in db_tools:
                db_actions_used += 1
            if action.tool == "run_diagnostic_query":
                diagnostic_actions_used += 1
            if action.tool in file_tools:
                file_action_counts[action.tool] = file_action_counts.get(action.tool, 0) + 1
        return results

    def _produce_final_repair(
        self,
        *,
        connection: Any,
        part_num: int,
        error_text: str,
        header: HeaderMetadata,
        part_sql_by_num: Dict[int, str],
        runtime_state: RuntimeExecutionState,
        preparer: TrinoSQLPreparer,
        resolver: ParameterResolver,
        report: Dict[str, Any],
        session: RepairSession,
        planner_result: RepairPlannerResult,
        action_results: List[Dict[str, Any]],
    ) -> RepairFinalResult:
        del header, part_sql_by_num
        rounds = 0
        accumulated_results = list(action_results)
        while True:
            context = self._build_followup_context(
                part_num=part_num,
                error_text=error_text,
                runtime_state=runtime_state,
                preparer=preparer,
                resolver=resolver,
                session=session,
                planner_result=planner_result,
                action_results=accumulated_results,
            )
            try:
                raw_text = self._call_raw_json_mode(context, "final_fix")
                final_result = self._parse_final_fix(raw_text, default_part=planner_result.target_part_candidate)
            except Exception as exc:
                if self._is_repair_contract_error(exc):
                    self._log_event(
                        report,
                        "repair_contract_fallback_requested",
                        part=part_num,
                        session_id=session.session_id,
                        current_target_part=planner_result.target_part_candidate,
                        error=str(exc),
                    )
                    return self._request_full_rewrite_after_contract_failure(
                        context=context,
                        contract_error=str(exc),
                        default_part=planner_result.target_part_candidate,
                    )
                raise
            if not final_result.need_more_actions:
                return final_result
            if rounds >= self.REPAIR_AGENT_MAX_FOLLOWUP_ROUNDS or not final_result.actions:
                return final_result
            self._log_event(
                report,
                "repair_additional_context_requested",
                part=part_num,
                session_id=session.session_id,
                current_target_part=planner_result.target_part_candidate,
                status="requested",
            )
            followup_planner = RepairPlannerResult(
                hypothesis=planner_result.hypothesis,
                target_part_candidate=planner_result.target_part_candidate,
                actions=final_result.actions,
                stop_and_fix_now=False,
                why=planner_result.why,
            )
            new_results = self._execute_repair_actions(
                connection=connection,
                planner_result=followup_planner,
                runtime_state=runtime_state,
                preparer=preparer,
                report=report,
                session=session,
            )
            accumulated_results.extend(new_results)
            rounds += 1

    def _request_full_rewrite_after_contract_failure(
        self,
        *,
        context: str,
        contract_error: str,
        default_part: int,
    ) -> RepairFinalResult:
        fallback_context = "\n".join(
            [
                context,
                "",
                "=== CONTRACT FAILURE FALLBACK ===",
                f"Your previous answer could not be applied: {contract_error}",
                "Do not return edits.",
                "Return ONLY valid JSON with change_type=\"full_rewrite\".",
                "fixed_sql must contain the full corrected SQL for the entire target part.",
                "Preserve all unrelated logic unchanged while rewriting the part.",
            ]
        )
        raw_text = self._call_raw_json_mode(fallback_context, "full_rewrite_fallback")
        payload = self._extract_json_payload(raw_text)
        fixed_sql = payload.get("fixed_sql")
        if not isinstance(fixed_sql, str) or not fixed_sql.strip():
            raise TrinoRuntimeError("full_rewrite_fallback must contain fixed_sql")
        if payload.get("edits"):
            raise TrinoRuntimeError("full_rewrite_fallback must not contain edits")
        payload["change_type"] = "full_rewrite"
        return self._parse_final_fix(json.dumps(payload, ensure_ascii=False), default_part=default_part)

    def _call_raw_json_mode(self, context: str, mode: str) -> str:
        raw_text = self.raw_repair_client.complete(context)
        try:
            self._extract_json_payload(raw_text)
            return raw_text
        except Exception as first_exc:
            retry_context = "\n".join(
                [
                    context,
                    "",
                    "=== JSON FORMAT RETRY ===",
                    f"Previous {mode} output was not valid JSON: {first_exc}",
                    "Return only valid JSON according to the schema above. No markdown, no comments, no SQL outside JSON.",
                ]
            )
            retry_text = self.raw_repair_client.complete(retry_context)
            self._extract_json_payload(retry_text)
            return retry_text

    def _load_prompt_template(self, filename: str) -> str:
        path = settings.golden_dataset_path / filename
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8").strip()

    def _optimization_guidance_for_error(self, error_text: str) -> str:
        if not self._is_resource_execution_error(error_text):
            return ""
        return self._load_prompt_template("runtime_repair_cpu_optimization_prompt.md")

    def _is_repair_contract_error(self, error: Exception | str) -> bool:
        message = str(error)
        markers = (
            "new must be a single line",
            "new_lines must",
            "SQL line edit",
            "replace_line old mismatch",
            "replace_range old_lines mismatch",
            "replace_range out of range",
            "replace_line line out of range",
            "insert_after_line out of range",
            "Final repair result must contain exactly one of edits or fixed_sql",
            "Line patch must contain at least one edit",
        )
        return any(marker in message for marker in markers)

    def _is_resource_execution_error(self, error_text: str) -> bool:
        upper_error = error_text.upper()
        resource_markers = (
            "EXCEEDED_CPU_LIMIT",
            "INSUFFICIENT_RESOURCES",
            "EXCEEDED_LOCAL_MEMORY_LIMIT",
        )
        return any(marker in upper_error for marker in resource_markers)

    def _runtime_error_signature(self, error_text: str) -> str:
        normalized = " ".join(str(error_text or "").split())
        normalized = re.sub(r"query_id\s*=\s*[^,)]+", "query_id=<redacted>", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"\bQuery [0-9A-Za-z_:-]+\b", "Query <redacted>", normalized, flags=re.IGNORECASE)
        return normalized or "<empty_error>"

    def _compact_knowledge_for_repair(self, knowledge: Dict[str, Any], part_num: int) -> Dict[str, Any]:
        compact: Dict[str, Any] = {}
        for key in ("pattern_guard_history", "api_validation_history", "runtime_test_history", "llm_fix_history"):
            if key in knowledge:
                compact[key] = self._compact_history_items(knowledge.get(key), part_num)
        return compact

    def _compact_history_items(self, value: Any, part_num: int) -> Any:
        if isinstance(value, list):
            relevant = [item for item in value if self._history_item_matches_part(item, part_num)]
            selected = relevant[-5:] if relevant else value[-3:]
            return [self._truncate_prompt_value(item, 900) for item in selected]
        if isinstance(value, dict):
            direct = value.get(f"part_{part_num}") or value.get(str(part_num)) or value.get(part_num)
            if direct is not None:
                return self._truncate_prompt_value(direct, 1800)
            return self._truncate_prompt_value(value, 1800)
        return self._truncate_prompt_value(value, 900)

    def _history_item_matches_part(self, item: Any, part_num: int) -> bool:
        if not isinstance(item, dict):
            return False
        candidates = {
            item.get("part"),
            item.get("part_num"),
            item.get("target_part"),
            item.get("fix_target_part"),
        }
        return part_num in candidates or str(part_num) in candidates

    def _truncate_prompt_value(self, value: Any, limit: int) -> Any:
        if isinstance(value, str):
            return value if len(value) <= limit else f"{value[:limit]}... [truncated]"
        text = json.dumps(value, ensure_ascii=False, default=str)
        if len(text) <= limit:
            return value
        return f"{text[:limit]}... [truncated]"

    def _expanded_repair_context(
        self,
        *,
        part_num: int,
        related_parts: Dict[str, Any],
        report_limit: int = 5,
    ) -> Dict[str, Any]:
        state = self.state_manager.load_state() or {}
        intent_memory = ((state.get("parts") or {}).get("intent_memory") or {})
        related_part_nums = self._related_part_numbers(related_parts)
        part_keys = [f"part_{part_num}"] + [f"part_{num}" for num in related_part_nums]
        intents = {
            key: self._truncate_prompt_value(intent_memory.get(key), 2400)
            for key in part_keys
            if intent_memory.get(key) is not None
        }
        return {
            "current_and_related_part_intents": intents,
            "recent_repair_sessions": self._recent_repair_sessions(limit=report_limit),
            "recent_fix_attempts": self._recent_fix_attempts(part_num=part_num, limit=report_limit),
        }

    def _related_part_numbers(self, related_parts: Dict[str, Any]) -> List[int]:
        result: List[int] = []
        for section in ("upstream", "downstream"):
            for item in related_parts.get(section, []) or []:
                try:
                    value = int(item.get("part_num"))
                except (TypeError, ValueError, AttributeError):
                    continue
                if value not in result:
                    result.append(value)
        return result[:8]

    def _recent_repair_sessions(self, limit: int) -> List[Any]:
        path = self._report_path("trino_test_report.json")
        if not path.exists():
            return []
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
        sessions = report.get("repair_sessions") or []
        return [self._truncate_prompt_value(session, 2200) for session in sessions[-limit:]]

    def _recent_fix_attempts(self, *, part_num: int, limit: int) -> List[Any]:
        path = self.state_manager.work_dir / "logs" / "fix_attempt_journal.json"
        if not path.exists():
            return []
        try:
            journal = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
        if not isinstance(journal, list):
            return []
        relevant = [
            item for item in journal
            if isinstance(item, dict)
            and (item.get("target_part") == part_num or item.get("part") == part_num or str(item.get("target_part")) == str(part_num))
        ]
        selected = relevant[-limit:] if relevant else journal[-min(limit, 3):]
        return [self._truncate_prompt_value(item, 2400) for item in selected]

    def _build_planner_context(
        self,
        *,
        part_num: int,
        error_text: str,
        runtime_state: RuntimeExecutionState,
        preparer: TrinoSQLPreparer,
        resolver: ParameterResolver,
    ) -> str:
        trino_sql = self._load_part_content(part_num, is_vertica=False)
        prepared_sql = resolver.substitute(preparer.rewrite_part_sql(trino_sql))
        runtime_snapshot = self._read_runtime_state_snapshot(runtime_state)
        related_parts = self._list_related_parts(part_num)
        dependencies = self._read_part_dependencies(part_num)
        runtime_failure_invariants = self._runtime_failure_invariants(part_num, error_text, runtime_state)
        optimization_guidance = self._optimization_guidance_for_error(error_text)
        knowledge = (self.state_manager.load_state() or {}).get("knowledge", {})
        compact_knowledge = self._compact_knowledge_for_repair(knowledge, part_num)
        expanded_context = self._expanded_repair_context(part_num=part_num, related_parts=related_parts)
        return "\n".join(
            [
                self._load_prompt_template("runtime_repair_planner_prompt.md"),
                optimization_guidance,
                "Return ONLY valid JSON.",
                "",
                "Planner JSON schema:",
                json.dumps(
                    {
                        "hypothesis": "short probable cause",
                        "target_part_candidate": part_num,
                        "actions": [
                            {"tool": "read_trino_part", "args": {"part_num": part_num}, "purpose": "why"},
                        ],
                        "stop_and_fix_now": False,
                        "why": "why this target part",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                "",
                "Allowed tools:",
                "read_trino_part(part_num): current translated SQL of another part. Example: {'tool':'read_trino_part','args':{'part_num':4},'purpose':'inspect producer SQL'}",
                "read_trino_part_lines(part_num, start_line, end_line): current translated SQL with 1-based line numbers. Example: {'tool':'read_trino_part_lines','args':{'part_num':4,'start_line':20,'end_line':45},'purpose':'inspect exact patch range'}",
                "read_vertica_part(part_num): original Vertica logic of another part. Example: {'tool':'read_vertica_part','args':{'part_num':4},'purpose':'compare original logic'}",
                "list_part_versions(part_num): list all saved Trino versions. Example: {'tool':'list_part_versions','args':{'part_num':15},'purpose':'find last sane version'}",
                "diff_part_versions(part_num, from_version, to_version): compare two versions. Example: {'tool':'diff_part_versions','args':{'part_num':15,'from_version':2,'to_version':3},'purpose':'see bad rewrite'}",
                "read_part_dependencies(part_num): producers/consumers of a part. Example: {'tool':'read_part_dependencies','args':{'part_num':6},'purpose':'understand lineage'}",
                "read_part_intent(part_num): compact memory of created/read tables, aliases and columns. Example: {'tool':'read_part_intent','args':{'part_num':23},'purpose':'check alias map'}",
                "read_runtime_state(): current runtime execution snapshot. Example: {'tool':'read_runtime_state','args':{},'purpose':'see executed parts and runtime tables'}",
                "read_full_script(kind): read assembled trino or vertica script. Example: {'tool':'read_full_script','args':{'kind':'trino'},'purpose':'understand global flow'}",
                "search_parts(query, kind): search in trino, vertica or intent. Example: {'tool':'search_parts','args':{'query':'gos_client','kind':'trino'},'purpose':'find producer/consumer'}",
                "inspect_alias_sources(part_num): parse aliases in a part. Example: {'tool':'inspect_alias_sources','args':{'part_num':23},'purpose':'resolve ctc alias'}",
                "resolve_column_reference(part_num, reference): resolve alias.column to a source table. Example: {'tool':'resolve_column_reference','args':{'part_num':23,'reference':'ctc.isfraud'},'purpose':'find source table'}",
                "inspect_source_columns(table_name): read information_schema columns for a source table. Example: {'tool':'inspect_source_columns','args':{'table_name':'analytics_src.activity_events_trino'},'purpose':'check available columns'}",
                "suggest_column_candidates(missing_column, available_columns): fuzzy column suggestions. Example: {'tool':'suggest_column_candidates','args':{'missing_column':'isfraud','available_columns':['is_fraud']},'purpose':'find renamed column'}",
                "inspect_runtime_table(table_name): sample rows/columns of runtime table. Example: {'tool':'inspect_runtime_table','args':{'table_name':'users'},'purpose':'see created runtime table'}",
                "inspect_information_schema(table_name_pattern): check which schema/table exists. Example: {'tool':'inspect_information_schema','args':{'table_name_pattern':'users'},'purpose':'find actual schema'}",
                "run_diagnostic_query(sql): temporary read-only SQL investigation. Example: {'tool':'run_diagnostic_query','args':{'sql':'SELECT * FROM runtime_schema.users LIMIT 5'},'purpose':'validate hypothesis'}",
                "run_column_probe(sql): read-only column/schema probe. Example: {'tool':'run_column_probe','args':{'sql':'SELECT is_flagged FROM analytics_src.activity_events_trino LIMIT 1'},'purpose':'verify renamed column'}",
                "list_related_parts(part_num): direct upstream/downstream parts. Example: {'tool':'list_related_parts','args':{'part_num':6},'purpose':'choose which file to inspect next'}",
                "",
                f"Failed part: {part_num}",
                f"Runtime schema: {self._get_runtime_schema()}",
                "Current Trino SQL of failed part:",
                trino_sql,
                "",
                "Prepared SQL that failed:",
                prepared_sql,
                "",
                "Exact Trino error:",
                error_text,
                "",
                "Direct dependencies summary:",
                json.dumps(dependencies, ensure_ascii=False, indent=2, default=str),
                "",
                "Direct related parts:",
                json.dumps(related_parts, ensure_ascii=False, indent=2, default=str),
                "",
                "Runtime failure invariants:",
                json.dumps(runtime_failure_invariants, ensure_ascii=False, indent=2, default=str),
                "",
                "Runtime state snapshot:",
                json.dumps(runtime_snapshot, ensure_ascii=False, indent=2, default=str),
                "",
                "Relevant compact evidence:",
                json.dumps(
                    MigrationKnowledgeRegistry(self.state_manager).evidence_for(
                        part_num=part_num,
                        problem_text=error_text,
                        limit=8,
                    ),
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                ),
                "",
                "Expanded repair memory:",
                json.dumps(expanded_context, ensure_ascii=False, indent=2, default=str),
                "",
                "Compact pattern/API/runtime history:",
                json.dumps(compact_knowledge, ensure_ascii=False, indent=2, default=str),
            ]
        )

    def _build_followup_context(
        self,
        *,
        part_num: int,
        error_text: str,
        runtime_state: RuntimeExecutionState,
        preparer: TrinoSQLPreparer,
        resolver: ParameterResolver,
        session: RepairSession,
        planner_result: RepairPlannerResult,
        action_results: List[Dict[str, Any]],
    ) -> str:
        trino_sql = self._load_part_content(part_num, is_vertica=False)
        prepared_sql = resolver.substitute(preparer.rewrite_part_sql(trino_sql))
        optimization_guidance = self._optimization_guidance_for_error(error_text)
        return "\n".join(
            [
                self._load_prompt_template("runtime_repair_final_prompt.md"),
                optimization_guidance,
                "Return ONLY valid JSON.",
                "",
                "Final fix JSON schema:",
                json.dumps(
                    {
                        "target_part": planner_result.target_part_candidate,
                        "change_type": "line_patch",
                        "edits": [
                            {
                                "op": "replace_line",
                                "line": 33,
                                "old": "    CAST(bd.date_create AS DATE),",
                                "new": "    CAST(bd.date_create AS DATE) AS date_create,",
                            }
                        ],
                        "reason": "why this fix is correct",
                        "summary": "what was fixed",
                        "confidence": 0.75,
                        "used_evidence": ["key observation 1", "key observation 2"],
                        "expected_preserved_invariants": ["same created table", "same output contract"],
                        "risk_notes": ["any remaining uncertainty"],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                "Use line_patch with edits for local fixes. Use full_rewrite with fixed_sql only when the part is structurally broken.",
                "Return exactly one repair body: either non-empty edits or non-empty fixed_sql, never both.",
                "If you still need more investigation, return:",
                json.dumps(
                    {
                        "need_more_actions": True,
                        "actions": [
                            {"tool": "inspect_information_schema", "args": {"table_name_pattern": "users"}, "purpose": "verify schema"},
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                "",
                f"Failed part: {part_num}",
                f"Planner target candidate: {planner_result.target_part_candidate}",
                f"Runtime schema: {self._get_runtime_schema()}",
                "Current Trino SQL of failed part with 1-based line numbers:",
                self._format_sql_with_line_numbers(trino_sql),
                "",
                "Prepared SQL that failed:",
                prepared_sql,
                "",
                "Exact Trino error:",
                error_text,
                "",
                "Planner output:",
                json.dumps(session.planner_output, ensure_ascii=False, indent=2, default=str),
                "",
                "Investigation results:",
                json.dumps(action_results, ensure_ascii=False, indent=2, default=str),
                "",
                "Runtime state snapshot:",
                json.dumps(self._read_runtime_state_snapshot(runtime_state), ensure_ascii=False, indent=2, default=str),
            ]
        )

    def _parse_repair_plan(self, raw_text: Optional[str], *, default_part: int) -> RepairPlannerResult:
        payload = self._extract_json_payload(raw_text)
        actions: List[RepairAction] = []
        for raw_action in payload.get("actions", []) or []:
            if not isinstance(raw_action, dict):
                raise TrinoRuntimeError("Malformed repair action")
            tool = raw_action.get("tool")
            if tool not in self.REPAIR_AGENT_ALLOWED_TOOLS:
                raise TrinoRuntimeError(f"Unknown repair tool: {tool}")
            args = raw_action.get("args") or {}
            if not isinstance(args, dict):
                raise TrinoRuntimeError(f"Malformed args for tool: {tool}")
            actions.append(
                RepairAction(
                    tool=tool,
                    args=args,
                    purpose=str(raw_action.get("purpose") or "").strip(),
                )
            )
        return RepairPlannerResult(
            hypothesis=str(payload.get("hypothesis") or "").strip() or "unspecified",
            target_part_candidate=self._normalize_part_num(payload.get("target_part_candidate"), default_part),
            actions=actions,
            stop_and_fix_now=bool(payload.get("stop_and_fix_now")),
            why=str(payload.get("why") or "").strip() or "unspecified",
        )

    def _parse_final_fix(self, raw_text: Optional[str], *, default_part: int) -> RepairFinalResult:
        payload = self._extract_json_payload(raw_text)
        if payload.get("need_more_actions"):
            actions: List[RepairAction] = []
            for raw_action in payload.get("actions", []) or []:
                if not isinstance(raw_action, dict):
                    continue
                tool = raw_action.get("tool")
                if tool not in self.REPAIR_AGENT_ALLOWED_TOOLS:
                    continue
                args = raw_action.get("args") or {}
                if not isinstance(args, dict):
                    continue
                actions.append(RepairAction(tool=tool, args=args, purpose=str(raw_action.get("purpose") or "").strip()))
            return RepairFinalResult(
                target_part=default_part,
                fixed_sql="",
                summary="need_more_actions",
                confidence=0.0,
                used_evidence=[],
                need_more_actions=True,
                actions=actions,
            )
        target_part = self._normalize_part_num(payload.get("target_part"), default_part)
        raw_fixed_sql = payload.get("fixed_sql")
        has_fixed_sql = isinstance(raw_fixed_sql, str) and bool(raw_fixed_sql.strip())
        raw_edits = payload.get("edits") or []
        has_edits = isinstance(raw_edits, list) and bool(raw_edits)
        if has_fixed_sql and has_edits:
            raise TrinoRuntimeError("Final repair result must contain exactly one of edits or fixed_sql")
        if not has_fixed_sql and not has_edits:
            raise TrinoRuntimeError("Final repair result must contain exactly one of edits or fixed_sql")

        edits: List[SQLLineEdit] = []
        if has_edits:
            edits = self._parse_sql_line_edits(raw_edits)
            old_sql = self._load_part_content(target_part, is_vertica=False)
            fixed_sql = SQLPatchApplier.apply(old_sql, edits)
        else:
            fixed_sql = raw_fixed_sql
        used_evidence = payload.get("used_evidence") or []
        if not isinstance(used_evidence, list):
            used_evidence = []
        confidence = payload.get("confidence", 0.0)
        try:
            confidence_value = float(confidence)
        except (TypeError, ValueError):
            confidence_value = 0.0
        return RepairFinalResult(
            target_part=target_part,
            fixed_sql=fixed_sql,
            summary=str(payload.get("summary") or "").strip() or "repair_agent_fix",
            confidence=confidence_value,
            used_evidence=[str(item) for item in used_evidence],
            edits=edits,
            change_type=str(payload.get("change_type") or ("line_patch" if has_edits else "full_rewrite")).strip()
            or ("line_patch" if has_edits else "full_rewrite"),
            reason=str(payload.get("reason") or "").strip(),
            expected_preserved_invariants=[str(item) for item in (payload.get("expected_preserved_invariants") or []) if item is not None],
            risk_notes=[str(item) for item in (payload.get("risk_notes") or []) if item is not None],
        )

    def _parse_sql_line_edits(self, raw_edits: List[Any]) -> List[SQLLineEdit]:
        edits: List[SQLLineEdit] = []
        for raw_edit in raw_edits:
            if not isinstance(raw_edit, dict):
                raise TrinoRuntimeError("SQL line edit must be an object")
            op = str(raw_edit.get("op") or "").strip()
            if op not in SQLPatchApplier.VALID_OPS:
                raise TrinoRuntimeError(f"Unknown SQL line edit op: {op}")
            edits.append(
                SQLLineEdit(
                    op=op,
                    line=self._optional_int(raw_edit.get("line")),
                    start_line=self._optional_int(raw_edit.get("start_line")),
                    end_line=self._optional_int(raw_edit.get("end_line")),
                    after_line=self._optional_int(raw_edit.get("after_line")),
                    old=raw_edit.get("old") if isinstance(raw_edit.get("old"), str) else None,
                    old_lines=self._string_list(raw_edit.get("old_lines")),
                    new=raw_edit.get("new") if isinstance(raw_edit.get("new"), str) else None,
                    new_lines=self._string_list(raw_edit.get("new_lines"), fallback=raw_edit.get("new")),
                )
            )
        return edits

    @staticmethod
    def _optional_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str) and re.fullmatch(r"-?\d+", value.strip()):
            return int(value.strip())
        return None

    @staticmethod
    def _string_list(value: Any, *, fallback: Any = None) -> List[str]:
        if isinstance(value, list):
            return [item for item in value if isinstance(item, str)]
        if isinstance(fallback, str):
            return [fallback]
        return []

    def _extract_json_payload(self, raw_text: Optional[str]) -> Dict[str, Any]:
        if not raw_text or not isinstance(raw_text, str):
            raise TrinoRuntimeError("LLM did not return JSON output")
        text = raw_text.strip()
        match = JSON_BLOCK_RE.search(text)
        candidate = match.group(1) if match else text
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            sanitized_candidate = self._escape_json_control_chars(candidate)
            if sanitized_candidate != candidate:
                try:
                    payload = json.loads(sanitized_candidate)
                except json.JSONDecodeError:
                    payload = None
                else:
                    if not isinstance(payload, dict):
                        raise TrinoRuntimeError("LLM JSON payload must be an object")
                    return payload
            start = candidate.find("{")
            end = candidate.rfind("}")
            if start == -1 or end == -1 or end <= start:
                raise TrinoRuntimeError("LLM output is not valid JSON")
            candidate_object = candidate[start : end + 1]
            try:
                payload = json.loads(candidate_object)
            except json.JSONDecodeError:
                sanitized_object = self._escape_json_control_chars(candidate_object)
                payload = json.loads(sanitized_object)
        if not isinstance(payload, dict):
            raise TrinoRuntimeError("LLM JSON payload must be an object")
        return payload

    def _escape_json_control_chars(self, text: str) -> str:
        """Экранирует сырые control chars внутри JSON strings, не меняя остальную разметку."""
        escaped: List[str] = []
        in_string = False
        backslash = False
        substitutions = {
            "\b": "\\b",
            "\f": "\\f",
            "\n": "\\n",
            "\r": "\\r",
            "\t": "\\t",
        }

        for char in text:
            if in_string:
                if backslash:
                    escaped.append(char)
                    backslash = False
                    continue
                if char == "\\":
                    escaped.append(char)
                    backslash = True
                    continue
                if char == '"':
                    escaped.append(char)
                    in_string = False
                    continue
                if ord(char) < 0x20:
                    escaped.append(substitutions.get(char, f"\\u{ord(char):04x}"))
                    continue
                escaped.append(char)
                continue

            escaped.append(char)
            if char == '"':
                in_string = True

        return "".join(escaped)

    def _execute_repair_action(
        self,
        *,
        connection: Any,
        action: RepairAction,
        runtime_state: RuntimeExecutionState,
        preparer: TrinoSQLPreparer,
    ) -> Dict[str, Any]:
        try:
            if action.tool == "read_trino_part":
                result = {"content": self._load_part_content(self._normalize_part_num(action.args.get("part_num")), is_vertica=False)}
            elif action.tool == "read_trino_part_lines":
                result = self._read_trino_part_lines(
                    self._normalize_part_num(action.args.get("part_num")),
                    start_line=self._optional_int(action.args.get("start_line")),
                    end_line=self._optional_int(action.args.get("end_line")),
                )
            elif action.tool == "read_vertica_part":
                result = {"content": self._load_part_content(self._normalize_part_num(action.args.get("part_num")), is_vertica=True)}
            elif action.tool == "list_part_versions":
                result = self._list_part_versions(self._normalize_part_num(action.args.get("part_num")))
            elif action.tool == "diff_part_versions":
                result = self._diff_part_versions(
                    self._normalize_part_num(action.args.get("part_num")),
                    action.args.get("from_version"),
                    action.args.get("to_version"),
                )
            elif action.tool == "read_part_dependencies":
                result = self._read_part_dependencies(self._normalize_part_num(action.args.get("part_num")))
            elif action.tool == "read_part_intent":
                result = self._read_part_intent(self._normalize_part_num(action.args.get("part_num")))
            elif action.tool == "read_runtime_state":
                result = self._read_runtime_state_snapshot(runtime_state)
            elif action.tool == "read_full_script":
                result = self._read_full_script(str(action.args.get("kind") or "trino"))
            elif action.tool == "search_parts":
                result = self._search_parts(str(action.args.get("query") or ""), str(action.args.get("kind") or "trino"))
            elif action.tool == "inspect_alias_sources":
                result = self._inspect_alias_sources(self._normalize_part_num(action.args.get("part_num")))
            elif action.tool == "resolve_column_reference":
                result = self._resolve_column_reference(
                    self._normalize_part_num(action.args.get("part_num")),
                    str(action.args.get("reference") or ""),
                )
            elif action.tool == "inspect_source_columns":
                result = self._inspect_source_columns(connection, str(action.args.get("table_name") or ""))
            elif action.tool == "suggest_column_candidates":
                result = self._suggest_column_candidates(
                    str(action.args.get("missing_column") or ""),
                    action.args.get("available_columns") or [],
                )
            elif action.tool == "inspect_runtime_table":
                table_name = str(action.args.get("table_name") or "")
                available = sorted(runtime_state.runtime_tables_created)
                if not available:
                    for tables in runtime_state.expected_tables_by_part.values():
                        available.extend(tables)
                result = self._inspect_table(connection, table_name, available)
                expected_creator = self._expected_creator_for_table(table_name, runtime_state)
                if expected_creator is not None:
                    expected_table = table_name.split(".")[-1]
                    result["expected_creator_part"] = expected_creator
                    result["expected_runtime_table"] = expected_table
                    result["producer_contract"] = (
                        f"Producer part {expected_creator} is expected to create runtime table "
                        f"`{expected_table}` before consumers read it."
                    )
                    if result.get("status") == "error":
                        result["guidance"] = (
                            "The table is missing at runtime. Inspect the producer part and verify that it "
                            "materializes the expected table instead of only returning a SELECT/CTE."
                        )
            elif action.tool == "inspect_information_schema":
                result = self._inspect_information_schema(connection, str(action.args.get("table_name_pattern") or ""))
            elif action.tool == "run_diagnostic_query":
                result = self._run_diagnostic_query(connection, str(action.args.get("sql") or ""), -1)
            elif action.tool == "run_column_probe":
                result = self._run_diagnostic_query(connection, str(action.args.get("sql") or ""), -1)
            elif action.tool == "list_related_parts":
                result = self._list_related_parts(self._normalize_part_num(action.args.get("part_num")))
            else:
                raise TrinoRuntimeError(f"Unsupported repair tool: {action.tool}")
            return {
                "tool": action.tool,
                "args": action.args,
                "purpose": action.purpose,
                "status": "ok",
                "result": result,
            }
        except Exception as exc:
            return {
                "tool": action.tool,
                "args": action.args,
                "purpose": action.purpose,
                "status": "error",
                "error": str(exc),
            }

    def _list_part_versions(self, part_num: int) -> Dict[str, Any]:
        versions = []
        for path in self.state_manager.get_translation_version_paths(part_num):
            version_match = re.search(r"_v(?P<version>\d+)\.sql$", path.name)
            versions.append(
                {
                    "version": int(version_match.group("version")) if version_match else 0,
                    "path": str(path),
                    "bytes": path.stat().st_size,
                }
            )
        return {"part_num": part_num, "versions": versions}

    def _diff_part_versions(self, part_num: int, from_version: Any, to_version: Any) -> Dict[str, Any]:
        paths = {entry["version"]: Path(entry["path"]) for entry in self._list_part_versions(part_num)["versions"]}
        from_v = self._normalize_part_num(from_version, 0)
        to_v = self._normalize_part_num(to_version, self.state_manager.get_latest_version_number(part_num))
        if from_v not in paths or to_v not in paths:
            raise TrinoRuntimeError(f"Requested versions are not available for part {part_num}: {from_v}, {to_v}")
        old = paths[from_v].read_text(encoding="utf-8")
        new = paths[to_v].read_text(encoding="utf-8")
        diff = list(difflib.unified_diff(old.splitlines(), new.splitlines(), fromfile=paths[from_v].name, tofile=paths[to_v].name, lineterm="", n=3))
        return {"part_num": part_num, "from_version": from_v, "to_version": to_v, "diff": diff[:200]}

    def _read_part_intent(self, part_num: int) -> Dict[str, Any]:
        state = self.state_manager.load_state() or {}
        memory = ((state.get("parts") or {}).get("intent_memory") or {})
        cached = memory.get(f"part_{part_num}")
        if cached:
            return cached
        return PartIntentMemory(self.state_manager).update_part(part_num)

    def _read_full_script(self, kind: str) -> Dict[str, Any]:
        kind = kind.lower()
        if kind == "trino":
            path = self.state_manager.work_dir / "final" / f"{self.query_name}_final.sql"
            if path.exists():
                content = path.read_text(encoding="utf-8")
            else:
                content = "\n\n".join(self._load_part_content(i, is_vertica=False) for i in range((self.state_manager.load_state() or {}).get("total_parts", 0)))
        elif kind == "vertica":
            content = "\n\n".join(self._load_part_content(i, is_vertica=True) for i in range((self.state_manager.load_state() or {}).get("total_parts", 0)))
        else:
            raise TrinoRuntimeError("kind must be trino or vertica")
        return {"kind": kind, "content": content[:30000], "truncated": len(content) > 30000}

    def _search_parts(self, query: str, kind: str) -> Dict[str, Any]:
        if not query.strip():
            raise TrinoRuntimeError("query is required")
        total_parts = (self.state_manager.load_state() or {}).get("total_parts", 0)
        query_lower = query.lower()
        matches = []
        for part_num in range(total_parts):
            if kind == "intent":
                content = json.dumps(self._read_part_intent(part_num), ensure_ascii=False, default=str)
            else:
                content = self._safe_load_part_content(part_num, is_vertica=(kind == "vertica")) or ""
            if query_lower in content.lower():
                matches.append({"part_num": part_num, "preview": self._match_preview(content, query_lower)})
        return {"query": query, "kind": kind, "matches": matches[:50]}

    def _inspect_alias_sources(self, part_num: int) -> Dict[str, Any]:
        intent = PartIntentMemory(self.state_manager).extract_intent(self._load_part_content(part_num, is_vertica=False))
        return {"part_num": part_num, "alias_sources": intent.get("alias_sources", {})}

    def _resolve_column_reference(self, part_num: int, reference: str) -> Dict[str, Any]:
        if "." not in reference:
            raise TrinoRuntimeError("reference must be alias.column")
        alias, column = reference.split(".", 1)
        aliases = self._inspect_alias_sources(part_num)["alias_sources"]
        table = aliases.get(alias)
        return {"part_num": part_num, "reference": reference, "alias": alias, "column": column, "source_table": table}

    def _inspect_source_columns(self, connection: Any, table_name: str) -> Dict[str, Any]:
        qualified = table_name.strip()
        if not qualified:
            raise TrinoRuntimeError("table_name is required")
        parts = qualified.split(".")
        if len(parts) == 1:
            schema, table = self._get_runtime_schema(), parts[0]
        elif len(parts) == 2:
            schema, table = parts
        else:
            schema, table = parts[-2], parts[-1]
        rows = self._rows(
            connection,
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            f"WHERE lower(table_schema) = lower('{self._sql_literal(schema)}') "
            f"AND lower(table_name) = lower('{self._sql_literal(table)}') "
            "ORDER BY ordinal_position",
        )
        return {
            "table_name": qualified,
            "schema": schema,
            "table": table,
            "columns": [{"name": str(row[0]), "type": str(row[1])} for row in rows],
        }

    def _suggest_column_candidates(self, missing_column: str, available_columns: Any) -> Dict[str, Any]:
        if isinstance(available_columns, dict):
            available_columns = [item.get("name") for item in available_columns.get("columns", [])]
        normalized_available = [str(item) for item in available_columns if item]
        target = self._normalize_identifier_for_match(missing_column)
        scored = []
        for column in normalized_available:
            normalized = self._normalize_identifier_for_match(column)
            score = difflib.SequenceMatcher(None, target, normalized).ratio()
            if target == normalized:
                score = 1.0
            scored.append({"column": column, "score": round(score, 3)})
        scored.sort(key=lambda item: item["score"], reverse=True)
        return {"missing_column": missing_column, "candidates": scored[:10]}

    def _normalize_identifier_for_match(self, value: str) -> str:
        return re.sub(r"[^a-z0-9]", "", value.lower())

    def _match_preview(self, content: str, query_lower: str) -> str:
        index = content.lower().find(query_lower)
        start = max(0, index - 160)
        end = min(len(content), index + len(query_lower) + 160)
        return " ".join(content[start:end].split())

    def _read_part_dependencies(self, part_num: int) -> Dict[str, Any]:
        state = self.state_manager.load_state() or {}
        dependencies = (state.get("parts") or {}).get("dependencies", {})
        return dependencies.get(f"part_{part_num}", {"interpolate_dependency": [], "table_dependency": [], "execution_order_hint": []})

    def _read_runtime_state_snapshot(self, runtime_state: RuntimeExecutionState) -> Dict[str, Any]:
        return {
            "runtime_schema": self._get_runtime_schema(),
            "executed_parts_successfully": sorted(runtime_state.executed_parts_successfully),
            "store_params_by_part": runtime_state.store_params_by_part,
            "runtime_tables_created": sorted(runtime_state.runtime_tables_created),
            "table_to_creator_part": runtime_state.table_to_creator_part,
            "expected_tables_by_part": {
                str(part): sorted(tables) for part, tables in runtime_state.expected_tables_by_part.items()
            },
            "current_run_id": runtime_state.current_run_id,
        }

    def _runtime_failure_invariants(
        self,
        part_num: int,
        error_text: str,
        runtime_state: RuntimeExecutionState,
    ) -> List[Dict[str, Any]]:
        producer_part = self._resolve_missing_table_producer(error_text, part_num, runtime_state)
        if producer_part is None:
            return []
        match = re.search(r"Table '([^']+)' does not exist", error_text, re.IGNORECASE)
        table_name = match.group(1).split(".")[-1] if match else ""
        return [
            {
                "type": "runtime_table_producer_contract",
                "table": table_name,
                "failed_part": part_num,
                "producer_part": producer_part,
                "message": (
                    f"Producer part {producer_part} is expected to create runtime table "
                    f"`{table_name}` before part {part_num} reads it."
                ),
                "suggested_actions": [
                    {
                        "tool": "read_trino_part_lines",
                        "args": {"part_num": producer_part, "start_line": 1, "end_line": 80},
                        "purpose": f"inspect whether producer part {producer_part} creates `{table_name}`",
                    },
                    {
                        "tool": "read_trino_part",
                        "args": {"part_num": producer_part},
                        "purpose": "fallback if line range is insufficient",
                    },
                ],
                "producer_fix_hint": (
                    f"If part {producer_part} is a bare SELECT/WITH and does not materialize `{table_name}`, "
                    f"fix part {producer_part} by adding `CREATE TABLE {table_name} AS` instead of changing "
                    f"consumer part {part_num}."
                ),
            }
        ]

    def _expected_creator_for_table(
        self,
        table_name: str,
        runtime_state: RuntimeExecutionState,
    ) -> Optional[int]:
        normalized = (table_name or "").strip().split(".")[-1].lower()
        if not normalized:
            return None
        return runtime_state.table_to_creator_part.get(normalized)

    def _inspect_information_schema(self, connection: Any, table_name_pattern: str) -> Dict[str, Any]:
        pattern = (table_name_pattern or "").strip()
        if not pattern:
            raise TrinoRuntimeError("table_name_pattern is required")
        pattern_parts = [part for part in pattern.split(".") if part]
        if len(pattern_parts) >= 2:
            search_schemas = [pattern_parts[-2]]
            table_pattern = pattern_parts[-1]
        else:
            search_schemas = [self._get_runtime_schema()]
            table_pattern = pattern
        like = f"%{table_pattern.lower()}%"
        schema_filter = ", ".join(f"'{self._sql_literal(schema.lower())}'" for schema in search_schemas)
        cursor = connection.cursor()
        cursor.execute(
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            f"WHERE lower(table_schema) IN ({schema_filter}) "
            f"AND lower(table_name) LIKE '{self._sql_literal(like)}' "
            "ORDER BY table_schema, table_name "
            "LIMIT 20"
        )
        table_rows = cursor.fetchall()
        cursor = connection.cursor()
        cursor.execute(
            "SELECT table_schema, table_name, column_name, data_type "
            "FROM information_schema.columns "
            f"WHERE lower(table_schema) IN ({schema_filter}) "
            f"AND lower(table_name) LIKE '{self._sql_literal(like)}' "
            "ORDER BY table_schema, table_name, ordinal_position "
            "LIMIT 50"
        )
        column_rows = cursor.fetchall()
        return {
            "table_name_pattern": pattern,
            "searched_schemas": search_schemas,
            "tables": [[str(value) for value in row] for row in table_rows],
            "columns": [[str(value) for value in row] for row in column_rows],
        }

    def _list_related_parts(self, part_num: int) -> Dict[str, Any]:
        state = self.state_manager.load_state() or {}
        dependencies = (state.get("parts") or {}).get("dependencies", {})
        upstream = self._build_dependency_fix_context(part_num)
        downstream: List[Dict[str, Any]] = []
        for key, dep in dependencies.items():
            try:
                candidate_part = int(str(key).split("_")[-1])
            except ValueError:
                continue
            for table_dep in dep.get("table_dependency", []):
                if table_dep.get("created_in_part") == part_num:
                    downstream.append(
                        {
                            "part_num": candidate_part,
                            "relation": "table_dependency",
                            "source_table": table_dep.get("table"),
                        }
                    )
            for interpolate_dep in dep.get("interpolate_dependency", []):
                if interpolate_dep.get("source_part") == part_num:
                    downstream.append(
                        {
                            "part_num": candidate_part,
                            "relation": "interpolate_dependency",
                            "source_table": interpolate_dep.get("source_table"),
                        }
                    )
        deduped_downstream: Dict[Tuple[int, str, Optional[str]], Dict[str, Any]] = {}
        for item in downstream:
            deduped_downstream[(item["part_num"], item["relation"], item.get("source_table"))] = item
        return {"upstream": upstream, "downstream": list(deduped_downstream.values())}

    def _normalize_part_num(self, value: Any, default: Optional[int] = None) -> int:
        if value is None:
            if default is None:
                raise TrinoRuntimeError("part_num is required")
            return default
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            if default is not None:
                return default
            raise TrinoRuntimeError(f"Invalid part_num: {value}") from exc

    def _ensure_no_proxy_for_lm(self) -> None:
        ensure_no_proxy_for_llm(settings.llm_base_url)

    def _build_fix_context(
        self,
        *,
        part_num: int,
        error_text: str,
        trino_sql: str,
        prepared_sql: str,
        preparer: TrinoSQLPreparer,
        resolver: ParameterResolver,
        introspection: Optional[Dict[str, Any]],
        diagnostic_results: Optional[Dict[str, Any]],
        target_part_num: Optional[int] = None,
    ) -> str:
        target_part_num = target_part_num if target_part_num is not None else part_num
        part_metadata = self.state_manager.get_part_metadata(target_part_num)
        knowledge = (self.state_manager.load_state() or {}).get("knowledge", {})
        dependency_context = self._build_dependency_fix_context(part_num)
        available_table_refs = self._table_refs_for_introspection(trino_sql, prepared_sql, preparer)
        introspection_block = (
            json.dumps(introspection, ensure_ascii=False, indent=2, default=str)
            if introspection is not None
            else "Not requested yet."
        )
        diagnostic_block = (
            json.dumps(diagnostic_results, ensure_ascii=False, indent=2, default=str)
            if diagnostic_results is not None
            else "Not requested yet."
        )
        return "\n".join(
            [
                "=== TRINO RUNTIME TEST ERROR ===",
                f"Failed part: {part_num}",
                f"Part to fix: {target_part_num}",
                f"TRINO_SCHEMA: {self._get_runtime_schema()}",
                f"Intermediate runtime-created tables must be created/written only in schema {self._get_runtime_schema()}.",
                "Final datamart targets such as target_schema.<target> must remain in their original target schema.",
                "Do not save the runtime test schema in the corrected SQL file.",
                "Preserve CREATE/INSERT target names from CURRENT TRINO SQL by default.",
                "Exception: rename a CREATE/INSERT target only when the current Trino target is inconsistent with the source Vertica part semantics or when a final target_schema.<target> must become target_schema.<target>_trino.",
                "The tester rewrites intermediate runtime tables to TRINO_SCHEMA only in memory before execution.",
                "Read-only source schemas such as dma, dict, dds must remain unchanged.",
                "If the root cause is in an upstream producer part, you may fix that upstream part instead of the failing consumer.",
                "Use APPLY_TO_PART to redirect the fix to the producer part when needed.",
                "Available runtime tools you can use:",
                f"1. Fix current/producer part directly by returning corrected SQL only.",
                f"2. Redirect fix to another part with one line: -- {APPLY_TO_PART_MARKER} {target_part_num}",
                f"3. Request read-only introspection with one line: -- {INTROSPECT_MARKER} part0_columns, table:gos_client",
                f"4. Run one temporary read-only research query between markers -- {DIAGNOSTIC_QUERY_START_MARKER} / -- {DIAGNOSTIC_QUERY_END_MARKER}",
                "If the error cannot be fixed from the SQL and error text alone, request read-only introspection instead of guessing.",
                f"Return exactly one comment line like: -- {INTROSPECT_MARKER} part0_columns, table:some_table, table:schema.some_table",
                f"To run a temporary read-only research query, return a block between -- {DIAGNOSTIC_QUERY_START_MARKER} and -- {DIAGNOSTIC_QUERY_END_MARKER}.",
                "Diagnostic query must be read-only and is never saved as the part SQL file.",
                f"If another part must be updated, prepend one comment line like: -- {APPLY_TO_PART_MARKER} {target_part_num}",
                "Use introspection only when needed; do not request it for routine syntax/type fixes.",
                "The tester will skip protected target-schema reads and then call you again with the introspection result.",
                "Header parameters used for this run:",
                json.dumps(resolver.values, ensure_ascii=False, indent=2),
                "",
                "Available table names from the failed SQL/runtime targets:",
                json.dumps(available_table_refs, ensure_ascii=False, indent=2),
                "",
                "Current Trino SQL file content to preserve for production (the file you should edit):",
                trino_sql,
                "",
                "Prepared SQL that failed:",
                prepared_sql,
                "",
                "Trino error:",
                error_text,
                "",
                "Runtime introspection context:",
                introspection_block,
                "",
                "Diagnostic query result context:",
                diagnostic_block,
                "",
                "Previous part metadata:",
                json.dumps(part_metadata, ensure_ascii=False, indent=2, default=str),
                "",
                "Direct upstream dependency context:",
                json.dumps(dependency_context, ensure_ascii=False, indent=2, default=str),
                "",
                "Pattern/API/runtime history:",
                json.dumps(knowledge, ensure_ascii=False, indent=2, default=str),
                "",
                "All part files:",
                json.dumps(self._list_part_files(), ensure_ascii=False, indent=2),
                "",
                "Return ONLY corrected Trino SQL for this part.",
            ]
        )

    def _build_dependency_fix_context(self, part_num: int) -> List[Dict[str, Any]]:
        state = self.state_manager.load_state() or {}
        dependencies = (state.get("parts") or {}).get("dependencies", {})
        part_info = dependencies.get(f"part_{part_num}", {})
        upstream_context: List[Dict[str, Any]] = []

        for dep in part_info.get("table_dependency", []):
            source_part = dep.get("created_in_part")
            if source_part is None:
                continue
            upstream_context.append(
                self._build_upstream_part_context(
                    source_part=source_part,
                    relation="table_dependency",
                    source_table=dep.get("table"),
                )
            )

        for dep in part_info.get("interpolate_dependency", []):
            source_part = dep.get("source_part")
            if source_part is None:
                continue
            upstream_context.append(
                self._build_upstream_part_context(
                    source_part=source_part,
                    relation="interpolate_dependency",
                    source_table=dep.get("source_table"),
                )
            )

        deduped: Dict[Tuple[int, str], Dict[str, Any]] = {}
        for item in upstream_context:
            deduped[(item["part_num"], item["relation"])] = item
        return list(deduped.values())

    def _build_upstream_part_context(self, *, source_part: int, relation: str, source_table: Optional[str]) -> Dict[str, Any]:
        return {
            "part_num": source_part,
            "relation": relation,
            "source_table": source_table,
            "vertica_sql": self._safe_load_part_content(source_part, is_vertica=True),
            "trino_sql": self._safe_load_part_content(source_part, is_vertica=False),
        }

    def _safe_load_part_content(self, part_num: int, *, is_vertica: bool) -> Optional[str]:
        try:
            return self._load_part_content(part_num, is_vertica=is_vertica)
        except Exception:
            return None

    def _parse_diagnostic_query(self, text: str) -> Optional[str]:
        lines = text.splitlines()
        capture = False
        captured: List[str] = []
        for line in lines:
            if DIAGNOSTIC_QUERY_START_MARKER in line:
                capture = True
                continue
            if DIAGNOSTIC_QUERY_END_MARKER in line and capture:
                break
            if capture:
                captured.append(line)
        query = "\n".join(captured).strip()
        return query or None

    def _run_diagnostic_query(self, connection: Any, sql: str, part_num: int) -> Dict[str, Any]:
        statements = [statement.strip().rstrip(";").strip() for statement in sqlparse.split(sql) if statement.strip()]
        if len(statements) != 1:
            raise TrinoRuntimeError(f"Diagnostic query for part {part_num} must contain exactly one statement")
        statement = statements[0]
        if not re.match(r"^(SELECT|WITH|SHOW|DESCRIBE)\b", statement, re.IGNORECASE):
            raise TrinoRuntimeError(f"Diagnostic query for part {part_num} must be read-only")
        if self._contains_forbidden_test_reads(statement):
            raise TrinoRuntimeError(
                f"Diagnostic query for part {part_num} cannot read from protected target schemas"
            )

        cursor = connection.cursor()
        cursor.execute(statement)
        rows = cursor.fetchall()
        columns = [item[0] for item in (cursor.description or [])]
        sample_rows = [
            [self._stringify_sample_value(value) for value in row]
            for row in rows[:10]
        ]
        return {
            "status": "ok",
            "query": statement,
            "columns": columns,
            "row_count_sample": len(rows),
            "sample_rows": sample_rows,
        }

    def _parse_introspection_request(self, text: str) -> List[str]:
        requested: List[str] = []
        seen = set()
        for line in text.splitlines():
            marker_index = line.find(INTROSPECT_MARKER)
            if marker_index == -1:
                continue
            payload = line[marker_index + len(INTROSPECT_MARKER):]
            for raw_item in payload.split(","):
                item = raw_item.strip()
                if not item:
                    continue
                if item.lower() == "part0_columns":
                    normalized = "part0_columns"
                else:
                    table_match = re.fullmatch(
                        r"table:(?P<table>(?:(?:[A-Za-z_][\w$]*\.){0,2}[A-Za-z_][\w$]*))",
                        item,
                        re.IGNORECASE,
                    )
                    if not table_match:
                        continue
                    normalized = f"table:{table_match.group('table')}"
                key = normalized.lower()
                if key not in seen:
                    requested.append(normalized)
                    seen.add(key)
        return requested

    def _parse_apply_to_part(self, text: str, default_part: int) -> int:
        for line in text.splitlines():
            marker_index = line.find(APPLY_TO_PART_MARKER)
            if marker_index == -1:
                continue
            payload = line[marker_index + len(APPLY_TO_PART_MARKER):].strip()
            if payload.isdigit():
                return int(payload)
        return default_part

    def _list_part_files(self) -> Dict[str, str]:
        state = self.state_manager.load_state() or {}
        total_parts = state.get("total_parts", 0)
        result: Dict[str, str] = {}
        for part_num in range(total_parts):
            path = self.state_manager.get_latest_version_path(part_num)
            if path is not None:
                result[f"part_{part_num}"] = str(path)
        return result

    def _build_requested_introspection_context(
        self,
        *,
        connection: Any,
        part_num: int,
        trino_sql: str,
        prepared_sql: str,
        preparer: TrinoSQLPreparer,
        requested_items: List[str],
    ) -> Dict[str, Any]:
        context: Dict[str, Any] = {
            "part": part_num,
            "requested": requested_items,
            "part_0_columns": [],
            "tables": [],
            "notes": [
                "Introspection uses read-only SELECT * ... LIMIT 1.",
                "protected target_schema reads are skipped during tests.",
                f"Unqualified runtime tables are inspected in {self._get_runtime_schema()}.",
            ],
        }
        table_refs = set(self._table_refs_for_introspection(trino_sql, prepared_sql, preparer))
        runtime_tables = set(preparer.runtime_table_names())
        requested_tables: List[str] = []
        for item in requested_items:
            if item == "part0_columns":
                context["part_0_columns"] = self._extract_part0_columns()
                continue
            if not item.lower().startswith("table:"):
                continue
            table_name = item.split(":", 1)[1]
            if table_name in table_refs or table_name in runtime_tables or "." in table_name:
                requested_tables.append(table_name)
            else:
                context["tables"].append(
                    {
                        "table": table_name,
                        "status": "skipped",
                        "reason": "table was not present in failed SQL/runtime targets and is not schema-qualified",
                    }
                )

        for table_name in requested_tables[:8]:
            context["tables"].append(self._inspect_table(connection, table_name, preparer.runtime_table_names()))
        return context

    def _table_refs_for_introspection(
        self,
        trino_sql: str,
        prepared_sql: str,
        preparer: TrinoSQLPreparer,
    ) -> List[str]:
        refs: List[str] = []
        seen = set()
        for sql in (trino_sql, prepared_sql):
            for match in TABLE_REF_RE.finditer(sql):
                table = match.group("table")
                key = table.lower()
                if key not in seen:
                    refs.append(table)
                    seen.add(key)
        for table_name in preparer.runtime_table_names():
            key = table_name.lower()
            if key not in seen:
                refs.append(table_name)
                seen.add(key)
        return refs

    def _inspect_table(
        self,
        connection: Any,
        table_name: str,
        runtime_table_names: List[str],
    ) -> Dict[str, Any]:
        qualified = self._qualify_table_for_introspection(table_name, runtime_table_names)
        parts = [part.lower() for part in qualified.split(".")]
        schema_parts = parts[:-1]
        result: Dict[str, Any] = {"table": table_name, "query_table": qualified}
        if any(part in FORBIDDEN_TEST_READ_SCHEMAS for part in schema_parts):
            result["status"] = "skipped"
            result["reason"] = "reading protected target schemas is forbidden in Trino runtime tests"
            return result

        try:
            cursor = connection.cursor()
            cursor.execute(f"SELECT * FROM {qualified} LIMIT 1")
            rows = cursor.fetchall()
            result["status"] = "ok"
            result["columns"] = [item[0] for item in (cursor.description or [])]
            result["sample_row"] = [self._stringify_sample_value(value) for value in rows[0]] if rows else []
        except Exception as exc:
            result["status"] = "error"
            result["error"] = str(exc)
        return result

    def _qualify_table_for_introspection(self, table_name: str, runtime_table_names: List[str]) -> str:
        if "." in table_name:
            return table_name
        if table_name.lower() in {name.lower() for name in runtime_table_names}:
            return f"{self._get_runtime_schema()}.{table_name}"
        return table_name

    def _extract_part0_columns(self) -> List[Dict[str, str]]:
        try:
            part0_sql = self._load_part_content(0, is_vertica=False)
        except Exception:
            return []

        sql = HeaderParser.strip_header(part0_sql)
        start = re.search(r"\bCREATE\s+TABLE\b[^(]*\(", sql, re.IGNORECASE | re.DOTALL)
        if not start:
            return []

        body_start = start.end()
        depth = 1
        index = body_start
        while index < len(sql) and depth:
            char = sql[index]
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            index += 1
        if depth:
            return []

        columns: List[Dict[str, str]] = []
        for raw_column in self._split_top_level_commas(sql[body_start:index - 1]):
            column = raw_column.strip()
            if not column or column.upper().startswith(("PRIMARY ", "UNIQUE ", "CONSTRAINT ")):
                continue
            match = re.match(r'"?(?P<name>[A-Za-z_][\w$]*)"?\s+(?P<type>.+)$', column, re.DOTALL)
            if match:
                columns.append(
                    {
                        "name": match.group("name"),
                        "type": " ".join(match.group("type").split()),
                    }
                )
        return columns

    def _split_top_level_commas(self, text: str) -> List[str]:
        chunks: List[str] = []
        start = 0
        depth = 0
        in_single_quote = False
        index = 0
        while index < len(text):
            char = text[index]
            if char == "'" and (index + 1 >= len(text) or text[index + 1] != "'"):
                in_single_quote = not in_single_quote
            elif not in_single_quote:
                if char == "(":
                    depth += 1
                elif char == ")" and depth:
                    depth -= 1
                elif char == "," and depth == 0:
                    chunks.append(text[start:index])
                    start = index + 1
            index += 1
        chunks.append(text[start:])
        return chunks

    def _stringify_sample_value(self, value: Any) -> str:
        text = str(value)
        return text if len(text) <= 200 else f"{text[:197]}..."

    def _compare(self, connection: Any, header: HeaderMetadata, resolver: ParameterResolver) -> Dict[str, Any]:
        if not header.datamart_schema or not header.datamart_table or not header.target_table:
            return {"success": False, "error": "Header datamart is required for comparison"}

        target_schema = self._resolve_compare_target_schema(header)
        target = QualifiedTable(target_schema, header.target_table)
        reference = QualifiedTable(header.datamart_schema, header.datamart_table)
        date_col = self._pick_period_column(header)
        filter_sql = self._period_filter(date_col, resolver) if date_col else ""

        runtime_schema = self._get_runtime_schema()
        target_count = self._scalar(connection, f"SELECT COUNT(*) FROM {target.render(runtime_schema)} {filter_sql}")
        reference_count = self._scalar(connection, f"SELECT COUNT(*) FROM {reference.render(runtime_schema)} {filter_sql}")
        strategy = self.choose_compare_strategy(int(target_count), settings.trino_compare_full_limit)

        common_columns = self._common_columns(connection, target, reference)
        base = {
            "success": True,
            "strategy": strategy,
            "target_table": target.render(runtime_schema),
            "reference_table": reference.render(runtime_schema),
            "period_column": date_col,
            "target_count": int(target_count),
            "reference_count": int(reference_count),
            "row_count_match": int(target_count) == int(reference_count),
        }
        if not base["row_count_match"]:
            base["success"] = False

        if strategy == "keys_metrics":
            details = self._compare_keys_metrics(
                connection,
                target,
                reference,
                header.keys,
                common_columns,
                filter_sql,
            )
        else:
            details = self._compare_aggregates(
                connection,
                target,
                reference,
                header.keys,
                common_columns,
                filter_sql,
                date_col,
            )
        base.update(details)
        base["success"] = bool(base["row_count_match"]) and bool(details.get("success", False))
        return base

    def _resolve_compare_target_schema(self, header: HeaderMetadata) -> str:
        target_schema = self._get_runtime_schema()
        try:
            latest_parts = self._load_latest_parts()
            preparer = self._build_preparer(header, latest_parts)
            if preparer.final_target_schema:
                return preparer.final_target_schema
        except Exception:
            pass
        return target_schema

    def _analyze_reconciliation(self, compare_result: Dict[str, Any]) -> Dict[str, Any]:
        if compare_result.get("success"):
            return {"status": "success", "summary": "target and reference are aligned"}

        reasons: List[str] = []
        if not compare_result.get("row_count_match", True):
            reasons.append("row_count_mismatch")
        if compare_result.get("missing_keys"):
            reasons.append("missing_keys")
        if compare_result.get("extra_keys"):
            reasons.append("extra_keys")
        if compare_result.get("metric_mismatches"):
            reasons.append("metric_mismatches")
        if compare_result.get("error"):
            reasons.append("compare_error")
        summary = " | ".join(reasons) if reasons else "mismatch_without_classified_reason"
        return {
            "status": "warning",
            "summary": summary,
            "root_cause_hints": reasons,
        }

    @staticmethod
    def choose_compare_strategy(row_count: int, full_limit: int) -> str:
        return "aggregates" if row_count > full_limit else "keys_metrics"

    def _compare_keys_metrics(
        self,
        connection: Any,
        target: QualifiedTable,
        reference: QualifiedTable,
        keys: List[str],
        common_columns: Dict[str, str],
        filter_sql: str,
    ) -> Dict[str, Any]:
        if not keys:
            return {"success": False, "error": "Header keys are required for keys+metrics comparison"}

        key_predicate = " AND ".join([f"t.{key} IS NOT DISTINCT FROM r.{key}" for key in keys])
        runtime_schema = self._get_runtime_schema()
        target_cte = f"(SELECT * FROM {target.render(runtime_schema)} {filter_sql})"
        ref_cte = f"(SELECT * FROM {reference.render(runtime_schema)} {filter_sql})"
        missing = self._scalar(
            connection,
            f"SELECT COUNT(*) FROM {ref_cte} r WHERE NOT EXISTS (SELECT 1 FROM {target_cte} t WHERE {key_predicate})",
        )
        extra = self._scalar(
            connection,
            f"SELECT COUNT(*) FROM {target_cte} t WHERE NOT EXISTS (SELECT 1 FROM {ref_cte} r WHERE {key_predicate})",
        )

        mismatch_columns: Dict[str, int] = {}
        metric_columns = [
            column
            for column in common_columns
            if column not in TECHNICAL_COLUMNS and column not in {key.lower() for key in keys}
        ]
        for column in metric_columns[:100]:
            mismatch = self._scalar(
                connection,
                f"SELECT COUNT(*) FROM {target_cte} t JOIN {ref_cte} r ON {key_predicate} "
                f"WHERE NOT ({self._column_equality('t', 'r', column, common_columns[column])})",
            )
            if int(mismatch):
                mismatch_columns[column] = int(mismatch)

        return {
            "missing_keys": int(missing),
            "extra_keys": int(extra),
            "metric_mismatches": mismatch_columns,
            "success": int(missing) == 0 and int(extra) == 0 and not mismatch_columns,
        }

    def _compare_aggregates(
        self,
        connection: Any,
        target: QualifiedTable,
        reference: QualifiedTable,
        keys: List[str],
        common_columns: Dict[str, str],
        filter_sql: str,
        date_col: Optional[str],
    ) -> Dict[str, Any]:
        numeric_columns = [
            column
            for column, data_type in common_columns.items()
            if column not in TECHNICAL_COLUMNS and any(marker in data_type.lower() for marker in NUMERIC_TYPE_MARKERS)
        ][:100]
        aggregates: Dict[str, Any] = {}
        for label, table in (("target", target), ("reference", reference)):
            pieces = ["COUNT(*) AS row_count"]
            if date_col:
                pieces.extend([f"MIN({date_col}) AS min_{date_col}", f"MAX({date_col}) AS max_{date_col}"])
            for key in keys[:5]:
                pieces.append(f"COUNT(DISTINCT CAST({key} AS VARCHAR)) AS distinct_{key}")
            for column in numeric_columns:
                pieces.append(f"SUM({column}) AS sum_{column}")
                pieces.append(f"COUNT_IF({column} IS NULL) AS nulls_{column}")
            aggregates[label] = self._row(
                connection,
                f"SELECT {', '.join(pieces)} FROM {table.render(self._get_runtime_schema())} {filter_sql}",
            )
        return {"aggregates": aggregates, "success": aggregates.get("target") == aggregates.get("reference")}

    def _column_equality(self, left_alias: str, right_alias: str, column: str, data_type: str) -> str:
        if any(marker in data_type.lower() for marker in NUMERIC_TYPE_MARKERS):
            return (
                f"{left_alias}.{column} IS NOT DISTINCT FROM {right_alias}.{column} OR "
                f"ABS(CAST({left_alias}.{column} AS DOUBLE) - CAST({right_alias}.{column} AS DOUBLE)) <= 0.000001"
            )
        return f"{left_alias}.{column} IS NOT DISTINCT FROM {right_alias}.{column}"

    def _pick_period_column(self, header: HeaderMetadata) -> Optional[str]:
        if header.date_col:
            return header.date_col
        if header.days_col:
            return header.days_col
        for key in header.keys:
            if "date" in key.lower() or key.lower().endswith("_dt"):
                return key
        return None

    def _period_filter(self, date_col: str, resolver: ParameterResolver) -> str:
        first_date = resolver.values.get("first_date") or resolver.values.get("actual_date")
        last_date = resolver.values.get("last_date") or resolver.values.get("actual_date")
        if not first_date or not last_date:
            return ""
        return f"WHERE CAST({date_col} AS DATE) BETWEEN DATE {first_date} AND DATE {last_date}"

    def _common_columns(
        self,
        connection: Any,
        target: QualifiedTable,
        reference: QualifiedTable,
    ) -> Dict[str, str]:
        target_columns = self._columns(connection, target)
        reference_columns = self._columns(connection, reference)
        return {
            column: target_columns[column]
            for column in target_columns.keys() & reference_columns.keys()
        }

    def _columns(self, connection: Any, table: QualifiedTable) -> Dict[str, str]:
        rows = self._rows(
            connection,
            "SELECT lower(column_name), lower(data_type) "
            "FROM information_schema.columns "
            f"WHERE lower(table_schema) = lower('{self._sql_literal(table.schema or self._get_runtime_schema())}') "
            f"AND lower(table_name) = lower('{self._sql_literal(table.table)}')",
        )
        return {row[0]: row[1] for row in rows}

    def _drop_runtime_tables(self, connection: Any, table_names: List[str]) -> None:
        cursor = connection.cursor()
        for table_name in table_names:
            cursor.execute(f"DROP TABLE IF EXISTS {self._get_runtime_schema()}.{table_name}")

    def _load_header(self) -> HeaderMetadata:
        candidates = [
            self.state_manager.work_dir / f"{self.query_name}.sql",
            self.state_manager.work_dir / "final" / f"{self.query_name}_final.sql",
        ]
        for path in candidates:
            if path.exists():
                header = HeaderParser.parse(path.read_text(encoding="utf-8"))
                if header.datamart:
                    return header
        return HeaderMetadata()

    def _load_latest_parts(self) -> Dict[int, str]:
        state = self.state_manager.load_state() or {}
        total_parts = state.get("total_parts", 0)
        parts: Dict[int, str] = {}
        for part_num in range(total_parts):
            parts[part_num] = self._load_part_content(part_num, is_vertica=False)
        return parts

    def _read_trino_part_lines(
        self,
        part_num: int,
        *,
        start_line: Optional[int] = None,
        end_line: Optional[int] = None,
    ) -> Dict[str, Any]:
        sql = self._load_part_content(part_num, is_vertica=False)
        lines = sql.splitlines()
        line_count = len(lines)
        if line_count == 0:
            if start_line is not None or end_line is not None:
                raise TrinoRuntimeError("Cannot read line range from empty SQL part")
            return {
                "part_num": part_num,
                "line_count": 0,
                "start_line": None,
                "end_line": None,
                "lines": [],
                "content": "",
            }

        start = start_line if start_line is not None else 1
        end = min(end_line if end_line is not None else line_count, line_count)
        if start < 1 or end < start or end > line_count:
            raise TrinoRuntimeError(f"Invalid line range {start}-{end}; part {part_num} has {line_count} lines")

        selected = [{"line": number, "text": lines[number - 1]} for number in range(start, end + 1)]
        return {
            "part_num": part_num,
            "line_count": line_count,
            "start_line": start,
            "end_line": end,
            "lines": selected,
            "content": self._format_sql_with_line_numbers("\n".join(lines[start - 1 : end]), start_line=start),
        }

    @staticmethod
    def _format_sql_with_line_numbers(sql: str, *, start_line: int = 1) -> str:
        lines = sql.splitlines()
        if not lines:
            return ""
        width = len(str(start_line + len(lines) - 1))
        return "\n".join(f"{line_number:>{width}} | {line}" for line_number, line in enumerate(lines, start=start_line))

    def _load_part_content(self, part_num: int, *, is_vertica: bool) -> str:
        if is_vertica:
            path = self.state_manager.vertica_parts_path / f"{self.query_name}_part_{part_num}.sql"
        else:
            path = self.state_manager.get_latest_version_path(part_num)
        if path is None or not path.exists():
            raise TrinoRuntimeError(f"Part {part_num} file not found")
        return path.read_text(encoding="utf-8")

    def _save_guarded_fix(
        self,
        part_num: int,
        fixed_sql: str,
        *,
        report: Dict[str, Any],
        source_stage: str,
        root_failed_part: int,
        attempt_num: int,
        final_result: Optional[RepairFinalResult] = None,
    ) -> Tuple[Path, Dict[str, Any]]:
        old_path = self.state_manager.get_latest_version_path(part_num)
        if old_path is None or not old_path.exists():
            raise TrinoRuntimeError(f"Cannot guard fix: part {part_num} has no base Trino file")
        old_sql = old_path.read_text(encoding="utf-8")
        guard = RepairPatchGuard(self.state_manager).validate(part_num=part_num, old_sql=old_sql, new_sql=fixed_sql)
        guard_result = guard.as_dict()
        edit_summary = SQLPatchApplier.summary(final_result.edits) if final_result else []
        change_type = getattr(final_result, "change_type", source_stage)
        repair_summary = getattr(final_result, "summary", source_stage)
        repair_confidence = getattr(final_result, "confidence", 0.0)
        used_evidence = getattr(final_result, "used_evidence", [])
        self._log_event(
            report,
            "repair_patch_guard_checked",
            part=root_failed_part,
            fix_target_part=part_num,
            fix_attempt=attempt_num,
            source_stage=source_stage,
            guard_result=guard_result,
            change_type=change_type,
            edit_summary=edit_summary,
            summary=repair_summary,
            confidence=repair_confidence,
            used_evidence=used_evidence,
        )
        if not guard.accepted:
            self.state_manager.append_knowledge(
                "fix_attempt_journal",
                {
                    "session_id": report.get("event_log", [{}])[-1].get("session_id"),
                    "stage": "runtime_test",
                    "error_or_diff_context": report.get("error"),
                    "target_part": part_num,
                    "planner_summary": repair_summary,
                    "change_type": change_type,
                    "edit_summary": edit_summary,
                    "confidence": repair_confidence,
                    "used_evidence": used_evidence,
                    "guard_result": guard_result,
                    "status": "rejected",
                    "timestamp": datetime.utcnow().isoformat(),
                },
            )
            self._write_fix_attempt_journal_artifact()
            raise TrinoRuntimeError("Repair Patch Contract rejected fix: " + "; ".join(guard.reasons))

        saved_path = self._save_fixed_version(part_num, fixed_sql)
        PartIntentMemory(self.state_manager).update_part(part_num)
        self.state_manager.append_knowledge(
            "fix_attempt_journal",
            {
                "stage": "runtime_test",
                "error_or_diff_context": report.get("error"),
                "target_part": part_num,
                "old_version": self.state_manager.get_latest_version_number(part_num) - 1,
                "new_version": self.state_manager.get_latest_version_number(part_num),
                "diff_summary": guard_result.get("diff_summary"),
                "guard_result": guard_result,
                "summary": repair_summary,
                "change_type": change_type,
                "edit_summary": edit_summary,
                "confidence": repair_confidence,
                "used_evidence": used_evidence,
                "status": "saved",
                "timestamp": datetime.utcnow().isoformat(),
            },
        )
        self._write_fix_attempt_journal_artifact()
        return saved_path, guard_result

    def _write_fix_attempt_journal_artifact(self) -> None:
        state = self.state_manager.load_state() or {}
        journal = (state.get("knowledge") or {}).get("fix_attempt_journal", [])
        path = self.state_manager.work_dir / "logs" / "fix_attempt_journal.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(journal, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        self.state_manager.register_report("fix_attempt_journal", str(path))

    def _save_fixed_version(self, part_num: int, content: str) -> Path:
        latest_path = self.state_manager.get_latest_version_path(part_num)
        if latest_path is None:
            raise TrinoRuntimeError(f"Cannot save fixed version: part {part_num} has no base Trino file")
        next_version = self.state_manager.get_latest_version_number(part_num) + 1
        base_name = re.sub(r"_v\d+$", "", latest_path.stem)
        new_path = latest_path.parent / f"{base_name}_v{next_version}.sql"
        new_path.write_text(content, encoding="utf-8")
        format_status = self._format_fixed_version(part_num, new_path)
        self.state_manager.set_part_status(
            part_num,
            "pattern_fixed",
            {
                "trino_runtime_fixed": True,
                "fix_version": next_version,
                "fix_timestamp": datetime.utcnow().isoformat(),
                "runtime_format": format_status,
            },
        )
        return new_path

    def _format_fixed_version(self, part_num: int, path: Path) -> Dict[str, Any]:
        if part_num == 0:
            return {
                "status": "skipped",
                "reason": "part_0 is skipped by the existing formatter policy",
            }

        try:
            from core.formatter import TrinoFormatter

            formatter = TrinoFormatter()
            success, error = formatter.format_file(path)
            if success:
                print(f"[Trino Test] Formatted runtime fix: {path.name}", flush=True)
                return {"status": "formatted"}
            print(f"[Trino Test] Runtime fix formatting warning for {path.name}: {error}", flush=True)
            return {"status": "warning", "error": error}
        except Exception as exc:
            print(f"[Trino Test] Runtime fix formatting warning for {path.name}: {exc}", flush=True)
            return {"status": "warning", "error": str(exc)}

    def _reassemble_artifacts(self) -> None:
        self._assemble_final_artifact()

    def _assemble_final_artifact(self) -> None:
        from core.assembler import Assembler

        self._update_runtime_status(
            status="running",
            phase="reassemble",
            message="Пересборка final SQL артефакта",
        )
        assembler = Assembler(self.state_manager)
        assembler.assemble_final()

    def _scalar(self, connection: Any, sql: str) -> Any:
        row = self._row(connection, sql)
        return row[0] if row else None

    def _row(self, connection: Any, sql: str) -> Tuple[Any, ...]:
        rows = self._rows(connection, sql)
        return tuple(rows[0]) if rows else tuple()

    def _rows(self, connection: Any, sql: str) -> List[Tuple[Any, ...]]:
        cursor = connection.cursor()
        cursor.execute(sql)
        return [tuple(row) for row in cursor.fetchall()]

    def _write_report(self, report: Dict[str, Any]) -> None:
        report["finished_at"] = datetime.utcnow().isoformat()
        report_path = self._report_path("trino_test_report.json")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    def _report_path(self, filename: str) -> Path:
        return self.state_manager.work_dir / "reports" / filename

    def _log_event(self, report: Dict[str, Any], event: str, **details: Any) -> None:
        event_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "event": event,
            "details": details,
        }
        report.setdefault("event_log", []).append(event_entry)

        summary = self._format_event_summary(event, details)
        print(f"[Trino Test] {summary}", flush=True)

        runtime_update: Dict[str, Any] = {
            "last_event": event_entry,
        }
        if "part" in details:
            runtime_update["current_part"] = details["part"]
        if "fix_attempt" in details:
            runtime_update["current_fix_attempt"] = details["fix_attempt"]
        if "fix_target_part" in details:
            runtime_update["current_fix_target_part"] = details["fix_target_part"]
        if "rebuild_start_part" in details:
            runtime_update["rebuild_start_part"] = details["rebuild_start_part"]
        if "root_failed_part" in details:
            runtime_update["root_failed_part"] = details["root_failed_part"]
        if "error" in details:
            runtime_update["last_error_text"] = details["error"]
            runtime_update["last_failed_part"] = details.get("part")
        self._update_runtime_status(**runtime_update)
        self._write_report(report)

    def _format_event_summary(self, event: str, details: Dict[str, Any]) -> str:
        part = details.get("part")
        total_parts = details.get("total_parts")
        fix_attempt = details.get("fix_attempt")
        if event == "runtime_test_started":
            return f"Runtime test started in schema {details.get('schema')}"
        if event == "part_started":
            return f"Executing SQL part {part}/{total_parts - 1 if total_parts else '?'}"
        if event == "store_part_started":
            return f"Executing @store part {part}/{total_parts - 1 if total_parts else '?'}"
        if event == "part_succeeded":
            return f"SQL part {part} finished successfully"
        if event == "store_part_succeeded":
            return f"@store part {part} finished with status {details.get('store_status')}"
        if event == "part_failed":
            return f"Part {part} failed on fix attempt {fix_attempt or 0}: {details.get('error')}"
        if event == "llm_fix_started":
            return f"Sending part {part} to LLM for runtime fix, attempt {fix_attempt}"
        if event == "llm_requested_introspection":
            return f"LLM requested introspection for part {part}, attempt {fix_attempt}"
        if event == "llm_requested_diagnostic_query":
            return f"LLM requested diagnostic query for part {part}, attempt {fix_attempt}"
        if event == "llm_fix_completed":
            return f"LLM returned fix for part {details.get('source_part')} -> save to part {details.get('target_part')} (attempt {fix_attempt})"
        if event == "upstream_fix_selected":
            return f"Runtime fix for failing part {part} will be applied to upstream part {details.get('fix_target_part')} (attempt {fix_attempt})"
        if event == "fix_saved":
            return f"Saved runtime fix for part {part}, attempt {fix_attempt}: {details.get('fixed_file')}"
        if event == "repair_session_started":
            return f"Repair session started for part {part} (attempt {fix_attempt})"
        if event == "repair_plan_generated":
            return f"Repair plan generated for part {part}: {details.get('hypothesis')}"
        if event == "repair_action_started":
            return f"Repair action {details.get('action_index')} started: {details.get('tool')}"
        if event == "repair_action_finished":
            return f"Repair action {details.get('action_index')} finished: {details.get('tool')} [{details.get('status')}]"
        if event == "repair_additional_context_requested":
            return f"Repair agent requested more context for part {part}"
        if event == "repair_fix_generated":
            return f"Repair agent selected part {details.get('fix_target_part')} to fix"
        if event == "repair_fix_saved":
            return f"Repair agent saved fix to {details.get('fixed_file')}"
        if event == "repair_session_finished":
            return f"Repair session finished for part {part}: {details.get('status')}"
        if event == "repair_session_exhausted":
            return f"Repair session exhausted for part {part}: {details.get('summary')}"
        if event == "rebuild_started":
            return (
                f"Starting targeted rebuild from part {details.get('rebuild_start_part')} "
                f"after failure in part {details.get('root_failed_part')} ({details.get('reason')})"
            )
        if event == "fix_limit_exceeded":
            return f"Fix limit exceeded for part {part}: {details.get('error')}"
        if event == "compare_started":
            return f"Starting comparison for {details.get('datamart')}"
        if event == "compare_finished":
            return f"Comparison finished with success={details.get('success')}"
        if event == "runtime_test_exception":
            return f"Runtime test failed with exception: {details.get('error')}"
        return f"{event}: {json.dumps(details, ensure_ascii=False, default=str)}"

    def _update_runtime_status(self, **updates: Any) -> None:
        current_state = self.state_manager.load_state() or {}
        runtime_state = current_state.get("test_runtime") or {}
        merged = dict(runtime_state)
        merged.update({key: value for key, value in updates.items() if value is not None})
        merged["updated_at"] = datetime.utcnow().isoformat()
        self.state_manager.update_section("test_runtime", merged)

    @staticmethod
    def _sql_literal(value: str) -> str:
        return value.replace("'", "''")
