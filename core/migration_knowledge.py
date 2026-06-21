"""
Compact migration knowledge helpers for runtime and compare repair stages.

The translation DSPy module remains the translator.  This module only builds
small evidence snippets and guardrails for tool-enabled repair stages.
"""

from __future__ import annotations

import difflib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

from config import settings
from core.state_manager import StateManager


TABLE_REF_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+(?P<table>(?:(?:[A-Za-z_][\w$]*\.){0,2}[A-Za-z_][\w$]*))"
    r"(?:\s+(?:AS\s+)?(?P<alias>[A-Za-z_][\w$]*))?",
    re.IGNORECASE,
)
CREATE_TABLE_RE = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<table>(?:(?:[A-Za-z_][\w$]*\.){0,2}[A-Za-z_][\w$]*))",
    re.IGNORECASE,
)
VERTICA_CREATE_TABLE_RE = re.compile(
    r"\bCREATE\s+(?:LOCAL\s+)?(?:GLOBAL\s+)?(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<table>(?:(?:[A-Za-z_][\w$]*\.){0,2}[A-Za-z_][\w$]*))",
    re.IGNORECASE,
)
INSERT_TABLE_RE = re.compile(
    r"\bINSERT\s+INTO\s+(?P<table>(?:(?:[A-Za-z_][\w$]*\.){0,2}[A-Za-z_][\w$]*))",
    re.IGNORECASE,
)
ALIAS_COLUMN_RE = re.compile(r"\b(?P<alias>[A-Za-z_][\w$]*)\.(?P<column>[A-Za-z_][\w$]*)\b")


@dataclass
class PatchGuardResult:
    accepted: bool
    reasons: List[str]
    diff_summary: Dict[str, Any]
    old_intent: Dict[str, Any]
    new_intent: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "accepted": self.accepted,
            "reasons": self.reasons,
            "diff_summary": self.diff_summary,
            "old_intent": self.old_intent,
            "new_intent": self.new_intent,
        }


class PartIntentMemory:
    """Extracts and persists compact per-part SQL intent."""

    def __init__(self, state_manager: StateManager):
        self.state_manager = state_manager

    def update_part(self, part_num: int) -> Dict[str, Any]:
        trino_sql = self._safe_read_latest_trino(part_num)
        vertica_sql = self._safe_read_vertica(part_num)
        intent = self.extract_intent(trino_sql or "", vertica_sql or "")
        intent["latest_version"] = self._latest_version(part_num)
        intent["updated_at"] = datetime.utcnow().isoformat()

        state = self.state_manager.load_state() or {}
        parts = state.get("parts") or {}
        memory = parts.get("intent_memory") if isinstance(parts.get("intent_memory"), dict) else {}
        memory[f"part_{part_num}"] = intent
        parts["intent_memory"] = memory
        self.state_manager.update_section("parts", parts)
        return intent

    def extract_intent(self, trino_sql: str, vertica_sql: str = "") -> Dict[str, Any]:
        sql = self._strip_comments(trino_sql)
        creates = self._created_tables(sql)
        reads, aliases = self._read_tables_and_aliases(sql)
        referenced = self._referenced_columns(sql)
        return {
            "creates_tables": creates,
            "reads_tables": reads,
            "alias_sources": aliases,
            "referenced_columns_by_alias": referenced,
            "output_columns": self._output_columns(sql),
            "source_vertica_summary": self._summarize(vertica_sql),
            "trino_summary": self._summarize(trino_sql),
        }

    def _created_tables(self, sql: str) -> List[str]:
        tables = [match.group("table") for match in CREATE_TABLE_RE.finditer(sql)]
        tables.extend(match.group("table") for match in INSERT_TABLE_RE.finditer(sql))
        return self._dedupe(tables)

    def _read_tables_and_aliases(self, sql: str) -> tuple[List[str], Dict[str, str]]:
        reads: List[str] = []
        aliases: Dict[str, str] = {}
        for match in TABLE_REF_RE.finditer(sql):
            table = match.group("table")
            alias = match.group("alias")
            reads.append(table)
            if alias and alias.upper() not in {"ON", "WHERE", "JOIN", "LEFT", "RIGHT", "FULL", "INNER", "CROSS"}:
                aliases[alias] = table
        return self._dedupe(reads), aliases

    def _referenced_columns(self, sql: str) -> Dict[str, List[str]]:
        result: Dict[str, List[str]] = {}
        for match in ALIAS_COLUMN_RE.finditer(sql):
            result.setdefault(match.group("alias"), [])
            result[match.group("alias")].append(match.group("column"))
        return {alias: self._dedupe(columns) for alias, columns in result.items()}

    def _output_columns(self, sql: str) -> List[str]:
        insert_match = re.search(r"\bINSERT\s+INTO\b[^(]*\((?P<cols>.*?)\)", sql, re.IGNORECASE | re.DOTALL)
        if insert_match:
            return self._split_identifier_list(insert_match.group("cols"))
        create_match = re.search(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?TABLE\b[^(]*\((?P<body>.*?)\)\s*(?:WITH|AS|;|$)", sql, re.IGNORECASE | re.DOTALL)
        if create_match:
            cols = []
            for raw in self._split_top_level_commas(create_match.group("body")):
                match = re.match(r'\s*"?([A-Za-z_][\w$]*)"?\s+', raw)
                if match and match.group(1).lower() not in {"primary", "constraint", "unique"}:
                    cols.append(match.group(1))
            return self._dedupe(cols)
        return []

    def _split_identifier_list(self, text: str) -> List[str]:
        return self._dedupe([item.strip().strip('"') for item in text.split(",") if item.strip()])

    def _split_top_level_commas(self, text: str) -> List[str]:
        chunks: List[str] = []
        start = 0
        depth = 0
        in_quote = False
        for index, char in enumerate(text):
            if char == "'" and (index + 1 >= len(text) or text[index + 1] != "'"):
                in_quote = not in_quote
            elif not in_quote:
                if char == "(":
                    depth += 1
                elif char == ")" and depth:
                    depth -= 1
                elif char == "," and depth == 0:
                    chunks.append(text[start:index])
                    start = index + 1
        chunks.append(text[start:])
        return chunks

    def _strip_comments(self, sql: str) -> str:
        sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
        return re.sub(r"--.*?$", " ", sql, flags=re.MULTILINE)

    def _summarize(self, sql: str) -> str:
        compact = " ".join((sql or "").split())
        return compact[:500]

    def _dedupe(self, values: Iterable[str]) -> List[str]:
        seen = set()
        result = []
        for value in values:
            key = value.lower()
            if key not in seen:
                result.append(value)
                seen.add(key)
        return result

    def _safe_read_latest_trino(self, part_num: int) -> Optional[str]:
        path = self.state_manager.get_latest_version_path(part_num)
        if path and path.exists():
            return path.read_text(encoding="utf-8")
        return None

    def _safe_read_vertica(self, part_num: int) -> Optional[str]:
        path = self.state_manager.vertica_parts_path / f"{self.state_manager.query_name}_part_{part_num}.sql"
        if path.exists():
            return path.read_text(encoding="utf-8")
        return None

    def _latest_version(self, part_num: int) -> int:
        try:
            return self.state_manager.get_latest_version_number(part_num)
        except Exception:
            return 0


class MigrationKnowledgeRegistry:
    """Returns compact evidence blocks from existing project knowledge."""

    def __init__(self, state_manager: StateManager, dataset_dir: Optional[Path] = None):
        self.state_manager = state_manager
        self.dataset_dir = dataset_dir or settings.golden_dataset_path

    def evidence_for(
        self,
        *,
        part_num: int,
        problem_text: str,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        blocks: List[Dict[str, Any]] = []
        tokens = self._tokens(problem_text)
        state = self.state_manager.load_state() or {}

        intent = ((state.get("parts") or {}).get("intent_memory") or {}).get(f"part_{part_num}")
        if intent:
            blocks.append(
                {
                    "source": "PartIntentMemory",
                    "reason": "compact current-part intent",
                    "confidence": 0.8,
                    "text": json.dumps(intent, ensure_ascii=False, default=str)[:1800],
                }
            )

        blocks.extend(self._pattern_guard_blocks(state, part_num))
        blocks.extend(self._api_blocks(state, part_num))
        blocks.extend(self._golden_blocks(tokens))
        blocks.extend(self._forbidden_pattern_blocks(tokens))
        return blocks[:limit]

    def _pattern_guard_blocks(self, state: Dict[str, Any], part_num: int) -> List[Dict[str, Any]]:
        part_meta = (state.get("parts_map") or {}).get(f"part_{part_num}", {})
        context = part_meta.get("pattern_context")
        if not context:
            return []
        return [
            {
                "source": "PatternGuard",
                "reason": "patterns previously matched for this part",
                "confidence": 0.85,
                "text": json.dumps(context, ensure_ascii=False, default=str)[:1600],
            }
        ]

    def _api_blocks(self, state: Dict[str, Any], part_num: int) -> List[Dict[str, Any]]:
        history = (state.get("knowledge") or {}).get("api_validation_history") or {}
        part_history = history.get(f"part_{part_num}") or history.get(str(part_num))
        if not part_history:
            return []
        return [
            {
                "source": "APIValidation",
                "reason": "API validation history for this part",
                "confidence": 0.7,
                "text": json.dumps(part_history, ensure_ascii=False, default=str)[:1400],
            }
        ]

    def _golden_blocks(self, tokens: set[str]) -> List[Dict[str, Any]]:
        path = self.dataset_dir / "examples.json"
        if not path.exists():
            return []
        try:
            items = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
        scored = []
        for item in items:
            haystack = " ".join(
                str(item.get(key, ""))
                for key in ("id", "notes", "context_hint", "patterns_detected", "vertica", "trino")
            ).lower()
            score = len(tokens & self._tokens(haystack))
            if score:
                text = json.dumps(
                    {
                        "id": item.get("id"),
                        "patterns_detected": item.get("patterns_detected", []),
                        "notes": item.get("notes", ""),
                        "context_hint": item.get("context_hint", ""),
                    },
                    ensure_ascii=False,
                    default=str,
                )
                scored.append((score, text))
        scored.sort(reverse=True, key=lambda pair: pair[0])
        return [
            {"source": "GoldenDataset", "reason": "similar example metadata", "confidence": min(0.9, 0.3 + score / 10), "text": text[:1200]}
            for score, text in scored[:2]
        ]

    def _forbidden_pattern_blocks(self, tokens: set[str]) -> List[Dict[str, Any]]:
        path = self.dataset_dir / "forbidden_patterns.json"
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
        blocks = []
        for pattern in payload.get("patterns", []):
            haystack = " ".join(str(pattern.get(key, "")) for key in ("id", "description", "fix_hint", "pattern")).lower()
            score = len(tokens & self._tokens(haystack))
            if score:
                blocks.append(
                    {
                        "source": "ForbiddenPatterns",
                        "reason": f"possibly relevant forbidden pattern: {pattern.get('id')}",
                        "confidence": min(0.8, 0.25 + score / 10),
                        "text": json.dumps(pattern, ensure_ascii=False, default=str)[:1000],
                    }
                )
        blocks.sort(key=lambda item: item["confidence"], reverse=True)
        return blocks[:2]

    def _tokens(self, text: str) -> set[str]:
        return {token for token in re.findall(r"[a-zA-Z_][a-zA-Z0-9_]{2,}", (text or "").lower())}


class RepairPatchGuard:
    """Rejects obviously unrelated repair rewrites before a new part version is saved."""

    def __init__(self, state_manager: StateManager):
        self.state_manager = state_manager
        self.intent_memory = PartIntentMemory(state_manager)

    def validate(self, *, part_num: int, old_sql: str, new_sql: str) -> PatchGuardResult:
        old_intent = self.intent_memory.extract_intent(old_sql)
        new_intent = self.intent_memory.extract_intent(new_sql)
        reasons: List[str] = []

        old_creates = self._normalized_tables(old_intent.get("creates_tables", []))
        new_creates = self._normalized_tables(new_intent.get("creates_tables", []))
        if old_creates and new_creates and old_creates != new_creates and not self._is_allowed_created_table_change(
            part_num=part_num,
            old_creates=old_creates,
            new_creates=new_creates,
        ):
            reasons.append(f"created table changed unexpectedly: {sorted(old_creates)} -> {sorted(new_creates)}")

        old_outputs = {col.lower() for col in old_intent.get("output_columns", [])}
        new_outputs = {col.lower() for col in new_intent.get("output_columns", [])}
        dropped = sorted(old_outputs - new_outputs)
        if old_outputs and len(dropped) > max(5, len(old_outputs) // 2):
            reasons.append(f"too many output columns disappeared: {dropped[:20]}")

        old_len = max(1, len(old_sql.strip()))
        new_len = len(new_sql.strip())
        if old_len > 200 and (new_len < old_len * 0.25 or new_len > old_len * 4):
            reasons.append("fixed SQL size differs too much from previous version")

        if not new_sql.strip():
            reasons.append("fixed SQL is empty")

        diff = list(
            difflib.unified_diff(
                old_sql.splitlines(),
                new_sql.splitlines(),
                fromfile=f"part_{part_num}_old",
                tofile=f"part_{part_num}_new",
                lineterm="",
                n=2,
            )
        )
        return PatchGuardResult(
            accepted=not reasons,
            reasons=reasons,
            diff_summary={
                "changed_lines": sum(1 for line in diff if line.startswith(("+", "-")) and not line.startswith(("+++", "---"))),
                "preview": diff[:80],
            },
            old_intent=old_intent,
            new_intent=new_intent,
        )

    def _table_tail(self, table: str) -> str:
        return table.split(".")[-1].lower()

    def _normalized_table(self, table: str) -> str:
        return table.strip().strip('"').lower()

    def _normalized_tables(self, tables: Iterable[str]) -> Set[str]:
        return {self._normalized_table(table) for table in tables if table}

    def _canonical_table(self, table: str) -> str:
        normalized = self._normalized_table(table)
        if normalized.startswith(("sandbox.", "target_schema.")):
            return normalized
        return self._table_tail(normalized)

    def _canonical_tables(self, tables: Iterable[str]) -> Set[str]:
        return {self._canonical_table(table) for table in tables if table}

    def _is_allowed_created_table_change(
        self,
        *,
        part_num: int,
        old_creates: Set[str],
        new_creates: Set[str],
    ) -> bool:
        return self._is_sandbox_trino_suffix_change(old_creates, new_creates) or self._matches_vertica_semantics_with_downstream_usage(
            part_num=part_num,
            old_creates=old_creates,
            new_creates=new_creates,
        )

    def _is_sandbox_trino_suffix_change(self, old_creates: Set[str], new_creates: Set[str]) -> bool:
        if len(old_creates) != len(new_creates) or not old_creates:
            return False
        expected = {self._with_trino_suffix(table) for table in old_creates}
        return expected == new_creates and all(
            table.startswith(("sandbox.", "target_schema.")) for table in old_creates
        )

    def _with_trino_suffix(self, table: str) -> str:
        if "." not in table:
            return table if table.endswith("_trino") else f"{table}_trino"
        schema, name = table.rsplit(".", 1)
        if name.endswith("_trino"):
            return f"{schema}.{name}"
        return f"{schema}.{name}_trino"

    def _matches_vertica_semantics_with_downstream_usage(
        self,
        *,
        part_num: int,
        old_creates: Set[str],
        new_creates: Set[str],
    ) -> bool:
        vertica_creates = self._load_vertica_created_tables(part_num)
        if not vertica_creates:
            return False

        old_canonical = self._canonical_tables(old_creates)
        new_canonical = self._canonical_tables(new_creates)
        vertica_canonical = self._canonical_tables(vertica_creates)
        renamed_targets = sorted(new_canonical - old_canonical)

        if not renamed_targets or new_canonical != vertica_canonical or old_canonical == vertica_canonical:
            return False

        return all(self._has_downstream_reference(part_num, target) for target in renamed_targets)

    def _load_vertica_created_tables(self, part_num: int) -> List[str]:
        path = self.state_manager.vertica_parts_path / f"{self.state_manager.query_name}_part_{part_num}.sql"
        if not path.exists():
            return []
        try:
            vertica_sql = path.read_text(encoding="utf-8")
        except Exception:
            return []
        return self._extract_created_tables_from_vertica_sql(vertica_sql)

    def _extract_created_tables_from_vertica_sql(self, sql: str) -> List[str]:
        stripped_sql = self.intent_memory._strip_comments(sql)
        tables = [match.group("table") for match in VERTICA_CREATE_TABLE_RE.finditer(stripped_sql)]
        tables.extend(match.group("table") for match in INSERT_TABLE_RE.finditer(stripped_sql))
        return self.intent_memory._dedupe(tables)

    def _has_downstream_reference(self, part_num: int, table_name: str) -> bool:
        current_part = part_num + 1
        total_parts = self._total_parts()
        while current_part <= total_parts:
            if self._part_references_table(current_part, table_name):
                return True
            current_part += 1
        return False

    def _part_references_table(self, part_num: int, table_name: str) -> bool:
        trino_path = self.state_manager.get_latest_version_path(part_num)
        vertica_path = self.state_manager.vertica_parts_path / f"{self.state_manager.query_name}_part_{part_num}.sql"
        trino_sql = trino_path.read_text(encoding="utf-8") if trino_path and trino_path.exists() else ""
        vertica_sql = vertica_path.read_text(encoding="utf-8") if vertica_path.exists() else ""
        canonical_target = self._canonical_table(table_name)
        for sql in (trino_sql, vertica_sql):
            if not sql:
                continue
            intent = self.intent_memory.extract_intent(sql)
            referenced_tables = self._canonical_tables(
                list(intent.get("reads_tables", [])) + list(intent.get("creates_tables", []))
            )
            if canonical_target in referenced_tables:
                return True
        return False

    def _total_parts(self) -> int:
        state = self.state_manager.load_state() or {}
        total_parts = state.get("total_parts")
        if isinstance(total_parts, int) and total_parts > 0:
            return total_parts
        return 0
