"""
Модуль валидации и исправления паттернов Vertica в переведенном SQL.
Проверяет на запрещенные конструкции и исправляет их через lightweight repair-path.
"""

import difflib
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import json
from openai import OpenAI

from config import settings
from core.llm_profiles import (
    ensure_no_proxy_for_llm,
    get_openai_request_kwargs,
    resolve_profile,
    resolve_stage_profile,
    strip_openai_provider_prefix,
)
from core.state_manager import StateManager
from core.translator import build_interpolate_context


class PatternGuardError(Exception):
    """Исключение для критических ошибок в PatternGuard."""
    pass


class RawPatternRepairClient:
    """Lightweight OpenAI-compatible client for forbidden-pattern repair."""

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


class PatternGuard:
    """
    Валидатор переведенного SQL на наличие Vertica-специфичных артефактов.
    
    Workflow:
    1. Загружает паттерны из golden_dataset/forbidden_patterns.json
    2. Проверяет каждую часть на совпадение с паттернами
    3. При обнаружении: запускает цикл исправления через LLM (max 3 итерации)
    4. Сохраняет версии: v0 (исходный перевод), v1, v2, v3 (исправленные)
    5. Обновляет StateManager статусом валидации
    """
    
    def __init__(self, compiled_module=None, state_manager: Optional[StateManager] = None, repair_client: Optional[Any] = None):
        """
        Инициализация PatternGuard.
        
        Args:
            compiled_module: Deprecated compatibility arg; no longer used for repair.
            state_manager: Менеджер состояния для отслеживания прогресса
            repair_client: OpenAI-compatible raw repair client for forbidden-pattern fixes.
        """
        del compiled_module
        self.state_manager = state_manager
        self.repair_client = repair_client or RawPatternRepairClient()
        self.patterns: List[Dict[str, Any]] = []
        self.pattern_file = settings.golden_dataset_path / "forbidden_patterns.json"
        
        self._load_patterns()
        
        # Компилируем регексы для performance
        self._compiled_patterns: Dict[str, re.Pattern] = {}
        self._compile_patterns()
    
    def _load_patterns(self) -> None:
        """Загружает паттерны из JSON файла."""
        if not self.pattern_file.exists():
            raise PatternGuardError(f"Forbidden patterns file not found: {self.pattern_file}")
        
        try:
            with open(self.pattern_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.patterns = data.get("patterns", [])
                self.strategies = data.get("auto_fix_strategies", {})
                print(f"[PatternGuard] Loaded {len(self.patterns)} forbidden patterns")
        except json.JSONDecodeError as e:
            raise PatternGuardError(f"Invalid JSON in patterns file: {e}")
    
    def _compile_patterns(self) -> None:
        """Компилирует регекс-паттерны для быстроты."""
        for p in self.patterns:
            pattern_id = p["id"]
            pattern_text = p["pattern"]
            pattern_type = p.get("type", "substring")
            
            try:
                if pattern_type == "regex":
                    self._compiled_patterns[pattern_id] = re.compile(pattern_text, re.IGNORECASE)
                else:
                    # Для substring делаем экранирование и ищем как подстроку
                    escaped = re.escape(pattern_text)
                    self._compiled_patterns[pattern_id] = re.compile(escaped, re.IGNORECASE)
            except re.error as e:
                print(f"[PatternGuard] Warning: Invalid regex for pattern {pattern_id}: {e}")

    def _function_name_before_paren(self, content: str, match_start: int) -> str:
        """Возвращает имя функции перед открывающей скобкой непосредственно перед match."""
        idx = match_start - 1

        while idx >= 0 and content[idx].isspace():
            idx -= 1

        if idx < 0 or content[idx] != "(":
            return ""

        idx -= 1
        while idx >= 0 and content[idx].isspace():
            idx -= 1

        token_end = idx + 1
        while idx >= 0 and (content[idx].isalpha() or content[idx] == "_"):
            idx -= 1

        return content[idx + 1:token_end].upper()

    def _is_explicit_cast(self, content: str, match_start: int) -> bool:
        """Проверяет, что выражение сразу обернуто в CAST/TRY_CAST."""
        return self._function_name_before_paren(content, match_start) in {"CAST", "TRY_CAST"}

    def _is_safe_date_literal_context(self, content: str, match_start: int) -> bool:
        """Пропускает DATE '...', DATE('...'), CAST('...' AS DATE), TRY_CAST(...)."""
        func_name = self._function_name_before_paren(content, match_start)
        if func_name in {"CAST", "TRY_CAST", "DATE"}:
            return True

        idx = match_start - 1
        while idx >= 0 and content[idx].isspace():
            idx -= 1

        token_end = idx + 1
        while idx >= 0 and (content[idx].isalpha() or content[idx] == "_"):
            idx -= 1

        return content[idx + 1:token_end].upper() == "DATE"

    def _is_risky_date_literal_usage(self, content: str, match_start: int, match_end: int) -> bool:
        """Ищет сравнения/диапазоны, где строковая дата почти наверняка должна быть типизирована."""
        prefix = content[max(0, match_start - 80):match_start].upper()
        suffix = content[match_end:match_end + 20].upper()

        if re.search(r"(=|<|>|<=|>=|<>|!=)\s*$", prefix):
            return True
        if re.search(r"\bBETWEEN\s*$", prefix):
            return True
        if re.search(r"\bAND\s*$", prefix) and "BETWEEN" in prefix:
            return True
        if re.search(r"INTERVAL\s*$", prefix):
            return True
        if re.search(r"^\s*(=|<|>|<=|>=|<>|!=)\b", suffix):
            return True

        return False

    def _collect_matches(self, pattern_def: Dict[str, Any], compiled: re.Pattern, content: str) -> List[str]:
        """Возвращает совпадения с фильтрацией известных ложных срабатываний."""
        if pattern_def.get("type") != "regex":
            match = compiled.search(content)
            return [match.group()] if match else []

        matches: List[str] = []
        pattern_id = pattern_def["id"]

        for match in compiled.finditer(content):
            if pattern_id == "uncasted_parameter":
                if self._is_explicit_cast(content, match.start()):
                    continue
            elif pattern_id == "unwrapped_date_literal":
                if self._is_safe_date_literal_context(content, match.start()):
                    continue
                if not self._is_risky_date_literal_usage(content, match.start(), match.end()):
                    continue

            matches.append(match.group())

        return matches
    
    def check_patterns(self, part_num: int) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Проверяет часть на наличие запрещенных паттернов.
        
        Args:
            part_num: Номер части для проверки
            
        Returns:
            (is_clean, found_patterns):
            - is_clean: True если запрещенных паттернов не найдено
            - found_patterns: Список найденных паттернов с контекстом
                [{"id": "...", "description": "...", "matches": ["...", "..."]}, ...]
        """
        if self.state_manager is None:
            raise PatternGuardError("StateManager not provided")
        
        # Загружаем текущую версию файла (последняя доступная)
        file_path = self.get_latest_valid_version(part_num)
        if not file_path or not file_path.exists():
            raise PatternGuardError(f"File for part {part_num} not found")
        
        content = file_path.read_text(encoding='utf-8')
        content_upper = content.upper()
        
        found_patterns = []
        
        for pattern_def in self.patterns:
            pattern_id = pattern_def["id"]
            compiled = self._compiled_patterns.get(pattern_id)
            
            if not compiled:
                continue
            
            # Проверяем контекст если указан (например, только в CREATE TABLE)
            context = pattern_def.get("context")
            if context and not self._check_context(content, context):
                continue
            
            # Поиск совпадений
            matches = self._collect_matches(pattern_def, compiled, content)
            
            if matches:
                found_patterns.append({
                    "id": pattern_id,
                    "pattern": pattern_def["pattern"],
                    "description": pattern_def["description"],
                    "severity": pattern_def.get("severity", "error"),
                    "fix_hint": pattern_def.get("fix_hint", ""),
                    "matches": matches[:3]  # Ограничиваем количество для лога
                })
        
        is_clean = len(found_patterns) == 0
        
        if not is_clean:
            print(f"[PatternGuard] Part {part_num}: Found {len(found_patterns)} forbidden patterns")
            for fp in found_patterns:
                print(f"  - {fp['id']}: {fp['description']}")

        self._persist_pattern_context(
            part_num=part_num,
            source_path=file_path,
            source_version=self.state_manager.get_latest_version_number(part_num),
            found_patterns=found_patterns,
            context_rules=self._build_fix_context(found_patterns, part_num=part_num),
            resolved=is_clean,
            error=None,
        )
        
        return is_clean, found_patterns
    
    def _check_context(self, content: str, context: str) -> bool:
        """Проверяет контекст применения паттерна."""
        if context == "create_table":
            return bool(re.search(r'CREATE\s+TABLE', content, re.IGNORECASE))
        return True
    
    def fix_patterns(self, part_num: int, found_patterns: List[Dict[str, Any]]) -> Tuple[bool, Optional[Path]]:
        """
        Цикл исправления паттернов через LLM.
        
        Выполняет до pattern_fix_max_iter итераций (из config).
        Сохраняет версии: v1, v2, v3.
        
        Args:
            part_num: Номер части
            found_patterns: Список найденных паттернов из check_patterns()
            
        Returns:
            (success, final_path):
            - success: True если после исправлений паттернов не осталось
            - final_path: Путь к финальному файлу (v0, v1, v2 или v3)
        """
        max_iterations = settings.pattern_fix_max_iter
        
        current_path = self.get_latest_valid_version(part_num)
        if current_path is None or not current_path.exists():
            current_path = self._get_part_file_path(part_num, version=0)
        current_version = self.state_manager.get_latest_version_number(part_num)
        
        # Загружаем оригинал Vertica для контекста
        original_vertica = self._get_original_vertica(part_num)

        deterministic_fix = self._fix_version_id_store_part(
            part_num=part_num,
            found_patterns=found_patterns,
            current_path=current_path,
            current_version=current_version,
        )
        if deterministic_fix is not None:
            return deterministic_fix

        for iteration in range(1, max_iterations + 1):
            print(f"[PatternGuard] Part {part_num}: Fix iteration {iteration}/{max_iterations}")

            # Читаем текущую версию Trino SQL
            current_sql = current_path.read_text(encoding='utf-8')

            # Формируем lightweight repair context
            context_rules = self._build_fix_context(found_patterns, part_num=part_num)

            try:
                fixed_sql = self.repair_client.complete(context_rules)
                
                # Очищаем markdown если есть
                fixed_sql = self._clean_sql_output(fixed_sql)
                
                # Сохраняем новую версию
                current_version += 1
                new_path = self._save_version(part_num, current_version, fixed_sql)
                
                # Проверяем снова
                is_clean, remaining_patterns = self.check_part_content(fixed_sql)

                self._persist_pattern_context(
                    part_num=part_num,
                    source_path=current_path,
                    source_version=max(current_version - 1, 0),
                    found_patterns=found_patterns,
                    context_rules=context_rules,
                    resolved=is_clean,
                    result_version=current_version,
                    change_details=self._build_change_details(current_sql, fixed_sql),
                    error=None if is_clean else "Patterns still present after fix iteration",
                )
                
                if is_clean:
                    print(f"[PatternGuard] Part {part_num}: All patterns fixed in v{current_version}")
                    self._update_state(part_num, "pattern_fixed", current_version)
                    return True, new_path
                
                # Если остались паттерны, продолжаем цикл
                found_patterns = remaining_patterns
                current_path = new_path
                
                # Логируем изменения если доступно
            except Exception as e:
                print(f"[PatternGuard] Error during fix iteration {iteration}: {e}")
                self._persist_pattern_context(
                    part_num=part_num,
                    source_path=current_path,
                    source_version=current_version,
                    found_patterns=found_patterns,
                    context_rules=context_rules,
                    resolved=False,
                    result_version=current_version,
                    error=str(e),
                )
                break
        
        # Исчерпаны попытки исправления
        print(f"[PatternGuard] Part {part_num}: Pattern fix exhausted after {max_iterations} attempts")
        self._persist_pattern_context(
            part_num=part_num,
            source_path=current_path,
            source_version=current_version,
            found_patterns=found_patterns,
            context_rules=self._build_fix_context(found_patterns, part_num=part_num),
            resolved=False,
            result_version=current_version,
            error="Fix iterations exhausted",
        )
        self._update_state(part_num, "pattern_error", current_version, error="Fix iterations exhausted")
        return False, current_path

    def _fix_version_id_store_part(
        self,
        *,
        part_num: int,
        found_patterns: List[Dict[str, Any]],
        current_path: Path,
        current_version: int,
    ) -> Optional[Tuple[bool, Path]]:
        """Replace Trino-unsupported version_id store parts with a harmless no-op."""
        if not self._has_pattern(found_patterns, "version_id_forbidden"):
            return None

        current_sql = current_path.read_text(encoding="utf-8")
        if not self._is_version_id_store_part(current_sql):
            return None

        fixed_sql = "SELECT 1;\n"
        new_version = current_version + 1
        new_path = self._save_version(part_num, new_version, fixed_sql)
        is_clean, remaining_patterns = self.check_part_content(fixed_sql)
        context_rules = self._build_version_id_store_noop_context(current_sql)

        self._persist_pattern_context(
            part_num=part_num,
            source_path=current_path,
            source_version=current_version,
            found_patterns=found_patterns,
            context_rules=context_rules,
            resolved=is_clean,
            result_version=new_version,
            change_details=self._build_change_details(current_sql, fixed_sql),
            error=None if is_clean else "Deterministic version_id no-op still has forbidden patterns",
        )

        if is_clean:
            print(
                f"[PatternGuard] Part {part_num}: Replaced version_id store with SELECT 1 in v{new_version}"
            )
            self._update_state(part_num, "pattern_fixed", new_version)
            return True, new_path

        self._update_state(
            part_num,
            "pattern_error",
            new_version,
            error=f"Deterministic version_id no-op failed: {remaining_patterns}",
        )
        return False, new_path

    @staticmethod
    def _has_pattern(found_patterns: List[Dict[str, Any]], pattern_id: str) -> bool:
        return any(pattern.get("id") == pattern_id for pattern in found_patterns)

    @staticmethod
    def _is_version_id_store_part(sql: str) -> bool:
        """Detect a standalone part whose only purpose is deriving version_id."""
        without_comments = re.sub(r"--.*?$", " ", sql, flags=re.MULTILINE)
        without_comments = re.sub(r"/\*.*?\*/", " ", without_comments, flags=re.DOTALL)
        normalized = without_comments.strip().rstrip(";").strip()
        if not normalized:
            return False

        upper_sql = normalized.upper()
        if re.search(r"\b(CREATE|INSERT|UPDATE|DELETE|MERGE|DROP|ALTER)\b", upper_sql):
            return False

        has_store_marker = bool(re.search(r"^\s*--\s*@store\b", sql, flags=re.IGNORECASE | re.MULTILINE))
        computes_version_id = bool(
            re.search(r"\bMAX\s*\(\s*version_id\s*\)", normalized, flags=re.IGNORECASE)
            or re.search(r"\bAS\s+(?:next_)?v(?:ersion)?_?id\b", normalized, flags=re.IGNORECASE)
            or re.search(r"\bAS\s+version_id\b", normalized, flags=re.IGNORECASE)
        )

        return "VERSION_ID" in upper_sql and (has_store_marker or computes_version_id)

    @staticmethod
    def _build_version_id_store_noop_context(current_sql: str) -> str:
        return "\n".join(
            [
                "=== DETERMINISTIC VERSION_ID STORE FIX ===",
                "Trino migration does not use version_id.",
                "This part only derives/stores version_id, so it is replaced with a harmless no-op.",
                "",
                "=== ORIGINAL CURRENT TRINO ===",
                current_sql.strip(),
                "",
                "=== FIXED TRINO ===",
                "SELECT 1;",
            ]
        )
    
    def check_part_content(self, content: str) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Проверяет строку SQL на паттерны (вспомогательный метод).
        
        Returns:
            (is_clean, found_patterns) - аналогично check_patterns, но без чтения файла
        """
        found_patterns = []
        
        for pattern_def in self.patterns:
            pattern_id = pattern_def["id"]
            compiled = self._compiled_patterns.get(pattern_id)
            
            if not compiled:
                continue
            
            matches = self._collect_matches(pattern_def, compiled, content)
            
            if matches:
                found_patterns.append({
                    "id": pattern_id,
                    "description": pattern_def["description"],
                    "severity": pattern_def.get("severity", "error"),
                    "fix_hint": pattern_def.get("fix_hint", ""),  
                    "pattern": pattern_def["pattern"]              
                })
        
        return len(found_patterns) == 0, found_patterns
    
    def _build_fix_context(self, found_patterns: List[Dict[str, Any]], part_num: Optional[int] = None) -> str:
        """Формирует lightweight pattern-repair prompt."""
        pattern_lines = []
        repair_reasons = []
        pattern_ids = []

        for fp in found_patterns:
            pattern_ids.append(fp["id"])
            repair_reasons.append(fp["description"])
            pattern_lines.append(
                f"- id={fp['id']}\n"
                f"  repair_reason={fp['description']}\n"
                f"  pattern_context={fp.get('fix_hint', '')}\n"
                f"  matches={json.dumps(fp.get('matches', []), ensure_ascii=False)}"
            )

        interpolate_context = ""
        if part_num is not None:
            interpolate_context = build_interpolate_context(self.state_manager, part_num)

        previous_context = self._render_previous_pattern_context(part_num)
        allowed_scope = self._build_allowed_change_scope(found_patterns)
        forbidden_scope = self._build_forbidden_change_scope(found_patterns)

        lines = [
            "You are a Trino forbidden-pattern repair assistant.",
            "Your task is to minimally repair the CURRENT TRINO SQL so that the detected forbidden pattern(s) are removed.",
            "Do not translate from scratch. Preserve semantics and unrelated working SQL.",
            "Return only corrected Trino SQL without markdown or explanations.",
            "",
            "=== ORIGINAL VERTICA ===",
            self._get_original_vertica(part_num) if part_num is not None else "",
            "",
            "=== CURRENT TRINO ===",
            self.get_latest_valid_version(part_num).read_text(encoding='utf-8')
            if part_num is not None and self.get_latest_valid_version(part_num) is not None else "",
            "",
            "=== PATTERN SUMMARY ===",
            f"pattern_ids={json.dumps(pattern_ids, ensure_ascii=False)}",
            f"repair_reasons={json.dumps(repair_reasons, ensure_ascii=False)}",
            *pattern_lines,
            "",
            "=== ALLOWED CHANGE SCOPE ===",
            allowed_scope,
            "",
            "=== FORBIDDEN CHANGE SCOPE ===",
            forbidden_scope,
            "",
            "Repair contract:",
            "- Remove only the detected forbidden pattern(s).",
            "- Keep table names, joins, filters, grouping, aliases, selected columns, and output schema unchanged unless the pattern is directly located there.",
            "- Do not use Vertica-only syntax.",
            "- Output only final corrected Trino SQL.",
        ]

        if previous_context:
            lines.extend(["", "=== PREVIOUS PATTERN CONTEXT ===", previous_context])

        if interpolate_context:
            lines.extend(["", "=== INTERPOLATE CONTEXT ===", interpolate_context])

        return "\n".join(lines).strip()

    def _build_allowed_change_scope(self, found_patterns: List[Dict[str, Any]]) -> str:
        pattern_ids = {item["id"] for item in found_patterns}
        scoped_rules = []

        if "temporary_flag_true" in pattern_ids:
            scoped_rules.append(
                "Change only the CREATE TABLE header area needed to remove 'temporary = true'."
            )
        if "uncasted_parameter" in pattern_ids:
            scoped_rules.append(
                "Change only parameter expressions that must be wrapped into explicit CAST."
            )

        if not scoped_rules:
            scoped_rules.append(
                "Change only SQL fragments directly required to eliminate the detected forbidden pattern(s)."
            )

        return "\n".join(f"- {rule}" for rule in scoped_rules)

    def _build_forbidden_change_scope(self, found_patterns: List[Dict[str, Any]]) -> str:
        pattern_ids = {item["id"] for item in found_patterns}
        scoped_rules = [
            "Do not rewrite unrelated business logic.",
            "Do not change SELECT expressions, JOIN predicates, WHERE filters, GROUP BY, ORDER BY, or output schema unless the detected pattern is located there.",
            "Do not modify aliases, column mappings, or table semantics outside the minimal repair scope.",
        ]

        if "temporary_flag_true" in pattern_ids:
            scoped_rules.append(
                "For temporary_flag_true specifically, do not modify JOIN/ON clauses or selected/grouped columns."
            )

        return "\n".join(f"- {rule}" for rule in scoped_rules)
    
    def _clean_sql_output(self, sql: str) -> str:
        """Очищает SQL от markdown-разметки и лишних пробелов."""
        # Удаляем markdown блоки
        sql = re.sub(r'```sql\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'```\s*', '', sql)
        
        # Удаляем начальные/конечные пустые строки
        sql = sql.strip()
        
        return sql

    def _render_previous_pattern_context(self, part_num: Optional[int]) -> str:
        """Собирает прошлый pattern context части в компактный текст."""
        if part_num is None or self.state_manager is None:
            return ""

        part_metadata = self.state_manager.get_part_metadata(part_num)
        pattern_context = part_metadata.get("pattern_context") or {}
        history = pattern_context.get("history") or []
        if not history:
            return ""

        lines: List[str] = []
        for index, entry in enumerate(history[-3:], start=1):
            lines.append(
                f"{index}. source_version={entry.get('source_version')} resolved={entry.get('resolved')} "
                f"result_version={entry.get('result_version')} error={entry.get('error') or 'none'}"
            )
            for pattern in entry.get("found_patterns", []):
                lines.append(
                    f"   - {pattern.get('id')}: {pattern.get('description')} | fix_hint={pattern.get('fix_hint', '')}"
                )
        return "\n".join(lines)

    def _persist_pattern_context(
        self,
        *,
        part_num: int,
        source_path: Optional[Path],
        source_version: int,
        found_patterns: List[Dict[str, Any]],
        context_rules: str,
        resolved: bool,
        result_version: Optional[int] = None,
        change_details: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        """Сохраняет историю срабатывания pattern guard в metadata.parts_map."""
        if self.state_manager is None:
            return

        part_metadata = self.state_manager.get_part_metadata(part_num)
        pattern_context = part_metadata.get("pattern_context") or {}
        history = pattern_context.get("history") or []

        history.append({
            "updated_at": datetime.utcnow().isoformat(),
            "source_path": str(source_path) if source_path else None,
            "source_version": source_version,
            "found_patterns": found_patterns,
            "context_rules": context_rules,
            "resolved": resolved,
            "result_version": result_version,
            "change_details": change_details or {},
            "error": error,
        })

        pattern_context.update({
            "history": history[-10:],
            "active_patterns": [] if resolved else found_patterns,
            "last_context_rules": context_rules,
            "resolved": resolved,
            "resolved_at": datetime.utcnow().isoformat() if resolved else None,
            "last_source_path": str(source_path) if source_path else None,
            "last_source_version": source_version,
            "last_result_version": result_version,
            "last_change_details": change_details or {},
            "last_error": error,
        })

        self.state_manager.update_part_metadata(part_num, {"pattern_context": pattern_context})

    def _build_change_details(self, old_sql: str, new_sql: str) -> Dict[str, Any]:
        diff = list(
            difflib.unified_diff(
                (old_sql or "").splitlines(),
                (new_sql or "").splitlines(),
                fromfile="pattern_guard_old",
                tofile="pattern_guard_new",
                lineterm="",
                n=2,
            )
        )
        return {
            "changed_lines": sum(
                1 for line in diff if line.startswith(("+", "-")) and not line.startswith(("+++", "---"))
            ),
            "diff_preview": diff[:80],
        }
    
    def _get_part_file_path(self, part_num: int, version: int = 0) -> Optional[Path]:
        """Возвращает путь к файлу части (v0 - исходный перевод)."""
        if self.state_manager is None:
            return None
        
        suffix = "" if version == 0 else f"_v{version}"
        filename = f"{self.state_manager.query_name}_part_{part_num}_trino{suffix}.sql"
        return self.state_manager.trino_parts_path / filename
    
    def _get_original_vertica(self, part_num: int) -> str:
        """Загружает оригинальный Vertica SQL для контекста."""
        if self.state_manager is None:
            return ""
        
        filename = f"{self.state_manager.query_name}_part_{part_num}.sql"
        path = self.state_manager.vertica_parts_path / filename
        
        if path.exists():
            return path.read_text(encoding='utf-8')
        return ""
    
    def _save_version(self, part_num: int, version: int, content: str) -> Path:
        """Сохраняет версию исправленного SQL."""
        path = self._get_part_file_path(part_num, version)
        if path is None:
            raise PatternGuardError("Cannot save version: path is None")
        
        # Атомарная запись
        temp_path = path.with_suffix('.tmp')
        temp_path.write_text(content, encoding='utf-8')
        temp_path.replace(path)
        
        print(f"[PatternGuard] Saved version v{version}: {path.name}")
        return path
    
    def _update_state(self, part_num: int, status: str, version: int, error: Optional[str] = None) -> None:
        """Обновляет состояние в StateManager."""
        if self.state_manager is None:
            return
        
        metadata = {
            "fix_version": version,
            "fixed_at": str(Path().stat().st_mtime)  # Просто для примера
        }
        if error:
            metadata["error"] = error
        
        self.state_manager.set_part_status(part_num, status, metadata)
    
    def get_latest_valid_version(self, part_num: int) -> Optional[Path]:
        """
        Возвращает путь к последней валидной версии части.
        """
        return self.state_manager.get_latest_version_path(part_num)
