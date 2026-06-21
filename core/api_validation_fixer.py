"""
High-level fixer for SQL validation API findings.

Responsibilities:
- analyze structured validation errors from the API client;
- choose auto-fix or LLM-fix strategy;
- save fixed part versions and reassemble final SQL.
"""

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime

import dspy
from config import settings
from core.llm_profiles import ensure_dspy_defaults, get_dspy_lm, resolve_stage_profile
from core.state_manager import StateManager
from core.api_validation_client import SQLValidator, ValidationError


@dataclass
class FixStrategy:
    """Стратегия исправления ошибки"""
    error_type: str
    target_part: Optional[int]  # None если множественные части
    context_parts: List[int]    # Дополнительные части для контекста
    auto_fix: bool              # Можно ли исправить без LLM
    description: str


class APIValidationFixer:
    """
    Исправляет ошибки валидации API согласно 4 стратегиям:
    1. #part_0 - ошибки из part_0, передаем part_0 в LLM
    2. #fuzzy_search - примерный поиск части с ошибкой
    3. #search sandbox and insert - поиск части с sandbox и insert
    4. #search in list replace _trino - автозамена таблиц без _trino на _trino
    """
    
    # Маппинг ошибок на стратегии
    ERROR_STRATEGIES = {
        'old_table': {'strategy': 'replace_trino', 'auto': True},
        'tbl_name_part_0': {'strategy': 'part_0', 'auto': False}, 
        'no_header': {'strategy': 'part_0', 'auto': False},
        'header_required_attrs': {'strategy': 'part_0', 'auto': False},
        'header_param_types': {'strategy': 'part_0', 'auto': False},
        'header_param_fix': {'strategy': 'part_0', 'auto': False},
        'header_params_usage': {'strategy': 'part_0', 'auto': False},
        'header_scheduled_check': {'strategy': 'part_0', 'auto': False},
        'header_engine_fix': {'strategy': 'part_0', 'auto': False},
        'header_keys_check': {'strategy': 'part_0', 'auto': False},
        'header_type_check': {'strategy': 'part_0', 'auto': False},
        'header_days_col': {'strategy': 'part_0', 'auto': False},     
        'header_days_to_keep': {'strategy': 'part_0', 'auto': False},  
        'ddl_check': {'strategy': 'part_0', 'auto': False},
        'col_varchar_numeric_size': {'strategy': 'part_0', 'auto': False},
        'col_decimal_size_limit': {'strategy': 'part_0', 'auto': False},
        'col_launch_id_first': {'strategy': 'part_0', 'auto': False},
        'col_key_exists': {'strategy': 'part_0', 'auto': False},
        'sql_table_n': {'strategy': 'fuzzy_search', 'auto': False},
        'dml_last_part': {'strategy': 'sandbox_insert', 'auto': False},
        'full_refresh_part': {'strategy': 'fuzzy_search', 'auto': False},
        'tbl_delete_check': {'strategy': 'fuzzy_search', 'auto': False},
        'attrs_logical_vertical': {'strategy': 'fuzzy_search', 'auto': False},
        'schema_allowed_check': {'strategy': 'fuzzy_search', 'auto': False},
        'dict_repository_exists': {'strategy': 'fuzzy_search', 'auto': False},
    }
    
    def __init__(self, state_manager: StateManager, compiled_module: Optional[dspy.Module] = None):
        self.state_manager = state_manager
        self.query_name = state_manager.query_name
        self.compiled_module = compiled_module or self._load_module()
        
    def _load_module(self) -> dspy.Module:
        """Загружает скомпилированный DSPy модуль"""
        try:
            from core.translator import get_compiled_module
            return get_compiled_module()
        except Exception as e:
            print(f"[APIFixer] Warning: Could not load compiled module: {e}")
            return None
    
    def validate_and_fix(self, max_iterations: int = 3) -> Tuple[bool, Dict[str, Any]]:
        """
        Основной метод: валидирует финальный SQL и исправляет ошибки.
        
        Returns:
            (success, report): Успех операции и отчет об исправлениях
        """
        # Собираем финальный SQL если еще не собран
        final_sql_path = self._get_final_sql_path()
        if not final_sql_path.exists():
            return False, {"error": "Final SQL not found", "path": str(final_sql_path)}
        
        report = {
            "iterations": [],
            "fixes_applied": [],
            "remaining_errors": [],
            "success": False,
            "validation_report_url": None,
        }
        
        for iteration in range(max_iterations):
            print(f"\n[APIFixer] Iteration {iteration + 1}/{max_iterations}")
            
            # Валидируем текущий вариант
            validator = SQLValidator(base_url=settings.api_validator_url)
            try:
                validator.validate(
                    str(final_sql_path),
                    max_wait_seconds=settings.api_validator_timeout,
                    poll_interval=settings.api_validator_poll_interval
                )
            except Exception as e:
                return False, {"error": f"Validation API error: {e}"}

            if validator.report_url:
                report["validation_report_url"] = validator.report_url
            
            if not validator.has_errors():
                print("[APIFixer] ✓ No errors found, validation passed!")
                report["success"] = True
                return True, report
            
            # Анализируем ошибки и определяем стратегии
            strategies = self._analyze_errors(validator)
            
            if not strategies:
                print("[APIFixer] No fix strategies found for remaining errors")
                report["remaining_errors"] = self._collect_all_errors(validator)
                break
            
            # Применяем исправления
            iteration_report = self._apply_fixes(strategies, validator)
            report["iterations"].append(iteration_report)
            report["fixes_applied"].extend(iteration_report.get("fixes", []))
            
            # Пересобираем финальный файл после исправлений
            self._reassemble_final()
        
        # Финальная проверка
        validator = SQLValidator(base_url=settings.api_validator_url)
        try:
            validator.validate(str(final_sql_path))
            if validator.report_url:
                report["validation_report_url"] = validator.report_url
            if not validator.has_errors():
                report["success"] = True
            else:
                report["remaining_errors"] = self._collect_all_errors(validator)
        except:
            pass
            
        return report["success"], report
    
    def _analyze_errors(self, validator: SQLValidator) -> List[FixStrategy]:
        """Анализирует ошибки и создает стратегии их исправления"""
        strategies = []
        all_errors = validator.get_all_errors()
        
        for error_name, error_data in all_errors.items():
            if error_name not in self.ERROR_STRATEGIES:
                print(f"[APIFixer] Unknown error type: {error_name}, skipping")
                continue

            strategy = self._dispatch_strategy(error_name, error_data)
            if strategy:
                strategies.append(strategy)
        
        return strategies

    def _dispatch_strategy(self, error_name: str, error_data: Any) -> Optional[FixStrategy]:
        strategy_type = self.ERROR_STRATEGIES[error_name]["strategy"]
        handler = {
            "replace_trino": self._handle_replace_trino,
            "part_0": self._handle_part_0_error,
            "sandbox_insert": self._handle_sandbox_insert,
            "fuzzy_search": self._handle_fuzzy_search,
        }.get(strategy_type)
        if handler is None:
            print(f"[APIFixer] Unsupported strategy type: {strategy_type}, skipping")
            return None
        return handler(error_name, error_data)
    
    def _handle_replace_trino(self, error_name: str, error_data: ValidationError) -> Optional[FixStrategy]:
        """
        Стратегия 4: автозамена таблиц без _trino на таблицы с _trino.
        Извлекает список таблиц из комментария ошибки.
        """
        comment = error_data.comment if isinstance(error_data, ValidationError) else str(error_data)
        
        # Извлекаем список таблиц из []
        match = re.search(r'\[(.*?)\]', comment, re.DOTALL)
        if not match:
            return None
        
        tables_str = match.group(1)
        # Парсим таблицы, учитывая кавычки
        tables = [t.strip().strip("'\"") for t in tables_str.split(',')]
        
        return FixStrategy(
            error_type=error_name,
            target_part=None,  # Применяется ко всем частям
            context_parts=[],
            auto_fix=True,
            description=f"Replace tables to _trino suffix: {tables}"
        )
    
    def _handle_part_0_error(self, error_name: str, error_data: Any) -> FixStrategy:
        """Стратегия 1: ошибки из part_0"""
        return FixStrategy(
            error_type=error_name,
            target_part=0,
            context_parts=[],
            auto_fix=False,
            description=f"Part 0 error: {error_name}"
        )
    
    def _handle_sandbox_insert(self, error_name: str, error_data: Any) -> FixStrategy:
        """Стратегия 3: поиск части с sandbox и insert"""
        target_part = self._find_part_with_pattern(
            r'(?i)(sandbox.*insert|insert.*sandbox)'
        )
        
        return FixStrategy(
            error_type=error_name,
            target_part=target_part,
            context_parts=[],
            auto_fix=False,
            description="DML block with sandbox and insert"
        )
    
    def _handle_fuzzy_search(self, error_name: str, error_data: Any) -> FixStrategy:
        """
        Стратегия 2: fuzzy поиск части с ошибкой.
        Ищет ключевые слова из ошибки в частях SQL.
        """
        if isinstance(error_data, list):
            matched_error = self._select_sql_error_for_part(error_data)
            if matched_error and matched_error.metadata and matched_error.metadata.get("part_num") is not None:
                return FixStrategy(
                    error_type=error_name,
                    target_part=matched_error.metadata["part_num"],
                    context_parts=[],
                    auto_fix=False,
                    description=f"Direct part match from validator metadata: {matched_error.metadata['part_num']}"
                )
            if matched_error and matched_error.metadata and matched_error.metadata.get("query"):
                detected_part = self._find_part_by_query(matched_error.metadata["query"])
                if detected_part is not None:
                    return FixStrategy(
                        error_type=error_name,
                        target_part=detected_part,
                        context_parts=[],
                        auto_fix=False,
                        description=f"Part match from validator query: {detected_part}"
                    )

        # Извлекаем ключевые слова из описания ошибки
        if isinstance(error_data, ValidationError):
            search_text = f"{error_data.title} {error_data.comment}"
        elif isinstance(error_data, list):
            search_text = self._build_search_text_from_errors(error_data)
        else:
            search_text = str(error_data)
        
        # Ищем упоминания таблиц, колонок или SQL конструкций
        potential_tables = re.findall(r'(\w+\.\w+)', search_text)
        potential_columns = re.findall(r'column\s+(\w+)', search_text, re.I)
        
        target_part = None
        
        # Пробуем найти по таблицам
        for table in potential_tables:
            part = self._find_part_with_table(table)
            if part is not None:
                target_part = part
                break
        
        # Если не нашли, ищем по ключевым словам ошибки
        if target_part is None:
            keywords = self._extract_search_keywords(search_text)
            target_part = self._find_part_by_keywords(keywords)
        
        return FixStrategy(
            error_type=error_name,
            target_part=target_part,
            context_parts=[],
            auto_fix=False,
            description=f"Fuzzy search result for: {error_name}"
        )
    
    def _apply_fixes(self, strategies: List[FixStrategy], validator: SQLValidator) -> Dict:
        """Применяет исправления согласно стратегиям"""
        report = {"fixes": [], "errors": []}
        
        for strategy in strategies:
            try:
                if strategy.auto_fix:
                    # Автоматическое исправление (замена таблиц)
                    if strategy.error_type == 'old_table':
                        fix_count = self._apply_trino_replacements(strategy.description)
                        report["fixes"].append({
                            "type": "auto_replace",
                            "error": strategy.error_type,
                            "count": fix_count
                        })
                else:
                    # Исправление через LLM
                    if strategy.target_part is not None:
                        self._fix_part_with_llm(strategy, validator)
                        report["fixes"].append({
                            "type": "llm_fix",
                            "error": strategy.error_type,
                            "part": strategy.target_part
                        })
            except Exception as e:
                report["errors"].append({
                    "strategy": strategy.error_type,
                    "error": str(e)
                })
        
        return report
    
    def _apply_trino_replacements(self, description: str) -> int:
        """
        Применяет замену таблиц без _trino на таблицы с _trino.
        ИСПРАВЛЕНО: Теперь работает с ЛЮБЫМ регистром (source.Table -> source.table_trino).
        Возвращает количество замен.
        """
        # Извлекаем список таблиц из описания (формат: "Replace tables to _trino suffix: ['source.table_trino', ...]")
        match = re.search(r': \[(.*?)\](?:\s*$|\s*\n)', description, re.DOTALL)
        if not match:
            print(f"[APIFixer] Warning: Could not parse table list from description")
            return 0
        
        tables_str = match.group(1)
        tables = [t.strip().strip("'\"") for t in tables_str.split(',')]
        
        fix_count = 0
        
        # Применяем замену только к последней доступной версии каждой части.
        # Это важно, чтобы не перетирать версии, уже созданные pattern guard.
        total_parts = (self.state_manager.load_state() or {}).get("total_parts", 0)
        for part_num in range(total_parts):
            part_file = self.state_manager.get_latest_version_path(part_num)
            if part_file is None or not part_file.exists():
                continue

            content = part_file.read_text(encoding='utf-8')
            original_content = content
            
            for table in tables:
                # Убираем _trino если есть (на всякий случай), чтобы избежать двойного суффикса
                # ИСПРАВЛЕНИЕ: flags=re.IGNORECASE для обработки source.TABLE_TRINO
                base_table = re.sub(r'_trino$', '', table, flags=re.IGNORECASE)
                
                # Создаем паттерн с word boundaries
                # ИСПРАВЛЕНИЕ: re.escape экранирует точку в schema.table
                pattern = rf'(?<!\w){re.escape(base_table)}(?!\w)'
                
                # ИСПРАВЛЕНИЕ: flags=re.IGNORECASE для замены независимо от регистра
                # Заменяем на версию с _trino (в нижнем регистре как стандарт)
                replacement = f"{base_table}_trino"
                
                new_content, count = re.subn(pattern, replacement, content, flags=re.IGNORECASE)
                
                if count > 0:
                    content = new_content
                    fix_count += count
                    print(f"[APIFixer]   Replaced '{base_table}' -> '{replacement}' in {part_file.name} ({count} times)")
            
            if content != original_content:
                # Сохраняем как новую версию (v1, v2 и т.д.)
                self._save_new_version(part_file, content)
        
        return fix_count
    
    def _fix_part_with_llm(self, strategy: FixStrategy, validator: SQLValidator):
        """Исправляет часть с помощью LLM с полным контекстом"""
        part_num = strategy.target_part
        
        # Загружаем исходный Vertica SQL
        vertica_content = self._load_part_content(part_num, is_vertica=True)
        if not vertica_content:
            raise ValueError(f"Cannot load Vertica source for part {part_num}")
        
        # Загружаем текущую последнюю версию Trino
        trino_content = self._load_part_content(part_num, is_vertica=False)
        if not trino_content:
            raise ValueError(f"Cannot load Trino content for part {part_num}")
        
        # Получаем детали ошибки
        error_details = self._get_error_details(validator, strategy.error_type, strategy.target_part)
        
        # Формируем полноценный контекст
        context = self._build_fix_context(
            strategy=strategy,
            error_details=error_details,
            vertica_sql=vertica_content,
            trino_sql=trino_content,
            part_num=part_num
        )
        
        print(f"[APIFixer] Sending part {part_num} to LLM for fixing {strategy.error_type}...")
        
        # Вызываем LLM
        if self.compiled_module:
            # Используем vertica_sql как основной input, но с rich context hint
            ensure_dspy_defaults()
            lm = get_dspy_lm(
                resolve_stage_profile("api_validate"),
                max_tokens=settings.llm_max_tokens,
                temperature=0.1,
                cache=False,
                ensure_no_proxy=True,
            )
            with dspy.context(lm=lm, adapter=dspy.ChatAdapter()):
                result = self.compiled_module(
                    vertica_sql=vertica_content,  # Оригинал для reference
                    context_hint=context          # Полный контекст с текущим Trino и ошибкой
                )
            fixed_sql = result.trino_sql
        else:
            raise RuntimeError("Compiled DSPy module not available")
                
        # Сохраняем исправленную версию
        self._save_fixed_version(part_num, fixed_sql, strategy.error_type)
        print(f"[APIFixer] Part {part_num} fixed and saved as new version")

    def _build_fix_context(
        self, 
        strategy: FixStrategy, 
        error_details: Dict[str, Any],
        vertica_sql: str,
        trino_sql: str,
        part_num: int
    ) -> str:
        """Строит детальный контекст для LLM с инструкциями"""
        lines = []
        
        # 1. Контекст исходников
        lines.append("=== REFERENCE: ORIGINAL VERTICA SQL ===")
        lines.append(vertica_sql)
        lines.append("")
        
        # 2. Текущий Trino для исправления
        lines.append("=== CURRENT: TRINO SQL TO FIX ===")
        lines.append(trino_sql)
        lines.append("")
        
        # 3. Детали ошибки
        lines.append("=== API VALIDATION ERROR ===")
        lines.append(f"Error Type: {strategy.error_type}")
        lines.append(f"Title: {error_details.get('title', 'N/A')}")
        lines.append(f"Description: {error_details.get('comment', 'N/A')}")

        metadata = error_details.get('metadata') or {}
        if metadata.get("part_num") is not None:
            lines.append(f"Detected Part: {metadata['part_num']}")
        if metadata.get("error_messages"):
            lines.append("Parsed error messages:")
            for msg in metadata["error_messages"][:5]:
                lines.append(f"  - {msg}")
        if metadata.get("query"):
            lines.append("Validator query block:")
            lines.append(metadata["query"])
        
        if error_details.get('examples'):
            lines.append("Examples:")
            for i, ex in enumerate(error_details['examples'][:3], 1):
                lines.append(f"  {i}. {ex}")
        lines.append("")

        pattern_context = self._get_pattern_context_summary(part_num)
        if pattern_context:
            lines.append("=== PREVIOUS PATTERN GUARD CONTEXT ===")
            lines.append(pattern_context)
            lines.append("")
        
        # 4. Специфические инструкции
        lines.append("=== FIX INSTRUCTIONS ===")
        
        if part_num == 0:
            # Для part_0 загружаем инструкцию из файла
            header_rules = self._load_header_rules()
            if header_rules:
                lines.append("HEADER RULES (apply these to fix the error):")
                lines.append(header_rules)
                lines.append("")
            
            lines.append("Specific fixes for header part (part_0):")
            if "header" in strategy.error_type:
                lines.append("- Ensure all required header attributes are present (@header, scheduled, type, etc.)")
                lines.append("- Check parameter types match their declarations")
                lines.append("- Verify all declared parameters are used in the code")
            elif "ddl" in strategy.error_type:
                lines.append("- Fix DDL syntax for CREATE TABLE")
                lines.append("- Ensure all column types are valid for Trino")
            elif "col_" in strategy.error_type:
                lines.append("- Fix column definitions:")
                if "varchar_numeric_size" in strategy.error_type:
                    lines.append("  * Add size limits to VARCHAR and NUMERIC columns (e.g., VARCHAR(255), DECIMAL(18,2))")
                if "decimal_size_limit" in strategy.error_type:
                    lines.append("  * Reduce excessive precision in DECIMAL types")
                if "launch_id_first" in strategy.error_type:
                    lines.append("  * Move launch_id to be the first column in the table")
                if "key_exists" in strategy.error_type:
                    lines.append("  * Ensure all key columns exist in the table definition")
        
        elif strategy.error_type == 'dml_last_part':
            lines.append("This is a DML block targeting a write schema. Required fixes:")
            lines.append("- Ensure safe data update patterns (use MERGE or INSERT OVERWRITE appropriately)")
            lines.append("- Verify partition handling for FULL_REFRESH tables")
            
        elif strategy.error_type == 'sql_table_n':
            lines.append("SQL syntax error detected. Fix the syntax to be compatible with Trino:")
            lines.append("- Check for Vertica-specific functions and replace with Trino equivalents")
            lines.append("- Verify all table references use correct schema.table format")
            lines.append("- Ensure all columns exist and have correct types")
            
        else:
            lines.append("Fix the SQL according to the error description above.")
            lines.append("- Maintain the original logic from Vertica")
            lines.append("- Use Trino-compatible syntax only")
        
        lines.append("")
        lines.append("=== TASK ===")
        lines.append("Return ONLY the corrected Trino SQL code. Do not add explanations.")
        lines.append("The returned SQL must resolve the API validation error described above.")
        lines.append("Preserve or improve previous pattern_guard fixes; do not reintroduce forbidden Vertica patterns.")
        
        return "\n".join(lines)

    def _get_pattern_context_summary(self, part_num: int) -> str:
        """Возвращает краткую сводку pattern guard history для части."""
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
            if entry.get("context_rules"):
                lines.append("   context_rules:")
                for line in str(entry["context_rules"]).splitlines():
                    lines.append(f"     {line}")
        return "\n".join(lines)
    
    def _load_header_rules(self) -> Optional[str]:
        """Загружает правила для header из MD файла"""
        rule_path = settings.header_rules_first_part
        if not rule_path.exists():
            return None
        try:
            return rule_path.read_text(encoding='utf-8')
        except Exception as e:
            print(f"[APIFixer] Warning: Could not read header rules: {e}")
        return None

    def _get_error_details(self, validator: SQLValidator, error_type: str, target_part: Optional[int] = None) -> Dict[str, Any]:
        """Получает детальную информацию об ошибке"""
        error_obj = getattr(validator, error_type, None)
        
        if not error_obj:
            return {"title": "Unknown", "comment": "No details available", "examples": []}
        
        # Обработка списка ошибок (sql_table_n)
        if isinstance(error_obj, list):
            if not error_obj:
                return {"title": "Unknown", "comment": "No details available", "examples": []}
            primary = self._select_sql_error_for_part(error_obj, target_part) or error_obj[0]
            return {
                "title": primary.title,
                "comment": primary.comment,
                "examples": primary.examples,
                "metadata": primary.metadata or {},
                "count": len(error_obj),
                "all_errors": [
                    {"title": e.title, "comment": e.comment, "metadata": e.metadata or {}}
                    for e in error_obj
                ]
            }
        else:
            return {
                "title": error_obj.title,
                "comment": error_obj.comment,
                "examples": error_obj.examples,
                "metadata": error_obj.metadata or {}
            }
    
    def _get_error_info(self, validator: SQLValidator, error_type: str) -> str:
        """Получает информацию об ошибке из валидатора"""
        error_obj = getattr(validator, error_type, None)
        if error_obj:
            if isinstance(error_obj, list):
                return "; ".join([f"{e.title}: {e.comment}" for e in error_obj[:3]])
            else:
                return f"{error_obj.title}: {error_obj.comment}"
        return "Unknown error"

    def _build_search_text_from_errors(self, errors: List[ValidationError]) -> str:
        """Строит поисковую строку из списка ошибок с учетом метаданных."""
        chunks: List[str] = []

        for error in errors:
            chunks.append(f"{error.title} {error.comment}")
            metadata = error.metadata or {}
            if metadata.get("query"):
                chunks.append(metadata["query"])
            if metadata.get("raw_errors"):
                chunks.append(metadata["raw_errors"])
            if metadata.get("identifiers"):
                chunks.append(" ".join(metadata["identifiers"]))

        return "\n".join(chunks)

    def _select_sql_error_for_part(self, errors: List[ValidationError], part_num: Optional[int] = None) -> Optional[ValidationError]:
        """Выбирает SQL ошибку, максимально релевантную целевой части."""
        if not errors:
            return None

        target_part = part_num
        if target_part is None:
            for error in errors:
                metadata = error.metadata or {}
                if metadata.get("part_num") is not None:
                    target_part = metadata["part_num"]
                    break
                query = metadata.get("query")
                if query:
                    detected_part = self._find_part_by_query(query)
                    if detected_part is not None:
                        target_part = detected_part
                        break

        if target_part is None:
            return errors[0]

        for error in errors:
            metadata = error.metadata or {}
            if metadata.get("part_num") == target_part:
                return error
            query = metadata.get("query")
            if query and self._find_part_by_query(query) == target_part:
                return error

        return errors[0]
    
    def _find_part_with_pattern(self, pattern: str) -> Optional[int]:
        """Находит номер части, содержащей паттерн"""
        vertica_parts_path = self.state_manager.vertica_parts_path
        
        for part_file in vertica_parts_path.glob(f"{self.query_name}_part_*.sql"):
            content = part_file.read_text(encoding='utf-8')
            if re.search(pattern, content):
                # Извлекаем номер части из имени файла
                match = re.search(r'_part_(\d+)', part_file.name)
                if match:
                    return int(match.group(1))
        return None
    
    def _find_part_with_table(self, table_name: str) -> Optional[int]:
        """Находит часть, содержащую упоминание таблицы"""
        pattern = rf'\b{re.escape(table_name)}\b'
        return self._find_part_with_pattern(pattern)
    
    def _find_part_by_keywords(self, keywords: List[str]) -> Optional[int]:
        """Находит часть по ключевым словам"""
        vertica_parts_path = self.state_manager.vertica_parts_path
        best_part = None
        best_score = 0
        
        for part_file in vertica_parts_path.glob(f"{self.query_name}_part_*.sql"):
            content = part_file.read_text(encoding='utf-8').lower()
            score = sum(1 for kw in keywords if kw.lower() in content)
            
            if score > best_score:
                best_score = score
                match = re.search(r'_part_(\d+)', part_file.name)
                if match:
                    best_part = int(match.group(1))
        
        return best_part

    def _find_part_by_query(self, query: str) -> Optional[int]:
        """
        Находит часть по QUERY из API-валидатора.
        Валидатор часто отдает укороченный фрагмент запроса, поэтому используем:
        1. нормализованное вхождение всей строки;
        2. устойчивый префикс из первых SQL-токенов.
        """
        normalized_query = self._normalize_sql_for_match(query)
        if not normalized_query:
            return None

        query_tokens = self._extract_sql_match_tokens(normalized_query)
        if not query_tokens:
            return None

        vertica_parts_path = self.state_manager.vertica_parts_path
        best_part = None
        best_score = 0.0

        for part_file in vertica_parts_path.glob(f"{self.query_name}_part_*.sql"):
            content = part_file.read_text(encoding='utf-8')
            normalized_content = self._normalize_sql_for_match(content)
            if not normalized_content:
                continue

            if normalized_query in normalized_content:
                match = re.search(r'_part_(\d+)', part_file.name)
                if match:
                    return int(match.group(1))

            prefix_len = min(len(query_tokens), 8)
            prefix_tokens = query_tokens[:prefix_len]
            prefix = " ".join(prefix_tokens)
            score = 0.0

            if prefix and prefix in normalized_content:
                score += 100.0

            matched_tokens = sum(1 for token in prefix_tokens if token in normalized_content)
            score += matched_tokens / max(prefix_len, 1)

            if query_tokens:
                all_token_matches = sum(1 for token in query_tokens if token in normalized_content)
                score += all_token_matches / len(query_tokens)

            if score > best_score:
                match = re.search(r'_part_(\d+)', part_file.name)
                if match:
                    best_part = int(match.group(1))
                    best_score = score

        return best_part

    @staticmethod
    def _normalize_sql_for_match(text: str) -> str:
        """Нормализует SQL для устойчивого сопоставления укороченных фрагментов."""
        text = text.lower()
        text = re.sub(r'--.*?$', ' ', text, flags=re.MULTILINE)
        text = re.sub(r'/\*.*?\*/', ' ', text, flags=re.DOTALL)
        text = re.sub(r'\s+', ' ', text)
        return text.strip(" ;")

    @staticmethod
    def _extract_sql_match_tokens(text: str) -> List[str]:
        """Извлекает значимые SQL-токены, сохраняя порядок для prefix-match."""
        sql_stop_words = {
            'as', 'with', 'select', 'from', 'join', 'left', 'right', 'inner', 'outer',
            'where', 'and', 'or', 'on', 'case', 'when', 'then', 'else', 'end',
            'cast', 'date', 'interval', 'distinct', 'group', 'by', 'order', 'having',
        }
        tokens = re.findall(r'[a-z_][a-z0-9_]*', text)
        return [token for token in tokens if len(token) >= 4 and token not in sql_stop_words]
    
    def _extract_search_keywords(self, text: str) -> List[str]:
        """Извлекает ключевые слова для поиска"""
        # Убираем стоп-слова и короткие слова
        stop_words = {'the', 'and', 'or', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 
                     'is', 'are', 'was', 'were', 'be', 'been', 'error', 'check', 'validation'}
        words = re.findall(r'\b\w{4,}\b', text.lower())
        return [w for w in words if w not in stop_words][:5]  # Берем топ-5 ключевых слов
    
    def _load_part_content(self, part_num: int, is_vertica: bool = False) -> Optional[str]:
        """Загружает содержимое части"""
        if is_vertica:
            path = self.state_manager.vertica_parts_path / f"{self.query_name}_part_{part_num}.sql"
        else:
            # Ищем последнюю версию Trino
            path = self.state_manager.get_latest_version_path(part_num)
        
        if path and path.exists():
            return path.read_text(encoding='utf-8')
        return None
    
    def _save_new_version(self, original_path: Path, content: str):
        """Сохраняет новую версию файла (v1, v2, etc.)"""
        # Определяем следующую версию
        stem = original_path.stem
        version_match = re.search(r'_v(\d+)$', stem)
        if version_match:
            current_ver = int(version_match.group(1))
            base_name = re.sub(r'_v\d+$', '', stem)
        else:
            current_ver = 0
            base_name = stem
        
        new_ver = current_ver + 1
        new_name = f"{base_name}_v{new_ver}.sql"
        new_path = original_path.parent / new_name
        
        new_path.write_text(content, encoding='utf-8')
        print(f"[APIFixer] Saved new version: {new_path.name}")
    
    def _save_fixed_version(self, part_num: int, content: str, error_type: str):
        """Сохраняет исправленную версию части с пометкой об ошибке"""
        # Ищем последнюю версию и инкрементируем
        versions = self.state_manager.get_translation_versions(part_num)
        
        if versions:
            last_path = Path(versions[-1])
            self._save_new_version(last_path, content)
        else:
            # Создаем v1
            new_path = self.state_manager.trino_parts_path / f"{self.query_name}_part_{part_num}_trino_v1.sql"
            new_path.write_text(content, encoding='utf-8')
        
        latest_version = self.state_manager.get_latest_version_number(part_num)

        # Обновляем статус
        self.state_manager.set_part_status(
            part_num, 
            "pattern_fixed",
            {
                "api_error_fixed": error_type,
                "fix_timestamp": datetime.utcnow().isoformat(),
                "fix_version": latest_version,
            }
        )
    
    def _reassemble_final(self):
        """Пересобирает финальный SQL после исправлений."""
        from core.assembler import Assembler
        assembler = Assembler(self.state_manager)
        assembler.assemble_final()
    
    def _get_final_sql_path(self) -> Path:
        """Возвращает путь к финальному SQL файлу"""
        return self.state_manager.work_dir / "final" / f"{self.query_name}_final.sql"
    
    def _collect_all_errors(self, validator: SQLValidator) -> List[Dict]:
        """Собирает все ошибки в структурированном виде"""
        errors = []
        all_errors = validator.get_all_errors()
        
        for name, data in all_errors.items():
            if isinstance(data, list):
                for item in data:
                    errors.append({"type": name, "title": item.title, "comment": item.comment})
            else:
                errors.append({"type": name, "title": data.title, "comment": data.comment})
        
        return errors

class TranslationError(Exception):
    """Ошибка перевода"""
    pass
