"""
Модуль перевода SQL Vertica → Trino через скомпилированный DSPy модуль.
Каждая часть переводится независимо (без контекста предыдущих частей).
"""

import re
from pathlib import Path
from typing import Optional, Tuple

import dspy

from config import settings
from core.llm_profiles import ensure_dspy_defaults, get_dspy_lm, resolve_stage_profile
from core.state_manager import StateManager
from dspy_modules.signature import VerticaToTrinoProgram


class TranslationError(Exception):
    """Исключение для ошибок перевода."""
    pass


# =============================================================================
# SINGLETON: Кэширование скомпилированного DSPy-модуля
# =============================================================================

_compiled_module_cache: Optional[dspy.Module] = None


def build_interpolate_context(state_manager: Optional[StateManager], part_num: int) -> str:
    """
    Формирует полный контекст для части с учетом INTERPOLATE зависимостей.
    Выделено отдельно для повторного использования в translate и repair-mode.
    """
    if state_manager is None:
        return ""

    state = state_manager.load_state()
    if not state:
        return ""

    parts_map = state.get("parts_map", {})
    part_key = f"part_{part_num}"
    part_info = parts_map.get(part_key, {})
    context_lines = []

    def load_part_content(target_part_num: int, *, is_vertica: bool = True) -> Optional[str]:
        if is_vertica:
            path = state_manager.vertica_parts_path / f"{state_manager.query_name}_part_{target_part_num}.sql"
        else:
            path = state_manager.get_latest_version_path(target_part_num)

        if path and path.exists():
            return path.read_text(encoding='utf-8')
        return None

    consumers = part_info.get("interpolate_consumers", [])
    if consumers:
        context_lines.append(
            "=== INSTRUCTION FOR TRANSLATING THE INTERPOLATION SOURCE ===\n"
            "This temporary table is used as a source for INTERPOLATE JOIN "
            "in another part of the query. To migrate to Trino, you must:\n"
            "1. Add a column(s) using the LEAD() window function "
            "to obtain the 'next' value\n"
            "2. Keep the original columns unchanged\n"
            "3. LEAD() must be computed using the same keys as in INTERPOLATE JOIN \n\n"
            "=== CONSUMER PART CODE (for understanding the usage context) ==="
        )

        for consumer in consumers:
            consumer_num = consumer["consumer_part"]
            consumer_sql = load_part_content(consumer_num, is_vertica=True)
            if consumer_sql:
                context_lines.append(f"\n--- Part {consumer_num} (потребитель INTERPOLATE) ---")
                context_lines.append(consumer_sql)
                context_lines.append("--- конец кода потребителя ---\n")

    sources = part_info.get("interpolate_sources", [])
    if sources:
        context_lines.append(
            "=== INSTRUCTION FOR TRANSLATING THE INTERPOLATION CONSUMER ===\n"
            "This part contains a Vertica INTERPOLATE PREVIOUS VALUE join.\n"
            "Translate it to Trino using the precomputed range columns from the source table.\n\n"
            "Rules:\n"
            "1. Do NOT use INTERPOLATE PREVIOUS VALUE in Trino.\n"
            "2. Do NOT calculate LEAD() inside the consumer query or inside the JOIN condition.\n"
            "3. Assume the source table has already been translated and already contains a precomputed upper-bound column such as next_actual_date, next_event_date, or another equivalent next_* column.\n"
            "4. Use that existing next_* column from the source table in the JOIN condition.\n"
            "5. The consumer timestamp/date must be compared against the source interval, not наоборот.\n"
            "6. Correct pattern: consumer.date >= source.date AND consumer.date < source.next_date.\n"
            "7. Keep all non-INTERPOLATE join keys unchanged.\n"
            "8. If the source next_* column is NULL, use COALESCE(source.next_date, DATE '2999-12-31') or another safe upper bound only if such fallback is really needed.\n"
            "9. Never generate JOIN conditions like LEAD(source.date) OVER (...) inside ON; this is invalid SQL for this case.\n\n"
            "Correct shape example:\n"
            "LEFT JOIN source_table s\n"
            "  ON consumer.user_id = s.user_id\n"
            " AND consumer.event_date >= s.actual_date\n"
            " AND consumer.event_date < s.next_actual_date\n\n"
            "Wrong shape example:\n"
            "LEFT JOIN source_table s\n"
            "  ON consumer.user_id = s.user_id\n"
            " AND consumer.event_date >= s.actual_date\n"
            " AND consumer.event_date <= LEAD(s.actual_date) OVER (PARTITION BY ... ORDER BY ...)\n\n"
            "Use the source SQL below to identify which existing precomputed next_* column must be used.\n\n"
            "=== DATA SOURCE CODE (used for INTERPOLATE) ==="
        )

        for source in sources:
            source_num = source["source_part"]
            source_vertica = load_part_content(source_num, is_vertica=True)
            if source_vertica:
                context_lines.append(f"\n--- Part {source_num} (источник данных, Vertica) ---")
                context_lines.append(source_vertica)
                context_lines.append("--- конец кода источника ---")

            source_trino = load_part_content(source_num, is_vertica=False)
            if source_trino:
                context_lines.append(f"\n--- Part {source_num} (уже переведен в Trino) ---")
                context_lines.append(source_trino)
                context_lines.append("--- конец перевода источника ---\n")

    return "\n".join(context_lines)


def get_compiled_module() -> dspy.Module:
    """
    Singleton-фабрика для загрузки скомпилированного DSPy-модуля.
    Модуль загружается один раз при первом вызове и переиспользуется.
    """
    global _compiled_module_cache
    if _compiled_module_cache is None:
        _compiled_module_cache = _load_compiled_module()
    return _compiled_module_cache


def _load_compiled_module() -> dspy.Module:
    """
    Загружает скомпилированный модуль из checkpoint.
    Пробует сначала полную программу (save_program=True), затем .pkl
    """
    # Пробуем сначала полную программу
    full_program_path = settings.checkpoint_path.parent / "compiled_module_full"
    pkl_path = settings.checkpoint_path.with_suffix('.pkl')
    
    if full_program_path.exists():
        print(f"[Translator] Loading full program from: {full_program_path}")
        try:
            return dspy.load(str(full_program_path), allow_pickle=True)
        except Exception as e:
            print(f"[Translator] Warning: Failed to load full program: {e}")
    
    # Fallback на .pkl файл
    if pkl_path.exists():
        print(f"[Translator] Loading module from: {pkl_path}")
        try:
            module = VerticaToTrinoProgram()
            module.load(str(pkl_path), allow_pickle=True)
            return module
        except Exception as e:
            raise TranslationError(f"Failed to load compiled module from {pkl_path}: {e}")
    
    # Если ничего не нашли — ошибка
    raise TranslationError(
        f"No compiled module found. Expected one of:\n"
        f"  - {full_program_path}\n"
        f"  - {pkl_path}\n"
        f"Run: python -m dspy_modules.compiler to compile the module first."
    )


# =============================================================================


class PartTranslator:
    """
    Переводчик отдельных частей SQL с использованием скомпилированного DSPy модуля.
    
    Особенности:
    - Каждая часть переводится независимо (нет зависимости от предыдущих частей)
    - Использует singleton get_compiled_module() (модуль загружается один раз)
    - Retry-механизм при ошибках парсинга (до translation_retry_limit раз)
    - Автоматическая очистка markdown из вывода LLM
    - Полный контекст INTERPOLATE без обрезки (для локальной LLM без лимита токенов)
    """
    
    def __init__(
        self, 
        compiled_module: Optional[dspy.Module] = None,
        state_manager: Optional[StateManager] = None
    ):
        """
        Инициализация переводчика.
        
        Args:
            compiled_module: Предзагруженный скомпилированный модуль DSPy.
                           Если None — используется singleton get_compiled_module().
            state_manager: Менеджер состояния для отслеживания прогресса.
        """
        self.state_manager = state_manager
        self.retry_limit = settings.translation_retry_limit
        
        # Используем предоставленный модуль или singleton
        if compiled_module is not None:
            self.compiled_module = compiled_module
        else:
            self.compiled_module = get_compiled_module()
    
    def translate_part(self, part_num: int) -> Tuple[bool, Optional[str]]:
        """
        Переводит одну часть SQL из Vertica в Trino.
        
        Args:
            part_num: Номер части (0-based)
            
        Returns:
            Tuple (success, error_message):
            - success: True если перевод успешен и сохранен
            - error_message: Описание ошибки (если success=False)
        """
        if self.state_manager is None:
            return False, "StateManager not provided"
        
        query_name = self.state_manager.query_name
        print(f"[Translator] [{query_name}] Translating part {part_num}...")
        
        # Проверяем, не переведена ли уже эта часть
        existing_path = self._get_output_path(part_num)
        if existing_path.exists():
            print(f"[Translator] Part {part_num} already translated: {existing_path.name}")
            self.state_manager.set_part_status(part_num, "translated", {"file": str(existing_path)})
            return True, None
        
        # Загружаем исходный Vertica SQL
        vertica_sql = self._load_vertica_part(part_num)
        if vertica_sql is None:
            return False, f"Source file for part {part_num} not found"
        
        last_validation_error: Optional[str] = None

        # Пробуем перевести с retry
        for attempt in range(1, self.retry_limit + 1):
            print(f"[Translator] Attempt {attempt}/{self.retry_limit}...")
            
            try:
                # Передаем part_num для построения контекста INTERPOLATE
                trino_sql = self._translate_with_dspy(
                    vertica_sql,
                    part_num,
                    retry_feedback=last_validation_error,
                )
                
                # Валидация результата
                is_valid, error_msg = self._validate_output(trino_sql)
                
                if is_valid:
                    # Сохраняем результат
                    output_path = self._save_translation(part_num, trino_sql)
                    
                    # Обновляем статус
                    self.state_manager.set_part_status(
                        part_num, 
                        "translated",
                        {"file": str(output_path), "attempts": attempt}
                    )
                    
                    print(f"[Translator] Part {part_num} translated successfully")
                    return True, None
                else:
                    print(f"[Translator] Validation failed: {error_msg}")
                    last_validation_error = error_msg
                    if attempt == self.retry_limit:
                        return False, f"Validation failed after {self.retry_limit} attempts: {error_msg}"
                    
            except Exception as e:
                print(f"[Translator] Error on attempt {attempt}: {e}")
                if attempt == self.retry_limit:
                    return False, f"Translation failed after {self.retry_limit} attempts: {str(e)}"
        
        return False, "Unknown error"
    
    def _translate_with_dspy(
        self,
        vertica_sql: str,
        part_num: int,
        retry_feedback: Optional[str] = None,
    ) -> str:
        """
        Вызов скомпилированного DSPy модуля с учетом INTERPOLATE контекста.
        
        Args:
            vertica_sql: Исходный SQL Vertica
            part_num: Номер части для получения контекста INTERPOLATE
            
        Returns:
            Переведенный SQL Trino
        """
        # Получаем полный контекст INTERPOLATE для этой части (без обрезки)
        context_hint = build_interpolate_context(self.state_manager, part_num)
        
        # Определяем тип части только для внутренней логики (добавление контекста)
        part_type = self._determine_part_type(vertica_sql)
        
        # Добавляем специфические правила для header part_0
        if part_num == 0:
            header_rule_path = settings.header_rules_first_part  # <-- относительный путь из config
            
            if header_rule_path.exists():
                rule_text = header_rule_path.read_text(encoding='utf-8')
                context_hint = f"{rule_text}"
            else:
                print(f"[Translator] Warning: Header rules file not found at {header_rule_path}")

        if retry_feedback:
            retry_block = (
                "=== RETRY FEEDBACK ===\n"
                "The previous translation output failed validation.\n"
                f"Validation error: {retry_feedback}\n"
                "Please fix this issue in the next translation attempt and return only corrected Trino SQL.\n"
            )
            context_hint = f"{context_hint}\n\n{retry_block}" if context_hint else retry_block

        ensure_dspy_defaults()
        lm = get_dspy_lm(
            resolve_stage_profile("translate"),
            max_tokens=settings.llm_max_tokens,
            temperature=0.1,
            cache=False,
            ensure_no_proxy=True,
        )
        with dspy.context(lm=lm, adapter=dspy.ChatAdapter()):
            result = self.compiled_module(
                vertica_sql=vertica_sql,
                context_hint=context_hint
            )
        
        return result.trino_sql
    
    def _determine_part_type(self, sql: str) -> str:
        """
        Определяет тип SQL части для внутренней логики (context hint).
        Не используется в DSPy сигнатуре - только для добавления специфических правил.
        
        Returns:
            "header", "temp_table", "main_table", или "dml_block"
        """
        sql_upper = sql.upper().strip()

        # === @HEADER в любом месте текста ===
        if '@HEADER' in sql_upper:
            return "header"
        
        # Проверяем временные таблицы
        if re.search(r'CREATE\s+(?:LOCAL\s+)?(?:TEMP|TEMPORARY)\s+TABLE', sql_upper):
            return "temp_table"
        
        # DML операции
        if 'INSERT' in sql_upper and 'sandbox' in sql_upper:
            return "dml_block"
        
        # По умолчанию
        return "other"
    
    def _validate_output(self, trino_sql: str) -> Tuple[bool, Optional[str]]:
        """
        Базовая валидация вывода LLM.
        
        Checks:
        - Не пустой ли результат
        - Не является ли результатом объяснением вместо SQL
        """
        if not trino_sql or not trino_sql.strip():
            return False, "Empty output from LLM"
        
        cleaned = trino_sql.strip()
        
        # Проверка что это не markdown с объяснением
        if cleaned.startswith(("Here's", "This is", "The translation", "Note:", "Explanation:")):
            return False, "Output contains explanation instead of SQL"

        if self._is_single_line_output_too_long(cleaned):
            return False, (
                "SQL was returned as a single line longer than 50 characters. "
                "Please preserve readable formatting and rewrite it as multi-line Trino SQL."
            )

        return True, None

    def _is_single_line_output_too_long(self, sql: str) -> bool:
        """Возвращает True, если вывод сплющен в одну длинную строку."""
        lines = [line for line in sql.splitlines() if line.strip()]
        return len(lines) == 1 and len(lines[0].strip()) > 50
    
    def _clean_sql_output(self, sql: str) -> str:
        """Очищает SQL от markdown и лишних пробелов."""
        # Удаляем markdown блоки кода
        sql = re.sub(r'```sql\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'```\s*', '', sql)
        
        # Удаляем начальные комментарии типа "Translated SQL:"
        sql = re.sub(r'^(?:Translated\s+SQL:|SQL:|Trino\s+SQL:)\s*', '', sql, flags=re.IGNORECASE)
        
        return sql.strip()
    
    def _load_vertica_part(self, part_num: int) -> Optional[str]:
        """Загружает исходный SQL части."""
        filename = f"{self.state_manager.query_name}_part_{part_num}.sql"
        path = self.state_manager.vertica_parts_path / filename
        
        if not path.exists():
            return None
        
        return path.read_text(encoding='utf-8')
    
    def _get_output_path(self, part_num: int) -> Path:
        """Возвращает путь для сохранения переведенной части (v0)."""
        filename = f"{self.state_manager.query_name}_part_{part_num}_trino.sql"
        return self.state_manager.trino_parts_path / filename
    
    def _save_translation(self, part_num: int, trino_sql: str) -> Path:
        """Сохраняет переведенный SQL (атомарно)."""
        output_path = self._get_output_path(part_num)
        
        # Создаем директорию если нужно
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Очищаем вывод
        cleaned_sql = self._clean_sql_output(trino_sql)
        
        # Атомарная запись
        temp_path = output_path.with_suffix('.tmp')
        temp_path.write_text(cleaned_sql, encoding='utf-8')
        temp_path.replace(output_path)
        
        return output_path
    
    def translate_all_pending(self) -> Tuple[int, int]:
        """
        Переводит все ожидающие части (convenience метод).
        
        Returns:
            Tuple (success_count, failed_count)
        """
        if self.state_manager is None:
            raise TranslationError("StateManager not provided")
        
        state = self.state_manager.load_state()
        if not state:
            raise TranslationError("No state found")
        
        total_parts = state.get("total_parts", 0)
        success_count = 0
        failed_count = 0
        
        for part_num in range(total_parts):
            # Проверяем статус - пропускаем уже переведенные
            part_status = self.state_manager.get_part_status(part_num)
            if part_status.get("translated"):
                print(f"[Translator] Part {part_num} already translated, skipping")
                success_count += 1
                continue
            
            success, error = self.translate_part(part_num)
            if success:
                success_count += 1
            else:
                failed_count += 1
                print(f"[Translator] Failed to translate part {part_num}: {error}")
        
        return success_count, failed_count

    def _load_part_content(self, part_num: int, is_vertica: bool = True) -> Optional[str]:
        """Загружает содержимое части (Vertica или Trino)."""
        if self.state_manager is None:
            return None

        if is_vertica:
            path = self.state_manager.vertica_parts_path / f"{self.state_manager.query_name}_part_{part_num}.sql"
        else:
            path = self.state_manager.trino_parts_path / f"{self.state_manager.query_name}_part_{part_num}_trino.sql"

        if path.exists():
            return path.read_text(encoding='utf-8')
        return None
