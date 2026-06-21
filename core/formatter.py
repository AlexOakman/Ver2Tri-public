"""
Модуль форматирования Trino SQL через sqlfluff.
Запускается после создания trino_parts.
"""

import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional, Tuple

from config import settings
from core.state_manager import StateManager


class FormatterError(Exception):
    """Исключение для ошибок форматирования."""
    pass


class TrinoFormatter:
    """
    SQL Formatter для Trino используя sqlfluff.
    
    Конфигурация через .env:
    - ENABLE_SQL_FORMATTER=true/false
    - SQL_FORMATTER_DIALECT=trino (или presto)
    - SQL_FORMATTER_RULES=L001,L002,... (опционально, по умолчанию все)
    """
    
    def __init__(self):
        self.enabled = getattr(settings, 'enable_sql_formatter', True)
        self.dialect = getattr(settings, 'sql_formatter_dialect', 'trino')
        self.rules = getattr(settings, 'sql_formatter_rules', None)  # None = все правила
        self.exclude_rules = getattr(settings, 'sql_formatter_exclude_rules', 'L034,L035')  # Опционально
        self._bind_pattern = re.compile(r'(?P<param>:\w+|\$\{\w+\})')
        self.last_errors: list[dict] = []

    def _sanitize_sql_for_sqlfluff(self, sql: str) -> tuple[str, list[tuple[str, str]]]:
        """
        Подменяет bind-параметры на безопасные строковые литералы,
        чтобы sqlfluff мог распарсить запрос.
        """
        replacements: list[tuple[str, str]] = []

        def replace(match: re.Match[str]) -> str:
            token = match.group("param")
            marker = f"'__VER2TRI_PARAM_{len(replacements)}__'"
            replacements.append((marker, token))
            return marker

        return self._bind_pattern.sub(replace, sql), replacements

    @staticmethod
    def _restore_sql_placeholders(sql: str, replacements: list[tuple[str, str]]) -> str:
        """Возвращает исходные bind-параметры после форматирования."""
        restored = sql
        for marker, token in replacements:
            restored = restored.replace(marker, token)
        return restored

    def _build_sqlfluff_cmd(self, action: str, target_path: Path) -> list[str]:
        """Собирает команду запуска sqlfluff через бинарь или module fallback."""
        candidate_bins = [
            shutil.which("sqlfluff"),
            str(Path(sys.executable).with_name("sqlfluff")),
            str(Path(__file__).resolve().parent.parent / ".ver2tri" / "bin" / "sqlfluff"),
        ]

        for candidate in candidate_bins:
            if candidate and Path(candidate).exists():
                return [candidate, action, str(target_path)]

        return [sys.executable, "-m", "sqlfluff", action, str(target_path)]
        
    def format_file(self, file_path: Path) -> Tuple[bool, Optional[str]]:
        """
        Форматирует один SQL файл.
        
        Returns:
            (success, error_message)
        """
        if not self.enabled:
            return True, None
            
        if not file_path.exists():
            return False, f"File not found: {file_path}"
            
        try:
            original_sql = file_path.read_text(encoding='utf-8')
            sanitized_sql, replacements = self._sanitize_sql_for_sqlfluff(original_sql)

            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".sql",
                encoding="utf-8",
                delete=False,
            ) as tmp_file:
                tmp_file.write(sanitized_sql)
                temp_path = Path(tmp_file.name)

            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".cfg",
                encoding="utf-8",
                delete=False,
            ) as cfg_file:
                cfg_file.write("[sqlfluff]\nlarge_file_skip_byte_limit = 0\n")
                config_path = Path(cfg_file.name)

            cmd = self._build_sqlfluff_cmd("fix", temp_path)
            cmd.extend([
                "--dialect", self.dialect,
                "--processes", "1",  # Последовательно для стабильности
                "--config", str(config_path),
            ])
            
            # Добавляем правила если указаны
            if self.rules:
                cmd.extend(["--rules", self.rules])
            if self.exclude_rules:
                cmd.extend(["--exclude-rules", self.exclude_rules])
            
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    timeout=60  # Защита от зависания на сложных файлах
                )
            finally:
                formatted_sql = temp_path.read_text(encoding='utf-8') if temp_path.exists() else sanitized_sql
                temp_path.unlink(missing_ok=True)
                config_path.unlink(missing_ok=True)

            combined_output = f"{result.stdout}\n{result.stderr}".strip()

            if result.returncode not in [0, 1]:
                error_msg = result.stderr if result.stderr else result.stdout
                return False, f"Exit code {result.returncode}: {error_msg[:200]}"

            if "No module named sqlfluff" in combined_output:
                return False, "sqlfluff is not available in the current Python environment"

            if "templating/parsing errors found" in combined_output or "PRS |" in combined_output:
                return False, f"sqlfluff parsing error: {combined_output[:300]}"

            restored_sql = self._restore_sql_placeholders(formatted_sql, replacements)
            unsafe_change = self._unsafe_format_change(original_sql, restored_sql)
            if unsafe_change:
                return False, unsafe_change

            file_path.write_text(restored_sql, encoding='utf-8')
            return True, None
                
        except subprocess.TimeoutExpired:
            return False, "Timeout (60s) exceeded"
        except FileNotFoundError:
            return False, "sqlfluff not found. Install: pip install sqlfluff"
        except Exception as e:
            return False, str(e)

    def _unsafe_format_change(self, original_sql: str, formatted_sql: str) -> Optional[str]:
        original_aliases = self._extract_unnest_column_aliases(original_sql)
        if not original_aliases:
            return None

        formatted_aliases = self._extract_unnest_column_aliases(formatted_sql)
        missing_aliases = [alias for alias in original_aliases if alias not in formatted_aliases]
        if missing_aliases:
            return "sqlfluff unsafe change: removed UNNEST column alias"
        return None

    def _extract_unnest_column_aliases(self, sql: str) -> list[str]:
        aliases: list[str] = []
        for match in re.finditer(r"\bUNNEST\s*\(", sql, re.IGNORECASE):
            open_paren = sql.find("(", match.start())
            close_paren = self._find_matching_paren(sql, open_paren)
            if close_paren == -1:
                continue

            suffix = sql[close_paren + 1:]
            alias_match = re.match(
                r"\s+AS\s+[A-Za-z_][\w$]*\s*\((?P<columns>[^)]*)\)",
                suffix,
                re.IGNORECASE,
            )
            if alias_match:
                columns = ",".join(
                    column.strip().lower()
                    for column in alias_match.group("columns").split(",")
                    if column.strip()
                )
                aliases.append(columns)
        return aliases

    def _find_matching_paren(self, sql: str, open_paren: int) -> int:
        depth = 0
        in_single_quote = False
        index = open_paren
        while index < len(sql):
            char = sql[index]
            if char == "'":
                if in_single_quote and index + 1 < len(sql) and sql[index + 1] == "'":
                    index += 2
                    continue
                in_single_quote = not in_single_quote
            elif not in_single_quote:
                if char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                    if depth == 0:
                        return index
            index += 1
        return -1
    
    def format_parts(self, parts_dir: Path, query_name: str, total_parts: int) -> Tuple[int, int]:
        """
        Форматирует только последние актуальные версии частей Trino SQL.
        `part_0` пропускается, чтобы не менять header/DDL блок.
        
        Args:
            parts_dir: Директория trino_parts/
            query_name: Имя запроса
            total_parts: Общее количество частей
            
        Returns:
            (formatted_count, error_count)
        """
        if not self.enabled:
            print("[Formatter] ℹ️  SQL formatting disabled in .env")
            return 0, 0
            
        if not parts_dir.exists():
            return 0, 0
            
        print(f"\n[Formatter] Running sqlfluff (dialect: {self.dialect})...")
        
        formatted = 0
        errors = 0
        self.last_errors = []
        
        state_manager = StateManager(query_name, base_path=parts_dir.parent.parent)

        # Форматируем только latest-version SQL для исполняемых частей.
        for part_num in range(1, total_parts):
            file_path = state_manager.get_latest_version_path(part_num)
            if file_path is None or not file_path.exists():
                continue

            success, error = self.format_file(file_path)
            if success:
                print(f"[Formatter]   ✓ {file_path.name}")
                formatted += 1
            else:
                print(f"[Formatter]   ⚠️  {file_path.name}: {error}")
                errors += 1
                self.last_errors.append({
                    "file": str(file_path),
                    "message": error,
                })
        
        print(f"[Formatter] Done: {formatted} files formatted, {errors} errors")
        return formatted, errors
    
    def check_file(self, file_path: Path) -> Tuple[bool, list]:
        """
        Проверяет файл на ошибки (lint) без исправления.
        Returns: (is_valid, list_of_violations)
        """
        if not self.enabled or not file_path.exists():
            return True, []
            
        try:
            cmd = [
                "sqlfluff", 
                "lint",
                str(file_path),
                "--dialect", self.dialect,
                "--format", "json"
            ]
            
            result = subprocess.run(
                cmd, capture_output=True, text=True, encoding='utf-8', timeout=30
            )
            
            import json
            violations = json.loads(result.stdout) if result.stdout else []
            return result.returncode == 0, violations
            
        except Exception:
            return True, []  # При ошибке проверки пропускаем
